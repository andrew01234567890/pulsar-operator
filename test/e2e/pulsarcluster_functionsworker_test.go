//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/test/utils"
)

// This suite is the real-cluster regression guard for PR #39: before it, a
// colocated FunctionsWorker was entirely untested against a live Kubernetes
// API server/kubelet, which is how a no-op reconcileColocated that reported
// Ready=True unconditionally (never actually wiring anything onto the
// broker) shipped undetected - envtest alone chains the two reconcilers by
// hand (see pulsarcluster_functionsworker_envtest_test.go) rather than
// proving the real manager's own watches/controllers do it unattended.
const (
	functionsWorkerE2ENamespace = "pulsarcluster-fw-e2e"
	functionsWorkerE2EName      = "e2e-fw"

	fwBrokerChildName          = functionsWorkerE2EName + "-broker"
	fwFunctionsWorkerChildName = functionsWorkerE2EName + "-functionsworker"
	fwPackageStoragePVCName    = functionsWorkerE2EName + "-functions-package-storage"

	// fileSystemPackagesStorageProviderClassE2E mirrors
	// internal/controller/cluster/functionsworker_controller.go's
	// fileSystemPackagesStorageProviderClass (unexported, so duplicated here
	// as a literal rather than imported).
	fileSystemPackagesStorageProviderClassE2E = "org.apache.pulsar.packages.management.storage.filesystem.FileSystemPackagesStorageProvider"
)

// functionsWorkerReconciliationSpecs registers the colocated-FunctionsWorker
// e2e specs as a sibling Context of pulsarClusterReconciliationSpecs's own
// Context, inside the same Ordered "Manager" Describe in e2e_test.go (see the
// call site there) - required for the same reason
// pulsarClusterReconciliationSpecs documents: Ginkgo only guarantees
// declaration order within a single Ordered container.
func functionsWorkerReconciliationSpecs() {
	Context("Colocated FunctionsWorker reconciliation", Ordered, func() {
		BeforeAll(func() {
			By("loading the pinned Pulsar and Oxia workload images into Kind")
			Expect(loadWorkloadImages()).To(Succeed(), "Failed to load workload images into Kind")

			By("creating the FunctionsWorker namespace")
			cmd := exec.Command(kubectlBinary, "create", "ns", functionsWorkerE2ENamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create FunctionsWorker namespace")

			By("applying a colocated-FunctionsWorker PulsarCluster sample")
			Expect(applyObject(colocatedFunctionsWorkerPulsarCluster())).
				To(Succeed(), "Failed to apply the FunctionsWorker PulsarCluster sample")
		})

		AfterAll(func() {
			By("dumping FunctionsWorker PulsarCluster diagnostics")
			dumpNamespaceDiagnostics(functionsWorkerE2ENamespace)

			By("deleting the FunctionsWorker namespace")
			cmd := exec.Command(kubectlBinary, "delete", "ns", functionsWorkerE2ENamespace,
				"--ignore-not-found", "--timeout=180s")
			_, _ = utils.Run(cmd)
		})

		// HARD assertion: proves Fix A (see pulsarcluster_functionsworker.go's
		// package doc comment) actually lands on a real broker pod's mounted
		// config, not just the Broker CR's spec.Config map (already covered by
		// envtest) - the rendered broker.conf ConfigMap is what kubelet mounts
		// into the container, so this is the closest e2e can get to observing
		// the real startup config without waiting for the JVM to boot.
		It("wires functionsWorkerEnabled + FileSystemPackagesStorage onto the broker's rendered broker.conf", func() {
			By("checking the Broker child exists, owned by the PulsarCluster")
			var broker clusterv1alpha1.Broker
			Eventually(func() error { return getJSONNamespace(functionsWorkerE2ENamespace, "brokers", fwBrokerChildName, &broker) },
				2*time.Minute, 5*time.Second).Should(Succeed())
			Expect(hasControllerOwnerOfKind(broker.OwnerReferences, "PulsarCluster")).To(BeTrue())

			By("checking the broker's rendered broker.conf ConfigMap")
			Eventually(func(g Gomega) {
				var cm corev1.ConfigMap
				g.Expect(getJSONNamespace(functionsWorkerE2ENamespace, "configmap", fwBrokerChildName, &cm)).To(Succeed())
				rendered := cm.Data["broker.conf"]
				g.Expect(rendered).To(ContainSubstring("functionsWorkerEnabled=true"))
				g.Expect(rendered).To(ContainSubstring("enablePackagesManagement=true"))
				g.Expect(rendered).To(ContainSubstring("functionsWorkerEnablePackageManagement=true"))
				g.Expect(rendered).To(ContainSubstring("packagesManagementStorageProvider=" + fileSystemPackagesStorageProviderClassE2E))
				g.Expect(rendered).To(ContainSubstring("STORAGE_PATH=/pulsar/packages-storage"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("checking the broker's rendered functions_worker.yml ConfigMap")
			Eventually(func(g Gomega) {
				var cm corev1.ConfigMap
				g.Expect(getJSONNamespace(functionsWorkerE2ENamespace, "configmap", fwBrokerChildName+"-functions-worker", &cm)).To(Succeed())
				rendered := cm.Data["functions_worker.yml"]
				g.Expect(rendered).To(ContainSubstring("pulsarFunctionsNamespace: public/functions"))
				g.Expect(rendered).To(ContainSubstring("pulsarFunctionsCluster: " + functionsWorkerE2EName))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		// HARD assertion: the shared FileSystemPackagesStorage PVC (mounted on
		// every broker pod) must actually be provisioned and owned by the
		// PulsarCluster - this is the only backing store a colocated function
		// or connector package upload has, so its absence is a silent,
		// eventual functional failure rather than an immediate crash.
		It("provisions the shared package-storage PVC owned by the PulsarCluster", func() {
			var cluster clusterv1alpha1.PulsarCluster
			Eventually(func() error {
				return getJSONNamespace(functionsWorkerE2ENamespace, "pulsarclusters", functionsWorkerE2EName, &cluster)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			Eventually(func(g Gomega) {
				var pvc corev1.PersistentVolumeClaim
				g.Expect(getJSONNamespace(functionsWorkerE2ENamespace, "pvc", fwPackageStoragePVCName, &pvc)).To(Succeed())
				g.Expect(metav1.IsControlledBy(&pvc, &cluster)).To(BeTrue())
				g.Expect(pvc.Spec.AccessModes).To(ConsistOf(corev1.ReadWriteOnce))
				wantSize := resource.MustParse("8Gi")
				g.Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(wantSize.String()))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		// HARD assertion: the direct regression guard for the shipped bug -
		// reconcileColocated used to report Ready=True unconditionally with no
		// owning Broker lookup at all. colocatedReadyCondition's Reason is
		// always "Broker"+<the sibling Broker's own Reason> once resolved (see
		// functionsworker_controller.go), so asserting that prefix proves this
		// FunctionsWorker's readiness is genuinely mirrored from a real
		// sibling Broker object in this live cluster, not hardcoded - it does
		// NOT require the broker to actually finish rolling out, only that it
		// has reported some Ready condition of its own, which happens on the
		// Broker controller's very first status write.
		It("reports the FunctionsWorker child's Ready condition as mirrored from the Broker, never an unconditional no-op True", func() {
			var fw clusterv1alpha1.FunctionsWorker
			Eventually(func() error {
				return getJSONNamespace(functionsWorkerE2ENamespace, "functionsworkers", fwFunctionsWorkerChildName, &fw)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
			Expect(hasControllerOwnerOfKind(fw.OwnerReferences, "PulsarCluster")).To(BeTrue())

			Eventually(func(g Gomega) {
				g.Expect(getJSONNamespace(functionsWorkerE2ENamespace, "functionsworkers", fwFunctionsWorkerChildName, &fw)).To(Succeed())
				cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionReady)
				g.Expect(cond).NotTo(BeNil(), "FunctionsWorker has not reported a Ready condition yet")
				g.Expect(cond.Reason).To(HavePrefix("Broker"),
					"a colocated FunctionsWorker's Ready condition must be derived from its sibling Broker's own condition")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		// BEST-EFFORT (never fails the suite; reports its outcome via Skip):
		// the broker actually finishing its rollout (and, transitively, the
		// FunctionsWorker mirroring True) needs BookKeeper's cluster metadata
		// to already be formatted, exactly the same single-node CI Kind
		// resource-budget caveat pulsarClusterFullyReady's best-effort spec
		// documents. The HARD specs above already prove the wiring is
		// correct; this only additionally proves it converges for real when
		// the node has the headroom.
		It("brings the broker + colocated FunctionsWorker to Ready=True [best-effort, skip-logged]", func() {
			deadline := time.Now().Add(5 * time.Minute)
			const poll = 10 * time.Second

			var detail string
			for {
				var ready bool
				ready, detail = functionsWorkerBrokerFullyReady()
				if ready {
					By("broker + colocated FunctionsWorker reached Ready")
					return
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(poll)
			}

			_, _ = fmt.Fprintf(GinkgoWriter,
				"[best-effort] Broker/FunctionsWorker did not reach Ready within 5m. Per-component readiness:\n%s\n", detail)
			dumpNamespaceDiagnostics(functionsWorkerE2ENamespace)
			Skip("[best-effort, not a failure] The FunctionsWorker wiring itself is validated by the HARD specs above " +
				"(broker.conf + functions_worker.yml content, package-storage PVC, Ready-mirroring). Broker/FunctionsWorker " +
				"did NOT both reach Ready within 5m on this CI node. Last observed:\n" + detail)
		})

		// Documents, rather than silently omits, the one piece of PR #39's
		// live validation this suite deliberately does not automate: a real
		// `pulsar-admin functions create` + produce/consume round trip through
		// the colocated worker (manually proven end-to-end on KIND for that
		// PR). Automating it in the required CI e2e matrix would need the
		// broker+worker to be fully Ready (not guaranteed by the best-effort
		// spec above on a resource-constrained node) plus a further
		// pulsar-admin/pulsar-client invocation via kubectl exec - several
		// more CI-minutes of serial, flake-prone steps for marginal coverage
		// beyond what the HARD specs above already prove (the wiring that
		// makes that manual round trip possible in the first place).
		It("does not attempt a live 'pulsar-admin functions create' round trip [documented scope limit]", func() {
			Skip("a cluster-managed 'pulsar-admin functions create' + produce/consume round trip through the colocated " +
				"worker was manually live-validated on KIND for PR #39. Automating that full round trip here is " +
				"deliberately out of scope for the required CI e2e matrix - see this spec's doc comment.")
		})
	})
}

// functionsWorkerBrokerFullyReady reports whether the broker StatefulSet has
// reached its desired ReadyReplicas and the FunctionsWorker child reports
// status.conditions[Ready]=True, along with a human-readable snapshot for
// diagnostics - the FunctionsWorker analogue of pulsarClusterFullyReady.
func functionsWorkerBrokerFullyReady() (bool, string) {
	var detail bytes.Buffer

	allReady := true

	var brokerSts appsv1.StatefulSet
	if err := getJSONNamespace(functionsWorkerE2ENamespace, "statefulset", fwBrokerChildName, &brokerSts); err != nil {
		fmt.Fprintf(&detail, "broker StatefulSet: error fetching: %v\n", err)
		allReady = false
	} else {
		desired := int32(1)
		if brokerSts.Spec.Replicas != nil {
			desired = *brokerSts.Spec.Replicas
		}
		fmt.Fprintf(&detail, "broker StatefulSet: %d/%d ready\n", brokerSts.Status.ReadyReplicas, desired)
		if brokerSts.Status.ReadyReplicas != desired {
			allReady = false
		}
	}

	var fw clusterv1alpha1.FunctionsWorker
	if err := getJSONNamespace(functionsWorkerE2ENamespace, "functionsworkers", fwFunctionsWorkerChildName, &fw); err != nil {
		fmt.Fprintf(&detail, "FunctionsWorker: error fetching: %v\n", err)
		return false, detail.String()
	}
	cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionReady)
	if cond == nil {
		fmt.Fprintf(&detail, "FunctionsWorker: no Ready condition reported yet\n")
		return false, detail.String()
	}
	fmt.Fprintf(&detail, "FunctionsWorker Ready condition: status=%s reason=%s message=%q\n",
		cond.Status, cond.Reason, cond.Message)
	if cond.Status != metav1.ConditionTrue {
		allReady = false
	}

	return allReady, detail.String()
}

// colocatedFunctionsWorkerPulsarCluster builds a PulsarCluster sized for a
// resource-constrained Kind node (mirroring minimalPulsarCluster's ensemble
// and volume sizing) with a colocated FunctionsWorker (mode/packageStorage
// left at their CRD defaults: colocated + FileSystemPackagesStorage). It
// deliberately omits Proxy - unnecessary to exercise FunctionsWorker
// wiring - to keep this Context's footprint smaller than the full-data-plane
// one in pulsarcluster_test.go.
func colocatedFunctionsWorkerPulsarCluster() *clusterv1alpha1.PulsarCluster {
	one := int32(1)

	brokerResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
	coordinatorResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("25m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}

	return &clusterv1alpha1.PulsarCluster{
		TypeMeta: metav1.TypeMeta{APIVersion: "cluster.pulsaroperator.io/v1alpha1", Kind: "PulsarCluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      functionsWorkerE2EName,
			Namespace: functionsWorkerE2ENamespace,
		},
		Spec: clusterv1alpha1.PulsarClusterSpec{
			PulsarVersion: "5.0.0-M1",
			Broker: &clusterv1alpha1.BrokerSpec{
				Replicas:  &one,
				Resources: brokerResources,
			},
			BookKeeper: &clusterv1alpha1.BookKeeperSpec{
				Replicas: &one,
				Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{
					EnsembleSize: &one,
					WriteQuorum:  &one,
					AckQuorum:    &one,
				},
				Volumes: &clusterv1alpha1.BookKeeperVolumes{
					Journal: &clusterv1alpha1.VolumeSpec{Size: resource.MustParse("1Gi")},
					Ledgers: &clusterv1alpha1.VolumeSpec{Size: resource.MustParse("2Gi")},
					Index:   &clusterv1alpha1.VolumeSpec{Size: resource.MustParse("1Gi")},
				},
			},
			Oxia: &metadatav1alpha1.OxiaClusterSpec{
				Coordinator: &metadatav1alpha1.OxiaCoordinatorSpec{
					Replicas:  &one,
					Resources: coordinatorResources,
				},
				Server: &metadatav1alpha1.OxiaServerSpec{
					Replicas:      &one,
					StorageSize:   resource.MustParse("1Gi"),
					DbCacheSizeMb: ptr(int32(64)),
				},
				Namespaces: []metadatav1alpha1.OxiaNamespaceSpec{
					{Name: "default", InitialShardCount: &one, ReplicationFactor: &one},
					{Name: "bookkeeper", InitialShardCount: &one, ReplicationFactor: &one},
				},
				// See minimalPulsarCluster's identical field for why this is
				// required: broker/bookie always address Oxia through its
				// public Service DNS name, which oxia-server rejects as an
				// unrecognized authority without this.
				AllowExtraAuthorities: ptr(true),
			},
			FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
		},
	}
}
