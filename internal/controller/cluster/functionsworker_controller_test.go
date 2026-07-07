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

package cluster

import (
	"context"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

var _ = Describe("FunctionsWorker Controller", func() {
	const resourceNamespace = "default"

	ctx := context.Background()

	reconcileFunctionsWorker := func(name string) *clusterv1alpha1.FunctionsWorker {
		key := types.NamespacedName{Name: name, Namespace: resourceNamespace}
		controllerReconciler := &FunctionsWorkerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		fw := &clusterv1alpha1.FunctionsWorker{}
		Expect(k8sClient.Get(ctx, key, fw)).To(Succeed())
		return fw
	}

	Context("reconciling a colocated-mode FunctionsWorker (the default) with no owning PulsarCluster", func() {
		const resourceName = "functionsworker-colocated"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.FunctionsWorker{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		// Regression: colocated mode used to unconditionally report Ready=True
		// even though it manages no workload of its own (the embedded worker
		// actually runs inside broker pods). With no owning PulsarCluster to
		// find the sibling Broker through, there is no readiness signal to
		// report at all, so it must say Unknown rather than lie True.
		It("manages no workload and reports Unknown readiness rather than an unconditional Ready=True", func() {
			fw := reconcileFunctionsWorker(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.StatefulSet{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, key, &corev1.Service{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, &policyv1.PodDisruptionBudget{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal("BrokerOwnerUnknown"))
		})
	})

	Context("reconciling a colocated-mode FunctionsWorker owned by a PulsarCluster", func() {
		const clusterName = "fw-owner-cluster"
		const resourceName = clusterName + "-functionsworker"
		const brokerName = clusterName + "-broker"

		BeforeEach(func() {
			fw := &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: resourceNamespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: clusterv1alpha1.GroupVersion.String(),
						Kind:       pulsarClusterOwnerKind,
						Name:       clusterName,
						UID:        types.UID("fw-owner-cluster-uid"),
						Controller: ptr(true),
					}},
				},
				Spec: clusterv1alpha1.FunctionsWorkerSpec{},
			}
			Expect(k8sClient.Create(ctx, fw)).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.FunctionsWorker{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &clusterv1alpha1.Broker{ObjectMeta: metav1.ObjectMeta{Name: brokerName, Namespace: resourceNamespace}}))).To(Succeed())
		})

		It("reports not-Ready when the sibling Broker does not exist yet", func() {
			fw := reconcileFunctionsWorker(resourceName)
			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("BrokerMissing"))
		})

		// This is the core "readiness mirrors the broker" regression test:
		// colocated FunctionsWorker readiness must track whatever the actual
		// broker (the thing running the embedded worker) reports, not an
		// unconditional True.
		It("mirrors the sibling Broker's own Ready condition", func() {
			broker := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: brokerName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.BrokerSpec{},
			}
			Expect(k8sClient.Create(ctx, broker)).To(Succeed())

			By("the broker exists but has not reported a Ready condition yet")
			fw := reconcileFunctionsWorker(resourceName)
			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("BrokerStatusMissing"))

			By("the broker reports Ready")
			broker.Status.Conditions = []metav1.Condition{{
				Type:               conditionTypeReady,
				Status:             metav1.ConditionTrue,
				Reason:             reasonAllReady,
				Message:            "all 3 broker replicas ready and up to date",
				ObservedGeneration: broker.Generation,
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

			fw = reconcileFunctionsWorker(resourceName)
			cond = apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("Broker" + reasonAllReady))
			Expect(cond.Message).To(ContainSubstring("all 3 broker replicas ready"))

			By("the broker regresses to not-Ready (e.g. a rolling restart)")
			broker.Status.Conditions = []metav1.Condition{{
				Type:               conditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             reasonProgressing,
				Message:            "rollout in progress",
				ObservedGeneration: broker.Generation,
				LastTransitionTime: metav1.Now(),
			}}
			Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

			fw = reconcileFunctionsWorker(resourceName)
			cond = apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("Broker" + reasonProgressing))
		})
	})

	// Fix B: standalone FunctionsWorker cannot run on this Oxia-only operator
	// (Pulsar's standalone functions-worker startup unconditionally requires
	// a ZooKeeper-backed metadata store for its DistributedLog package
	// storage, with no config workaround), so the CRD's CEL rule must reject
	// it outright - it is no longer possible to get a standalone-mode
	// FunctionsWorker into the cluster at all, by any path (create or
	// update), which is what makes reconcileStandalone's own StatefulSet/
	// Service/ConfigMap/PDB-building logic unreachable in practice; it is
	// kept in the codebase only in case a future upstream Pulsar release
	// lifts the DLog limitation (see the CEL rule's message on
	// FunctionsWorkerSpec).
	Context("standalone mode is rejected outright", func() {
		const resourceName = "functionsworker-standalone-rejected"

		It("refuses to create a standalone-mode FunctionsWorker", func() {
			err := k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{Mode: functionsWorkerModeStandalone},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(functionsWorkerModeStandalone))
			Expect(err.Error()).To(ContainSubstring(functionsWorkerModeColocated))
		})

		It("refuses to update an existing colocated FunctionsWorker to standalone", func() {
			fw := &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{},
			}
			Expect(k8sClient.Create(ctx, fw)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, fw)).To(Succeed())
			}()

			fw.Spec.Mode = functionsWorkerModeStandalone
			Expect(k8sClient.Update(ctx, fw)).To(HaveOccurred())
		})
	})

	// Fix #1: non-FileSystem package storage crashes the broker on Oxia (the
	// provider falls back to the ZooKeeper-backed BookKeeper default), so the
	// CRD's CEL rule rejects it outright with a message pointing at
	// FileSystemPackagesStorage.
	Context("non-FileSystem package storage is rejected outright", func() {
		DescribeTable("rejecting a non-FileSystem packageStorage",
			func(packageStorage string) {
				err := k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
					ObjectMeta: metav1.ObjectMeta{Name: "fw-pkgstorage-" + packageStorage, Namespace: resourceNamespace},
					Spec:       clusterv1alpha1.FunctionsWorkerSpec{PackageStorage: packageStorage},
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("FileSystemPackagesStorage"))
			},
			Entry("S3 package storage", testPackageStorageS3),
			Entry("GCS package storage", testPackageStorageGCS),
		)

		It("accepts FileSystemPackagesStorage (the default)", func() {
			fw := &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: "fw-pkgstorage-fs", Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{PackageStorage: packageStorageFileSystem},
			}
			Expect(k8sClient.Create(ctx, fw)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fw)).To(Succeed())
		})
	})

	// Regression companion to Fix B: even though standalone can no longer be
	// created, colocated mode's cleanup of standalone-shaped leftovers (e.g.
	// from an operator version predating this CEL rule) must still work.
	// This exercises that cleanup directly against plain, CEL-unrestricted
	// child objects rather than via an actual standalone-mode FunctionsWorker
	// (which can no longer exist).
	Context("colocated mode cleans up leftover standalone-shaped resources", func() {
		const resourceName = "functionsworker-cleanup"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{},
			})).To(Succeed())

			Expect(k8sClient.Create(ctx, &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: appsv1.StatefulSetSpec{
					Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"k": "v"}},
					ServiceName: resourceName,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"k": "v"}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "i"}}},
					},
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       corev1.ServiceSpec{ClusterIP: "None", Ports: []corev1.ServicePort{{Port: 1}}},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-config", Namespace: resourceNamespace},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName + "-pdb", Namespace: resourceNamespace},
				Spec: policyv1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"k": "v"}},
				},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.FunctionsWorker{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("deletes the leftover StatefulSet, Service, ConfigMap and PDB and reports Unknown readiness (no owner)", func() {
			fw := reconcileFunctionsWorker(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.StatefulSet{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, key, &corev1.Service{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, &policyv1.PodDisruptionBudget{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("BrokerOwnerUnknown"))
		})
	})

	Context("when the FunctionsWorker is not found", func() {
		It("returns without error", func() {
			controllerReconciler := &FunctionsWorkerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: testResourceNotFound, Namespace: resourceNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

func TestFunctionsWorkerMode(t *testing.T) {
	tests := []struct {
		name string
		spec clusterv1alpha1.FunctionsWorkerSpec
		want string
	}{
		{name: "unset defaults to colocated", spec: clusterv1alpha1.FunctionsWorkerSpec{}, want: functionsWorkerModeColocated},
		{name: "explicit colocated", spec: clusterv1alpha1.FunctionsWorkerSpec{Mode: "colocated"}, want: functionsWorkerModeColocated},
		{name: "explicit standalone", spec: clusterv1alpha1.FunctionsWorkerSpec{Mode: "standalone"}, want: functionsWorkerModeStandalone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := functionsWorkerMode(tt.spec); got != tt.want {
				t.Errorf("functionsWorkerMode(%+v) = %q, want %q", tt.spec, got, tt.want)
			}
		})
	}
}

func TestFunctionsWorkerReplicas(t *testing.T) {
	four := int32(4)
	tests := []struct {
		name string
		spec clusterv1alpha1.FunctionsWorkerSpec
		want int32
	}{
		{name: testCaseUnsetDefaultsToOne, spec: clusterv1alpha1.FunctionsWorkerSpec{}, want: 1},
		{name: testCaseExplicitValueWins, spec: clusterv1alpha1.FunctionsWorkerSpec{Replicas: &four}, want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := functionsWorkerReplicas(tt.spec); got != tt.want {
				t.Errorf("functionsWorkerReplicas(%+v) = %d, want %d", tt.spec, got, tt.want)
			}
		})
	}
}

// TestFunctionsWorkerPackageStorageConfig pins the Oxia-only-cluster
// requirement: FileSystemPackagesStorage (the default and the only
// core-Pulsar-builtin option) must default the provider class so Functions
// work without a ZooKeeper-backed BookKeeperPackagesStorage.
func TestFunctionsWorkerPackageStorageConfig(t *testing.T) {
	tests := []struct {
		name           string
		packageStorage string
		wantProvider   string
	}{
		{name: "unset defaults to filesystem provider", packageStorage: "", wantProvider: fileSystemPackagesStorageProviderClass},
		{name: "explicit FileSystemPackagesStorage", packageStorage: packageStorageFileSystem, wantProvider: fileSystemPackagesStorageProviderClass},
		{name: "S3PackagesStorage has no built-in provider default", packageStorage: testPackageStorageS3, wantProvider: ""},
		{name: "GCSPackagesStorage has no built-in provider default", packageStorage: testPackageStorageGCS, wantProvider: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := functionsWorkerPackageStorageConfig(tt.packageStorage)
			if got["functionsWorkerEnablePackageManagement"] != configValTrue {
				t.Errorf("functionsWorkerEnablePackageManagement = %q, want \"true\"", got["functionsWorkerEnablePackageManagement"])
			}
			if provider := got["packagesManagementStorageProvider"]; provider != tt.wantProvider {
				t.Errorf("packagesManagementStorageProvider = %q, want %q", provider, tt.wantProvider)
			}
		})
	}
}

func TestFunctionsWorkerMergedConfigUserOverrideWins(t *testing.T) {
	got := functionsWorkerMergedConfig(clusterv1alpha1.FunctionsWorkerSpec{
		Config: map[string]string{"packagesManagementStorageProvider": "com.example.CustomProvider"},
	})
	if got["packagesManagementStorageProvider"] != "com.example.CustomProvider" {
		t.Errorf("packagesManagementStorageProvider = %q, want user override", got["packagesManagementStorageProvider"])
	}
}

func TestFunctionsWorkerMergedConfigLeavesBrokerURLsBlank(t *testing.T) {
	got := functionsWorkerMergedConfig(clusterv1alpha1.FunctionsWorkerSpec{})
	for _, key := range []string{cfgKeyPulsarServiceURL, cfgKeyPulsarWebServiceURL, configKeyConfigurationMetadataStoreURL} {
		if v, ok := got[key]; !ok || v != "" {
			t.Errorf("%s = %q (present=%v), want blank (must not invent broker/metadata-store naming)", key, v, ok)
		}
	}
}

// TestFunctionsWorkerDefaultConfig pins EVERY key/value in
// functionsWorkerDefaultConfig(). This is the crash-guard regression test:
// most of these keys have no compiled-in Java default in WorkerConfig, and a
// silent drop of any one of them crashes the broker at startup (or wedges the
// embedded worker) the moment functionsWorkerEnabled is exercised for real -
// verified against a live cluster:
//   - schedulerClassName / functionRuntimeFactoryClassName -> NullPointerException
//     (Class.forName(null))
//   - failureCheckFreqMs / instanceLivenessCheckFreqMs -> IllegalArgumentException
//     (scheduleAtFixedRate period 0)
//   - functionMetadataTopicName / clusterCoordinationTopicName /
//     functionAssignmentTopicName -> all three internal topics collapse to
//     persistent://<ns>/null and fence each other -> permanent "Leader not
//     yet ready"
//
// The whole map is asserted exactly (not just spot-checked) so that neither a
// dropped key nor an unvetted added key slips through unnoticed. Values match
// upstream's shipped conf/functions_worker.yml.
func TestFunctionsWorkerDefaultConfig(t *testing.T) {
	want := map[string]string{
		configKeyConfigurationMetadataStoreURL: "",
		cfgKeyPulsarServiceURL:                 "",
		cfgKeyPulsarWebServiceURL:              "",
		cfgKeyWorkerPort:                       "6750",
		"numFunctionPackageReplicas":           "1",
		"downloadDirectory":                    "download/pulsar_functions",
		"connectorsDirectory":                  "./connectors",
		"functionsDirectory":                   "./functions",
		"pulsarFunctionsNamespace":             "public/functions",
		cfgKeyPulsarFunctionsCluster:           functionsWorkerClusterPlaceholder,
		"schedulerClassName":                   "org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler",
		"functionRuntimeFactoryClassName":      "org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory",
		"failureCheckFreqMs":                   "30000",
		"instanceLivenessCheckFreqMs":          "30000",
		"initialBrokerReconnectMaxRetries":     "60",
		"assignmentWriteMaxRetries":            "60",
		"functionMetadataTopicName":            "metadata",
		"clusterCoordinationTopicName":         "coordinate",
		"functionAssignmentTopicName":          "assignments",
	}

	got := functionsWorkerDefaultConfig()
	if len(got) != len(want) {
		t.Errorf("functionsWorkerDefaultConfig has %d keys, want %d: got %v", len(got), len(want), got)
	}
	for k, wantVal := range want {
		gotVal, ok := got[k]
		if !ok {
			t.Errorf("missing key %q (its absence crashes the embedded worker)", k)
			continue
		}
		if gotVal != wantVal {
			t.Errorf("%s = %q, want %q", k, gotVal, wantVal)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected key %q added to defaults without a pinned value", k)
		}
	}
}

// TestRenderFunctionsWorkerYAML covers the functionRuntimeFactoryConfigs
// nested-mapping default: it must always be present as at least an empty
// mapping (an absent key crashes the runtime factory with an NPE), but a
// user override in spec.config must be honored, not duplicated.
func TestRenderFunctionsWorkerYAML(t *testing.T) {
	t.Run("appends empty nested mapping when absent", func(t *testing.T) {
		out := renderFunctionsWorkerYAML(map[string]string{cfgKeyWorkerPort: "6750"})
		if !strings.Contains(out, "functionRuntimeFactoryConfigs: {}") {
			t.Errorf("rendered functions_worker.yml missing nested default:\n%s", out)
		}
	})

	t.Run("honors a user override without duplicating the key", func(t *testing.T) {
		out := renderFunctionsWorkerYAML(map[string]string{"functionRuntimeFactoryConfigs": "custom-runtime-configs"})
		if strings.Contains(out, "functionRuntimeFactoryConfigs: {}") {
			t.Errorf("nested default clobbered/duplicated the user override:\n%s", out)
		}
		if strings.Count(out, "functionRuntimeFactoryConfigs:") != 1 {
			t.Errorf("functionRuntimeFactoryConfigs emitted %d times, want exactly 1:\n%s",
				strings.Count(out, "functionRuntimeFactoryConfigs:"), out)
		}
	})
}

func TestWithFunctionsWorkerClusterDefault(t *testing.T) {
	got := withFunctionsWorkerClusterDefault(nil, "fw-cluster")
	if got[cfgKeyPulsarFunctionsCluster] != "fw-cluster" {
		t.Errorf("pulsarFunctionsCluster = %q, want %q", got[cfgKeyPulsarFunctionsCluster], "fw-cluster")
	}

	overridden := withFunctionsWorkerClusterDefault(map[string]string{cfgKeyPulsarFunctionsCluster: "user-set"}, "fw-cluster")
	if overridden[cfgKeyPulsarFunctionsCluster] != "user-set" {
		t.Errorf("pulsarFunctionsCluster = %q, want user override preserved", overridden[cfgKeyPulsarFunctionsCluster])
	}
}

func TestFunctionsWorkerImage(t *testing.T) {
	if got := functionsWorkerImage(""); got != functionsWorkerDefaultImage {
		t.Errorf("functionsWorkerImage(\"\") = %q, want default %q", got, functionsWorkerDefaultImage)
	}
	if got := functionsWorkerImage(testCustomImage); got != testCustomImage {
		t.Errorf("functionsWorkerImage(custom) = %q, want %q", got, testCustomImage)
	}
}
