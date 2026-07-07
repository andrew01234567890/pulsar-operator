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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// This spec is the umbrella's single start-to-finish integration story: every
// component wired up together, the operator-injected config landing on the
// right children, the Broker's OWN reconciler chained onto the
// umbrella-produced Broker CR to prove the #28 load-balancer defaults compose
// correctly with the umbrella's injected metadata keys in the final rendered
// broker.conf, the dual-purpose metadata-init Job, an ordered version
// rollout, and finally deletion. Every other envtest spec in this package
// drives only PulsarClusterReconciler in isolation (or, for
// pulsarcluster_upgrade_envtest_test.go, drives the same ordered-rollout
// machinery but without any of the metadata/load-balancer/deletion
// assertions); this is deliberately the only spec that chains a second
// reconciler and walks the full create -> bootstrap -> upgrade -> delete
// story in one flow, so it is not a duplicate of any single existing test.
var _ = Describe("PulsarCluster full lifecycle", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		clusterName string
		req         reconcile.Request
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-lifecycle-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		reconciler = &PulsarClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		clusterName = "lifecycle-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	It("provisions every component, wires operator-injected config end to end, bootstraps cluster metadata, rolls out a version bump in dependency order, and preserves owner references through deletion", func() {
		const versionBefore = "5.0.0-M1"
		const versionAfter = "5.0.1"
		imageFor := func(v string) string { return "apachepulsar/pulsar:" + v }

		oxiaKey := types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}
		bookKeeperKey := types.NamespacedName{Name: clusterName + "-bookkeeper", Namespace: namespace.Name}
		brokerKey := types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}
		proxyKey := types.NamespacedName{Name: clusterName + "-proxy", Namespace: namespace.Name}
		autoRecoveryKey := types.NamespacedName{Name: clusterName + "-autorecovery", Namespace: namespace.Name}
		functionsWorkerKey := types.NamespacedName{Name: clusterName + "-functionsworker", Namespace: namespace.Name}
		metadataInitKey := types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}

		getOxia := func() *metadatav1alpha1.OxiaCluster {
			o := &metadatav1alpha1.OxiaCluster{}
			Expect(k8sClient.Get(ctx, oxiaKey, o)).To(Succeed())
			return o
		}
		getBookKeeper := func() *clusterv1alpha1.BookKeeper {
			bk := &clusterv1alpha1.BookKeeper{}
			Expect(k8sClient.Get(ctx, bookKeeperKey, bk)).To(Succeed())
			return bk
		}
		getBroker := func() *clusterv1alpha1.Broker {
			b := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, brokerKey, b)).To(Succeed())
			return b
		}
		getProxy := func() *clusterv1alpha1.Proxy {
			p := &clusterv1alpha1.Proxy{}
			Expect(k8sClient.Get(ctx, proxyKey, p)).To(Succeed())
			return p
		}
		getAutoRecovery := func() *clusterv1alpha1.AutoRecovery {
			ar := &clusterv1alpha1.AutoRecovery{}
			Expect(k8sClient.Get(ctx, autoRecoveryKey, ar)).To(Succeed())
			return ar
		}
		getFunctionsWorker := func() *clusterv1alpha1.FunctionsWorker {
			fw := &clusterv1alpha1.FunctionsWorker{}
			Expect(k8sClient.Get(ctx, functionsWorkerKey, fw)).To(Succeed())
			return fw
		}
		getCluster := func() *clusterv1alpha1.PulsarCluster {
			c := &clusterv1alpha1.PulsarCluster{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, c)).To(Succeed())
			return c
		}
		// markReady takes an ALREADY-FRESHLY-FETCHED child (never re-Gets
		// itself), sets its Ready condition for its own current generation,
		// and writes it straight back - so it can never 409-conflict on a
		// stale ResourceVersion, e.g. right after chaining BrokerReconciler
		// onto the Broker below.
		markReady := func(obj client.Object) {
			var conditions *[]metav1.Condition
			switch o := obj.(type) {
			case *metadatav1alpha1.OxiaCluster:
				conditions = &o.Status.Conditions
			case *clusterv1alpha1.BookKeeper:
				conditions = &o.Status.Conditions
			case *clusterv1alpha1.AutoRecovery:
				conditions = &o.Status.Conditions
			case *clusterv1alpha1.Broker:
				conditions = &o.Status.Conditions
			case *clusterv1alpha1.Proxy:
				conditions = &o.Status.Conditions
			case *clusterv1alpha1.FunctionsWorker:
				conditions = &o.Status.Conditions
			}
			*conditions = []metav1.Condition{readyConditionForGeneration(obj.GetGeneration(), "ready")}
			Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())
		}

		By("creating a PulsarCluster configuring every component (Oxia + BookKeeper + Broker + Proxy + AutoRecovery + FunctionsWorker)")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				PulsarVersion: versionBefore,
				Oxia:          &metadatav1alpha1.OxiaClusterSpec{Server: &metadatav1alpha1.OxiaServerSpec{}},
				Broker:        &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Volumes: &clusterv1alpha1.BookKeeperVolumes{Journal: &clusterv1alpha1.VolumeSpec{}},
				},
				AutoRecovery:    &clusterv1alpha1.AutoRecoverySpec{Replicas: ptr(int32(1))},
				Proxy:           &clusterv1alpha1.ProxySpec{},
				FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling the umbrella once: every child is created eagerly")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("every child carries a controller owner reference back to the PulsarCluster")
		Expect(metav1.IsControlledBy(getOxia(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getBookKeeper(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getBroker(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getProxy(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getAutoRecovery(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getFunctionsWorker(), pulsarCluster)).To(BeTrue())

		By("the operator-injected metadata config landed on the right children")
		wantOxiaURL := "oxia://" + clusterName + "-oxia-oxia:6648/default"
		Expect(getBroker().Spec.Config).To(HaveKeyWithValue(configKeyMetadataStoreURL, wantOxiaURL))
		Expect(getBroker().Spec.Config).To(HaveKeyWithValue(configKeyConfigurationMetadataStoreURL, wantOxiaURL))
		Expect(getBroker().Spec.Config).To(HaveKeyWithValue(configKeyClusterName, clusterName))

		Expect(getProxy().Spec.Config).To(HaveKeyWithValue(configKeyMetadataStoreURL, wantOxiaURL))
		Expect(getProxy().Spec.Config).To(HaveKeyWithValue(configKeyClusterName, clusterName))

		wantBookieURI := "metadata-store:oxia://" + clusterName + "-oxia-oxia:6648/bookkeeper"
		Expect(getBookKeeper().Spec.Config).To(HaveKeyWithValue(configKeyMetadataServiceURI, wantBookieURI))

		By("chaining the Broker's OWN reconciler onto the umbrella-produced Broker CR: the #28 load-balancer defaults land in the rendered broker.conf alongside the umbrella-injected metadata keys")
		brokerReconciler := &BrokerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err = brokerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: brokerKey})
		Expect(err).NotTo(HaveOccurred())

		brokerConfigMap := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, brokerKey, brokerConfigMap)).To(Succeed())
		renderedBrokerConf := brokerConfigMap.Data[brokerConfFileName]
		Expect(renderedBrokerConf).To(ContainSubstring(confKeyLoadManagerClassName + "=" + extensibleLoadManagerClassName))
		Expect(renderedBrokerConf).To(ContainSubstring(confKeyLoadBalancerLoadSheddingStrategy + "=" + transferShedderClassName))
		Expect(renderedBrokerConf).To(ContainSubstring(confKeyLoadBalancerTransferEnabled + "=" + configValTrue))
		Expect(renderedBrokerConf).To(ContainSubstring(configKeyMetadataStoreURL + "=" + wantOxiaURL))

		By("marking Oxia Ready so the cluster-metadata-init Job can be created")
		markReady(getOxia())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("the metadata-init Job carries an owner reference and its script bootstraps BOTH BookKeeper's own cluster metadata AND Pulsar's, wired against the oxia:// store")
		metadataInitJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, metadataInitKey, metadataInitJob)).To(Succeed())
		Expect(metav1.IsControlledBy(metadataInitJob, pulsarCluster)).To(BeTrue())

		script := metadataInitJob.Spec.Template.Spec.Containers[0].Args[0]
		Expect(script).To(ContainSubstring("bin/bookkeeper shell whatisinstanceid"))
		Expect(script).To(ContainSubstring("bin/bookkeeper shell initnewcluster"))
		Expect(script).To(ContainSubstring("bin/pulsar initialize-cluster-metadata"))
		Expect(script).To(ContainSubstring(`--metadata-store "` + wantOxiaURL + `"`))
		Expect(script).To(ContainSubstring(`--configuration-store "` + wantOxiaURL + `"`))
		Expect(strings.Index(script, "initnewcluster")).To(
			BeNumerically("<", strings.Index(script, "initialize-cluster-metadata")),
			"BookKeeper's own cluster metadata must be bootstrapped before Pulsar's initialize-cluster-metadata")

		By("marking the metadata-init Job Succeeded (envtest runs no Job controller to run it)")
		metadataInitJob.Status.Succeeded = 1
		Expect(k8sClient.Status().Update(ctx, metadataInitJob)).To(Succeed())
		succeededJobResourceVersion := metadataInitJob.ResourceVersion

		By("marking every remaining tier Ready for its current generation")
		markReady(getBookKeeper())
		markReady(getAutoRecovery())
		markReady(getBroker())
		markReady(getProxy())
		markReady(getFunctionsWorker())

		By("re-reconciling: MetadataInitialized and the aggregated Ready condition both go True")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(apimeta.IsStatusConditionTrue(getCluster().Status.Conditions, conditionTypeMetadataInitialized)).To(BeTrue())
		Expect(apimeta.IsStatusConditionTrue(getCluster().Status.Conditions, conditionTypeReady)).To(BeTrue())

		By("bumping spec.pulsarVersion: BookKeeper/AutoRecovery roll immediately (Oxia already settled), while Broker/Proxy/FunctionsWorker stay pinned to the old image, gated behind them")
		cluster := getCluster()
		cluster.Spec.PulsarVersion = versionAfter
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(getBookKeeper().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getAutoRecovery().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getBroker().Spec.Image).To(Equal(imageFor(versionBefore)), "Broker is gated behind BookKeeper/AutoRecovery settling")
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		By("the already-succeeded metadata-init Job is left completely untouched by the in-flight rollout")
		untouchedJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, metadataInitKey, untouchedJob)).To(Succeed())
		Expect(untouchedJob.ResourceVersion).To(Equal(succeededJobResourceVersion))
		Expect(apimeta.IsStatusConditionTrue(getCluster().Status.Conditions, conditionTypeMetadataInitialized)).To(BeTrue())

		By("settling BookKeeper and AutoRecovery unblocks Broker only - Proxy/FunctionsWorker stay deferred")
		markReady(getBookKeeper())
		markReady(getAutoRecovery())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(getBroker().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		By("settling Broker unblocks Proxy, then settling Proxy unblocks FunctionsWorker, completing the rollout")
		markReady(getBroker())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		markReady(getProxy())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionAfter)))

		markReady(getFunctionsWorker())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		finalCluster := getCluster()
		Expect(apimeta.IsStatusConditionTrue(finalCluster.Status.Conditions, conditionTypeReady)).To(BeTrue())
		upgrading := apimeta.FindStatusCondition(finalCluster.Status.Conditions, conditionTypeUpgrading)
		Expect(upgrading).NotTo(BeNil())
		Expect(upgrading.Status).To(Equal(metav1.ConditionFalse))
		Expect(upgrading.Reason).To(Equal(reasonUpgradeSettled))

		By("deleting the PulsarCluster: every child still carries its controller owner reference (the GC precondition - envtest runs no garbage-collector controller, so actual cascade deletion is proven separately by the chainsaw e2e suite, not asserted here)")
		Expect(k8sClient.Delete(ctx, pulsarCluster)).To(Succeed())

		Expect(metav1.IsControlledBy(getOxia(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getBookKeeper(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getBroker(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getProxy(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getAutoRecovery(), pulsarCluster)).To(BeTrue())
		Expect(metav1.IsControlledBy(getFunctionsWorker(), pulsarCluster)).To(BeTrue())
	})
})
