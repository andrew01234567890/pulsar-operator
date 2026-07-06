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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

var _ = Describe("PulsarCluster Controller", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		clusterName string
		req         reconcile.Request
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-test-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		reconciler = &PulsarClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		clusterName = "test-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	It("stamps out configured child CRs with owner references and propagated specs, and aggregates their readiness", func() {
		const clusterImage = "cluster/pulsar:5.0.0"
		const storageClass = "fast-ssd"

		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Image:  clusterImage,
				Global: &clusterv1alpha1.GlobalSpec{StorageClassName: ptr(storageClass)},
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3))},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Volumes: &clusterv1alpha1.BookKeeperVolumes{
						Journal: &clusterv1alpha1.VolumeSpec{},
					},
				},
				Oxia: &metadatav1alpha1.OxiaClusterSpec{
					Server: &metadatav1alpha1.OxiaServerSpec{},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling the PulsarCluster")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("creating the Broker child with an owner reference and the propagated image")
		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		Expect(broker.Spec.Image).To(Equal(clusterImage))
		Expect(broker.Spec.Replicas).To(HaveValue(Equal(int32(3))))
		Expect(metav1.IsControlledBy(broker, pulsarCluster)).To(BeTrue())

		By("creating the BookKeeper child with the propagated global storage class")
		bookKeeper := &clusterv1alpha1.BookKeeper{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-bookkeeper", Namespace: namespace.Name}, bookKeeper)).To(Succeed())
		Expect(bookKeeper.Spec.Volumes.Journal.StorageClassName).To(HaveValue(Equal(storageClass)))
		Expect(metav1.IsControlledBy(bookKeeper, pulsarCluster)).To(BeTrue())

		By("creating the OxiaCluster child with the propagated image and storage class")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		Expect(oxia.Spec.Server.Image).To(Equal(clusterImage))
		Expect(oxia.Spec.Server.StorageClassName).To(HaveValue(Equal(storageClass)))
		Expect(metav1.IsControlledBy(oxia, pulsarCluster)).To(BeTrue())

		By("not creating children whose sub-spec was left unconfigured")
		proxy := &clusterv1alpha1.Proxy{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-proxy", Namespace: namespace.Name}, proxy)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("reporting Ready=False because no child has reported readiness yet")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Reason).To(Equal(reasonComponentNotReady))
		Expect(pulsarCluster.Status.BrokerPhase).To(Equal(phaseNotReady))
		Expect(pulsarCluster.Status.BookKeeperPhase).To(Equal(phaseNotReady))
		Expect(pulsarCluster.Status.OxiaPhase).To(Equal(phaseNotReady))
		Expect(pulsarCluster.Status.ProxyPhase).To(BeEmpty())
		Expect(pulsarCluster.Status.ObservedGeneration).To(Equal(pulsarCluster.Generation))

		By("marking every configured child Ready for its current generation")
		broker.Status.Conditions = []metav1.Condition{readyConditionForGeneration(broker.Generation, "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())
		bookKeeper.Status.Conditions = []metav1.Condition{readyConditionForGeneration(bookKeeper.Generation, "all bookie pods ready")}
		Expect(k8sClient.Status().Update(ctx, bookKeeper)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("re-reconciling and observing the aggregated Ready=True condition")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond = apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		Expect(readyCond.Reason).To(Equal(reasonAllComponentsReady))
		Expect(pulsarCluster.Status.BrokerPhase).To(Equal(phaseReady))
		Expect(pulsarCluster.Status.BookKeeperPhase).To(Equal(phaseReady))
		Expect(pulsarCluster.Status.OxiaPhase).To(Equal(phaseReady))
	})

	It("always provisions a default OxiaCluster even when no sub-specs are configured", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("creating the mandatory OxiaCluster child")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		Expect(metav1.IsControlledBy(oxia, pulsarCluster)).To(BeTrue())

		By("not creating optional Pulsar components")
		broker := &clusterv1alpha1.Broker{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("reporting Ready=False on the still-unready Oxia store, not NoComponentsConfigured")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Reason).To(Equal(reasonComponentNotReady))
		Expect(pulsarCluster.Status.OxiaPhase).To(Equal(phaseNotReady))
	})

	It("prunes a child CR when its sub-spec is removed from the PulsarCluster", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling with the broker configured")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())

		By("removing the broker sub-spec")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.Broker = nil
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		By("reconciling again")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the now-undesired Broker child")
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("clearing the broker phase from status")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(pulsarCluster.Status.BrokerPhase).To(BeEmpty())
	})

	It("does not aggregate a stale child Ready condition after the child's spec changes", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling and marking both broker and the mandatory oxia Ready")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		broker.Status.Conditions = []metav1.Condition{readyConditionForGeneration(broker.Generation, "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(apimeta.IsStatusConditionTrue(pulsarCluster.Status.Conditions, conditionTypeReady)).To(BeTrue())

		By("bumping the broker's desired spec so its child generation advances past its Ready status")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.Broker.Replicas = ptr(int32(5))
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("reporting the broker as progressing rather than trusting its stale Ready condition")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Message).To(ContainSubstring(reasonComponentProgressing))
		Expect(pulsarCluster.Status.BrokerPhase).To(Equal(phaseNotReady))
	})
})

// readyConditionForGeneration builds a Ready=True metav1.Condition observed
// against the given generation, for stamping onto a child's status in tests.
func readyConditionForGeneration(generation int64, message string) metav1.Condition {
	return metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             testReasonAllPodsReady,
		Message:            message,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
	}
}
