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

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}

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

		By("marking every configured child Ready")
		broker.Status.Conditions = []metav1.Condition{readyCondition("AllPodsReady", "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

		bookKeeper.Status.Conditions = []metav1.Condition{readyCondition("AllPodsReady", "all bookie pods ready")}
		Expect(k8sClient.Status().Update(ctx, bookKeeper)).To(Succeed())

		oxia.Status.Conditions = []metav1.Condition{readyCondition("AllPodsReady", "all oxia pods ready")}
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

	It("does not create any child CRs and reports NoComponentsConfigured when no sub-specs are set", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Reason).To(Equal(reasonNoComponentsConfigured))
	})
})

// readyCondition builds a Ready=True metav1.Condition for stamping onto a
// child's status in tests.
func readyCondition(reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
}
