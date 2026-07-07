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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// int32Ptr is shared with the other cluster-package tests
// (bookkeeper_build_test.go); boolPtr and hasOwnerRef are broker-local.
func boolPtr(v bool) *bool { return &v }

func hasOwnerRef(refs []metav1.OwnerReference, ownerUID types.UID) bool {
	for _, ref := range refs {
		if ref.UID == ownerUID && ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

var _ = Describe("Broker Controller", func() {
	ctx := context.Background()
	const resourceNamespace = "default"

	Context("When reconciling a newly-created Broker", func() {
		const resourceName = "broker-basic"
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		var reconciler *BrokerReconciler

		BeforeEach(func() {
			reconciler = &BrokerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			broker := &clusterv1alpha1.Broker{}
			err := k8sClient.Get(ctx, typeNamespacedName, broker)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &clusterv1alpha1.Broker{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
					Spec: clusterv1alpha1.BrokerSpec{
						Replicas: int32Ptr(2),
						Config: map[string]string{
							"metadataStoreUrl": testMetadataStoreURL,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("provisions a StatefulSet, headless Service, ConfigMap, and PodDisruptionBudget", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			broker := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())

			By("the StatefulSet")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(2)))
			Expect(sts.Spec.ServiceName).To(Equal(resourceName))
			Expect(sts.Spec.Selector.MatchLabels).To(Equal(builder.SelectorLabels(resourceName, brokerComponent)))
			Expect(hasOwnerRef(sts.OwnerReferences, broker.UID)).To(BeTrue())

			Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
			container := sts.Spec.Template.Spec.Containers[0]
			Expect(container.Image).To(Equal(defaultBrokerImage))
			Expect(container.Ports).To(ConsistOf(
				corev1.ContainerPort{Name: brokerPortName, ContainerPort: 6650, Protocol: corev1.ProtocolTCP},
				corev1.ContainerPort{Name: httpPortName, ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
			))
			Expect(container.ReadinessProbe.HTTPGet.Path).To(Equal("/admin/v2/brokers/health"))
			Expect(container.ReadinessProbe.HTTPGet.Port.IntValue()).To(Equal(8080))
			Expect(container.LivenessProbe.HTTPGet.Path).To(Equal("/admin/v2/brokers/health"))

			By("graceful drain wiring")
			Expect(container.Lifecycle).NotTo(BeNil())
			Expect(container.Lifecycle.PreStop).NotTo(BeNil())
			Expect(container.Lifecycle.PreStop.Exec.Command).To(ContainElement(ContainSubstring("sleep 5")))
			Expect(sts.Spec.Template.Spec.TerminationGracePeriodSeconds).NotTo(BeNil())
			Expect(*sts.Spec.Template.Spec.TerminationGracePeriodSeconds).To(Equal(int64(65))) // default 60s brokerShutdownTimeoutMs + 5s preStop sleep

			By("the config-checksum annotation")
			Expect(sts.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))
			Expect(sts.Spec.Template.Annotations[builder.ConfigChecksumAnnotation]).NotTo(BeEmpty())

			By("the headless Service")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal("None"))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())
			Expect(svc.Spec.Selector).To(Equal(builder.SelectorLabels(resourceName, brokerComponent)))
			Expect(hasOwnerRef(svc.OwnerReferences, broker.UID)).To(BeTrue())

			By("the ConfigMap")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, cm)).To(Succeed())
			Expect(cm.Data["broker.conf"]).To(ContainSubstring("loadManagerClassName=org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl"))
			Expect(cm.Data["broker.conf"]).To(ContainSubstring("metadataStoreUrl=" + testMetadataStoreURL))
			Expect(hasOwnerRef(cm.OwnerReferences, broker.UID)).To(BeTrue())

			By("the PodDisruptionBudget")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, pdb)).To(Succeed())
			Expect(pdb.Spec.Selector.MatchLabels).To(Equal(builder.SelectorLabels(resourceName, brokerComponent)))
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))
			Expect(hasOwnerRef(pdb.OwnerReferences, broker.UID)).To(BeTrue())

			By("status: not Ready until the StatefulSet reports full readiness")
			Expect(broker.Status.ObservedGeneration).To(Equal(broker.Generation))
			Expect(broker.Status.ReadyReplicas).To(Equal(int32(0)))
			cond := apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))

			By("status: still NotReady mid-rollout when ready but not all pods are updated (revision skew)")
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.Replicas = 2
			sts.Status.ReadyReplicas = 2
			sts.Status.UpdatedReplicas = 1 // one pod still on the previous revision
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())
			cond = apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))

			By("status: Ready once the rollout is fully observed, updated, and ready")
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.Replicas = 2
			sts.Status.ReadyReplicas = 2
			sts.Status.UpdatedReplicas = 2
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())
			Expect(broker.Status.ReadyReplicas).To(Equal(int32(2)))
			cond = apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonAllReady))
			Expect(cond.ObservedGeneration).To(Equal(broker.Generation))
		})
	})

	// Regression guard: a no-op reconcile must not re-issue a StatefulSet
	// update. If the checksum annotation or any other pod-template field
	// were ever computed nondeterministically, this would flap the
	// StatefulSet's ResourceVersion (and trigger a pointless rolling
	// restart) on every single reconcile.
	Context("When reconciling an already up-to-date Broker", func() {
		const resourceName = "broker-idempotent"
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		var reconciler *BrokerReconciler

		BeforeEach(func() {
			reconciler = &BrokerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			broker := &clusterv1alpha1.Broker{}
			err := k8sClient.Get(ctx, typeNamespacedName, broker)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &clusterv1alpha1.Broker{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("is idempotent across repeated reconciles", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			resourceVersionAfterFirstReconcile := sts.ResourceVersion

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			Expect(sts.ResourceVersion).To(Equal(resourceVersionAfterFirstReconcile))
		})
	})

	Context("When the PodDisruptionBudget is disabled", func() {
		const resourceName = "broker-pdb-disabled"
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		var reconciler *BrokerReconciler

		BeforeEach(func() {
			reconciler = &BrokerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			broker := &clusterv1alpha1.Broker{}
			err := k8sClient.Get(ctx, typeNamespacedName, broker)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &clusterv1alpha1.Broker{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
					Spec: clusterv1alpha1.BrokerSpec{
						Pdb: &clusterv1alpha1.PodDisruptionBudgetConfig{Enabled: boolPtr(false)},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("does not create a PodDisruptionBudget", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			pdb := &policyv1.PodDisruptionBudget{}
			err = k8sClient.Get(ctx, typeNamespacedName, pdb)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})
