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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

var _ = Describe("BookKeeper Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		bookkeeper := &clusterv1alpha1.BookKeeper{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind BookKeeper")
			err := k8sClient.Get(ctx, typeNamespacedName, bookkeeper)
			if err != nil && errors.IsNotFound(err) {
				replicas := int32(3)
				resource := &clusterv1alpha1.BookKeeper{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: clusterv1alpha1.BookKeeperSpec{
						Replicas: &replicas,
						Config: map[string]string{
							"metadataServiceUri": "metadata-store:oxia://oxia-server:6648/bookkeeper",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &clusterv1alpha1.BookKeeper{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance BookKeeper")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should reconcile a bookie StatefulSet, headless Service, ConfigMap, and PodDisruptionBudget", func() {
			controllerReconciler := &BookKeeperReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("Reconciling the created resource")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, bookkeeper)).To(Succeed())
			owner := bookkeeper

			By("creating a StatefulSet with three disk-role volumeClaimTemplates")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())

			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(3))
			claimNames := make([]string, 0, len(sts.Spec.VolumeClaimTemplates))
			for _, vct := range sts.Spec.VolumeClaimTemplates {
				claimNames = append(claimNames, vct.Name)
			}
			Expect(claimNames).To(ConsistOf(volumeNameJournal, volumeNameLedgers, volumeNameIndex))

			Expect(sts.Spec.ServiceName).To(Equal(resourceName))
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.ParallelPodManagement))

			By("setting a config-checksum annotation on the pod template")
			checksum, ok := sts.Spec.Template.Annotations[builder.ConfigChecksumAnnotation]
			Expect(ok).To(BeTrue())
			Expect(checksum).NotTo(BeEmpty())

			By("owning the StatefulSet so it is garbage-collected with the BookKeeper")
			Expect(ownerRefNames(sts.OwnerReferences)).To(ContainElement(owner.Name))

			By("creating a headless Service for bookie peer discovery")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal("None"))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())
			Expect(ownerRefNames(svc.OwnerReferences)).To(ContainElement(owner.Name))

			By("creating a ConfigMap rendering bookkeeper.conf from operator defaults and spec.config")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, cm)).To(Succeed())
			Expect(cm.Data[configMapKey]).To(ContainSubstring("metadataServiceUri=metadata-store:oxia://oxia-server:6648/bookkeeper"))
			Expect(cm.Data[configMapKey]).To(ContainSubstring("journalDirectories=" + defaultJournalDir))
			Expect(ownerRefNames(cm.OwnerReferences)).To(ContainElement(owner.Name))

			By("creating a PodDisruptionBudget scoped to the bookie selector")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, pdb)).To(Succeed())
			Expect(pdb.Spec.Selector.MatchLabels).To(Equal(builder.SelectorLabels(resourceName, bookkeeperComponent)))
			Expect(ownerRefNames(pdb.OwnerReferences)).To(ContainElement(owner.Name))
		})

		It("should report Ready once the StatefulSet's ready replicas match desired replicas", func() {
			controllerReconciler := &BookKeeperReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("the StatefulSet not yet reporting ready pods")
			Expect(k8sClient.Get(ctx, typeNamespacedName, bookkeeper)).To(Succeed())
			Expect(bookkeeper.Status.ReadyReplicas).To(Equal(int32(0)))
			readyCond := findCondition(bookkeeper.Status.Conditions, conditionTypeReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))

			By("simulating the StatefulSet becoming fully ready")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			sts.Status.Replicas = 3
			sts.Status.ReadyReplicas = 3
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, bookkeeper)).To(Succeed())
			Expect(bookkeeper.Status.ReadyReplicas).To(Equal(int32(3)))
			Expect(bookkeeper.Status.ObservedGeneration).To(Equal(bookkeeper.Generation))
			readyCond = findCondition(bookkeeper.Status.Conditions, conditionTypeReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCond.Reason).To(Equal(reasonAllReady))
		})

		// Regression: an earlier revision of this reconciler re-set
		// StatefulSet.Spec.Selector/ServiceName/PodManagementPolicy/
		// VolumeClaimTemplates on every reconcile. Those fields are immutable
		// once the StatefulSet exists, so a real API server rejects any
		// update that even re-sends the same value verbatim if the
		// surrounding object diff logic doesn't special-case them -
		// reconciling twice in a row must stay a no-op error-wise.
		It("should reconcile idempotently without erroring on immutable StatefulSet fields", func() {
			controllerReconciler := &BookKeeperReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sts)).To(Succeed())
			Expect(sts.Spec.ServiceName).To(Equal(resourceName))
			Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.ParallelPodManagement))
			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(3))
		})
	})
})

func ownerRefNames(refs []metav1.OwnerReference) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Name)
	}
	return names
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
