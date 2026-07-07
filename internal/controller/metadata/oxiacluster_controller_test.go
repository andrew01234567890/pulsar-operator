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

package metadata

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

var _ = Describe("OxiaCluster Controller", func() {
	Context("When reconciling a resource", func() {
		var (
			resourceName       string
			resourceNamespace  string
			typeNamespacedName types.NamespacedName
			reconciler         *OxiaClusterReconciler
		)

		BeforeEach(func() {
			resourceName = "oxia-it"
			resourceNamespace = testEnvtestNamespace
			typeNamespacedName = types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
			reconciler = &OxiaClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			oxia := &metadatav1alpha1.OxiaCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: resourceNamespace,
				},
				Spec: metadatav1alpha1.OxiaClusterSpec{
					Coordinator: &metadatav1alpha1.OxiaCoordinatorSpec{
						Replicas: ptr(int32(2)),
					},
					Server: &metadatav1alpha1.OxiaServerSpec{
						Replicas:    ptr(int32(3)),
						StorageSize: resource.MustParse("8Gi"),
					},
					Namespaces: []metadatav1alpha1.OxiaNamespaceSpec{
						{Name: testNamespaceDefault},
						{Name: testNamespaceBroker},
						{Name: "bookkeeper"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, oxia)).To(Succeed())
		})

		AfterEach(func() {
			oxia := &metadatav1alpha1.OxiaCluster{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, oxia)).To(Succeed())
			Expect(k8sClient.Delete(ctx, oxia)).To(Succeed())
		})

		It("creates the coordinator Deployment, ConfigMap, RBAC and Service", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, deploy)).To(Succeed())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(2)))
			Expect(deploy.OwnerReferences).To(HaveLen(1))
			Expect(deploy.OwnerReferences[0].Name).To(Equal(resourceName))
			Expect(deploy.Spec.Template.Spec.ServiceAccountName).To(Equal(coordinatorName(resourceName)))
			Expect(deploy.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))

			By("soft anti-affinity keyed on the coordinator selector (stateless tier)")
			affinity := deploy.Spec.Template.Spec.Affinity
			Expect(affinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(BeEmpty())
			Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))

			By("default zone topology spread constraints")
			Expect(deploy.Spec.Template.Spec.TopologySpreadConstraints).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal(builder.ZoneTopologyKey))

			By("a flat-1 PodDisruptionBudget (stateless: losing a coordinator only costs a leader-election handover)")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, pdb)).To(Succeed())
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))
			Expect(pdb.OwnerReferences).To(HaveLen(1))

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, cm)).To(Succeed())
			Expect(cm.OwnerReferences).To(HaveLen(1))
			var parsed coordinatorConfig
			Expect(yaml.Unmarshal([]byte(cm.Data[coordinatorConfigFileName]), &parsed)).To(Succeed())
			Expect(parsed.Servers).To(HaveLen(3))
			Expect(parsed.Namespaces).To(HaveLen(3))

			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, sa)).To(Succeed())

			role := &rbacv1.Role{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, role)).To(Succeed())
			Expect(role.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     coordinatorConfigMapVerbs,
			}))

			rb := &rbacv1.RoleBinding{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, rb)).To(Succeed())
			Expect(rb.RoleRef.Name).To(Equal(coordinatorName(resourceName)))
			Expect(rb.Subjects).To(ContainElement(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      coordinatorName(resourceName),
				Namespace: resourceNamespace,
			}))

			statusCM := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorStatusConfigMapName(resourceName), Namespace: resourceNamespace}, statusCM)).To(Succeed())

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, svc)).To(Succeed())
			Expect(portNames(svc.Spec.Ports)).To(ConsistOf(portNameInternal, portNameMetrics))
		})

		It("creates the server StatefulSet and headless/public Services", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: serverName(resourceName), Namespace: resourceNamespace}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.ServiceName).To(Equal(serverHeadlessServiceName(resourceName)))
			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))
			Expect(sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String()).To(Equal("8Gi"))
			Expect(sts.OwnerReferences).To(HaveLen(1))
			Expect(sts.OwnerReferences[0].Name).To(Equal(resourceName))

			By("hard anti-affinity keyed on the server selector (stateful quorum tier)")
			affinity := sts.Spec.Template.Spec.Affinity
			Expect(affinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
			Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(BeEmpty())

			By("default zone topology spread constraints")
			Expect(sts.Spec.Template.Spec.TopologySpreadConstraints).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal(builder.ZoneTopologyKey))

			By("a quorum-derived PodDisruptionBudget: floor((3-1)/2) = 1")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: serverName(resourceName), Namespace: resourceNamespace}, pdb)).To(Succeed())
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))
			Expect(pdb.OwnerReferences).To(HaveLen(1))

			headless := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: serverHeadlessServiceName(resourceName), Namespace: resourceNamespace}, headless)).To(Succeed())
			Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(headless.Spec.PublishNotReadyAddresses).To(BeTrue())
			Expect(portNames(headless.Spec.Ports)).To(ConsistOf(portNamePublic, portNameInternal, portNameMetrics))

			public := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: publicServiceName(resourceName), Namespace: resourceNamespace}, public)).To(Succeed())
			Expect(public.Spec.ClusterIP).NotTo(Equal(corev1.ClusterIPNone))
			Expect(portNames(public.Spec.Ports)).To(ConsistOf(portNamePublic, portNameInternal, portNameMetrics))
		})

		It("regenerates the coordinator ConfigMap and rolls the coordinator when server replicas change", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			beforeCM := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, beforeCM)).To(Succeed())
			beforeDeploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, beforeDeploy)).To(Succeed())
			beforeChecksum := beforeDeploy.Spec.Template.Annotations[builder.ConfigChecksumAnnotation]

			oxia := &metadatav1alpha1.OxiaCluster{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, oxia)).To(Succeed())
			oxia.Spec.Server.Replicas = ptr(int32(5))
			Expect(k8sClient.Update(ctx, oxia)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			afterCM := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, afterCM)).To(Succeed())
			Expect(afterCM.Data[coordinatorConfigFileName]).NotTo(Equal(beforeCM.Data[coordinatorConfigFileName]))

			var parsed coordinatorConfig
			Expect(yaml.Unmarshal([]byte(afterCM.Data[coordinatorConfigFileName]), &parsed)).To(Succeed())
			Expect(parsed.Servers).To(HaveLen(5))

			afterDeploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coordinatorName(resourceName), Namespace: resourceNamespace}, afterDeploy)).To(Succeed())
			afterChecksum := afterDeploy.Spec.Template.Annotations[builder.ConfigChecksumAnnotation]
			Expect(afterChecksum).NotTo(Equal(beforeChecksum))
		})

		It("does not error when the coordinator/server StatefulSet do not exist yet during status reconciliation", func() {
			// Regression: reconcileStatus must tolerate NotFound on both
			// child objects (e.g. on a fresh Reconcile before either has
			// been created), rather than erroring the whole Reconcile.
			empty := &metadatav1alpha1.OxiaCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "no-children-yet", Namespace: resourceNamespace},
			}
			Expect(k8sClient.Create(ctx, empty)).To(Succeed())
			defer func() {
				Expect(k8sClient.Delete(ctx, empty)).To(Succeed())
			}()

			err := reconciler.reconcileStatus(ctx, empty)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("when the OxiaCluster no longer exists", func() {
		It("returns no error", func() {
			reconciler := &OxiaClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "does-not-exist", Namespace: testEnvtestNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

func portNames(ports []corev1.ServicePort) []string {
	names := make([]string, 0, len(ports))
	for _, p := range ports {
		names = append(names, p.Name)
	}
	return names
}
