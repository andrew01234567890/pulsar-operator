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
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

var _ = Describe("AutoRecovery Controller", func() {
	const resourceNamespace = "default"

	ctx := context.Background()

	reconcileAutoRecovery := func(name string) *clusterv1alpha1.AutoRecovery {
		key := types.NamespacedName{Name: name, Namespace: resourceNamespace}
		controllerReconciler := &AutoRecoveryReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		autoRecovery := &clusterv1alpha1.AutoRecovery{}
		Expect(k8sClient.Get(ctx, key, autoRecovery)).To(Succeed())
		return autoRecovery
	}

	Context("reconciling an embedded-mode AutoRecovery (the default)", func() {
		const resourceName = "autorecovery-embedded"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.AutoRecovery{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.AutoRecoverySpec{},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.AutoRecovery{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("manages no workload and reports Ready with an EmbeddedMode reason", func() {
			autoRecovery := reconcileAutoRecovery(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.Deployment{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("EmbeddedMode"))
			Expect(autoRecovery.Status.Replicas).To(Equal(int32(0)))
			Expect(autoRecovery.Status.ReadyReplicas).To(Equal(int32(0)))
		})
	})

	Context("reconciling a dedicated-mode AutoRecovery", func() {
		const resourceName = "autorecovery-dedicated"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
		two := int32(2)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.AutoRecovery{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.AutoRecoverySpec{
					Mode:     autoRecoveryModeDedicated,
					Replicas: &two,
				},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.AutoRecovery{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("creates a Deployment running bin/bookkeeper autorecovery with the requested replica count", func() {
			autoRecovery := reconcileAutoRecovery(resourceName)

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, key, deploy)).To(Succeed())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(2)))
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"bin/bookkeeper"}))
			Expect(deploy.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{autoRecoverySubcommand}))
			Expect(deploy.Spec.Template.Spec.Containers[0].LivenessProbe).NotTo(BeNil())
			Expect(deploy.Spec.Template.Spec.Containers[0].ReadinessProbe).NotTo(BeNil())
			Expect(deploy.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))

			By("hard anti-affinity keyed on the autorecovery selector (stateful quorum tier)")
			affinity := deploy.Spec.Template.Spec.Affinity
			Expect(affinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
			Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey).To(Equal(builder.HostnameTopologyKey))
			Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(BeEmpty())

			By("default zone topology spread constraints")
			Expect(deploy.Spec.Template.Spec.TopologySpreadConstraints).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal(builder.ZoneTopologyKey))
			Expect(deploy.Spec.Template.Spec.TopologySpreadConstraints[0].WhenUnsatisfiable).To(Equal(corev1.ScheduleAnyway))

			By("a quorum-derived PodDisruptionBudget")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, key, pdb)).To(Succeed())
			Expect(pdb.Spec.Selector.MatchLabels).To(Equal(builder.SelectorLabels(resourceName, autoRecoveryComponent)))
			Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			// floor((2-1)/2) = 0: a 2-replica majority-vote group has no
			// safe margin, so voluntary disruption is fully blocked.
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(0))

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, cm)).To(Succeed())
			Expect(cm.Data[autoRecoveryConfigFileName]).To(ContainSubstring("httpServerPort=8000"))
			// metadataServiceUri must never be hardcoded to an Oxia-specific default.
			Expect(cm.Data[autoRecoveryConfigFileName]).To(ContainSubstring("metadataServiceUri=\n"))

			// envtest runs no Deployment controller, so it never observes its
			// generation or gets Ready pods: AutoRecovery must honestly report
			// Progressing.
			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))
		})

		// Regression: dedicated AutoRecovery readiness must track the
		// Deployment rollout, not just readyReplicas, so a config/image change
		// doesn't flash Ready before the rolling restart converges.
		It("reports Ready only once the Deployment rollout has converged, and Progressing on revision skew", func() {
			reconcileAutoRecovery(resourceName)

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, key, deploy)).To(Succeed())

			By("simulating a fully converged rollout")
			setDeploymentRolloutStatus(deploy, deploy.Generation, two, two, two)
			autoRecovery := reconcileAutoRecovery(resourceName)
			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonReplicasReady))

			By("simulating revision skew: fewer updated replicas than desired")
			Expect(k8sClient.Get(ctx, key, deploy)).To(Succeed())
			setDeploymentRolloutStatus(deploy, deploy.Generation, 1, two, two)
			autoRecovery = reconcileAutoRecovery(resourceName)
			cond = apimeta.FindStatusCondition(autoRecovery.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))
		})
	})

	Context("a dedicated-mode AutoRecovery scaled to zero replicas", func() {
		const resourceName = "autorecovery-zero"
		zero := int32(0)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.AutoRecovery{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.AutoRecoverySpec{Mode: autoRecoveryModeDedicated, Replicas: &zero},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.AutoRecovery{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("reports Ready with a ScaledToZero reason", func() {
			autoRecovery := reconcileAutoRecovery(resourceName)
			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonScaledToZero))
		})
	})

	Context("switching from dedicated to embedded mode", func() {
		const resourceName = "autorecovery-mode-switch"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.AutoRecovery{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.AutoRecoverySpec{Mode: autoRecoveryModeDedicated},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.AutoRecovery{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		// Regression: a cluster that turns dedicated autorecovery back off
		// must not leave an orphaned Deployment/ConfigMap running forever.
		It("deletes the dedicated Deployment and ConfigMap once mode flips to embedded", func() {
			reconcileAutoRecovery(resourceName)
			Expect(k8sClient.Get(ctx, key, &appsv1.Deployment{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).To(Succeed())
			Expect(k8sClient.Get(ctx, key, &policyv1.PodDisruptionBudget{})).To(Succeed())

			autoRecovery := &clusterv1alpha1.AutoRecovery{}
			Expect(k8sClient.Get(ctx, key, autoRecovery)).To(Succeed())
			autoRecovery.Spec.Mode = autoRecoveryModeEmbedded
			Expect(k8sClient.Update(ctx, autoRecovery)).To(Succeed())

			autoRecovery = reconcileAutoRecovery(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.Deployment{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, key, &policyv1.PodDisruptionBudget{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("EmbeddedMode"))
		})
	})

	Context("when the AutoRecovery is not found", func() {
		It("returns without error", func() {
			controllerReconciler := &AutoRecoveryReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: testResourceNotFound, Namespace: resourceNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

func TestAutoRecoveryMode(t *testing.T) {
	tests := []struct {
		name string
		spec clusterv1alpha1.AutoRecoverySpec
		want string
	}{
		{name: "unset defaults to embedded", spec: clusterv1alpha1.AutoRecoverySpec{}, want: autoRecoveryModeEmbedded},
		{name: "explicit embedded", spec: clusterv1alpha1.AutoRecoverySpec{Mode: "embedded"}, want: autoRecoveryModeEmbedded},
		{name: "explicit dedicated", spec: clusterv1alpha1.AutoRecoverySpec{Mode: "dedicated"}, want: autoRecoveryModeDedicated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := autoRecoveryMode(tt.spec); got != tt.want {
				t.Errorf("autoRecoveryMode(%+v) = %q, want %q", tt.spec, got, tt.want)
			}
		})
	}
}

func TestAutoRecoveryReplicas(t *testing.T) {
	three := int32(3)
	tests := []struct {
		name string
		spec clusterv1alpha1.AutoRecoverySpec
		want int32
	}{
		{name: testCaseUnsetDefaultsToOne, spec: clusterv1alpha1.AutoRecoverySpec{}, want: 1},
		{name: testCaseExplicitValueWins, spec: clusterv1alpha1.AutoRecoverySpec{Replicas: &three}, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := autoRecoveryReplicas(tt.spec); got != tt.want {
				t.Errorf("autoRecoveryReplicas(%+v) = %d, want %d", tt.spec, got, tt.want)
			}
		})
	}
}

func TestAutoRecoveryMergedConfigLeavesMetadataServiceURIBlank(t *testing.T) {
	got := autoRecoveryMergedConfig(clusterv1alpha1.AutoRecoverySpec{})
	if v, ok := got[autoRecoveryKeyMetadataServiceURI]; !ok || v != "" {
		t.Errorf("metadataServiceUri = %q (present=%v), want blank (must not hardcode a store naming convention)", v, ok)
	}
}

func TestAutoRecoveryMergedConfigUserOverrideWins(t *testing.T) {
	const override = "metadata-store:oxia://oxia-coordinator:6648/bookkeeper"
	got := autoRecoveryMergedConfig(clusterv1alpha1.AutoRecoverySpec{
		Config: map[string]string{autoRecoveryKeyMetadataServiceURI: override},
	})
	if got[autoRecoveryKeyMetadataServiceURI] != override {
		t.Errorf("metadataServiceUri = %q, want user override", got[autoRecoveryKeyMetadataServiceURI])
	}
}

func TestAutoRecoveryImage(t *testing.T) {
	if got := autoRecoveryImage(""); got != autoRecoveryDefaultImage {
		t.Errorf("autoRecoveryImage(\"\") = %q, want default %q", got, autoRecoveryDefaultImage)
	}
	if got := autoRecoveryImage(testCustomImage); got != testCustomImage {
		t.Errorf("autoRecoveryImage(custom) = %q, want %q", got, testCustomImage)
	}
}

// setDeploymentRolloutStatus writes rollout fields onto a Deployment's status
// subresource, standing in for the Deployment controller that envtest does not
// run, so tests can drive each phase of a rolling update.
func setDeploymentRolloutStatus(deploy *appsv1.Deployment, observedGeneration int64, updated, ready, replicas int32) {
	deploy.Status.ObservedGeneration = observedGeneration
	deploy.Status.UpdatedReplicas = updated
	deploy.Status.ReadyReplicas = ready
	deploy.Status.Replicas = replicas
	ExpectWithOffset(1, k8sClient.Status().Update(context.Background(), deploy)).To(Succeed())
}
