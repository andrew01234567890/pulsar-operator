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

			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, readyConditionType)
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
			Expect(deploy.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{"autorecovery"}))
			Expect(deploy.Spec.Template.Spec.Containers[0].LivenessProbe).NotTo(BeNil())
			Expect(deploy.Spec.Template.Spec.Containers[0].ReadinessProbe).NotTo(BeNil())
			Expect(deploy.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, cm)).To(Succeed())
			Expect(cm.Data[autoRecoveryConfigFileName]).To(ContainSubstring("httpServerPort=8000"))
			// metadataServiceUri must never be hardcoded to an Oxia-specific default.
			Expect(cm.Data[autoRecoveryConfigFileName]).To(ContainSubstring("metadataServiceUri=\n"))

			// envtest runs no Deployment controller, so it never actually gets
			// Ready pods: AutoRecovery must honestly report NotReady.
			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, readyConditionType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonReplicasNotReady))
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

			autoRecovery := &clusterv1alpha1.AutoRecovery{}
			Expect(k8sClient.Get(ctx, key, autoRecovery)).To(Succeed())
			autoRecovery.Spec.Mode = autoRecoveryModeEmbedded
			Expect(k8sClient.Update(ctx, autoRecovery)).To(Succeed())

			autoRecovery = reconcileAutoRecovery(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.Deployment{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(autoRecovery.Status.Conditions, readyConditionType)
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
	if v, ok := got["metadataServiceUri"]; !ok || v != "" {
		t.Errorf("metadataServiceUri = %q (present=%v), want blank (must not hardcode a store naming convention)", v, ok)
	}
}

func TestAutoRecoveryMergedConfigUserOverrideWins(t *testing.T) {
	got := autoRecoveryMergedConfig(clusterv1alpha1.AutoRecoverySpec{
		Config: map[string]string{"metadataServiceUri": "metadata-store:oxia://oxia-coordinator:6648/bookkeeper"},
	})
	if got["metadataServiceUri"] != "metadata-store:oxia://oxia-coordinator:6648/bookkeeper" {
		t.Errorf("metadataServiceUri = %q, want user override", got["metadataServiceUri"])
	}
}

func TestAutoRecoveryReadyCondition(t *testing.T) {
	t.Run("ready equals desired", func(t *testing.T) {
		cond := autoRecoveryReadyCondition(4, 1, 1)
		if cond.Status != metav1.ConditionTrue || cond.Reason != reasonReplicasReady {
			t.Errorf("autoRecoveryReadyCondition(4, 1, 1) = %+v, want ConditionTrue/ReplicasReady", cond)
		}
	})

	t.Run("ready below desired", func(t *testing.T) {
		cond := autoRecoveryReadyCondition(1, 2, 0)
		if cond.Status != metav1.ConditionFalse || cond.Reason != reasonReplicasNotReady {
			t.Errorf("autoRecoveryReadyCondition(1, 2, 0) = %+v, want ConditionFalse/ReplicasNotReady", cond)
		}
	})
}

func TestAutoRecoveryImage(t *testing.T) {
	if got := autoRecoveryImage(""); got != autoRecoveryDefaultImage {
		t.Errorf("autoRecoveryImage(\"\") = %q, want default %q", got, autoRecoveryDefaultImage)
	}
	if got := autoRecoveryImage(testCustomImage); got != testCustomImage {
		t.Errorf("autoRecoveryImage(custom) = %q, want %q", got, testCustomImage)
	}
}
