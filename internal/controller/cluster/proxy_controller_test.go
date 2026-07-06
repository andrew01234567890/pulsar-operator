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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// Test-name/value literals shared by the Proxy, AutoRecovery, and
// FunctionsWorker controller test files, collected here to satisfy goconst
// rather than repeating the same literal across all three.
const (
	testCaseUnsetDefaultsToOne = "unset defaults to 1"
	testCaseExplicitValueWins  = "explicit value wins"
	testResourceNotFound       = "does-not-exist"
	testCustomImage            = "custom:tag"
)

var _ = Describe("Proxy Controller", func() {
	const resourceNamespace = "default"

	ctx := context.Background()

	reconcileProxy := func(name string) *clusterv1alpha1.Proxy {
		key := types.NamespacedName{Name: name, Namespace: resourceNamespace}
		controllerReconciler := &ProxyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		proxy := &clusterv1alpha1.Proxy{}
		Expect(k8sClient.Get(ctx, key, proxy)).To(Succeed())
		return proxy
	}

	Context("reconciling a basic Proxy", func() {
		const resourceName = "proxy-basic"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.Proxy{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.ProxySpec{},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.Proxy{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("creates a StatefulSet defaulted to one replica, mounting the rendered ConfigMap", func() {
			proxy := reconcileProxy(resourceName)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
			Expect(sts.Spec.ServiceName).To(Equal(resourceName))
			Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{cmdBinPulsar}))
			Expect(sts.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{"proxy"}))
			Expect(sts.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, cm)).To(Succeed())
			Expect(cm.Data[proxyConfigFileName]).To(ContainSubstring("servicePort=6650"))
			Expect(cm.Data[proxyConfigFileName]).To(ContainSubstring("webServicePort=8080"))
			// metadataStoreUrl must never be hardcoded to an Oxia-specific default.
			Expect(cm.Data[proxyConfigFileName]).To(ContainSubstring("metadataStoreUrl=\n"))

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, key, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal("None"))
			Expect(svc.Spec.Ports).To(HaveLen(2))

			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, pdb)).To(Succeed())

			// envtest runs no StatefulSet controller, so the StatefulSet never
			// observes its generation or gets Ready pods: the Proxy must
			// honestly report Progressing rather than lying about readiness.
			cond := apimeta.FindStatusCondition(proxy.Status.Conditions, readyConditionType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))
		})

		It("is idempotent across repeated reconciles", func() {
			reconcileProxy(resourceName)
			reconcileProxy(resourceName)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
		})
	})

	Context("reconciling a Proxy with spec.config overrides", func() {
		const resourceName = "proxy-config-override"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.Proxy{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.ProxySpec{
					Config: map[string]string{"metadataStoreUrl": "oxia://oxia-coordinator:6648/default"},
				},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.Proxy{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		// Regression: proves spec.config actually reaches the rendered
		// ConfigMap on top of the operator defaults (config.Merge precedence),
		// rather than defaults silently winning.
		It("layers spec.config on top of operator defaults in the rendered proxy.conf", func() {
			reconcileProxy(resourceName)

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, cm)).To(Succeed())
			Expect(cm.Data[proxyConfigFileName]).To(ContainSubstring("metadataStoreUrl=oxia://oxia-coordinator:6648/default"))
		})

		// Regression: the config-checksum annotation must change when
		// rendered config changes, so a config edit rolls the StatefulSet.
		It("changes the config-checksum annotation when spec.config changes", func() {
			reconcileProxy(resourceName)
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			firstChecksum := sts.Spec.Template.Annotations[builder.ConfigChecksumAnnotation]
			Expect(firstChecksum).NotTo(BeEmpty())

			proxy := &clusterv1alpha1.Proxy{}
			Expect(k8sClient.Get(ctx, key, proxy)).To(Succeed())
			proxy.Spec.Config["webServicePort"] = "9080"
			Expect(k8sClient.Update(ctx, proxy)).To(Succeed())

			reconcileProxy(resourceName)
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			Expect(sts.Spec.Template.Annotations[builder.ConfigChecksumAnnotation]).NotTo(Equal(firstChecksum))
		})
	})

	Context("reconciling a Proxy with TLS enabled", func() {
		const resourceName = "proxy-tls"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.Proxy{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.ProxySpec{
					Tls: &clusterv1alpha1.ProxyTlsConfig{Enabled: true, SecretName: "proxy-tls-secret"},
				},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.Proxy{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("wires TLS ports, config, and a cert volume mount", func() {
			reconcileProxy(resourceName)

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, cm)).To(Succeed())
			Expect(cm.Data[proxyConfigFileName]).To(ContainSubstring("servicePortTls=6651"))
			Expect(cm.Data[proxyConfigFileName]).To(ContainSubstring("webServicePortTls=8443"))

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			container := sts.Spec.Template.Spec.Containers[0]
			Expect(container.Ports).To(HaveLen(4))

			mountNames := make([]string, 0, len(container.VolumeMounts))
			for _, m := range container.VolumeMounts {
				mountNames = append(mountNames, m.Name)
			}
			Expect(mountNames).To(ContainElement(proxyTLSVolume))

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, key, svc)).To(Succeed())
			Expect(svc.Spec.Ports).To(HaveLen(4))
		})
	})

	Context("rollout-aware readiness", func() {
		const resourceName = "proxy-rollout"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
		two := int32(2)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.Proxy{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.ProxySpec{Replicas: &two},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.Proxy{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		// Regression: readiness must track the rollout, not just readyReplicas.
		// envtest has no StatefulSet controller, so the test drives the
		// StatefulSet's Status by hand to simulate each rollout phase.
		It("reports Ready only once the rollout has fully converged", func() {
			reconcileProxy(resourceName)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())

			By("simulating a fully converged rollout")
			setStatefulSetRolloutStatus(sts, sts.Generation, two, two, two)
			proxy := reconcileProxy(resourceName)
			cond := apimeta.FindStatusCondition(proxy.Status.Conditions, readyConditionType)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonReplicasReady))

			By("simulating revision skew: fewer updated replicas than desired")
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			setStatefulSetRolloutStatus(sts, sts.Generation, 1, two, two)
			proxy = reconcileProxy(resourceName)
			cond = apimeta.FindStatusCondition(proxy.Status.Conditions, readyConditionType)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))

			By("simulating a stale observedGeneration: controller has not yet acted")
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			setStatefulSetRolloutStatus(sts, sts.Generation-1, two, two, two)
			proxy = reconcileProxy(resourceName)
			cond = apimeta.FindStatusCondition(proxy.Status.Conditions, readyConditionType)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))
		})
	})

	Context("a Proxy scaled to zero replicas", func() {
		const resourceName = "proxy-zero"
		zero := int32(0)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.Proxy{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.ProxySpec{Replicas: &zero},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.Proxy{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("reports Ready with a ScaledToZero reason rather than a serving-looking ReplicasReady", func() {
			proxy := reconcileProxy(resourceName)
			cond := apimeta.FindStatusCondition(proxy.Status.Conditions, readyConditionType)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonScaledToZero))
		})
	})

	Context("when the Proxy is not found", func() {
		It("returns without error", func() {
			controllerReconciler := &ProxyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: testResourceNotFound, Namespace: resourceNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

// setStatefulSetRolloutStatus writes rollout fields onto a StatefulSet's
// status subresource, standing in for the StatefulSet controller that envtest
// does not run, so tests can drive each phase of a rolling update.
func setStatefulSetRolloutStatus(sts *appsv1.StatefulSet, observedGeneration int64, updated, ready, replicas int32) {
	sts.Status.ObservedGeneration = observedGeneration
	sts.Status.UpdatedReplicas = updated
	sts.Status.ReadyReplicas = ready
	sts.Status.Replicas = replicas
	ExpectWithOffset(1, k8sClient.Status().Update(context.Background(), sts)).To(Succeed())
}

func TestProxyReplicas(t *testing.T) {
	one := int32(1)
	five := int32(5)
	tests := []struct {
		name string
		spec clusterv1alpha1.ProxySpec
		want int32
	}{
		{name: testCaseUnsetDefaultsToOne, spec: clusterv1alpha1.ProxySpec{}, want: 1},
		{name: testCaseExplicitValueWins, spec: clusterv1alpha1.ProxySpec{Replicas: &five}, want: 5},
		{name: "explicit one", spec: clusterv1alpha1.ProxySpec{Replicas: &one}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := proxyReplicas(tt.spec); got != tt.want {
				t.Errorf("proxyReplicas(%+v) = %d, want %d", tt.spec, got, tt.want)
			}
		})
	}
}

func TestProxyTLSWired(t *testing.T) {
	tests := []struct {
		name string
		tls  *clusterv1alpha1.ProxyTlsConfig
		want bool
	}{
		{name: "nil", tls: nil, want: false},
		{name: "disabled", tls: &clusterv1alpha1.ProxyTlsConfig{Enabled: false, SecretName: "s"}, want: false},
		{name: "enabled without secret", tls: &clusterv1alpha1.ProxyTlsConfig{Enabled: true}, want: false},
		{name: "enabled with secret", tls: &clusterv1alpha1.ProxyTlsConfig{Enabled: true, SecretName: "s"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := proxyTLSWired(tt.tls); got != tt.want {
				t.Errorf("proxyTLSWired(%+v) = %v, want %v", tt.tls, got, tt.want)
			}
		})
	}
}

func TestProxyMergedConfig(t *testing.T) {
	t.Run("defaults leave metadataStoreUrl blank", func(t *testing.T) {
		got := proxyMergedConfig(clusterv1alpha1.ProxySpec{})
		if got["metadataStoreUrl"] != "" {
			t.Errorf("metadataStoreUrl = %q, want blank (must not hardcode a store naming convention)", got["metadataStoreUrl"])
		}
	})

	t.Run("TLS defaults are absent when TLS is not wired", func(t *testing.T) {
		got := proxyMergedConfig(clusterv1alpha1.ProxySpec{Tls: &clusterv1alpha1.ProxyTlsConfig{Enabled: true}})
		if _, ok := got["servicePortTls"]; ok {
			t.Errorf("servicePortTls present without a TLS secret, want absent")
		}
	})

	t.Run("TLS defaults are present when TLS is wired", func(t *testing.T) {
		got := proxyMergedConfig(clusterv1alpha1.ProxySpec{Tls: &clusterv1alpha1.ProxyTlsConfig{Enabled: true, SecretName: "s"}})
		if got["servicePortTls"] != "6651" {
			t.Errorf("servicePortTls = %q, want 6651", got["servicePortTls"])
		}
	})

	t.Run("spec.config overrides both operator and TLS defaults", func(t *testing.T) {
		got := proxyMergedConfig(clusterv1alpha1.ProxySpec{
			Tls:    &clusterv1alpha1.ProxyTlsConfig{Enabled: true, SecretName: "s"},
			Config: map[string]string{"servicePortTls": "16651", "webServicePort": "18080"},
		})
		if got["servicePortTls"] != "16651" {
			t.Errorf("servicePortTls = %q, want user override 16651", got["servicePortTls"])
		}
		if got["webServicePort"] != "18080" {
			t.Errorf("webServicePort = %q, want user override 18080", got["webServicePort"])
		}
	})
}

func TestProxyContainerAndServicePorts(t *testing.T) {
	if got := len(proxyContainerPorts(nil)); got != 2 {
		t.Errorf("proxyContainerPorts(nil) has %d ports, want 2", got)
	}
	if got := len(proxyServicePorts(nil)); got != 2 {
		t.Errorf("proxyServicePorts(nil) has %d ports, want 2", got)
	}

	tls := &clusterv1alpha1.ProxyTlsConfig{Enabled: true, SecretName: "s"}
	if got := len(proxyContainerPorts(tls)); got != 4 {
		t.Errorf("proxyContainerPorts(tls) has %d ports, want 4", got)
	}
	if got := len(proxyServicePorts(tls)); got != 4 {
		t.Errorf("proxyServicePorts(tls) has %d ports, want 4", got)
	}
}

// TestWorkloadReadyCondition covers the shared rollout-aware readiness helper
// used by the Proxy, AutoRecovery, and FunctionsWorker reconcilers. The
// revision-skew cases (updatedReplicas below desired, or observedGeneration
// lagging generation) are the regression guard: a stale, fully-ready-looking
// status during a rolling restart must not report Ready.
func TestWorkloadReadyCondition(t *testing.T) {
	tests := []struct {
		name       string
		desired    int32
		rollout    rolloutStatus
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "fully converged",
			desired:    2,
			rollout:    rolloutStatus{generation: 3, observedGeneration: 3, updatedReplicas: 2, readyReplicas: 2},
			wantStatus: metav1.ConditionTrue,
			wantReason: reasonReplicasReady,
		},
		{
			name:       "revision skew: fewer updated than desired",
			desired:    2,
			rollout:    rolloutStatus{generation: 4, observedGeneration: 4, updatedReplicas: 1, readyReplicas: 2},
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			name:       "stale observedGeneration",
			desired:    2,
			rollout:    rolloutStatus{generation: 5, observedGeneration: 4, updatedReplicas: 2, readyReplicas: 2},
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			name:       "not enough ready",
			desired:    3,
			rollout:    rolloutStatus{generation: 1, observedGeneration: 1, updatedReplicas: 3, readyReplicas: 1},
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			name:       "freshly created, nothing observed yet",
			desired:    1,
			rollout:    rolloutStatus{generation: 1, observedGeneration: 0, updatedReplicas: 0, readyReplicas: 0},
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			name:       "scaled to zero",
			desired:    0,
			rollout:    rolloutStatus{generation: 2, observedGeneration: 2, updatedReplicas: 0, readyReplicas: 0},
			wantStatus: metav1.ConditionTrue,
			wantReason: reasonScaledToZero,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := workloadReadyCondition(7, tt.desired, tt.rollout, "proxy")
			if cond.Status != tt.wantStatus || cond.Reason != tt.wantReason {
				t.Errorf("workloadReadyCondition(...) = %s/%s, want %s/%s", cond.Status, cond.Reason, tt.wantStatus, tt.wantReason)
			}
			if cond.ObservedGeneration != 7 {
				t.Errorf("ObservedGeneration = %d, want 7", cond.ObservedGeneration)
			}
		})
	}
}

func TestStatefulSetAndDeploymentRollout(t *testing.T) {
	sts := &appsv1.StatefulSet{}
	sts.Generation = 3
	sts.Status.ObservedGeneration = 2
	sts.Status.UpdatedReplicas = 1
	sts.Status.ReadyReplicas = 4
	if got := statefulSetRollout(sts); got != (rolloutStatus{generation: 3, observedGeneration: 2, updatedReplicas: 1, readyReplicas: 4}) {
		t.Errorf("statefulSetRollout() = %+v", got)
	}

	deploy := &appsv1.Deployment{}
	deploy.Generation = 9
	deploy.Status.ObservedGeneration = 9
	deploy.Status.UpdatedReplicas = 2
	deploy.Status.ReadyReplicas = 2
	if got := deploymentRollout(deploy); got != (rolloutStatus{generation: 9, observedGeneration: 9, updatedReplicas: 2, readyReplicas: 2}) {
		t.Errorf("deploymentRollout() = %+v", got)
	}
}

func TestProxyImage(t *testing.T) {
	if got := proxyImage(""); got != proxyDefaultImage {
		t.Errorf("proxyImage(\"\") = %q, want default %q", got, proxyDefaultImage)
	}
	if got := proxyImage(testCustomImage); got != testCustomImage {
		t.Errorf("proxyImage(custom) = %q, want %q", got, testCustomImage)
	}
}
