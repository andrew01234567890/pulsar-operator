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

	Context("reconciling a colocated-mode FunctionsWorker (the default)", func() {
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

		It("manages no workload and reports Ready with a ColocatedMode reason", func() {
			fw := reconcileFunctionsWorker(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.StatefulSet{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, key, &corev1.Service{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, &policyv1.PodDisruptionBudget{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("ColocatedMode"))
		})
	})

	Context("reconciling a standalone-mode FunctionsWorker", func() {
		const resourceName = "functionsworker-standalone"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{Mode: functionsWorkerModeStandalone},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.FunctionsWorker{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("creates a StatefulSet defaulting package storage to FileSystemPackagesStorage", func() {
			fw := reconcileFunctionsWorker(resourceName)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
			Expect(sts.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{cmdBinPulsar}))
			Expect(sts.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{"functions-worker"}))
			Expect(sts.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))

			By("soft anti-affinity keyed on the functionsworker selector (stateless tier)")
			affinity := sts.Spec.Template.Spec.Affinity
			Expect(affinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(BeEmpty())
			Expect(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))

			By("default zone topology spread constraints")
			Expect(sts.Spec.Template.Spec.TopologySpreadConstraints).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal(builder.ZoneTopologyKey))

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, key, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal("None"))

			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, pdb)).To(Succeed())

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, cm)).To(Succeed())
			rendered := cm.Data[functionsWorkerConfigFileName]
			Expect(rendered).To(ContainSubstring("packagesManagementStorageProvider: " + fileSystemPackagesStorageProviderClass))
			Expect(rendered).To(ContainSubstring("functionsWorkerEnablePackageManagement: \"true\""))

			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))
		})

		// Regression: standalone readiness must track the StatefulSet rollout,
		// not just readyReplicas, so a config/image change doesn't flash Ready
		// before the rolling restart converges.
		It("reports Ready only once the rollout has converged, and Progressing on revision skew", func() {
			reconcileFunctionsWorker(resourceName)

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())

			By("simulating a fully converged rollout")
			setStatefulSetRolloutStatus(sts, sts.Generation, 1, 1, 1)
			fw := reconcileFunctionsWorker(resourceName)
			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonReplicasReady))

			By("simulating a stale observedGeneration during a template change")
			Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
			setStatefulSetRolloutStatus(sts, sts.Generation-1, 1, 1, 1)
			fw = reconcileFunctionsWorker(resourceName)
			cond = apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonProgressing))
		})
	})

	Context("a standalone-mode FunctionsWorker scaled to zero replicas", func() {
		const resourceName = "functionsworker-zero"
		zero := int32(0)

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{Mode: functionsWorkerModeStandalone, Replicas: &zero},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.FunctionsWorker{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		It("reports Ready with a ScaledToZero reason", func() {
			fw := reconcileFunctionsWorker(resourceName)
			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonScaledToZero))
		})
	})

	Context("switching from standalone to colocated mode", func() {
		const resourceName = "functionsworker-mode-switch"
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &clusterv1alpha1.FunctionsWorker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec:       clusterv1alpha1.FunctionsWorkerSpec{Mode: functionsWorkerModeStandalone},
			})).To(Succeed())
		})

		AfterEach(func() {
			Expect(k8sClient.Delete(ctx, &clusterv1alpha1.FunctionsWorker{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})).To(Succeed())
		})

		// Regression: a cluster that turns standalone functions-worker back
		// off must not leave orphaned resources running forever.
		It("deletes the standalone StatefulSet, Service, ConfigMap and PDB once mode flips to colocated", func() {
			reconcileFunctionsWorker(resourceName)
			Expect(k8sClient.Get(ctx, key, &appsv1.StatefulSet{})).To(Succeed())
			Expect(k8sClient.Get(ctx, key, &corev1.Service{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, &policyv1.PodDisruptionBudget{})).To(Succeed())

			fw := &clusterv1alpha1.FunctionsWorker{}
			Expect(k8sClient.Get(ctx, key, fw)).To(Succeed())
			fw.Spec.Mode = functionsWorkerModeColocated
			Expect(k8sClient.Update(ctx, fw)).To(Succeed())

			fw = reconcileFunctionsWorker(resourceName)

			Expect(k8sClient.Get(ctx, key, &appsv1.StatefulSet{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, key, &corev1.Service{})).To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: resourceNamespace}, &corev1.ConfigMap{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-pdb", Namespace: resourceNamespace}, &policyv1.PodDisruptionBudget{})).
				To(MatchError(errors.IsNotFound, "IsNotFound"))

			cond := apimeta.FindStatusCondition(fw.Status.Conditions, conditionTypeReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("ColocatedMode"))
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
		{name: "S3PackagesStorage has no built-in provider default", packageStorage: "S3PackagesStorage", wantProvider: ""},
		{name: "GCSPackagesStorage has no built-in provider default", packageStorage: "GCSPackagesStorage", wantProvider: ""},
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
	for _, key := range []string{"pulsarServiceUrl", "pulsarWebServiceUrl", configKeyConfigurationMetadataStoreURL} {
		if v, ok := got[key]; !ok || v != "" {
			t.Errorf("%s = %q (present=%v), want blank (must not invent broker/metadata-store naming)", key, v, ok)
		}
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
