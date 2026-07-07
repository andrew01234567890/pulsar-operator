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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// This spec proves Fix A end to end: a colocated FunctionsWorker must make
// the BROKER actually run the embedded worker with filesystem package
// storage - see pulsarcluster_functionsworker.go's package doc comment for
// the verified-against-Pulsar-source background. Complements the pure
// wireFunctionsWorkerColocated unit tests in
// pulsarcluster_functionsworker_test.go.
var _ = Describe("PulsarCluster colocated FunctionsWorker wiring", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		recorder    *events.FakeRecorder
		clusterName string
		req         reconcile.Request
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-fw-colocated-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		recorder = events.NewFakeRecorder(100)
		reconciler = &PulsarClusterReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}
		clusterName = "fw-colocated-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	brokerKey := func() types.NamespacedName {
		return types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}
	}
	pvcKey := func() types.NamespacedName {
		return types.NamespacedName{Name: clusterName + "-functions-package-storage", Namespace: namespace.Name}
	}
	getBroker := func() *clusterv1alpha1.Broker {
		b := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, brokerKey(), b)).To(Succeed())
		return b
	}

	It("wires functionsWorkerEnabled + FileSystemPackagesStorage onto the broker and provisions the shared PVC", func() {
		cluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker:          &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
				FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := getBroker()
		Expect(broker.Spec.Config).To(HaveKeyWithValue(confKeyFunctionsWorkerEnabled, configValTrue))
		Expect(broker.Spec.Config).To(HaveKeyWithValue(confKeyEnablePackagesManagement, configValTrue))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("functionsWorkerEnablePackageManagement", configValTrue))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("packagesManagementStorageProvider", fileSystemPackagesStorageProviderClass))
		Expect(broker.Spec.Config).To(HaveKeyWithValue(confKeyStoragePath, functionsWorkerPackageStorageMountPath))

		// broker.Spec.FunctionsWorkerConfig only carries the umbrella's own
		// override (pulsarFunctionsCluster) - functionsWorkerDefaultConfig's
		// baseline (including pulsarFunctionsNamespace) is layered in later by
		// BrokerReconciler itself when rendering functions_worker.yml (see
		// the rendered-content assertion below), mirroring how
		// Broker.Spec.Config only ever carries overrides on top of
		// defaultBrokerConfig.
		Expect(broker.Spec.FunctionsWorkerConfig).To(HaveKeyWithValue(cfgKeyPulsarFunctionsCluster, clusterName))

		var packageVol *corev1.Volume
		for i := range broker.Spec.Volumes {
			if broker.Spec.Volumes[i].Name == functionsWorkerPackageStorageVolumeName {
				packageVol = &broker.Spec.Volumes[i]
			}
		}
		Expect(packageVol).NotTo(BeNil())
		Expect(packageVol.PersistentVolumeClaim).NotTo(BeNil())
		Expect(packageVol.PersistentVolumeClaim.ClaimName).To(Equal(pvcKey().Name))

		var packageMount *corev1.VolumeMount
		for i := range broker.Spec.VolumeMounts {
			if broker.Spec.VolumeMounts[i].Name == functionsWorkerPackageStorageVolumeName {
				packageMount = &broker.Spec.VolumeMounts[i]
			}
		}
		Expect(packageMount).NotTo(BeNil())
		Expect(packageMount.MountPath).To(Equal(functionsWorkerPackageStorageMountPath))

		By("the shared package-storage PVC is created, owned by the PulsarCluster, ReadWriteOnce by default")
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey(), pvc)).To(Succeed())
		Expect(metav1.IsControlledBy(pvc, cluster)).To(BeTrue())
		Expect(pvc.Spec.AccessModes).To(ConsistOf(corev1.ReadWriteOnce))
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal(defaultFunctionsWorkerPackageStorageSize.String()))

		By("chaining the Broker's own reconciler: functions_worker.yml is rendered, mounted, and checksummed alongside broker.conf")
		brokerReconciler := &BrokerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err = brokerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: brokerKey()})
		Expect(err).NotTo(HaveOccurred())

		fwConfigMap := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: functionsWorkerBrokerConfigMapName(brokerKey().Name), Namespace: namespace.Name}, fwConfigMap)).To(Succeed())
		rendered := fwConfigMap.Data[functionsWorkerConfigFileName]
		Expect(rendered).To(ContainSubstring("pulsarFunctionsNamespace: public/functions"))
		Expect(rendered).To(ContainSubstring("pulsarFunctionsCluster: " + clusterName))

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, brokerKey(), sts)).To(Succeed())
		podSpec := sts.Spec.Template.Spec
		Expect(podSpec.Containers).To(HaveLen(1))

		volumeNames := func(vols []corev1.Volume) []string {
			names := make([]string, len(vols))
			for i, v := range vols {
				names[i] = v.Name
			}
			return names
		}
		Expect(volumeNames(podSpec.Volumes)).To(ContainElements(configVolumeName, functionsWorkerConfigVolumeName, functionsWorkerPackageStorageVolumeName))

		mountNames := func(mounts []corev1.VolumeMount) []string {
			names := make([]string, len(mounts))
			for i, m := range mounts {
				names[i] = m.Name
			}
			return names
		}
		Expect(mountNames(podSpec.Containers[0].VolumeMounts)).To(ContainElements(configVolumeName, functionsWorkerConfigVolumeName, functionsWorkerPackageStorageVolumeName))
		Expect(sts.Spec.Template.Annotations).To(HaveKey(builder.ConfigChecksumAnnotation))
	})

	It("never edits a pre-existing package-storage PVC (e.g. a pre-provisioned ReadWriteMany claim)", func() {
		customStorageClass := "my-rwx-storage-class"
		Expect(k8sClient.Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcKey().Name, Namespace: namespace.Name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				StorageClassName: &customStorageClass,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("42Gi")},
				},
			},
		})).To(Succeed())

		cluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker:          &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3))},
				FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey(), pvc)).To(Succeed())
		Expect(pvc.Spec.AccessModes).To(ConsistOf(corev1.ReadWriteMany))
		Expect(pvc.Spec.StorageClassName).To(HaveValue(Equal(customStorageClass)))
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("42Gi"))
		Expect(metav1.IsControlledBy(pvc, cluster)).To(BeFalse(), "a pre-existing PVC must not be adopted/mutated")
	})

	// Regression: with more than one broker replica, the operator's default
	// ReadWriteOnce PVC can only ever be mounted by pods on the same node as
	// the bound volume - the honest "surface it" half of Fix A's multi-broker
	// caveat (see wireFunctionsWorkerColocated).
	It("emits a Warning event when more than one broker replica shares the default ReadWriteOnce PVC", func() {
		cluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker:          &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3))},
				FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Eventually(recorder.Events).Should(Receive(SatisfyAll(
			ContainSubstring("Warning"),
			ContainSubstring("FunctionsPackageStorageSingleWriter"),
			ContainSubstring("ReadWriteMany"),
		)))
	})

	It("stops wiring functionsWorkerEnabled and removes the volumes once FunctionsWorker is removed", func() {
		cluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker:          &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
				FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Spec.Config).To(HaveKeyWithValue(confKeyFunctionsWorkerEnabled, configValTrue))

		By("settling Oxia so the next reconcile's broker update is not deferred")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("removing FunctionsWorker from the (freshly re-fetched) cluster spec")
		Expect(k8sClient.Get(ctx, req.NamespacedName, cluster)).To(Succeed())
		cluster.Spec.FunctionsWorker = nil
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := getBroker()
		Expect(broker.Spec.Config).NotTo(HaveKey(confKeyFunctionsWorkerEnabled))
		Expect(broker.Spec.FunctionsWorkerConfig).To(BeNil())
		for _, v := range broker.Spec.Volumes {
			Expect(v.Name).NotTo(Equal(functionsWorkerPackageStorageVolumeName))
		}

		By("the PVC itself is left in place (data preservation) even though the wiring is gone")
		Expect(k8sClient.Get(ctx, pvcKey(), &corev1.PersistentVolumeClaim{})).To(Succeed())

		By("chaining the Broker's own reconciler deletes the now-unused functions_worker.yml ConfigMap")
		brokerReconciler := &BrokerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err = brokerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: brokerKey()})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: functionsWorkerBrokerConfigMapName(brokerKey().Name), Namespace: namespace.Name}, &corev1.ConfigMap{})).
			To(MatchError(apierrors.IsNotFound, "IsNotFound"))
	})
})
