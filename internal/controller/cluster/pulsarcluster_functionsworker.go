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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// This file wires a colocated-mode FunctionsWorker onto the broker it
// actually runs inside of. Background (verified against
// /tmp/pulsar-refs/pulsar): Pulsar's embedded (colocated) functions worker is
// enabled by broker.conf's functionsWorkerEnabled key
// (PulsarBrokerStarter.call: `starterArguments.runFunctionsWorker ||
// brokerConfig.isFunctionsWorkerEnabled()`), which then unconditionally loads
// conf/functions_worker.yml from the broker's own conf directory
// (PulsarService.initializeWorkerConfigFromBrokerConfig) - it is NOT
// sufficient to just flip broker.conf keys; a functions_worker.yml must also
// be mounted on the broker pod, or the broker fails to start once
// functionsWorkerEnabled is true. Package storage defaults to the
// ZooKeeper-only DistributedLog path, which colocated mode skips
// initializing against Oxia (isMetadataStoreBackedByZookeeper() is false) but
// never replaces - so without also enabling the Packages Management Service
// with FileSystemPackagesStorage, functions/connectors still fail the first
// time a package actually needs to be stored. See
// pulsar-helm-chart's charts/pulsar/templates/broker-configmap.yaml and
// broker-package-storage.yaml for the parity reference this mirrors.
const (
	// confKeyFunctionsWorkerEnabled is broker.conf's ServiceConfiguration
	// field that turns the embedded functions worker on.
	confKeyFunctionsWorkerEnabled = "functionsWorkerEnabled"

	// confKeyEnablePackagesManagement is broker.conf's ServiceConfiguration
	// field that turns on the broker's Packages Management Service.
	confKeyEnablePackagesManagement = "enablePackagesManagement"

	// confKeyStoragePath is FileSystemPackagesStorage's storage-directory
	// key (see FileSystemPackagesStorage.java's STORAGE_PATH constant). It is
	// not a first-class ServiceConfiguration field - PulsarService reads it
	// out of ServiceConfiguration's generic properties passthrough map
	// (config.getProperties()), so it needs no special prefix in broker.conf
	// (the "PULSAR_PREFIX_"/"PF_" env-var conventions pulsar-helm-chart uses
	// are specific to the upstream Docker image's apply-config-from-env
	// entrypoint script, which this operator's "bin/pulsar broker" container
	// command bypasses entirely - broker.conf is always the final, literal,
	// fully-rendered file here).
	confKeyStoragePath = "STORAGE_PATH"

	// functionsWorkerPackageStorageMountPath is where the shared PVC is
	// mounted on every broker pod, matching pulsar-helm-chart's own
	// broker.packageManagement.fileSystemStorage.storagePath default.
	functionsWorkerPackageStorageMountPath = "/pulsar/packages-storage"

	functionsWorkerPackageStorageVolumeName = "functions-package-storage"

	// functionsWorkerPackageStorageNameSuffix names the operator-managed PVC
	// deterministically off the PulsarCluster name (see childName), so a
	// user who needs ReadWriteMany for more than one broker replica can
	// pre-provision a PVC of that exact name before creating the cluster;
	// reconcileFunctionsWorkerPackageStoragePVC only ever creates it if
	// missing and never edits an existing one.
	functionsWorkerPackageStorageNameSuffix = "functions-package-storage"
)

// defaultFunctionsWorkerPackageStorageSize is deliberately modest: this PVC
// holds function/connector/source/sink package jars, not Pulsar message
// data, so it does not need BookKeeper-ledger-sized defaults.
var defaultFunctionsWorkerPackageStorageSize = resource.MustParse("8Gi")

// isFileSystemPackageStorage reports whether packageStorage selects (or, at
// the empty default, implies) FileSystemPackagesStorage - the only backend
// the operator self-provisions a broker-mounted volume for. S3/GCS store
// packages remotely, so functionsWorkerPackageStorageConfig instead leaves
// their provider-specific config entirely to the user's spec.config.
func isFileSystemPackageStorage(packageStorage string) bool {
	return packageStorage == "" || packageStorage == packageStorageFileSystem
}

// functionsWorkerPackageStoragePVCName deterministically names the shared
// package-storage PVC after its owning PulsarCluster.
func functionsWorkerPackageStoragePVCName(clusterName string) string {
	return childName(clusterName, functionsWorkerPackageStorageNameSuffix)
}

// withFunctionsWorkerColocatedBrokerDefaults sets the broker.conf keys the
// embedded functions worker's package storage needs, unless the user
// already set them via Broker.spec.config: functionsWorkerEnabled turns the
// embedded worker on, and the packages-management keys route its package
// storage through the broker's own Packages Management Service instead of
// letting it fall back to the ZooKeeper-only DistributedLog path (see the
// package doc comment above). It reuses functionsWorkerPackageStorageConfig
// (the same function already used to default standalone mode's package
// storage) since the underlying provider-class keys are identical
// broker.conf/functions_worker.yml field names either way.
func withFunctionsWorkerColocatedBrokerDefaults(cfg map[string]string, fwSpec clusterv1alpha1.FunctionsWorkerSpec) map[string]string {
	cfg = setConfigDefault(cfg, confKeyFunctionsWorkerEnabled, configValTrue)
	cfg = setConfigDefault(cfg, confKeyEnablePackagesManagement, configValTrue)
	for k, v := range functionsWorkerPackageStorageConfig(fwSpec.PackageStorage) {
		cfg = setConfigDefault(cfg, k, v)
	}
	if isFileSystemPackageStorage(fwSpec.PackageStorage) {
		cfg = setConfigDefault(cfg, confKeyStoragePath, functionsWorkerPackageStorageMountPath)
	}
	return cfg
}

// withFunctionsWorkerClusterDefault sets pulsarFunctionsCluster to the
// PulsarCluster's own name, unless the user already set it, mirroring
// withClusterNameDefault for Broker/Proxy. In colocated mode this is
// belt-and-suspenders - PulsarService.initializeWorkerConfigFromBrokerConfig
// always overrides pulsarFunctionsCluster from the broker's own clusterName
// regardless of what functions_worker.yml says - but it costs nothing to be
// correct here too, and it is the only place standalone mode (which has no
// broker to inherit from) gets a real cluster name instead of
// functionsWorkerDefaultConfig's cluster-less "standalone" placeholder.
func withFunctionsWorkerClusterDefault(cfg map[string]string, clusterName string) map[string]string {
	return setConfigDefault(cfg, cfgKeyPulsarFunctionsCluster, clusterName)
}

func functionsWorkerPackageStorageVolume(pvcName string) corev1.Volume {
	return corev1.Volume{
		Name: functionsWorkerPackageStorageVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
		},
	}
}

func functionsWorkerPackageStorageVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      functionsWorkerPackageStorageVolumeName,
		MountPath: functionsWorkerPackageStorageMountPath,
	}
}

// wireFunctionsWorkerColocated layers a colocated-mode FunctionsWorker's
// config and volumes onto the broker's desired spec, called from
// reconcileBroker before the desired spec is hashed/written. It is a no-op
// whenever FunctionsWorker isn't configured or isn't in (the default)
// colocated mode - standalone mode needs none of this (it runs its own
// dedicated workload, reconciled entirely by FunctionsWorkerReconciler), and
// is rejected by CRD validation anyway (see FunctionsWorkerSpec's CEL rule).
func (r *PulsarClusterReconciler) wireFunctionsWorkerColocated(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, broker *clusterv1alpha1.BrokerSpec) error {
	fw := cluster.Spec.FunctionsWorker
	if fw == nil || functionsWorkerMode(*fw) != functionsWorkerModeColocated {
		return nil
	}

	fwConfig := withFunctionsWorkerClusterDefault(buildFunctionsWorkerSpec(cluster.Spec).Config, cluster.Name)
	broker.FunctionsWorkerConfig = fwConfig
	broker.Config = withFunctionsWorkerColocatedBrokerDefaults(broker.Config, *fw)

	if !isFileSystemPackageStorage(fw.PackageStorage) {
		return nil
	}

	if err := r.reconcileFunctionsWorkerPackageStoragePVC(ctx, cluster, *fw); err != nil {
		return fmt.Errorf("functions worker package storage: %w", err)
	}

	pvcName := functionsWorkerPackageStoragePVCName(cluster.Name)
	broker.Volumes = append(broker.Volumes, functionsWorkerPackageStorageVolume(pvcName))
	broker.VolumeMounts = append(broker.VolumeMounts, functionsWorkerPackageStorageVolumeMount())

	if brokerMaxReplicas(broker) > 1 {
		r.recorder().Eventf(cluster, nil, corev1.EventTypeWarning, "FunctionsPackageStorageSingleWriter", "PackageStorage",
			fmt.Sprintf(
				"colocated FunctionsWorker's FileSystemPackagesStorage PVC %q is provisioned ReadWriteOnce by default; "+
					"with more than one broker replica, only pods that land on the same node as the bound volume can mount it. "+
					"Pre-provision a ReadWriteMany PVC named %q in this namespace before creating the PulsarCluster "+
					"(the operator only creates this PVC if it does not already exist, and never edits an existing one), "+
					"or accept that only one broker replica can actually serve function-package uploads/downloads.",
				pvcName, pvcName))
	}
	return nil
}

// brokerMaxReplicas is the largest replica count the broker tier could reach,
// used to decide whether the default ReadWriteOnce package-storage PVC is a
// real single-writer hazard. It must account for the broker autoscaler: an
// autoscaled broker starts at 1 replica and scales UP later, so keying the
// warning off the current replica count alone would stay silent right up
// until the scale-up hits a multi-attach failure on the RWO volume. When the
// autoscaler is enabled its max bound is authoritative; otherwise the static
// desired replica count is.
func brokerMaxReplicas(broker *clusterv1alpha1.BrokerSpec) int32 {
	current := brokerReplicas(broker.Replicas)
	if brokerAutoscalerEnabled(broker) && broker.Autoscaler.Max != nil && *broker.Autoscaler.Max > current {
		return *broker.Autoscaler.Max
	}
	return current
}

// reconcileFunctionsWorkerPackageStoragePVC creates the shared
// FileSystemPackagesStorage PVC if it does not already exist. It
// deliberately never updates an existing PVC: most PersistentVolumeClaimSpec
// fields are immutable after creation, and a user who pre-provisioned their
// own (e.g. a ReadWriteMany claim for a multi-broker cluster) must be left
// completely alone.
func (r *PulsarClusterReconciler) reconcileFunctionsWorkerPackageStoragePVC(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, fw clusterv1alpha1.FunctionsWorkerSpec) error {
	name := functionsWorkerPackageStoragePVCName(cluster.Name)

	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: cluster.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting functions worker package storage PVC: %w", err)
	}

	size := defaultFunctionsWorkerPackageStorageSize
	var storageClassName *string
	if vol := fw.PackageStorageVolume; vol != nil {
		if !vol.Size.IsZero() {
			size = vol.Size
		}
		storageClassName = vol.StorageClassName
	}
	if storageClassName == nil {
		storageClassName = globalStorageClassName(cluster.Spec)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    builder.Labels(cluster.Name, functionsWorkerComponent),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
			StorageClassName: storageClassName,
		},
	}
	if err := controllerutil.SetControllerReference(cluster, pvc, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on functions worker package storage PVC: %w", err)
	}
	if err := r.Create(ctx, pvc); err != nil {
		return fmt.Errorf("creating functions worker package storage PVC: %w", err)
	}
	return nil
}
