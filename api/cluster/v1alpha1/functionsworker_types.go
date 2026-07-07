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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// FunctionsWorkerSpec defines the desired state of FunctionsWorker.
//
// standalone mode is rejected by the CEL rule below: Pulsar's standalone
// functions-worker startup (PulsarStandalone/PulsarBrokerStarter's
// runFunctionsWorker path) unconditionally builds a DistributedLog
// package-storage URI from the metadata store URL, which throws "authority
// component is missing in service uri" against oxia:// with no config
// workaround - a hard upstream limitation, not something this operator can
// route around. Colocated mode has no such crash (PulsarWorkerService.
// initInBroker gates the same DLog init behind
// ServiceConfiguration.isMetadataStoreBackedByZookeeper(), which is false for
// Oxia, so it is skipped rather than attempted) and is fully supported - see
// the umbrella PulsarCluster reconciler's broker package-management wiring.
// If a future Pulsar release fixes the standalone DLog path to tolerate
// non-ZooKeeper metadata stores, this rule can be relaxed.
//
// The second rule rejects any non-FileSystem packageStorage: on this
// Oxia-only operator, FileSystemPackagesStorage is the only Packages
// Management Service provider that initializes without ZooKeeper. Both the
// core-Pulsar default (BookKeeperPackagesStorageProvider, DistributedLog-
// backed) and the S3/GCS enum values (which have no built-in core-Pulsar
// provider class, so the broker also falls back to the BookKeeper default)
// require a ZooKeeper-backed metadata store and crash the broker at startup
// on Oxia - the exact failure mode this whole feature exists to avoid. An
// advanced user who has their own DistributedLog-free provider can still set
// packagesManagementStorageProvider directly on Broker.spec.config while
// leaving packageStorage=FileSystemPackagesStorage to pass this rule.
// +kubebuilder:validation:XValidation:rule="self.mode != 'standalone'",message="standalone FunctionsWorker is unsupported on this Oxia-only operator (Pulsar's standalone functions-worker startup unconditionally requires a ZooKeeper-backed metadata store for its DistributedLog package storage); use mode: colocated instead"
// +kubebuilder:validation:XValidation:rule="self.packageStorage == 'FileSystemPackagesStorage'",message="only FileSystemPackagesStorage is supported on this Oxia-only operator: S3PackagesStorage/GCSPackagesStorage have no built-in Pulsar package-storage provider, so the broker falls back to the ZooKeeper-backed BookKeeper provider and crashes on Oxia; use packageStorage: FileSystemPackagesStorage"
type FunctionsWorkerSpec struct {
	// mode selects whether the functions worker runs colocated inside broker
	// pods or as its own standalone deployment. standalone is rejected (see
	// the CEL rule on this type) since it cannot run against an Oxia-only
	// metadata store.
	// +optional
	// +kubebuilder:default=colocated
	// +kubebuilder:validation:Enum=colocated;standalone
	Mode string `json:"mode,omitempty"`

	// replicas is the number of standalone functions-worker pods. Ignored when
	// mode is "colocated".
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the functions-worker container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// config sets functions_worker.yml key/value overrides layered on top of
	// operator defaults.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// packageStorage selects the function-package storage backend. Oxia does
	// not provide the ZooKeeper-backed distributed log package storage KAAP
	// defaults to, so "FileSystemPackagesStorage" is required (and enforced by
	// the struct-level CEL rule) while the metadata store is Oxia-only. The
	// S3PackagesStorage/GCSPackagesStorage enum values are retained only so a
	// selection of them yields a clear, targeted CEL message rather than a
	// generic enum error; they are rejected because core Pulsar ships no
	// package-storage provider for them (unlike tiered-storage OFFLOAD of
	// message data, which is a wholly separate S3/GCS path and is unaffected).
	// +optional
	// +kubebuilder:default=FileSystemPackagesStorage
	// +kubebuilder:validation:Enum=FileSystemPackagesStorage;S3PackagesStorage;GCSPackagesStorage
	PackageStorage string `json:"packageStorage,omitempty"`

	// packageStorageVolume configures the PersistentVolumeClaim the operator
	// provisions for FileSystemPackagesStorage when mode is colocated (the
	// PVC is mounted on every broker pod, since colocated functions run
	// embedded in the broker). Ignored when packageStorage is not
	// FileSystemPackagesStorage. The operator only creates this PVC if it
	// does not already exist and never edits it afterwards (most PVC fields
	// are immutable) - a ReadWriteOnce PVC is created by default, which is
	// fine for a single broker replica; for more than one broker replica the
	// volume must be a shared ReadWriteMany filesystem, so pre-provision one
	// yourself (matching this PVC's deterministic name,
	// "<cluster>-functions-package-storage") before creating the
	// PulsarCluster, or accept single-writer semantics.
	// +optional
	PackageStorageVolume *VolumeSpec `json:"packageStorageVolume,omitempty"`
}

// FunctionsWorkerStatus defines the observed state of FunctionsWorker.
type FunctionsWorkerStatus struct {
	// replicas is the observed number of functions-worker pods.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the observed number of Ready functions-worker pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the FunctionsWorker resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FunctionsWorker is the Schema for the functionsworkers API
type FunctionsWorker struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of FunctionsWorker
	// +required
	Spec FunctionsWorkerSpec `json:"spec"`

	// status defines the observed state of FunctionsWorker
	// +optional
	Status FunctionsWorkerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// FunctionsWorkerList contains a list of FunctionsWorker
type FunctionsWorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []FunctionsWorker `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &FunctionsWorker{}, &FunctionsWorkerList{})
		return nil
	})
}
