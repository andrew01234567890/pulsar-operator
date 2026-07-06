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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// MetadataStoreSpec selects and configures the cluster metadata store.
//
// The type is kept as an enum-of-one ("oxia") today so a ZooKeeper option can
// be added later without a schema break; there is no ZooKeeper CRD in v1.
type MetadataStoreSpec struct {
	// type selects the metadata store implementation.
	// +optional
	// +kubebuilder:default=oxia
	// +kubebuilder:validation:Enum=oxia
	Type string `json:"type,omitempty"`
}

// GlobalSpec holds defaults applied across every PulsarCluster component.
type GlobalSpec struct {
	// affinity is the default pod affinity/anti-affinity applied to every
	// component unless overridden per-component.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// zoneSpread turns on a default topologySpreadConstraints (maxSkew=1) across
	// zones for every component, so multi-AZ HA works out of the box.
	// +optional
	// +kubebuilder:default=true
	ZoneSpread bool `json:"zoneSpread,omitempty"`

	// storageClassName is the default StorageClass for every component's PVCs,
	// unless overridden per-component.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// OffloadSpec configures tiered-storage offload of closed ledgers to object
// storage. This is cost/retention tiering, not backup: offloaded objects are
// orphaned if metadata is lost, and are deleted when their topic is deleted.
// Requires the apachepulsar/pulsar-all image (offloader jars).
type OffloadSpec struct {
	// driver selects the tiered-storage backend.
	// +required
	// +kubebuilder:validation:Enum=aws-s3;google-cloud-storage;azureblob;filesystem
	Driver string `json:"driver"`

	// bucket is the object-storage bucket/container to offload into. Not used
	// for the filesystem driver.
	// +optional
	Bucket string `json:"bucket,omitempty"`

	// region is the object-storage region. Not used for the filesystem driver.
	// +optional
	Region string `json:"region,omitempty"`

	// endpoint overrides the object-storage service endpoint, for
	// S3-compatible stores that aren't AWS.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// offloadThresholdBytes is the managed-ledger size, in bytes, above which
	// closed ledgers are offloaded.
	// +optional
	OffloadThresholdBytes *int64 `json:"offloadThresholdBytes,omitempty"`

	// credentialsSecretRef references the Secret holding the offload driver's
	// object-storage credentials.
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`
}

// PulsarClusterSpec defines the desired state of PulsarCluster.
//
// PulsarCluster is the single user-facing umbrella resource; the reconciler
// decomposes it into per-component child resources (Broker, BookKeeper,
// Proxy, AutoRecovery, FunctionsWorker, and metadata.OxiaCluster).
type PulsarClusterSpec struct {
	// pulsarVersion pins the exact Pulsar image tag deployed across components.
	// +optional
	// +kubebuilder:default="5.0.0-M1"
	PulsarVersion string `json:"pulsarVersion,omitempty"`

	// image is the default Pulsar container image for every component, unless
	// overridden per-component.
	// +optional
	Image string `json:"image,omitempty"`

	// imagePullSecrets are the default image pull secrets for every component's
	// pods.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// metadataStore selects and configures the cluster metadata store.
	// +optional
	MetadataStore *MetadataStoreSpec `json:"metadataStore,omitempty"`

	// global holds defaults applied across every component.
	// +optional
	Global *GlobalSpec `json:"global,omitempty"`

	// broker configures the broker component.
	// +optional
	Broker *BrokerSpec `json:"broker,omitempty"`

	// bookKeeper configures the bookie component.
	// +optional
	BookKeeper *BookKeeperSpec `json:"bookKeeper,omitempty"`

	// proxy configures the optional proxy component.
	// +optional
	Proxy *ProxySpec `json:"proxy,omitempty"`

	// oxia configures the metadata.OxiaCluster child resource.
	// +optional
	Oxia *metadatav1alpha1.OxiaClusterSpec `json:"oxia,omitempty"`

	// autoRecovery configures the autorecovery component.
	// +optional
	AutoRecovery *AutoRecoverySpec `json:"autoRecovery,omitempty"`

	// functionsWorker configures the functions-worker component.
	// +optional
	FunctionsWorker *FunctionsWorkerSpec `json:"functionsWorker,omitempty"`

	// offload configures the cluster-default tiered-storage offload policy.
	// +optional
	Offload *OffloadSpec `json:"offload,omitempty"`
}

// PulsarClusterStatus defines the observed state of PulsarCluster.
type PulsarClusterStatus struct {
	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// brokerPhase is the observed phase of the child Broker resource.
	// +optional
	BrokerPhase string `json:"brokerPhase,omitempty"`

	// bookKeeperPhase is the observed phase of the child BookKeeper resource.
	// +optional
	BookKeeperPhase string `json:"bookKeeperPhase,omitempty"`

	// proxyPhase is the observed phase of the child Proxy resource.
	// +optional
	ProxyPhase string `json:"proxyPhase,omitempty"`

	// oxiaPhase is the observed phase of the child OxiaCluster resource.
	// +optional
	OxiaPhase string `json:"oxiaPhase,omitempty"`

	// autoRecoveryPhase is the observed phase of the child AutoRecovery resource.
	// +optional
	AutoRecoveryPhase string `json:"autoRecoveryPhase,omitempty"`

	// functionsWorkerPhase is the observed phase of the child FunctionsWorker resource.
	// +optional
	FunctionsWorkerPhase string `json:"functionsWorkerPhase,omitempty"`

	// conditions represent the current state of the PulsarCluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.pulsarVersion`
// +kubebuilder:printcolumn:name="Broker",type=string,JSONPath=`.status.brokerPhase`
// +kubebuilder:printcolumn:name="BookKeeper",type=string,JSONPath=`.status.bookKeeperPhase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PulsarCluster is the Schema for the pulsarclusters API
type PulsarCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PulsarCluster
	// +required
	Spec PulsarClusterSpec `json:"spec"`

	// status defines the observed state of PulsarCluster
	// +optional
	Status PulsarClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PulsarClusterList contains a list of PulsarCluster
type PulsarClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PulsarCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PulsarCluster{}, &PulsarClusterList{})
		return nil
	})
}
