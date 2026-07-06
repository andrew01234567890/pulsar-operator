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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// OxiaCoordinatorSpec configures the Oxia coordinator Deployment, which owns
// leader election and the server-shard assignment ConfigMap.
type OxiaCoordinatorSpec struct {
	// replicas is the number of coordinator pods. At least 2 are required for
	// coordinator leader-election HA.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the coordinator container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// resources are the compute resource requirements for the coordinator container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// OxiaServerSpec configures the Oxia server StatefulSet, which holds the
// authoritative Pulsar metadata (managed-ledger pointers, topic ownership,
// schemas, cursors) and has no native snapshot/export tooling.
type OxiaServerSpec struct {
	// replicas is the number of server pods. Must be odd for quorum correctness;
	// enforced by a future validating webhook.
	// +optional
	// +kubebuilder:default=3
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the server container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// storageClassName is the StorageClass used to provision each server pod's
	// data/wal PVCs. Falls back to PulsarCluster.spec.global.storageClassName
	// when unset.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// storageSize is the requested capacity for each server pod's data/wal PVCs.
	// +optional
	StorageSize resource.Quantity `json:"storageSize,omitempty"`

	// dbCacheSizeMb sizes the server's RocksDB block cache, in megabytes.
	// +optional
	DbCacheSizeMb *int32 `json:"dbCacheSizeMb,omitempty"`

	// walSyncData forces fsync on every WAL write for durability at the cost of
	// write latency.
	// +optional
	// +kubebuilder:default=true
	WalSyncData *bool `json:"walSyncData,omitempty"`
}

// OxiaNamespaceSpec declares an Oxia namespace (a shard-keyspace) to
// provision, one each for Pulsar's default/broker/bookkeeper metadata trees.
type OxiaNamespaceSpec struct {
	// name is the Oxia namespace name.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// initialShardCount is the number of shards the namespace starts with.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	InitialShardCount *int32 `json:"initialShardCount,omitempty"`

	// replicationFactor is the number of server replicas each shard is
	// replicated to.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	ReplicationFactor *int32 `json:"replicationFactor,omitempty"`
}

// OxiaClusterSpec defines the desired state of OxiaCluster.
type OxiaClusterSpec struct {
	// coordinator configures the coordinator Deployment.
	// +optional
	Coordinator *OxiaCoordinatorSpec `json:"coordinator,omitempty"`

	// server configures the server StatefulSet.
	// +optional
	Server *OxiaServerSpec `json:"server,omitempty"`

	// namespaces are the Oxia namespaces to provision.
	// +optional
	Namespaces []OxiaNamespaceSpec `json:"namespaces,omitempty"`

	// allowExtraAuthorities allows oxia:// clients to specify additional shard
	// authorities beyond the coordinator's own service address.
	// +optional
	// +kubebuilder:default=false
	AllowExtraAuthorities *bool `json:"allowExtraAuthorities,omitempty"`
}

// OxiaClusterStatus defines the observed state of OxiaCluster.
type OxiaClusterStatus struct {
	// coordinatorReplicas is the observed number of Ready coordinator pods.
	// +optional
	CoordinatorReplicas int32 `json:"coordinatorReplicas,omitempty"`

	// serverReplicas is the observed number of Ready server pods.
	// +optional
	ServerReplicas int32 `json:"serverReplicas,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the OxiaCluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Coordinators",type=integer,JSONPath=`.status.coordinatorReplicas`
// +kubebuilder:printcolumn:name="Servers",type=integer,JSONPath=`.status.serverReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OxiaCluster is the Schema for the oxiaclusters API
type OxiaCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of OxiaCluster
	// +required
	Spec OxiaClusterSpec `json:"spec"`

	// status defines the observed state of OxiaCluster
	// +optional
	Status OxiaClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// OxiaClusterList contains a list of OxiaCluster
type OxiaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []OxiaCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &OxiaCluster{}, &OxiaClusterList{})
		return nil
	})
}
