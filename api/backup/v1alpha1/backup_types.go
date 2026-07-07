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
)

// Backup consistency modes.
const (
	// BackupConsistencyCrash captures a crash-consistent snapshot, with no
	// coordination with running Pulsar components.
	BackupConsistencyCrash = "crash"

	// BackupConsistencyApplication additionally quiesces/coordinates with
	// the cluster for an application-consistent capture.
	BackupConsistencyApplication = "application"
)

// Backup lifecycle phases.
const (
	BackupPhasePending   = "Pending"
	BackupPhaseRunning   = "Running"
	BackupPhaseCompleted = "Completed"
	BackupPhaseFailed    = "Failed"
)

// BackupComponents selects which cluster state a Backup captures. v1 only
// captures the Oxia metadata store's keyspace; the volume-level snapshot
// fields are reserved for a future release so the on-disk shape of
// BackupSpec doesn't need to change again to add them.
type BackupComponents struct {
	// oxiaMetadata captures the Oxia metadata store's keyspace.
	// +optional
	// +kubebuilder:default=true
	OxiaMetadata *bool `json:"oxiaMetadata,omitempty"`

	// oxiaVolumes additionally snapshots Oxia's persistent volumes.
	// Not yet implemented; reserved for a future release.
	// +optional
	// +kubebuilder:default=false
	OxiaVolumes *bool `json:"oxiaVolumes,omitempty"`

	// bookkeeperVolumes additionally snapshots BookKeeper's persistent
	// volumes. Not yet implemented; reserved for a future release.
	// +optional
	// +kubebuilder:default=false
	BookkeeperVolumes *bool `json:"bookkeeperVolumes,omitempty"`
}

// BackupSpec defines the desired state of Backup.
type BackupSpec struct {
	// clusterRef names the PulsarCluster, in the same namespace, to back up.
	// +required
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// components selects which cluster state this Backup captures.
	// +optional
	Components BackupComponents `json:"components,omitempty"`

	// destination is the object-store target the backup artifact is written to.
	// +required
	Destination BackupDestination `json:"destination"`

	// consistency selects the point-in-time guarantee for the captured
	// state. "crash" (default) captures a crash-consistent snapshot with no
	// coordination with Pulsar components. "application" additionally
	// quiesces/coordinates with the cluster for an application-consistent
	// capture.
	// +optional
	// +kubebuilder:default=crash
	// +kubebuilder:validation:Enum=crash;application
	Consistency string `json:"consistency,omitempty"`

	// includeEphemeral additionally captures ephemeral/transient state (e.g.
	// in-flight cursors) that is normally excluded because consumers
	// reconstruct it after a restore.
	// +optional
	// +kubebuilder:default=false
	IncludeEphemeral *bool `json:"includeEphemeral,omitempty"`
}

// BackupStatus defines the observed state of Backup.
type BackupStatus struct {
	// phase is the observed lifecycle phase of the Backup.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	Phase string `json:"phase,omitempty"`

	// startTime is when the backup capture began.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// completionTime is when the backup capture finished, successfully or not.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// artifactURI is the fully-qualified location of the written backup
	// artifact within destination.
	// +optional
	ArtifactURI string `json:"artifactURI,omitempty"`

	// oxiaKeysCaptured is the number of Oxia keys captured in the artifact.
	// +optional
	OxiaKeysCaptured int64 `json:"oxiaKeysCaptured,omitempty"`

	// sizeBytes is the size of the written backup artifact, in bytes.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// capturedInstanceId is the BookKeeper cluster instanceId observed at
	// capture time, recorded so a later Restore can verify cookie lineage
	// against the target cluster before replaying ledger metadata into it.
	// +optional
	CapturedInstanceID string `json:"capturedInstanceId,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Backup resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Backup is the Schema for the backups API
type Backup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Backup
	// +required
	Spec BackupSpec `json:"spec"`

	// status defines the observed state of Backup
	// +optional
	Status BackupStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackupList contains a list of Backup
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Backup `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Backup{}, &BackupList{})
		return nil
	})
}
