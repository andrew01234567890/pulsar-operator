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

// Restore cookie lineage policies.
const (
	// CookieLineagePolicyEnforce refuses to restore on a BookKeeper cookie
	// lineage mismatch between the backup's capturedInstanceId and the
	// target cluster's current instanceId. This is the default: replaying
	// Oxia metadata against a BookKeeper cluster with a different
	// instanceId risks pointing at ledgers that don't exist, or worse,
	// silently reading the wrong ones.
	CookieLineagePolicyEnforce = "enforce"

	// CookieLineagePolicyOverride is a documented, supervised force-rewrite
	// for the rare case an operator has verified a lineage mismatch is
	// safe. It is never the default.
	CookieLineagePolicyOverride = "override"
)

// Restore lifecycle phases.
const (
	RestorePhasePending   = "Pending"
	RestorePhaseRunning   = "Running"
	RestorePhaseCompleted = "Completed"
	RestorePhaseFailed    = "Failed"
)

// Cookie lineage check results.
const (
	CookieLineageCheckResultPassed   = "Passed"
	CookieLineageCheckResultMismatch = "Mismatch"
	CookieLineageCheckResultUnknown  = "Unknown"
)

// RestoreSource locates the backup artifact a Restore reads from.
type RestoreSource struct {
	// destination locates the object-store artifact.
	// +required
	Destination BackupDestination `json:"destination"`

	// artifactURI is the fully-qualified location of the backup artifact
	// within destination, as recorded in Backup.status.artifactURI.
	// +required
	ArtifactURI string `json:"artifactURI"`
}

// RestoreSpec defines the desired state of Restore.
type RestoreSpec struct {
	// source locates the backup artifact to restore from.
	// +required
	Source RestoreSource `json:"source"`

	// targetClusterRef names the PulsarCluster, in the same namespace, to
	// restore into.
	// +required
	TargetClusterRef corev1.LocalObjectReference `json:"targetClusterRef"`

	// skipEphemeral excludes ephemeral/transient state captured in the
	// backup (if any) from the restore.
	// +optional
	// +kubebuilder:default=true
	SkipEphemeral *bool `json:"skipEphemeral,omitempty"`

	// cookieLineagePolicy controls how a BookKeeper cookie lineage mismatch
	// between the backup's capturedInstanceId and the target cluster's
	// current instanceId is handled. See CookieLineagePolicyEnforce and
	// CookieLineagePolicyOverride.
	// +optional
	// +kubebuilder:default=enforce
	// +kubebuilder:validation:Enum=enforce;override
	CookieLineagePolicy string `json:"cookieLineagePolicy,omitempty"`
}

// CookieLineageCheck reports the outcome of the BookKeeper cookie lineage
// check performed before replaying a backup into the target cluster.
type CookieLineageCheck struct {
	// result is the outcome of the cookie lineage check.
	// +optional
	// +kubebuilder:validation:Enum=Passed;Mismatch;Unknown
	Result string `json:"result,omitempty"`

	// detail is a human-readable explanation of the result, e.g. the
	// mismatched instanceId values.
	// +optional
	Detail string `json:"detail,omitempty"`
}

// RestoreStatus defines the observed state of Restore.
type RestoreStatus struct {
	// phase is the observed lifecycle phase of the Restore.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	Phase string `json:"phase,omitempty"`

	// keysRestored is the number of Oxia keys replayed into the target cluster.
	// +optional
	KeysRestored int64 `json:"keysRestored,omitempty"`

	// cookieLineageCheck reports the outcome of the BookKeeper cookie
	// lineage check performed before replay.
	// +optional
	CookieLineageCheck CookieLineageCheck `json:"cookieLineageCheck,omitzero"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Restore resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target Cluster",type=string,JSONPath=`.spec.targetClusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Lineage",type=string,JSONPath=`.status.cookieLineageCheck.result`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Restore is the Schema for the restores API
type Restore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Restore
	// +required
	Spec RestoreSpec `json:"spec"`

	// status defines the observed state of Restore
	// +optional
	Status RestoreStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RestoreList contains a list of Restore
type RestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Restore `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Restore{}, &RestoreList{})
		return nil
	})
}
