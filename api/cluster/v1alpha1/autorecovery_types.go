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

// AutoRecoverySpec defines the desired state of AutoRecovery.
//
// AutoRecovery runs its own Auditor + ReplicationWorker as a dedicated
// StatefulSet (never embedded in bookie pods) so it survives bookie restarts.
type AutoRecoverySpec struct {
	// mode selects whether autorecovery runs embedded in bookie pods or as its
	// own dedicated deployment. The operator always deploys it as a standalone
	// StatefulSet; "embedded" additionally colocates the auditor process
	// configuration with bookies for smaller clusters.
	// +optional
	// +kubebuilder:default=embedded
	// +kubebuilder:validation:Enum=embedded;dedicated
	Mode string `json:"mode,omitempty"`

	// replicas is the number of autorecovery pods.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the autorecovery container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// config sets bookkeeper.conf key/value overrides layered on top of operator defaults.
	// +optional
	Config map[string]string `json:"config,omitempty"`
}

// AutoRecoveryStatus defines the observed state of AutoRecovery.
type AutoRecoveryStatus struct {
	// replicas is the observed number of autorecovery pods.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the observed number of Ready autorecovery pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the AutoRecovery resource.
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

// AutoRecovery is the Schema for the autorecoveries API
type AutoRecovery struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of AutoRecovery
	// +required
	Spec AutoRecoverySpec `json:"spec"`

	// status defines the observed state of AutoRecovery
	// +optional
	Status AutoRecoveryStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// AutoRecoveryList contains a list of AutoRecovery
type AutoRecoveryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []AutoRecovery `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &AutoRecovery{}, &AutoRecoveryList{})
		return nil
	})
}
