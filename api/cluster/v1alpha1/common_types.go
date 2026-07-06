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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// AntiAffinityConfig configures pod anti-affinity for a component's pods.
type AntiAffinityConfig struct {
	// enabled turns on the component's default anti-affinity rules.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// host requires (hard) rather than prefers (soft) spreading pods across nodes.
	// Stateful tiers default this to hard; stateless tiers default it to soft.
	// +optional
	// +kubebuilder:default=false
	Host *bool `json:"host,omitempty"`

	// zone requires (hard) rather than prefers (soft) spreading pods across zones.
	// +optional
	// +kubebuilder:default=false
	Zone *bool `json:"zone,omitempty"`
}

// PodDisruptionBudgetConfig configures the PodDisruptionBudget generated for a component.
type PodDisruptionBudgetConfig struct {
	// enabled controls whether the operator creates a PodDisruptionBudget for this component.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// maxUnavailable is the maximum number of pods that can be unavailable during a
	// voluntary disruption. Defaults are computed from component quorum math.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// VolumeSpec configures a single persistent volume for a stateful component.
type VolumeSpec struct {
	// storageClassName is the StorageClass used to provision the volume's PVC.
	// Falls back to PulsarCluster.spec.global.storageClassName when unset.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// size is the requested storage capacity.
	// +optional
	Size resource.Quantity `json:"size,omitempty"`
}
