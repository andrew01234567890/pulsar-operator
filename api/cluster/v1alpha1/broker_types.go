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

// BrokerAutoscalerSpec configures the broker CPU/throughput autoscaler.
//
// Scale-up requires every broker's composite load (max of CPU, bandwidth-in,
// bandwidth-out, mirroring Pulsar's ThresholdShedder) to exceed
// higherCpuThreshold; scale-down requires every broker below lowerCpuThreshold.
// A single hot or idle broker never triggers a scaling action on its own.
// +kubebuilder:validation:XValidation:rule="self.lowerCpuThreshold < self.higherCpuThreshold",message="lower threshold must be below higher"
// +kubebuilder:validation:XValidation:rule="!has(self.max) || !has(self.min) || self.min <= self.max",message="min must be <= max"
type BrokerAutoscalerSpec struct {
	// enabled turns on the broker autoscaler.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// min is the minimum number of brokers. The autoscaler never scales below this.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Min *int32 `json:"min,omitempty"`

	// max is the maximum number of brokers. The autoscaler never scales above this.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Max *int32 `json:"max,omitempty"`

	// lowerCpuThreshold triggers a scale-down when every broker's load is below
	// it. Expressed as a whole-number percent in the range 0-100 (e.g. 30 means
	// 30% CPU); autoscaler controllers divide by 100.
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	LowerCpuThreshold *int32 `json:"lowerCpuThreshold,omitempty"`

	// higherCpuThreshold triggers a scale-up when every broker's load is at or
	// above it. Expressed as a whole-number percent in the range 0-100 (e.g. 80
	// means 80% CPU); autoscaler controllers divide by 100.
	// +optional
	// +kubebuilder:default=80
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	HigherCpuThreshold *int32 `json:"higherCpuThreshold,omitempty"`

	// scaleUpBy is the number of brokers to add on each scale-up.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	ScaleUpBy *int32 `json:"scaleUpBy,omitempty"`

	// scaleDownBy is the number of brokers to remove on each scale-down.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	ScaleDownBy *int32 `json:"scaleDownBy,omitempty"`

	// stabilizationWindowSeconds gates scaling decisions until all broker pods have
	// been continuously Ready for this long, preventing flapping.
	// +optional
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	StabilizationWindowSeconds *int32 `json:"stabilizationWindowSeconds,omitempty"`

	// periodSeconds is the interval between autoscaler evaluations.
	// +optional
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`

	// resourcesUsageSource selects where broker load is read from: the Pulsar load
	// manager report, or raw Kubernetes CPU metrics.
	// +optional
	// +kubebuilder:default=PulsarLBReport
	// +kubebuilder:validation:Enum=PulsarLBReport;K8SMetrics
	ResourcesUsageSource string `json:"resourcesUsageSource,omitempty"`
}

// BrokerSpec defines the desired state of Broker.
type BrokerSpec struct {
	// replicas is the number of broker pods.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the broker container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// config sets broker.conf key/value overrides layered on top of operator defaults.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// resources are the compute resource requirements for the broker container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// loadBalancer selects the Pulsar load manager implementation. "extensible"
	// (ExtensibleLoadManagerImpl) is required for live bundle-transfer scale-down
	// and is strongly recommended over the legacy "simple" load manager.
	// +optional
	// +kubebuilder:default=extensible
	// +kubebuilder:validation:Enum=extensible;simple
	LoadBalancer string `json:"loadBalancer,omitempty"`

	// autoscaler configures CPU/throughput-driven horizontal scaling.
	// +optional
	Autoscaler *BrokerAutoscalerSpec `json:"autoscaler,omitempty"`

	// antiaffinity configures pod anti-affinity rules for broker pods.
	// +optional
	Antiaffinity *AntiAffinityConfig `json:"antiaffinity,omitempty"`

	// pdb configures the PodDisruptionBudget for broker pods.
	// +optional
	Pdb *PodDisruptionBudgetConfig `json:"pdb,omitempty"`
}

// BrokerStatus defines the observed state of Broker.
type BrokerStatus struct {
	// replicas is the observed number of broker pods.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the observed number of Ready broker pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Broker resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Broker is the Schema for the brokers API
type Broker struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Broker
	// +required
	Spec BrokerSpec `json:"spec"`

	// status defines the observed state of Broker
	// +optional
	Status BrokerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BrokerList contains a list of Broker
type BrokerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Broker `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Broker{}, &BrokerList{})
		return nil
	})
}
