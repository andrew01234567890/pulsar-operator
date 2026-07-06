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

// ProxyTlsConfig configures TLS/mTLS termination on the proxy.
//
// enabled without a secretName is rejected at admission: it would otherwise
// bring the proxy up plaintext-only while the user believed TLS was on, a
// silent security downgrade.
// +kubebuilder:validation:XValidation:rule="!self.enabled || (has(self.secretName) && size(self.secretName) > 0)",message="tls.secretName is required when tls.enabled is true"
type ProxyTlsConfig struct {
	// enabled turns on TLS termination on the proxy's client-facing ports.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// secretName is the Secret holding the TLS certificate/key served by the
	// proxy. Required when enabled is true.
	// +optional
	SecretName string `json:"secretName,omitempty"`
}

// ProxyAutoscalerSpec configures optional CPU-based horizontal scaling for the
// stateless proxy tier.
type ProxyAutoscalerSpec struct {
	// enabled turns on the proxy autoscaler.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// min is the minimum number of proxy pods.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Min *int32 `json:"min,omitempty"`

	// max is the maximum number of proxy pods.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Max *int32 `json:"max,omitempty"`

	// targetCPUUtilizationPercentage is the average CPU utilization target across
	// proxy pods.
	// +optional
	// +kubebuilder:default=80
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty"`
}

// ProxySpec defines the desired state of Proxy.
type ProxySpec struct {
	// replicas is the number of proxy pods.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// image overrides the proxy container image. Falls back to
	// PulsarCluster.spec.image when unset.
	// +optional
	Image string `json:"image,omitempty"`

	// config sets proxy.conf key/value overrides layered on top of operator defaults.
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// tls configures TLS/mTLS termination on the proxy.
	// +optional
	Tls *ProxyTlsConfig `json:"tls,omitempty"`

	// resources are the compute resource requirements for the proxy container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// autoscaler optionally configures CPU-based horizontal scaling.
	// +optional
	Autoscaler *ProxyAutoscalerSpec `json:"autoscaler,omitempty"`
}

// ProxyStatus defines the observed state of Proxy.
type ProxyStatus struct {
	// replicas is the observed number of proxy pods.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the observed number of Ready proxy pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Proxy resource.
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

// Proxy is the Schema for the proxies API
type Proxy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Proxy
	// +required
	Spec ProxySpec `json:"spec"`

	// status defines the observed state of Proxy
	// +optional
	Status ProxyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ProxyList contains a list of Proxy
type ProxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Proxy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Proxy{}, &ProxyList{})
		return nil
	})
}
