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
type FunctionsWorkerSpec struct {
	// mode selects whether the functions worker runs colocated inside broker
	// pods or as its own standalone deployment.
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
	// defaults to, so "FileSystemPackagesStorage" is required while the
	// metadata store is Oxia-only; a future webhook enforces this.
	// +optional
	// +kubebuilder:default=FileSystemPackagesStorage
	// +kubebuilder:validation:Enum=FileSystemPackagesStorage;S3PackagesStorage;GCSPackagesStorage
	PackageStorage string `json:"packageStorage,omitempty"`
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
