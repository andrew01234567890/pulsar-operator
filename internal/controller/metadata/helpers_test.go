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

package metadata

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// Shared literals/builders for table-driven tests across this package.
const (
	testOxiaName      = "myoxia"
	testOxiaNamespace = "pulsar-ns"

	// testEnvtestNamespace is a real Kubernetes Namespace envtest always
	// provisions, used by the integration suite for objects that must
	// actually round-trip through the API server. It is unrelated to (and
	// happens to share a spelling with) testNamespaceDefault below, which
	// names an Oxia namespace inside OxiaClusterSpec.Namespaces.
	testEnvtestNamespace = "default"

	testNamespaceDefault = "default"
	testNamespaceBroker  = "broker"
)

func ptr[T any](v T) *T { return &v }

func newTestOxiaCluster(mutators ...func(*metadatav1alpha1.OxiaCluster)) *metadatav1alpha1.OxiaCluster {
	oxia := &metadatav1alpha1.OxiaCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testOxiaName,
			Namespace: testOxiaNamespace,
		},
	}
	for _, m := range mutators {
		m(oxia)
	}
	return oxia
}

func withServerReplicas(replicas int32) func(*metadatav1alpha1.OxiaCluster) {
	return func(oxia *metadatav1alpha1.OxiaCluster) {
		if oxia.Spec.Server == nil {
			oxia.Spec.Server = &metadatav1alpha1.OxiaServerSpec{}
		}
		oxia.Spec.Server.Replicas = ptr(replicas)
	}
}

func withNamespaces(namespaces ...metadatav1alpha1.OxiaNamespaceSpec) func(*metadatav1alpha1.OxiaCluster) {
	return func(oxia *metadatav1alpha1.OxiaCluster) {
		oxia.Spec.Namespaces = namespaces
	}
}

func withAllowExtraAuthorities(allow bool) func(*metadatav1alpha1.OxiaCluster) {
	return func(oxia *metadatav1alpha1.OxiaCluster) {
		oxia.Spec.AllowExtraAuthorities = ptr(allow)
	}
}
