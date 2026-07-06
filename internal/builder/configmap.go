package builder

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConfigMap builds a ConfigMap holding data (e.g. a rendered properties
// file). It does not set an owner reference or the config-checksum
// annotation; callers add those with SetControllerOwner and
// WithConfigChecksum respectively.
func ConfigMap(name, namespace string, labels, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Data: data,
	}
}
