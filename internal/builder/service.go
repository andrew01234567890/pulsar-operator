package builder

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// clusterIPNone is the special ClusterIP value that makes a Service
// headless: no virtual IP, only per-Pod DNS A/AAAA records.
const clusterIPNone = "None"

// Service builds a normal (virtual-IP) ClusterIP Service in front of
// selector.
func Service(name, namespace string, labels, selector map[string]string, ports []corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports:    ports,
		},
	}
}

// HeadlessService builds a headless (ClusterIP: None) Service for
// StatefulSet peer discovery. PublishNotReadyAddresses is set because
// quorum-forming peers (BookKeeper ensembles, Oxia nodes, ...) need to
// resolve each other's DNS records during cluster formation, before any of
// them is individually Ready.
func HeadlessService(name, namespace string, labels, selector map[string]string, ports []corev1.ServicePort) *corev1.Service {
	svc := Service(name, namespace, labels, selector, ports)
	svc.Spec.ClusterIP = clusterIPNone
	svc.Spec.PublishNotReadyAddresses = true
	return svc
}
