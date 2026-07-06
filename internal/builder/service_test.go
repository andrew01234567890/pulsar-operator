package builder

import (
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func testPorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: testComponentBroker, Port: 6650, TargetPort: intstr.FromInt32(6650)},
		{Name: "http", Port: 8080, TargetPort: intstr.FromInt32(8080)},
	}
}

func TestService(t *testing.T) {
	labels := map[string]string{labelComponent: testComponentBroker}
	selector := map[string]string{labelComponent: testComponentBroker, labelInstance: testClusterName}
	ports := testPorts()

	svc := Service(testNameBroker, testNamespace, labels, selector, ports)

	if svc.Name != testNameBroker || svc.Namespace != testNamespace {
		t.Errorf("got name/namespace %q/%q, want %q/%q", svc.Name, svc.Namespace, testNameBroker, testNamespace)
	}
	if !maps.Equal(svc.Labels, labels) {
		t.Errorf("Labels = %v, want %v", svc.Labels, labels)
	}
	if !maps.Equal(svc.Spec.Selector, selector) {
		t.Errorf("Selector = %v, want %v", svc.Spec.Selector, selector)
	}
	if len(svc.Spec.Ports) != len(ports) {
		t.Fatalf("got %d ports, want %d", len(svc.Spec.Ports), len(ports))
	}
	for i := range ports {
		if svc.Spec.Ports[i] != ports[i] {
			t.Errorf("Ports[%d] = %+v, want %+v", i, svc.Spec.Ports[i], ports[i])
		}
	}
	if svc.Spec.ClusterIP != "" {
		t.Errorf("ClusterIP = %q, want unset (normal ClusterIP allocation)", svc.Spec.ClusterIP)
	}
	if svc.Spec.PublishNotReadyAddresses {
		t.Error("Service must not publish not-ready addresses")
	}
}

func TestHeadlessService(t *testing.T) {
	labels := map[string]string{labelComponent: testComponentBookkeeper}
	selector := map[string]string{labelComponent: testComponentBookkeeper}
	ports := testPorts()

	svc := HeadlessService(testNameBookkeeper, testNamespace, labels, selector, ports)

	if svc.Spec.ClusterIP != "None" {
		t.Errorf("ClusterIP = %q, want %q", svc.Spec.ClusterIP, "None")
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("PublishNotReadyAddresses = false, want true (peers need DNS before Ready)")
	}
	if !maps.Equal(svc.Spec.Selector, selector) {
		t.Errorf("Selector = %v, want %v", svc.Spec.Selector, selector)
	}
	if !maps.Equal(svc.Labels, labels) {
		t.Errorf("Labels = %v, want %v", svc.Labels, labels)
	}
	if svc.Name != testNameBookkeeper || svc.Namespace != testNamespace {
		t.Errorf("got name/namespace %q/%q, want %q/%q", svc.Name, svc.Namespace, testNameBookkeeper, testNamespace)
	}
}
