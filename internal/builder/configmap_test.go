package builder

import (
	"maps"
	"testing"
)

func TestConfigMap(t *testing.T) {
	labels := map[string]string{labelComponent: testComponentBroker}
	data := map[string]string{"broker.conf": "brokerServicePort=6650\n"}

	cm := ConfigMap(testNameBroker, testNamespace, labels, data)

	if cm.Name != testNameBroker {
		t.Errorf("Name = %q, want %q", cm.Name, testNameBroker)
	}
	if cm.Namespace != testNamespace {
		t.Errorf("Namespace = %q, want %q", cm.Namespace, testNamespace)
	}
	if !maps.Equal(cm.Labels, labels) {
		t.Errorf("Labels = %v, want %v", cm.Labels, labels)
	}
	if !maps.Equal(cm.Data, data) {
		t.Errorf("Data = %v, want %v", cm.Data, data)
	}
}
