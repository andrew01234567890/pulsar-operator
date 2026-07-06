package builder

import (
	"maps"
	"testing"
)

func TestSelectorLabels(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		component   string
		want        map[string]string
	}{
		{
			name:        "broker component",
			clusterName: testClusterName,
			component:   testComponentBroker,
			want: map[string]string{
				labelName:      nameValue,
				labelInstance:  testClusterName,
				labelComponent: testComponentBroker,
			},
		},
		{
			name:        "different cluster and component",
			clusterName: testClusterName2,
			component:   testComponentBookkeeper,
			want: map[string]string{
				labelName:      nameValue,
				labelInstance:  testClusterName2,
				labelComponent: testComponentBookkeeper,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectorLabels(tt.clusterName, tt.component)
			if !maps.Equal(got, tt.want) {
				t.Errorf("SelectorLabels(%q, %q) = %v, want %v", tt.clusterName, tt.component, got, tt.want)
			}
		})
	}
}

// TestLabels is a regression assertion on the exact standard label set: a
// silent change to a key or value here would stop matching the selector
// baked into an already-created StatefulSet/Deployment and orphan its pods.
func TestLabels(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		component   string
		want        map[string]string
	}{
		{
			name:        "broker component",
			clusterName: testClusterName,
			component:   testComponentBroker,
			want: map[string]string{
				"app.kubernetes.io/name":       "pulsar",
				"app.kubernetes.io/instance":   testClusterName,
				"app.kubernetes.io/component":  testComponentBroker,
				"app.kubernetes.io/managed-by": "pulsar-operator",
			},
		},
		{
			name:        "different cluster and component",
			clusterName: testClusterName2,
			component:   testComponentFunctionsWorker,
			want: map[string]string{
				"app.kubernetes.io/name":       "pulsar",
				"app.kubernetes.io/instance":   testClusterName2,
				"app.kubernetes.io/component":  testComponentFunctionsWorker,
				"app.kubernetes.io/managed-by": "pulsar-operator",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Labels(tt.clusterName, tt.component)
			if !maps.Equal(got, tt.want) {
				t.Errorf("Labels(%q, %q) = %v, want %v", tt.clusterName, tt.component, got, tt.want)
			}
		})
	}
}

func TestLabelsIsSupersetOfSelectorLabels(t *testing.T) {
	labels := Labels(testClusterName, testComponentBroker)
	selector := SelectorLabels(testClusterName, testComponentBroker)

	for k, v := range selector {
		got, ok := labels[k]
		if !ok {
			t.Errorf("Labels missing selector key %q present in SelectorLabels", k)
			continue
		}
		if got != v {
			t.Errorf("Labels[%q] = %q, want %q (from SelectorLabels)", k, got, v)
		}
	}
}

func TestLabelsDoesNotMutateSelectorLabels(t *testing.T) {
	// Labels is implemented in terms of SelectorLabels; guard against a
	// future refactor that mutates a shared/cached map instead of the copy
	// SelectorLabels returns today.
	first := Labels(testClusterName, testComponentBroker)
	second := SelectorLabels(testClusterName, testComponentBroker)

	if _, ok := second[labelManagedBy]; ok {
		t.Errorf("SelectorLabels leaked %s from a prior Labels call: %v", labelManagedBy, second)
	}
	_ = first
}
