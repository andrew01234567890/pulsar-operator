package builder

import (
	"maps"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestPodDisruptionBudget(t *testing.T) {
	tests := []struct {
		name           string
		maxUnavailable intstr.IntOrString
	}{
		{name: "integer maxUnavailable", maxUnavailable: intstr.FromInt32(1)},
		{name: "percentage maxUnavailable", maxUnavailable: intstr.FromString("25%")},
	}

	labels := map[string]string{labelComponent: testComponentBookkeeper}
	selector := map[string]string{labelComponent: testComponentBookkeeper, labelInstance: testClusterName}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pdb := PodDisruptionBudget(testNameBookkeeper, testNamespace, labels, selector, tt.maxUnavailable)

			if pdb.Name != testNameBookkeeper || pdb.Namespace != testNamespace {
				t.Errorf("got name/namespace %q/%q, want %q/%q", pdb.Name, pdb.Namespace, testNameBookkeeper, testNamespace)
			}
			if !maps.Equal(pdb.Labels, labels) {
				t.Errorf("Labels = %v, want %v", pdb.Labels, labels)
			}
			if pdb.Spec.Selector == nil || !maps.Equal(pdb.Spec.Selector.MatchLabels, selector) {
				t.Errorf("Selector.MatchLabels = %v, want %v", pdb.Spec.Selector, selector)
			}
			if pdb.Spec.MaxUnavailable == nil || *pdb.Spec.MaxUnavailable != tt.maxUnavailable {
				t.Errorf("MaxUnavailable = %v, want %v", pdb.Spec.MaxUnavailable, tt.maxUnavailable)
			}
			if pdb.Spec.MinAvailable != nil {
				t.Errorf("MinAvailable = %v, want nil (mutually exclusive with MaxUnavailable)", pdb.Spec.MinAvailable)
			}
		})
	}
}
