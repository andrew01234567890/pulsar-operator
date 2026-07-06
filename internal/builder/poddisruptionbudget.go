package builder

import (
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PodDisruptionBudget builds a PodDisruptionBudget scoped to selector,
// bounding voluntary disruptions (e.g. node drains, cluster autoscaler
// evictions) to maxUnavailable pods at a time.
func PodDisruptionBudget(name, namespace string, labels, selector map[string]string, maxUnavailable intstr.IntOrString) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
		},
	}
}
