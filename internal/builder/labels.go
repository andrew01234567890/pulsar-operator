// Package builder provides small, generic helpers for assembling the
// Kubernetes objects shared by every Pulsar component reconciler (Oxia,
// BookKeeper, Broker, Proxy, AutoRecovery, FunctionsWorker): standard
// labels, a config-checksum annotation, and thin constructors for
// ConfigMaps, Services, and PodDisruptionBudgets. Reconcilers assemble
// their own StatefulSets/Deployments and use these helpers only for the
// pieces that are identical across components.
package builder

const (
	labelName      = "app.kubernetes.io/name"
	labelInstance  = "app.kubernetes.io/instance"
	labelComponent = "app.kubernetes.io/component"
	labelManagedBy = "app.kubernetes.io/managed-by"

	nameValue      = "pulsar"
	managedByValue = "pulsar-operator"
)

// Labels returns the standard label set applied to every object owned by a
// Pulsar component reconciler. It is a superset of SelectorLabels: on top of
// the stable selector subset it adds app.kubernetes.io/managed-by, a
// descriptive key that is safe to change over time. Because of that it must
// never itself be used as a pod/label selector — use SelectorLabels there.
func Labels(clusterName, component string) map[string]string {
	labels := SelectorLabels(clusterName, component)
	labels[labelManagedBy] = managedByValue
	return labels
}

// SelectorLabels returns the stable label subset — name, instance, and
// component — safe to use as an immutable pod/label selector. Unlike
// Labels, it must never gain additional keys: a selector is immutable once
// a StatefulSet/Deployment is created, so widening it would orphan
// existing pods.
func SelectorLabels(clusterName, component string) map[string]string {
	return map[string]string{
		labelName:      nameValue,
		labelInstance:  clusterName,
		labelComponent: component,
	}
}
