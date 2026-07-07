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

package broker

import (
	"context"
	"errors"
	"fmt"
	"math"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// podMetricsGVK addresses the metrics-server aggregated API directly via an
// unstructured Get rather than pulling in the k8s.io/metrics clientset - the
// operator's controller-runtime client already resolves arbitrary GVKs
// through the cluster's discovery/RESTMapper, so no extra dependency (or
// scheme registration) is needed for this one read-only lookup.
var podMetricsGVK = schema.GroupVersionKind{Group: "metrics.k8s.io", Version: "v1beta1", Kind: "PodMetrics"}

// K8sMetricsClient is the ResourcesUsageSource=K8SMetrics LoadClient: it
// reads raw container CPU usage from the metrics-server aggregated API and
// expresses it as a percent of the pod's own CPU limit. Unlike
// PulsarLoadReportClient this says nothing about what Pulsar's load manager
// considers "hot" - it is the fallback for clusters that don't want to rely
// on the broker's own admin API.
type K8sMetricsClient struct {
	Client client.Client
}

// CPUPercentByBroker requires every broker container to declare a CPU
// limit: without one there is no denominator to compute a percentage
// against, so that pod is reported as an error rather than silently
// guessing.
func (c *K8sMetricsClient) CPUPercentByBroker(ctx context.Context, pods []corev1.Pod) (map[string]int32, error) {
	result := make(map[string]int32, len(pods))
	var errs []error
	for _, pod := range pods {
		percent, err := c.fetchOne(ctx, pod)
		if err != nil {
			errs = append(errs, fmt.Errorf("broker pod %s: %w", pod.Name, err))
			continue
		}
		result[pod.Name] = percent
	}

	return result, errors.Join(errs...)
}

func (c *K8sMetricsClient) fetchOne(ctx context.Context, pod corev1.Pod) (int32, error) {
	limitMilli := cpuLimitMillis(pod)
	if limitMilli <= 0 {
		return 0, errors.New("no CPU limit set on any container, can't compute a CPU percentage")
	}

	metrics := &unstructured.Unstructured{}
	metrics.SetGroupVersionKind(podMetricsGVK)
	key := types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}
	if err := c.Client.Get(ctx, key, metrics); err != nil {
		return 0, fmt.Errorf("fetching PodMetrics: %w", err)
	}

	usageMilli, err := containerCPUUsageMillis(metrics)
	if err != nil {
		return 0, err
	}

	percent := math.Round(float64(usageMilli) / float64(limitMilli) * 100)
	if percent > 100 {
		percent = 100
	}
	return int32(percent), nil
}

func cpuLimitMillis(pod corev1.Pod) int64 {
	var total int64
	for _, c := range pod.Spec.Containers {
		if limit, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
			total += limit.MilliValue()
		}
	}
	return total
}

func containerCPUUsageMillis(metrics *unstructured.Unstructured) (int64, error) {
	containers, found, err := unstructured.NestedSlice(metrics.Object, "containers")
	if err != nil {
		return 0, fmt.Errorf("reading PodMetrics.containers: %w", err)
	}
	if !found || len(containers) == 0 {
		return 0, errors.New("PodMetrics reported no containers")
	}

	var total int64
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		cpuStr, found, err := unstructured.NestedString(container, "usage", "cpu")
		if err != nil {
			return 0, fmt.Errorf("reading PodMetrics.containers[].usage.cpu: %w", err)
		}
		if !found {
			continue
		}
		quantity, err := resource.ParseQuantity(cpuStr)
		if err != nil {
			return 0, fmt.Errorf("parsing CPU usage quantity %q: %w", cpuStr, err)
		}
		total += quantity.MilliValue()
	}
	return total, nil
}
