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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// stubGetClient implements just enough of client.Client (via embedding a nil
// client.Client for every other method) to feed K8sMetricsClient a canned
// PodMetrics response without a real API server.
type stubGetClient struct {
	client.Client
	getFunc func(ctx context.Context, key client.ObjectKey, obj client.Object) error
}

func (s *stubGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	return s.getFunc(ctx, key, obj)
}

func podMetricsWithContainerCPU(cpu ...string) func(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	return func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return errors.New("expected *unstructured.Unstructured")
		}
		containers := make([]any, 0, len(cpu))
		for _, c := range cpu {
			containers = append(containers, map[string]any{
				"usage": map[string]any{"cpu": c},
			})
		}
		return unstructured.SetNestedSlice(u.Object, containers, "containers")
	}
}

func podWithCPULimit() corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testBroker0},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "broker",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("500m"),
						},
					},
				},
			},
		},
	}
}

func TestK8sMetricsClient_CPUPercentByBroker(t *testing.T) {
	t.Run("computes usage as a percent of the pod's CPU limit", func(t *testing.T) {
		stub := &stubGetClient{getFunc: podMetricsWithContainerCPU("250m")}
		c := &K8sMetricsClient{Client: stub}

		got, err := c.CPUPercentByBroker(context.Background(), []corev1.Pod{podWithCPULimit()})
		if err != nil {
			t.Fatalf("CPUPercentByBroker() error = %v", err)
		}
		if got[testBroker0] != 50 {
			t.Errorf("CPUPercentByBroker()[broker-0] = %d, want 50", got[testBroker0])
		}
	})

	t.Run("sums CPU usage across multiple containers", func(t *testing.T) {
		stub := &stubGetClient{getFunc: podMetricsWithContainerCPU("100m", "150m")}
		pod := podWithCPULimit()
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "sidecar"})
		c := &K8sMetricsClient{Client: stub}

		got, err := c.CPUPercentByBroker(context.Background(), []corev1.Pod{pod})
		if err != nil {
			t.Fatalf("CPUPercentByBroker() error = %v", err)
		}
		if got[testBroker0] != 50 {
			t.Errorf("CPUPercentByBroker()[broker-0] = %d, want 50", got[testBroker0])
		}
	})

	t.Run("a pod with no CPU limit is an error, not a guess", func(t *testing.T) {
		stub := &stubGetClient{getFunc: podMetricsWithContainerCPU("250m")}
		c := &K8sMetricsClient{Client: stub}
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: testBroker0},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "broker"}}},
		}

		got, err := c.CPUPercentByBroker(context.Background(), []corev1.Pod{pod})
		if err == nil {
			t.Fatal("CPUPercentByBroker() error = nil, want an error for a pod with no CPU limit")
		}
		if _, ok := got[testBroker0]; ok {
			t.Error("CPUPercentByBroker() should not report a percent for a pod with no CPU limit")
		}
	})

	t.Run("a metrics-server fetch failure is an error, not silently dropped", func(t *testing.T) {
		stub := &stubGetClient{getFunc: func(context.Context, client.ObjectKey, client.Object) error {
			return errors.New("metrics-server unavailable")
		}}
		c := &K8sMetricsClient{Client: stub}

		got, err := c.CPUPercentByBroker(context.Background(), []corev1.Pod{podWithCPULimit()})
		if err == nil {
			t.Fatal("CPUPercentByBroker() error = nil, want an error")
		}
		if len(got) != 0 {
			t.Errorf("CPUPercentByBroker() = %v, want empty", got)
		}
	})

	t.Run("usage above the limit clamps to 100 percent", func(t *testing.T) {
		stub := &stubGetClient{getFunc: podMetricsWithContainerCPU("900m")}
		c := &K8sMetricsClient{Client: stub}

		got, err := c.CPUPercentByBroker(context.Background(), []corev1.Pod{podWithCPULimit()})
		if err != nil {
			t.Fatalf("CPUPercentByBroker() error = %v", err)
		}
		if got[testBroker0] != 100 {
			t.Errorf("CPUPercentByBroker()[broker-0] = %d, want 100", got[testBroker0])
		}
	})
}
