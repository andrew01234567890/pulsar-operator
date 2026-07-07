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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodsStable reports whether exactly expectedReplicas broker pods exist and
// every one of them currently reports PodReady=True. Evaluating CPU signals
// against a StatefulSet that hasn't converged (too few/many pods, or a pod
// still starting) would read a rollout as load, so the autoscaler must
// refuse to act until this is true.
func PodsStable(pods []corev1.Pod, expectedReplicas int32) bool {
	if int32(len(pods)) != expectedReplicas {
		return false
	}
	for _, pod := range pods {
		if !isPodReady(pod) {
			return false
		}
	}
	return true
}

func isPodReady(pod corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// StabilizationElapsed reports whether at least windowSeconds have passed
// since lastScaleTime. A nil lastScaleTime - no scaling event has ever
// happened for this Broker - always elapses immediately, so the very first
// scaling decision isn't blocked forever waiting for a scale that never
// occurred.
func StabilizationElapsed(lastScaleTime *metav1.Time, windowSeconds int32, now time.Time) bool {
	if lastScaleTime == nil {
		return true
	}
	return !now.Before(lastScaleTime.Add(time.Duration(windowSeconds) * time.Second))
}
