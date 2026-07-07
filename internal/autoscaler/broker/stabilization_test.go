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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func readyPod(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func notReadyPod(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}
}

func TestPodsStable(t *testing.T) {
	tests := []struct {
		name             string
		pods             []corev1.Pod
		expectedReplicas int32
		want             bool
	}{
		{
			name:             "all ready and count matches",
			pods:             []corev1.Pod{readyPod("b-0"), readyPod("b-1")},
			expectedReplicas: 2,
			want:             true,
		},
		{
			name:             "fewer pods than expected replicas",
			pods:             []corev1.Pod{readyPod("b-0")},
			expectedReplicas: 2,
			want:             false,
		},
		{
			name:             "more pods than expected replicas",
			pods:             []corev1.Pod{readyPod("b-0"), readyPod("b-1"), readyPod("b-2")},
			expectedReplicas: 2,
			want:             false,
		},
		{
			name:             "one pod not ready",
			pods:             []corev1.Pod{readyPod("b-0"), notReadyPod("b-1")},
			expectedReplicas: 2,
			want:             false,
		},
		{
			name:             "pod missing a PodReady condition entirely",
			pods:             []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "b-0"}}},
			expectedReplicas: 1,
			want:             false,
		},
		{
			name:             "zero expected replicas with no pods",
			pods:             nil,
			expectedReplicas: 0,
			want:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PodsStable(tt.pods, tt.expectedReplicas); got != tt.want {
				t.Errorf("PodsStable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStabilizationElapsed(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		lastScaleTime *metav1.Time
		windowSeconds int32
		want          bool
	}{
		{
			name:          "no prior scale always elapses",
			lastScaleTime: nil,
			windowSeconds: 300,
			want:          true,
		},
		{
			name:          "well within the window blocks",
			lastScaleTime: ptrTime(now.Add(-1 * time.Minute)),
			windowSeconds: 300,
			want:          false,
		},
		{
			name:          "past the window elapses",
			lastScaleTime: ptrTime(now.Add(-301 * time.Second)),
			windowSeconds: 300,
			want:          true,
		},
		{
			name:          "exactly at the window boundary elapses",
			lastScaleTime: ptrTime(now.Add(-300 * time.Second)),
			windowSeconds: 300,
			want:          true,
		},
		{
			name:          "zero-second window always elapses",
			lastScaleTime: ptrTime(now),
			windowSeconds: 0,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StabilizationElapsed(tt.lastScaleTime, tt.windowSeconds, now); got != tt.want {
				t.Errorf("StabilizationElapsed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}
