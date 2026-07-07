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

package cluster

import (
	"testing"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

// TestResolveAutoscalerHwmPercent and TestResolveAutoscalerPeriodSeconds cover
// the bookie autoscaler's hysteresis/polling config defaulting: every
// envtest spec in bookkeeper_autoscaler_controller_test.go always leaves
// DiskUsageToleranceHwm/PeriodSeconds unset, so the "explicit value wins"
// branch of each resolver was previously untested.
func TestResolveAutoscalerHwmPercent(t *testing.T) {
	tests := []struct {
		name       string
		autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec
		want       int32
	}{
		{name: testCaseUnsetFallsBackToDefault, autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{}, want: defaultAutoscalerHwmPercent},
		{
			name:       testCaseExplicitValueWins,
			autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{DiskUsageToleranceHwm: int32Ptr(80)},
			want:       80,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveAutoscalerHwmPercent(tt.autoscaler); got != tt.want {
				t.Errorf("resolveAutoscalerHwmPercent() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestResolveAutoscalerPeriodSeconds(t *testing.T) {
	tests := []struct {
		name       string
		autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec
		want       int32
	}{
		{name: testCaseUnsetFallsBackToDefault, autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{}, want: defaultAutoscalerPeriodSecs},
		{
			name:       testCaseExplicitValueWins,
			autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{PeriodSeconds: int32Ptr(45)},
			want:       45,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveAutoscalerPeriodSeconds(tt.autoscaler); got != tt.want {
				t.Errorf("resolveAutoscalerPeriodSeconds() = %d, want %d", got, tt.want)
			}
		})
	}
}
