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

import "testing"

// testBroker0 names the first broker pod across this package's tests
// (decision_test.go, pulsarloadreport_test.go, k8smetrics_test.go); most
// single-pod test cases only ever need this one name.
const (
	testBroker0 = "broker-0"
	testBroker1 = "broker-1"
	testBroker2 = "broker-2"
)

func defaultParams() Params {
	return Params{
		LowerCPUPercent:  30,
		HigherCPUPercent: 80,
		ScaleUpBy:        1,
		ScaleDownBy:      1,
		MinReplicas:      2,
		MaxReplicas:      5,
		CurrentReplicas:  3,
	}
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name          string
		cpu           map[string]int32
		params        Params
		wantDirection Direction
		wantTarget    int32
		wantReasonSet string // when non-empty, exact Reason match
	}{
		{
			name:          "all brokers above higher threshold scale up",
			cpu:           map[string]int32{testBroker0: 85, testBroker1: 90, testBroker2: 99},
			params:        defaultParams(),
			wantDirection: ScaleUp,
			wantTarget:    4,
			wantReasonSet: string(ScaleUp),
		},
		{
			name:          "all brokers below lower threshold scale down",
			cpu:           map[string]int32{testBroker0: 10, testBroker1: 5, testBroker2: 29},
			params:        defaultParams(),
			wantDirection: ScaleDown,
			wantTarget:    2,
			wantReasonSet: string(ScaleDown),
		},
		{
			name:          "mixed hot and cold brokers is a no-op",
			cpu:           map[string]int32{testBroker0: 95, testBroker1: 5, testBroker2: 50},
			params:        defaultParams(),
			wantDirection: NoOp,
			wantReasonSet: ReasonMixedSignals,
		},
		{
			name:          "single hot broker among otherwise-normal brokers never scales alone",
			cpu:           map[string]int32{testBroker0: 95, testBroker1: 50, testBroker2: 55},
			params:        defaultParams(),
			wantDirection: NoOp,
			wantReasonSet: ReasonMixedSignals,
		},
		{
			name:          "single cold broker among otherwise-normal brokers never scales alone",
			cpu:           map[string]int32{testBroker0: 5, testBroker1: 50, testBroker2: 55},
			params:        defaultParams(),
			wantDirection: NoOp,
			wantReasonSet: ReasonMixedSignals,
		},
		{
			name:          "every broker sitting between thresholds is a no-op",
			cpu:           map[string]int32{testBroker0: 40, testBroker1: 50, testBroker2: 60},
			params:        defaultParams(),
			wantDirection: NoOp,
			wantReasonSet: ReasonMixedSignals,
		},
		{
			name:          "no readings at all is a no-op",
			cpu:           map[string]int32{},
			params:        defaultParams(),
			wantDirection: NoOp,
			wantReasonSet: ReasonNoMetrics,
		},
		{
			// Regression: a naive off-by-one (<=/>= instead of </>) would
			// treat exactly-at-threshold as hot/cold. KAAP's own algorithm
			// treats the boundary as "in between" (a no-op), and so must we.
			name:          "regression: exactly at either threshold is a no-op, not a scale",
			cpu:           map[string]int32{testBroker0: 30, testBroker1: 80},
			params:        defaultParams(),
			wantDirection: NoOp,
			wantReasonSet: ReasonMixedSignals,
		},
		{
			// Regression: the unanimous vote must not implicitly require a
			// quorum of brokers - a single-broker cluster still scales.
			name:          "regression: a single-broker cluster can still scale up unanimously",
			cpu:           map[string]int32{testBroker0: 95},
			params:        defaultParams(),
			wantDirection: ScaleUp,
			wantTarget:    4,
			wantReasonSet: string(ScaleUp),
		},
		{
			name: "scale up clamps to max and no-ops once already there",
			cpu:  map[string]int32{testBroker0: 95, testBroker1: 95},
			params: Params{
				LowerCPUPercent: 30, HigherCPUPercent: 80,
				ScaleUpBy: 1, ScaleDownBy: 1,
				MinReplicas: 2, MaxReplicas: 5, CurrentReplicas: 5,
			},
			wantDirection: NoOp,
			wantReasonSet: ReasonAtReplicaBound,
		},
		{
			name: "scale up clamps target to max when the step would overshoot",
			cpu:  map[string]int32{testBroker0: 95, testBroker1: 95},
			params: Params{
				LowerCPUPercent: 30, HigherCPUPercent: 80,
				ScaleUpBy: 3, ScaleDownBy: 1,
				MinReplicas: 2, MaxReplicas: 5, CurrentReplicas: 4,
			},
			wantDirection: ScaleUp,
			wantTarget:    5,
			wantReasonSet: string(ScaleUp),
		},
		{
			name: "scale down clamps to min and no-ops once already there",
			cpu:  map[string]int32{testBroker0: 5, testBroker1: 5},
			params: Params{
				LowerCPUPercent: 30, HigherCPUPercent: 80,
				ScaleUpBy: 1, ScaleDownBy: 1,
				MinReplicas: 2, MaxReplicas: 5, CurrentReplicas: 2,
			},
			wantDirection: NoOp,
			wantReasonSet: ReasonAtReplicaBound,
		},
		{
			name: "scale down clamps target to min when the step would undershoot",
			cpu:  map[string]int32{testBroker0: 5, testBroker1: 5},
			params: Params{
				LowerCPUPercent: 30, HigherCPUPercent: 80,
				ScaleUpBy: 1, ScaleDownBy: 3,
				MinReplicas: 2, MaxReplicas: 5, CurrentReplicas: 4,
			},
			wantDirection: ScaleDown,
			wantTarget:    2,
			wantReasonSet: string(ScaleDown),
		},
		{
			name: "max==0 means unbounded scale up",
			cpu:  map[string]int32{testBroker0: 95},
			params: Params{
				LowerCPUPercent: 30, HigherCPUPercent: 80,
				ScaleUpBy: 1, ScaleDownBy: 1,
				MinReplicas: 2, MaxReplicas: 0, CurrentReplicas: 100,
			},
			wantDirection: ScaleUp,
			wantTarget:    101,
			wantReasonSet: string(ScaleUp),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(tt.cpu, tt.params)

			if got.Direction != tt.wantDirection {
				t.Fatalf("Decide() direction = %v, want %v (message: %q)", got.Direction, tt.wantDirection, got.Message)
			}
			if tt.wantDirection != NoOp && got.TargetReplicas != tt.wantTarget {
				t.Errorf("Decide() target = %d, want %d", got.TargetReplicas, tt.wantTarget)
			}
			if tt.wantReasonSet != "" && got.Reason != tt.wantReasonSet {
				t.Errorf("Decide() reason = %q, want %q", got.Reason, tt.wantReasonSet)
			}
			if got.Message == "" {
				t.Error("Decide() message must never be empty - it feeds a status condition/Event")
			}
		})
	}
}
