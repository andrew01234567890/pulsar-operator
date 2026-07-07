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

package bookkeeper

import (
	"context"
	"errors"
	"testing"
)

// Bookie addresses reused across the table-driven cases below.
const (
	bk0 = "bk-0"
	bk1 = "bk-1"
	bk2 = "bk-2"
	bk3 = "bk-3"
	bk4 = "bk-4"
)

// connErr is a ConnectionError, modelling a bookie that is unreachable or
// unhealthy this tick (as opposed to a data-integrity error).
func connErr(addr string) error {
	return &ConnectionError{BookieAddr: addr, Err: errors.New("connection refused")}
}

// mockAdminClient is a BookieAdminClient test double keyed by bookie
// address, so table-driven tests can describe an ensemble as plain maps
// instead of standing up a live bookie or an HTTP server.
type mockAdminClient struct {
	states map[string]BookieState
	infos  map[string]BookieInfo

	// stateErrs/infoErrs inject a per-address error for that call, letting a
	// test model one flaky bookie in an otherwise healthy ensemble.
	stateErrs map[string]error
	infoErrs  map[string]error

	stateErr error
	infoErr  error
}

func (m *mockAdminClient) State(_ context.Context, addr string) (BookieState, error) {
	if m.stateErr != nil {
		return BookieState{}, m.stateErr
	}
	if err := m.stateErrs[addr]; err != nil {
		return BookieState{}, err
	}
	return m.states[addr], nil
}

func (m *mockAdminClient) Info(_ context.Context, addr string) (BookieInfo, error) {
	if m.infoErr != nil {
		return BookieInfo{}, m.infoErr
	}
	if err := m.infoErrs[addr]; err != nil {
		return BookieInfo{}, err
	}
	return m.infos[addr], nil
}

func (m *mockAdminClient) UnderReplicatedLedgerCount(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func writableBookie(usedFraction float64) (BookieState, BookieInfo) {
	const total = int64(100)
	used := int64(usedFraction * float64(total))
	return BookieState{Running: true}, BookieInfo{LedgerDisks: []BookieDiskUsage{{UsedBytes: used, TotalBytes: total}}}
}

func readOnlyBookie() BookieState {
	return BookieState{Running: true, ReadOnly: true}
}

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name   string
		params Params
		states map[string]BookieState
		infos  map[string]BookieInfo
		want   Decision
	}{
		{
			name: "deficit scales up by exactly the shortfall",
			params: Params{
				CurrentReplicas:              4,
				MinWritableBookies:           4,
				ScaleUpBy:                    1,
				ScaleUpMaxLimit:              10,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{
				bk0: 0.10, bk1: 0.10, bk2: 0.10,
			}, []string{bk3}),
			infos: mustInfos(map[string]float64{
				bk0: 0.10, bk1: 0.10, bk2: 0.10,
			}),
			want: Decision{ShouldScale: true, TargetReplicas: 5, WritableBookies: 3, Reason: ReasonWritableBookieDeficit},
		},
		{
			name: "at-risk high watermark scales up by ScaleUpBy",
			params: Params{
				CurrentReplicas:              4,
				MinWritableBookies:           3,
				ScaleUpBy:                    2,
				ScaleUpMaxLimit:              10,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{
				bk0: 0.95, bk1: 0.10, bk2: 0.10, bk3: 0.10,
			}, nil),
			infos: mustInfos(map[string]float64{
				bk0: 0.95, bk1: 0.10, bk2: 0.10, bk3: 0.10,
			}),
			want: Decision{ShouldScale: true, TargetReplicas: 6, WritableBookies: 4, Reason: ReasonDiskUsageAtHighWatermark},
		},
		{
			name: "neither deficit nor at-risk is a no-op",
			params: Params{
				CurrentReplicas:              4,
				MinWritableBookies:           3,
				ScaleUpBy:                    1,
				ScaleUpMaxLimit:              10,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{
				bk0: 0.50, bk1: 0.50, bk2: 0.50, bk3: 0.50,
			}, nil),
			infos: mustInfos(map[string]float64{
				bk0: 0.50, bk1: 0.50, bk2: 0.50, bk3: 0.50,
			}),
			want: Decision{ShouldScale: false, TargetReplicas: 4, WritableBookies: 4, Reason: ReasonStable},
		},
		{
			name: "deficit clamps to ScaleUpMaxLimit",
			params: Params{
				CurrentReplicas:              4,
				MinWritableBookies:           20,
				ScaleUpBy:                    1,
				ScaleUpMaxLimit:              6,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{bk0: 0.10}, nil),
			infos:  mustInfos(map[string]float64{bk0: 0.10}),
			// Deficit is 19 (20-1), which would ask for 23 replicas; must
			// clamp to the configured ceiling of 6, not overshoot it.
			want: Decision{ShouldScale: true, TargetReplicas: 6, WritableBookies: 1, Reason: ReasonWritableBookieDeficit},
		},
		{
			name: "disk usage exactly at the HWM percentage is at-risk (>=, not >)",
			params: Params{
				CurrentReplicas:              3,
				MinWritableBookies:           1,
				ScaleUpBy:                    1,
				ScaleUpMaxLimit:              10,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{bk0: 0.92}, nil),
			infos:  mustInfos(map[string]float64{bk0: 0.92}),
			want:   Decision{ShouldScale: true, TargetReplicas: 4, WritableBookies: 1, Reason: ReasonDiskUsageAtHighWatermark},
		},
		{
			// Regression: writable bookie count exactly equal to
			// minWritableBookies must NOT be treated as a deficit. The
			// deficit branch is `writable < min`, and an easy off-by-one
			// slip (`<=` instead of `<`) would scale up forever even when
			// the ensemble already meets its minimum, since the deficit
			// branch takes priority over the (also false, here) HWM check
			// every single tick.
			name: "regression: writable count equal to minimum is not a deficit",
			params: Params{
				CurrentReplicas:              4,
				MinWritableBookies:           4,
				ScaleUpBy:                    1,
				ScaleUpMaxLimit:              10,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{
				bk0: 0.10, bk1: 0.10, bk2: 0.10, bk3: 0.10,
			}, nil),
			infos: mustInfos(map[string]float64{
				bk0: 0.10, bk1: 0.10, bk2: 0.10, bk3: 0.10,
			}),
			want: Decision{ShouldScale: false, TargetReplicas: 4, WritableBookies: 4, Reason: ReasonStable},
		},
		{
			name: "a read-only bookie's disk usage never counts toward at-risk",
			params: Params{
				CurrentReplicas:              3,
				MinWritableBookies:           2,
				ScaleUpBy:                    1,
				ScaleUpMaxLimit:              10,
				DiskUsageToleranceHwmPercent: 92,
			},
			states: mustStates(map[string]float64{bk0: 0.10, bk1: 0.10}, []string{bk2}),
			infos:  mustInfos(map[string]float64{bk0: 0.10, bk1: 0.10}),
			want:   Decision{ShouldScale: false, TargetReplicas: 3, WritableBookies: 2, Reason: ReasonStable},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockAdminClient{states: tt.states, infos: tt.infos}
			addrs := make([]string, 0, len(tt.states))
			for addr := range tt.states {
				addrs = append(addrs, addr)
			}

			got, err := Evaluate(context.Background(), client, addrs, tt.params)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if got.ShouldScale != tt.want.ShouldScale {
				t.Errorf("ShouldScale = %v, want %v", got.ShouldScale, tt.want.ShouldScale)
			}
			if got.TargetReplicas != tt.want.TargetReplicas {
				t.Errorf("TargetReplicas = %d, want %d", got.TargetReplicas, tt.want.TargetReplicas)
			}
			if got.WritableBookies != tt.want.WritableBookies {
				t.Errorf("WritableBookies = %d, want %d", got.WritableBookies, tt.want.WritableBookies)
			}
			if got.Reason != tt.want.Reason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.want.Reason)
			}
		})
	}
}

func TestEvaluate_PropagatesClientErrors(t *testing.T) {
	client := &mockAdminClient{stateErr: errors.New("boom")}
	_, err := Evaluate(context.Background(), client, []string{bk0}, Params{MinWritableBookies: 1})
	if err == nil {
		t.Fatal("expected an error from a failing State() call, got nil")
	}
}

// One unreachable bookie must not suppress a high-watermark scale-up on the
// healthy bookies: the connection error is skipped and the at-risk signal on
// the responders still drives a scale-up.
func TestEvaluate_UnreachableBookieStillScalesUpOnHighWatermark(t *testing.T) {
	fullState, fullInfo := writableBookie(0.95) // at/above the 92% HWM
	_, lowInfo := writableBookie(0.10)

	client := &mockAdminClient{
		states:    map[string]BookieState{bk0: fullState, bk2: {Running: true}, bk3: {Running: true}},
		infos:     map[string]BookieInfo{bk0: fullInfo, bk2: lowInfo, bk3: lowInfo},
		stateErrs: map[string]error{bk1: connErr(bk1)},
	}
	params := Params{
		CurrentReplicas:              4,
		MinWritableBookies:           3,
		ScaleUpBy:                    1,
		ScaleUpMaxLimit:              10,
		DiskUsageToleranceHwmPercent: 92,
	}

	got, err := Evaluate(context.Background(), client, []string{bk0, bk1, bk2, bk3}, params)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if !got.ShouldScale || got.TargetReplicas != 5 || got.Reason != ReasonDiskUsageAtHighWatermark {
		t.Errorf("got %+v, want a HWM scale-up to 5", got)
	}
}

// A partial poll (some bookies unreachable) must never fabricate a
// writable-bookie deficit: even though only 2 writable bookies were observed
// against a minimum of 3, the incomplete poll makes the deficit branch
// unsafe, so the tick is a no-op rather than a permanent phantom scale-up.
func TestEvaluate_IncompletePollSuppressesDeficit(t *testing.T) {
	_, lowInfo := writableBookie(0.10)
	client := &mockAdminClient{
		states: map[string]BookieState{
			bk1: readOnlyBookie(), bk2: readOnlyBookie(),
			bk3: {Running: true}, bk4: {Running: true},
		},
		infos:     map[string]BookieInfo{bk3: lowInfo, bk4: lowInfo},
		stateErrs: map[string]error{bk0: connErr(bk0)},
	}
	params := Params{
		CurrentReplicas:              5,
		MinWritableBookies:           3,
		ScaleUpBy:                    1,
		ScaleUpMaxLimit:              10,
		DiskUsageToleranceHwmPercent: 92,
	}

	got, err := Evaluate(context.Background(), client, []string{bk0, bk1, bk2, bk3, bk4}, params)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if got.ShouldScale {
		t.Errorf("expected no scale-up from an incomplete poll, got %+v", got)
	}
	if got.WritableBookies != 2 {
		t.Errorf("WritableBookies = %d, want 2", got.WritableBookies)
	}
}

// When too few bookies respond to safely decide (below the responder quorum),
// the tick fails with an error rather than acting on a sliver of the
// ensemble.
func TestEvaluate_TooFewRespondersFailsTick(t *testing.T) {
	client := &mockAdminClient{
		states:    map[string]BookieState{bk2: {Running: true}, bk3: {Running: true}},
		infos:     map[string]BookieInfo{},
		stateErrs: map[string]error{bk0: connErr(bk0), bk1: connErr(bk1)},
	}
	params := Params{
		CurrentReplicas:              4,
		MinWritableBookies:           3,
		ScaleUpBy:                    1,
		ScaleUpMaxLimit:              10,
		DiskUsageToleranceHwmPercent: 92,
	}

	_, err := Evaluate(context.Background(), client, []string{bk0, bk1, bk2, bk3}, params)
	if err == nil {
		t.Fatal("expected an error when only 2 of 4 bookies respond, got nil")
	}
}

// A data-integrity error (a corrupt reading, not a ConnectionError) must fail
// the whole tick, so a misread can never be silently skipped and let a
// scale-up proceed on incomplete data.
func TestEvaluate_DataIntegrityErrorFailsTick(t *testing.T) {
	client := &mockAdminClient{
		states:   map[string]BookieState{bk0: {Running: true}},
		infoErrs: map[string]error{bk0: errors.New("bookie bk-0 reported non-positive totalSpace 0")},
	}
	params := Params{
		CurrentReplicas:              3,
		MinWritableBookies:           1,
		ScaleUpBy:                    1,
		ScaleUpMaxLimit:              10,
		DiskUsageToleranceHwmPercent: 92,
	}

	_, err := Evaluate(context.Background(), client, []string{bk0}, params)
	if err == nil {
		t.Fatal("expected a data-integrity Info error to fail the tick, got nil")
	}
	if IsConnectionError(err) {
		t.Errorf("data-integrity error must not be classified as a ConnectionError: %v", err)
	}
}

func TestBookieDiskUsage_Fraction(t *testing.T) {
	tests := []struct {
		name string
		d    BookieDiskUsage
		want float64
	}{
		{"normal usage", BookieDiskUsage{UsedBytes: 50, TotalBytes: 100}, 0.5},
		{"zero total is treated as fully used", BookieDiskUsage{UsedBytes: 0, TotalBytes: 0}, 1},
		{"negative total is treated as fully used", BookieDiskUsage{UsedBytes: 0, TotalBytes: -1}, 1},
		{"negative used clamps to zero", BookieDiskUsage{UsedBytes: -5, TotalBytes: 100}, 0},
		{"used above total clamps to total", BookieDiskUsage{UsedBytes: 150, TotalBytes: 100}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.Fraction(); got != tt.want {
				t.Errorf("Fraction() = %v, want %v", got, tt.want)
			}
		})
	}
}

// mustStates builds a states map where every key in usedFractions is a
// writable bookie at that disk-usage fraction, and every entry in
// readOnlyAddrs is a writable-pod-but-read-only-service bookie (running,
// but not accepting writes).
func mustStates(usedFractions map[string]float64, readOnlyAddrs []string) map[string]BookieState {
	states := make(map[string]BookieState, len(usedFractions)+len(readOnlyAddrs))
	for addr := range usedFractions {
		state, _ := writableBookie(0)
		states[addr] = state
	}
	for _, addr := range readOnlyAddrs {
		states[addr] = readOnlyBookie()
	}
	return states
}

func mustInfos(usedFractions map[string]float64) map[string]BookieInfo {
	infos := make(map[string]BookieInfo, len(usedFractions))
	for addr, frac := range usedFractions {
		_, info := writableBookie(frac)
		infos[addr] = info
	}
	return infos
}
