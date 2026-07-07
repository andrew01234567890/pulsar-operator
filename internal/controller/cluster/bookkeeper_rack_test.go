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
	"context"
	"errors"
	"maps"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

// Shared rack-sync test fixtures, reused across this file and
// bookkeeper_rack_controller_test.go (same package).
const (
	testZoneA = "us-east-1a"
	testZoneB = "us-east-1b"
	testRackA = "/" + testZoneA
	testRackB = "/" + testZoneB

	testBookieID0 = "bk-0.bk.default:3181"
	testBookieID1 = "bk-1.bk.default:3181"
)

func TestBookieID(t *testing.T) {
	bk := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: "my-bk", Namespace: "pulsar-ns"},
	}

	got := bookieID(bk, "my-bk-2")
	want := "my-bk-2.my-bk.pulsar-ns:3181"
	if got != want {
		t.Errorf("bookieID() = %q, want %q", got, want)
	}
}

func TestRackForZone(t *testing.T) {
	tests := []struct {
		zone string
		want string
	}{
		{zone: testZoneA, want: testRackA},
		{zone: testZoneB, want: testRackB},
		{zone: "eu-west-1", want: "/eu-west-1"},
	}
	for _, tt := range tests {
		if got := rackForZone(tt.zone); got != tt.want {
			t.Errorf("rackForZone(%q) = %q, want %q", tt.zone, got, tt.want)
		}
	}
}

// mockRackSetter records every SetBookieRack call it receives, optionally
// failing calls for specific bookie IDs, so tests can assert exactly which
// bookies were (or were not) touched.
type mockRackSetter struct {
	calls   []rackTarget
	failFor map[string]error
}

func (m *mockRackSetter) SetBookieRack(_ context.Context, bookieID, rack string) error {
	m.calls = append(m.calls, rackTarget{bookieID: bookieID, rack: rack})
	if m.failFor != nil {
		if err, ok := m.failFor[bookieID]; ok {
			return err
		}
	}
	return nil
}

func TestApplyRackMapping_SkipsUnchangedBookies(t *testing.T) {
	setter := &mockRackSetter{}
	targets := []rackTarget{
		{bookieID: testBookieID0, rack: testRackA},
		{bookieID: testBookieID1, rack: testRackB},
	}
	previouslyApplied := map[string]string{
		testBookieID0: testRackA, // already correct - must not be re-applied
	}

	applied, synced, failed := applyRackMapping(context.Background(), setter, targets, previouslyApplied)

	if len(setter.calls) != 1 || setter.calls[0].bookieID != testBookieID1 {
		t.Fatalf("SetBookieRack calls = %+v, want exactly one call for bk-1 (bk-0 is unchanged)", setter.calls)
	}
	if synced != 1 {
		t.Errorf("synced = %d, want 1", synced)
	}
	if len(failed) != 0 {
		t.Errorf("failed = %v, want none", failed)
	}
	want := map[string]string{testBookieID0: testRackA, testBookieID1: testRackB}
	if !maps.Equal(applied, want) {
		t.Errorf("applied = %v, want %v", applied, want)
	}
}

func TestApplyRackMapping_ReSyncsWhenRackChanged(t *testing.T) {
	setter := &mockRackSetter{}
	targets := []rackTarget{{bookieID: testBookieID0, rack: testRackB}}
	previouslyApplied := map[string]string{testBookieID0: testRackA} // zone moved

	applied, synced, failed := applyRackMapping(context.Background(), setter, targets, previouslyApplied)

	if len(setter.calls) != 1 {
		t.Fatalf("SetBookieRack calls = %+v, want exactly one call (rack changed)", setter.calls)
	}
	if setter.calls[0].rack != testRackB {
		t.Errorf("SetBookieRack rack = %q, want %q", setter.calls[0].rack, testRackB)
	}
	if synced != 1 || len(failed) != 0 {
		t.Errorf("synced=%d failed=%v, want synced=1 failed=none", synced, failed)
	}
	if applied[testBookieID0] != testRackB {
		t.Errorf("applied[bk-0] = %q, want the new rack", applied[testBookieID0])
	}
}

func TestApplyRackMapping_NoPreviousState_SyncsEveryTarget(t *testing.T) {
	setter := &mockRackSetter{}
	targets := []rackTarget{
		{bookieID: testBookieID0, rack: testRackA},
		{bookieID: testBookieID1, rack: testRackB},
	}

	_, synced, failed := applyRackMapping(context.Background(), setter, targets, nil)

	if len(setter.calls) != 2 {
		t.Fatalf("SetBookieRack calls = %+v, want 2 (no prior cache, first sync)", setter.calls)
	}
	if synced != 2 || len(failed) != 0 {
		t.Errorf("synced=%d failed=%v, want synced=2 failed=none", synced, failed)
	}
}

// TestApplyRackMapping_FailurePerBookie_SkipsAndContinues is the resilience
// regression: one bookie's SetBookieRack failure must not stop the rest of
// the ensemble from being synced, and the failed bookie's prior cache entry
// (if any) must be carried forward so the next tick retries it instead of
// wrongly recording it as converged.
func TestApplyRackMapping_FailurePerBookie_SkipsAndContinues(t *testing.T) {
	setter := &mockRackSetter{failFor: map[string]error{testBookieID0: errors.New("pod exec failed")}}
	targets := []rackTarget{
		{bookieID: testBookieID0, rack: testRackB}, // will fail
		{bookieID: testBookieID1, rack: testRackB}, // will succeed
	}
	previouslyApplied := map[string]string{testBookieID0: testRackA}

	applied, synced, failed := applyRackMapping(context.Background(), setter, targets, previouslyApplied)

	if len(setter.calls) != 2 {
		t.Fatalf("SetBookieRack calls = %+v, want 2 (both attempted)", setter.calls)
	}
	if synced != 1 {
		t.Errorf("synced = %d, want 1 (only bk-1 succeeded)", synced)
	}
	if len(failed) != 1 || failed[0] != testBookieID0 {
		t.Errorf("failed = %v, want [%s]", failed, testBookieID0)
	}
	// bk-0 kept its stale, pre-failure rack so the next tick retries it
	// rather than treating the failed write as if it had converged.
	if applied[testBookieID0] != testRackA {
		t.Errorf("applied[bk-0] = %q, want stale prior rack %q preserved after failure", applied[testBookieID0], testRackA)
	}
	if applied[testBookieID1] != testRackB {
		t.Errorf("applied[bk-1] = %q, want the newly synced rack", applied[testBookieID1])
	}
}

func TestApplyRackMapping_DropsStaleBookiesNoLongerDesired(t *testing.T) {
	setter := &mockRackSetter{}
	// bk-0 has scaled away: it is no longer in targets.
	targets := []rackTarget{{bookieID: testBookieID1, rack: testRackB}}
	previouslyApplied := map[string]string{
		testBookieID0: testRackA,
		testBookieID1: testRackB,
	}

	applied, _, _ := applyRackMapping(context.Background(), setter, targets, previouslyApplied)

	if _, ok := applied[testBookieID0]; ok {
		t.Errorf("applied = %v, want bk-0 dropped (no longer a desired target)", applied)
	}
	if len(setter.calls) != 0 {
		t.Errorf("SetBookieRack calls = %+v, want none (bk-1 unchanged, bk-0 simply dropped)", setter.calls)
	}
}

func TestResolveRackPeriodSeconds(t *testing.T) {
	tests := []struct {
		name string
		cfg  *clusterv1alpha1.BookKeeperAutoRackConfig
		want int32
	}{
		{name: testCaseUnsetFallsBackToDefault, cfg: &clusterv1alpha1.BookKeeperAutoRackConfig{}, want: defaultRackSyncPeriodSeconds},
		{name: testCaseExplicitValueWins, cfg: &clusterv1alpha1.BookKeeperAutoRackConfig{PeriodSeconds: int32Ptr(30)}, want: 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveRackPeriodSeconds(tt.cfg); got != tt.want {
				t.Errorf("resolveRackPeriodSeconds() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPulsarClusterOwnerName(t *testing.T) {
	t.Run("no owner references", func(t *testing.T) {
		bk := &clusterv1alpha1.BookKeeper{}
		if _, ok := pulsarClusterOwnerName(bk); ok {
			t.Errorf("pulsarClusterOwnerName() ok = true, want false for a standalone BookKeeper")
		}
	})

	t.Run("owned by a PulsarCluster", func(t *testing.T) {
		bk := &clusterv1alpha1.BookKeeper{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{{Kind: "PulsarCluster", Name: "my-cluster"}},
			},
		}
		name, ok := pulsarClusterOwnerName(bk)
		if !ok || name != "my-cluster" {
			t.Errorf("pulsarClusterOwnerName() = (%q, %v), want (\"my-cluster\", true)", name, ok)
		}
	})

	t.Run("owned by something else", func(t *testing.T) {
		bk := &clusterv1alpha1.BookKeeper{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{{Kind: "SomeOtherKind", Name: "irrelevant"}},
			},
		}
		if _, ok := pulsarClusterOwnerName(bk); ok {
			t.Errorf("pulsarClusterOwnerName() ok = true, want false")
		}
	})
}
