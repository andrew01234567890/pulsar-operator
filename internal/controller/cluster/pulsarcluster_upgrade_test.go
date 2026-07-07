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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testHashDesired/testHashStored/testHashApplied/testHashDrift stand in for
// distinct spec-hash values across the orderedChildDecision table.
const (
	testHashDesired = "desired"
	testHashStored  = "stored"
	testHashApplied = "applied"
	testHashDrift   = "drift"
)

func TestOrderedChildDecision(t *testing.T) {
	cases := []struct {
		name              string
		storedDesiredHash string
		desiredHash       string
		storedAppliedHash string
		actualHash        string
		upstreamSettled   bool
		want              childAction
	}{
		{
			name:              "steady state: desired unchanged and live matches applied is a no-op",
			storedDesiredHash: testHashDesired,
			desiredHash:       testHashDesired,
			storedAppliedHash: testHashApplied,
			actualHash:        testHashApplied,
			upstreamSettled:   true,
			want:              childNoop,
		},
		{
			name:              "genuine roll with upstream settled applies immediately",
			storedDesiredHash: testHashStored,
			desiredHash:       testHashDesired,
			storedAppliedHash: testHashApplied,
			actualHash:        testHashApplied,
			upstreamSettled:   true,
			want:              childApplyRoll,
		},
		{
			name:              "genuine roll with upstream NOT settled is deferred",
			storedDesiredHash: testHashStored,
			desiredHash:       testHashDesired,
			storedAppliedHash: testHashApplied,
			actualHash:        testHashApplied,
			upstreamSettled:   false,
			want:              childDeferRoll,
		},
		{
			// F2: no roll pending (desired unchanged) but the child's LIVE spec
			// diverged from what we last stored. Corrected UNGATED, so even an
			// unsettled upstream must not block it.
			name:              "no roll pending but live spec drifted is drift-corrected ungated",
			storedDesiredHash: testHashDesired,
			desiredHash:       testHashDesired,
			storedAppliedHash: testHashApplied,
			actualHash:        testHashDrift,
			upstreamSettled:   false,
			want:              childDriftCorrect,
		},
		{
			// A genuine roll takes precedence over drift: both the desired and
			// the live spec differ from their baselines, but it is still ordered.
			name:              "roll pending and drifted, upstream unsettled, still deferred (roll wins)",
			storedDesiredHash: testHashStored,
			desiredHash:       testHashDesired,
			storedAppliedHash: testHashApplied,
			actualHash:        testHashDrift,
			upstreamSettled:   false,
			want:              childDeferRoll,
		},
		{
			name:              "never-applied child (empty stored desired hash) is a genuine roll",
			storedDesiredHash: "",
			desiredHash:       testHashDesired,
			storedAppliedHash: "",
			actualHash:        testHashApplied,
			upstreamSettled:   true,
			want:              childApplyRoll,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := orderedChildDecision(tc.storedDesiredHash, tc.desiredHash, tc.storedAppliedHash, tc.actualHash, tc.upstreamSettled)
			if got != tc.want {
				t.Errorf("orderedChildDecision(%q, %q, %q, %q, %v) = %v, want %v",
					tc.storedDesiredHash, tc.desiredHash, tc.storedAppliedHash, tc.actualHash, tc.upstreamSettled, got, tc.want)
			}
		})
	}
}

func TestTierSettled(t *testing.T) {
	cases := []struct {
		name     string
		ready    bool
		deferred bool
		want     bool
	}{
		{"ready and applied (not deferred) is settled", true, false, true},
		{"ready but sitting on a deferred update is not settled", true, true, false},
		{"not ready is never settled", false, false, false},
		{"not ready and deferred is not settled", false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tierSettled(tc.ready, tc.deferred); got != tc.want {
				t.Errorf("tierSettled(%v, %v) = %v, want %v", tc.ready, tc.deferred, got, tc.want)
			}
		})
	}
}

func TestSpecHash(t *testing.T) {
	type sample struct {
		A string
		B map[string]string
	}

	h1, err := specHash(sample{A: "x", B: map[string]string{"k1": "v1", "k2": "v2"}})
	if err != nil {
		t.Fatalf("specHash() error = %v", err)
	}

	// encoding/json sorts map keys, so a spec built with the map populated in
	// a different order must still hash identically: Go's map iteration
	// order is randomized, so the hash must not depend on it.
	h2, err := specHash(sample{A: "x", B: map[string]string{"k2": "v2", "k1": "v1"}})
	if err != nil {
		t.Fatalf("specHash() error = %v", err)
	}
	if h1 != h2 {
		t.Errorf("specHash() = %q and %q, want equal regardless of map key insertion order", h1, h2)
	}

	// A pointer must hash identically to the value it points at, since desired
	// specs are passed by pointer and live specs by value.
	v := sample{A: "x", B: map[string]string{"k1": "v1", "k2": "v2"}}
	hp, err := specHash(&v)
	if err != nil {
		t.Fatalf("specHash() error = %v", err)
	}
	if hp != h1 {
		t.Errorf("specHash(&v) = %q, specHash(v) = %q, want equal", hp, h1)
	}

	h3, err := specHash(sample{A: "y", B: map[string]string{"k1": "v1", "k2": "v2"}})
	if err != nil {
		t.Fatalf("specHash() error = %v", err)
	}
	if h1 == h3 {
		t.Errorf("specHash() of two different specs must differ, both = %q", h1)
	}
}

// deferredTier builds a present tier whose spec roll is deferred behind an
// unsettled upstream tier - the shape the Upgrading condition names.
func deferredTier(t tierName) tierState {
	return tierState{tier: t, present: true, outcome: tierOutcome{deferred: true}}
}

func TestRollingOutCondition(t *testing.T) {
	const generation = int64(4)

	t.Run("no tier rolling reports Settled", func(t *testing.T) {
		// Fresh-install shape: children present but NOT rolling (they are
		// merely not-yet-Ready). Upgrading must be False/Settled - F1.
		got := rollingOutCondition(generation, []tierState{
			{tier: tierOxia, present: true},
			{tier: tierBookKeeper, present: true},
			{tier: tierBroker, present: false},
		})
		if got.Type != conditionTypeUpgrading {
			t.Errorf("Type = %q, want %q", got.Type, conditionTypeUpgrading)
		}
		if got.Status != metav1.ConditionFalse {
			t.Errorf("Status = %q, want False", got.Status)
		}
		if got.Reason != reasonUpgradeSettled {
			t.Errorf("Reason = %q, want %q", got.Reason, reasonUpgradeSettled)
		}
	})

	t.Run("names the most-upstream rolling tier", func(t *testing.T) {
		got := rollingOutCondition(generation, []tierState{
			{tier: tierOxia, present: true},
			deferredTier(tierBookKeeper),
			deferredTier(tierBroker),
		})
		if got.Status != metav1.ConditionTrue {
			t.Errorf("Status = %q, want True", got.Status)
		}
		if got.Reason != tierBookKeeper.upgradeReason() {
			t.Errorf("Reason = %q, want %q (the upstream-most rolling tier, not broker)", got.Reason, tierBookKeeper.upgradeReason())
		}
	})

	t.Run("AutoRecovery rolling on the oxia gate is named", func(t *testing.T) {
		// F3: AutoRecovery is a first-class tier between bookkeeper and broker.
		got := rollingOutCondition(generation, []tierState{
			{tier: tierOxia, present: true},
			{tier: tierBookKeeper, present: true},
			deferredTier(tierAutoRecovery),
			{tier: tierBroker, present: true},
		})
		if got.Reason != tierAutoRecovery.upgradeReason() {
			t.Errorf("Reason = %q, want %q", got.Reason, tierAutoRecovery.upgradeReason())
		}
	})

	t.Run("an absent tier is skipped even if its outcome looks rolling", func(t *testing.T) {
		got := rollingOutCondition(generation, []tierState{
			{tier: tierOxia, present: true},
			{tier: tierProxy, present: false, outcome: tierOutcome{deferred: true}},
			deferredTier(tierFunctionsWorker),
		})
		if got.Reason != tierFunctionsWorker.upgradeReason() {
			t.Errorf("Reason = %q, want %q (skipping the absent proxy tier)", got.Reason, tierFunctionsWorker.upgradeReason())
		}
	})

	if got := rollingOutCondition(generation, nil); got.Status != metav1.ConditionFalse || got.Reason != reasonUpgradeSettled {
		t.Errorf("rollingOutCondition(nil) = %+v, want Settled/False (nothing rolling)", got)
	}
}

func TestRolloutEvent(t *testing.T) {
	appliedReason, _, appliedNote := rolloutEvent(tierBroker, true)
	if appliedReason != "RolloutStarted" {
		t.Errorf("applied reason = %q, want RolloutStarted", appliedReason)
	}
	if !strings.Contains(appliedNote, "broker") {
		t.Errorf("applied note = %q, want it to mention the tier", appliedNote)
	}

	deferredReason, _, deferredNote := rolloutEvent(tierProxy, false)
	if deferredReason != "RolloutDeferred" {
		t.Errorf("deferred reason = %q, want RolloutDeferred", deferredReason)
	}
	if !strings.Contains(deferredNote, "proxy") {
		t.Errorf("deferred note = %q, want it to mention the tier", deferredNote)
	}
}

func TestRolloutBottleneckChanged(t *testing.T) {
	rollingOxia := metav1.Condition{Status: metav1.ConditionTrue, Reason: tierOxia.upgradeReason()}
	rollingBK := metav1.Condition{Status: metav1.ConditionTrue, Reason: tierBookKeeper.upgradeReason()}
	settled := metav1.Condition{Status: metav1.ConditionFalse, Reason: reasonUpgradeSettled}

	if !rolloutBottleneckChanged(nil, rollingOxia) {
		t.Error("nil prior -> rolling should be a change")
	}
	if rolloutBottleneckChanged(&rollingOxia, rollingOxia) {
		t.Error("same bottleneck across reconciles must NOT be a change (dedup)")
	}
	if !rolloutBottleneckChanged(&rollingOxia, rollingBK) {
		t.Error("bottleneck moving oxia -> bookkeeper should be a change")
	}
	if !rolloutBottleneckChanged(&rollingOxia, settled) {
		t.Error("rolling -> settled should be a change")
	}
}
