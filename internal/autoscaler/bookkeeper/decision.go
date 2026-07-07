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
	"fmt"
)

// Condition reasons, used both as the returned Decision.Reason and as the
// caller's Kubernetes Condition/Event reason.
const (
	ReasonWritableBookieDeficit    = "WritableBookieDeficit"
	ReasonDiskUsageAtHighWatermark = "DiskUsageAtHighWatermark"
	ReasonStable                   = "Stable"
)

// Params configures one autoscaler tick's scale-up decision. It holds
// already-resolved values (spec pointer defaults applied by the caller), so
// this package never has to know about the BookKeeper CRD's optional
// fields/kubebuilder defaults.
type Params struct {
	// CurrentReplicas is BookKeeper.spec.replicas as currently set.
	CurrentReplicas int32

	// MinWritableBookies is the minimum writable-bookie count to maintain.
	// Callers must ensure this is >= the BookKeeper's configured
	// ensembleSize before calling Evaluate/Decide; this package does not
	// re-validate that invariant.
	MinWritableBookies int32

	// ScaleUpBy is how many bookies to add on a high-watermark scale-up.
	ScaleUpBy int32

	// ScaleUpMaxLimit is the highest replica count the autoscaler may ever
	// set. A value <= 0 means unbounded (no configured ceiling).
	ScaleUpMaxLimit int32

	// DiskUsageToleranceHwmPercent is the high watermark, as a whole-number
	// percent (0-100): a writable bookie whose ledger directories are ALL
	// at or above this usage is "at risk" and triggers a pre-emptive
	// scale-up.
	DiskUsageToleranceHwmPercent int32
}

// Decision is the outcome of one autoscaler tick.
type Decision struct {
	// ShouldScale is true when TargetReplicas differs from
	// Params.CurrentReplicas and the caller should patch spec.replicas.
	ShouldScale bool

	// TargetReplicas is the replica count to scale to (when ShouldScale),
	// or the unchanged current replica count (when not).
	TargetReplicas int32

	// WritableBookies is the observed writable-bookie count this tick,
	// regardless of ShouldScale - callers use it to populate
	// BookKeeper.status.writableBookies.
	WritableBookies int32

	// Reason is a short CamelCase machine-readable reason, suitable for a
	// Kubernetes Condition.Reason or Event.Reason.
	Reason string

	// Message is a human-readable explanation of Reason.
	Message string
}

// Evaluate polls every address in bookieAddrs via client and applies the
// strict-priority scale-up algorithm: (1) if fewer than
// params.MinWritableBookies bookies are writable, scale up by the deficit;
// (2) else if any writable bookie has ALL of its ledger directories at or
// above the HWM, scale up by params.ScaleUpBy; (3) else, no-op. The result is
// clamped to params.ScaleUpMaxLimit and never drops below
// params.CurrentReplicas: this package only ever scales up.
//
// Evaluate is resilient to a flaky bookie: a per-bookie ConnectionError
// (unreachable/unhealthy right now) skips that bookie and keeps polling the
// rest, so one dead bookie can't suppress a legitimate high-watermark
// scale-up on the healthy ones. But because a partial poll under-counts
// writable bookies, the deficit branch fires ONLY when every polled bookie
// responded — an incomplete poll can raise disk usage (a safe, positive
// signal) but never fabricate a writable-bookie deficit (which, being
// scale-up-only, would strand a permanent phantom replica). The tick fails
// (returns an error) when too few bookies responded to decide safely, or on
// the first data-integrity error (a corrupt reading, distinct from a
// ConnectionError), so a misread can never silently drive a scale-up.
func Evaluate(ctx context.Context, client BookieAdminClient, bookieAddrs []string, params Params) (Decision, error) {
	var writable, responded int32
	atRisk := false
	pollComplete := true
	var connErrs []error

	for _, addr := range bookieAddrs {
		state, err := client.State(ctx, addr)
		if err != nil {
			if IsConnectionError(err) {
				pollComplete = false
				connErrs = append(connErrs, err)
				continue
			}
			return Decision{}, fmt.Errorf("get bookie state for %s: %w", addr, err)
		}
		responded++
		if !state.Writable() {
			continue
		}
		writable++

		info, err := client.Info(ctx, addr)
		if err != nil {
			if IsConnectionError(err) {
				pollComplete = false
				connErrs = append(connErrs, err)
				continue
			}
			return Decision{}, fmt.Errorf("get bookie info for %s: %w", addr, err)
		}
		if allLedgerDisksAtOrAboveHwm(info.LedgerDisks, params.DiskUsageToleranceHwmPercent) {
			atRisk = true
		}
	}

	// Require at least min(MinWritableBookies, len(addrs)) bookies to answer
	// before deciding. Clamping to len(addrs) preserves the legitimate
	// deficit case where the ensemble is smaller than MinWritableBookies (so
	// every addressed bookie must respond), while still refusing to act when
	// the ensemble is mostly dark.
	quorum := params.MinWritableBookies
	if n := int32(len(bookieAddrs)); n < quorum {
		quorum = n
	}
	if responded < quorum {
		return Decision{}, fmt.Errorf("only %d of %d bookies responded; too few to safely evaluate scale-up: %w",
			responded, len(bookieAddrs), errors.Join(connErrs...))
	}

	return decide(params, writable, atRisk, pollComplete), nil
}

// allLedgerDisksAtOrAboveHwm reports whether disks is non-empty and every
// entry's usage fraction is >= the HWM. A bookie with no reported ledger
// disks is never "at risk" - there is nothing to measure.
func allLedgerDisksAtOrAboveHwm(disks []BookieDiskUsage, hwmPercent int32) bool {
	if len(disks) == 0 {
		return false
	}
	hwmFraction := float64(hwmPercent) / 100
	for _, d := range disks {
		if d.Fraction() < hwmFraction {
			return false
		}
	}
	return true
}

// decide is the pure priority-ordered scale-up decision, split out from
// Evaluate so it never needs a BookieAdminClient (or a mock of one) to test.
// pollComplete guards the deficit branch: a writable-bookie deficit is only
// trustworthy when every polled bookie responded, since an unreachable bookie
// could actually be writable and counting it as missing would strand a
// permanent phantom replica. The high-watermark branch needs no such guard —
// any writable bookie observed at/above the HWM is a real, positive signal.
func decide(params Params, writableBookies int32, anyWritableBookieAtRisk, pollComplete bool) Decision {
	current := params.CurrentReplicas
	target := current
	reason := ReasonStable
	message := "writable bookie count and disk usage are within tolerance; no scale-up needed"

	switch {
	case pollComplete && writableBookies < params.MinWritableBookies:
		deficit := params.MinWritableBookies - writableBookies
		target = current + deficit
		reason = ReasonWritableBookieDeficit
		message = fmt.Sprintf("only %d/%d writable bookies; scaling up by the %d-bookie deficit",
			writableBookies, params.MinWritableBookies, deficit)
	case anyWritableBookieAtRisk:
		target = current + params.ScaleUpBy
		reason = ReasonDiskUsageAtHighWatermark
		message = fmt.Sprintf("a writable bookie's ledger directories are all at/above the %d%% disk high-watermark; scaling up by %d",
			params.DiskUsageToleranceHwmPercent, params.ScaleUpBy)
	}

	if params.ScaleUpMaxLimit > 0 && target > params.ScaleUpMaxLimit {
		target = params.ScaleUpMaxLimit
	}
	if target < current {
		target = current
	}

	return Decision{
		ShouldScale:     target != current,
		TargetReplicas:  target,
		WritableBookies: writableBookies,
		Reason:          reason,
		Message:         message,
	}
}
