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
	"encoding/json"
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

// This file implements the ordered rolling-upgrade state machine described in
// docs/docs/high-availability.md ("Ordered rolling upgrades"): a PulsarCluster
// spec change (e.g. a pulsarVersion bump) must not land on every component at
// once. Tiers are rolled out strictly in dependency order - OxiaCluster
// (metadata) -> BookKeeper (bookies, with AutoRecovery alongside) -> Broker ->
// Proxy -> FunctionsWorker - and a downstream tier's *update* is deferred
// until every upstream tier has settled. Creating a still-missing child is
// never gated: there is nothing running yet for it to disrupt.
//
// A canary/partitioned rollout per tier (StatefulSet RollingUpdate
// partitions) and an AutoRecovery enable/disable toggle around the bookie
// roll are follow-ups, not implemented here.

const (
	// specHashAnnotation is stamped on every ordered-rollout tier's child CR
	// with a checksum of the DESIRED spec last written to it (the SpecDiffer).
	// Comparing it against a freshly computed desired-spec hash tells "this
	// tier's desired spec changed since the last write" (a genuine roll) apart
	// from "nothing to roll", without keeping a full copy of the previous spec
	// around, and without depending on the child's own reported status (which
	// necessarily lags until its controller catches up).
	specHashAnnotation = "pulsaroperator.io/spec-hash"

	// appliedSpecHashAnnotation is stamped with a checksum of the child's spec
	// as the apiserver actually STORED it after our last write (i.e. after
	// admission defaulting). Drift detection compares it against a hash of the
	// child's current live spec: they diverge only when something edited the
	// child out-of-band. Hashing the stored (post-default) spec - rather than
	// comparing the live spec straight against desired - is what keeps a
	// steady-state cluster write-free: a field the operator leaves unset but
	// the CRD defaults (e.g. oxia server replicas) would otherwise read as
	// perpetual "drift" and trigger a redundant Update every reconcile.
	appliedSpecHashAnnotation = "pulsaroperator.io/applied-spec-hash"

	// conditionTypeUpgrading reports ordered-rollout progress: which tier (if
	// any) currently has a spec roll in flight - either being written now or
	// deferred behind an unsettled upstream tier. It reports genuine spec
	// rolls only, NOT mere not-yet-Ready-ness: a fresh install whose children
	// are still coming up is not "Upgrading". It complements, rather than
	// replaces, the Ready condition.
	conditionTypeUpgrading = "Upgrading"

	// reasonUpgradeSettled marks Upgrading=False: no tier has a spec roll in
	// flight (steady state, or a fresh install still converging to Ready).
	reasonUpgradeSettled = "Settled"

	// rolloutRequeueInterval spaces out the defensive recheck while an
	// ordered rollout is in progress (some tier's desired spec differs from
	// what's applied - just written and not yet converged, or deferred behind
	// an unsettled upstream tier). Owner watches on the child CRs already
	// trigger a reconcile the moment a child's status changes; this is only a
	// backstop, and a steady-state cluster never requeues on this path.
	rolloutRequeueInterval = 15 * time.Second
)

// tierName identifies one stage of the ordered rollout, in dependency order.
type tierName string

const (
	tierOxia            tierName = "oxia"
	tierBookKeeper      tierName = "bookkeeper"
	tierAutoRecovery    tierName = "autorecovery"
	tierBroker          tierName = "broker"
	tierProxy           tierName = "proxy"
	tierFunctionsWorker tierName = "functionsworker"
)

// upgradeReason renders the tier as an Upgrading condition Reason.
func (t tierName) upgradeReason() string {
	switch t {
	case tierOxia:
		return "RollingOutOxia"
	case tierBookKeeper:
		return "RollingOutBookKeeper"
	case tierAutoRecovery:
		return "RollingOutAutoRecovery"
	case tierBroker:
		return "RollingOutBroker"
	case tierProxy:
		return "RollingOutProxy"
	case tierFunctionsWorker:
		return "RollingOutFunctionsWorker"
	default:
		return "RollingOut"
	}
}

// tierOutcome is one tier's post-reconcile ordered-rollout state.
type tierOutcome struct {
	// settled is true when the tier has fully caught up to its latest desired
	// spec - Ready and not sitting on a deferred update - so downstream tiers
	// may roll. Feeds the next tier's upstreamSettled gate.
	settled bool
	// applied is true only on the single reconcile that actually wrote a
	// changed desired spec to the child (the spec-hash re-stamp makes it
	// self-clearing next reconcile). Drives the RolloutStarted event.
	applied bool
	// deferred is true while a genuine spec change is withheld because an
	// upstream tier has not settled. Drives the RolloutDeferred event and
	// blocks this tier from counting as settled.
	deferred bool
}

// rolling reports whether a genuine spec roll is in flight for this tier -
// being written now (applied) or held back behind an upstream tier
// (deferred). This, NOT Ready-ness, is what makes the cluster "Upgrading".
func (o tierOutcome) rolling() bool {
	return o.applied || o.deferred
}

// specHash returns a deterministic checksum of a spec value. encoding/json
// sorts map keys when marshaling, so the same spec always encodes to the same
// bytes regardless of Go's randomized map iteration order, and a pointer
// hashes identically to the value it points at.
func specHash(spec any) (string, error) {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshaling spec: %w", err)
	}
	return config.Checksum(string(encoded)), nil
}

// setSpecHashAnnotation returns a copy of annotations with key set to hash.
// annotations is not mutated; pass nil to start from an empty set.
func setSpecHashAnnotation(annotations map[string]string, key, hash string) map[string]string {
	out := make(map[string]string, len(annotations)+1)
	maps.Copy(out, annotations)
	out[key] = hash
	return out
}

// childAction is the pure decision for an existing ordered-rollout child.
type childAction int

const (
	// childNoop: the child already matches desired; write nothing.
	childNoop childAction = iota
	// childApplyRoll: a genuine spec change whose upstream tiers have settled;
	// write desired now and re-stamp the spec-hash.
	childApplyRoll
	// childDeferRoll: a genuine spec change withheld because an upstream tier
	// hasn't settled; write nothing this reconcile.
	childDeferRoll
	// childDriftCorrect: no spec change is pending, but the child's live spec
	// has diverged from desired (e.g. an out-of-band edit); re-assert desired
	// UNGATED - this is drift correction, not an upgrade.
	childDriftCorrect
)

// orderedChildDecision is the pure core of the ordered-rollout gate for an
// EXISTING child, keeping four hashes distinct:
//   - storedDesiredHash: the spec-hash annotation last stamped on the child
//     (a hash of the DESIRED spec we last wrote).
//   - desiredHash: hash of the freshly built desired spec.
//   - storedAppliedHash: the applied-spec-hash annotation - a hash of the
//     child's spec as the apiserver STORED it after our last write.
//   - actualHash: hash of the child's CURRENT live spec.
//
// rollPending (storedDesiredHash != desiredHash) means the PulsarCluster spec
// changed since this child was last stamped - a genuine roll, honored in
// dependency order (applied only once upstreamSettled, else deferred). When no
// roll is pending, a live spec that diverges from what we last stored
// (actualHash != storedAppliedHash) is an out-of-band edit, corrected UNGATED:
// desired is unchanged, so re-asserting it is always safe and must never be
// blocked by tier ordering or read as an upgrade. Comparing the live spec to
// the stored-applied hash rather than to desiredHash is deliberate: it makes
// steady state write-free even for children with CRD-defaulted fields the
// operator leaves unset.
func orderedChildDecision(storedDesiredHash, desiredHash, storedAppliedHash, actualHash string, upstreamSettled bool) childAction {
	if storedDesiredHash != desiredHash {
		if upstreamSettled {
			return childApplyRoll
		}
		return childDeferRoll
	}
	if actualHash != storedAppliedHash {
		return childDriftCorrect
	}
	return childNoop
}

// tierSettled reports whether a tier has fully caught up to its latest desired
// spec: individually Ready and not currently sitting on a deferred update.
// reportFromConditions already treats a Ready condition observed against a
// stale generation as not-ready, so ready alone captures "no pending roll the
// child's own controller hasn't observed yet"; deferred captures the
// upstream-gate outcome on top of that.
func tierSettled(ready, deferred bool) bool {
	return ready && !deferred
}

// applyOrderedChild creates a missing child eagerly - nothing is running yet
// to disrupt - or, for an existing child, applies orderedChildDecision:
// applies or defers a genuine spec roll per tier ordering, corrects
// out-of-band drift ungated, or does nothing. child is always left populated
// with its current cluster state (even on the deferred and no-op paths), so
// the caller can read its Status/Generation immediately after.
//
// liveSpec returns the child's current .Spec (read after Get); setSpec writes
// the desired spec onto child. Both are closures over the concrete child type,
// since client.Object erases .Spec.
//
// onExisting, when non-nil, runs once - only when the child already exists,
// right after the Get that confirms it and before any hash is computed - so
// the caller can fold live state the umbrella does not own into desiredSpec
// ahead of hashing/writing. It is nil for every child except Broker and
// BookKeeper, whose reconcile funcs use it to copy an enabled autoscaler's
// current live spec.replicas onto the desired spec (see
// reconcileBroker/reconcileBookKeeper), so a genuine roll never resets the
// field. Those same reconcile funcs also swap in a desiredSpec/liveSpec pair
// that hides spec.replicas from the hash entirely in that case: onExisting
// alone would stop a roll from resetting replicas, but the desired-spec hash
// stamped on the PREVIOUS write still has last reconcile's replicas value
// baked in, so without also hiding the field from the hash, every autoscaler
// tick would still look like a "genuine roll" (the desired hash changed) and
// spuriously flip the tier to Upgrading.
func (r *PulsarClusterReconciler) applyOrderedChild(
	ctx context.Context,
	cluster *clusterv1alpha1.PulsarCluster,
	child client.Object,
	desiredSpec any,
	liveSpec func() any,
	setSpec func(),
	upstreamSettled bool,
	onExisting func(),
) (outcome tierOutcome, err error) {
	key := client.ObjectKeyFromObject(child)
	getErr := r.Get(ctx, key, child)
	switch {
	case apierrors.IsNotFound(getErr):
		desiredHash, err := specHash(desiredSpec)
		if err != nil {
			return tierOutcome{}, err
		}
		if err := r.writeOrderedChild(ctx, cluster, child, desiredHash, liveSpec, setSpec, true); err != nil {
			return tierOutcome{}, err
		}
		return tierOutcome{}, nil
	case getErr != nil:
		return tierOutcome{}, fmt.Errorf("getting child: %w", getErr)
	}

	if onExisting != nil {
		onExisting()
	}

	desiredHash, err := specHash(desiredSpec)
	if err != nil {
		return tierOutcome{}, err
	}
	actualHash, err := specHash(liveSpec())
	if err != nil {
		return tierOutcome{}, err
	}

	annotations := child.GetAnnotations()
	switch orderedChildDecision(annotations[specHashAnnotation], desiredHash, annotations[appliedSpecHashAnnotation], actualHash, upstreamSettled) {
	case childApplyRoll:
		if err := r.writeOrderedChild(ctx, cluster, child, desiredHash, liveSpec, setSpec, false); err != nil {
			return tierOutcome{}, err
		}
		return tierOutcome{applied: true}, nil
	case childDeferRoll:
		return tierOutcome{deferred: true}, nil
	case childDriftCorrect:
		if err := r.writeOrderedChild(ctx, cluster, child, desiredHash, liveSpec, setSpec, false); err != nil {
			return tierOutcome{}, err
		}
		return tierOutcome{}, nil
	default: // childNoop
		return tierOutcome{}, nil
	}
}

// writeOrderedChild applies the desired spec, sets the owner reference, stamps
// the desired-spec hash, and creates or updates the child - then records the
// stored (post-default) spec hash in a second, annotation-only write so the
// next reconcile's drift check has an accurate baseline. The second write is
// spec-identical (generation stable) and only runs when something actually
// changed, so steady-state reconciles stay write-free.
func (r *PulsarClusterReconciler) writeOrderedChild(
	ctx context.Context,
	cluster *clusterv1alpha1.PulsarCluster,
	child client.Object,
	desiredHash string,
	liveSpec func() any,
	setSpec func(),
	create bool,
) error {
	setSpec()
	child.SetAnnotations(setSpecHashAnnotation(child.GetAnnotations(), specHashAnnotation, desiredHash))
	if err := controllerutil.SetControllerReference(cluster, child, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference: %w", err)
	}
	if create {
		if err := r.Create(ctx, child); err != nil {
			return fmt.Errorf("creating child: %w", err)
		}
	} else if err := r.Update(ctx, child); err != nil {
		return fmt.Errorf("updating child: %w", err)
	}

	// child now carries the apiserver-defaulted spec; record its hash as the
	// drift baseline.
	appliedHash, err := specHash(liveSpec())
	if err != nil {
		return err
	}
	child.SetAnnotations(setSpecHashAnnotation(child.GetAnnotations(), appliedSpecHashAnnotation, appliedHash))
	if err := r.Update(ctx, child); err != nil {
		return fmt.Errorf("stamping applied-spec hash: %w", err)
	}
	return nil
}

// tierState is one tier's post-reconcile state, in ordered-rollout order, used
// to build the Upgrading condition and choose rollout events.
type tierState struct {
	tier    tierName
	present bool
	outcome tierOutcome
}

// rolling reports whether this present tier has a genuine spec roll in flight.
func (s tierState) rolling() bool {
	return s.present && s.outcome.rolling()
}

// rollingBottleneck returns the most-upstream tier with a spec roll in flight
// - the tier the ordered-rollout gate is currently working through - if any.
func rollingBottleneck(states []tierState) (tierState, bool) {
	for _, s := range states {
		if s.rolling() {
			return s, true
		}
	}
	return tierState{}, false
}

// rollingOutCondition reports Upgrading=True naming the rolling bottleneck
// (the most-upstream tier with a spec roll in flight), or Upgrading=False when
// no tier is rolling. Crucially it keys on a genuine spec roll being in
// flight, NOT on Ready-ness: children that are merely still coming up (a fresh
// install) leave the cluster Settled, not Upgrading.
func rollingOutCondition(generation int64, states []tierState) metav1.Condition {
	if s, ok := rollingBottleneck(states); ok {
		return metav1.Condition{
			Type:               conditionTypeUpgrading,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			Reason:             s.tier.upgradeReason(),
			Message:            fmt.Sprintf("rolling out the %s tier before downstream tiers", s.tier),
		}
	}
	return metav1.Condition{
		Type:               conditionTypeUpgrading,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             reasonUpgradeSettled,
		Message:            "no tier has a spec roll in flight",
	}
}

// rolloutEvent maps a tier's applied-vs-deferred state to the Normal event
// describing it. applied means the tier's updated spec was just written this
// reconcile; deferred means the write is held back behind an unsettled
// upstream tier.
func rolloutEvent(tier tierName, applied bool) (reason, action, note string) {
	if applied {
		return "RolloutStarted", "StartRollout",
			fmt.Sprintf("rolling out updated spec to the %s tier", tier)
	}
	return "RolloutDeferred", "DeferRollout",
		fmt.Sprintf("%s tier update deferred until upstream tiers settle", tier)
}

// recordRolloutEvents emits ordered-rollout Events on the PulsarCluster so
// `kubectl describe` surfaces upgrade progress without inspecting every child.
// It de-dups: RolloutStarted is inherently single-shot (a tier's spec-hash is
// re-stamped on write, so applied is true for exactly one reconcile), while
// RolloutDeferred fires only when the deferred bottleneck changes, so a tier
// parked behind an unsettled upstream doesn't re-emit every 15s requeue.
func (r *PulsarClusterReconciler) recordRolloutEvents(
	cluster *clusterv1alpha1.PulsarCluster,
	prior *metav1.Condition,
	next metav1.Condition,
	states []tierState,
) {
	for _, s := range states {
		if s.outcome.applied {
			reason, action, note := rolloutEvent(s.tier, true)
			r.recorder().Eventf(cluster, nil, corev1.EventTypeNormal, reason, action, note)
		}
	}

	if next.Status != metav1.ConditionTrue || !rolloutBottleneckChanged(prior, next) {
		return
	}
	if s, ok := rollingBottleneck(states); ok && s.outcome.deferred {
		reason, action, note := rolloutEvent(s.tier, false)
		r.recorder().Eventf(cluster, nil, corev1.EventTypeNormal, reason, action, note)
	}
}

// rolloutBottleneckChanged reports whether the Upgrading condition's rolling
// bottleneck changed since the prior reconcile (status or named tier), the
// signal that a deferred-rollout event is worth emitting again.
func rolloutBottleneckChanged(prior *metav1.Condition, next metav1.Condition) bool {
	if prior == nil {
		return true
	}
	return prior.Status != next.Status || prior.Reason != next.Reason
}

func (r *PulsarClusterReconciler) recorder() events.EventRecorder {
	if r.Recorder != nil {
		return r.Recorder
	}
	// A zero-value FakeRecorder has a nil Events channel and discards every
	// event, so it is a safe no-op sink when no recorder was wired in.
	return &events.FakeRecorder{}
}
