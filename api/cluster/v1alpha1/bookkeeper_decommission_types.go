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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// BookKeeperConditionTypeDecommissioning reports progress of the guarded,
	// serialized bookie scale-down state machine described by
	// BookKeeperDecommissionStatus. It is True while a decommission is in
	// flight and False once it finishes, whether by completing, aborting a
	// pre-flight guard, or auto-reverting after a timeout/failure.
	BookKeeperConditionTypeDecommissioning = "Decommissioning"

	// AnnotationDrainBookieOrdinal requests a manual, on-demand drain of the
	// bookie at the given StatefulSet ordinal (e.g. for planned node
	// maintenance), reusing the exact same guarded decommission state machine
	// as an automatic scale-down.
	//
	// Because a StatefulSet can only ever shrink by removing its highest
	// ordinal, the annotation value must equal (current replicas - 1) at the
	// time it is observed, or the request is rejected with a Warning event
	// and left untouched. Requiring the caller to name the current highest
	// ordinal (rather than accepting a bare boolean trigger) also guards
	// against a stale annotation -- set before the cluster scaled up or down
	// -- silently draining a different, unintended bookie. The annotation is
	// removed once the request has been acted on (started, rejected, aborted,
	// or reverted), so it never causes a repeat drain on a future scale event
	// that happens to land on the same ordinal.
	AnnotationDrainBookieOrdinal = "bookkeeper.cluster.pulsaroperator.io/drain-bookie-ordinal"
)

// BookKeeperDecommissionPhase enumerates the guarded, resumable bookie
// scale-down state machine's phases. Each phase is safe to resume from cold
// (e.g. after an operator restart) using only the persisted
// BookKeeperDecommissionStatus, rather than needing to restart the whole
// decommission from scratch.
type BookKeeperDecommissionPhase string

const (
	// BookKeeperDecommissionPhaseVerifying re-checks that removing the target
	// bookie still leaves ensembleSize >= writeQuorum >= ackQuorum satisfiable
	// and rack/zone placement intact. Nothing has been changed on the target
	// bookie yet at this phase, so failing the check aborts cleanly with
	// nothing to revert.
	BookKeeperDecommissionPhaseVerifying BookKeeperDecommissionPhase = "Verifying"

	// BookKeeperDecommissionPhaseMarkingReadOnly marks the target bookie
	// read-only via its admin API.
	BookKeeperDecommissionPhaseMarkingReadOnly BookKeeperDecommissionPhase = "MarkingReadOnly"

	// BookKeeperDecommissionPhaseTriggeringRecovery runs
	// `bin/bookkeeper shell decommissionbookie` (falling back to `recover -f`)
	// against the target bookie to force re-replication of its ledgers off
	// of it.
	BookKeeperDecommissionPhaseTriggeringRecovery BookKeeperDecommissionPhase = "TriggeringRecovery"

	// BookKeeperDecommissionPhaseAwaitingReplication blocks until the target
	// bookie has zero ledgers and the cluster has zero under-replicated
	// ledgers, or until DecommissionTimeoutSeconds elapses since StartedAt --
	// which auto-reverts the bookie to writable instead of leaving it stuck
	// read-only.
	BookKeeperDecommissionPhaseAwaitingReplication BookKeeperDecommissionPhase = "AwaitingReplication"

	// BookKeeperDecommissionPhaseInvalidatingCookie renames (never deletes)
	// the target bookie's on-disk cookie VERSION file, so it can never
	// silently rejoin the ensemble with its old identity while keeping the
	// resulting state diagnosable. This is the last phase from which an
	// automatic revert is attempted: once the cookie has been invalidated,
	// bringing the bookie back online as a writable member is no longer a
	// clean operation, so later phases retry indefinitely instead of
	// reverting.
	BookKeeperDecommissionPhaseInvalidatingCookie BookKeeperDecommissionPhase = "InvalidatingCookie"

	// BookKeeperDecommissionPhaseScalingDown decrements BookKeeper.spec.replicas
	// by exactly one so the (unmodified) BookKeeper reconciler shrinks the
	// StatefulSet by one ordinal.
	BookKeeperDecommissionPhaseScalingDown BookKeeperDecommissionPhase = "ScalingDown"

	// BookKeeperDecommissionPhaseDeletingPVCs waits for the terminated pod at
	// the target ordinal to disappear and then deletes that ordinal's PVCs
	// itself -- the operator never relies on StatefulSet
	// persistentVolumeClaimRetentionPolicy to do this.
	BookKeeperDecommissionPhaseDeletingPVCs BookKeeperDecommissionPhase = "DeletingPVCs"
)

// BookKeeperDecommissionStatus persists the guarded bookie scale-down state
// machine's progress on BookKeeper.status so it survives operator restarts:
// Reconcile resumes from Phase rather than starting the decommission over.
type BookKeeperDecommissionStatus struct {
	// phase is the state machine's current step.
	// +optional
	Phase BookKeeperDecommissionPhase `json:"phase,omitempty"`

	// targetOrdinal is the StatefulSet ordinal of the bookie being
	// decommissioned.
	// +optional
	TargetOrdinal *int32 `json:"targetOrdinal,omitempty"`

	// targetBookieId is the BookKeeper bookie ID (host:port form) of the
	// bookie being decommissioned.
	// +optional
	TargetBookieID string `json:"targetBookieId,omitempty"`

	// manual is true when this decommission was requested via
	// AnnotationDrainBookieOrdinal rather than triggered automatically by the
	// disk-watermark/under-replication guard.
	// +optional
	Manual bool `json:"manual,omitempty"`

	// startedAt is when the state machine began. DecommissionTimeoutSeconds
	// is measured from this timestamp.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// message is a human-readable summary of the current phase, or of why the
	// decommission was aborted/reverted/completed.
	// +optional
	Message string `json:"message,omitempty"`
}
