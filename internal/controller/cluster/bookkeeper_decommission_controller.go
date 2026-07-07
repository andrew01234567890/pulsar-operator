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

// This file implements the guarded, safe bookie scale-DOWN / decommission
// state machine described in docs/docs/autoscaling.md ("Safe bookie
// scale-down"). It is a *separate* controller from BookKeeperReconciler
// (bookkeeper_controller.go, which owns the StatefulSet/PVCs/ConfigMap/
// Service/PDB) so that the highest-risk logic in this operator -- the one
// whose bugs cause permanent data loss -- lives in one dedicated, narrowly
// reviewable place, gated OFF by default, and never touches the StatefulSet
// object directly: it only ever decrements BookKeeper.spec.replicas, which
// the unmodified BookKeeperReconciler then propagates.
package cluster

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	bkadmin "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/bookkeeper"
)

const (
	reasonDecommissionQuorumWouldBreak       = "QuorumWouldBreak"
	reasonDecommissionPlacementUnsatisfiable = "PlacementUnsatisfiable"
	reasonDecommissionInProgress             = "DecommissionInProgress"
	reasonDecommissionComplete               = "DecommissionComplete"
	reasonDecommissionTimedOut               = "DecommissionTimedOut"
	reasonDecommissionFailed                 = "DecommissionFailed"
	reasonManualDrainRejected                = "ManualDrainOrdinalMismatch"

	// eventAction is the events API "action" field for every Event this
	// controller emits: all of them are steps of the same overarching
	// guarded-decommission operation, so reason (which varies per call site)
	// already carries the specific distinction action would otherwise add.
	eventAction = "BookieDecommission"

	// defaultDecommissionTimeoutSeconds mirrors
	// BookKeeperAutoscalerSpec.DecommissionTimeoutSeconds's kubebuilder
	// default, used as a Go-level fallback when Autoscaler is constructed
	// directly (bypassing CRD defaulting), matching the existing
	// defaultWriteQuorum/defaultBookKeeperReplicas pattern in
	// bookkeeper_controller.go.
	defaultDecommissionTimeoutSeconds int32 = 1800

	// defaultEnsembleSize mirrors BookKeeperEnsembleSpec.EnsembleSize's
	// kubebuilder default (see bookkeeper_controller.go's defaultWriteQuorum /
	// defaultAckQuorum for the analogous quorum fallbacks, which this file
	// reuses rather than redeclaring).
	defaultEnsembleSize int32 = 3

	defaultBookieDecommissionPeriodSeconds int32 = 10
	defaultStabilizationWindowSeconds      int32 = 300
	defaultDiskUsageToleranceLwmPct        int32 = 75

	// phaseRequeueInterval paces the state machine's polling phases
	// (awaiting replication, awaiting pod termination) so they don't hammer
	// the bookie admin API / API server in a tight loop.
	phaseRequeueInterval = 5 * time.Second

	// labelTopologyZone is the well-known node label the rack-awareness
	// placement check reads, matching the "operator sets each bookie's rack
	// = node zone" design documented in docs/docs/high-availability.md.
	labelTopologyZone = "topology.kubernetes.io/zone"
)

// BookKeeperDecommissionReconciler implements the guarded bookie scale-down
// state machine. It is gated on spec.autoscaler.enabled &&
// spec.autoscaler.scaleDownEnabled (both required; both default false), so
// constructing this reconciler is always safe -- it is a no-op until a
// cluster operator explicitly opts in.
type BookKeeperDecommissionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Recorder emits Normal events for each phase transition and Warning
	// events for rejected/aborted/reverted decommissions. Optional: nil-safe
	// so unit/integration tests can construct the reconciler without one.
	Recorder events.EventRecorder

	// RESTConfig and Clientset back the default (real) AdminClient, which
	// execs into bookie pods. Only required when AdminClientFactory is nil.
	RESTConfig *rest.Config
	Clientset  kubernetes.Interface

	// AdminClientFactory, when set, overrides the default PodExecAdminClient
	// construction -- tests inject a mock here instead of exec-ing into real
	// pods.
	AdminClientFactory func(bk *clusterv1alpha1.BookKeeper) bkadmin.AdminClient

	// Now, when set, overrides time.Now() so tests can simulate elapsed time
	// (stabilization windows, decommission timeouts) deterministically.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives the guarded bookie decommission state machine. On every
// call it re-fetches BookKeeper fresh (never trusting a cached copy from an
// earlier reconcile) and either resumes an in-flight decommission from its
// persisted phase, starts one (manual annotation or automatic guard), or
// does nothing.
func (r *BookKeeperDecommissionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	bk := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, req.NamespacedName, bk); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !decommissionGateEnabled(bk.Spec) {
		return ctrl.Result{}, nil
	}

	admin := r.adminClient(bk)

	if bk.Status.Decommission != nil && bk.Status.Decommission.Phase != "" {
		return r.resume(ctx, bk, admin)
	}

	ordinal, requested, err := evaluateManualTrigger(bk)
	if err != nil {
		log.Info("rejecting manual drain request", "reason", err.Error())
		r.event(bk, corev1.EventTypeWarning, reasonManualDrainRejected, err.Error())
		return r.clearManualDrainAnnotation(ctx, bk)
	}
	if requested {
		return r.beginDecommission(ctx, bk, ordinal, true)
	}

	shouldStart, ordinal, err := r.evaluateAutoTrigger(ctx, bk, admin)
	if err != nil {
		log.Error(err, "evaluating bookie scale-down trigger")
		return ctrl.Result{}, err
	}
	if !shouldStart {
		return ctrl.Result{RequeueAfter: resolvePeriod(bk.Spec)}, nil
	}
	return r.beginDecommission(ctx, bk, ordinal, false)
}

// resume dispatches to the handler for the persisted phase. Every phase
// handler re-derives everything it needs from bk and the persisted
// BookKeeperDecommissionStatus, so resuming after an operator restart takes
// exactly the same code path as continuing within one long-running process.
func (r *BookKeeperDecommissionReconciler) resume(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient) (ctrl.Result, error) {
	d := bk.Status.Decommission
	if d.TargetOrdinal == nil {
		return r.finish(ctx, bk, corev1.EventTypeWarning, reasonDecommissionFailed,
			"decommission status missing targetOrdinal; clearing corrupt state")
	}
	ordinal := *d.TargetOrdinal
	podName := bookiePodName(bk, ordinal)

	switch d.Phase {
	case clusterv1alpha1.BookKeeperDecommissionPhaseVerifying:
		return r.phaseVerify(ctx, bk, admin, ordinal)
	case clusterv1alpha1.BookKeeperDecommissionPhaseMarkingReadOnly:
		return r.phaseMarkReadOnly(ctx, bk, admin, podName)
	case clusterv1alpha1.BookKeeperDecommissionPhaseTriggeringRecovery:
		return r.phaseTriggerRecovery(ctx, bk, admin, podName, d.TargetBookieID)
	case clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication:
		return r.phaseAwaitReplication(ctx, bk, admin, podName, d.TargetBookieID)
	case clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie:
		return r.phaseInvalidateCookie(ctx, bk, admin, podName)
	case clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown:
		return r.phaseScaleDown(ctx, bk, ordinal)
	case clusterv1alpha1.BookKeeperDecommissionPhaseDeletingPVCs:
		return r.phaseDeletePVCs(ctx, bk, ordinal)
	default:
		return r.finish(ctx, bk, corev1.EventTypeWarning, reasonDecommissionFailed,
			fmt.Sprintf("unknown decommission phase %q; clearing state", d.Phase))
	}
}

// --- Phase 1: Verifying ---

// phaseVerify re-checks the invariants that must hold AFTER the target
// bookie is removed: the static ensembleSize >= writeQuorum >= ackQuorum
// relationship, that enough writable bookies remain to still stripe a full
// ensemble (and meet minWritableBookies), and that rack/zone placement
// remains satisfiable. Nothing has been changed on the target bookie yet, so
// a failed check here aborts cleanly -- there is nothing to revert.
func (r *BookKeeperDecommissionReconciler) phaseVerify(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, ordinal int32) (ctrl.Result, error) {
	ensembleSize := resolveEnsembleSize(bk.Spec)
	writeQuorum := resolveWriteQuorum(bk.Spec)
	ackQuorum := resolveAckQuorum(bk.Spec)
	minWritable := resolveMinWritableBookies(bk.Spec, ensembleSize)
	podName := bookiePodName(bk, ordinal)

	if !bkadmin.QuorumHolds(ensembleSize, writeQuorum, ackQuorum) {
		return r.abort(ctx, bk, reasonDecommissionQuorumWouldBreak, fmt.Sprintf(
			"ensembleSize=%d writeQuorum=%d ackQuorum=%d no longer satisfy ensembleSize >= writeQuorum >= ackQuorum",
			ensembleSize, writeQuorum, ackQuorum))
	}

	writableAfter, err := r.writableCountExcluding(ctx, bk, admin, ordinal)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !bkadmin.CapacityHoldsAfterRemoval(writableAfter, ensembleSize, minWritable) {
		return r.abort(ctx, bk, reasonDecommissionQuorumWouldBreak, fmt.Sprintf(
			"removing bookie %s would leave %d writable bookies, below max(ensembleSize=%d, minWritableBookies=%d)",
			podName, writableAfter, ensembleSize, minWritable))
	}

	placementOK, err := r.placementSatisfiableAfterRemoval(ctx, bk, ordinal, ensembleSize)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !placementOK {
		return r.abort(ctx, bk, reasonDecommissionPlacementUnsatisfiable, fmt.Sprintf(
			"removing bookie %s would leave rack/zone placement unable to satisfy ensembleSize=%d", podName, ensembleSize))
	}

	return r.advancePhase(ctx, bk, clusterv1alpha1.BookKeeperDecommissionPhaseMarkingReadOnly,
		fmt.Sprintf("quorum and placement re-verified for removing bookie %s", podName))
}

// --- Phase 2: MarkingReadOnly ---

func (r *BookKeeperDecommissionReconciler) phaseMarkReadOnly(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, podName string) (ctrl.Result, error) {
	if r.timedOut(bk) {
		return r.revert(ctx, bk, admin, podName, "timed out before the bookie could be marked read-only")
	}
	if err := admin.SetReadOnly(ctx, podName, true); err != nil {
		return ctrl.Result{}, fmt.Errorf("marking bookie %s read-only: %w", podName, err)
	}
	return r.advancePhase(ctx, bk, clusterv1alpha1.BookKeeperDecommissionPhaseTriggeringRecovery,
		fmt.Sprintf("bookie %s marked read-only", podName))
}

// --- Phase 3: TriggeringRecovery ---

func (r *BookKeeperDecommissionReconciler) phaseTriggerRecovery(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, podName, bookieID string) (ctrl.Result, error) {
	if r.timedOut(bk) {
		return r.revert(ctx, bk, admin, podName, "timed out before decommissionbookie/recover could be triggered")
	}
	if err := admin.TriggerDecommission(ctx, podName, bookieID); err != nil {
		// Retried with the controller's normal error backoff; timedOut above
		// bounds how long these retries can go on for before reverting.
		return ctrl.Result{}, fmt.Errorf("triggering decommission of bookie %s: %w", podName, err)
	}
	return r.advancePhase(ctx, bk, clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication,
		fmt.Sprintf("triggered decommissionbookie for %s", podName))
}

// --- Phase 4: AwaitingReplication ---

// phaseAwaitReplication blocks (by repeatedly requeuing) until the target
// bookie has zero ledgers and the cluster has zero under-replicated ledgers,
// or until the decommission times out -- at which point it reverts the
// bookie to writable rather than leaving it stuck read-only.
func (r *BookKeeperDecommissionReconciler) phaseAwaitReplication(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, podName, bookieID string) (ctrl.Result, error) {
	if r.timedOut(bk) {
		return r.revert(ctx, bk, admin, podName, "timed out waiting for re-replication to finish")
	}

	hasLedgers, err := admin.HasLedgers(ctx, podName, bookieID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("checking ledgers on bookie %s: %w", podName, err)
	}
	noUnderReplication, err := admin.NoUnderReplicatedLedgers(ctx, podName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("checking cluster under-replication via bookie %s: %w", podName, err)
	}
	if hasLedgers || !noUnderReplication {
		return ctrl.Result{RequeueAfter: phaseRequeueInterval}, nil
	}

	return r.advancePhase(ctx, bk, clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie,
		fmt.Sprintf("bookie %s has zero ledgers and the cluster has zero under-replicated ledgers", podName))
}

// --- Phase 5: InvalidatingCookie ---

// phaseInvalidateCookie is the last phase an automatic revert applies to:
// once the cookie has been renamed, the bookie's on-disk identity is already
// invalidated, so bringing it back as a writable ensemble member is no
// longer a clean operation. From here on, failures are retried indefinitely
// (mechanical Kubernetes API operations) rather than reverted.
func (r *BookKeeperDecommissionReconciler) phaseInvalidateCookie(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, podName string) (ctrl.Result, error) {
	if err := admin.RenameCookie(ctx, podName); err != nil {
		return ctrl.Result{}, fmt.Errorf("invalidating cookie for bookie %s: %w", podName, err)
	}
	return r.advancePhase(ctx, bk, clusterv1alpha1.BookKeeperDecommissionPhaseScalingDown,
		fmt.Sprintf("cookie invalidated (renamed, not deleted) for bookie %s", podName))
}

// --- Phase 6: ScalingDown ---

// phaseScaleDown decrements BookKeeper.spec.replicas by exactly one -- never
// the StatefulSet directly, which BookKeeperReconciler (unmodified) owns and
// continuously reconciles from this same field.
//
// target is derived from the persisted target ordinal -- fixed for the
// lifetime of this decommission -- rather than from "whatever spec.replicas
// currently holds minus one". That distinction matters for idempotency: the
// ordinal was the highest one when this decommission started, so the
// replica count after removing it is exactly that ordinal. Recomputing
// "current - 1" on every call would instead keep decrementing further on
// every resumed retry of this phase, which is exactly the kind of bug this
// state machine exists to prevent. Checking the current value before writing
// still makes the Kubernetes API call itself idempotent: resuming mid-phase
// after the spec write already landed (but before the phase advanced) is a
// no-op patch.
func (r *BookKeeperDecommissionReconciler) phaseScaleDown(ctx context.Context, bk *clusterv1alpha1.BookKeeper, ordinal int32) (ctrl.Result, error) {
	latest := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(bk), latest); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching BookKeeper before scaling down: %w", err)
	}

	target := ordinal

	if latest.Spec.Replicas == nil || *latest.Spec.Replicas != target {
		latest.Spec.Replicas = &target
		if err := r.Update(ctx, latest); err != nil {
			return ctrl.Result{}, fmt.Errorf("scaling BookKeeper %s replicas to %d: %w", latest.Name, target, err)
		}
	}

	bk.ResourceVersion = latest.ResourceVersion
	return r.advancePhase(ctx, bk, clusterv1alpha1.BookKeeperDecommissionPhaseDeletingPVCs,
		fmt.Sprintf("scaled BookKeeper %s replicas to %d", latest.Name, target))
}

// --- Phase 7: DeletingPVCs ---

// phaseDeletePVCs waits for the StatefulSet controller to finish terminating
// the target ordinal's pod, then deletes that ordinal's PVCs itself -- the
// operator never relies on StatefulSet persistentVolumeClaimRetentionPolicy
// for this (see desiredStatefulSet's comment in bookkeeper_controller.go).
func (r *BookKeeperDecommissionReconciler) phaseDeletePVCs(ctx context.Context, bk *clusterv1alpha1.BookKeeper, ordinal int32) (ctrl.Result, error) {
	podName := bookiePodName(bk, ordinal)

	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: bk.Namespace, Name: podName}, pod)
	switch {
	case err == nil:
		return ctrl.Result{RequeueAfter: phaseRequeueInterval}, nil
	case apierrors.IsNotFound(err):
		// fall through to PVC deletion
	default:
		return ctrl.Result{}, fmt.Errorf("checking termination of bookie pod %s: %w", podName, err)
	}

	for _, volName := range []string{volumeNameJournal, volumeNameLedgers, volumeNameIndex} {
		pvcName := fmt.Sprintf("%s-%s", volName, podName)
		pvc := &corev1.PersistentVolumeClaim{}
		getErr := r.Get(ctx, types.NamespacedName{Namespace: bk.Namespace, Name: pvcName}, pvc)
		if apierrors.IsNotFound(getErr) {
			continue
		}
		if getErr != nil {
			return ctrl.Result{}, fmt.Errorf("looking up PVC %s: %w", pvcName, getErr)
		}
		if err := r.Delete(ctx, pvc); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("deleting PVC %s: %w", pvcName, err)
		}
	}

	return r.finish(ctx, bk, corev1.EventTypeNormal, reasonDecommissionComplete, fmt.Sprintf(
		"decommission of bookie %s complete: cookie invalidated, replicas scaled down, PVCs deleted", podName))
}

// --- Starting / finishing a decommission ---

func (r *BookKeeperDecommissionReconciler) beginDecommission(ctx context.Context, bk *clusterv1alpha1.BookKeeper, ordinal int32, manual bool) (ctrl.Result, error) {
	podName := bookiePodName(bk, ordinal)
	message := fmt.Sprintf("starting guarded decommission of bookie %s (manual=%t)", podName, manual)

	latest := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(bk), latest); err != nil {
		return ctrl.Result{}, fmt.Errorf("re-fetching BookKeeper before starting decommission: %w", err)
	}

	started := metav1.NewTime(r.now())
	latest.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
		Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseVerifying,
		TargetOrdinal:  &ordinal,
		TargetBookieID: bookieIDFor(bk, ordinal),
		Manual:         manual,
		StartedAt:      &started,
		Message:        message,
	}
	apimeta.SetStatusCondition(&latest.Status.Conditions, decommissioningCondition(latest.Generation, metav1.ConditionTrue, reasonDecommissionInProgress, message))
	if err := r.Status().Update(ctx, latest); err != nil {
		return ctrl.Result{}, fmt.Errorf("persisting decommission start: %w", err)
	}

	r.event(bk, corev1.EventTypeNormal, "DecommissionStarted", message)
	return ctrl.Result{RequeueAfter: phaseRequeueInterval}, nil
}

// advancePhase persists the next phase onto a freshly re-fetched BookKeeper,
// so this write can never clobber a concurrent update to fields
// BookKeeperReconciler owns (Replicas, ReadyReplicas, ObservedGeneration, the
// Ready condition) with a stale in-memory copy from earlier in this
// reconcile.
func (r *BookKeeperDecommissionReconciler) advancePhase(ctx context.Context, bk *clusterv1alpha1.BookKeeper, next clusterv1alpha1.BookKeeperDecommissionPhase, message string) (ctrl.Result, error) {
	latest, err := r.writeDecommissionStatus(ctx, bk, func(d *clusterv1alpha1.BookKeeperDecommissionStatus) {
		d.Phase = next
		d.Message = message
	}, reasonDecommissionInProgress, metav1.ConditionTrue, message)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("advancing decommission phase to %s: %w", next, err)
	}

	r.event(latest, corev1.EventTypeNormal, string(next), message)
	return ctrl.Result{RequeueAfter: phaseRequeueInterval}, nil
}

// abort clears an in-flight decommission before anything has been changed on
// the target bookie (a phaseVerify guard failed): nothing to revert, just
// stop and report why.
func (r *BookKeeperDecommissionReconciler) abort(ctx context.Context, bk *clusterv1alpha1.BookKeeper, reason, message string) (ctrl.Result, error) {
	return r.finish(ctx, bk, corev1.EventTypeWarning, reason, message)
}

// revert brings the target bookie back to writable before clearing the
// decommission state -- the guard against ever leaving a bookie stuck
// read-only. If SetReadOnly itself fails, state is deliberately NOT cleared:
// the next reconcile will re-enter this same phase (still timed out) and
// retry the revert, rather than silently abandoning a bookie in read-only.
func (r *BookKeeperDecommissionReconciler) revert(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, podName, why string) (ctrl.Result, error) {
	if err := admin.SetReadOnly(ctx, podName, false); err != nil {
		return ctrl.Result{}, fmt.Errorf("reverting bookie %s to writable: %w", podName, err)
	}
	message := fmt.Sprintf("auto-reverted bookie %s to writable: %s", podName, why)
	return r.finish(ctx, bk, corev1.EventTypeWarning, reasonDecommissionTimedOut, message)
}

// finish clears status.decommission, sets the terminal Decommissioning
// condition, clears a manual-drain annotation if present (a request is
// consumed once acted on, whatever the outcome), and emits an Event. Used by
// abort, revert, and successful completion alike.
func (r *BookKeeperDecommissionReconciler) finish(ctx context.Context, bk *clusterv1alpha1.BookKeeper, eventType, reason, message string) (ctrl.Result, error) {
	latest, err := r.writeDecommissionStatus(ctx, bk, nil, reason, metav1.ConditionFalse, message)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("finishing decommission (%s): %w", reason, err)
	}

	if _, ok := latest.Annotations[clusterv1alpha1.AnnotationDrainBookieOrdinal]; ok {
		if _, err := r.clearManualDrainAnnotation(ctx, latest); err != nil {
			return ctrl.Result{}, err
		}
	}

	r.event(latest, eventType, reason, message)
	return ctrl.Result{RequeueAfter: resolvePeriod(latest.Spec)}, nil
}

// writeDecommissionStatus re-fetches the latest BookKeeper immediately
// before writing, applies mutate to a copy of its (possibly nil, for a
// terminal write) decommission status, sets the Decommissioning condition,
// and writes .status. Re-fetching immediately before the write is what keeps
// this controller from clobbering bookkeeper_controller.go's concurrent
// writes to Replicas/ReadyReplicas/ObservedGeneration/the Ready condition --
// the two controllers share one Status subresource but must each only ever
// touch their own fields.
func (r *BookKeeperDecommissionReconciler) writeDecommissionStatus(
	ctx context.Context,
	bk *clusterv1alpha1.BookKeeper,
	mutate func(*clusterv1alpha1.BookKeeperDecommissionStatus),
	reason string,
	status metav1.ConditionStatus,
	message string,
) (*clusterv1alpha1.BookKeeper, error) {
	latest := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(bk), latest); err != nil {
		return nil, fmt.Errorf("re-fetching BookKeeper before status write: %w", err)
	}

	if mutate == nil {
		latest.Status.Decommission = nil
	} else {
		d := latest.Status.Decommission
		if d == nil {
			d = &clusterv1alpha1.BookKeeperDecommissionStatus{}
		} else {
			d = d.DeepCopy()
		}
		mutate(d)
		latest.Status.Decommission = d
	}

	apimeta.SetStatusCondition(&latest.Status.Conditions, decommissioningCondition(latest.Generation, status, reason, message))
	if err := r.Status().Update(ctx, latest); err != nil {
		return nil, err
	}
	return latest, nil
}

// clearManualDrainAnnotation removes AnnotationDrainBookieOrdinal, if
// present, via a merge patch so it never re-triggers on a future reconcile.
func (r *BookKeeperDecommissionReconciler) clearManualDrainAnnotation(ctx context.Context, bk *clusterv1alpha1.BookKeeper) (ctrl.Result, error) {
	if _, ok := bk.Annotations[clusterv1alpha1.AnnotationDrainBookieOrdinal]; !ok {
		return ctrl.Result{RequeueAfter: resolvePeriod(bk.Spec)}, nil
	}
	patch := client.MergeFrom(bk.DeepCopy())
	delete(bk.Annotations, clusterv1alpha1.AnnotationDrainBookieOrdinal)
	if err := r.Patch(ctx, bk, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("clearing manual drain annotation: %w", err)
	}
	return ctrl.Result{RequeueAfter: resolvePeriod(bk.Spec)}, nil
}

func decommissioningCondition(generation int64, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               clusterv1alpha1.BookKeeperConditionTypeDecommissioning,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	}
}

// --- Trigger evaluation ---

// decommissionGateEnabled is the single hard safety gate covering both the
// automatic trigger and the manual drain annotation: both
// spec.autoscaler.enabled and spec.autoscaler.scaleDownEnabled must be true
// (both default false), so this controller is a no-op out of the box.
func decommissionGateEnabled(spec clusterv1alpha1.BookKeeperSpec) bool {
	a := spec.Autoscaler
	return a != nil && a.Enabled && a.ScaleDownEnabled != nil && *a.ScaleDownEnabled
}

// evaluateManualTrigger inspects AnnotationDrainBookieOrdinal. It returns
// requested=false with no error when the annotation is absent, and an error
// when it names anything other than the current highest (only removable)
// ordinal, so the caller can reject the request loudly instead of silently
// draining the wrong bookie.
func evaluateManualTrigger(bk *clusterv1alpha1.BookKeeper) (ordinal int32, requested bool, err error) {
	raw, ok := bk.Annotations[clusterv1alpha1.AnnotationDrainBookieOrdinal]
	if !ok {
		return 0, false, nil
	}

	parsed, convErr := strconv.ParseInt(raw, 10, 32)
	if convErr != nil {
		return 0, false, fmt.Errorf("annotation %s value %q is not an integer ordinal: %w",
			clusterv1alpha1.AnnotationDrainBookieOrdinal, raw, convErr)
	}

	highest := resolveReplicas(bk.Spec) - 1
	if int32(parsed) != highest {
		return 0, false, fmt.Errorf(
			"annotation %s requests ordinal %d, but the current highest (only removable) ordinal is %d; "+
				"a StatefulSet can only shrink from its highest ordinal -- set the annotation to %d to confirm, or remove it to cancel",
			clusterv1alpha1.AnnotationDrainBookieOrdinal, parsed, highest, highest)
	}
	return int32(parsed), true, nil
}

// evaluateAutoTrigger implements the automatic scale-down guard documented
// in docs/docs/autoscaling.md: the cluster must be stable, have more
// writable bookies than max(minWritableBookies, largestEnsembleSize), have
// every writable bookie below the low watermark, and have zero cluster-wide
// under-replicated ledgers. Any I/O error aborts the evaluation (returning
// it, not a false "no" or true "yes") so a transient failure never causes an
// incorrect scale-down decision.
func (r *BookKeeperDecommissionReconciler) evaluateAutoTrigger(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient) (bool, int32, error) {
	replicas := resolveReplicas(bk.Spec)
	if replicas <= 0 {
		return false, 0, nil
	}
	if !r.clusterStable(ctx, bk, replicas) {
		return false, 0, nil
	}

	lwm := resolveLWMFraction(bk.Spec)
	var writable int32
	allBelowLWM := true
	for ord := range replicas {
		podName := bookiePodName(bk, ord)

		isWritable, err := admin.IsWritable(ctx, podName)
		if err != nil {
			return false, 0, fmt.Errorf("checking writable state of bookie %s: %w", podName, err)
		}
		if !isWritable {
			continue
		}
		writable++

		below, err := admin.LedgerDiskUsageBelow(ctx, podName, lwm)
		if err != nil {
			return false, 0, fmt.Errorf("checking disk usage of bookie %s: %w", podName, err)
		}
		if !below {
			allBelowLWM = false
		}
	}

	noUnderReplication, err := admin.NoUnderReplicatedLedgers(ctx, bookiePodName(bk, 0))
	if err != nil {
		return false, 0, fmt.Errorf("checking cluster under-replication: %w", err)
	}

	ensembleSize := resolveEnsembleSize(bk.Spec)
	should := bkadmin.ShouldTriggerScaleDown(bkadmin.ScaleDownGuardInput{
		WritableBookies:     writable,
		MinWritableBookies:  resolveMinWritableBookies(bk.Spec, ensembleSize),
		LargestEnsembleSize: ensembleSize,
		AllBelowLWM:         allBelowLWM,
		NoUnderReplication:  noUnderReplication,
	})
	if !should {
		return false, 0, nil
	}
	return true, replicas - 1, nil
}

// clusterStable requires every configured bookie pod to exist, report Ready,
// and have done so continuously for at least the stabilization window --
// mirroring KAAP's AutoscalerUtils.isStsReadyToScale, but keyed off each
// pod's Ready condition transition time (rather than pod start time) so a
// pod that flaps Ready without restarting still resets the window.
func (r *BookKeeperDecommissionReconciler) clusterStable(ctx context.Context, bk *clusterv1alpha1.BookKeeper, replicas int32) bool {
	if bk.Status.ReadyReplicas != replicas || bk.Status.Replicas != replicas {
		return false
	}

	cutoff := r.now().Add(-resolveStabilizationWindow(bk.Spec))
	for ord := range replicas {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: bk.Namespace, Name: bookiePodName(bk, ord)}, pod); err != nil {
			return false
		}
		cond := findPodReadyCondition(pod)
		if cond == nil || cond.Status != corev1.ConditionTrue || cond.LastTransitionTime.After(cutoff) {
			return false
		}
	}
	return true
}

func findPodReadyCondition(pod *corev1.Pod) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// writableCountExcluding counts writable bookies among all configured
// ordinals except excludeOrdinal (the decommission target), regardless of
// the target's own current writable state -- this is the true "capacity
// after removal" the phaseVerify guard needs.
func (r *BookKeeperDecommissionReconciler) writableCountExcluding(ctx context.Context, bk *clusterv1alpha1.BookKeeper, admin bkadmin.AdminClient, excludeOrdinal int32) (int32, error) {
	replicas := resolveReplicas(bk.Spec)
	var count int32
	for ord := range replicas {
		if ord == excludeOrdinal {
			continue
		}
		podName := bookiePodName(bk, ord)
		writable, err := admin.IsWritable(ctx, podName)
		if err != nil {
			return 0, fmt.Errorf("checking writable state of bookie %s: %w", podName, err)
		}
		if writable {
			count++
		}
	}
	return count, nil
}

// placementSatisfiableAfterRemoval gathers the distinct node zones backing
// every remaining (non-target) bookie pod and defers the actual decision to
// bkadmin.PlacementSatisfiable so that decision stays table-testable without
// a Kubernetes dependency.
func (r *BookKeeperDecommissionReconciler) placementSatisfiableAfterRemoval(ctx context.Context, bk *clusterv1alpha1.BookKeeper, targetOrdinal, ensembleSize int32) (bool, error) {
	replicas := resolveReplicas(bk.Spec)
	zones := map[string]struct{}{}
	var zoneLabelsSeen int

	for ord := range replicas {
		if ord == targetOrdinal {
			continue
		}
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: bk.Namespace, Name: bookiePodName(bk, ord)}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("looking up bookie pod for placement check: %w", err)
		}
		if pod.Spec.NodeName == "" {
			continue
		}

		node := &corev1.Node{}
		if err := r.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("looking up node %s for placement check: %w", pod.Spec.NodeName, err)
		}
		if zone, ok := node.Labels[labelTopologyZone]; ok && zone != "" {
			zoneLabelsSeen++
			zones[zone] = struct{}{}
		}
	}

	return bkadmin.PlacementSatisfiable(len(zones), zoneLabelsSeen, ensembleSize), nil
}

// --- Small helpers ---

// event records an Event via the events API (regarding=obj, no related
// object, eventAction as the action, message as the note), nil-safe so tests
// can construct the reconciler without a Recorder.
func (r *BookKeeperDecommissionReconciler) event(obj runtime.Object, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, eventAction, message)
}

func (r *BookKeeperDecommissionReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *BookKeeperDecommissionReconciler) adminClient(bk *clusterv1alpha1.BookKeeper) bkadmin.AdminClient {
	if r.AdminClientFactory != nil {
		return r.AdminClientFactory(bk)
	}
	return &bkadmin.PodExecAdminClient{
		RESTConfig:    r.RESTConfig,
		Clientset:     r.Clientset,
		Namespace:     bk.Namespace,
		ContainerName: bookieContainerName,
		AdminPort:     bookieAdminPort,
		JournalDir:    journalMountPath,
	}
}

func (r *BookKeeperDecommissionReconciler) timedOut(bk *clusterv1alpha1.BookKeeper) bool {
	d := bk.Status.Decommission
	if d == nil || d.StartedAt == nil {
		return false
	}
	return bkadmin.DecommissionTimedOut(d.StartedAt.Time, r.now(), resolveDecommissionTimeout(bk.Spec))
}

func bookiePodName(bk *clusterv1alpha1.BookKeeper, ordinal int32) string {
	return fmt.Sprintf("%s-%d", bk.Name, ordinal)
}

// bookieIDFor mirrors KAAP's PodExecBookieAdminClient bookie-ID derivation:
// <pod>.<headless-svc>.<namespace>.svc.<cluster-domain>:<bookiePort>. The
// headless Service is named after the BookKeeper (see desiredHeadlessService
// in bookkeeper_controller.go); cluster.local is assumed as the cluster
// domain since it is not currently exposed as a spec field.
func bookieIDFor(bk *clusterv1alpha1.BookKeeper, ordinal int32) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local:%d", bookiePodName(bk, ordinal), bk.Name, bk.Namespace, bookiePort)
}

func resolveEnsembleSize(spec clusterv1alpha1.BookKeeperSpec) int32 {
	if spec.Ensemble != nil && spec.Ensemble.EnsembleSize != nil {
		return *spec.Ensemble.EnsembleSize
	}
	return defaultEnsembleSize
}

// resolveAckQuorum and resolveWriteQuorum are declared in
// bookkeeper_controller.go (same package) and reused here.

func resolveMinWritableBookies(spec clusterv1alpha1.BookKeeperSpec, ensembleSize int32) int32 {
	if spec.Autoscaler != nil && spec.Autoscaler.MinWritableBookies != nil {
		return *spec.Autoscaler.MinWritableBookies
	}
	return ensembleSize
}

func resolveDecommissionTimeout(spec clusterv1alpha1.BookKeeperSpec) int32 {
	if spec.Autoscaler != nil && spec.Autoscaler.DecommissionTimeoutSeconds != nil {
		return *spec.Autoscaler.DecommissionTimeoutSeconds
	}
	return defaultDecommissionTimeoutSeconds
}

func resolvePeriod(spec clusterv1alpha1.BookKeeperSpec) time.Duration {
	seconds := defaultBookieDecommissionPeriodSeconds
	if spec.Autoscaler != nil && spec.Autoscaler.PeriodSeconds != nil {
		seconds = *spec.Autoscaler.PeriodSeconds
	}
	return time.Duration(seconds) * time.Second
}

func resolveStabilizationWindow(spec clusterv1alpha1.BookKeeperSpec) time.Duration {
	seconds := defaultStabilizationWindowSeconds
	if spec.Autoscaler != nil && spec.Autoscaler.StabilizationWindowSeconds != nil {
		seconds = *spec.Autoscaler.StabilizationWindowSeconds
	}
	return time.Duration(seconds) * time.Second
}

func resolveLWMFraction(spec clusterv1alpha1.BookKeeperSpec) float64 {
	pct := defaultDiskUsageToleranceLwmPct
	if spec.Autoscaler != nil && spec.Autoscaler.DiskUsageToleranceLwm != nil {
		pct = *spec.Autoscaler.DiskUsageToleranceLwm
	}
	return float64(pct) / 100.0
}

// SetupWithManager sets up the controller with the Manager.
func (r *BookKeeperDecommissionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.BookKeeper{}).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &clusterv1alpha1.BookKeeper{})).
		Named("cluster-bookkeeper-decommission").
		Complete(r)
}
