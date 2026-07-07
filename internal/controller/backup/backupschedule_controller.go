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

package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

const (
	// conditionTypeScheduleValid is the BackupSchedule's summary condition for
	// whether spec.schedule parses as a cron expression. It is orthogonal to
	// spec.suspend: a suspended schedule can still be Valid.
	conditionTypeScheduleValid = "ScheduleValid"

	reasonValid                  = "Valid"
	reasonInvalidSchedule        = "InvalidSchedule"
	reasonBackupCreated          = "BackupCreated"
	reasonTooManyMissedSchedules = "TooManyMissedSchedules"
)

// BackupScheduleReconciler reconciles a BackupSchedule object: it stamps out
// owned Backups on a cron schedule (mirroring Kubernetes CronJob) and
// garbage-collects old ones per spec.retention.
type BackupScheduleReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// Now returns the current time; nil defaults to time.Now. Tests override
	// it so due-time computation is deterministic without real sleeps,
	// mirroring BackupReconciler's clock injection.
	Now func() time.Time
}

// now returns the current time, honoring an injected clock for tests.
func (r *BackupScheduleReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backupschedules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backupschedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backupschedules/finalizers,verbs=update
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backups,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a BackupSchedule: it recomputes status.active and
// status.lastSuccessfulTime from its owned Backups, garbage-collects old
// ones per spec.retention, then - unless suspended or the schedule fails to
// parse - stamps out a Backup for the most recent due cron tick and requeues
// for the next one.
func (r *BackupScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var schedule backupv1alpha1.BackupSchedule
	if err := r.Get(ctx, req.NamespacedName, &schedule); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	base := schedule.DeepCopy()
	schedule.Status.ObservedGeneration = schedule.Generation

	result, err := r.advance(ctx, &schedule)

	if patchErr := r.patchStatus(ctx, base, &schedule); patchErr != nil {
		if err == nil {
			err = patchErr
		}
	}
	return result, err
}

// advance mutates schedule.Status toward its next state and returns the
// requeue decision. It never persists status itself - Reconcile patches
// once, after.
func (r *BackupScheduleReconciler) advance(ctx context.Context, schedule *backupv1alpha1.BackupSchedule) (ctrl.Result, error) {
	children, err := r.listChildren(ctx, schedule)
	if err != nil {
		return ctrl.Result{}, err
	}
	updateChildStatus(schedule, children)

	// Retention GC runs regardless of validity/suspend - exactly as
	// Kubernetes CronJob prunes job history even while suspended: suspend
	// only pauses new stamping, not cleanup of what already exists.
	if err := r.reconcileRetention(ctx, schedule, children); err != nil {
		return ctrl.Result{}, err
	}

	sched, parseErr := parseSchedule(schedule.Spec.Schedule)
	if parseErr != nil {
		r.setInvalidSchedule(schedule, parseErr)
		return ctrl.Result{}, nil
	}
	r.setValidSchedule(schedule)

	if scheduleSuspended(schedule.Spec) {
		return ctrl.Result{}, nil
	}

	return r.reconcileDueBackup(ctx, schedule, sched)
}

// reconcileDueBackup stamps out a Backup for the most recent due cron tick
// (if any) and requeues at the next tick after now.
func (r *BackupScheduleReconciler) reconcileDueBackup(ctx context.Context, schedule *backupv1alpha1.BackupSchedule, sched cron.Schedule) (ctrl.Result, error) {
	now := r.now()
	earliest := earliestScheduleTime(schedule)

	due, ok, tooMany := mostRecentDueTime(sched, earliest, now)
	if ok {
		if tooMany {
			r.recorder().Eventf(schedule, nil, corev1.EventTypeWarning, reasonTooManyMissedSchedules, "BackupSchedule",
				"too many missed schedule times for %q since %s; skipping the backlog and stamping only the most recent tick (%s)",
				schedule.Spec.Schedule, earliest.UTC().Format(time.RFC3339), due.UTC().Format(time.RFC3339))
		}
		if err := r.ensureBackupForTick(ctx, schedule, due); err != nil {
			return ctrl.Result{}, err
		}
		scheduledAt := metav1.NewTime(due)
		schedule.Status.LastScheduleTime = &scheduledAt
	}

	// Computed as next.Sub(now) rather than time.Until(next) so it stays
	// correct under an injected clock (r.Now): time.Until measures against
	// the real wall clock, which would desync from "now" in tests (and, more
	// subtly, from whatever instant r.now() actually returned in production).
	return ctrl.Result{RequeueAfter: sched.Next(now).Sub(now)}, nil
}

// ensureBackupForTick creates the owned Backup for a due cron tick. It is
// idempotent by construction: the Backup's name is deterministic per tick
// (scheduledBackupName), so re-processing the same tick - e.g. a crash
// between creating the Backup and persisting status.lastScheduleTime -
// resolves to the same name and simply observes AlreadyExists rather than
// creating a duplicate.
func (r *BackupScheduleReconciler) ensureBackupForTick(ctx context.Context, schedule *backupv1alpha1.BackupSchedule, tick time.Time) error {
	backup := buildBackupFromTemplate(schedule, tick)
	if err := controllerutil.SetControllerReference(schedule, backup, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on scheduled backup: %w", err)
	}

	err := r.Create(ctx, backup)
	switch {
	case err == nil:
		r.recorder().Eventf(schedule, nil, corev1.EventTypeNormal, reasonBackupCreated, "BackupSchedule",
			"created Backup %q for schedule tick %s", backup.Name, tick.UTC().Format(time.RFC3339))
		return nil
	case apierrors.IsAlreadyExists(err):
		return nil
	default:
		return fmt.Errorf("creating scheduled backup %q: %w", backup.Name, err)
	}
}

// setInvalidSchedule records that spec.schedule does not parse as a cron
// expression. The Warning event only fires the first reconcile the failure
// is observed (deduped against the current condition), so a schedule left
// invalid doesn't re-fire the Warning on every reconcile.
func (r *BackupScheduleReconciler) setInvalidSchedule(schedule *backupv1alpha1.BackupSchedule, parseErr error) {
	prior := apimeta.FindStatusCondition(schedule.Status.Conditions, conditionTypeScheduleValid)
	changed := prior == nil || prior.Status != metav1.ConditionFalse
	apimeta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
		Type:               conditionTypeScheduleValid,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: schedule.Generation,
		Reason:             reasonInvalidSchedule,
		Message:            parseErr.Error(),
	})
	if changed {
		r.recorder().Eventf(schedule, nil, corev1.EventTypeWarning, reasonInvalidSchedule, "BackupSchedule", "%s", parseErr.Error())
	}
}

// setValidSchedule records that spec.schedule parses successfully.
func (r *BackupScheduleReconciler) setValidSchedule(schedule *backupv1alpha1.BackupSchedule) {
	apimeta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
		Type:               conditionTypeScheduleValid,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: schedule.Generation,
		Reason:             reasonValid,
		Message:            "schedule is a valid cron expression",
	})
}

// patchStatus persists the BackupSchedule's status subresource only when it
// changed, via an optimistic merge against the pre-reconcile snapshot.
func (r *BackupScheduleReconciler) patchStatus(ctx context.Context, base, updated *backupv1alpha1.BackupSchedule) error {
	if apiequality.Semantic.DeepEqual(&base.Status, &updated.Status) {
		return nil
	}
	return r.Status().Patch(ctx, updated, client.MergeFrom(base))
}

func (r *BackupScheduleReconciler) recorder() events.EventRecorder {
	if r.Recorder != nil {
		return r.Recorder
	}
	return &events.FakeRecorder{}
}

// SetupWithManager sets up the controller with the Manager, watching the
// Backups it owns so a child's phase change (e.g. Running -> Completed)
// re-triggers reconciliation.
func (r *BackupScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.BackupSchedule{}).
		Owns(&backupv1alpha1.Backup{}).
		Named("backup-backupschedule").
		Complete(r)
}
