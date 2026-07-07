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
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const (
	// backupScheduleComponentName labels every Backup a BackupSchedule stamps
	// out, keyed by the schedule's own name (as instance), exactly as the
	// export Job labels itself under its owning Backup (see
	// backupComponentName in backup_job.go). It is what lets listChildren find
	// this schedule's children without listing every Backup in the namespace.
	backupScheduleComponentName = "backup-schedule"

	// annotationScheduledTime records, on each stamped-out Backup, the cron
	// tick it was created for - useful for a human inspecting `kubectl get
	// backups -o yaml`, alongside the tick already encoded in the name.
	annotationScheduledTime = "backup.pulsaroperator.io/scheduled-time"
)

// buildBackupFromTemplate renders the Backup a BackupSchedule stamps out for
// a due cron tick, copying spec.backupTemplate verbatim (mirroring CronJob's
// jobTemplate pattern). The caller sets the owner reference and creates it.
func buildBackupFromTemplate(schedule *backupv1alpha1.BackupSchedule, tick time.Time) *backupv1alpha1.Backup {
	return &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduledBackupName(schedule.Name, tick),
			Namespace: schedule.Namespace,
			Labels:    builder.Labels(schedule.Name, backupScheduleComponentName),
			Annotations: map[string]string{
				annotationScheduledTime: tick.UTC().Format(time.RFC3339),
			},
		},
		Spec: *schedule.Spec.BackupTemplate.DeepCopy(),
	}
}

// listChildren returns the Backups owned by this BackupSchedule. It lists by
// the stable label selector rather than scanning every Backup in the
// namespace, then double-checks controller ownership - the same
// label-plus-IsControlledBy pattern backup_controller.go uses to find an
// export Job's pods.
func (r *BackupScheduleReconciler) listChildren(ctx context.Context, schedule *backupv1alpha1.BackupSchedule) ([]backupv1alpha1.Backup, error) {
	var list backupv1alpha1.BackupList
	if err := r.List(ctx, &list,
		client.InNamespace(schedule.Namespace),
		client.MatchingLabels(builder.SelectorLabels(schedule.Name, backupScheduleComponentName)),
	); err != nil {
		return nil, fmt.Errorf("listing child backups: %w", err)
	}

	children := make([]backupv1alpha1.Backup, 0, len(list.Items))
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], schedule) {
			children = append(children, list.Items[i])
		}
	}
	return children, nil
}

// backupTerminal reports whether a Backup has reached a terminal phase
// (Completed or Failed). A Backup that is Pending/Running, or whose phase
// has not been observed yet (empty string, just created), is still in
// flight and must never be touched by retention GC.
func backupTerminal(b *backupv1alpha1.Backup) bool {
	switch b.Status.Phase {
	case backupv1alpha1.BackupPhaseCompleted, backupv1alpha1.BackupPhaseFailed:
		return true
	default:
		return false
	}
}

// backupCompletionTime is the timestamp retention age and "most recent
// success" comparisons are measured from: the Backup's own completionTime,
// falling back to its creationTimestamp for the (defensive-only) case of a
// terminal Backup that somehow never got one stamped.
func backupCompletionTime(b *backupv1alpha1.Backup) time.Time {
	if b.Status.CompletionTime != nil {
		return b.Status.CompletionTime.Time
	}
	return b.CreationTimestamp.Time
}

// updateChildStatus recomputes status.active and status.lastSuccessfulTime
// from the current set of children. It is pure recomputation from a fresh
// List every reconcile (rather than an incremental diff against a previous
// value), which is what makes it correct both on a normal timer-driven
// reconcile and on the reconcile triggered by the Owns(&Backup{}) watch when
// a child's phase changes (e.g. Running -> Completed). lastSuccessfulTime is
// only ever advanced, never cleared, so it survives a later retention GC of
// the Backup that set it.
func updateChildStatus(schedule *backupv1alpha1.BackupSchedule, children []backupv1alpha1.Backup) {
	active := make([]corev1.ObjectReference, 0, len(children))
	var newestSuccess *backupv1alpha1.Backup

	for i := range children {
		child := &children[i]
		if !backupTerminal(child) {
			active = append(active, corev1.ObjectReference{
				APIVersion: backupv1alpha1.GroupVersion.String(),
				Kind:       "Backup",
				Namespace:  child.Namespace,
				Name:       child.Name,
				UID:        child.UID,
			})
		}
		if child.Status.Phase == backupv1alpha1.BackupPhaseCompleted &&
			(newestSuccess == nil || backupCompletionTime(child).After(backupCompletionTime(newestSuccess))) {
			newestSuccess = child
		}
	}

	schedule.Status.Active = active
	if newestSuccess != nil {
		t := metav1.NewTime(backupCompletionTime(newestSuccess))
		schedule.Status.LastSuccessfulTime = &t
	}
}

// reconcileRetention deletes children beyond the retention policy: it keeps
// only the newest spec.retention.successfulBackupsHistoryLimit Completed
// Backups (deleting older Completed ones), and separately deletes any
// terminal (Completed or Failed) child older than spec.retention.maxAge, when
// set. A Backup that is not yet terminal (Pending/Running/unobserved) is
// never considered for deletion by either rule - an in-flight capture must
// never be interrupted by GC.
func (r *BackupScheduleReconciler) reconcileRetention(ctx context.Context, schedule *backupv1alpha1.BackupSchedule, children []backupv1alpha1.Backup) error {
	limit := max(int(successfulBackupsHistoryLimit(schedule.Spec.Retention)), 0)
	maxAge := schedule.Spec.Retention.MaxAge
	now := r.now()

	var completed []*backupv1alpha1.Backup
	toDelete := map[types.UID]*backupv1alpha1.Backup{}

	for i := range children {
		child := &children[i]
		if !backupTerminal(child) {
			continue
		}
		if child.Status.Phase == backupv1alpha1.BackupPhaseCompleted {
			completed = append(completed, child)
		}
		if maxAge != nil && now.Sub(backupCompletionTime(child)) > maxAge.Duration {
			toDelete[child.UID] = child
		}
	}

	slices.SortFunc(completed, func(a, b *backupv1alpha1.Backup) int {
		return backupCompletionTime(b).Compare(backupCompletionTime(a)) // newest first
	})
	for _, b := range completed[min(len(completed), limit):] {
		toDelete[b.UID] = b
	}

	for _, b := range toDelete {
		if err := r.Delete(ctx, b); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting backup %q for retention: %w", b.Name, err)
		}
	}
	return nil
}
