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
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

// maxMissedSchedules bounds how many activation times mostRecentDueTime will
// scan before giving up and returning only the most recent one found so far
// (with tooMany=true). This mirrors Kubernetes CronJob's own hard-coded
// 100-tick bound (kubernetes/kubernetes pkg/controller/cronjob/utils.go): it
// exists so a schedule that goes unreconciled for a long time (spec.suspend
// left on for months, or the controller itself down) never stamps out a
// burst of backlog Backups when it catches up - only the single most recent
// tick is ever actioned, exactly as CronJob's "too many missed start times"
// handling skips straight to the latest.
const maxMissedSchedules = 100

// parseSchedule parses a BackupSchedule's cron expression using the same
// "standard" parser Kubernetes CronJob uses: 5 space-separated fields
// (minute hour dom month dow), or an @-prefixed macro (@yearly, @monthly,
// @weekly, @daily, @hourly, @every <duration>). The CEL rule on
// BackupScheduleSpec.Schedule only checks the rough shape (non-empty,
// @-prefixed or 5+ fields) at admission time; this catches everything CEL
// cannot, such as out-of-range field values.
func parseSchedule(expr string) (cron.Schedule, error) {
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule %q: %w", expr, err)
	}
	return sched, nil
}

// earliestScheduleTime is the lower bound mostRecentDueTime scans forward
// from: the last tick this BackupSchedule actually actioned, or - for a
// schedule that has never fired - its own creation time, so a newly created
// schedule never reaches back for ticks that occurred before it existed.
func earliestScheduleTime(schedule *backupv1alpha1.BackupSchedule) time.Time {
	if schedule.Status.LastScheduleTime != nil {
		return schedule.Status.LastScheduleTime.Time
	}
	return schedule.CreationTimestamp.Time
}

// mostRecentDueTime scans a schedule's activation times in (earliest, now]
// and returns the most recent one that is due. ok is false when nothing in
// that window has come due yet. tooMany is true when more than
// maxMissedSchedules ticks were found in the window - the scan is abandoned
// early in that case, but the most recent tick found so far is still
// returned so the caller can action it, skipping the rest of the backlog.
func mostRecentDueTime(sched cron.Schedule, earliest, now time.Time) (due time.Time, ok bool, tooMany bool) {
	if earliest.After(now) {
		return time.Time{}, false, false
	}

	var last time.Time
	count := 0
	for t := sched.Next(earliest); !t.After(now); t = sched.Next(t) {
		last = t
		count++
		if count > maxMissedSchedules {
			return last, true, true
		}
	}
	if last.IsZero() {
		return time.Time{}, false, false
	}
	return last, true, false
}

// scheduledBackupName deterministically names the Backup a BackupSchedule
// stamps out for a given cron tick, keyed by the schedule's name and the
// tick's unix time. This determinism is what makes stamping idempotent:
// reconciling the same due tick twice (e.g. a crash between creating the
// Backup and persisting status.lastScheduleTime) resolves to the same name,
// so the second attempt hits AlreadyExists instead of creating a duplicate.
func scheduledBackupName(scheduleName string, tick time.Time) string {
	return fmt.Sprintf("%s-%d", scheduleName, tick.Unix())
}
