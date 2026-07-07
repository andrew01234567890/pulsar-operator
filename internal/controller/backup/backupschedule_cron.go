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

// maxMissedSchedules is the "too many missed start times" threshold, mirroring
// Kubernetes CronJob's hard-coded 100 (kubernetes/kubernetes
// pkg/controller/cronjob/utils.go). It does NOT bound how far mostRecentDueTime
// scans - that computation is O(1) - it only decides when to emit the
// TooManyMissedSchedules Warning. Either way exactly ONE Backup is stamped (for
// the genuine most-recent tick), never a per-missed-tick catch-up burst.
const maxMissedSchedules = 100

// parseSchedule parses a BackupSchedule's cron expression using the same
// "standard" parser Kubernetes CronJob uses: 5 space-separated fields
// (minute hour dom month dow), or an @-prefixed macro (@yearly, @monthly,
// @weekly, @daily, @hourly, @every <duration>). The CEL rule on
// BackupScheduleSpec.Schedule only checks the rough shape (non-empty,
// @-prefixed or 5+ fields) at admission time; this catches malformed field
// values. It does NOT catch a schedule that parses but matches no calendar
// date (e.g. February 31) - see scheduleNeverFires for that.
func parseSchedule(expr string) (cron.Schedule, error) {
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule %q: %w", expr, err)
	}
	return sched, nil
}

// scheduleNeverFires reports whether a parseable schedule matches no calendar
// time and so will never fire (e.g. "0 0 31 2 *" - February 31). robfig's
// Next searches a ~5-year horizon and returns the zero time.Time when it finds
// no match; such a schedule must be surfaced as invalid rather than silently
// driving mostRecentDueTime toward the year-1 zero tick.
func scheduleNeverFires(sched cron.Schedule, reference time.Time) bool {
	return sched.Next(reference).IsZero()
}

// earliestScheduleTime is the lower bound mostRecentDueTime computes forward
// from: the last tick this BackupSchedule actually actioned, or - for a
// schedule that has never fired - its own creation time, so a newly created
// schedule never reaches back for ticks that occurred before it existed.
func earliestScheduleTime(schedule *backupv1alpha1.BackupSchedule) time.Time {
	if schedule.Status.LastScheduleTime != nil {
		return schedule.Status.LastScheduleTime.Time
	}
	return schedule.CreationTimestamp.Time
}

// mostRecentDueTime returns the single most-recent activation time in
// (earliest, now] that is due, mirroring Kubernetes CronJob's
// getMostRecentScheduleTime. It does NOT walk every missed tick: after a long
// outage there may be thousands, so it derives the schedule's interval from
// the first two activations and jumps directly (O(1)) to the last tick at or
// before now. The caller stamps exactly ONE Backup for that tick - never one
// per missed tick - which is what prevents a catch-up burst.
//
// ok is false when nothing in the window has come due yet. tooMany is true
// when the arithmetic count of skipped ticks exceeds maxMissedSchedules; it is
// advisory only (drives a Warning), and the returned due time is still the
// genuine most-recent tick either way.
func mostRecentDueTime(sched cron.Schedule, earliest, now time.Time) (due time.Time, ok bool, tooMany bool) {
	if !earliest.Before(now) {
		return time.Time{}, false, false
	}

	t1 := sched.Next(earliest)
	if t1.IsZero() || now.Before(t1) {
		// Never fires within the horizon (defensive; scheduleNeverFires
		// catches this upstream), or the first tick is not due yet.
		return time.Time{}, false, false
	}

	t2 := sched.Next(t1)
	if t2.IsZero() || now.Before(t2) {
		// Exactly one tick (t1) is due, or the schedule fires only once
		// within the search horizon: stamp t1.
		return t1, true, false
	}

	// From here at least two ticks (t1, t2) are at or before now. Derive the
	// interval and jump straight to the most-recent tick instead of walking.
	interval := int64(t2.Sub(t1).Round(time.Second).Seconds())
	if interval < 1 {
		// Sub-second/degenerate interval we cannot count against; the one
		// tick we know is due is t1.
		return t1, true, false
	}

	elapsed := int64(now.Sub(t1).Seconds())
	missed := elapsed/interval + 1
	tooMany = missed > maxMissedSchedules

	// potentialEarliest lands one interval before the most-recent tick (using
	// earliest as the grid anchor keeps @every schedules aligned to their
	// original phase); Next then resolves the exact tick, correcting for any
	// non-uniform spacing (DST, month lengths) the arithmetic estimate missed.
	// This loop runs ~once for a regular schedule - it is not a per-tick walk.
	potentialEarliest := earliest.Add(time.Duration((missed-1)*interval) * time.Second)
	mostRecent := t1
	for t := sched.Next(potentialEarliest); !t.IsZero() && !t.After(now); t = sched.Next(t) {
		mostRecent = t
	}
	return mostRecent, true, tooMany
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
