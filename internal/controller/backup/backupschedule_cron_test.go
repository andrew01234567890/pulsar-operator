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
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

func mustParseSchedule(t *testing.T, expr string) cron.Schedule {
	t.Helper()
	sched, err := parseSchedule(expr)
	if err != nil {
		t.Fatalf("parseSchedule(%q) unexpected error: %v", expr, err)
	}
	return sched
}

func TestParseScheduleValid(t *testing.T) {
	for _, expr := range []string{
		"0 0 * * *",
		"*/5 * * * *",
		"@daily",
		"@hourly",
		"@every 1h30m",
	} {
		t.Run(expr, func(t *testing.T) {
			if _, err := parseSchedule(expr); err != nil {
				t.Errorf("parseSchedule(%q) unexpected error: %v", expr, err)
			}
		})
	}
}

func TestParseScheduleInvalid(t *testing.T) {
	for _, expr := range []string{
		"99 99 * * *", // out-of-range minute/hour, shape-valid, semantically invalid
		"not a schedule at all",
		"@every not-a-duration",
	} {
		t.Run(expr, func(t *testing.T) {
			if _, err := parseSchedule(expr); err == nil {
				t.Errorf("parseSchedule(%q) expected an error, got nil", expr)
			}
		})
	}
}

func TestMostRecentDueTimeNotYetDue(t *testing.T) {
	sched := mustParseSchedule(t, "0 0 * * *") // daily at midnight
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Only an hour has passed since the last tick; the next tick (midnight
	// the following day) is not due yet.
	_, ok, tooMany := mostRecentDueTime(sched, base, base.Add(time.Hour))
	if ok {
		t.Errorf("expected not due, got due")
	}
	if tooMany {
		t.Errorf("expected tooMany=false")
	}
}

func TestMostRecentDueTimeSingleTick(t *testing.T) {
	sched := mustParseSchedule(t, "0 0 * * *")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	want := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	due, ok, tooMany := mostRecentDueTime(sched, base, base.Add(25*time.Hour))
	if !ok {
		t.Fatalf("expected due, got not due")
	}
	if tooMany {
		t.Errorf("expected tooMany=false")
	}
	if !due.Equal(want) {
		t.Errorf("due = %v, want %v", due, want)
	}
}

func TestMostRecentDueTimeExactBoundary(t *testing.T) {
	sched := mustParseSchedule(t, "0 0 * * *")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tick := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	// now == the tick itself must count as due (CronJob's own !t.After(now)
	// semantics), not "not yet".
	due, ok, _ := mostRecentDueTime(sched, base, tick)
	if !ok {
		t.Fatalf("expected due at the exact boundary")
	}
	if !due.Equal(tick) {
		t.Errorf("due = %v, want %v", due, tick)
	}
}

func TestMostRecentDueTimeCollapsesBacklogToLatest(t *testing.T) {
	sched := mustParseSchedule(t, "0 0 * * *")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// 10 days of missed daily ticks.
	now := base.AddDate(0, 0, 10)
	want := base.AddDate(0, 0, 10)

	due, ok, tooMany := mostRecentDueTime(sched, base, now)
	if !ok {
		t.Fatalf("expected due")
	}
	if tooMany {
		t.Errorf("expected tooMany=false for only 10 missed ticks")
	}
	if !due.Equal(want) {
		t.Errorf("due = %v, want %v (only the latest missed tick, not the whole backlog)", due, want)
	}
}

func TestMostRecentDueTimeTooManyMissed(t *testing.T) {
	sched := mustParseSchedule(t, "* * * * *") // every minute
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// More than maxMissedSchedules (100) minutes have elapsed.
	now := base.Add(200 * time.Minute)
	// The true most-recent tick at or before now - not the 101st tick after
	// base (the bug), and not any earlier backlog tick.
	want := base.Add(200 * time.Minute)

	due, ok, tooMany := mostRecentDueTime(sched, base, now)
	if !ok {
		t.Fatalf("expected due")
	}
	if !tooMany {
		t.Errorf("expected tooMany=true when more than %d ticks are missed", maxMissedSchedules)
	}
	// Pin the exact tick: it must be the genuine latest (base+200m), so the
	// caller stamps ONE Backup for it and advances lastScheduleTime past the
	// whole backlog - no catch-up burst.
	if !due.Equal(want) {
		t.Errorf("due = %v, want %v (the genuine most-recent tick, not a stale backlog tick)", due, want)
	}
}

func TestMostRecentDueTimeJumpsDirectlyToLatestForHugeBacklog(t *testing.T) {
	// A year of missed minute-ticks: the arithmetic O(1) jump must still land
	// exactly on now's tick without walking (or capping partway through) the
	// ~525,600 missed ticks.
	sched := mustParseSchedule(t, "* * * * *")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base.AddDate(1, 0, 0)

	due, ok, tooMany := mostRecentDueTime(sched, base, now)
	if !ok || !tooMany {
		t.Fatalf("expected due and tooMany, got ok=%v tooMany=%v", ok, tooMany)
	}
	if !due.Equal(now) {
		t.Errorf("due = %v, want %v (exact latest tick after a year-long backlog)", due, now)
	}
}

func TestScheduleNeverFiresParsesButHasNoNextTime(t *testing.T) {
	// February 31 never occurs: cron.ParseStandard accepts it (dom 31, month
	// 2 are individually in range) but Next() finds no matching date within
	// its search horizon and returns the zero time.
	sched := mustParseSchedule(t, "0 0 31 2 *")
	if !scheduleNeverFires(sched, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected February 31 schedule to never fire")
	}

	// A real schedule must NOT be flagged as never-firing.
	daily := mustParseSchedule(t, "0 0 * * *")
	if scheduleNeverFires(daily, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("expected daily schedule to fire")
	}
}

func TestMostRecentDueTimeEarliestAfterNow(t *testing.T) {
	sched := mustParseSchedule(t, "0 0 * * *")
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	earliest := now.Add(time.Hour) // in the future relative to now

	_, ok, tooMany := mostRecentDueTime(sched, earliest, now)
	if ok {
		t.Errorf("expected not due when earliest is after now")
	}
	if tooMany {
		t.Errorf("expected tooMany=false")
	}
}

func TestScheduledBackupNameDeterministic(t *testing.T) {
	tick := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	a := scheduledBackupName("nightly", tick)
	b := scheduledBackupName("nightly", tick)
	if a != b {
		t.Fatalf("scheduledBackupName is not deterministic: %q != %q", a, b)
	}

	other := scheduledBackupName("nightly", tick.Add(time.Minute))
	if a == other {
		t.Errorf("expected different ticks to produce different names, both were %q", a)
	}
}
