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

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

func boolPtr(v bool) *bool    { return &v }
func int32Ptr(v int32) *int32 { return &v }

func TestOxiaMetadataEnabled(t *testing.T) {
	tests := []struct {
		name       string
		components backupv1alpha1.BackupComponents
		want       bool
	}{
		{name: "unset defaults to true", components: backupv1alpha1.BackupComponents{}, want: true},
		{name: testCaseExplicitTrue, components: backupv1alpha1.BackupComponents{OxiaMetadata: boolPtr(true)}, want: true},
		{name: testCaseExplicitFalse, components: backupv1alpha1.BackupComponents{OxiaMetadata: boolPtr(false)}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := oxiaMetadataEnabled(tt.components); got != tt.want {
				t.Errorf("oxiaMetadataEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIncludeEphemeralEnabled(t *testing.T) {
	tests := []struct {
		name string
		spec backupv1alpha1.BackupSpec
		want bool
	}{
		{name: "unset defaults to false", spec: backupv1alpha1.BackupSpec{}, want: false},
		{name: testCaseExplicitTrue, spec: backupv1alpha1.BackupSpec{IncludeEphemeral: boolPtr(true)}, want: true},
		{name: testCaseExplicitFalse, spec: backupv1alpha1.BackupSpec{IncludeEphemeral: boolPtr(false)}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := includeEphemeralEnabled(tt.spec); got != tt.want {
				t.Errorf("includeEphemeralEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBackupConsistency(t *testing.T) {
	tests := []struct {
		name string
		spec backupv1alpha1.BackupSpec
		want string
	}{
		{name: "unset defaults to crash", spec: backupv1alpha1.BackupSpec{}, want: backupv1alpha1.BackupConsistencyCrash},
		{name: "explicit crash", spec: backupv1alpha1.BackupSpec{Consistency: backupv1alpha1.BackupConsistencyCrash}, want: backupv1alpha1.BackupConsistencyCrash},
		{name: "explicit application", spec: backupv1alpha1.BackupSpec{Consistency: backupv1alpha1.BackupConsistencyApplication}, want: backupv1alpha1.BackupConsistencyApplication},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := backupConsistency(tt.spec); got != tt.want {
				t.Errorf("backupConsistency() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSkipEphemeralEnabled(t *testing.T) {
	tests := []struct {
		name string
		spec backupv1alpha1.RestoreSpec
		want bool
	}{
		{name: "unset defaults to true", spec: backupv1alpha1.RestoreSpec{}, want: true},
		{name: testCaseExplicitTrue, spec: backupv1alpha1.RestoreSpec{SkipEphemeral: boolPtr(true)}, want: true},
		{name: testCaseExplicitFalse, spec: backupv1alpha1.RestoreSpec{SkipEphemeral: boolPtr(false)}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := skipEphemeralEnabled(tt.spec); got != tt.want {
				t.Errorf("skipEphemeralEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCookieLineagePolicy(t *testing.T) {
	tests := []struct {
		name string
		spec backupv1alpha1.RestoreSpec
		want string
	}{
		{name: "unset defaults to enforce", spec: backupv1alpha1.RestoreSpec{}, want: backupv1alpha1.CookieLineagePolicyEnforce},
		{name: "explicit enforce", spec: backupv1alpha1.RestoreSpec{CookieLineagePolicy: backupv1alpha1.CookieLineagePolicyEnforce}, want: backupv1alpha1.CookieLineagePolicyEnforce},
		{name: "explicit override", spec: backupv1alpha1.RestoreSpec{CookieLineagePolicy: backupv1alpha1.CookieLineagePolicyOverride}, want: backupv1alpha1.CookieLineagePolicyOverride},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cookieLineagePolicy(tt.spec); got != tt.want {
				t.Errorf("cookieLineagePolicy() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestScheduleSuspended(t *testing.T) {
	tests := []struct {
		name string
		spec backupv1alpha1.BackupScheduleSpec
		want bool
	}{
		{name: "unset defaults to false", spec: backupv1alpha1.BackupScheduleSpec{}, want: false},
		{name: testCaseExplicitTrue, spec: backupv1alpha1.BackupScheduleSpec{Suspend: boolPtr(true)}, want: true},
		{name: testCaseExplicitFalse, spec: backupv1alpha1.BackupScheduleSpec{Suspend: boolPtr(false)}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := scheduleSuspended(tt.spec); got != tt.want {
				t.Errorf("scheduleSuspended() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSuccessfulBackupsHistoryLimit(t *testing.T) {
	tests := []struct {
		name      string
		retention backupv1alpha1.BackupRetentionPolicy
		want      int32
	}{
		{name: "unset defaults to 3", retention: backupv1alpha1.BackupRetentionPolicy{}, want: 3},
		{name: "explicit zero", retention: backupv1alpha1.BackupRetentionPolicy{SuccessfulBackupsHistoryLimit: int32Ptr(0)}, want: 0},
		{name: "explicit ten", retention: backupv1alpha1.BackupRetentionPolicy{SuccessfulBackupsHistoryLimit: int32Ptr(10)}, want: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := successfulBackupsHistoryLimit(tt.retention); got != tt.want {
				t.Errorf("successfulBackupsHistoryLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}
