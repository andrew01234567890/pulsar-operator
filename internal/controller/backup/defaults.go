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
	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

// defaultSuccessfulBackupsHistoryLimit mirrors the
// +kubebuilder:default=3 marker on BackupRetentionPolicy.SuccessfulBackupsHistoryLimit,
// for code paths that construct/inspect specs without going through the API
// server's defaulting (e.g. unit tests, or a schedule template copied
// in-process before being submitted).
const defaultSuccessfulBackupsHistoryLimit int32 = 3

// oxiaMetadataEnabled reports whether a Backup captures Oxia metadata,
// applying the CRD default (true) when unset.
func oxiaMetadataEnabled(components backupv1alpha1.BackupComponents) bool {
	return components.OxiaMetadata == nil || *components.OxiaMetadata
}

// includeEphemeralEnabled reports whether a Backup captures ephemeral
// state, applying the CRD default (false) when unset.
func includeEphemeralEnabled(spec backupv1alpha1.BackupSpec) bool {
	return spec.IncludeEphemeral != nil && *spec.IncludeEphemeral
}

// backupConsistency returns the effective consistency mode, applying the
// CRD default ("crash") when unset.
func backupConsistency(spec backupv1alpha1.BackupSpec) string {
	if spec.Consistency == "" {
		return backupv1alpha1.BackupConsistencyCrash
	}
	return spec.Consistency
}

// skipEphemeralEnabled reports whether a Restore skips ephemeral state,
// applying the CRD default (true) when unset.
func skipEphemeralEnabled(spec backupv1alpha1.RestoreSpec) bool {
	return spec.SkipEphemeral == nil || *spec.SkipEphemeral
}

// cookieLineagePolicy returns the effective cookie lineage policy, applying
// the CRD default ("enforce") when unset.
func cookieLineagePolicy(spec backupv1alpha1.RestoreSpec) string {
	if spec.CookieLineagePolicy == "" {
		return backupv1alpha1.CookieLineagePolicyEnforce
	}
	return spec.CookieLineagePolicy
}

// scheduleSuspended reports whether a BackupSchedule is suspended, applying
// the CRD default (false) when unset.
func scheduleSuspended(spec backupv1alpha1.BackupScheduleSpec) bool {
	return spec.Suspend != nil && *spec.Suspend
}

// successfulBackupsHistoryLimit returns the effective retention history
// limit, applying the CRD default (3) when unset.
func successfulBackupsHistoryLimit(retention backupv1alpha1.BackupRetentionPolicy) int32 {
	if retention.SuccessfulBackupsHistoryLimit == nil {
		return defaultSuccessfulBackupsHistoryLimit
	}
	return *retention.SuccessfulBackupsHistoryLimit
}
