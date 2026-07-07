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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

// jobSucceeded reports whether the export Job has completed successfully,
// preferring its Complete condition and falling back to the Succeeded counter
// (which envtest specs manufacture directly, there being no Job controller).
func jobSucceeded(job *batchv1.Job) bool {
	if c := findJobCondition(job, batchv1.JobComplete); c != nil {
		return c.Status == corev1.ConditionTrue
	}
	return job.Status.Succeeded > 0
}

// jobFailedPermanently reports whether the export Job has been marked
// terminally Failed (BackoffLimit exhausted).
func jobFailedPermanently(job *batchv1.Job) bool {
	c := findJobCondition(job, batchv1.JobFailed)
	return c != nil && c.Status == corev1.ConditionTrue
}

func findJobCondition(job *batchv1.Job, condType batchv1.JobConditionType) *batchv1.JobCondition {
	for i := range job.Status.Conditions {
		if job.Status.Conditions[i].Type == condType {
			return &job.Status.Conditions[i]
		}
	}
	return nil
}

// statusEqual reports whether two BackupStatus values are semantically equal,
// so the reconciler skips a status patch when nothing changed. Semantic
// equality handles metav1.Time and the conditions slice correctly.
func statusEqual(a, b *backupv1alpha1.BackupStatus) bool {
	return apiequality.Semantic.DeepEqual(a, b)
}
