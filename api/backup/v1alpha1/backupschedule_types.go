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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// BackupRetentionPolicy controls retention of Backups created by a
// BackupSchedule.
type BackupRetentionPolicy struct {
	// successfulBackupsHistoryLimit caps the number of completed Backups
	// retained; older ones beyond the limit are eligible for cleanup.
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	SuccessfulBackupsHistoryLimit *int32 `json:"successfulBackupsHistoryLimit,omitempty"`

	// maxAge additionally bounds retention by age: Backups older than
	// maxAge are eligible for cleanup regardless of
	// successfulBackupsHistoryLimit.
	// +optional
	MaxAge *metav1.Duration `json:"maxAge,omitempty"`
}

// BackupScheduleSpec defines the desired state of BackupSchedule.
type BackupScheduleSpec struct {
	// schedule is the cron expression on which this BackupSchedule stamps
	// out a new Backup. Full parsing happens in the reconciler; this is
	// only checked for a non-empty, rough cron-like shape.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.size() > 0 && (self.startsWith('@') || self.split(' ').size() >= 5)",message="schedule must be a non-empty cron expression (5+ space-separated fields, or an @-prefixed macro like @daily)"
	Schedule string `json:"schedule"`

	// suspend pauses stamping out new Backups without affecting existing ones.
	// +optional
	// +kubebuilder:default=false
	Suspend *bool `json:"suspend,omitempty"`

	// backupTemplate is the Backup spec stamped into each Backup this
	// schedule creates, mirroring CronJob's jobTemplate pattern. clusterRef
	// is part of the template (as it is part of BackupSpec), so it only
	// needs to be set once here rather than duplicated at the schedule level.
	// +required
	BackupTemplate BackupSpec `json:"backupTemplate"`

	// retention controls how many completed Backups this schedule retains.
	// Uses "omitempty" rather than "omitzero" so an omitted retention block
	// still serializes as {} on the wire: the apiserver's structural-schema
	// defaulting only recurses into a sub-object's own field defaults (e.g.
	// successfulBackupsHistoryLimit) when that sub-object is present at all,
	// even as an empty object.
	// +optional
	Retention BackupRetentionPolicy `json:"retention,omitempty"`
}

// BackupScheduleStatus defines the observed state of BackupSchedule.
type BackupScheduleStatus struct {
	// lastScheduleTime is the last time a Backup was successfully stamped out.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// lastSuccessfulTime is the completion time of the most recent Backup
	// that reached phase Completed.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// active references the Backups currently in flight (not yet Completed
	// or Failed) for this schedule.
	// +optional
	Active []corev1.ObjectReference `json:"active,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the BackupSchedule resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Suspend",type=boolean,JSONPath=`.spec.suspend`
// +kubebuilder:printcolumn:name="Last Schedule",type=date,JSONPath=`.status.lastScheduleTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupSchedule is the Schema for the backupschedules API
type BackupSchedule struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BackupSchedule
	// +required
	Spec BackupScheduleSpec `json:"spec"`

	// status defines the observed state of BackupSchedule
	// +optional
	Status BackupScheduleStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackupScheduleList contains a list of BackupSchedule
type BackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BackupSchedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &BackupSchedule{}, &BackupScheduleList{})
		return nil
	})
}
