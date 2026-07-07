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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

// These specs pin the CRD marker defaults to two other sources of truth:
//
//  1. that the apiserver actually materializes them even for a raw
//     (kubectl-style) submission that omits the parent object entirely — the
//     case the typed Go client masks because it always serializes nested
//     structs as {}, and
//  2. that the Go nil-fallback helpers in defaults.go return the same value
//     the apiserver stamps, so the two encodings of each default can't drift
//     apart silently.
var _ = Describe("CRD default materialization", func() {
	defCtx := context.Background()

	// F1: a raw apply that omits `retention:` entirely must still get the
	// nested successfulBackupsHistoryLimit default (3). Submitted as
	// unstructured so `retention` is genuinely absent from the request bytes;
	// the typed client would serialize `retention: {}` and hide the gap.
	Context("BackupSchedule retention default via raw apply", func() {
		It("materializes successfulBackupsHistoryLimit=3 when retention is absent", func() {
			raw := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "backup.pulsaroperator.io/v1alpha1",
				"kind":       "BackupSchedule",
				"metadata": map[string]any{
					"name":      "schedule-raw-no-retention",
					"namespace": testEnvtestNamespace,
				},
				"spec": map[string]any{
					"schedule": testCronDaily,
					"backupTemplate": map[string]any{
						"clusterRef":  map[string]any{"name": testClusterName},
						"destination": map[string]any{"driver": testDriverFilesystem},
					},
				},
			}}

			_, found, err := unstructured.NestedFieldNoCopy(raw.Object, "spec", "retention")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse(), "retention must be absent from the submitted bytes for this test to be meaningful")

			Expect(k8sClient.Create(defCtx, raw)).To(Succeed())

			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(raw.GroupVersionKind())
			Expect(k8sClient.Get(defCtx, client.ObjectKeyFromObject(raw), got)).To(Succeed())

			limit, found, err := unstructured.NestedInt64(got.Object, "spec", "retention", "successfulBackupsHistoryLimit")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue(), "successfulBackupsHistoryLimit should be materialized by the apiserver")
			Expect(limit).To(Equal(int64(3)))

			Expect(k8sClient.Delete(defCtx, got)).To(Succeed())
		})
	})

	// F2: every +kubebuilder:default is re-encoded as an independent Go
	// nil-fallback in defaults.go. Create a minimal CR of each kind so the
	// apiserver applies the CRD markers, then assert the Go helper called on
	// the corresponding empty/nil field returns the SAME value. This ties the
	// marker and the helper together so neither can drift unnoticed.
	Context("Go helper defaults match apiserver-stamped defaults", func() {
		It("agrees on every defaulted field", func() {
			backup := &backupv1alpha1.Backup{}
			backup.SetName("backup-defaults-crosscheck")
			backup.SetNamespace(testEnvtestNamespace)
			backup.Spec = backupv1alpha1.BackupSpec{
				ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
				Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
			}
			Expect(k8sClient.Create(defCtx, backup)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(defCtx, backup)).To(Succeed()) })
			gotBackup := &backupv1alpha1.Backup{}
			Expect(k8sClient.Get(defCtx, client.ObjectKeyFromObject(backup), gotBackup)).To(Succeed())

			restore := &backupv1alpha1.Restore{}
			restore.SetName("restore-defaults-crosscheck")
			restore.SetNamespace(testEnvtestNamespace)
			restore.Spec = backupv1alpha1.RestoreSpec{
				Source: backupv1alpha1.RestoreSource{
					Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
					ArtifactURI: testArtifactURI,
				},
				TargetClusterRef: corev1.LocalObjectReference{Name: testClusterName},
			}
			Expect(k8sClient.Create(defCtx, restore)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(defCtx, restore)).To(Succeed()) })
			gotRestore := &backupv1alpha1.Restore{}
			Expect(k8sClient.Get(defCtx, client.ObjectKeyFromObject(restore), gotRestore)).To(Succeed())

			schedule := &backupv1alpha1.BackupSchedule{}
			schedule.SetName("schedule-defaults-crosscheck")
			schedule.SetNamespace(testEnvtestNamespace)
			schedule.Spec = backupv1alpha1.BackupScheduleSpec{
				Schedule: testCronDaily,
				BackupTemplate: backupv1alpha1.BackupSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
					Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
				},
			}
			Expect(k8sClient.Create(defCtx, schedule)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(defCtx, schedule)).To(Succeed()) })
			gotSchedule := &backupv1alpha1.BackupSchedule{}
			Expect(k8sClient.Get(defCtx, client.ObjectKeyFromObject(schedule), gotSchedule)).To(Succeed())

			// Pointer-typed defaulted fields must be non-nil after the
			// apiserver stamps them; guard here so the table below can
			// safely dereference to plain comparable values.
			Expect(gotBackup.Spec.Components.OxiaMetadata).NotTo(BeNil())
			Expect(gotBackup.Spec.IncludeEphemeral).NotTo(BeNil())
			Expect(gotRestore.Spec.SkipEphemeral).NotTo(BeNil())
			Expect(gotSchedule.Spec.Suspend).NotTo(BeNil())
			Expect(gotSchedule.Spec.Retention.SuccessfulBackupsHistoryLimit).NotTo(BeNil())

			cases := []struct {
				field  string
				server any
				helper any
			}{
				{
					field:  "backup.spec.components.oxiaMetadata",
					server: *gotBackup.Spec.Components.OxiaMetadata,
					helper: oxiaMetadataEnabled(backupv1alpha1.BackupComponents{}),
				},
				{
					field:  "backup.spec.consistency",
					server: gotBackup.Spec.Consistency,
					helper: backupConsistency(backupv1alpha1.BackupSpec{}),
				},
				{
					field:  "backup.spec.includeEphemeral",
					server: *gotBackup.Spec.IncludeEphemeral,
					helper: includeEphemeralEnabled(backupv1alpha1.BackupSpec{}),
				},
				{
					field:  "restore.spec.skipEphemeral",
					server: *gotRestore.Spec.SkipEphemeral,
					helper: skipEphemeralEnabled(backupv1alpha1.RestoreSpec{}),
				},
				{
					field:  "restore.spec.cookieLineagePolicy",
					server: gotRestore.Spec.CookieLineagePolicy,
					helper: cookieLineagePolicy(backupv1alpha1.RestoreSpec{}),
				},
				{
					field:  "backupschedule.spec.suspend",
					server: *gotSchedule.Spec.Suspend,
					helper: scheduleSuspended(backupv1alpha1.BackupScheduleSpec{}),
				},
				{
					field:  "backupschedule.spec.retention.successfulBackupsHistoryLimit",
					server: *gotSchedule.Spec.Retention.SuccessfulBackupsHistoryLimit,
					helper: successfulBackupsHistoryLimit(backupv1alpha1.BackupRetentionPolicy{}),
				},
			}

			for _, tc := range cases {
				By(tc.field)
				Expect(tc.helper).To(Equal(tc.server),
					"%s: Go helper default %v drifted from apiserver-stamped default %v", tc.field, tc.helper, tc.server)
			}
		})
	})
})
