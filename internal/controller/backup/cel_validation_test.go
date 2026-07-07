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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
)

// This spec exercises the CEL (x-kubernetes-validations) admission rule
// generated from the +kubebuilder:validation:XValidation marker on
// BackupDestination (api/backup/v1alpha1/common_types.go). It asserts
// against the real envtest apiserver (k8sClient/ctx from suite_test.go),
// since CEL rules are evaluated by the apiserver itself and are invisible
// to a fake client. It also exercises that the three backup CRDs install
// and that a minimal CR of each round-trips (create + get).
var _ = Describe("CEL admission validation", func() {
	celCtx := context.Background()

	Context("BackupDestination bucket requirement", func() {
		It("rejects an aws-s3 destination without a bucket", func() {
			backup := &backupv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "backup-cel-no-bucket", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: testClusterName},
					Destination: backupv1alpha1.BackupDestination{
						Driver: testDriverAWSS3,
					},
				},
			}
			err := k8sClient.Create(celCtx, backup)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("bucket is required for object-store destination drivers"))
		})

		It("accepts an aws-s3 destination with a bucket", func() {
			backup := &backupv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "backup-cel-with-bucket", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: testClusterName},
					Destination: backupv1alpha1.BackupDestination{
						Driver: testDriverAWSS3,
						Bucket: "my-backup-bucket",
					},
				},
			}
			Expect(k8sClient.Create(celCtx, backup)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, backup)).To(Succeed())
		})

		It("rejects a Restore source with an azureblob destination without a bucket", func() {
			restore := &backupv1alpha1.Restore{
				ObjectMeta: metav1.ObjectMeta{Name: "restore-cel-no-bucket", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.RestoreSpec{
					Source: backupv1alpha1.RestoreSource{
						Destination: backupv1alpha1.BackupDestination{Driver: "azureblob"},
						ArtifactURI: "https://example.blob.core.windows.net/backups/backup-1.tar",
					},
					TargetClusterRef: corev1.LocalObjectReference{Name: testClusterName},
				},
			}
			err := k8sClient.Create(celCtx, restore)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("bucket is required for object-store destination drivers"))
		})
	})

	Context("BackupSchedule cron shape", func() {
		It("rejects an empty schedule", func() {
			schedule := &backupv1alpha1.BackupSchedule{
				ObjectMeta: metav1.ObjectMeta{Name: "schedule-cel-empty", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupScheduleSpec{
					Schedule: "",
					BackupTemplate: backupv1alpha1.BackupSpec{
						ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
						Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
					},
				},
			}
			err := k8sClient.Create(celCtx, schedule)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
		})

		It("rejects a schedule with too few cron fields", func() {
			schedule := &backupv1alpha1.BackupSchedule{
				ObjectMeta: metav1.ObjectMeta{Name: "schedule-cel-short", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupScheduleSpec{
					Schedule: "0 0 *",
					BackupTemplate: backupv1alpha1.BackupSpec{
						ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
						Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
					},
				},
			}
			err := k8sClient.Create(celCtx, schedule)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
		})

		It("accepts a well-formed five-field schedule", func() {
			schedule := &backupv1alpha1.BackupSchedule{
				ObjectMeta: metav1.ObjectMeta{Name: "schedule-cel-valid", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupScheduleSpec{
					Schedule: testCronDaily,
					BackupTemplate: backupv1alpha1.BackupSpec{
						ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
						Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
					},
				},
			}
			Expect(k8sClient.Create(celCtx, schedule)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, schedule)).To(Succeed())
		})

		It("accepts an @-prefixed macro schedule", func() {
			schedule := &backupv1alpha1.BackupSchedule{
				ObjectMeta: metav1.ObjectMeta{Name: "schedule-cel-macro", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupScheduleSpec{
					Schedule: "@daily",
					BackupTemplate: backupv1alpha1.BackupSpec{
						ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
						Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
					},
				},
			}
			Expect(k8sClient.Create(celCtx, schedule)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, schedule)).To(Succeed())
		})
	})

	Context("minimal CR round-trip", func() {
		It("creates and gets a minimal Backup", func() {
			backup := &backupv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "backup-roundtrip", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
					Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
				},
			}
			Expect(k8sClient.Create(celCtx, backup)).To(Succeed())

			got := &backupv1alpha1.Backup{}
			Expect(k8sClient.Get(celCtx, client.ObjectKeyFromObject(backup), got)).To(Succeed())
			Expect(got.Spec.ClusterRef.Name).To(Equal(testClusterName))
			Expect(got.Spec.Consistency).To(Equal(backupv1alpha1.BackupConsistencyCrash))
			Expect(*got.Spec.Components.OxiaMetadata).To(BeTrue())

			Expect(k8sClient.Delete(celCtx, backup)).To(Succeed())
		})

		It("creates and gets a minimal Restore", func() {
			restore := &backupv1alpha1.Restore{
				ObjectMeta: metav1.ObjectMeta{Name: "restore-roundtrip", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.RestoreSpec{
					Source: backupv1alpha1.RestoreSource{
						Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
						ArtifactURI: "file:///backups/test-cluster/backup-1.tar",
					},
					TargetClusterRef: corev1.LocalObjectReference{Name: testClusterName},
				},
			}
			Expect(k8sClient.Create(celCtx, restore)).To(Succeed())

			got := &backupv1alpha1.Restore{}
			Expect(k8sClient.Get(celCtx, client.ObjectKeyFromObject(restore), got)).To(Succeed())
			Expect(got.Spec.TargetClusterRef.Name).To(Equal(testClusterName))
			Expect(got.Spec.CookieLineagePolicy).To(Equal(backupv1alpha1.CookieLineagePolicyEnforce))
			Expect(*got.Spec.SkipEphemeral).To(BeTrue())

			Expect(k8sClient.Delete(celCtx, restore)).To(Succeed())
		})

		It("creates and gets a minimal BackupSchedule", func() {
			schedule := &backupv1alpha1.BackupSchedule{
				ObjectMeta: metav1.ObjectMeta{Name: "backupschedule-roundtrip", Namespace: testEnvtestNamespace},
				Spec: backupv1alpha1.BackupScheduleSpec{
					Schedule: testCronDaily,
					BackupTemplate: backupv1alpha1.BackupSpec{
						ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
						Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
					},
				},
			}
			Expect(k8sClient.Create(celCtx, schedule)).To(Succeed())

			got := &backupv1alpha1.BackupSchedule{}
			Expect(k8sClient.Get(celCtx, client.ObjectKeyFromObject(schedule), got)).To(Succeed())
			Expect(got.Spec.Schedule).To(Equal(testCronDaily))
			Expect(*got.Spec.Retention.SuccessfulBackupsHistoryLimit).To(Equal(int32(3)))
			Expect(*got.Spec.Suspend).To(BeFalse())

			Expect(k8sClient.Delete(celCtx, schedule)).To(Succeed())
		})
	})
})

// testEnvtestNamespace is the real Kubernetes Namespace envtest always
// provisions, used for objects that must actually round-trip through the
// API server.
const testEnvtestNamespace = "default"
