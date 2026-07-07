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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

var _ = Describe("BackupSchedule Controller", func() {
	const resourceNamespace = "default"
	ctx := context.Background()

	// baseTime anchors every scenario's fake clock so tests never depend on
	// wall-clock time: status.lastScheduleTime is always seeded explicitly
	// (never left to fall back to the envtest apiserver's real
	// creationTimestamp), and r.Now is set relative to it.
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	newReconciler := func(rec events.EventRecorder, now time.Time) *BackupScheduleReconciler {
		return &BackupScheduleReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: rec,
			Now:      func() time.Time { return now },
		}
	}

	reconcileOnce := func(r *BackupScheduleReconciler, name string) reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
		return res
	}

	getSchedule := func(name string) *backupv1alpha1.BackupSchedule {
		s := &backupv1alpha1.BackupSchedule{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, s)).To(Succeed())
		return s
	}

	// seedSchedule creates a BackupSchedule and, if lastScheduleTime is
	// non-zero, immediately stamps status.lastScheduleTime to it - decoupling
	// "due" computation from the real creationTimestamp the envtest apiserver
	// assigns.
	seedSchedule := func(name, cronExpr string, suspend bool, retention backupv1alpha1.BackupRetentionPolicy, lastScheduleTime time.Time) {
		schedule := &backupv1alpha1.BackupSchedule{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec: backupv1alpha1.BackupScheduleSpec{
				Schedule: cronExpr,
				Suspend:  &suspend,
				BackupTemplate: backupv1alpha1.BackupSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
					Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
				},
				Retention: retention,
			},
		}
		Expect(k8sClient.Create(ctx, schedule)).To(Succeed())

		if !lastScheduleTime.IsZero() {
			t := metav1.NewTime(lastScheduleTime)
			schedule.Status.LastScheduleTime = &t
			Expect(k8sClient.Status().Update(ctx, schedule)).To(Succeed())
		}
	}

	listChildBackups := func(scheduleName string) []backupv1alpha1.Backup {
		var list backupv1alpha1.BackupList
		Expect(k8sClient.List(ctx, &list,
			client.InNamespace(resourceNamespace),
			client.MatchingLabels(builder.SelectorLabels(scheduleName, backupScheduleComponentName)),
		)).To(Succeed())
		return list.Items
	}

	// createChildBackup directly creates a Backup owned by an existing
	// schedule, standing in for one this reconciler would have stamped out,
	// so retention/status-aggregation specs can set up children with
	// arbitrary phases/completionTimes without driving the whole scheduling
	// state machine.
	createChildBackup := func(schedule *backupv1alpha1.BackupSchedule, name, phase string, completionTime time.Time) *backupv1alpha1.Backup {
		backup := &backupv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: resourceNamespace,
				Labels:    builder.Labels(schedule.Name, backupScheduleComponentName),
			},
			Spec: backupv1alpha1.BackupSpec{
				ClusterRef:  corev1.LocalObjectReference{Name: testClusterName},
				Destination: backupv1alpha1.BackupDestination{Driver: testDriverFilesystem},
			},
		}
		Expect(controllerutil.SetControllerReference(schedule, backup, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		backup.Status.Phase = phase
		if !completionTime.IsZero() {
			t := metav1.NewTime(completionTime)
			backup.Status.CompletionTime = &t
		}
		Expect(k8sClient.Status().Update(ctx, backup)).To(Succeed())
		return backup
	}

	findCondition := func(schedule *backupv1alpha1.BackupSchedule) *metav1.Condition {
		for i := range schedule.Status.Conditions {
			if schedule.Status.Conditions[i].Type == conditionTypeScheduleValid {
				return &schedule.Status.Conditions[i]
			}
		}
		return nil
	}

	Context("stamping out Backups on schedule", func() {
		It("creates exactly one Backup from the template when a tick is due", func() {
			const name = "sched-due"
			seedSchedule(name, testCronDaily, false, backupv1alpha1.BackupRetentionPolicy{}, baseTime)

			r := newReconciler(nil, baseTime.Add(25*time.Hour))
			res := reconcileOnce(r, name)

			wantTick := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
			children := listChildBackups(name)
			Expect(children).To(HaveLen(1))
			Expect(children[0].Name).To(Equal(scheduledBackupName(name, wantTick)))
			Expect(children[0].Spec.ClusterRef.Name).To(Equal(testClusterName))
			Expect(children[0].Spec.Destination.Driver).To(Equal(testDriverFilesystem))
			Expect(children[0].OwnerReferences).To(HaveLen(1))
			Expect(children[0].OwnerReferences[0].Name).To(Equal(name))
			Expect(*children[0].OwnerReferences[0].Controller).To(BeTrue())

			schedule := getSchedule(name)
			Expect(schedule.Status.LastScheduleTime).NotTo(BeNil())
			Expect(schedule.Status.LastScheduleTime.Time).To(BeTemporally("==", wantTick))
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		})

		It("creates nothing when no tick is due yet", func() {
			const name = "sched-not-due"
			seedSchedule(name, testCronDaily, false, backupv1alpha1.BackupRetentionPolicy{}, baseTime)

			r := newReconciler(nil, baseTime.Add(time.Hour))
			reconcileOnce(r, name)

			Expect(listChildBackups(name)).To(BeEmpty())
			schedule := getSchedule(name)
			Expect(schedule.Status.LastScheduleTime.Time).To(BeTemporally("==", baseTime))
		})

		It("does nothing but update status when suspended, even if a tick is due", func() {
			const name = "sched-suspended"
			seedSchedule(name, testCronDaily, true, backupv1alpha1.BackupRetentionPolicy{}, baseTime)

			r := newReconciler(nil, baseTime.Add(25*time.Hour))
			res := reconcileOnce(r, name)

			Expect(listChildBackups(name)).To(BeEmpty())
			schedule := getSchedule(name)
			Expect(schedule.Status.LastScheduleTime.Time).To(BeTemporally("==", baseTime))
			Expect(res.RequeueAfter).To(BeZero())
		})

		It("is idempotent for the same due tick across repeated reconciles", func() {
			const name = "sched-idempotent"
			seedSchedule(name, testCronDaily, false, backupv1alpha1.BackupRetentionPolicy{}, baseTime)

			now := baseTime.Add(25 * time.Hour)
			r := newReconciler(nil, now)
			reconcileOnce(r, name)
			reconcileOnce(r, name)
			reconcileOnce(r, name)

			Expect(listChildBackups(name)).To(HaveLen(1))
		})

		It("skips a long backlog and stamps only the most recent missed tick", func() {
			const name = "sched-backlog"
			// Every minute, 200 missed ticks - well past maxMissedSchedules.
			seedSchedule(name, "* * * * *", false, backupv1alpha1.BackupRetentionPolicy{}, baseTime)

			now := baseTime.Add(200 * time.Minute)
			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, now)
			reconcileOnce(r, name)

			// Exactly one Backup, not 200.
			Expect(listChildBackups(name)).To(HaveLen(1))
			Expect(rec.Events).To(Receive(ContainSubstring("TooManyMissedSchedules")))
		})
	})

	Context("cron validity", func() {
		It("surfaces an invalid-schedule condition and emits a Warning, without erroring, for a semantically-invalid cron", func() {
			const name = "sched-invalid"
			// Shape-valid (5 fields, passes CEL), but the minute/hour values
			// are out of range - only the reconciler's real cron parse catches
			// this.
			seedSchedule(name, "99 99 * * *", false, backupv1alpha1.BackupRetentionPolicy{}, time.Time{})

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, baseTime)
			res := reconcileOnce(r, name)

			schedule := getSchedule(name)
			cond := findCondition(schedule)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonInvalidSchedule))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
			Expect(listChildBackups(name)).To(BeEmpty())
			Expect(res.RequeueAfter).To(BeZero())
		})

		It("does not re-fire the Warning on a second reconcile of the same invalid schedule", func() {
			const name = "sched-invalid-dedup"
			seedSchedule(name, "99 99 * * *", false, backupv1alpha1.BackupRetentionPolicy{}, time.Time{})

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, baseTime)
			reconcileOnce(r, name)
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))

			reconcileOnce(r, name)
			Consistently(rec.Events).ShouldNot(Receive())
		})
	})

	Context("retention garbage collection", func() {
		It("keeps only the newest successfulBackupsHistoryLimit Completed backups", func() {
			const name = "sched-retention-count"
			seedSchedule(name, testCronDaily, true, backupv1alpha1.BackupRetentionPolicy{SuccessfulBackupsHistoryLimit: int32Ptr(2)}, baseTime)
			schedule := getSchedule(name)

			var kept []string
			for i := range 4 {
				childName := fmt.Sprintf("%s-child-%d", name, i)
				createChildBackup(schedule, childName, backupv1alpha1.BackupPhaseCompleted, baseTime.Add(time.Duration(i)*time.Hour))
				if i >= 2 { // the two newest (i=2,3) must survive
					kept = append(kept, childName)
				}
			}

			r := newReconciler(nil, baseTime)
			reconcileOnce(r, name)

			children := listChildBackups(name)
			names := make([]string, 0, len(children))
			for _, c := range children {
				names = append(names, c.Name)
			}
			Expect(names).To(ConsistOf(kept))
		})

		It("deletes any terminal backup older than maxAge, but keeps newer ones", func() {
			const name = "sched-retention-age"
			seedSchedule(name, testCronDaily, true, backupv1alpha1.BackupRetentionPolicy{
				MaxAge: &metav1.Duration{Duration: 24 * time.Hour},
			}, baseTime)
			schedule := getSchedule(name)

			now := baseTime.Add(72 * time.Hour)
			createChildBackup(schedule, name+"-old", backupv1alpha1.BackupPhaseCompleted, now.Add(-48*time.Hour))
			recentFailed := createChildBackup(schedule, name+"-recent-failed", backupv1alpha1.BackupPhaseFailed, now.Add(-1*time.Hour))

			r := newReconciler(nil, now)
			reconcileOnce(r, name)

			children := listChildBackups(name)
			names := make([]string, 0, len(children))
			for _, c := range children {
				names = append(names, c.Name)
			}
			Expect(names).To(ConsistOf(recentFailed.Name))
		})

		It("never deletes a Running backup regardless of age or count limit", func() {
			const name = "sched-retention-running"
			seedSchedule(name, testCronDaily, true, backupv1alpha1.BackupRetentionPolicy{
				SuccessfulBackupsHistoryLimit: int32Ptr(0),
				MaxAge:                        &metav1.Duration{Duration: time.Minute},
			}, baseTime)
			schedule := getSchedule(name)

			running := createChildBackup(schedule, name+"-running", backupv1alpha1.BackupPhaseRunning, time.Time{})

			r := newReconciler(nil, baseTime.Add(365*24*time.Hour))
			reconcileOnce(r, name)

			children := listChildBackups(name)
			Expect(children).To(HaveLen(1))
			Expect(children[0].Name).To(Equal(running.Name))
		})
	})

	Context("status aggregation", func() {
		It("maintains status.active for non-terminal children and status.lastSuccessfulTime for the newest Completed one", func() {
			const name = "sched-status"
			seedSchedule(name, testCronDaily, true, backupv1alpha1.BackupRetentionPolicy{}, baseTime)
			schedule := getSchedule(name)

			createChildBackup(schedule, name+"-running", backupv1alpha1.BackupPhaseRunning, time.Time{})
			createChildBackup(schedule, name+"-old-success", backupv1alpha1.BackupPhaseCompleted, baseTime.Add(1*time.Hour))
			newest := createChildBackup(schedule, name+"-new-success", backupv1alpha1.BackupPhaseCompleted, baseTime.Add(2*time.Hour))

			r := newReconciler(nil, baseTime)
			reconcileOnce(r, name)

			got := getSchedule(name)
			Expect(got.Status.Active).To(HaveLen(1))
			Expect(got.Status.Active[0].Name).To(Equal(name + "-running"))
			Expect(got.Status.LastSuccessfulTime).NotTo(BeNil())
			Expect(got.Status.LastSuccessfulTime.Time).To(BeTemporally("==", backupCompletionTime(newest)))
		})
	})
})
