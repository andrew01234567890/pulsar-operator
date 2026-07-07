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
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	backuptool "github.com/andrew01234567890/pulsar-operator/internal/backup"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

var _ = Describe("Backup Controller", func() {
	const resourceNamespace = "default"
	ctx := context.Background()

	// ensureCluster creates the referenced PulsarCluster if absent.
	ensureCluster := func(name string) {
		cluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
		}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, cluster)
		if err != nil {
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		}
	}

	newReconciler := func(rec events.EventRecorder) *BackupReconciler {
		return &BackupReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			Recorder:      rec,
			OperatorImage: testOperatorImage,
		}
	}

	reconcileOnce := func(r *BackupReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	getBackup := func(name string) *backupv1alpha1.Backup {
		b := &backupv1alpha1.Backup{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, b)).To(Succeed())
		return b
	}

	getJob := func(name string) *batchv1.Job {
		j := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backupJobName(name), Namespace: resourceNamespace}, j)).To(Succeed())
		return j
	}

	// createS3Backup creates a uniquely-named crash-consistent Backup targeting
	// aws-s3, so each spec runs against fully independent objects (envtest
	// shares one apiserver and deletes asynchronously).
	createS3Backup := func(name string) {
		ensureCluster("c1")
		backup := &backupv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec: backupv1alpha1.BackupSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "c1"},
				Destination: backupv1alpha1.BackupDestination{
					Driver:               testDriverAWSS3,
					Bucket:               testBucket,
					Region:               testRegionUSEast,
					Prefix:               testPrefixC1,
					CredentialsSecretRef: &corev1.LocalObjectReference{Name: testSecretS3},
				},
			},
		}
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())
	}

	Context("when reconciling a crash-consistent Backup to aws-s3", func() {
		It("creates an owner-ref'd export Job with the right image, args and creds, and goes Pending", func() {
			const name = "backup-s3-create"
			createS3Backup(name)
			reconcileOnce(newReconciler(nil), name)

			job := getJob(name)
			Expect(job.OwnerReferences).To(HaveLen(1))
			Expect(job.OwnerReferences[0].Name).To(Equal(name))
			Expect(job.OwnerReferences[0].Kind).To(Equal("Backup"))
			Expect(*job.OwnerReferences[0].Controller).To(BeTrue())

			c := job.Spec.Template.Spec.Containers[0]
			Expect(c.Image).To(Equal(testOperatorImage))
			Expect(c.Command).To(Equal([]string{managerBinary}))
			Expect(c.Args[0]).To(Equal("backup-export"))
			oxia, _ := argValue(c.Args, "--oxia")
			Expect(oxia).To(Equal("c1-oxia-oxia:6648"))
			driver, _ := argValue(c.Args, "--dest-driver")
			Expect(driver).To(Equal(testDriverAWSS3))
			key, _ := argValue(c.Args, "--dest-key")
			Expect(key).To(Equal(name + ".manifest"))
			Expect(findEnv(c.Env, envAWSAccessKeyID)).NotTo(BeNil())
			Expect(findEnv(c.Env, envAWSSecretAccessKey)).NotTo(BeNil())

			backup := getBackup(name)
			Expect(backup.Status.Phase).To(Equal(backupv1alpha1.BackupPhasePending))
			Expect(backup.Status.StartTime).NotTo(BeNil())
			Expect(backup.Status.ArtifactURI).To(Equal("s3://my-bucket/backups/c1/" + name + ".manifest"))
			Expect(backup.Status.ObservedGeneration).To(Equal(backup.Generation))
		})

		It("advances Pending -> Running when the Job's pod is active", func() {
			const name = "backup-s3-running"
			createS3Backup(name)
			r := newReconciler(nil)
			reconcileOnce(r, name)

			job := getJob(name)
			job.Status.Active = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)
			Expect(getBackup(name).Status.Phase).To(Equal(backupv1alpha1.BackupPhaseRunning))
		})

		It("completes with captured metrics read back from the Job's termination message", func() {
			const name = "backup-s3-complete"
			createS3Backup(name)
			r := newReconciler(nil)
			reconcileOnce(r, name)

			job := getJob(name)
			result := backuptool.ExportResult{
				ArtifactURI:        "s3://my-bucket/backups/c1/" + name + ".manifest",
				OxiaKeysCaptured:   42,
				CapturedInstanceID: "instance-abc-123",
				SizeBytes:          4096,
			}
			createTerminatedExportPod(ctx, name, job, result)

			job.Status.Succeeded = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)

			backup := getBackup(name)
			Expect(backup.Status.Phase).To(Equal(backupv1alpha1.BackupPhaseCompleted))
			Expect(backup.Status.CompletionTime).NotTo(BeNil())
			Expect(backup.Status.OxiaKeysCaptured).To(Equal(int64(42)))
			Expect(backup.Status.CapturedInstanceID).To(Equal("instance-abc-123"))
			Expect(backup.Status.SizeBytes).To(Equal(int64(4096)))

			cond := findCondition(backup)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonCompleted))
		})

		It("marks Failed and emits a Warning event when the Job fails", func() {
			const name = "backup-s3-failed"
			createS3Backup(name)
			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec)
			reconcileOnce(r, name)

			job := getJob(name)
			now := metav1.Now()
			job.Status.StartTime = &now
			// Recent apiservers require FailureTarget=true before Failed=true.
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", LastTransitionTime: now},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", LastTransitionTime: now},
			}
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)

			backup := getBackup(name)
			Expect(backup.Status.Phase).To(Equal(backupv1alpha1.BackupPhaseFailed))
			Expect(backup.Status.CompletionTime).NotTo(BeNil())
			cond := findCondition(backup)
			Expect(cond.Reason).To(Equal(reasonExportJobFailed))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
		})

		It("marks Failed and emits a Warning when the Job's pod is stuck on an image pull", func() {
			const name = "backup-s3-imgpull"
			createS3Backup(name)
			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec)
			reconcileOnce(r, name)

			job := getJob(name)
			createStuckImagePullPod(ctx, name, job)

			// The pod is stuck Pending on ImagePullBackOff; the Job itself never
			// trips Failed, so this exercises the direct image-pull detection.
			job.Status.Active = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)

			backup := getBackup(name)
			Expect(backup.Status.Phase).To(Equal(backupv1alpha1.BackupPhaseFailed))
			cond := findCondition(backup)
			Expect(cond.Reason).To(Equal(reasonImagePullError))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
		})
	})

	Context("when the requested consistency is not yet supported", func() {
		const name = "backup-app-consistency"

		AfterEach(func() {
			_ = k8sClient.Delete(ctx, &backupv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace}})
		})

		It("fails with an ApplicationConsistencyNotImplemented condition and creates no Job", func() {
			ensureCluster("c1")
			backup := &backupv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
				Spec: backupv1alpha1.BackupSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
					Consistency: backupv1alpha1.BackupConsistencyApplication,
					Destination: backupv1alpha1.BackupDestination{Driver: "filesystem"},
				},
			}
			Expect(k8sClient.Create(ctx, backup)).To(Succeed())

			rec := events.NewFakeRecorder(8)
			reconcileOnce(newReconciler(rec), name)

			got := getBackup(name)
			Expect(got.Status.Phase).To(Equal(backupv1alpha1.BackupPhaseFailed))
			Expect(findCondition(got).Reason).To(Equal(reasonConsistencyUnsup))

			job := &batchv1.Job{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: backupJobName(name), Namespace: resourceNamespace}, job)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when the referenced PulsarCluster does not exist", func() {
		const name = "backup-no-cluster"

		AfterEach(func() {
			_ = k8sClient.Delete(ctx, &backupv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace}})
		})

		It("fails with a ClusterNotFound condition", func() {
			backup := &backupv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
				Spec: backupv1alpha1.BackupSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: "nonexistent"},
					Destination: backupv1alpha1.BackupDestination{Driver: "filesystem"},
				},
			}
			Expect(k8sClient.Create(ctx, backup)).To(Succeed())

			reconcileOnce(newReconciler(events.NewFakeRecorder(8)), name)

			got := getBackup(name)
			Expect(got.Status.Phase).To(Equal(backupv1alpha1.BackupPhaseFailed))
			Expect(findCondition(got).Reason).To(Equal(reasonClusterNotFound))
		})
	})
})

// createTerminatedExportPod creates a pod controlled by job, carrying the
// export container's ExportResult in its terminated-container Message - the
// same channel the real export tool writes and the reconciler reads back.
func createTerminatedExportPod(ctx context.Context, backupName string, job *batchv1.Job, result backuptool.ExportResult) {
	msg, err := json.Marshal(result)
	Expect(err).NotTo(HaveOccurred())

	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupJobName(backupName) + "-pod",
			Namespace: job.Namespace,
			Labels:    builder.SelectorLabels(backupName, backupComponentName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       job.Name,
				UID:        job.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: exportContainerName, Image: "img"}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodSucceeded,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: exportContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", Message: string(msg)},
			},
		}},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// createStuckImagePullPod creates a pod controlled by job whose export
// container is Waiting on ImagePullBackOff - the kubelet state that leaves the
// pod Pending forever without ever tripping the Job's Failed condition.
func createStuckImagePullPod(ctx context.Context, backupName string, job *batchv1.Job) {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupJobName(backupName) + "-pod",
			Namespace: job.Namespace,
			Labels:    builder.SelectorLabels(backupName, backupComponentName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       job.Name,
				UID:        job.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: exportContainerName, Image: testOperatorImage}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodPending,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:  exportContainerName,
			Image: testOperatorImage,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: containerWaitingReasonImagePullBackOff},
			},
		}},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

func findCondition(backup *backupv1alpha1.Backup) *metav1.Condition {
	for i := range backup.Status.Conditions {
		if backup.Status.Conditions[i].Type == conditionTypeExportSucceeded {
			return &backup.Status.Conditions[i]
		}
	}
	return nil
}
