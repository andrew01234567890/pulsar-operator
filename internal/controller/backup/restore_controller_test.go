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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

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
	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

// testRestoreJobBackoffReason is a placeholder JobCondition.Reason for a
// terminally-failed import Job: jobFailedPermanently only inspects the
// JobFailed condition's Status, never its Reason, so any distinct string
// here exercises the same code path as the real "BackoffLimitExceeded" a Job
// controller would report - and using a different literal than
// backup_controller_test.go's own keeps this file from duplicating it.
const testRestoreJobBackoffReason = "restore-job-backoff-limit-exceeded"

// testRestoreImportImage is a placeholder container image for manufactured
// pod fixtures; its value is never inspected by the reconciler.
const testRestoreImportImage = "restore-test-image"

// fakeTargetOxiaClient is an in-memory stand-in for the TARGET Oxia's
// "bookkeeper" namespace client, seeded with either a single INSTANCEID
// record (found) or none (a fresh, uninitialized target).
type fakeTargetOxiaClient struct {
	instanceID string
	found      bool
	scanErr    error
}

func (f *fakeTargetOxiaClient) RangeScanAll(_ context.Context) <-chan backuptool.ScanResult {
	ch := make(chan backuptool.ScanResult, 1)
	switch {
	case f.scanErr != nil:
		ch <- backuptool.ScanResult{Err: f.scanErr}
	case f.found:
		ch <- backuptool.ScanResult{Key: "/ledgers/INSTANCEID", Value: []byte(f.instanceID)}
	}
	close(ch)
	return ch
}

func (f *fakeTargetOxiaClient) Put(_ context.Context, _ string, _ []byte) error { return nil }
func (f *fakeTargetOxiaClient) Close() error                                    { return nil }

// fakeTargetOxiaFactory builds a RestoreReconciler.NewOxiaClientFactory that
// always hands back the same fake target state, regardless of the address
// requested - envtest has no real Oxia to connect to.
func fakeTargetOxiaFactory(instanceID string, found bool) func(string) backuptool.ClientFactory {
	return func(_ string) backuptool.ClientFactory {
		return func(_ string) (backuptool.OxiaClient, error) {
			return &fakeTargetOxiaClient{instanceID: instanceID, found: found}, nil
		}
	}
}

// fakeTargetOxiaErrorFactory builds a factory whose target client fails its
// RangeScanAll with scanErr, so ReadTargetInstanceID surfaces an error -
// standing in for a target Oxia whose existing lineage cannot be read (an
// unreachable/erroring metadata store), which must NOT be mistaken for a
// fresh target.
func fakeTargetOxiaErrorFactory(scanErr error) func(string) backuptool.ClientFactory {
	return func(_ string) backuptool.ClientFactory {
		return func(_ string) (backuptool.OxiaClient, error) {
			return &fakeTargetOxiaClient{scanErr: scanErr}, nil
		}
	}
}

var _ = Describe("Restore Controller", func() {
	const resourceNamespace = "default"
	const restoreTargetCluster = "restore-target-1"
	ctx := context.Background()

	ensureRestoreCluster := func(name string) {
		cluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
		}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, cluster)
		if err != nil {
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		}
	}

	newReconciler := func(rec events.EventRecorder, instanceID string, found bool) *RestoreReconciler {
		return &RestoreReconciler{
			Client:               k8sClient,
			Scheme:               k8sClient.Scheme(),
			Recorder:             rec,
			OperatorImage:        testOperatorImage,
			NewOxiaClientFactory: fakeTargetOxiaFactory(instanceID, found),
		}
	}

	reconcileOnce := func(r *RestoreReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	reconcileResult := func(r *RestoreReconciler, name string) reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
		return res
	}

	// reconcileExpectError drives one reconcile and returns its error, for the
	// transient-failure paths that must requeue (a non-nil error) rather than
	// terminally Fail.
	reconcileExpectError := func(r *RestoreReconciler, name string) error {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace},
		})
		return err
	}

	getRestore := func(name string) *backupv1alpha1.Restore {
		rst := &backupv1alpha1.Restore{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, rst)).To(Succeed())
		return rst
	}

	getImportJob := func(name string) *batchv1.Job {
		j := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: restoreJobName(name), Namespace: resourceNamespace}, j)).To(Succeed())
		return j
	}

	importJobExists := func(name string) bool {
		j := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: restoreJobName(name), Namespace: resourceNamespace}, j)
		return err == nil
	}

	// seedManifest uploads a manifest carrying just a header (this package's
	// tests never run the real import Job, so no records are needed) to dest
	// under key, returning the artifactURI a Restore.spec.source would carry.
	seedManifest := func(dest backupv1alpha1.BackupDestination, key, capturedInstanceID string) string {
		cfg := destConfig(dest)
		store, err := objectstore.New(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())
		header := backuptool.ManifestHeader{SchemaVersion: backuptool.SchemaVersion, CapturedInstanceID: capturedInstanceID}
		data, err := json.Marshal(header)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Upload(ctx, key, bytes.NewReader(data))).To(Succeed())
		return store.URI(key)
	}

	// seedRawManifest uploads arbitrary raw bytes to dest under key (bypassing
	// the manifest encoder), returning the artifactURI - used to plant a
	// present-but-undecodable manifest object.
	seedRawManifest := func(dest backupv1alpha1.BackupDestination, key string, data []byte) string {
		cfg := destConfig(dest)
		store, err := objectstore.New(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Upload(ctx, key, bytes.NewReader(data))).To(Succeed())
		return store.URI(key)
	}

	filesystemDest := func() backupv1alpha1.BackupDestination {
		return backupv1alpha1.BackupDestination{Driver: testDriverFilesystem, Bucket: GinkgoT().TempDir()}
	}

	createRestore := func(name string, dest backupv1alpha1.BackupDestination, artifactURI, policy string) {
		ensureRestoreCluster(restoreTargetCluster)
		restore := &backupv1alpha1.Restore{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec: backupv1alpha1.RestoreSpec{
				Source:              backupv1alpha1.RestoreSource{Destination: dest, ArtifactURI: artifactURI},
				TargetClusterRef:    corev1.LocalObjectReference{Name: restoreTargetCluster},
				CookieLineagePolicy: policy,
			},
		}
		Expect(k8sClient.Create(ctx, restore)).To(Succeed())
	}

	findRestoreCondition := func(restore *backupv1alpha1.Restore) *metav1.Condition {
		for i := range restore.Status.Conditions {
			if restore.Status.Conditions[i].Type == conditionTypeImportSucceeded {
				return &restore.Status.Conditions[i]
			}
		}
		return nil
	}

	Context("cookie-lineage pre-flight check", func() {
		It("Passes and creates the import Job when the target Oxia is fresh (no existing instanceId)", func() {
			const name = "restore-fresh-target"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "captured-instance-1")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "", false)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultPassed))
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhasePending))

			job := getImportJob(name)
			Expect(job.OwnerReferences).To(HaveLen(1))
			Expect(job.OwnerReferences[0].Name).To(Equal(name))
			Expect(job.OwnerReferences[0].Kind).To(Equal("Restore"))
			Expect(*job.OwnerReferences[0].Controller).To(BeTrue())

			c := job.Spec.Template.Spec.Containers[0]
			Expect(c.Image).To(Equal(testOperatorImage))
			Expect(c.Command).To(Equal([]string{managerBinary}))
			Expect(c.Args[0]).To(Equal("backup-import"))
			oxia, _ := argValue(c.Args, oxiaFlagName)
			Expect(oxia).To(Equal(restoreTargetCluster + "-oxia-oxia:6648"))
			driver, _ := argValue(c.Args, "--src-driver")
			Expect(driver).To(Equal(testDriverFilesystem))
			key, _ := argValue(c.Args, "--src-key")
			Expect(key).To(Equal("backup.manifest"))
		})

		It("Passes and creates the import Job when the target instanceId matches the backup's captured instanceId", func() {
			const name = "restore-matching-target"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "instance-abc-123", true)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultPassed))
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhasePending))
			Expect(importJobExists(name)).To(BeTrue())
		})

		It("halts BEFORE creating a Job on a Mismatch under the default enforce policy, with a Warning naming both instanceIds", func() {
			const name = "restore-mismatch-enforce"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "captured-instance-999")
			createRestore(name, dest, uri, "") // "" -> default (enforce)

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "target-instance-abc", true)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(restore.Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultMismatch))
			Expect(restore.Status.CookieLineageCheck.Detail).To(ContainSubstring("target-instance-abc"))
			Expect(restore.Status.CookieLineageCheck.Detail).To(ContainSubstring("captured-instance-999"))

			cond := findRestoreCondition(restore)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal(reasonCookieLineageMismatch))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))

			Expect(importJobExists(name)).To(BeFalse())
		})

		It("proceeds with a Warning on a Mismatch under the override policy, recording the mismatch in status", func() {
			const name = "restore-mismatch-override"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "captured-instance-999")
			createRestore(name, dest, uri, backupv1alpha1.CookieLineagePolicyOverride)

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "target-instance-abc", true)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).NotTo(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(restore.Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultMismatch))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))

			Expect(importJobExists(name)).To(BeTrue())
		})

		It("treats a manifest with no captured instanceId against an already-initialized target as Unknown, gated the same as Mismatch", func() {
			const name = "restore-unknown-lineage"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "") // no captured instanceId
			createRestore(name, dest, uri, "")

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "target-instance-abc", true)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(restore.Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultUnknown))
			Expect(importJobExists(name)).To(BeFalse())
		})

		It("fails with ArtifactNotFound when the backup artifact does not exist at the resolved location", func() {
			const name = "restore-missing-artifact"
			dest := filesystemDest()
			missingURI := objectstore.URI(destConfig(dest), "does-not-exist.manifest")
			createRestore(name, dest, missingURI, "")

			r := newReconciler(nil, "target-instance-abc", true)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonArtifactNotFound))
			Expect(importJobExists(name)).To(BeFalse())
		})

		It("requeues (never treats as fresh/passing) when the target Oxia's existing lineage cannot be read", func() {
			// Safety property: a target whose existing instanceId can't be read
			// must NOT be mistaken for a fresh target and Passed - the read
			// failure must requeue, leaving no lineage verdict and no import Job.
			const name = "restore-target-unreadable"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "", false)
			r.NewOxiaClientFactory = fakeTargetOxiaErrorFactory(errors.New("target oxia unreachable"))

			err := reconcileExpectError(r, name)
			Expect(err).To(HaveOccurred())

			restore := getRestore(name)
			Expect(restore.Status.CookieLineageCheck.Result).To(BeEmpty())
			Expect(restore.Status.Phase).NotTo(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(restore.Status.Phase).NotTo(Equal(backupv1alpha1.RestorePhaseCompleted))
			Expect(importJobExists(name)).To(BeFalse())
		})

		It("halts with a terminal ManifestUnreadable when the manifest object is present but its bytes cannot be decoded", func() {
			// After the transient-vs-terminal split, an object that EXISTS but
			// whose bytes are not a decodable manifest header is the only
			// terminal manifest failure: re-reading it will never help, so it
			// is a Failed/Unknown halt with no import Job.
			const name = "restore-garbage-manifest"
			dest := filesystemDest()
			uri := seedRawManifest(dest, "backup.manifest", []byte("this is not a valid manifest header at all"))
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "target-instance-abc", true)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(restore.Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultUnknown))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonManifestUnreadable))
			Expect(importJobExists(name)).To(BeFalse())
		})

		It("requeues (never Fails) when the source credentials Secret is momentarily unreadable", func() {
			// A transient credentials-Secret Get failure while peeking the
			// manifest header must requeue, not terminally Fail an otherwise
			// valid Restore. An aws-s3 destination forces the header read to
			// resolve a credentials Secret first; a missing one here stands in
			// for that transient Get failure (and never reaches a real S3 call).
			const name = "restore-transient-secret"
			dest := backupv1alpha1.BackupDestination{
				Driver:               testDriverAWSS3,
				Bucket:               testBucket,
				CredentialsSecretRef: &corev1.LocalObjectReference{Name: "missing-restore-secret"},
			}
			artifactURI := objectstore.URI(destConfig(dest), "backup.manifest")
			createRestore(name, dest, artifactURI, "")

			r := newReconciler(nil, "target-instance-abc", true)
			err := reconcileExpectError(r, name)
			Expect(err).To(HaveOccurred())

			restore := getRestore(name)
			Expect(restore.Status.CookieLineageCheck.Result).To(BeEmpty())
			Expect(restore.Status.Phase).NotTo(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(importJobExists(name)).To(BeFalse())
		})

		It("fails with ArtifactNotFound when source.artifactURI cannot be resolved against source.destination", func() {
			const name = "restore-unresolvable-artifact"
			dest := filesystemDest()
			createRestore(name, dest, "s3://some-other-bucket/backup.manifest", "")

			r := newReconciler(nil, "", false)
			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonArtifactNotFound))
			Expect(importJobExists(name)).To(BeFalse())
		})

		It("only runs the lineage check once - a later reconcile does not re-evaluate it", func() {
			const name = "restore-lineage-runs-once"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "instance-abc-123", true)
			reconcileOnce(r, name)
			Expect(getRestore(name).Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultPassed))

			// A second reconcile with a factory that would now report a
			// Mismatch must NOT flip the already-recorded result.
			r2 := newReconciler(nil, "a-completely-different-instance", true)
			reconcileOnce(r2, name)
			Expect(getRestore(name).Status.CookieLineageCheck.Result).To(Equal(backupv1alpha1.CookieLineageCheckResultPassed))
		})
	})

	Context("import Job status transitions", func() {
		It("advances Pending -> Running when the Job's pod is active", func() {
			const name = "restore-running"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "instance-abc-123", true)
			reconcileOnce(r, name)

			job := getImportJob(name)
			job.Status.Active = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)
			Expect(getRestore(name).Status.Phase).To(Equal(backupv1alpha1.RestorePhaseRunning))
		})

		It("completes with keysRestored read back from the Job's termination message", func() {
			const name = "restore-complete"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "instance-abc-123", true)
			reconcileOnce(r, name)

			job := getImportJob(name)
			result := backuptool.ImportResult{KeysRestored: 42, KeysSkippedEphemeral: 3, CapturedInstanceID: "instance-abc-123"}
			createTerminatedImportPod(ctx, name, job, result)

			job.Status.Succeeded = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseCompleted))
			Expect(restore.Status.KeysRestored).To(Equal(int64(42)))

			cond := findRestoreCondition(restore)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(reasonCompleted))
		})

		It("marks Failed and emits a Warning event when the Job fails", func() {
			const name = "restore-job-failed"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "instance-abc-123", true)
			reconcileOnce(r, name)

			job := getImportJob(name)
			now := metav1.Now()
			job.Status.StartTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: testRestoreJobBackoffReason, LastTransitionTime: now},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: testRestoreJobBackoffReason, LastTransitionTime: now},
			}
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonImportJobFailed))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
		})

		It("marks Failed when the Job reports success but its result is unreadable", func() {
			const name = "restore-noresult"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "instance-abc-123", true)
			reconcileOnce(r, name)

			job := getImportJob(name)
			createTerminatedImportPodNoResult(ctx, name, job)
			job.Status.Succeeded = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(restore.Status.KeysRestored).To(Equal(int64(0)))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonImportResultUnreadable))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
		})

		It("surfaces a Warning and requeues (does not fail) while an image pull is within the grace window", func() {
			const name = "restore-imgpull-grace"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "instance-abc-123", true)
			reconcileOnce(r, name)

			job := getImportJob(name)
			createStuckImagePullImportPod(ctx, name, job)
			job.Status.Active = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			res := reconcileResult(r, name)
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhasePending))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonImagePullError))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
		})

		It("marks Failed and emits a Warning once the image-pull grace period elapses", func() {
			const name = "restore-imgpull-expired"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			rec := events.NewFakeRecorder(8)
			r := newReconciler(rec, "instance-abc-123", true)
			reconcileOnce(r, name)

			job := getImportJob(name)
			pod := createStuckImagePullImportPod(ctx, name, job)
			job.Status.Active = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			r.Now = func() time.Time { return pod.CreationTimestamp.Add(importStuckGracePeriod + time.Minute) }

			reconcileOnce(r, name)

			restore := getRestore(name)
			Expect(restore.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(findRestoreCondition(restore).Reason).To(Equal(reasonImagePullError))
			Expect(rec.Events).To(Receive(ContainSubstring("Warning")))
		})
	})

	Context("when preconditions are not met", func() {
		It("fails with ClusterNotFound when the target PulsarCluster does not exist", func() {
			const name = "restore-no-cluster"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")

			restore := &backupv1alpha1.Restore{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
				Spec: backupv1alpha1.RestoreSpec{
					Source:           backupv1alpha1.RestoreSource{Destination: dest, ArtifactURI: uri},
					TargetClusterRef: corev1.LocalObjectReference{Name: "nonexistent-restore-cluster"},
				},
			}
			Expect(k8sClient.Create(ctx, restore)).To(Succeed())

			r := newReconciler(nil, "", false)
			reconcileOnce(r, name)

			got := getRestore(name)
			Expect(got.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(findRestoreCondition(got).Reason).To(Equal(reasonClusterNotFound))
		})

		It("fails with OperatorImageNotConfigured when OperatorImage is empty", func() {
			const name = "restore-no-operator-image"
			dest := filesystemDest()
			uri := seedManifest(dest, "backup.manifest", "instance-abc-123")
			createRestore(name, dest, uri, "")

			r := newReconciler(nil, "instance-abc-123", true)
			r.OperatorImage = ""
			reconcileOnce(r, name)

			got := getRestore(name)
			Expect(got.Status.Phase).To(Equal(backupv1alpha1.RestorePhaseFailed))
			Expect(findRestoreCondition(got).Reason).To(Equal(reasonOperatorImage))
		})
	})
})

// createTerminatedImportPod creates a pod controlled by job, carrying the
// import container's ImportResult in its terminated-container Message - the
// same channel the real import tool writes and the reconciler reads back.
func createTerminatedImportPod(ctx context.Context, restoreName string, job *batchv1.Job, result backuptool.ImportResult) {
	msg, err := json.Marshal(result)
	Expect(err).NotTo(HaveOccurred())

	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(restoreName) + "-pod",
			Namespace: job.Namespace,
			Labels:    builder.SelectorLabels(restoreName, restoreComponentName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: testJobOwnerAPIVer,
				Kind:       testJobOwnerKind,
				Name:       job.Name,
				UID:        job.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: importContainerName, Image: testRestoreImportImage}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodSucceeded,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: importContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: reasonCompleted, Message: string(msg)},
			},
		}},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// createTerminatedImportPodNoResult creates a controlled pod whose import
// container terminated with an EMPTY message - a succeeded Job with no
// readable ImportResult, which the reconciler must treat as a failure.
func createTerminatedImportPodNoResult(ctx context.Context, restoreName string, job *batchv1.Job) {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(restoreName) + "-pod",
			Namespace: job.Namespace,
			Labels:    builder.SelectorLabels(restoreName, restoreComponentName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: testJobOwnerAPIVer,
				Kind:       testJobOwnerKind,
				Name:       job.Name,
				UID:        job.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: importContainerName, Image: testRestoreImportImage}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodSucceeded,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: importContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: reasonCompleted, Message: ""},
			},
		}},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// createStuckImagePullImportPod creates a pod controlled by job whose import
// container is Waiting on ImagePullBackOff, returning the created pod so
// callers can anchor a fake clock to its CreationTimestamp.
func createStuckImagePullImportPod(ctx context.Context, restoreName string, job *batchv1.Job) *corev1.Pod {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(restoreName) + "-pod",
			Namespace: job.Namespace,
			Labels:    builder.SelectorLabels(restoreName, restoreComponentName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: testJobOwnerAPIVer,
				Kind:       testJobOwnerKind,
				Name:       job.Name,
				UID:        job.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: importContainerName, Image: testOperatorImage}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())

	pod.Status = corev1.PodStatus{
		Phase: corev1.PodPending,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:  importContainerName,
			Image: testOperatorImage,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: containerWaitingReasonImagePullBackOff},
			},
		}},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	return pod
}
