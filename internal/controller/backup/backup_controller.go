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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	backupv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/backup/v1alpha1"
	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	backuptool "github.com/andrew01234567890/pulsar-operator/internal/backup"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

const (
	// conditionTypeExportSucceeded is the Backup's single summary condition:
	// True once the export Job has captured the manifest to object storage,
	// False (with a describing reason) while pending/running or on any failure.
	conditionTypeExportSucceeded = "ExportSucceeded"

	reasonPending                = "Pending"
	reasonRunning                = "Running"
	reasonCompleted              = "Completed"
	reasonExportJobFailed        = "ExportJobFailed"
	reasonImagePullError         = "ImagePullError"
	reasonContainerConfigError   = "ContainerConfigError"
	reasonPodStartTimeout        = "PodStartTimeout"
	reasonExportResultUnreadable = "ExportResultUnreadable"
	reasonClusterNotFound        = "ClusterNotFound"
	reasonOperatorImage          = "OperatorImageNotConfigured"
	reasonConsistencyUnsup       = "ApplicationConsistencyNotImplemented"

	// exportPollInterval backstops the Owns(&Job{}) watch: a Job whose pod is
	// stuck Waiting on an image pull never changes the Job object, so nothing
	// else re-triggers reconciliation - this keeps re-checking until it does.
	exportPollInterval = 15 * time.Second

	// exportStuckGracePeriod is how long a wedged pod (see podStuck) is
	// tolerated - surfaced as a Warning + requeue so it can self-heal if the
	// wedge clears (e.g. a transient Docker Hub 429 during the image pull) -
	// before the Backup is failed. A backup's spec is immutable, so failing
	// too eagerly would make a recoverable blip permanently unrecoverable;
	// this mirrors the metadata-init recreate grace.
	exportStuckGracePeriod = 5 * time.Minute

	// Container Waiting reasons that never trip a Job's own Failed condition
	// (BackoffLimit is never consumed), so the reconciler must detect them
	// directly. The image-pull pair is a hard, immediately-reported wedge; the
	// CreateContainer* pair is a hard credential/config wedge (a missing
	// secretKeyRef, an unmountable secret); ContainerCreating/PodInitializing
	// are soft, normal-at-first waits that only count as wedged once they
	// outlast exportStuckGracePeriod (e.g. a GCS secret whose JSON is stored
	// under a data key other than the required key.json can never mount).
	containerWaitingReasonImagePullBackOff     = "ImagePullBackOff"
	containerWaitingReasonErrImagePull         = "ErrImagePull"
	containerWaitingReasonCreateConfigError    = "CreateContainerConfigError"
	containerWaitingReasonCreateContainerError = "CreateContainerError"
	containerWaitingReasonContainerCreating    = "ContainerCreating"
	containerWaitingReasonPodInitializing      = "PodInitializing"
)

// BackupReconciler reconciles a Backup object: it captures the cluster's Oxia
// metadata to object storage by launching an owner-ref'd export Job (the
// operator image's `manager backup-export` subcommand) and driving the
// Backup's status from the Job's observed state.
type BackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// OperatorImage is the image the export Job runs (the operator's own
	// image, which carries the `manager backup-export` subcommand). Wired from
	// the OPERATOR_IMAGE env in cmd/main.go; an empty value fails the Backup
	// with a clear condition rather than launching an unrunnable Job.
	OperatorImage string

	// Now returns the current time; nil defaults to time.Now. Tests override
	// it so the exportStuckGracePeriod-gated fail logic needs no real sleep.
	Now func() time.Time
}

// now returns the current time, honoring an injected clock for tests.
func (r *BackupReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=pulsarclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters,verbs=get;list;watch

// Reconcile drives a Backup through Pending -> Running -> Completed/Failed by
// reconciling its export Job and reading the Job's outcome back into status.
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var backup backupv1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A Backup is a one-shot capture: once it reaches a terminal phase there
	// is nothing left to reconcile (its spec is immutable), so bail out before
	// touching the export Job again.
	if backup.Status.Phase == backupv1alpha1.BackupPhaseCompleted ||
		backup.Status.Phase == backupv1alpha1.BackupPhaseFailed {
		return ctrl.Result{}, nil
	}

	base := backup.DeepCopy()
	backup.Status.ObservedGeneration = backup.Generation

	result, err := r.advance(ctx, &backup)

	if patchErr := r.patchStatus(ctx, base, &backup); patchErr != nil {
		if err == nil {
			err = patchErr
		}
	}
	return result, err
}

// advance mutates backup.Status toward the next phase and returns the requeue
// decision. It never persists status itself - Reconcile patches once, after.
func (r *BackupReconciler) advance(ctx context.Context, backup *backupv1alpha1.Backup) (ctrl.Result, error) {
	// application-consistent capture (broker quiesce) is a later phase: surface
	// it as a Failed condition + Warning rather than silently doing a
	// crash-consistent capture the user didn't ask for.
	if backupConsistency(backup.Spec) == backupv1alpha1.BackupConsistencyApplication {
		r.fail(backup, reasonConsistencyUnsup,
			"application-consistent backups are not yet implemented; set spec.consistency: crash")
		return ctrl.Result{}, nil
	}

	if r.OperatorImage == "" {
		r.fail(backup, reasonOperatorImage,
			"operator image is not configured; set the OPERATOR_IMAGE env on the controller-manager")
		return ctrl.Result{}, nil
	}

	var cluster clusterv1alpha1.PulsarCluster
	clusterKey := types.NamespacedName{Name: backup.Spec.ClusterRef.Name, Namespace: backup.Namespace}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.fail(backup, reasonClusterNotFound,
				fmt.Sprintf("PulsarCluster %q not found in namespace %q", clusterKey.Name, clusterKey.Namespace))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting PulsarCluster %q: %w", clusterKey.Name, err)
	}

	return r.reconcileExportJob(ctx, backup, &cluster)
}

// reconcileExportJob ensures the export Job exists and maps its observed state
// onto the Backup's phase/status. A missing Job is created (phase Pending); a
// running Job advances Pending->Running; a succeeded Job whose result is
// readable becomes Completed (an unreadable result is a failure, never a
// zeroed "success"); a terminally-failed Job becomes Failed; a wedged pod is
// tolerated for exportStuckGracePeriod (Warning + requeue so it can self-heal)
// and only then failed.
func (r *BackupReconciler) reconcileExportJob(ctx context.Context, backup *backupv1alpha1.Backup, cluster *clusterv1alpha1.PulsarCluster) (ctrl.Result, error) {
	name := backupJobName(backup.Name)
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: backup.Namespace}, &job)
	switch {
	case apierrors.IsNotFound(err):
		return r.createExportJob(ctx, backup, cluster)
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("getting export job %q: %w", name, err)
	}

	switch {
	case jobSucceeded(&job):
		return r.completeFromJob(ctx, backup, &job)
	case jobFailedPermanently(&job):
		r.fail(backup, reasonExportJobFailed, fmt.Sprintf("export job %q failed", name))
		return ctrl.Result{}, nil
	}

	stuck, err := r.podStuck(ctx, backup, &job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if stuck.stuck {
		return r.handleStuck(backup, name, stuck), nil
	}

	// Job exists, neither done nor failed nor wedged: Pending until its pod is
	// Active, Running once it is.
	if job.Status.Active > 0 {
		r.setInProgress(backup, backupv1alpha1.BackupPhaseRunning, reasonRunning,
			fmt.Sprintf("export job %q is running", name))
	} else {
		r.setInProgress(backup, backupv1alpha1.BackupPhasePending, reasonPending,
			fmt.Sprintf("export job %q is starting", name))
	}
	return ctrl.Result{RequeueAfter: exportPollInterval}, nil
}

// handleStuck decides what to do about a wedged export pod (see podStuck): a
// hard wedge (image pull, container config) is surfaced as a Warning + requeue
// immediately so it can self-heal, and failed only once it has outlasted
// exportStuckGracePeriod; a soft wait (ContainerCreating) is reported as plain
// Pending until it outlasts the grace period, then failed as a start timeout.
func (r *BackupReconciler) handleStuck(backup *backupv1alpha1.Backup, jobName string, stuck podStuckInfo) ctrl.Result {
	elapsed := r.now().Sub(stuck.since)
	message := fmt.Sprintf("export job %q %s", jobName, stuck.message)

	switch {
	case stuck.hard && elapsed >= exportStuckGracePeriod:
		r.fail(backup, stuck.reason, message)
		return ctrl.Result{}
	case stuck.hard:
		r.reportStuck(backup, stuck.reason, message)
		return ctrl.Result{RequeueAfter: exportPollInterval}
	case elapsed >= exportStuckGracePeriod:
		r.fail(backup, reasonPodStartTimeout, message)
		return ctrl.Result{}
	default:
		r.setInProgress(backup, backupv1alpha1.BackupPhasePending, reasonPending,
			fmt.Sprintf("export job %q pod is starting", jobName))
		return ctrl.Result{RequeueAfter: exportPollInterval}
	}
}

// createExportJob builds and creates the owner-ref'd export Job, records the
// destination artifact URI it will write to, and moves the Backup into
// Pending with its startTime stamped.
func (r *BackupReconciler) createExportJob(ctx context.Context, backup *backupv1alpha1.Backup, cluster *clusterv1alpha1.PulsarCluster) (ctrl.Result, error) {
	job := buildExportJob(backup, cluster, r.OperatorImage)
	if err := controllerutil.SetControllerReference(backup, job, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting owner reference on export job: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		return ctrl.Result{}, fmt.Errorf("creating export job: %w", err)
	}

	if backup.Status.StartTime == nil {
		now := metav1.Now()
		backup.Status.StartTime = &now
	}
	backup.Status.ArtifactURI = objectstore.URI(destConfig(backup.Spec.Destination), manifestObjectKey(backup))
	r.setInProgress(backup, backupv1alpha1.BackupPhasePending, reasonPending,
		fmt.Sprintf("export job %q created", job.Name))
	return ctrl.Result{RequeueAfter: exportPollInterval}, nil
}

// completeFromJob reads the ExportResult back from the succeeded Job's pod
// termination message and moves the Backup to Completed with the captured
// key/instanceId/size populated truthfully. A Job that reports success but
// whose result cannot be read (missing/empty/unparsable termination message)
// is a FAILURE, not a zeroed Completed: reporting an unverifiable backup as
// good is worse than reporting it failed, and Restore's lineage gate depends
// on a real capturedInstanceId. A transient error listing the pod requeues
// rather than failing.
func (r *BackupReconciler) completeFromJob(ctx context.Context, backup *backupv1alpha1.Backup, job *batchv1.Job) (ctrl.Result, error) {
	result, ok, err := r.readExportResult(ctx, backup, job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ok {
		r.fail(backup, reasonExportResultUnreadable, fmt.Sprintf(
			"export job %q reported success but no readable result was found in its pod termination message; the backup's contents cannot be verified",
			job.Name))
		return ctrl.Result{}, nil
	}

	if result.ArtifactURI != "" {
		backup.Status.ArtifactURI = result.ArtifactURI
	}
	backup.Status.OxiaKeysCaptured = result.OxiaKeysCaptured
	backup.Status.CapturedInstanceID = result.CapturedInstanceID
	backup.Status.SizeBytes = result.SizeBytes

	backup.Status.Phase = backupv1alpha1.BackupPhaseCompleted
	if backup.Status.CompletionTime == nil {
		now := metav1.Now()
		backup.Status.CompletionTime = &now
	}
	apimeta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               conditionTypeExportSucceeded,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: backup.Generation,
		Reason:             reasonCompleted,
		Message:            fmt.Sprintf("captured %d Oxia key(s) to %s", backup.Status.OxiaKeysCaptured, backup.Status.ArtifactURI),
	})
	r.recorder().Eventf(backup, nil, corev1.EventTypeNormal, reasonCompleted, "Backup",
		"captured %d Oxia key(s) to %s", backup.Status.OxiaKeysCaptured, backup.Status.ArtifactURI)
	return ctrl.Result{}, nil
}

// setInProgress sets a non-terminal phase (Pending/Running) and its matching
// ExportSucceeded=False condition without stamping completionTime.
func (r *BackupReconciler) setInProgress(backup *backupv1alpha1.Backup, phase, reason, message string) {
	backup.Status.Phase = phase
	apimeta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               conditionTypeExportSucceeded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		Reason:             reason,
		Message:            message,
	})
}

// reportStuck records a still-recoverable wedge as a Pending/False condition
// and emits a Warning event, but only the first reconcile the wedge's reason
// is observed (deduped against the current condition) so a stuck pod doesn't
// re-fire the Warning on every exportPollInterval requeue.
func (r *BackupReconciler) reportStuck(backup *backupv1alpha1.Backup, reason, message string) {
	prior := apimeta.FindStatusCondition(backup.Status.Conditions, conditionTypeExportSucceeded)
	changed := prior == nil || prior.Reason != reason
	r.setInProgress(backup, backupv1alpha1.BackupPhasePending, reason, message)
	if changed {
		r.recorder().Eventf(backup, nil, corev1.EventTypeWarning, reason, "Backup", "%s", message)
	}
}

// fail moves the Backup to Failed with a describing condition, stamps
// completionTime, and emits a Warning event so the failure is visible via
// `kubectl describe`/`get events` rather than only in status.
func (r *BackupReconciler) fail(backup *backupv1alpha1.Backup, reason, message string) {
	backup.Status.Phase = backupv1alpha1.BackupPhaseFailed
	if backup.Status.CompletionTime == nil {
		now := metav1.Now()
		backup.Status.CompletionTime = &now
	}
	apimeta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               conditionTypeExportSucceeded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		Reason:             reason,
		Message:            message,
	})
	r.recorder().Eventf(backup, nil, corev1.EventTypeWarning, reason, "Backup", "%s", message)
}

// readExportResult recovers the ExportResult the export tool wrote to its
// container termination message. Pods are matched via the component selector
// and filtered to ones this Job controls. The bool is false when the result is
// definitively absent (no controlled pod, no terminated export container, an
// empty message, or an unparsable one) - the caller treats that as a failed
// backup. A non-nil error is a transient infrastructure failure (the pod List)
// that the caller should requeue on, NOT a verdict on the backup.
func (r *BackupReconciler) readExportResult(ctx context.Context, backup *backupv1alpha1.Backup, job *batchv1.Job) (backuptool.ExportResult, bool, error) {
	log := logf.FromContext(ctx)
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels(builder.SelectorLabels(backup.Name, backupComponentName)),
	); err != nil {
		return backuptool.ExportResult{}, false, fmt.Errorf("listing export job pods to read result: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != exportContainerName || cs.State.Terminated == nil {
				continue
			}
			msg := cs.State.Terminated.Message
			if msg == "" {
				continue
			}
			result, err := backuptool.ParseExportResult([]byte(msg))
			if err != nil {
				log.Error(err, "parsing export job termination message", "pod", pod.Name)
				return backuptool.ExportResult{}, false, nil
			}
			return result, true, nil
		}
	}
	log.Info("export job succeeded but no result termination message was found", "job", job.Name)
	return backuptool.ExportResult{}, false, nil
}

// podStuckInfo describes an export pod wedged before it can run. The zero value
// means "not stuck". hard distinguishes a genuine error (image pull, container
// config) that is surfaced immediately from a soft ContainerCreating wait that
// only counts as wedged once it outlasts the grace period. since is the stuck
// pod's CreationTimestamp - stable across reconciles (the pod is not recreated
// while wedged), so the caller can rate-limit the fail without persisting a
// timestamp of its own.
type podStuckInfo struct {
	stuck   bool
	hard    bool
	reason  string
	message string
	since   time.Time
}

// podStuck inspects the export Job's pod(s) for a container wedged in a state
// that never trips the Job's own Failed condition, so it must be detected
// directly (mirrors the metadata-init fix). Hard wedges (image pull, container
// config) win over soft ones; among pods, the first hard wedge found is
// returned, else the last soft wait.
func (r *BackupReconciler) podStuck(ctx context.Context, backup *backupv1alpha1.Backup, job *batchv1.Job) (podStuckInfo, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels(builder.SelectorLabels(backup.Name, backupComponentName)),
	); err != nil {
		return podStuckInfo{}, fmt.Errorf("listing export job pods: %w", err)
	}

	var soft podStuckInfo
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		info, ok := exportPodWedge(pod)
		if !ok {
			continue
		}
		if info.hard {
			return info, nil
		}
		soft = info
	}
	return soft, nil
}

// exportPodWedge classifies a single pod's export container as a hard wedge, a
// soft start wait, or not stuck (running/terminated, or no status yet).
func exportPodWedge(pod *corev1.Pod) (podStuckInfo, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != exportContainerName || cs.State.Waiting == nil {
			continue
		}
		reason := cs.State.Waiting.Reason
		switch reason {
		case containerWaitingReasonImagePullBackOff, containerWaitingReasonErrImagePull:
			return podStuckInfo{
				stuck:   true,
				hard:    true,
				reason:  reasonImagePullError,
				message: fmt.Sprintf("cannot pull image %q: %s", cs.Image, reason),
				since:   pod.CreationTimestamp.Time,
			}, true
		case containerWaitingReasonCreateConfigError, containerWaitingReasonCreateContainerError:
			return podStuckInfo{
				stuck:   true,
				hard:    true,
				reason:  reasonContainerConfigError,
				message: fmt.Sprintf("container cannot be created (%s); check destination.credentialsSecretRef", reason),
				since:   pod.CreationTimestamp.Time,
			}, true
		case containerWaitingReasonContainerCreating, containerWaitingReasonPodInitializing, "":
			return podStuckInfo{
				stuck:   true,
				hard:    false,
				reason:  reasonPodStartTimeout,
				message: fmt.Sprintf("pod has not started (%s); check the destination volume/secret can be mounted", waitingReasonOrUnknown(reason)),
				since:   pod.CreationTimestamp.Time,
			}, true
		}
	}
	return podStuckInfo{}, false
}

func waitingReasonOrUnknown(reason string) string {
	if reason == "" {
		return "ContainerCreating"
	}
	return reason
}

// patchStatus persists the Backup's status subresource only when it changed,
// via an optimistic merge against the pre-reconcile snapshot.
func (r *BackupReconciler) patchStatus(ctx context.Context, base, updated *backupv1alpha1.Backup) error {
	if statusEqual(&base.Status, &updated.Status) {
		return nil
	}
	return r.Status().Patch(ctx, updated, client.MergeFrom(base))
}

func (r *BackupReconciler) recorder() events.EventRecorder {
	if r.Recorder != nil {
		return r.Recorder
	}
	return &events.FakeRecorder{}
}

// SetupWithManager sets up the controller with the Manager, watching the
// export Jobs it owns so a Job status change re-triggers reconciliation.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.Backup{}).
		Owns(&batchv1.Job{}).
		Named("backup-backup").
		Complete(r)
}
