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

	reasonPending          = "Pending"
	reasonRunning          = "Running"
	reasonCompleted        = "Completed"
	reasonExportJobFailed  = "ExportJobFailed"
	reasonImagePullError   = "ImagePullError"
	reasonClusterNotFound  = "ClusterNotFound"
	reasonOperatorImage    = "OperatorImageNotConfigured"
	reasonConsistencyUnsup = "ApplicationConsistencyNotImplemented"

	// exportPollInterval backstops the Owns(&Job{}) watch: a Job whose pod is
	// stuck Waiting on an image pull never changes the Job object, so nothing
	// else re-triggers reconciliation - this keeps re-checking until it does.
	exportPollInterval = 15 * time.Second

	// Container Waiting reasons for an unpullable image; such a pod sits
	// Pending forever and never trips the Job's own Failed condition, so it is
	// surfaced explicitly (mirrors the metadata-init ImagePullBackOff fix).
	containerWaitingReasonImagePullBackOff = "ImagePullBackOff"
	containerWaitingReasonErrImagePull     = "ErrImagePull"
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
// running Job advances Pending->Running; a succeeded Job is read back into
// Completed; a terminally-failed Job, or one wedged on an image pull, becomes
// Failed with a Warning event.
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
		r.complete(ctx, backup, &job)
		return ctrl.Result{}, nil
	case jobFailedPermanently(&job):
		r.fail(backup, reasonExportJobFailed, fmt.Sprintf("export job %q failed", name))
		return ctrl.Result{}, nil
	}

	stuck, err := r.imagePullStuck(ctx, backup, &job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if stuck.stuck {
		r.fail(backup, reasonImagePullError,
			fmt.Sprintf("export job %q cannot pull image %q: %s", name, stuck.image, stuck.reason))
		return ctrl.Result{}, nil
	}

	// Job exists but is neither done nor failed: Pending until its pod is
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

// complete reads the ExportResult back from the succeeded Job's pod
// termination message and moves the Backup to Completed with the captured
// key/instanceId/size populated truthfully.
func (r *BackupReconciler) complete(ctx context.Context, backup *backupv1alpha1.Backup, job *batchv1.Job) {
	if result, ok := r.readExportResult(ctx, backup, job); ok {
		if result.ArtifactURI != "" {
			backup.Status.ArtifactURI = result.ArtifactURI
		}
		backup.Status.OxiaKeysCaptured = result.OxiaKeysCaptured
		backup.Status.CapturedInstanceID = result.CapturedInstanceID
		backup.Status.SizeBytes = result.SizeBytes
	}

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
// and filtered to ones this Job controls. A missing/unparsable result is not
// fatal - the Backup still completes (the manifest was uploaded), just with
// zeroed metrics - so it is logged rather than errored.
func (r *BackupReconciler) readExportResult(ctx context.Context, backup *backupv1alpha1.Backup, job *batchv1.Job) (backuptool.ExportResult, bool) {
	log := logf.FromContext(ctx)
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels(builder.SelectorLabels(backup.Name, backupComponentName)),
	); err != nil {
		log.Error(err, "listing export job pods to read result")
		return backuptool.ExportResult{}, false
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
				return backuptool.ExportResult{}, false
			}
			return result, true
		}
	}
	log.Info("export job succeeded but no result termination message was found", "job", job.Name)
	return backuptool.ExportResult{}, false
}

// imagePullInfo is the export Job's pod stuck Waiting on an image it cannot
// pull. The zero value means "not stuck".
type imagePullInfo struct {
	stuck  bool
	image  string
	reason string
}

// imagePullStuck inspects the export Job's pod(s) for the single container
// Waiting on ImagePullBackOff/ErrImagePull - a state that never trips the
// Job's Failed condition, so it must be detected directly to avoid a silent
// wedge (mirrors the metadata-init fix).
func (r *BackupReconciler) imagePullStuck(ctx context.Context, backup *backupv1alpha1.Backup, job *batchv1.Job) (imagePullInfo, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels(builder.SelectorLabels(backup.Name, backupComponentName)),
	); err != nil {
		return imagePullInfo{}, fmt.Errorf("listing export job pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != exportContainerName || cs.State.Waiting == nil {
				continue
			}
			switch cs.State.Waiting.Reason {
			case containerWaitingReasonImagePullBackOff, containerWaitingReasonErrImagePull:
				return imagePullInfo{stuck: true, image: cs.Image, reason: cs.State.Waiting.Reason}, nil
			}
		}
	}
	return imagePullInfo{}, nil
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
