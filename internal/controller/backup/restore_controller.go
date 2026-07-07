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
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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
	// conditionTypeImportSucceeded is the Restore's single summary condition,
	// mirroring Backup's conditionTypeExportSucceeded.
	conditionTypeImportSucceeded = "ImportSucceeded"

	reasonImportJobFailed        = "ImportJobFailed"
	reasonImportResultUnreadable = "ImportResultUnreadable"
	reasonArtifactNotFound       = "ArtifactNotFound"
	reasonManifestUnreadable     = "ManifestUnreadable"
	reasonCookieLineageMismatch  = "CookieLineageMismatch"

	// importPollInterval backstops the Owns(&Job{}) watch, mirroring
	// exportPollInterval.
	importPollInterval = 15 * time.Second

	// importStuckGracePeriod mirrors exportStuckGracePeriod: how long a
	// wedged import pod is tolerated before the Restore is failed outright.
	importStuckGracePeriod = 5 * time.Minute
)

// errManifestUndecodable marks a manifest whose object was fetched
// successfully but whose header bytes are not a decodable manifest header (a
// genuine corruption/truncation). It is deliberately distinguished from a
// transport/credential failure fetching the object at all: only a real
// decode failure is a *terminal* Unknown lineage halt, whereas a transient
// download/secret error must requeue with backoff (mirroring the
// ReadTargetInstanceID sibling path) rather than permanently Fail an
// otherwise-valid Restore.
var errManifestUndecodable = errors.New("manifest header could not be decoded")

// RestoreReconciler reconciles a Restore object: it verifies BookKeeper
// cookie/instanceId lineage between the backup's manifest and the target
// cluster's current Oxia state *before* touching anything, then replays the
// manifest by launching an owner-ref'd import Job (the operator image's
// `manager backup-import` subcommand) and driving the Restore's status from
// the Job's observed state.
type RestoreReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// OperatorImage is the image the import Job runs (the operator's own
	// image, which carries the `manager backup-import` subcommand). Wired
	// from the OPERATOR_IMAGE env in cmd/main.go, exactly like the Backup
	// reconciler's own field.
	OperatorImage string

	// NewOxiaClientFactory builds a backuptool.ClientFactory for a given
	// Oxia service address; nil defaults to backuptool.NewOxiaClientFactory.
	// Tests inject a fake factory so the cookie-lineage pre-flight check
	// (which needs a live read of the TARGET Oxia's "bookkeeper" namespace)
	// can be exercised against fake target state - envtest has no real Oxia.
	NewOxiaClientFactory func(address string) backuptool.ClientFactory

	// Now returns the current time; nil defaults to time.Now. Tests override
	// it so the importStuckGracePeriod-gated fail logic needs no real sleep.
	Now func() time.Time
}

// now returns the current time, honoring an injected clock for tests.
func (r *RestoreReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// oxiaClientFactory builds a backuptool.ClientFactory for address, honoring
// an injected NewOxiaClientFactory for tests.
func (r *RestoreReconciler) oxiaClientFactory(address string) backuptool.ClientFactory {
	if r.NewOxiaClientFactory != nil {
		return r.NewOxiaClientFactory(address)
	}
	return backuptool.NewOxiaClientFactory(address)
}

// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=restores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=restores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.pulsaroperator.io,resources=restores/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=pulsarclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters,verbs=get;list;watch

// Reconcile drives a Restore through Pending -> Running -> Completed/Failed:
// it resolves the target cluster, runs the cookie-lineage pre-flight check
// exactly once (before any import Job ever exists), and then reconciles the
// import Job and maps its outcome back onto status.
func (r *RestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var restore backupv1alpha1.Restore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A Restore is a one-shot replay: once it reaches a terminal phase there
	// is nothing left to reconcile (its spec is immutable), so bail out
	// before touching the import Job (or re-running the lineage check) again.
	if restore.Status.Phase == backupv1alpha1.RestorePhaseCompleted ||
		restore.Status.Phase == backupv1alpha1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}

	base := restore.DeepCopy()
	restore.Status.ObservedGeneration = restore.Generation

	result, err := r.advance(ctx, &restore)

	if patchErr := r.patchStatus(ctx, base, &restore); patchErr != nil {
		if err == nil {
			err = patchErr
		}
	}
	return result, err
}

// advance mutates restore.Status toward the next phase and returns the
// requeue decision. It never persists status itself - Reconcile patches
// once, after.
func (r *RestoreReconciler) advance(ctx context.Context, restore *backupv1alpha1.Restore) (ctrl.Result, error) {
	if r.OperatorImage == "" {
		r.fail(restore, reasonOperatorImage,
			"operator image is not configured; set the OPERATOR_IMAGE env on the controller-manager")
		return ctrl.Result{}, nil
	}

	var cluster clusterv1alpha1.PulsarCluster
	clusterKey := types.NamespacedName{Name: restore.Spec.TargetClusterRef.Name, Namespace: restore.Namespace}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.fail(restore, reasonClusterNotFound,
				fmt.Sprintf("target PulsarCluster %q not found in namespace %q", clusterKey.Name, clusterKey.Namespace))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting target PulsarCluster %q: %w", clusterKey.Name, err)
	}

	srcKey, err := objectstore.KeyFromURI(destConfig(restore.Spec.Source.Destination), restore.Spec.Source.ArtifactURI)
	if err != nil {
		r.fail(restore, reasonArtifactNotFound, fmt.Sprintf(
			"cannot resolve source.artifactURI %q against source.destination: %s", restore.Spec.Source.ArtifactURI, err))
		return ctrl.Result{}, nil
	}

	// The cookie-lineage gate runs exactly once, and must win BEFORE an
	// import Job is ever created - not merely before that Job writes
	// anything - so that a mismatch under the default enforce policy leaves
	// no Job behind at all. Once CookieLineageCheck.Result is set, later
	// reconciles (polling the Job's progress) skip straight past it.
	if restore.Status.CookieLineageCheck.Result == "" {
		decision, err := r.checkCookieLineage(ctx, restore, &cluster, srcKey)
		if err != nil {
			return ctrl.Result{}, err
		}
		restore.Status.CookieLineageCheck = decision.check
		if decision.halt {
			r.fail(restore, decision.reason, decision.check.Detail)
			return ctrl.Result{}, nil
		}
		if decision.check.Result != backupv1alpha1.CookieLineageCheckResultPassed {
			// override: the mismatch/uncertainty is recorded in status above,
			// but proceeding on it must never be silent - a loud, supervised,
			// risk-acknowledged Warning every time this Restore reaches here.
			r.recorder().Eventf(restore, nil, corev1.EventTypeWarning, reasonCookieLineageMismatch, "Restore",
				"cookie lineage check result=%s (cookieLineagePolicy=override, proceeding anyway): %s",
				decision.check.Result, decision.check.Detail)
		}
	}

	return r.reconcileImportJob(ctx, restore, &cluster, srcKey)
}

// lineageDecision is checkCookieLineage's verdict: check is always recorded
// onto status; halt (with reason) means the Restore must be failed and no
// import Job created.
type lineageDecision struct {
	check  backupv1alpha1.CookieLineageCheck
	halt   bool
	reason string
}

// checkCookieLineage runs the restore's safety gate: it reads the TARGET
// Oxia's existing BookKeeper instanceId (a live, lightweight Oxia read - no
// object-store credentials needed) and, only if the target already has one,
// reads the backup manifest's header to learn its CapturedInstanceID. A
// fresh target (no existing instanceId) always Passes without needing the
// manifest at all: the import will simply write the captured lineage, so
// there is nothing to conflict with.
func (r *RestoreReconciler) checkCookieLineage(ctx context.Context, restore *backupv1alpha1.Restore, cluster *clusterv1alpha1.PulsarCluster, srcKey string) (lineageDecision, error) {
	targetAddr := oxiaExportAddress(cluster)
	targetInstanceID, targetFound, err := backuptool.ReadTargetInstanceID(ctx, r.oxiaClientFactory(targetAddr))
	if err != nil {
		return lineageDecision{}, fmt.Errorf("reading target Oxia instanceId: %w", err)
	}

	if !targetFound {
		return lineageDecision{check: backupv1alpha1.CookieLineageCheck{
			Result: backupv1alpha1.CookieLineageCheckResultPassed,
			Detail: "target Oxia has no existing BookKeeper instanceId (fresh cluster); the backup's captured lineage will be adopted by the import",
		}}, nil
	}

	header, err := r.readManifestHeader(ctx, restore, srcKey)
	if err != nil {
		switch {
		case objectstore.IsNotExist(err):
			// The manifest object genuinely does not exist at the resolved
			// location - a permanent misconfiguration, so halt.
			return lineageDecision{
				halt:   true,
				reason: reasonArtifactNotFound,
				check: backupv1alpha1.CookieLineageCheck{
					Result: backupv1alpha1.CookieLineageCheckResultUnknown,
					Detail: fmt.Sprintf("backup artifact %q was not found", restore.Spec.Source.ArtifactURI),
				},
			}, nil
		case errors.Is(err, errManifestUndecodable):
			// The object exists but its bytes are not a valid manifest header
			// (corrupt/truncated). Replaying it would be unsafe and re-reading
			// it will not fix it, so this is the one terminal Unknown halt.
			return lineageDecision{
				halt:   true,
				reason: reasonManifestUnreadable,
				check: backupv1alpha1.CookieLineageCheck{
					Result: backupv1alpha1.CookieLineageCheckResultUnknown,
					Detail: fmt.Sprintf("backup manifest could not be read: %s", err),
				},
			}, nil
		default:
			// A transient transport/credential failure (object-store 5xx or
			// throttle, DNS blip, a momentary credentials-Secret Get error).
			// Return it as a plain error so advance propagates it and Reconcile
			// requeues with backoff - never permanently Fail an otherwise-valid
			// Restore on a blip, matching the ReadTargetInstanceID sibling.
			return lineageDecision{}, err
		}
	}

	switch header.CapturedInstanceID {
	case "":
		detail := fmt.Sprintf(
			"target instanceId is %q but the backup manifest has no captured instanceId (its bookkeeper namespace wasn't captured, or predates cluster init); lineage cannot be verified",
			targetInstanceID)
		return r.gateOnPolicy(restore, backupv1alpha1.CookieLineageCheckResultUnknown, detail), nil
	case targetInstanceID:
		return lineageDecision{check: backupv1alpha1.CookieLineageCheck{
			Result: backupv1alpha1.CookieLineageCheckResultPassed,
			Detail: fmt.Sprintf("target instanceId %q matches the backup's captured instanceId", targetInstanceID),
		}}, nil
	default:
		detail := fmt.Sprintf("target instanceId %q does not match the backup's captured instanceId %q", targetInstanceID, header.CapturedInstanceID)
		return r.gateOnPolicy(restore, backupv1alpha1.CookieLineageCheckResultMismatch, detail), nil
	}
}

// gateOnPolicy decides whether a non-Passed lineage result halts the
// Restore, honoring spec.cookieLineagePolicy: enforce (the default) halts
// before any Job is created; override proceeds, but the mismatch/uncertainty
// is still recorded in status - overriding is a supervised,
// risk-acknowledged force, never a silent one (the caller emits a Warning
// event for it).
func (r *RestoreReconciler) gateOnPolicy(restore *backupv1alpha1.Restore, result, detail string) lineageDecision {
	check := backupv1alpha1.CookieLineageCheck{Result: result, Detail: detail}
	if cookieLineagePolicy(restore.Spec) == backupv1alpha1.CookieLineagePolicyOverride {
		return lineageDecision{check: check}
	}
	return lineageDecision{check: check, halt: true, reason: reasonCookieLineageMismatch}
}

// readManifestHeader peeks at the backup manifest's header (see
// backuptool.ReadManifestHeader) from inside the reconciler process. This is
// the one place in the operator that constructs an object-store Store
// outside a Job pod: every other object-store access happens inside a Job
// with its own dedicated environment/mounted credentials, but the
// cookie-lineage gate must know the manifest's CapturedInstanceID *before*
// any Job is created (see advance). To do that without touching the shared
// manager process's environment, it resolves destination.credentialsSecretRef
// itself and passes the values through objectstore.Config's inline
// credential fields (see that type's doc) - additive fields the Job's own
// path never sets, so this has no effect on the Job's env/volume-based
// credential wiring.
//
// Error contract (the caller relies on it to classify transient vs terminal):
// a missing object returns an objectstore.IsNotExist error; corrupt bytes
// return an errManifestUndecodable-wrapped error; every other failure
// (credentials-Secret Get, store construction, download transport) returns a
// plain error the caller requeues on.
func (r *RestoreReconciler) readManifestHeader(ctx context.Context, restore *backupv1alpha1.Restore, key string) (backuptool.ManifestHeader, error) {
	dest := restore.Spec.Source.Destination
	cfg := destConfig(dest)

	if dest.CredentialsSecretRef != nil {
		var secret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Name: dest.CredentialsSecretRef.Name, Namespace: restore.Namespace}, &secret); err != nil {
			return backuptool.ManifestHeader{}, fmt.Errorf("getting source credentials secret %q: %w", dest.CredentialsSecretRef.Name, err)
		}
		applySourceCredentials(&cfg, dest.Driver, secret.Data)
	}

	store, err := objectstore.New(ctx, cfg)
	if err != nil {
		return backuptool.ManifestHeader{}, fmt.Errorf("constructing object store for source.destination: %w", err)
	}
	rc, err := store.Download(ctx, key)
	if err != nil {
		// Preserve the object-store error verbatim so the caller can branch on
		// objectstore.IsNotExist (a genuinely-absent artifact) versus any
		// other download failure (a transient transport error that should
		// requeue).
		return backuptool.ManifestHeader{}, err
	}
	defer func() { _ = rc.Close() }()

	header, err := backuptool.ReadManifestHeader(rc)
	if err != nil {
		// The bytes were fetched but don't decode: tag this as terminal so the
		// caller distinguishes it from the transport errors above.
		return backuptool.ManifestHeader{}, fmt.Errorf("%w: %v", errManifestUndecodable, err)
	}
	return header, nil
}

// applySourceCredentials resolves a destination Secret's data onto cfg's
// inline credential fields, keyed by driver exactly like destCredentialEnv
// wires the same Secret onto a Job's environment - so the reconciler's
// in-process read authenticates identically to the Job it may go on to
// create.
func applySourceCredentials(cfg *objectstore.Config, driver string, data map[string][]byte) {
	switch driver {
	case objectstore.DriverAWSS3:
		cfg.AWSAccessKeyID = string(data[envAWSAccessKeyID])
		cfg.AWSSecretAccessKey = string(data[envAWSSecretAccessKey])
	case objectstore.DriverAzureBlob:
		cfg.AzureAccount = string(data[envAzureStorageAccount])
		cfg.AzureAccessKey = string(data[envAzureStorageAccessKey])
	case objectstore.DriverGCS:
		cfg.GCSCredentialsJSON = string(data[gcsKeySecretKey])
	}
}

// reconcileImportJob ensures the import Job exists and maps its observed
// state onto the Restore's phase/status - the mirror of
// BackupReconciler.reconcileExportJob.
func (r *RestoreReconciler) reconcileImportJob(ctx context.Context, restore *backupv1alpha1.Restore, cluster *clusterv1alpha1.PulsarCluster, srcKey string) (ctrl.Result, error) {
	name := restoreJobName(restore.Name)
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: restore.Namespace}, &job)
	switch {
	case apierrors.IsNotFound(err):
		return r.createImportJob(ctx, restore, cluster, srcKey)
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("getting import job %q: %w", name, err)
	}

	switch {
	case jobSucceeded(&job):
		return r.completeFromJob(ctx, restore, &job)
	case jobFailedPermanently(&job):
		r.fail(restore, reasonImportJobFailed, fmt.Sprintf("import job %q failed", name))
		return ctrl.Result{}, nil
	}

	stuck, err := r.podStuck(ctx, restore, &job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if stuck.stuck {
		return r.handleStuck(restore, name, stuck), nil
	}

	// Job exists, neither done nor failed nor wedged: Pending until its pod
	// is Active, Running once it is.
	if job.Status.Active > 0 {
		r.setInProgress(restore, backupv1alpha1.RestorePhaseRunning, reasonRunning,
			fmt.Sprintf("import job %q is running", name))
	} else {
		r.setInProgress(restore, backupv1alpha1.RestorePhasePending, reasonPending,
			fmt.Sprintf("import job %q is starting", name))
	}
	return ctrl.Result{RequeueAfter: importPollInterval}, nil
}

// handleStuck decides what to do about a wedged import pod (see podStuck),
// mirroring BackupReconciler.handleStuck exactly (a hard wedge gets a
// requeue-and-self-heal window before failing; a soft wait is plain Pending
// until it outlasts the grace period).
func (r *RestoreReconciler) handleStuck(restore *backupv1alpha1.Restore, jobName string, stuck podStuckInfo) ctrl.Result {
	elapsed := r.now().Sub(stuck.since)
	message := fmt.Sprintf("import job %q %s", jobName, stuck.message)

	switch {
	case stuck.hard && elapsed >= importStuckGracePeriod:
		r.fail(restore, stuck.reason, message)
		return ctrl.Result{}
	case stuck.hard:
		r.reportStuck(restore, stuck.reason, message)
		return ctrl.Result{RequeueAfter: importPollInterval}
	case elapsed >= importStuckGracePeriod:
		r.fail(restore, reasonPodStartTimeout, message)
		return ctrl.Result{}
	default:
		r.setInProgress(restore, backupv1alpha1.RestorePhasePending, reasonPending,
			fmt.Sprintf("import job %q pod is starting", jobName))
		return ctrl.Result{RequeueAfter: importPollInterval}
	}
}

// createImportJob builds and creates the owner-ref'd import Job and moves
// the Restore into Pending.
func (r *RestoreReconciler) createImportJob(ctx context.Context, restore *backupv1alpha1.Restore, cluster *clusterv1alpha1.PulsarCluster, srcKey string) (ctrl.Result, error) {
	job := buildImportJob(restore, cluster, srcKey, r.OperatorImage)
	if err := controllerutil.SetControllerReference(restore, job, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting owner reference on import job: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		return ctrl.Result{}, fmt.Errorf("creating import job: %w", err)
	}

	r.setInProgress(restore, backupv1alpha1.RestorePhasePending, reasonPending,
		fmt.Sprintf("import job %q created", job.Name))
	return ctrl.Result{RequeueAfter: importPollInterval}, nil
}

// completeFromJob reads the ImportResult back from the succeeded Job's pod
// termination message and moves the Restore to Completed - the mirror of
// BackupReconciler.completeFromJob. A Job that reports success but whose
// result cannot be read is a FAILURE, not a zeroed Completed, for the same
// reason Backup treats it that way: reporting an unverifiable restore as good
// is worse than reporting it failed.
func (r *RestoreReconciler) completeFromJob(ctx context.Context, restore *backupv1alpha1.Restore, job *batchv1.Job) (ctrl.Result, error) {
	result, ok, err := r.readImportResult(ctx, restore, job)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ok {
		r.fail(restore, reasonImportResultUnreadable, fmt.Sprintf(
			"import job %q reported success but no readable result was found in its pod termination message; the restore's effect cannot be verified",
			job.Name))
		return ctrl.Result{}, nil
	}

	restore.Status.KeysRestored = result.KeysRestored
	restore.Status.Phase = backupv1alpha1.RestorePhaseCompleted
	apimeta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:               conditionTypeImportSucceeded,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: restore.Generation,
		Reason:             reasonCompleted,
		Message:            fmt.Sprintf("restored %d Oxia key(s) from %s", result.KeysRestored, restore.Spec.Source.ArtifactURI),
	})
	r.recorder().Eventf(restore, nil, corev1.EventTypeNormal, reasonCompleted, "Restore",
		"restored %d Oxia key(s) from %s (capturedInstanceId=%q)", result.KeysRestored, restore.Spec.Source.ArtifactURI, result.CapturedInstanceID)
	return ctrl.Result{}, nil
}

// setInProgress sets a non-terminal phase (Pending/Running) and its matching
// ImportSucceeded=False condition.
func (r *RestoreReconciler) setInProgress(restore *backupv1alpha1.Restore, phase, reason, message string) {
	restore.Status.Phase = phase
	apimeta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:               conditionTypeImportSucceeded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: restore.Generation,
		Reason:             reason,
		Message:            message,
	})
}

// reportStuck records a still-recoverable wedge as a Pending/False condition
// and emits a Warning event, but only the first reconcile the wedge's reason
// is observed (deduped against the current condition).
func (r *RestoreReconciler) reportStuck(restore *backupv1alpha1.Restore, reason, message string) {
	prior := apimeta.FindStatusCondition(restore.Status.Conditions, conditionTypeImportSucceeded)
	changed := prior == nil || prior.Reason != reason
	r.setInProgress(restore, backupv1alpha1.RestorePhasePending, reason, message)
	if changed {
		r.recorder().Eventf(restore, nil, corev1.EventTypeWarning, reason, "Restore", "%s", message)
	}
}

// fail moves the Restore to Failed with a describing condition and emits a
// Warning event.
func (r *RestoreReconciler) fail(restore *backupv1alpha1.Restore, reason, message string) {
	restore.Status.Phase = backupv1alpha1.RestorePhaseFailed
	apimeta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:               conditionTypeImportSucceeded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: restore.Generation,
		Reason:             reason,
		Message:            message,
	})
	r.recorder().Eventf(restore, nil, corev1.EventTypeWarning, reason, "Restore", "%s", message)
}

// readImportResult recovers the ImportResult the import tool wrote to its
// container termination message - the mirror of
// BackupReconciler.readExportResult.
func (r *RestoreReconciler) readImportResult(ctx context.Context, restore *backupv1alpha1.Restore, job *batchv1.Job) (backuptool.ImportResult, bool, error) {
	log := logf.FromContext(ctx)
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(restore.Namespace),
		client.MatchingLabels(builder.SelectorLabels(restore.Name, restoreComponentName)),
	); err != nil {
		return backuptool.ImportResult{}, false, fmt.Errorf("listing import job pods to read result: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != importContainerName || cs.State.Terminated == nil {
				continue
			}
			msg := cs.State.Terminated.Message
			if msg == "" {
				continue
			}
			result, err := backuptool.ParseImportResult([]byte(msg))
			if err != nil {
				log.Error(err, "parsing import job termination message", "pod", pod.Name)
				return backuptool.ImportResult{}, false, nil
			}
			return result, true, nil
		}
	}
	log.Info("import job succeeded but no result termination message was found", "job", job.Name)
	return backuptool.ImportResult{}, false, nil
}

// podStuck inspects the import Job's pod(s) for a container wedged in a
// state that never trips the Job's own Failed condition - the mirror of
// BackupReconciler.podStuck, scoped to the import Job's own component/
// container names (kept as a separate, restore-specific implementation
// rather than a shared helper - see this package's other reconcilers for the
// same pattern duplicated per Job type).
func (r *RestoreReconciler) podStuck(ctx context.Context, restore *backupv1alpha1.Restore, job *batchv1.Job) (podStuckInfo, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(restore.Namespace),
		client.MatchingLabels(builder.SelectorLabels(restore.Name, restoreComponentName)),
	); err != nil {
		return podStuckInfo{}, fmt.Errorf("listing import job pods: %w", err)
	}

	var soft podStuckInfo
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		info, ok := importPodWedge(pod)
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

// importPodWedge classifies a single pod's import container as a hard
// wedge, a soft start wait, or not stuck - the mirror of exportPodWedge.
func importPodWedge(pod *corev1.Pod) (podStuckInfo, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != importContainerName || cs.State.Waiting == nil {
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
				message: fmt.Sprintf("container cannot be created (%s); check source.destination.credentialsSecretRef", reason),
				since:   pod.CreationTimestamp.Time,
			}, true
		case containerWaitingReasonContainerCreating, containerWaitingReasonPodInitializing, "":
			return podStuckInfo{
				stuck:   true,
				hard:    false,
				reason:  reasonPodStartTimeout,
				message: fmt.Sprintf("pod has not started (%s); check the source destination volume/secret can be mounted", waitingReasonOrUnknown(reason)),
				since:   pod.CreationTimestamp.Time,
			}, true
		}
	}
	return podStuckInfo{}, false
}

// patchStatus persists the Restore's status subresource only when it
// changed, via an optimistic merge against the pre-reconcile snapshot.
func (r *RestoreReconciler) patchStatus(ctx context.Context, base, updated *backupv1alpha1.Restore) error {
	if restoreStatusEqual(&base.Status, &updated.Status) {
		return nil
	}
	return r.Status().Patch(ctx, updated, client.MergeFrom(base))
}

// restoreStatusEqual reports whether two RestoreStatus values are
// semantically equal - the Restore-specific counterpart to statusEqual.
func restoreStatusEqual(a, b *backupv1alpha1.RestoreStatus) bool {
	return apiequality.Semantic.DeepEqual(a, b)
}

func (r *RestoreReconciler) recorder() events.EventRecorder {
	if r.Recorder != nil {
		return r.Recorder
	}
	return &events.FakeRecorder{}
}

// SetupWithManager sets up the controller with the Manager, watching the
// import Jobs it owns so a Job status change re-triggers reconciliation.
func (r *RestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.Restore{}).
		Owns(&batchv1.Job{}).
		Named("backup-restore").
		Complete(r)
}
