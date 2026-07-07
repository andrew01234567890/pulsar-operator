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

package cluster

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	"github.com/andrew01234567890/pulsar-operator/internal/config"
	oxiaurl "github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

const (
	// configKeyMetadataServiceURI is BookKeeper's bookkeeper.conf key for its
	// metadata store connection string (broker/proxy's equivalent,
	// configKeyMetadataStoreURL/configKeyConfigurationMetadataStoreURL, is
	// declared in proxy_controller.go and shared package-wide).
	configKeyMetadataServiceURI = "metadataServiceUri"

	// configKeyBookkeeperMetadataServiceURI is broker.conf's key telling the
	// broker's managed-ledger BookKeeper client where BookKeeper's OWN metadata
	// lives. It must equal the bookies' metadataServiceUri (the "bookkeeper"
	// Oxia namespace), NOT the broker's metadataStoreUrl (the "default"
	// namespace): absent it, the broker's BookKeeper client looks for available
	// bookies under its own metadata-store namespace, finds none, and every
	// ledger create fails "Not enough non-faulty bookies available" (rc=-6)
	// even though healthy, writable bookies are registered under the
	// "bookkeeper" namespace.
	configKeyBookkeeperMetadataServiceURI = "bookkeeperMetadataServiceUri"

	// configKeyClusterName is broker.conf/proxy.conf's key naming the Pulsar
	// cluster a component belongs to. Pulsar 5.0.0-M1 refuses to start
	// without it: the broker errors "Required clusterName is null" and the
	// proxy "Cluster name cannot be empty".
	configKeyClusterName = "clusterName"

	// conditionTypeMetadataInitialized tracks the one-time bin/pulsar
	// initialize-cluster-metadata Job that bootstraps the cluster's root
	// metadata in Oxia (cluster name, default namespace, broker/web service
	// URLs). A PulsarCluster is never Ready until this has succeeded: no
	// broker can serve traffic for a cluster name Oxia has never heard of.
	conditionTypeMetadataInitialized = "MetadataInitialized"

	reasonMetadataInitWaitingForOxia = "WaitingForOxia"
	reasonMetadataInitJobRunning     = "JobRunning"
	reasonMetadataInitJobFailed      = "JobFailed"
	reasonMetadataInitJobSucceeded   = "JobSucceeded"

	// reasonMetadataInitImagePullError reports the metadata-init Job's pod
	// stuck Waiting on ImagePullBackOff/ErrImagePull (see imagePullStuckInfo):
	// unlike a terminally-Failed Job, a stuck pull never flips the Job's own
	// Failed condition, so without this distinct reason the cluster would
	// look like it's merely still JobRunning forever.
	reasonMetadataInitImagePullError = "InitImagePullError"

	metadataInitComponentName = "metadata-init"
	metadataInitContainerName = "initialize-cluster-metadata"

	// containerWaitingReasonImagePullBackOff/ErrImagePull are the kubelet
	// Waiting-state reasons for a container whose image can't be pulled - a
	// nonexistent tag, a private registry it lacks credentials for, etc. The
	// pod never fails and is never recreated while in this state (the
	// kubelet just keeps retrying the pull on the same pod, backing off up to
	// ~5m between attempts), which is exactly why it never trips the Job's
	// own BackoffLimit/Failed condition.
	containerWaitingReasonImagePullBackOff = "ImagePullBackOff"
	containerWaitingReasonErrImagePull     = "ErrImagePull"

	// metadataInitBackoffLimit bounds the Job's own pod-level retries (the pod
	// RestartPolicy is OnFailure) before the Job is marked Failed. Once it
	// exhausts these, the operator deletes and recreates the Job for a fresh
	// attempt, so a transient dependency (e.g. Oxia not yet accepting writes)
	// never wedges the cluster on a terminally-Failed Job.
	metadataInitBackoffLimit int32 = 6

	// metadataInitRetryInterval spaces out the operator-level recreate of a
	// terminally-Failed init Job, or one stuck on an image pull (see
	// imagePullStuckInfo), so a hard misconfiguration doesn't spin in a tight
	// delete/recreate loop.
	metadataInitRetryInterval = 30 * time.Second
)

// oxiaPublicServiceName returns the Service name Pulsar/BookKeeper/
// FunctionsWorker components address to reach the OxiaCluster child this
// PulsarCluster provisions (see reconcileOxia/childName): the OxiaCluster
// reconciler's own client-facing "public" Service, backed by oxia-server
// pods. It must never be the coordinator's Service, which only assigns
// shards and never serves client reads/writes (see internal/metadata's own
// regression test guarding this).
func oxiaPublicServiceName(clusterName string) string {
	return oxiaurl.PublicServiceName(childName(clusterName, "oxia"))
}

// withBrokerProxyMetadataDefaults sets metadataStoreUrl and
// configurationMetadataStoreUrl to the cluster's Oxia metadata store, unless
// the user already set either. Broker and proxy conventionally point both
// keys at the same store/namespace (there is only one physical cluster here,
// so there is no separate "configuration store" to split them across).
func withBrokerProxyMetadataDefaults(cfg map[string]string, clusterName string) map[string]string {
	url := oxiaurl.MetadataStoreURL(oxiaPublicServiceName(clusterName), oxiaurl.DefaultNamespace)
	cfg = setConfigDefault(cfg, configKeyMetadataStoreURL, url)
	cfg = setConfigDefault(cfg, configKeyConfigurationMetadataStoreURL, url)
	return cfg
}

// withClusterNameDefault sets clusterName to the PulsarCluster's own name,
// unless the user already set it. See configKeyClusterName for why this is
// required, not cosmetic.
func withClusterNameDefault(cfg map[string]string, clusterName string) map[string]string {
	return setConfigDefault(cfg, configKeyClusterName, clusterName)
}

// withBookKeeperMetadataDefault sets metadataServiceUri to the cluster's Oxia
// metadata store, unless the user already set it.
func withBookKeeperMetadataDefault(cfg map[string]string, clusterName string) map[string]string {
	uri := oxiaurl.BookkeeperMetadataServiceURI(oxiaPublicServiceName(clusterName))
	return setConfigDefault(cfg, configKeyMetadataServiceURI, uri)
}

// withAutoRecoveryMetadataDefault sets metadataServiceUri to the SAME
// metadata-store:oxia://.../bookkeeper URI the bookies register under (see
// withBookKeeperMetadataDefault), unless the user already set it.
// AutoRecovery's dedicated Auditor + ReplicationWorker share BookKeeper's own
// metadata store, so it must resolve to the identical bookies as the bookie
// tier. The standalone AutoRecovery reconciler deliberately leaves
// metadataServiceUri blank (see autoRecoveryDefaultConfig) since a bare
// AutoRecovery has no notion of which metadata store implementation the
// cluster uses; the umbrella PulsarCluster reconciler is the one place with
// that context, exactly as it is for BookKeeper.
func withAutoRecoveryMetadataDefault(cfg map[string]string, clusterName string) map[string]string {
	return withBookKeeperMetadataDefault(cfg, clusterName)
}

// withBrokerBookkeeperMetadataDefault sets the broker's
// bookkeeperMetadataServiceUri to the SAME metadata-store:oxia://.../bookkeeper
// URI the bookies register under (see withBookKeeperMetadataDefault), unless
// the user already set it. Without this the broker's BookKeeper client can't
// find the bookies (see configKeyBookkeeperMetadataServiceURI).
func withBrokerBookkeeperMetadataDefault(cfg map[string]string, clusterName string) map[string]string {
	uri := oxiaurl.BookkeeperMetadataServiceURI(oxiaPublicServiceName(clusterName))
	return setConfigDefault(cfg, configKeyBookkeeperMetadataServiceURI, uri)
}

// withFunctionsWorkerMetadataDefault sets configurationMetadataStoreUrl (the
// only metadata-store key functions_worker.yml has) to the cluster's Oxia
// metadata store, unless the user already set it.
func withFunctionsWorkerMetadataDefault(cfg map[string]string, clusterName string) map[string]string {
	url := oxiaurl.MetadataStoreURL(oxiaPublicServiceName(clusterName), oxiaurl.DefaultNamespace)
	return setConfigDefault(cfg, configKeyConfigurationMetadataStoreURL, url)
}

// setConfigDefault sets cfg[key]=value unless the user already set key,
// allocating cfg if nil. Callers always pass a spec.Config already copied out
// of the PulsarCluster's own sub-spec (via buildXSpec's DeepCopy), so
// mutating it in place never reaches back into the stored PulsarCluster spec.
func setConfigDefault(cfg map[string]string, key, value string) map[string]string {
	if cfg == nil {
		cfg = make(map[string]string, 1)
	}
	if _, ok := cfg[key]; !ok {
		cfg[key] = value
	}
	return cfg
}

// metadataInitJobName deterministically names the cluster-metadata-init Job
// after its owning PulsarCluster.
func metadataInitJobName(clusterName string) string {
	return clusterName + "-metadata-init"
}

// metadataInitBrokerServiceURLs derives the broker's advertised web and
// binary service URLs the same way the Broker reconciler binds its ports:
// from the merged broker.conf, so a user override of webServicePort/
// brokerServicePort is honored here too. This matters because
// initialize-cluster-metadata PERSISTS these URLs in Oxia as the cluster's
// advertised addresses (used for cross-cluster lookup / geo-replication);
// hardcoding the defaults would silently register the wrong ports whenever a
// user overrides them. When no Broker is configured, the CRD-default ports
// are used (the Service name is still deterministic).
func metadataInitBrokerServiceURLs(cluster *clusterv1alpha1.PulsarCluster) (web, broker string) {
	svc := childName(cluster.Name, brokerComponent)
	ports := brokerPorts{binary: defaultBrokerServicePort, http: defaultWebServicePort}
	if cluster.Spec.Broker != nil {
		ports = resolveBrokerPorts(config.Merge(defaultBrokerConfig(*cluster.Spec.Broker), cluster.Spec.Broker.Config))
	}
	web = fmt.Sprintf("http://%s:%d", svc, ports.http)
	broker = fmt.Sprintf("pulsar://%s:%d", svc, ports.binary)
	return web, broker
}

// metadataInitScriptTemplate is the metadata-init Job's single container
// script. BookKeeper has its OWN cluster metadata, separate from Pulsar's:
// without initializing it first, bookies abort at startup with "BookKeeper
// cluster not initialized" and the broker can never create its
// loadbalancer-service-unit-state ledger ("Not enough non-faulty bookies
// available", rc=-6), so broker and proxy crashloop forever. This mirrors
// pulsar-helm-chart's separate bookkeeper-cluster-initialize.yaml Job, folded
// into one script so the whole bootstrap stays a single idempotent Job:
//   - `bin/bookkeeper shell whatisinstanceid` succeeding means BookKeeper's
//     cluster metadata already exists, exactly the check-then-init guard the
//     helm chart's own init job uses. `bin/bookkeeper shell initnewcluster` is
//     NOT idempotent (it errors on an already-initialized cluster), so this
//     guard is required both for a from-scratch idempotent Job and so a Job
//     retry (RestartPolicy OnFailure, e.g. after initialize-cluster-metadata
//     below fails transiently) never re-runs initnewcluster a second time.
//   - `bin/pulsar initialize-cluster-metadata` then runs as before to
//     initialize Pulsar's own cluster metadata (cluster name, default
//     namespace, broker/web service URLs). It is deliberately NOT
//     idempotent-guarded here: reconcileMetadataInitJob's alreadyInitialized
//     short-circuit is what prevents ever recreating this Job (and thus ever
//     re-running this line) once MetadataInitialized has gone True.
//
// BOOKIE_MEM/PULSAR_MEM are lowered to match pulsar-helm-chart's own init
// jobs: the image's default JVM heap sizing is tuned for a running
// broker/bookie, not a short-lived init Job, and can OOM-kill the pod under
// constrained resource limits.
const metadataInitScriptTemplate = `set -e
export BOOKIE_MEM="-Xmx128M"
export PULSAR_MEM="-Xmx128M"
if bin/bookkeeper shell whatisinstanceid; then
  echo "BookKeeper cluster metadata already initialized"
else
  bin/bookkeeper shell initnewcluster
fi
bin/pulsar initialize-cluster-metadata \
  --cluster "%s" \
  --metadata-store "%s" \
  --configuration-store "%s" \
  --web-service-url "%s" \
  --broker-service-url "%s"
`

func buildMetadataInitScript(clusterName, storeURL, webServiceURL, brokerServiceURL string) string {
	return fmt.Sprintf(metadataInitScriptTemplate, clusterName, storeURL, storeURL, webServiceURL, brokerServiceURL)
}

// buildMetadataInitConfigMap renders the bookkeeper.conf the metadata-init
// Job's container mounts at bookieConfMountPath: just enough config
// (metadataServiceUri, defaulted through the same withBookKeeperMetadataDefault
// helper the BookKeeper reconciler itself uses, so the Job addresses the
// identical Oxia-backed BookKeeper metadata store bookies will) for
// `bin/bookkeeper shell whatisinstanceid`/`initnewcluster` to run against.
func buildMetadataInitConfigMap(cluster *clusterv1alpha1.PulsarCluster) *corev1.ConfigMap {
	labels := builder.Labels(cluster.Name, metadataInitComponentName)
	rendered := config.RenderProperties(withBookKeeperMetadataDefault(nil, cluster.Name))
	return builder.ConfigMap(metadataInitJobName(cluster.Name), cluster.Namespace, labels, map[string]string{configMapKey: rendered})
}

// buildMetadataInitJob renders the Job that bootstraps both BookKeeper's own
// cluster metadata and Pulsar's cluster metadata against the cluster's Oxia
// metadata store (see metadataInitScriptTemplate) - the one-time bootstrap
// every Pulsar cluster needs before any bookie or broker can serve traffic.
// It is a pure function of cluster so it is unit-testable without a client;
// the caller reconciles buildMetadataInitConfigMap first, sets the owner
// reference, and creates it.
func buildMetadataInitJob(cluster *clusterv1alpha1.PulsarCluster) *batchv1.Job {
	labels := builder.Labels(cluster.Name, metadataInitComponentName)
	name := metadataInitJobName(cluster.Name)

	storeURL := oxiaurl.MetadataStoreURL(oxiaPublicServiceName(cluster.Name), oxiaurl.DefaultNamespace)
	webServiceURL, brokerServiceURL := metadataInitBrokerServiceURLs(cluster)
	script := buildMetadataInitScript(cluster.Name, storeURL, webServiceURL, brokerServiceURL)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptrInt32(metadataInitBackoffLimit),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    metadataInitContainerName,
							Image:   clusterDefaultImage(cluster.Spec),
							Command: []string{"sh", "-c"},
							Args:    []string{script},
							VolumeMounts: []corev1.VolumeMount{
								{Name: volumeNameConfig, MountPath: bookieConfMountPath, SubPath: configMapKey, ReadOnly: true},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: volumeNameConfig,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: name},
								},
							},
						},
					},
				},
			},
		},
	}
}

// reconcileMetadataInit reconciles the cluster-metadata-init Job and reports
// it as a componentReport so it participates in the umbrella Ready rollup
// exactly like a child component: the cluster is not Ready until the Job has
// succeeded. It also returns whether the caller should requeue (set when a
// terminally-Failed Job was deleted to be retried). When a non-Oxia metadata
// store is ever selected, the Job (like the OxiaCluster child) is pruned.
func (r *PulsarClusterReconciler) reconcileMetadataInit(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, oxiaReady bool) (componentReport, bool, error) {
	name := metadataInitJobName(cluster.Name)

	if !oxiaSelected(cluster.Spec) {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		report, err := pruneChild(ctx, r, metadataInitComponentName, job)
		if err != nil {
			return report, false, err
		}
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		if err := r.Delete(ctx, cm); client.IgnoreNotFound(err) != nil {
			return componentReport{}, false, fmt.Errorf("%s: pruning configmap: %w", metadataInitComponentName, err)
		}
		return report, false, nil
	}

	if err := r.reconcileMetadataInitConfigMap(ctx, cluster); err != nil {
		return componentReport{}, false, fmt.Errorf("%s: %w", metadataInitComponentName, err)
	}

	alreadyInitialized := apimeta.IsStatusConditionTrue(cluster.Status.Conditions, conditionTypeMetadataInitialized)

	job, imagePull, requeue, err := r.reconcileMetadataInitJob(ctx, cluster, oxiaReady, alreadyInitialized)
	if err != nil {
		return componentReport{}, false, fmt.Errorf("%s: %w", metadataInitComponentName, err)
	}

	cond := metadataInitializedCondition(cluster.Generation, oxiaReady, job, alreadyInitialized, imagePull)
	r.recordMetadataInitImagePullEvent(cluster, cond, imagePull)
	apimeta.SetStatusCondition(&cluster.Status.Conditions, cond)

	return componentReport{
		name:    metadataInitComponentName,
		present: true,
		ready:   cond.Status == metav1.ConditionTrue,
		reason:  cond.Reason,
		message: cond.Message,
	}, requeue, nil
}

// reconcileMetadataInitConfigMap ensures the metadata-init Job's
// bookkeeper.conf ConfigMap (see buildMetadataInitConfigMap) exists and
// carries the cluster's current Oxia-backed BookKeeper metadataServiceUri.
// Reconciled unconditionally, not gated on the Job not yet existing, so a
// ConfigMap deleted out from under a not-yet-created Job (or not yet
// reconciled on a fresh cluster) is always in place before
// reconcileMetadataInitJob creates the Job that mounts it; updating it after
// the Job has already run is harmless since the Job's pod template - and
// thus its volume mount - never changes once created.
func (r *PulsarClusterReconciler) reconcileMetadataInitConfigMap(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) error {
	wanted := buildMetadataInitConfigMap(cluster)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: wanted.Name, Namespace: wanted.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = wanted.Labels
		cm.Data = wanted.Data
		return controllerutil.SetControllerReference(cluster, cm, r.Scheme)
	})
	return err
}

// reconcileMetadataInitJob creates the cluster-metadata-init Job once the
// OxiaCluster child is Ready, and otherwise leaves a running/succeeded Job
// untouched: a Job's pod template is immutable, and initialize-cluster-metadata
// is meant to run exactly once per cluster. A nil Job with a nil error means
// the Job hasn't been created yet (Oxia isn't Ready, or bootstrap is already
// permanently done - see alreadyInitialized below). A terminally-Failed Job
// is deleted so a fresh attempt is recreated on the next reconcile - it must
// never wedge the cluster forever - and the still-Failed Job is returned so
// this cycle's status truthfully reports the failure; the returned bool then
// asks the caller to requeue the retry.
//
// A Job whose pod is stuck Waiting on ImagePullBackOff/ErrImagePull (see
// metadataInitImagePullStuck) never trips that Failed condition - the pod
// just sits Pending forever - so it gets the same treatment via a second,
// independent check: report it (imagePullStuckInfo, and the requeue bool) so
// the caller surfaces it instead of looking silently stuck, and once it's
// been stuck longer than metadataInitRetryInterval, delete the Job so the
// next reconcile (after the caller's requeue) recreates it - self-healing
// once the image becomes pullable, while the interval keeps a genuinely bad
// image from spinning in a tight delete/recreate loop.
//
// alreadyInitialized short-circuits the NotFound case before it would
// otherwise recreate the Job: initialize-cluster-metadata is NOT idempotent
// (it errors if the cluster's metadata already exists), so once
// MetadataInitialized has ever gone True, that bootstrap is a permanent fact
// and the Job must never be recreated even if it's since been deleted (e.g.
// an admin ran `kubectl delete job`, or a finished-Job TTL/cleanup policy
// reaped it). Recreating it would rerun the non-idempotent command, fail, and
// wedge the cluster Ready=False forever.
func (r *PulsarClusterReconciler) reconcileMetadataInitJob(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, oxiaReady, alreadyInitialized bool) (*batchv1.Job, imagePullStuckInfo, bool, error) {
	job := &batchv1.Job{}
	key := types.NamespacedName{Name: metadataInitJobName(cluster.Name), Namespace: cluster.Namespace}
	err := r.Get(ctx, key, job)
	switch {
	case err == nil:
		if jobFailedPermanently(job) {
			if delErr := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(delErr) != nil {
				return nil, imagePullStuckInfo{}, false, fmt.Errorf("deleting failed cluster-metadata-init job: %w", delErr)
			}
			return job, imagePullStuckInfo{}, true, nil
		}

		imagePull, err := r.metadataInitImagePullStuck(ctx, cluster, job)
		if err != nil {
			return nil, imagePullStuckInfo{}, false, err
		}
		if !imagePull.stuck {
			return job, imagePullStuckInfo{}, false, nil
		}
		if r.now().Sub(imagePull.since) >= metadataInitRetryInterval {
			if delErr := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(delErr) != nil {
				return nil, imagePullStuckInfo{}, false, fmt.Errorf("deleting cluster-metadata-init job stuck on image pull: %w", delErr)
			}
		}
		// Requeue even when the retry interval hasn't elapsed yet: once a Job
		// stops changing (no further Failed/Succeeded/Active transitions),
		// nothing else re-triggers this reconciler, so this is what keeps
		// re-checking the elapsed time until it crosses the threshold above.
		return job, imagePull, true, nil
	case !apierrors.IsNotFound(err):
		return nil, imagePullStuckInfo{}, false, fmt.Errorf("getting cluster-metadata-init job: %w", err)
	case !oxiaReady:
		return nil, imagePullStuckInfo{}, false, nil
	}

	if alreadyInitialized {
		return nil, imagePullStuckInfo{}, false, nil
	}

	desired := buildMetadataInitJob(cluster)
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return nil, imagePullStuckInfo{}, false, fmt.Errorf("setting owner reference on cluster-metadata-init job: %w", err)
	}
	if err := r.Create(ctx, desired); err != nil {
		return nil, imagePullStuckInfo{}, false, fmt.Errorf("creating cluster-metadata-init job: %w", err)
	}
	return desired, imagePullStuckInfo{}, false, nil
}

// imagePullStuckInfo captures the metadata-init Job's pod being stuck
// Waiting on an image it cannot pull (see
// containerWaitingReasonImagePullBackOff/ErrImagePull). The zero value means
// "not stuck". since is the stuck pod's own CreationTimestamp, not the time
// this was observed: the pod is never recreated while stuck (the kubelet
// retries the pull on the same pod), so it stays a stable measure of how
// long the pull has been failing across reconciles, letting the caller rate
// -limit the recreate to metadataInitRetryInterval without persisting its
// own timestamp anywhere.
type imagePullStuckInfo struct {
	stuck  bool
	image  string
	reason string
	since  time.Time
}

// metadataInitImagePullStuck inspects the metadata-init Job's own pod(s) for
// its single container Waiting on ImagePullBackOff/ErrImagePull. Pods are
// found via the component's stable selector labels (see builder.SelectorLabels)
// and filtered to ones this specific job controls, which guards against a
// stale pod from a just-deleted prior Job generation still existing under
// the same labels during the delete/recreate race window.
//
// This is what makes a bad/nonexistent image tag visible at all: such a pod
// stays Pending forever (see containerWaitingReasonImagePullBackOff) and so
// never reaches the terminal Failed Job condition jobFailedPermanently
// checks for - without this, the cluster looks like it's merely still
// JobRunning, forever, which is the silent-wedge bug this guards against.
func (r *PulsarClusterReconciler) metadataInitImagePullStuck(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, job *batchv1.Job) (imagePullStuckInfo, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(builder.SelectorLabels(cluster.Name, metadataInitComponentName))); err != nil {
		return imagePullStuckInfo{}, fmt.Errorf("listing cluster-metadata-init pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, job) {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != metadataInitContainerName || cs.State.Waiting == nil {
				continue
			}
			switch cs.State.Waiting.Reason {
			case containerWaitingReasonImagePullBackOff, containerWaitingReasonErrImagePull:
				return imagePullStuckInfo{
					stuck:  true,
					image:  cs.Image,
					reason: cs.State.Waiting.Reason,
					since:  pod.CreationTimestamp.Time,
				}, nil
			}
		}
	}
	return imagePullStuckInfo{}, nil
}

// recordMetadataInitImagePullEvent emits a Warning Event the first reconcile
// the metadata-init Job's pod is observed stuck on an image pull, so the
// failure is surfaced via `kubectl describe`/`get events` rather than the
// operator merely looking stuck - but not on every later reconcile of the
// same stuck state (e.g. the polling requeue in reconcileMetadataInitJob),
// which would otherwise re-fire it every metadataInitRetryInterval.
func (r *PulsarClusterReconciler) recordMetadataInitImagePullEvent(cluster *clusterv1alpha1.PulsarCluster, cond metav1.Condition, imagePull imagePullStuckInfo) {
	if !imagePull.stuck {
		return
	}
	prior := apimeta.FindStatusCondition(cluster.Status.Conditions, conditionTypeMetadataInitialized)
	if prior != nil && prior.Reason == reasonMetadataInitImagePullError {
		return
	}
	r.recorder().Eventf(cluster, nil, corev1.EventTypeWarning, reasonMetadataInitImagePullError, "MetadataInit", "%s", cond.Message)
}

// now returns the current time; nil Now defaults to time.Now. Tests override
// it so metadataInitRetryInterval-gated recreate logic doesn't need a real
// 30s sleep.
func (r *PulsarClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// metadataInitializedCondition computes the MetadataInitialized condition
// from whether Oxia is Ready, the observed state of the cluster-metadata-init
// Job (nil until it has been created, or once it's been pruned per
// alreadyInitialized below), and whether the condition was already True on a
// prior reconcile.
//
// A succeeded Job - or a prior True condition with the Job now absent - is
// checked FIRST so the condition is monotonic: cluster metadata bootstrap is
// a permanent one-time fact, so neither a later transient Oxia readiness blip
// (e.g. an Oxia pod restart) nor the Job object being deleted out from under
// the operator (admin cleanup, finished-Job TTL) may ever flip
// MetadataInitialized True->False and flap the umbrella's Ready. A
// terminally-Failed Job, or one stuck on an image pull (see
// imagePullStuckInfo), is likewise reported regardless of Oxia readiness so
// the real failure isn't masked as "waiting for Oxia".
func metadataInitializedCondition(generation int64, oxiaReady bool, job *batchv1.Job, alreadyInitialized bool, imagePull imagePullStuckInfo) metav1.Condition {
	base := metav1.Condition{Type: conditionTypeMetadataInitialized, ObservedGeneration: generation}

	switch {
	case jobSucceeded(job) || (alreadyInitialized && job == nil):
		base.Status = metav1.ConditionTrue
		base.Reason = reasonMetadataInitJobSucceeded
		base.Message = "cluster metadata initialized"
	case jobFailedPermanently(job):
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitJobFailed
		base.Message = fmt.Sprintf("cluster-metadata-init job %s failed", job.Name)
	case imagePull.stuck:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitImagePullError
		base.Message = fmt.Sprintf("cluster-metadata-init job %s cannot pull image %q: %s", job.Name, imagePull.image, imagePull.reason)
	case !oxiaReady:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitWaitingForOxia
		base.Message = "waiting for the OxiaCluster metadata store to become Ready before initializing cluster metadata"
	case job == nil:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitJobRunning
		base.Message = "cluster-metadata-init job has not been created yet"
	default:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitJobRunning
		base.Message = fmt.Sprintf("cluster-metadata-init job %s is running", job.Name)
	}

	return base
}

func jobSucceeded(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	if c := findJobCondition(job, batchv1.JobComplete); c != nil {
		return c.Status == corev1.ConditionTrue
	}
	return job.Status.Succeeded > 0
}

func jobFailedPermanently(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
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
