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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// PulsarClusterReconciler reconciles a PulsarCluster object.
//
// PulsarCluster is the umbrella resource: this controller stamps out and
// keeps reconciled the per-component child CRs (Broker, BookKeeper, Proxy,
// AutoRecovery, FunctionsWorker, metadata.OxiaCluster) that the component
// controllers own the workloads for (the KAAP hybrid pattern). Updates that
// change a child's spec are rolled out in dependency order rather than all at
// once - see pulsarcluster_upgrade.go.
type PulsarClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Recorder emits Events describing ordered-rollout progress (a tier
	// starting to roll, or a downstream tier's update deferred behind an
	// unsettled upstream tier). cmd/main.go wires it to
	// mgr.GetEventRecorder(...); a nil Recorder is treated as a no-op sink so
	// tests may leave it unset.
	Recorder events.EventRecorder
}

const (
	// conditionTypeReady is the PulsarCluster status condition aggregating
	// child-component readiness.
	conditionTypeReady = "Ready"

	reasonAllComponentsReady     = "AllComponentsReady"
	reasonComponentNotReady      = "ComponentNotReady"
	reasonComponentStatusMissing = "ComponentStatusMissing"
	reasonComponentProgressing   = "ComponentProgressing"
	reasonNoComponentsConfigured = "NoComponentsConfigured"

	phaseReady    = "Ready"
	phaseNotReady = "NotReady"

	// defaultImageRepository is the Pulsar image repo used to build a default
	// component image from spec.pulsarVersion when no explicit image is set.
	defaultImageRepository = "apachepulsar/pulsar"

	// metadataStoreOxia is the (currently only) supported metadata store.
	metadataStoreOxia = "oxia"
)

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=pulsarclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=pulsarclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=pulsarclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=proxies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=proxies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=autorecoveries,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=autorecoveries/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=functionsworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=functionsworkers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile decomposes a PulsarCluster into its per-component child CRs,
// creates the still-missing ones eagerly, and rolls out spec changes to the
// existing ones in dependency order - OxiaCluster (metadata) -> BookKeeper
// (bookies, with AutoRecovery alongside) -> Broker -> Proxy -> FunctionsWorker
// - so e.g. a pulsarVersion bump never lands on every component at once (see
// pulsarcluster_upgrade.go). It also prunes children whose sub-spec was
// removed and aggregates their reported readiness back onto
// PulsarCluster.Status.
func (r *PulsarClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &clusterv1alpha1.PulsarCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	reports := make([]componentReport, 0, 7)

	oxiaReport, oxiaOut, err := r.reconcileOxia(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child OxiaCluster")
		return ctrl.Result{}, err
	}
	cluster.Status.OxiaPhase = componentPhase(oxiaReport)
	reports = append(reports, oxiaReport)

	metadataInitReport, requeueMetadataInit, err := r.reconcileMetadataInit(ctx, cluster, oxiaReport.ready)
	if err != nil {
		log.Error(err, "failed to reconcile cluster-metadata-init Job")
		return ctrl.Result{}, err
	}
	reports = append(reports, metadataInitReport)

	bookKeeperReport, bookKeeperOut, err := r.reconcileBookKeeper(ctx, cluster, oxiaOut.settled)
	if err != nil {
		log.Error(err, "failed to reconcile child BookKeeper")
		return ctrl.Result{}, err
	}
	cluster.Status.BookKeeperPhase = componentPhase(bookKeeperReport)
	reports = append(reports, bookKeeperReport)

	// AutoRecovery rolls alongside BookKeeper (both gated on Oxia having
	// settled) rather than waiting on BookKeeper's own settlement: disabling
	// AutoRecovery for the duration of the bookie roll would be the ideal,
	// but that enable/disable toggle isn't implemented yet (follow-up).
	autoRecoveryReport, autoRecoveryOut, err := r.reconcileAutoRecovery(ctx, cluster, oxiaOut.settled)
	if err != nil {
		log.Error(err, "failed to reconcile child AutoRecovery")
		return ctrl.Result{}, err
	}
	cluster.Status.AutoRecoveryPhase = componentPhase(autoRecoveryReport)
	reports = append(reports, autoRecoveryReport)

	brokerReport, brokerOut, err := r.reconcileBroker(ctx, cluster, oxiaOut.settled && bookKeeperOut.settled)
	if err != nil {
		log.Error(err, "failed to reconcile child Broker")
		return ctrl.Result{}, err
	}
	cluster.Status.BrokerPhase = componentPhase(brokerReport)
	reports = append(reports, brokerReport)

	proxyReport, proxyOut, err := r.reconcileProxy(ctx, cluster, oxiaOut.settled && bookKeeperOut.settled && brokerOut.settled)
	if err != nil {
		log.Error(err, "failed to reconcile child Proxy")
		return ctrl.Result{}, err
	}
	cluster.Status.ProxyPhase = componentPhase(proxyReport)
	reports = append(reports, proxyReport)

	functionsWorkerReport, functionsWorkerOut, err := r.reconcileFunctionsWorker(
		ctx, cluster, oxiaOut.settled && bookKeeperOut.settled && brokerOut.settled && proxyOut.settled)
	if err != nil {
		log.Error(err, "failed to reconcile child FunctionsWorker")
		return ctrl.Result{}, err
	}
	cluster.Status.FunctionsWorkerPhase = componentPhase(functionsWorkerReport)
	reports = append(reports, functionsWorkerReport)

	states := []tierState{
		{tier: tierOxia, present: oxiaReport.present, outcome: oxiaOut},
		{tier: tierBookKeeper, present: bookKeeperReport.present, outcome: bookKeeperOut},
		{tier: tierAutoRecovery, present: autoRecoveryReport.present, outcome: autoRecoveryOut},
		{tier: tierBroker, present: brokerReport.present, outcome: brokerOut},
		{tier: tierProxy, present: proxyReport.present, outcome: proxyOut},
		{tier: tierFunctionsWorker, present: functionsWorkerReport.present, outcome: functionsWorkerOut},
	}

	cluster.Status.ObservedGeneration = cluster.Generation
	apimeta.SetStatusCondition(&cluster.Status.Conditions, aggregateReadyCondition(cluster.Generation, reports))

	upgrading := rollingOutCondition(cluster.Generation, states)
	priorUpgrading := apimeta.FindStatusCondition(cluster.Status.Conditions, conditionTypeUpgrading)
	r.recordRolloutEvents(cluster, priorUpgrading, upgrading, states)
	apimeta.SetStatusCondition(&cluster.Status.Conditions, upgrading)

	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating PulsarCluster status: %w", err)
	}

	if requeueMetadataInit {
		return ctrl.Result{RequeueAfter: metadataInitRetryInterval}, nil
	}
	if _, rolling := rollingBottleneck(states); rolling {
		// Some tier has a spec roll in flight - just written and not yet
		// converged, or deferred behind an unsettled upstream tier. Requeue
		// to keep making progress: owner watches on the child CRs already
		// fire when a child's status changes, but this is a defensive
		// backstop so gating decisions get rechecked even without one. A
		// steady-state cluster has no rolling tier, so this never churns.
		return ctrl.Result{RequeueAfter: rolloutRequeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// reconcileBroker creates the Broker child if it's missing, or - once
// upstreamSettled (Oxia and BookKeeper have both settled on their latest
// specs) - rolls out a changed desired spec to an existing one. It returns
// (report, outcome, err): outcome.settled feeds the Proxy/FunctionsWorker
// gate, and outcome's applied/deferred flags drive Reconcile's rollout
// events, requeue, and Upgrading condition.
func (r *PulsarClusterReconciler) reconcileBroker(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, upstreamSettled bool) (componentReport, tierOutcome, error) {
	const name = "broker"
	child := &clusterv1alpha1.Broker{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if cluster.Spec.Broker == nil {
		report, err := pruneChild(ctx, r, name, child)
		return report, tierOutcome{settled: true}, err
	}

	desired := buildBrokerSpec(cluster.Spec)
	desired.Config = withBrokerProxyMetadataDefaults(desired.Config, cluster.Name)
	desired.Config = withBrokerBookkeeperMetadataDefault(desired.Config, cluster.Name)
	desired.Config = withClusterNameDefault(desired.Config, cluster.Name)
	desired.Config = withBrokerOffloadDefaults(desired.Config, cluster.Spec.Offload)

	desiredSpec := any(desired)
	liveSpec := func() any { return child.Spec }
	var onExisting func()
	if brokerAutoscalerEnabled(desired) {
		desiredSpec = brokerHashSpec(desired)
		liveSpec = func() any { return brokerLiveHashSpec(child.Spec) }
		onExisting = func() { desired.Replicas = child.Spec.Replicas }
	}

	outcome, err := r.applyOrderedChild(ctx, cluster, child, desiredSpec,
		liveSpec, func() { child.Spec = *desired }, upstreamSettled, onExisting)
	if err != nil {
		return componentReport{}, tierOutcome{}, fmt.Errorf("broker: %w", err)
	}

	report := reportFromConditions(name, child.Generation, child.Status.Conditions)
	outcome.settled = tierSettled(report.ready, outcome.deferred)
	return report, outcome, nil
}

// reconcileBookKeeper is reconcileBroker's counterpart for the bookie tier:
// its update is gated on upstreamSettled, which the caller passes as Oxia's
// own settled state (BookKeeper is the tier directly downstream of Oxia).
func (r *PulsarClusterReconciler) reconcileBookKeeper(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, upstreamSettled bool) (componentReport, tierOutcome, error) {
	const name = "bookkeeper"
	child := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if cluster.Spec.BookKeeper == nil {
		report, err := pruneChild(ctx, r, name, child)
		return report, tierOutcome{settled: true}, err
	}

	desired := buildBookKeeperSpec(cluster.Spec)
	desired.Config = withBookKeeperMetadataDefault(desired.Config, cluster.Name)

	desiredSpec := any(desired)
	liveSpec := func() any { return child.Spec }
	var onExisting func()
	if bookKeeperAutoscalerEnabled(desired) {
		desiredSpec = bookKeeperHashSpec(desired)
		liveSpec = func() any { return bookKeeperLiveHashSpec(child.Spec) }
		onExisting = func() { desired.Replicas = child.Spec.Replicas }
	}

	outcome, err := r.applyOrderedChild(ctx, cluster, child, desiredSpec,
		liveSpec, func() { child.Spec = *desired }, upstreamSettled, onExisting)
	if err != nil {
		return componentReport{}, tierOutcome{}, fmt.Errorf("bookkeeper: %w", err)
	}

	report := reportFromConditions(name, child.Generation, child.Status.Conditions)
	outcome.settled = tierSettled(report.ready, outcome.deferred)
	return report, outcome, nil
}

// reconcileProxy gates its update on upstreamSettled, the caller-computed
// conjunction of every tier upstream of Proxy (Oxia, BookKeeper, Broker).
func (r *PulsarClusterReconciler) reconcileProxy(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, upstreamSettled bool) (componentReport, tierOutcome, error) {
	const name = "proxy"
	child := &clusterv1alpha1.Proxy{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if cluster.Spec.Proxy == nil {
		report, err := pruneChild(ctx, r, name, child)
		return report, tierOutcome{settled: true}, err
	}

	desired := buildProxySpec(cluster.Spec)
	desired.Config = withBrokerProxyMetadataDefaults(desired.Config, cluster.Name)
	desired.Config = withClusterNameDefault(desired.Config, cluster.Name)

	outcome, err := r.applyOrderedChild(ctx, cluster, child, desired,
		func() any { return child.Spec }, func() { child.Spec = *desired }, upstreamSettled, nil)
	if err != nil {
		return componentReport{}, tierOutcome{}, fmt.Errorf("proxy: %w", err)
	}

	report := reportFromConditions(name, child.Generation, child.Status.Conditions)
	outcome.settled = tierSettled(report.ready, outcome.deferred)
	return report, outcome, nil
}

// reconcileAutoRecovery gates its update on upstreamSettled - the caller
// passes Oxia's settled state, the same gate BookKeeper uses, so AutoRecovery
// rolls alongside the bookie tier rather than waiting on it.
func (r *PulsarClusterReconciler) reconcileAutoRecovery(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, upstreamSettled bool) (componentReport, tierOutcome, error) {
	const name = "autorecovery"
	child := &clusterv1alpha1.AutoRecovery{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if cluster.Spec.AutoRecovery == nil {
		report, err := pruneChild(ctx, r, name, child)
		return report, tierOutcome{settled: true}, err
	}

	desired := buildAutoRecoverySpec(cluster.Spec)

	outcome, err := r.applyOrderedChild(ctx, cluster, child, desired,
		func() any { return child.Spec }, func() { child.Spec = *desired }, upstreamSettled, nil)
	if err != nil {
		return componentReport{}, tierOutcome{}, fmt.Errorf("autorecovery: %w", err)
	}

	report := reportFromConditions(name, child.Generation, child.Status.Conditions)
	outcome.settled = tierSettled(report.ready, outcome.deferred)
	return report, outcome, nil
}

// reconcileFunctionsWorker gates its update on upstreamSettled, the
// caller-computed conjunction of every other tier: it is last in the rollout
// order.
func (r *PulsarClusterReconciler) reconcileFunctionsWorker(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, upstreamSettled bool) (componentReport, tierOutcome, error) {
	const name = "functionsworker"
	child := &clusterv1alpha1.FunctionsWorker{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if cluster.Spec.FunctionsWorker == nil {
		report, err := pruneChild(ctx, r, name, child)
		return report, tierOutcome{settled: true}, err
	}

	desired := buildFunctionsWorkerSpec(cluster.Spec)
	desired.Config = withFunctionsWorkerMetadataDefault(desired.Config, cluster.Name)

	outcome, err := r.applyOrderedChild(ctx, cluster, child, desired,
		func() any { return child.Spec }, func() { child.Spec = *desired }, upstreamSettled, nil)
	if err != nil {
		return componentReport{}, tierOutcome{}, fmt.Errorf("functionsworker: %w", err)
	}

	report := reportFromConditions(name, child.Generation, child.Status.Conditions)
	outcome.settled = tierSettled(report.ready, outcome.deferred)
	return report, outcome, nil
}

// reconcileOxia has no upstream tier of its own (it is first in the rollout
// order), so its update is never gated - upstreamSettled is hardcoded true.
//
// Oxia is the mandatory metadata store: brokers and bookies have nowhere to
// store metadata without it, so the child is always provisioned while oxia
// is the selected store (a default OxiaClusterSpec is used when the user
// omits spec.oxia). It is only pruned if a future non-Oxia store is ever
// selected.
func (r *PulsarClusterReconciler) reconcileOxia(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, tierOutcome, error) {
	const name = "oxia"
	child := &metadatav1alpha1.OxiaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if !oxiaSelected(cluster.Spec) {
		report, err := pruneChild(ctx, r, name, child)
		return report, tierOutcome{settled: true}, err
	}

	desired := buildOxiaSpec(cluster.Spec)

	outcome, err := r.applyOrderedChild(ctx, cluster, child, desired,
		func() any { return child.Spec }, func() { child.Spec = *desired }, true, nil)
	if err != nil {
		return componentReport{}, tierOutcome{}, fmt.Errorf("oxia: %w", err)
	}

	report := reportFromConditions(name, child.Generation, child.Status.Conditions)
	outcome.settled = tierSettled(report.ready, outcome.deferred)
	return report, outcome, nil
}

// pruneChild deletes a no-longer-desired child (identified by its deterministic
// name) and reports it as absent. Owner-reference garbage collection only fires
// on parent deletion, so removing a sub-spec must actively delete its child or
// the orphaned workload keeps running. A missing child is treated as success.
func pruneChild(ctx context.Context, r *PulsarClusterReconciler, name string, child client.Object) (componentReport, error) {
	if err := r.Delete(ctx, child); client.IgnoreNotFound(err) != nil {
		return componentReport{}, fmt.Errorf("%s: pruning: %w", name, err)
	}
	return componentReport{name: name}, nil
}

// childName deterministically names a child CR after its owning PulsarCluster.
func childName(clusterName, suffix string) string {
	return clusterName + "-" + suffix
}

// clusterDefaultImage is the cluster-wide default image a component inherits
// when it sets no image of its own: an explicit spec.image wins, otherwise one
// is built from spec.pulsarVersion (which pins the deployed Pulsar tag).
func clusterDefaultImage(spec clusterv1alpha1.PulsarClusterSpec) string {
	if spec.Image != "" {
		return spec.Image
	}
	if spec.PulsarVersion != "" {
		return defaultImageRepository + ":" + spec.PulsarVersion
	}
	return ""
}

// effectiveImage resolves a component's image against the cluster-wide
// default, matching the "falls back to PulsarCluster.spec.image when unset"
// contract documented on each component's Image field.
func effectiveImage(componentImage, clusterImage string) string {
	if componentImage != "" {
		return componentImage
	}
	return clusterImage
}

// globalStorageClassName returns the cluster-wide default StorageClass, or
// nil when none is configured.
func globalStorageClassName(spec clusterv1alpha1.PulsarClusterSpec) *string {
	if spec.Global == nil {
		return nil
	}
	return spec.Global.StorageClassName
}

// buildBrokerSpec copies PulsarCluster.spec.broker into the child Broker
// spec, applying the cluster-wide image default. When spec.offload is set,
// it also swaps in the apachepulsar/pulsar-all image (which bundles the
// tiered-storage offloader jars the slim default image lacks) unless the
// user set an explicit image of their own - either on the Broker sub-spec or
// cluster-wide - which is left untouched since it may already be pulsar-all
// or a custom build with offloaders baked in. It also wires
// spec.offload.credentialsSecretRef into the broker so the offloader driver
// can authenticate: as env vars for AWS/Azure (read as literal values), or as
// a mounted key file for GCS (whose credential is a path to a JSON key file).
func buildBrokerSpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.BrokerSpec {
	out := spec.Broker.DeepCopy()
	if out == nil {
		return out
	}

	explicitImage := out.Image != "" || spec.Image != ""
	out.Image = effectiveImage(out.Image, clusterDefaultImage(spec))

	if spec.Offload != nil {
		if !explicitImage {
			if img := pulsarAllImage(spec.PulsarVersion); img != "" {
				out.Image = img
			}
		}
		out.Env = append(out.Env, offloadCredentialEnv(spec.Offload)...)
		out.Volumes = append(out.Volumes, offloadCredentialVolumes(spec.Offload)...)
		out.VolumeMounts = append(out.VolumeMounts, offloadCredentialVolumeMounts(spec.Offload)...)
	}

	return out
}

// buildBookKeeperSpec copies PulsarCluster.spec.bookKeeper into the child
// BookKeeper spec, applying the cluster-wide image default and, for any
// configured volume that doesn't set its own StorageClass, the cluster-wide
// global storage class default.
func buildBookKeeperSpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.BookKeeperSpec {
	out := spec.BookKeeper.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, clusterDefaultImage(spec))

	if sc := globalStorageClassName(spec); sc != nil && out.Volumes != nil {
		for _, vol := range []*clusterv1alpha1.VolumeSpec{out.Volumes.Journal, out.Volumes.Ledgers, out.Volumes.Index} {
			if vol != nil && vol.StorageClassName == nil {
				vol.StorageClassName = sc
			}
		}
	}
	return out
}

// brokerAutoscalerEnabled reports whether the broker autoscaler is enabled on
// the (already cluster-derived) desired spec - the same gate
// BrokerAutoscalerReconciler itself checks before writing spec.replicas. When
// true, spec.replicas is autoscaler-owned and reconcileBroker must neither
// write nor revert it.
func brokerAutoscalerEnabled(spec *clusterv1alpha1.BrokerSpec) bool {
	return spec.Autoscaler != nil && spec.Autoscaler.Enabled
}

// bookKeeperAutoscalerEnabled is brokerAutoscalerEnabled's BookKeeper
// counterpart, mirroring the gate BookKeeperAutoscalerReconciler checks.
func bookKeeperAutoscalerEnabled(spec *clusterv1alpha1.BookKeeperSpec) bool {
	return spec.Autoscaler != nil && spec.Autoscaler.Enabled
}

// brokerHashSpec and brokerLiveHashSpec clear Replicas before a Broker spec is
// fed to specHash, for the desired and live sides of applyOrderedChild's hash
// comparisons respectively. reconcileBroker only swaps these in once the
// broker autoscaler is enabled: with the field hidden from both the
// desired-vs-stored-desired and live-vs-stored-applied comparisons, the
// autoscaler moving spec.replicas can never by itself register as a genuine
// roll or as drift, so the umbrella never reverts it. The real, live-preserved
// Replicas value is unaffected - it is set directly on the desired spec (see
// reconcileBroker's onExisting hook) and is what actually gets written on any
// roll or drift-correction triggered by some other field.
func brokerHashSpec(spec *clusterv1alpha1.BrokerSpec) any {
	out := spec.DeepCopy()
	out.Replicas = nil
	return out
}

func brokerLiveHashSpec(spec clusterv1alpha1.BrokerSpec) any {
	spec.Replicas = nil
	return spec
}

// bookKeeperHashSpec and bookKeeperLiveHashSpec are brokerHashSpec/
// brokerLiveHashSpec's BookKeeper counterparts; see those for the rationale.
func bookKeeperHashSpec(spec *clusterv1alpha1.BookKeeperSpec) any {
	out := spec.DeepCopy()
	out.Replicas = nil
	return out
}

func bookKeeperLiveHashSpec(spec clusterv1alpha1.BookKeeperSpec) any {
	spec.Replicas = nil
	return spec
}

// buildProxySpec copies PulsarCluster.spec.proxy into the child Proxy spec,
// applying the cluster-wide image default.
func buildProxySpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.ProxySpec {
	out := spec.Proxy.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, clusterDefaultImage(spec))
	return out
}

// buildAutoRecoverySpec copies PulsarCluster.spec.autoRecovery into the child
// AutoRecovery spec, applying the cluster-wide image default.
func buildAutoRecoverySpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.AutoRecoverySpec {
	out := spec.AutoRecovery.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, clusterDefaultImage(spec))
	return out
}

// buildFunctionsWorkerSpec copies PulsarCluster.spec.functionsWorker into the
// child FunctionsWorker spec, applying the cluster-wide image default.
func buildFunctionsWorkerSpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.FunctionsWorkerSpec {
	out := spec.FunctionsWorker.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, clusterDefaultImage(spec))
	return out
}

// oxiaSelected reports whether Oxia is the cluster's metadata store. The
// metadataStore type is an Oxia-only enum that defaults to "oxia", so this is
// true unless a future implementation explicitly selects a different store.
func oxiaSelected(spec clusterv1alpha1.PulsarClusterSpec) bool {
	if spec.MetadataStore == nil || spec.MetadataStore.Type == "" {
		return true
	}
	return spec.MetadataStore.Type == metadataStoreOxia
}

// buildOxiaSpec derives the child OxiaCluster spec, applying the global
// storage class default. When spec.oxia is omitted a default (empty)
// OxiaClusterSpec is used so CRD defaults (coordinator 2, server 3) provision
// a working metadata store. It never returns nil.
//
// Unlike the other buildXSpec helpers, it deliberately never applies
// clusterDefaultImage (PulsarCluster.spec.image / an image derived from
// spec.pulsarVersion, e.g. apachepulsar/pulsar:5.0.0-M1): Oxia ships in a
// wholly different image family (oxia/oxia) with no oxia binary in the
// apachepulsar/pulsar image, so stamping the Pulsar default onto an unset
// Coordinator/Server image would replace a working image with one that can't
// run oxia at all. Leaving Coordinator.Image/Server.Image untouched when the
// user hasn't set them lets the OxiaCluster reconciler's own default
// (defaultOxiaImage) apply instead; an explicit user-set oxia image is
// preserved as-is via the DeepCopy above.
func buildOxiaSpec(spec clusterv1alpha1.PulsarClusterSpec) *metadatav1alpha1.OxiaClusterSpec {
	out := spec.Oxia.DeepCopy()
	if out == nil {
		out = &metadatav1alpha1.OxiaClusterSpec{}
	}

	if out.Server != nil {
		if sc := globalStorageClassName(spec); sc != nil && out.Server.StorageClassName == nil {
			out.Server.StorageClassName = sc
		}
	}
	return out
}

// componentReport captures one child component's observed readiness for
// status aggregation. It is deliberately free of Kubernetes client types so
// aggregateReadyCondition can be table-tested in isolation.
type componentReport struct {
	name    string
	present bool
	ready   bool
	reason  string
	message string
}

// reportFromConditions builds a componentReport for a present child from its
// observed status conditions and current generation. A child is only counted
// Ready when its Ready condition is True *and* not stale: a Ready condition
// whose ObservedGeneration trails the child's current generation reflects the
// pre-update spec, so it is treated as still progressing.
func reportFromConditions(name string, generation int64, conditions []metav1.Condition) componentReport {
	cond := apimeta.FindStatusCondition(conditions, conditionTypeReady)
	if cond == nil {
		return componentReport{
			name:    name,
			present: true,
			ready:   false,
			reason:  reasonComponentStatusMissing,
			message: fmt.Sprintf("child %s has not reported a Ready condition yet", name),
		}
	}
	if cond.ObservedGeneration != 0 && cond.ObservedGeneration < generation {
		return componentReport{
			name:    name,
			present: true,
			ready:   false,
			reason:  reasonComponentProgressing,
			message: fmt.Sprintf("child %s Ready condition is stale (observed generation %d < %d)", name, cond.ObservedGeneration, generation),
		}
	}
	return componentReport{
		name:    name,
		present: true,
		ready:   cond.Status == metav1.ConditionTrue,
		reason:  cond.Reason,
		message: cond.Message,
	}
}

// componentPhase renders a componentReport as a PulsarClusterStatus phase string.
func componentPhase(r componentReport) string {
	switch {
	case !r.present:
		return ""
	case r.ready:
		return phaseReady
	default:
		return phaseNotReady
	}
}

// aggregateReadyCondition computes the top-level PulsarCluster Ready
// condition: True only when every configured (present) child component
// reports Ready. A cluster with zero configured components is not Ready
// either, guarding against vacuously treating "nothing to check" as success.
func aggregateReadyCondition(generation int64, reports []componentReport) metav1.Condition {
	var configuredCount int
	var notReady []componentReport
	for _, r := range reports {
		if !r.present {
			continue
		}
		configuredCount++
		if !r.ready {
			notReady = append(notReady, r)
		}
	}

	switch {
	case configuredCount == 0:
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             reasonNoComponentsConfigured,
			Message:            "no PulsarCluster components are configured",
		}
	case len(notReady) == 0:
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			Reason:             reasonAllComponentsReady,
			Message:            "all configured components are Ready",
		}
	default:
		parts := make([]string, 0, len(notReady))
		for _, r := range notReady {
			reason := r.reason
			if reason == "" {
				reason = "not Ready"
			}
			parts = append(parts, fmt.Sprintf("%s (%s)", r.name, reason))
		}
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             reasonComponentNotReady,
			Message:            fmt.Sprintf("components not Ready: %s", strings.Join(parts, ", ")),
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *PulsarClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.PulsarCluster{}).
		Owns(&clusterv1alpha1.Broker{}).
		Owns(&clusterv1alpha1.BookKeeper{}).
		Owns(&clusterv1alpha1.Proxy{}).
		Owns(&clusterv1alpha1.AutoRecovery{}).
		Owns(&clusterv1alpha1.FunctionsWorker{}).
		Owns(&metadatav1alpha1.OxiaCluster{}).
		Owns(&batchv1.Job{}).
		Named("cluster-pulsarcluster").
		Complete(r)
}
