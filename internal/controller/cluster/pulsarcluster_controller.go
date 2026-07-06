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

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// PulsarClusterReconciler reconciles a PulsarCluster object.
//
// PulsarCluster is the umbrella resource: this controller stamps out and
// keeps reconciled the per-component child CRs (Broker, BookKeeper, Proxy,
// AutoRecovery, FunctionsWorker, metadata.OxiaCluster) that the component
// controllers own the workloads for (the KAAP hybrid pattern).
type PulsarClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	// conditionTypeReady is the PulsarCluster status condition aggregating
	// child-component readiness.
	conditionTypeReady = "Ready"

	reasonAllComponentsReady     = "AllComponentsReady"
	reasonComponentNotReady      = "ComponentNotReady"
	reasonComponentStatusMissing = "ComponentStatusMissing"
	reasonNoComponentsConfigured = "NoComponentsConfigured"

	phaseReady    = "Ready"
	phaseNotReady = "NotReady"
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

// Reconcile decomposes a PulsarCluster into its per-component child CRs,
// creates or updates each one (owned by the PulsarCluster so they are
// garbage-collected with it), and aggregates their reported readiness back
// onto PulsarCluster.Status.
func (r *PulsarClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &clusterv1alpha1.PulsarCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	reports := make([]componentReport, 0, 6)

	brokerReport, err := r.reconcileBroker(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child Broker")
		return ctrl.Result{}, err
	}
	cluster.Status.BrokerPhase = componentPhase(brokerReport)
	reports = append(reports, brokerReport)

	bookKeeperReport, err := r.reconcileBookKeeper(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child BookKeeper")
		return ctrl.Result{}, err
	}
	cluster.Status.BookKeeperPhase = componentPhase(bookKeeperReport)
	reports = append(reports, bookKeeperReport)

	proxyReport, err := r.reconcileProxy(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child Proxy")
		return ctrl.Result{}, err
	}
	cluster.Status.ProxyPhase = componentPhase(proxyReport)
	reports = append(reports, proxyReport)

	autoRecoveryReport, err := r.reconcileAutoRecovery(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child AutoRecovery")
		return ctrl.Result{}, err
	}
	cluster.Status.AutoRecoveryPhase = componentPhase(autoRecoveryReport)
	reports = append(reports, autoRecoveryReport)

	functionsWorkerReport, err := r.reconcileFunctionsWorker(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child FunctionsWorker")
		return ctrl.Result{}, err
	}
	cluster.Status.FunctionsWorkerPhase = componentPhase(functionsWorkerReport)
	reports = append(reports, functionsWorkerReport)

	oxiaReport, err := r.reconcileOxia(ctx, cluster)
	if err != nil {
		log.Error(err, "failed to reconcile child OxiaCluster")
		return ctrl.Result{}, err
	}
	cluster.Status.OxiaPhase = componentPhase(oxiaReport)
	reports = append(reports, oxiaReport)

	cluster.Status.ObservedGeneration = cluster.Generation
	apimeta.SetStatusCondition(&cluster.Status.Conditions, aggregateReadyCondition(cluster.Generation, reports))

	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating PulsarCluster status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *PulsarClusterReconciler) reconcileBroker(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, error) {
	const name = "broker"
	if cluster.Spec.Broker == nil {
		return componentReport{name: name}, nil
	}

	desired := buildBrokerSpec(cluster.Spec)
	child := &clusterv1alpha1.Broker{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if err := r.createOrUpdateChild(ctx, cluster, child, func() error {
		child.Spec = *desired
		return nil
	}); err != nil {
		return componentReport{}, fmt.Errorf("broker: %w", err)
	}

	return reportFromConditions(name, child.Status.Conditions), nil
}

func (r *PulsarClusterReconciler) reconcileBookKeeper(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, error) {
	const name = "bookkeeper"
	if cluster.Spec.BookKeeper == nil {
		return componentReport{name: name}, nil
	}

	desired := buildBookKeeperSpec(cluster.Spec)
	child := &clusterv1alpha1.BookKeeper{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if err := r.createOrUpdateChild(ctx, cluster, child, func() error {
		child.Spec = *desired
		return nil
	}); err != nil {
		return componentReport{}, fmt.Errorf("bookkeeper: %w", err)
	}

	return reportFromConditions(name, child.Status.Conditions), nil
}

func (r *PulsarClusterReconciler) reconcileProxy(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, error) {
	const name = "proxy"
	if cluster.Spec.Proxy == nil {
		return componentReport{name: name}, nil
	}

	desired := buildProxySpec(cluster.Spec)
	child := &clusterv1alpha1.Proxy{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if err := r.createOrUpdateChild(ctx, cluster, child, func() error {
		child.Spec = *desired
		return nil
	}); err != nil {
		return componentReport{}, fmt.Errorf("proxy: %w", err)
	}

	return reportFromConditions(name, child.Status.Conditions), nil
}

func (r *PulsarClusterReconciler) reconcileAutoRecovery(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, error) {
	const name = "autorecovery"
	if cluster.Spec.AutoRecovery == nil {
		return componentReport{name: name}, nil
	}

	desired := buildAutoRecoverySpec(cluster.Spec)
	child := &clusterv1alpha1.AutoRecovery{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if err := r.createOrUpdateChild(ctx, cluster, child, func() error {
		child.Spec = *desired
		return nil
	}); err != nil {
		return componentReport{}, fmt.Errorf("autorecovery: %w", err)
	}

	return reportFromConditions(name, child.Status.Conditions), nil
}

func (r *PulsarClusterReconciler) reconcileFunctionsWorker(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, error) {
	const name = "functionsworker"
	if cluster.Spec.FunctionsWorker == nil {
		return componentReport{name: name}, nil
	}

	desired := buildFunctionsWorkerSpec(cluster.Spec)
	child := &clusterv1alpha1.FunctionsWorker{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if err := r.createOrUpdateChild(ctx, cluster, child, func() error {
		child.Spec = *desired
		return nil
	}); err != nil {
		return componentReport{}, fmt.Errorf("functionsworker: %w", err)
	}

	return reportFromConditions(name, child.Status.Conditions), nil
}

func (r *PulsarClusterReconciler) reconcileOxia(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster) (componentReport, error) {
	const name = "oxia"
	if !shouldCreateOxia(cluster.Spec) {
		return componentReport{name: name}, nil
	}

	desired := buildOxiaSpec(cluster.Spec)
	child := &metadatav1alpha1.OxiaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: childName(cluster.Name, name), Namespace: cluster.Namespace},
	}
	if err := r.createOrUpdateChild(ctx, cluster, child, func() error {
		child.Spec = *desired
		return nil
	}); err != nil {
		return componentReport{}, fmt.Errorf("oxia: %w", err)
	}

	return reportFromConditions(name, child.Status.Conditions), nil
}

// createOrUpdateChild creates or updates a child object, setting cluster as
// its controller owner reference so it is garbage-collected with the parent.
func (r *PulsarClusterReconciler) createOrUpdateChild(
	ctx context.Context,
	cluster *clusterv1alpha1.PulsarCluster,
	child client.Object,
	applySpec func() error,
) error {
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, child, func() error {
		if err := applySpec(); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(cluster, child, r.Scheme)
	})
	return err
}

// childName deterministically names a child CR after its owning PulsarCluster.
func childName(clusterName, suffix string) string {
	return clusterName + "-" + suffix
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
// spec, applying the cluster-wide image default.
func buildBrokerSpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.BrokerSpec {
	out := spec.Broker.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, spec.Image)
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
	out.Image = effectiveImage(out.Image, spec.Image)

	if sc := globalStorageClassName(spec); sc != nil && out.Volumes != nil {
		for _, vol := range []*clusterv1alpha1.VolumeSpec{out.Volumes.Journal, out.Volumes.Ledgers, out.Volumes.Index} {
			if vol != nil && vol.StorageClassName == nil {
				vol.StorageClassName = sc
			}
		}
	}
	return out
}

// buildProxySpec copies PulsarCluster.spec.proxy into the child Proxy spec,
// applying the cluster-wide image default.
func buildProxySpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.ProxySpec {
	out := spec.Proxy.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, spec.Image)
	return out
}

// buildAutoRecoverySpec copies PulsarCluster.spec.autoRecovery into the child
// AutoRecovery spec, applying the cluster-wide image default.
func buildAutoRecoverySpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.AutoRecoverySpec {
	out := spec.AutoRecovery.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, spec.Image)
	return out
}

// buildFunctionsWorkerSpec copies PulsarCluster.spec.functionsWorker into the
// child FunctionsWorker spec, applying the cluster-wide image default.
func buildFunctionsWorkerSpec(spec clusterv1alpha1.PulsarClusterSpec) *clusterv1alpha1.FunctionsWorkerSpec {
	out := spec.FunctionsWorker.DeepCopy()
	if out == nil {
		return out
	}
	out.Image = effectiveImage(out.Image, spec.Image)
	return out
}

// shouldCreateOxia decides whether the umbrella reconciler should stamp out
// the metadata.OxiaCluster child: spec.oxia must be configured, and
// spec.metadataStore, when set, must not select a non-Oxia implementation.
func shouldCreateOxia(spec clusterv1alpha1.PulsarClusterSpec) bool {
	if spec.Oxia == nil {
		return false
	}
	if spec.MetadataStore != nil && spec.MetadataStore.Type != "" && spec.MetadataStore.Type != "oxia" {
		return false
	}
	return true
}

// buildOxiaSpec copies PulsarCluster.spec.oxia into the child OxiaCluster
// spec, applying the cluster-wide image and global storage class defaults.
func buildOxiaSpec(spec clusterv1alpha1.PulsarClusterSpec) *metadatav1alpha1.OxiaClusterSpec {
	out := spec.Oxia.DeepCopy()
	if out == nil {
		return out
	}

	if out.Coordinator != nil {
		out.Coordinator.Image = effectiveImage(out.Coordinator.Image, spec.Image)
	}
	if out.Server != nil {
		out.Server.Image = effectiveImage(out.Server.Image, spec.Image)
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
// observed status conditions.
func reportFromConditions(name string, conditions []metav1.Condition) componentReport {
	if cond := apimeta.FindStatusCondition(conditions, conditionTypeReady); cond != nil {
		return componentReport{
			name:    name,
			present: true,
			ready:   cond.Status == metav1.ConditionTrue,
			reason:  cond.Reason,
			message: cond.Message,
		}
	}
	return componentReport{
		name:    name,
		present: true,
		ready:   false,
		reason:  reasonComponentStatusMissing,
		message: fmt.Sprintf("child %s has not reported a Ready condition yet", name),
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
		Named("cluster-pulsarcluster").
		Complete(r)
}
