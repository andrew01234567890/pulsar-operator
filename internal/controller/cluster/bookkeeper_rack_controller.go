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
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	"github.com/andrew01234567890/pulsar-operator/internal/rackawareness"
)

const (
	// conditionTypeRackSync reports the rack-sync controller's most recent
	// pass - distinct from conditionTypeReady (rollout convergence) and
	// conditionTypeAutoscaling (the disk-watermark autoscaler's decisions).
	conditionTypeRackSync = "RackSync"

	reasonRackSynced          = "RackSynced"
	reasonRackSyncPartial     = "RackSyncPartialFailure"
	reasonRackSyncUnavailable = "RackSetterUnavailable"

	// defaultRackSyncPeriodSeconds mirrors BookKeeperAutoRackConfig's own
	// kubebuilder default (api/cluster/v1alpha1/bookkeeper_types.go), used
	// when a BookKeeper was created against an older CRD version or a
	// client that skipped server-side defaulting.
	defaultRackSyncPeriodSeconds int32 = 60

	// pulsarClusterOwnerKind matches PulsarCluster's Kind, used to find a
	// BookKeeper's owning PulsarCluster (if any) so the default RackSetter
	// can locate a sibling Broker's pod to exec pulsar-admin into.
	pulsarClusterOwnerKind = "PulsarCluster"
)

// BookKeeperRackReconciler is a dedicated controller - deliberately separate
// from BookKeeperReconciler, which owns the bookie StatefulSet/Service/
// ConfigMap/PodDisruptionBudget - that keeps BookKeeper's rack-placement
// metadata in sync with each bookie pod's node zone, so BookKeeper's
// RackawareEnsemblePlacementPolicy can stripe ledger ensembles across
// availability zones. It only ever reads Pods/Nodes and applies rack
// metadata through the injectable RackSetter; it never touches the bookie
// workload.
//
// bookkeeperClientRackawarePolicyEnabled and the rack-aware ensemble
// placement policy itself are broker/bookie config keys, owned by
// BookKeeperReconciler/BrokerReconciler's operatorManagedConfig - out of
// scope for this controller (see the PR description).
type BookKeeperRackReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// RESTConfig/ClientSet build the default PodExecRackSetter's pod-exec
	// transport. Wired from mgr.GetConfig()/kubernetes.NewForConfig in
	// cmd/main.go; unused once RackSetter is set directly.
	RESTConfig *rest.Config
	ClientSet  kubernetes.Interface

	// RackSetter applies the computed bookie->rack mapping. Nil defaults to
	// a rackawareness.PodExecRackSetter built per-tick from RESTConfig/
	// ClientSet and a discovered exec target; tests inject a mock instead
	// of talking to a live cluster.
	RackSetter rackawareness.RackSetter

	// Recorder emits Events summarizing each rack-sync pass. cmd/main.go
	// wires it to mgr.GetEventRecorder(...); a nil Recorder is treated as a
	// no-op sink so tests may leave it unset.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile evaluates one bookie rack-awareness sync tick: a no-op unless
// spec.autoRackConfig.enabled, it lists the bookie ensemble's pods, resolves
// each addressable bookie's node zone into a desired rack, and applies only
// the entries that differ from BookKeeper.status.bookieRacks (its own cache
// of what was last applied) through the injectable RackSetter. It always
// requeues at spec.autoRackConfig.periodSeconds while enabled.
func (r *BookKeeperRackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	bk := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, req.NamespacedName, bk); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	rackCfg := bk.Spec.AutoRackConfig
	if rackCfg == nil || !rackCfg.Enabled {
		return ctrl.Result{}, nil
	}

	result := ctrl.Result{RequeueAfter: time.Duration(resolveRackPeriodSeconds(rackCfg)) * time.Second}

	pods, err := r.listBookiePods(ctx, bk)
	if err != nil {
		return ctrl.Result{}, err
	}

	targets, skipped := r.desiredRackMapping(ctx, bk, pods)

	setter, err := r.rackSetter(ctx, bk, pods)
	if err != nil {
		log.Error(err, "bookie rack sync has no RackSetter available this tick")
		if statusErr := r.recordRackSetterUnavailable(ctx, bk, err.Error()); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return result, nil
	}

	applied, synced, failed := applyRackMapping(ctx, setter, targets, bk.Status.BookieRacks)

	if err := r.recordRackSyncResult(ctx, bk, applied, len(targets), synced, failed, skipped); err != nil {
		return ctrl.Result{}, err
	}

	return result, nil
}

func (r *BookKeeperRackReconciler) listBookiePods(ctx context.Context, bk *clusterv1alpha1.BookKeeper) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(bk.Namespace), client.MatchingLabels(builder.SelectorLabels(bk.Name, bookkeeperComponent))); err != nil {
		return nil, fmt.Errorf("list bookie pods for %s/%s: %w", bk.Namespace, bk.Name, err)
	}
	return pods.Items, nil
}

// rackTarget is one bookie's desired rack-placement entry.
type rackTarget struct {
	bookieID string
	rack     string
}

// bookieID derives a bookie's advertised BookKeeper id from its pod name:
// <pod>.<headless-svc>.<namespace>:<bookiePort>. The headless Service is
// named after the BookKeeper CR itself (see desiredHeadlessService in
// bookkeeper_controller.go), so this must stay in lock-step with that
// naming if it ever changes.
func bookieID(bk *clusterv1alpha1.BookKeeper, podName string) string {
	return fmt.Sprintf("%s.%s.%s:%d", podName, bk.Name, bk.Namespace, bookiePort)
}

// rackForZone maps a node's zone label to a BookKeeper rack name. BookKeeper
// rejects a rack name of "/" or "" once RackawareEnsemblePlacementPolicy is
// enabled, so an empty zone is treated as "no mapping" by the caller
// (desiredRackMapping) rather than passed through here.
func rackForZone(zone string) string {
	return "/" + zone
}

// desiredRackMapping computes every addressable bookie pod's desired
// (bookieID, rack) pair from its scheduled node's zone label. A pod not yet
// scheduled, a Node lookup failure, or a missing zone label all skip that
// one bookie (counted in skipped, not fatal) rather than failing the whole
// tick - a transient issue on one bookie must never block syncing the rest.
// The result is sorted by bookieID for deterministic apply order, logging,
// and test assertions.
func (r *BookKeeperRackReconciler) desiredRackMapping(ctx context.Context, bk *clusterv1alpha1.BookKeeper, pods []corev1.Pod) (targets []rackTarget, skipped int) {
	log := logf.FromContext(ctx)

	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			log.V(1).Info("bookie pod not yet scheduled, skipping rack sync this tick", "pod", pod.Name)
			skipped++
			continue
		}

		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
			log.Error(err, "failed to get node for bookie pod", "pod", pod.Name, "node", pod.Spec.NodeName)
			skipped++
			continue
		}

		zone := node.Labels[builder.ZoneTopologyKey]
		if zone == "" {
			log.Info("bookie pod's node has no zone label, skipping rack sync this tick", "pod", pod.Name, "node", node.Name)
			skipped++
			continue
		}

		targets = append(targets, rackTarget{bookieID: bookieID(bk, pod.Name), rack: rackForZone(zone)})
	}

	slices.SortFunc(targets, func(a, b rackTarget) int { return strings.Compare(a.bookieID, b.bookieID) })
	return targets, skipped
}

// applyRackMapping is the diff-only apply loop: RackSetter.SetBookieRack is
// only called for a bookie whose desired rack differs from previouslyApplied
// (BookKeeper.status.bookieRacks, the operator's own cache of what it last
// wrote) - a stable cluster's steady-state tick issues zero admin-API calls.
// A single bookie's Set failure is recorded in failed and skipped (its prior
// entry, if any, is carried over so the next tick retries it); it never
// aborts the rest of the ensemble. The returned map is the full next-tick
// cache: bookies no longer present in targets (e.g. after a scale-down) are
// naturally dropped since it is rebuilt from targets alone.
func applyRackMapping(ctx context.Context, setter rackawareness.RackSetter, targets []rackTarget, previouslyApplied map[string]string) (applied map[string]string, synced int, failed []string) {
	log := logf.FromContext(ctx)
	applied = make(map[string]string, len(targets))

	for _, t := range targets {
		if previouslyApplied[t.bookieID] == t.rack {
			applied[t.bookieID] = t.rack
			continue
		}

		if err := setter.SetBookieRack(ctx, t.bookieID, t.rack); err != nil {
			log.Error(err, "failed to set bookie rack", "bookieId", t.bookieID, "rack", t.rack)
			failed = append(failed, t.bookieID)
			if prev, ok := previouslyApplied[t.bookieID]; ok {
				applied[t.bookieID] = prev
			}
			continue
		}

		log.Info("synced bookie rack", "bookieId", t.bookieID, "rack", t.rack)
		applied[t.bookieID] = t.rack
		synced++
	}

	return applied, synced, failed
}

func (r *BookKeeperRackReconciler) recorder() events.EventRecorder {
	if r.Recorder != nil {
		return r.Recorder
	}
	// A zero-value FakeRecorder has a nil Events channel and discards every
	// event, so it is a safe no-op sink when no recorder was wired in.
	return &events.FakeRecorder{}
}

// rackSetter resolves the RackSetter to apply this tick's mapping through.
// An explicitly injected RackSetter (tests, or a future manual override)
// always wins; otherwise a PodExecRackSetter is built from RESTConfig/
// ClientSet and a pod discovered by resolveExecTarget.
func (r *BookKeeperRackReconciler) rackSetter(ctx context.Context, bk *clusterv1alpha1.BookKeeper, bookiePods []corev1.Pod) (rackawareness.RackSetter, error) {
	if r.RackSetter != nil {
		return r.RackSetter, nil
	}

	if r.RESTConfig == nil || r.ClientSet == nil {
		return nil, fmt.Errorf("no RESTConfig/ClientSet wired for the default pod-exec RackSetter")
	}

	podName, container, err := r.resolveExecTarget(ctx, bk, bookiePods)
	if err != nil {
		return nil, err
	}

	return &rackawareness.PodExecRackSetter{
		RESTConfig: r.RESTConfig,
		ClientSet:  r.ClientSet,
		Namespace:  bk.Namespace,
		PodName:    podName,
		Container:  container,
	}, nil
}

// resolveExecTarget picks a running pod to pod-exec pulsar-admin into.
// pulsar-admin's bundled conf/client.conf resolves to a reachable admin REST
// endpoint out of the box when run inside a Pulsar broker pod (its own
// webServicePort, normally 8080), so a sibling Broker's pod is preferred
// whenever this BookKeeper's owner is a PulsarCluster (using the
// "<cluster>-broker" child-naming convention from childName in
// pulsarcluster_controller.go). Falling that (a standalone BookKeeper with
// no owning PulsarCluster, or no broker pod is Running yet), a running
// bookie pod is used as a best-effort fallback - see the PR description for
// the corresponding operational caveat.
func (r *BookKeeperRackReconciler) resolveExecTarget(ctx context.Context, bk *clusterv1alpha1.BookKeeper, bookiePods []corev1.Pod) (podName, container string, err error) {
	if pod, ok := r.findRunningBrokerPod(ctx, bk); ok {
		return pod, brokerComponent, nil
	}

	for _, pod := range bookiePods {
		if pod.Status.Phase == corev1.PodRunning {
			return pod.Name, bookieContainerName, nil
		}
	}

	return "", "", fmt.Errorf("no running bookie or broker pod available to exec pulsar-admin into")
}

func (r *BookKeeperRackReconciler) findRunningBrokerPod(ctx context.Context, bk *clusterv1alpha1.BookKeeper) (podName string, ok bool) {
	ownerName, ok := pulsarClusterOwnerName(bk)
	if !ok {
		return "", false
	}

	var pods corev1.PodList
	brokerName := childName(ownerName, brokerComponent)
	if err := r.List(ctx, &pods, client.InNamespace(bk.Namespace), client.MatchingLabels(builder.SelectorLabels(brokerName, brokerComponent))); err != nil {
		return "", false
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			return pod.Name, true
		}
	}
	return "", false
}

func pulsarClusterOwnerName(bk *clusterv1alpha1.BookKeeper) (name string, ok bool) {
	for _, ref := range bk.OwnerReferences {
		if ref.Kind == pulsarClusterOwnerKind {
			return ref.Name, true
		}
	}
	return "", false
}

// recordRackSyncResult sets the RackSync condition, persists the next-tick
// bookieRacks cache, and emits a summarizing Event. Status is only patched
// when the condition or the cache actually changed, and an Event is only
// emitted when the condition text changed, so a steady-state, all-synced
// tick after the first doesn't send a duplicate Event or empty PATCH every
// time.
func (r *BookKeeperRackReconciler) recordRackSyncResult(ctx context.Context, bk *clusterv1alpha1.BookKeeper, applied map[string]string, total, synced int, failed []string, skipped int) error {
	unchanged := total - synced - len(failed)

	cond := metav1.Condition{Type: conditionTypeRackSync, ObservedGeneration: bk.Generation}
	var eventType, eventReason string
	if len(failed) == 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = reasonRackSynced
		cond.Message = fmt.Sprintf("bookie rack sync: %d synced, %d already up to date, %d skipped (pod/node/zone not ready yet)", synced, unchanged, skipped)
		eventType, eventReason = corev1.EventTypeNormal, reasonRackSynced
	} else {
		cond.Status = metav1.ConditionFalse
		cond.Reason = reasonRackSyncPartial
		cond.Message = fmt.Sprintf("bookie rack sync: %d synced, %d already up to date, %d skipped, %d failed: %s",
			synced, unchanged, skipped, len(failed), strings.Join(failed, ", "))
		eventType, eventReason = corev1.EventTypeWarning, reasonRackSyncPartial
	}

	mapChanged := !maps.Equal(bk.Status.BookieRacks, applied)

	patch := client.MergeFrom(bk.DeepCopy())
	bk.Status.BookieRacks = applied
	condChanged := apimeta.SetStatusCondition(&bk.Status.Conditions, cond)

	if !condChanged && !mapChanged {
		return nil
	}

	if err := r.Status().Patch(ctx, bk, patch); err != nil {
		return fmt.Errorf("update bookkeeper %s/%s rack-sync status: %w", bk.Namespace, bk.Name, err)
	}

	if condChanged {
		r.recorder().Eventf(bk, nil, eventType, eventReason, "RackSync", "%s", cond.Message)
	}
	return nil
}

// recordRackSetterUnavailable surfaces a tick that could not resolve any
// RackSetter (e.g. no running bookie/broker pod to exec pulsar-admin into
// yet) as a Warning Event and a False RackSync condition, without touching
// bookieRacks: nothing was applied or diffed this tick, so the cache from
// the last successful tick must be left untouched.
func (r *BookKeeperRackReconciler) recordRackSetterUnavailable(ctx context.Context, bk *clusterv1alpha1.BookKeeper, cause string) error {
	patch := client.MergeFrom(bk.DeepCopy())
	changed := apimeta.SetStatusCondition(&bk.Status.Conditions, metav1.Condition{
		Type:               conditionTypeRackSync,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: bk.Generation,
		Reason:             reasonRackSyncUnavailable,
		Message:            fmt.Sprintf("bookie rack sync skipped this tick: %s", cause),
	})
	if !changed {
		return nil
	}
	if err := r.Status().Patch(ctx, bk, patch); err != nil {
		return fmt.Errorf("update bookkeeper %s/%s rack-sync status: %w", bk.Namespace, bk.Name, err)
	}
	r.recorder().Eventf(bk, nil, corev1.EventTypeWarning, reasonRackSyncUnavailable, "RackSync", "bookie rack sync skipped this tick: %s", cause)
	return nil
}

func resolveRackPeriodSeconds(cfg *clusterv1alpha1.BookKeeperAutoRackConfig) int32 {
	if cfg.PeriodSeconds != nil {
		return *cfg.PeriodSeconds
	}
	return defaultRackSyncPeriodSeconds
}

// SetupWithManager sets up the controller with the Manager.
func (r *BookKeeperRackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.BookKeeper{}).
		Named("cluster-bookkeeper-rack-sync").
		Complete(r)
}
