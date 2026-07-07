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

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	bkautoscaler "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/bookkeeper"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const (
	// conditionTypeAutoscaling ("Autoscaling") is declared once for this
	// package in broker_autoscaler_controller.go; this controller sets it on
	// BookKeeper only when it actually acts (a scale-up) or detects a
	// configuration problem, so a stable, no-op tick leaves it untouched
	// rather than resetting LastTransitionTime on every poll.

	reasonInvalidAutoscalerConfig = "InvalidAutoscalerConfig"
	reasonBookieAdminPollFailed   = "BookieAdminPollFailed"

	// The following mirror BookKeeperAutoscalerSpec's kubebuilder defaults
	// (api/cluster/v1alpha1/bookkeeper_types.go) and BookKeeperEnsembleSpec's
	// EnsembleSize default; they are the fallback used when a cluster was
	// created against an older CRD version or a client that skipped
	// server-side defaulting, so this controller never has to treat an
	// unset optional field as a zero value.
	defaultAutoscalerEnsembleSize            int32 = 3
	defaultAutoscalerScaleUpBy               int32 = 1
	defaultAutoscalerHwmPercent              int32 = 92
	defaultAutoscalerStabilizationWindowSecs int32 = 300
	defaultAutoscalerPeriodSecs              int32 = 10

	// unboundedScaleUpMaxLimit is used when
	// BookKeeperAutoscalerSpec.ScaleUpMaxLimit is unset: absent an explicit
	// ceiling, the autoscaler never clamps a scale-up.
	unboundedScaleUpMaxLimit int32 = 0
)

// BookKeeperAutoscalerReconciler implements the bookie disk-watermark
// scale-UP autoscaler documented in docs/docs/autoscaling.md. It is
// deliberately a separate controller from BookKeeperReconciler (which owns
// the StatefulSet/Service/ConfigMap/PodDisruptionBudget): this one only ever
// polls bookie admin state and patches BookKeeper.spec.replicas upward. The
// guarded, opt-in bookie scale-down/decommission state machine is out of
// scope here (BookKeeperAutoscalerSpec.ScaleDownEnabled is never consulted)
// and ships in a follow-up.
type BookKeeperAutoscalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Recorder emits Events for scale actions and configuration problems.
	// SetupWithManager defaults it to mgr.GetEventRecorderFor(...); a
	// nil Recorder is treated as a no-op sink so tests may leave it unset.
	Recorder record.EventRecorder

	// AdminClient polls each bookie's admin REST API. Nil defaults to
	// bkautoscaler.NewHTTPBookieAdminClient(), letting tests inject a mock
	// instead of talking to a live bookie ensemble.
	AdminClient bkautoscaler.BookieAdminClient

	// Now returns the current time; nil defaults to time.Now. Tests
	// override it to make the stabilization window deterministic.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile evaluates one bookie disk-watermark autoscaler tick: it is a
// no-op unless spec.autoscaler.enabled, gates on the stabilization window
// (see isStabilized), then applies the strict-priority scale-up algorithm in
// internal/autoscaler/bookkeeper against the bookie ensemble's admin REST
// API. It always requeues at spec.autoscaler.periodSeconds while enabled, so
// disk usage is re-checked on a fixed cadence rather than only on spec
// changes.
func (r *BookKeeperAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	bk := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, req.NamespacedName, bk); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	autoscaler := bk.Spec.Autoscaler
	if autoscaler == nil || !autoscaler.Enabled {
		return ctrl.Result{}, nil
	}

	result := ctrl.Result{RequeueAfter: time.Duration(resolveAutoscalerPeriodSeconds(autoscaler)) * time.Second}

	// minWritableBookies must never be lower than the ensemble size: doing
	// so would let the autoscaler consider the cluster "healthy" with too
	// few writable bookies to stripe a new ledger across. There is no
	// validating webhook for this yet, so it is checked here, every tick,
	// and surfaced as a Warning condition/Event instead of scaling.
	ensembleSize := resolveAutoscalerEnsembleSize(bk.Spec)
	minWritable := resolveAutoscalerMinWritableBookies(autoscaler, ensembleSize)
	if minWritable < ensembleSize {
		if err := r.recordInvalidConfig(ctx, bk, ensembleSize, minWritable); err != nil {
			return ctrl.Result{}, err
		}
		return result, nil
	}

	if !r.isStabilized(bk, autoscaler) {
		log.V(1).Info("bookie autoscaler stabilization gate is blocking evaluation this tick")
		return result, nil
	}

	pods, err := r.listBookiePods(ctx, bk)
	if err != nil {
		return ctrl.Result{}, err
	}

	currentReplicas := resolveReplicas(bk.Spec)

	// Only evaluate when every desired bookie is addressable this tick. A
	// pod mid-recreate has no PodIP yet, so polling would silently see a
	// short ensemble; combined with the stale (still all-ready) status the
	// stabilization gate reads, that under-counts writable bookies and — in
	// this scale-up-only controller — would fire the deficit branch and
	// strand a permanent phantom replica. Skipping the tick (and requeuing)
	// until the poll can be complete is the safe choice.
	addrs := bookieAdminAddrs(pods)
	if int32(len(addrs)) < currentReplicas {
		log.V(1).Info("bookie autoscaler skipping tick: not every bookie is addressable yet",
			"addressable", len(addrs), "desiredReplicas", currentReplicas)
		return result, nil
	}

	params := bkautoscaler.Params{
		CurrentReplicas:              currentReplicas,
		MinWritableBookies:           minWritable,
		ScaleUpBy:                    resolveAutoscalerScaleUpBy(autoscaler),
		ScaleUpMaxLimit:              resolveAutoscalerScaleUpMaxLimit(autoscaler),
		DiskUsageToleranceHwmPercent: resolveAutoscalerHwmPercent(autoscaler),
	}

	decision, err := bkautoscaler.Evaluate(ctx, r.adminClient(), addrs, params)
	if err != nil {
		log.Error(err, "failed to poll bookie admin API")
		r.recorder().Eventf(bk, corev1.EventTypeWarning, reasonBookieAdminPollFailed, "failed to poll bookie admin API: %v", err)
		return result, nil
	}

	if err := r.recordWritableBookies(ctx, bk, decision.WritableBookies); err != nil {
		return ctrl.Result{}, err
	}

	if !decision.ShouldScale {
		return result, nil
	}

	if err := r.scaleUp(ctx, bk, params.CurrentReplicas, decision); err != nil {
		return ctrl.Result{}, err
	}

	return result, nil
}

func (r *BookKeeperAutoscalerReconciler) adminClient() bkautoscaler.BookieAdminClient {
	if r.AdminClient != nil {
		return r.AdminClient
	}
	return bkautoscaler.NewHTTPBookieAdminClient()
}

func (r *BookKeeperAutoscalerReconciler) recorder() record.EventRecorder {
	if r.Recorder != nil {
		return r.Recorder
	}
	return &record.FakeRecorder{}
}

func (r *BookKeeperAutoscalerReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// isStabilized gates scaling on two independent conditions: every bookie pod
// must currently be Ready (read off BookKeeper.status, as last written by
// BookKeeperReconciler, so this controller never has to re-derive StatefulSet
// rollout state), and at least stabilizationWindowSeconds must have elapsed
// since the last scale-up, so a burst of scale-ups can't flap before the
// newly added bookies have had a chance to take on load.
func (r *BookKeeperAutoscalerReconciler) isStabilized(bk *clusterv1alpha1.BookKeeper, autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec) bool {
	desired := resolveReplicas(bk.Spec)
	if bk.Status.Replicas != desired || bk.Status.ReadyReplicas != desired {
		return false
	}

	readyCond := apimeta.FindStatusCondition(bk.Status.Conditions, conditionTypeReady)
	if readyCond == nil || readyCond.Status != metav1.ConditionTrue {
		return false
	}

	if bk.Status.LastScaleTime == nil {
		return true
	}
	window := time.Duration(resolveAutoscalerStabilizationWindowSeconds(autoscaler)) * time.Second
	return r.now().Sub(bk.Status.LastScaleTime.Time) >= window
}

func (r *BookKeeperAutoscalerReconciler) listBookiePods(ctx context.Context, bk *clusterv1alpha1.BookKeeper) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(bk.Namespace), client.MatchingLabels(builder.SelectorLabels(bk.Name, bookkeeperComponent))); err != nil {
		return nil, fmt.Errorf("list bookie pods for %s/%s: %w", bk.Namespace, bk.Name, err)
	}
	return pods.Items, nil
}

// bookieAdminAddrs resolves each pod to its admin REST address via pod IP
// rather than headless-Service DNS: it needs no assumption about the
// cluster's DNS suffix and works identically against a real cluster or an
// envtest-style test double. Pods without an assigned IP yet are dropped;
// the caller compares the returned count against the desired replica count
// and skips the whole tick when any bookie is not yet addressable, so a
// partial ensemble is never handed to Evaluate.
func bookieAdminAddrs(pods []corev1.Pod) []string {
	addrs := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod.Status.PodIP == "" {
			continue
		}
		addrs = append(addrs, fmt.Sprintf("%s:%d", pod.Status.PodIP, bookieAdminPort))
	}
	return addrs
}

func (r *BookKeeperAutoscalerReconciler) recordWritableBookies(ctx context.Context, bk *clusterv1alpha1.BookKeeper, writable int32) error {
	if bk.Status.WritableBookies == writable {
		return nil
	}
	patch := client.MergeFrom(bk.DeepCopy())
	bk.Status.WritableBookies = writable
	if err := r.Status().Patch(ctx, bk, patch); err != nil {
		return fmt.Errorf("update bookkeeper %s/%s status.writableBookies: %w", bk.Namespace, bk.Name, err)
	}
	return nil
}

// scaleUp patches spec.replicas up in its own request, then separately
// records status.lastScaleTime and the Autoscaling condition: the spec and
// status subresources are written independently in this codebase (see
// BookKeeperReconciler.updateStatus), and only the spec patch is required
// for the scale-up to take effect, so its success shouldn't depend on the
// status write also succeeding.
func (r *BookKeeperAutoscalerReconciler) scaleUp(ctx context.Context, bk *clusterv1alpha1.BookKeeper, previousReplicas int32, decision bkautoscaler.Decision) error {
	log := logf.FromContext(ctx)

	specPatch := client.MergeFrom(bk.DeepCopy())
	target := decision.TargetReplicas
	bk.Spec.Replicas = &target
	if err := r.Patch(ctx, bk, specPatch); err != nil {
		return fmt.Errorf("scale bookkeeper %s/%s from %d to %d replicas: %w", bk.Namespace, bk.Name, previousReplicas, target, err)
	}

	now := metav1.NewTime(r.now())
	statusPatch := client.MergeFrom(bk.DeepCopy())
	bk.Status.LastScaleTime = &now
	apimeta.SetStatusCondition(&bk.Status.Conditions, metav1.Condition{
		Type:               conditionTypeAutoscaling,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: bk.Generation,
		Reason:             decision.Reason,
		Message:            decision.Message,
	})
	if err := r.Status().Patch(ctx, bk, statusPatch); err != nil {
		return fmt.Errorf("update bookkeeper %s/%s autoscaler status: %w", bk.Namespace, bk.Name, err)
	}

	log.Info("bookie autoscaler scaled up", "from", previousReplicas, "to", target, "reason", decision.Reason)
	r.recorder().Eventf(bk, corev1.EventTypeNormal, decision.Reason, "scaled bookies from %d to %d: %s", previousReplicas, target, decision.Message)
	return nil
}

// recordInvalidConfig surfaces a misconfigured minWritableBookies as a
// Warning Event and a False Autoscaling condition. It patches status only
// when the condition actually changes, so a persistently misconfigured
// BookKeeper doesn't send an empty PATCH (and a duplicate Event) every
// single tick.
func (r *BookKeeperAutoscalerReconciler) recordInvalidConfig(ctx context.Context, bk *clusterv1alpha1.BookKeeper, ensembleSize, minWritable int32) error {
	msg := fmt.Sprintf(
		"autoscaler.minWritableBookies (%d) must be >= ensemble.ensembleSize (%d); bookie scale-up is disabled until this is corrected",
		minWritable, ensembleSize)

	patch := client.MergeFrom(bk.DeepCopy())
	changed := apimeta.SetStatusCondition(&bk.Status.Conditions, metav1.Condition{
		Type:               conditionTypeAutoscaling,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: bk.Generation,
		Reason:             reasonInvalidAutoscalerConfig,
		Message:            msg,
	})
	if !changed {
		return nil
	}
	if err := r.Status().Patch(ctx, bk, patch); err != nil {
		return fmt.Errorf("update bookkeeper %s/%s autoscaler status: %w", bk.Namespace, bk.Name, err)
	}
	r.recorder().Event(bk, corev1.EventTypeWarning, reasonInvalidAutoscalerConfig, msg)
	return nil
}

func resolveAutoscalerEnsembleSize(spec clusterv1alpha1.BookKeeperSpec) int32 {
	if spec.Ensemble != nil && spec.Ensemble.EnsembleSize != nil {
		return *spec.Ensemble.EnsembleSize
	}
	return defaultAutoscalerEnsembleSize
}

// resolveAutoscalerMinWritableBookies defaults an unset minWritableBookies
// to ensembleSize: the minimum value that still satisfies the
// minWritableBookies >= ensembleSize invariant this controller requires.
func resolveAutoscalerMinWritableBookies(autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec, ensembleSize int32) int32 {
	if autoscaler.MinWritableBookies != nil {
		return *autoscaler.MinWritableBookies
	}
	return ensembleSize
}

func resolveAutoscalerScaleUpBy(autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec) int32 {
	if autoscaler.ScaleUpBy != nil {
		return *autoscaler.ScaleUpBy
	}
	return defaultAutoscalerScaleUpBy
}

func resolveAutoscalerScaleUpMaxLimit(autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec) int32 {
	if autoscaler.ScaleUpMaxLimit != nil {
		return *autoscaler.ScaleUpMaxLimit
	}
	return unboundedScaleUpMaxLimit
}

func resolveAutoscalerHwmPercent(autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec) int32 {
	if autoscaler.DiskUsageToleranceHwm != nil {
		return *autoscaler.DiskUsageToleranceHwm
	}
	return defaultAutoscalerHwmPercent
}

func resolveAutoscalerStabilizationWindowSeconds(autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec) int32 {
	if autoscaler.StabilizationWindowSeconds != nil {
		return *autoscaler.StabilizationWindowSeconds
	}
	return defaultAutoscalerStabilizationWindowSecs
}

func resolveAutoscalerPeriodSeconds(autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec) int32 {
	if autoscaler.PeriodSeconds != nil {
		return *autoscaler.PeriodSeconds
	}
	return defaultAutoscalerPeriodSecs
}

// SetupWithManager sets up the controller with the Manager.
func (r *BookKeeperAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("bookkeeper-autoscaler")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.BookKeeper{}).
		Named("cluster-bookkeeper-autoscaler").
		Complete(r)
}
