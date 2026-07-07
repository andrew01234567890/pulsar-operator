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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	autoscalerbroker "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/broker"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const (
	resourceUsageSourceK8SMetrics = "K8SMetrics"

	// Defaults mirror BrokerAutoscalerSpec's own kubebuilder defaults so a
	// Broker built without going through API-server defaulting (a unit
	// test, or a directly-applied manifest on an old schema) still behaves
	// the way the CRD documents. min has no kubebuilder default (there's no
	// sensible cluster-wide constant to put in the schema), so it is
	// defaulted here instead, per the KAAP-derived design.
	defaultAutoscalerMinReplicas          int32 = 2
	defaultAutoscalerLowerCPUThreshold    int32 = 30
	defaultAutoscalerHigherCPUThreshold   int32 = 80
	defaultAutoscalerScaleStep            int32 = 1
	defaultAutoscalerStabilizationSeconds int32 = 300
	defaultAutoscalerPeriodSeconds        int32 = 60

	// conditionTypeAutoscaling reports the broker autoscaler's most recent
	// decision (or why it declined to decide) - distinct from
	// conditionTypeReady, which reports rollout convergence.
	conditionTypeAutoscaling = "Autoscaling"

	reasonAutoscalerDisabled    = "Disabled"
	reasonPodsNotReady          = "PodsNotReady"
	reasonAwaitingStabilization = "AwaitingStabilization"
	reasonMetricsUnavailable    = "MetricsUnavailable"
)

// BrokerAutoscalerReconciler drives BrokerSpec.Replicas from observed broker
// CPU load. It is deliberately a second, independent controller over Broker
// rather than logic folded into BrokerReconciler: the two have unrelated
// failure domains (rendering a StatefulSet vs. polling live metrics and
// making a scaling judgment call), and keeping them apart means a bug in one
// can never block the other from reconciling.
//
// Scaling down never gets special "drain" handling here: BrokerReconciler
// already wires a preStop sleep + terminationGracePeriodSeconds long enough
// for Pulsar's own shutdown hook to unload the terminating pod's bundles
// (see broker_controller.go). That is a graceful shutdown, not a live
// bundle-transfer handover (ExtensibleLoadManagerImpl's TransferShedder) -
// in-flight requests to the terminating broker can still be affected. This
// autoscaler only ever changes the replica count; it does not attempt to
// pick which ordinal drains first or to await zero owned bundles before
// terminating.
type BrokerAutoscalerReconciler struct {
	client.Client
	Recorder events.EventRecorder

	// LoadClient, when non-nil, is used for every Broker regardless of its
	// spec.autoscaler.resourcesUsageSource. Production wiring leaves this
	// nil so each Broker's own field selects between PulsarLoadReportClient
	// and K8sMetricsClient; tests set it to a mock to fully control CPU
	// readings.
	LoadClient autoscalerbroker.LoadClient

	// Now defaults to time.Now; tests override it to make the
	// stabilization-window gate deterministic without sleeping.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list

// Reconcile evaluates one Broker's autoscaler tick: gated on
// spec.autoscaler.enabled, it requires the StatefulSet to be fully stable
// (all pods Ready, stabilization window elapsed since the last scale) before
// reading CPU and applying the unanimous scale-up/scale-down vote.
func (r *BrokerAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	broker := &clusterv1alpha1.Broker{}
	if err := r.Get(ctx, req.NamespacedName, broker); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	spec := broker.Spec.Autoscaler
	if spec == nil || !spec.Enabled {
		return ctrl.Result{}, r.setCondition(ctx, broker, metav1.ConditionFalse, reasonAutoscalerDisabled, "broker autoscaler is disabled")
	}

	period := time.Duration(int32OrDefault(spec.PeriodSeconds, defaultAutoscalerPeriodSeconds)) * time.Second
	currentReplicas := brokerReplicas(broker.Spec.Replicas)

	pods, err := r.listBrokerPods(ctx, broker)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing broker pods: %w", err)
	}

	if !autoscalerbroker.PodsStable(pods, currentReplicas) {
		msg := fmt.Sprintf("waiting for all %d broker pods to be Ready before evaluating autoscaling", currentReplicas)
		return ctrl.Result{RequeueAfter: period}, r.setCondition(ctx, broker, metav1.ConditionFalse, reasonPodsNotReady, msg)
	}

	windowSeconds := int32OrDefault(spec.StabilizationWindowSeconds, defaultAutoscalerStabilizationSeconds)
	if !autoscalerbroker.StabilizationElapsed(broker.Status.LastScaleTime, windowSeconds, r.now()) {
		msg := fmt.Sprintf("stabilization window (%ds) has not elapsed since the last scaling event", windowSeconds)
		return ctrl.Result{RequeueAfter: period}, r.setCondition(ctx, broker, metav1.ConditionFalse, reasonAwaitingStabilization, msg)
	}

	httpPort := resolveBrokerPorts(mergedBrokerConfig(broker)).http
	cpuByBroker, metricsErr := r.resolveLoadClient(broker, httpPort).CPUPercentByBroker(ctx, pods)
	if int32(len(cpuByBroker)) != currentReplicas {
		msg := fmt.Sprintf("CPU reading available for %d/%d brokers", len(cpuByBroker), currentReplicas)
		if metricsErr != nil {
			log.Error(metricsErr, "reading broker CPU metrics")
			msg = fmt.Sprintf("%s: %v", msg, metricsErr)
		}
		return ctrl.Result{RequeueAfter: period}, r.setCondition(ctx, broker, metav1.ConditionFalse, reasonMetricsUnavailable, msg)
	}

	decision := autoscalerbroker.Decide(cpuByBroker, autoscalerbroker.Params{
		LowerCPUPercent:  int32OrDefault(spec.LowerCpuThreshold, defaultAutoscalerLowerCPUThreshold),
		HigherCPUPercent: int32OrDefault(spec.HigherCpuThreshold, defaultAutoscalerHigherCPUThreshold),
		ScaleUpBy:        int32OrDefault(spec.ScaleUpBy, defaultAutoscalerScaleStep),
		ScaleDownBy:      int32OrDefault(spec.ScaleDownBy, defaultAutoscalerScaleStep),
		MinReplicas:      int32OrDefault(spec.Min, defaultAutoscalerMinReplicas),
		MaxReplicas:      int32OrDefault(spec.Max, 0),
		CurrentReplicas:  currentReplicas,
	})

	if decision.Direction == autoscalerbroker.NoOp {
		return ctrl.Result{RequeueAfter: period}, r.setCondition(ctx, broker, metav1.ConditionFalse, decision.Reason, decision.Message)
	}

	if err := r.applyScale(ctx, broker, decision); err != nil {
		return ctrl.Result{}, fmt.Errorf("applying autoscaler decision: %w", err)
	}

	log.Info("broker autoscaler scaled replicas",
		"direction", decision.Direction, "from", currentReplicas, "to", decision.TargetReplicas, "reason", decision.Reason)
	if r.Recorder != nil {
		// events API: regarding=broker, no related object, reason is the
		// machine-readable scale direction, action names what the controller
		// did, note is the human-readable message.
		r.Recorder.Eventf(broker, nil, corev1.EventTypeNormal, decision.Reason, "Scale", decision.Message)
	}

	return ctrl.Result{RequeueAfter: period}, nil
}

// applyScale patches spec.replicas and, in the same pass, records the scale
// event onto status: last-scale-time (which gates the next stabilization
// window) and the Autoscaling condition. The spec and status updates are two
// API calls (status is a separate subresource) but there is no meaningful
// partial-failure recovery to add beyond returning the error and letting the
// next periodic tick re-evaluate.
func (r *BrokerAutoscalerReconciler) applyScale(ctx context.Context, broker *clusterv1alpha1.Broker, decision autoscalerbroker.Decision) error {
	broker.Spec.Replicas = ptrInt32(decision.TargetReplicas)
	if err := r.Update(ctx, broker); err != nil {
		return fmt.Errorf("updating broker replicas: %w", err)
	}

	now := metav1.NewTime(r.now())
	broker.Status.LastScaleTime = &now
	return r.setCondition(ctx, broker, metav1.ConditionTrue, decision.Reason, decision.Message)
}

func (r *BrokerAutoscalerReconciler) setCondition(ctx context.Context, broker *clusterv1alpha1.Broker, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&broker.Status.Conditions, metav1.Condition{
		Type:               conditionTypeAutoscaling,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: broker.Generation,
	})
	return r.Status().Update(ctx, broker)
}

func (r *BrokerAutoscalerReconciler) listBrokerPods(ctx context.Context, broker *clusterv1alpha1.Broker) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(broker.Namespace),
		client.MatchingLabels(builder.SelectorLabels(broker.Name, brokerComponent)),
	); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// resolveLoadClient picks the concrete LoadClient for a Broker's
// spec.autoscaler.resourcesUsageSource, unless a fixed LoadClient was
// injected (production wiring never sets one; tests do).
func (r *BrokerAutoscalerReconciler) resolveLoadClient(broker *clusterv1alpha1.Broker, httpPort int32) autoscalerbroker.LoadClient {
	if r.LoadClient != nil {
		return r.LoadClient
	}
	if broker.Spec.Autoscaler.ResourcesUsageSource == resourceUsageSourceK8SMetrics {
		return &autoscalerbroker.K8sMetricsClient{Client: r.Client}
	}
	return &autoscalerbroker.PulsarLoadReportClient{HTTPPort: httpPort}
}

func (r *BrokerAutoscalerReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func int32OrDefault(v *int32, def int32) int32 {
	if v != nil {
		return *v
	}
	return def
}

func ptrInt32(v int32) *int32 {
	return &v
}

// SetupWithManager sets up the controller with the Manager. It does not
// Own() the pods it lists (they belong to the StatefulSet BrokerReconciler
// owns, not to this Broker directly): the autoscaler is a polling loop by
// design, driven by RequeueAfter rather than pod-level watch events.
func (r *BrokerAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.Broker{}).
		Named("cluster-broker-autoscaler").
		Complete(r)
}
