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
	"math"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

const (
	brokerComponent = "broker"

	brokerPortName       = "pulsar"
	brokerPort     int32 = 6650
	httpPortName         = "http"
	httpPort       int32 = 8080

	// defaultBrokerReplicas mirrors the HA-defaults table: 3 brokers so a
	// rolling restart or single-pod disruption never drops to zero capacity.
	defaultBrokerReplicas int32 = 3

	// defaultBrokerImage keeps a standalone Broker CR (no owning
	// PulsarCluster, e.g. in tests or a directly-applied manifest)
	// schedulable even though PulsarCluster.spec.image is normally what
	// resolves BrokerSpec.Image before the child Broker is created.
	defaultBrokerImage = "apachepulsar/pulsar:5.0.0-M1"

	// extensibleLoadManagerClassName is the operator's recommended default:
	// the only load manager that supports live bundle-transfer scale-down.
	extensibleLoadManagerClassName = "org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl"
	// simpleLoadManagerClassName is upstream broker.conf's own legacy
	// default, kept selectable via BrokerSpec.LoadBalancer="simple".
	simpleLoadManagerClassName = "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl"
	loadBalancerSimple         = "simple"

	// broker.conf keys the operator sets or reads back out of the merged config.
	confKeyBrokerServicePort       = "brokerServicePort"
	confKeyWebServicePort          = "webServicePort"
	confKeyLoadManagerClassName    = "loadManagerClassName"
	confKeyBrokerShutdownTimeoutMs = "brokerShutdownTimeoutMs"

	// defaultBrokerShutdownTimeoutMs matches upstream broker.conf's own
	// default: how long Pulsar's shutdown hook is given to unload bundles
	// before the process is force-killed.
	defaultBrokerShutdownTimeoutMs int64 = 60000

	// preStopDrainSeconds is how long the preStop hook sleeps before SIGTERM
	// reaches the broker process. A Service only stops routing to a
	// terminating pod once Endpoints/kube-proxy converge, which races with
	// SIGTERM delivery; sleeping here gives that convergence a head start so
	// the broker isn't handed new lookups right as it begins unloading
	// bundles.
	preStopDrainSeconds int64 = 5

	// defaultBrokerMaxUnavailable bounds voluntary disruption to one broker
	// at a time. Unlike BookKeeper/Oxia, the broker tier has no ensemble
	// quorum math to derive this from - it's a flat, conservative default.
	defaultBrokerMaxUnavailable = 1

	configVolumeName    = "config"
	brokerConfFileName  = "broker.conf"
	brokerConfMountPath = "/pulsar/conf/broker.conf"

	brokerReadyConditionType = "Ready"
	reasonReplicasReady      = "ReplicasReady"
	reasonReplicasNotReady   = "ReplicasNotReady"

	healthCheckPath = "/admin/v2/brokers/health"
)

// BrokerReconciler reconciles a Broker object
type BrokerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile moves a Broker's actual cluster state - a StatefulSet, headless
// Service, ConfigMap, and PodDisruptionBudget - towards the state described
// by its spec.
func (r *BrokerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	broker := &clusterv1alpha1.Broker{}
	if err := r.Get(ctx, req.NamespacedName, broker); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	name := broker.Name
	labels := builder.Labels(broker.Name, brokerComponent)
	selectorLabels := builder.SelectorLabels(broker.Name, brokerComponent)

	mergedConfig := mergedBrokerConfig(broker)
	renderedConfig := config.RenderProperties(mergedConfig)

	if err := r.reconcileConfigMap(ctx, broker, name, labels, renderedConfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker configmap: %w", err)
	}

	if err := r.reconcileHeadlessService(ctx, broker, name, labels, selectorLabels); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker headless service: %w", err)
	}

	stsSpec := desiredBrokerStatefulSetSpec(broker, name, selectorLabels, labels, mergedConfig, renderedConfig)
	if err := r.reconcileStatefulSet(ctx, broker, name, labels, stsSpec); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker statefulset: %w", err)
	}

	if err := r.reconcilePDB(ctx, broker, name, labels, selectorLabels); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker poddisruptionbudget: %w", err)
	}

	if err := r.updateStatus(ctx, broker, name); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating broker status: %w", err)
	}

	log.V(1).Info("reconciled broker", "name", name)

	return ctrl.Result{}, nil
}

func (r *BrokerReconciler) reconcileConfigMap(ctx context.Context, broker *clusterv1alpha1.Broker, name string, labels map[string]string, renderedConfig string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: broker.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{brokerConfFileName: renderedConfig}
		return builder.SetControllerOwner(broker, cm, r.Scheme)
	})
	return err
}

func (r *BrokerReconciler) reconcileHeadlessService(ctx context.Context, broker *clusterv1alpha1.Broker, name string, labels, selectorLabels map[string]string) error {
	built := builder.HeadlessService(name, broker.Namespace, labels, selectorLabels, brokerServicePorts())

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: broker.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = built.Labels
		svc.Spec.Selector = built.Spec.Selector
		svc.Spec.Ports = built.Spec.Ports
		svc.Spec.ClusterIP = built.Spec.ClusterIP
		svc.Spec.PublishNotReadyAddresses = built.Spec.PublishNotReadyAddresses
		return builder.SetControllerOwner(broker, svc, r.Scheme)
	})
	return err
}

func (r *BrokerReconciler) reconcileStatefulSet(ctx context.Context, broker *clusterv1alpha1.Broker, name string, labels map[string]string, spec appsv1.StatefulSetSpec) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: broker.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = labels
		sts.Spec.Replicas = spec.Replicas
		sts.Spec.ServiceName = spec.ServiceName
		sts.Spec.Selector = spec.Selector
		sts.Spec.Template = spec.Template
		return builder.SetControllerOwner(broker, sts, r.Scheme)
	})
	return err
}

func (r *BrokerReconciler) reconcilePDB(ctx context.Context, broker *clusterv1alpha1.Broker, name string, labels, selectorLabels map[string]string) error {
	if !brokerPDBEnabled(broker.Spec.Pdb) {
		pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: broker.Namespace}}
		if err := r.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	built := builder.PodDisruptionBudget(name, broker.Namespace, labels, selectorLabels, brokerMaxUnavailable(broker.Spec.Pdb))

	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: broker.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = built.Labels
		pdb.Spec = built.Spec
		return builder.SetControllerOwner(broker, pdb, r.Scheme)
	})
	return err
}

func (r *BrokerReconciler) updateStatus(ctx context.Context, broker *clusterv1alpha1.Broker, name string) error {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: broker.Namespace}, sts); err != nil {
		return client.IgnoreNotFound(err)
	}

	desired := brokerReplicas(broker.Spec.Replicas)

	broker.Status.Replicas = sts.Status.Replicas
	broker.Status.ReadyReplicas = sts.Status.ReadyReplicas
	broker.Status.ObservedGeneration = broker.Generation
	apimeta.SetStatusCondition(&broker.Status.Conditions, brokerReadyCondition(sts.Status.ReadyReplicas, desired))

	return r.Status().Update(ctx, broker)
}

// brokerReadyCondition reports Ready=True only once every desired replica is
// Ready, mirroring the StatefulSet rollout itself rather than declaring
// success as soon as the StatefulSet object is created.
func brokerReadyCondition(readyReplicas, desired int32) metav1.Condition {
	if readyReplicas == desired {
		return metav1.Condition{
			Type:    brokerReadyConditionType,
			Status:  metav1.ConditionTrue,
			Reason:  reasonReplicasReady,
			Message: fmt.Sprintf("all %d broker replicas ready", desired),
		}
	}
	return metav1.Condition{
		Type:    brokerReadyConditionType,
		Status:  metav1.ConditionFalse,
		Reason:  reasonReplicasNotReady,
		Message: fmt.Sprintf("%d/%d broker replicas ready", readyReplicas, desired),
	}
}

// mergedBrokerConfig layers spec.Config on top of the operator's broker.conf
// defaults. metadataStoreUrl/configurationMetadataStoreUrl are deliberately
// absent from the defaults: they name the metadata store deployment (Oxia
// today), which is not this reconciler's concern, so they only ever come
// from spec.Config (typically populated by the PulsarCluster reconciler).
func mergedBrokerConfig(broker *clusterv1alpha1.Broker) map[string]string {
	return config.Merge(defaultBrokerConfig(broker.Spec), broker.Spec.Config)
}

func defaultBrokerConfig(spec clusterv1alpha1.BrokerSpec) map[string]string {
	return map[string]string{
		confKeyBrokerServicePort:       strconv.Itoa(int(brokerPort)),
		confKeyWebServicePort:          strconv.Itoa(int(httpPort)),
		confKeyLoadManagerClassName:    loadManagerClassName(spec.LoadBalancer),
		confKeyBrokerShutdownTimeoutMs: strconv.FormatInt(defaultBrokerShutdownTimeoutMs, 10),
	}
}

// loadManagerClassName maps BrokerSpec.LoadBalancer to the concrete Pulsar
// load-manager class. An empty value (a Broker constructed without going
// through API-server defaulting, e.g. in a unit test) is treated the same as
// "extensible", matching the field's kubebuilder default.
func loadManagerClassName(loadBalancer string) string {
	if loadBalancer == loadBalancerSimple {
		return simpleLoadManagerClassName
	}
	return extensibleLoadManagerClassName
}

func brokerReplicas(specReplicas *int32) int32 {
	if specReplicas != nil {
		return *specReplicas
	}
	return defaultBrokerReplicas
}

func brokerPDBEnabled(pdb *clusterv1alpha1.PodDisruptionBudgetConfig) bool {
	if pdb == nil || pdb.Enabled == nil {
		return true
	}
	return *pdb.Enabled
}

func brokerMaxUnavailable(pdb *clusterv1alpha1.PodDisruptionBudgetConfig) intstr.IntOrString {
	if pdb != nil && pdb.MaxUnavailable != nil {
		return *pdb.MaxUnavailable
	}
	return intstr.FromInt32(defaultBrokerMaxUnavailable)
}

// brokerShutdownTimeoutSeconds reads brokerShutdownTimeoutMs back out of the
// already-merged config (rather than off BrokerSpec directly) so a
// spec.Config override of it is honored the same way the rendered
// broker.conf itself would apply it.
func brokerShutdownTimeoutSeconds(mergedConfig map[string]string) int64 {
	ms := defaultBrokerShutdownTimeoutMs
	if v, ok := mergedConfig[confKeyBrokerShutdownTimeoutMs]; ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed >= 0 {
			ms = parsed
		}
	}
	return int64(math.Ceil(float64(ms) / 1000.0))
}

// terminationGracePeriodSeconds must exceed brokerShutdownTimeoutSeconds -
// otherwise kubelet SIGKILLs the process before Pulsar's own shutdown hook
// finishes unloading bundles - with headroom for the preStop drain sleep.
func terminationGracePeriodSeconds(mergedConfig map[string]string) int64 {
	return brokerShutdownTimeoutSeconds(mergedConfig) + preStopDrainSeconds
}

func brokerServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: brokerPortName, Port: brokerPort, TargetPort: intstr.FromInt32(brokerPort)},
		{Name: httpPortName, Port: httpPort, TargetPort: intstr.FromInt32(httpPort)},
	}
}

func brokerImage(specImage string) string {
	if specImage != "" {
		return specImage
	}
	return defaultBrokerImage
}

// brokerContainer runs "bin/pulsar broker" directly rather than the
// upstream image's apply-config-from-env.py entrypoint dance: the
// broker.conf ConfigMap is already the fully-rendered, final config
// (operator defaults merged with spec.Config), mounted straight over the
// image's own conf/broker.conf.
func brokerContainer(broker *clusterv1alpha1.Broker) corev1.Container {
	return corev1.Container{
		Name:    brokerComponent,
		Image:   brokerImage(broker.Spec.Image),
		Command: []string{"bin/pulsar", "broker"},
		Ports: []corev1.ContainerPort{
			{Name: brokerPortName, ContainerPort: brokerPort},
			{Name: httpPortName, ContainerPort: httpPort},
		},
		Resources: broker.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{Name: configVolumeName, MountPath: brokerConfMountPath, SubPath: brokerConfFileName, ReadOnly: true},
		},
		ReadinessProbe: brokerProbe(10, 10, 3),
		LivenessProbe:  brokerProbe(30, 30, 5),
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf("sleep %d", preStopDrainSeconds)}},
			},
		},
	}
}

func brokerProbe(initialDelaySeconds, periodSeconds, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: healthCheckPath,
				Port: intstr.FromInt32(httpPort),
			},
		},
		InitialDelaySeconds: initialDelaySeconds,
		PeriodSeconds:       periodSeconds,
		FailureThreshold:    failureThreshold,
	}
}

// brokerPodAnnotations feeds WithConfigChecksum a single, already-rendered
// properties string rather than ranging over the config map: map iteration
// order is randomized, so hashing key/value pairs individually would make
// the checksum - and therefore whether a config change triggers a rolling
// restart - nondeterministic between reconciles.
func brokerPodAnnotations(renderedConfig string) map[string]string {
	return builder.WithConfigChecksum(nil, renderedConfig)
}

func desiredBrokerStatefulSetSpec(broker *clusterv1alpha1.Broker, name string, selectorLabels, podLabels map[string]string, mergedConfig map[string]string, renderedConfig string) appsv1.StatefulSetSpec {
	replicas := brokerReplicas(broker.Spec.Replicas)
	gracePeriod := terminationGracePeriodSeconds(mergedConfig)

	return appsv1.StatefulSetSpec{
		Replicas:    &replicas,
		ServiceName: name,
		Selector:    &metav1.LabelSelector{MatchLabels: selectorLabels},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: brokerPodAnnotations(renderedConfig),
			},
			Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: &gracePeriod,
				Containers:                    []corev1.Container{brokerContainer(broker)},
				Volumes: []corev1.Volume{
					{
						Name: configVolumeName,
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: name},
							},
						},
					},
				},
			},
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *BrokerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.Broker{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("cluster-broker").
		Complete(r)
}
