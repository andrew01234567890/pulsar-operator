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

	brokerPortName = "pulsar"
	httpPortName   = "http"

	// defaultBrokerServicePort / defaultWebServicePort are the fallbacks used
	// only when the merged config carries no (or an invalid) port value.
	// Reconcile always resolves the real ports out of the merged config so
	// the container ports, probe ports, and Service ports can never desync
	// from what broker.conf actually binds; the operator owns these keys.
	defaultBrokerServicePort int32 = 6650
	defaultWebServicePort    int32 = 8080

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

	// transferShedderClassName is the only loadBalancerLoadSheddingStrategy
	// that implements NamespaceUnloadStrategy, the interface
	// ExtensibleLoadManagerImpl requires of its shedder. Pulsar's own
	// broker.conf default, ThresholdShedder, implements the older
	// LoadSheddingStrategy interface instead, so pairing it with
	// ExtensibleLoadManagerImpl makes the broker log "ThresholdShedder does
	// not implement NamespaceUnloadStrategy" and never shed load.
	transferShedderClassName = "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder"

	// broker.conf keys the operator sets or reads back out of the merged config.
	confKeyBrokerServicePort                = "brokerServicePort"
	confKeyWebServicePort                   = "webServicePort"
	confKeyLoadManagerClassName             = "loadManagerClassName"
	confKeyLoadBalancerLoadSheddingStrategy = "loadBalancerLoadSheddingStrategy"
	confKeyLoadBalancerTransferEnabled      = "loadBalancerTransferEnabled"
	confKeyBrokerShutdownTimeoutMs          = "brokerShutdownTimeoutMs"

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

	// functionsWorkerConfigVolumeName is deliberately distinct from
	// configVolumeName ("config", broker.conf's own volume): a colocated
	// FunctionsWorker mounts a SECOND file, functions_worker.yml, alongside
	// broker.conf (see functionsWorkerConfigFileName/functionsWorkerConfigMountPath
	// in functionsworker_controller.go, reused here unchanged since Pulsar's
	// broker loads it from the identical relative conf/ path either way).
	functionsWorkerConfigVolumeName = "functions-worker-config"

	// The Ready condition type and the reason vocabulary
	// (reasonAllReady / reasonProgressing / reasonNoReplicas) are shared
	// across this package's mandatory-tier reconcilers; they are declared
	// once in pulsarcluster_controller.go / bookkeeper_controller.go and
	// reused here so the umbrella rollup sees a consistent status language.

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
	ports := resolveBrokerPorts(mergedConfig)

	if err := r.reconcileConfigMap(ctx, broker, name, labels, renderedConfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker configmap: %w", err)
	}

	functionsWorkerRendered, err := r.reconcileFunctionsWorkerConfigMap(ctx, broker, name, labels)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker functions-worker configmap: %w", err)
	}

	if err := r.reconcileHeadlessService(ctx, broker, name, labels, selectorLabels, ports); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling broker headless service: %w", err)
	}

	stsSpec := desiredBrokerStatefulSetSpec(broker, name, selectorLabels, labels, mergedConfig, renderedConfig, functionsWorkerRendered, ports)
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

// functionsWorkerBrokerConfigMapName names the broker's own
// functions_worker.yml ConfigMap distinctly from its broker.conf ConfigMap
// (both are named after the Broker CR itself, so they cannot share a name).
func functionsWorkerBrokerConfigMapName(brokerName string) string {
	return brokerName + "-functions-worker"
}

// reconcileFunctionsWorkerConfigMap renders and reconciles the
// functions_worker.yml ConfigMap a colocated FunctionsWorker needs mounted
// on this broker (see BrokerSpec.FunctionsWorkerConfig's doc comment for
// why), returning the rendered content so the caller can fold it into the
// pod template's config checksum - otherwise a functions_worker.yml-only
// change (broker.conf unchanged) would never trigger a rolling restart.
// broker.Spec.FunctionsWorkerConfig == nil means no colocated worker on this
// broker; any previously-created ConfigMap is deleted (it holds no user
// data, unlike the package-storage PVC, so cleaning it up is safe) and "" is
// returned.
func (r *BrokerReconciler) reconcileFunctionsWorkerConfigMap(ctx context.Context, broker *clusterv1alpha1.Broker, name string, labels map[string]string) (string, error) {
	cmName := functionsWorkerBrokerConfigMapName(name)

	if broker.Spec.FunctionsWorkerConfig == nil {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: broker.Namespace}}
		if err := r.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
			return "", err
		}
		return "", nil
	}

	rendered := renderFunctionsWorkerYAML(config.Merge(functionsWorkerDefaultConfig(), broker.Spec.FunctionsWorkerConfig))

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: broker.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{functionsWorkerConfigFileName: rendered}
		return builder.SetControllerOwner(broker, cm, r.Scheme)
	})
	if err != nil {
		return "", err
	}
	return rendered, nil
}

func (r *BrokerReconciler) reconcileHeadlessService(ctx context.Context, broker *clusterv1alpha1.Broker, name string, labels, selectorLabels map[string]string, ports brokerPorts) error {
	built := builder.HeadlessService(name, broker.Namespace, labels, selectorLabels, brokerServicePorts(ports))

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
	apimeta.SetStatusCondition(&broker.Status.Conditions, brokerReadyCondition(sts, desired, broker.Generation))

	return r.Status().Update(ctx, broker)
}

// brokerReadyCondition reports Ready only once the StatefulSet's rollout has
// fully converged, not merely once enough pods are Ready. Gating on
// ReadyReplicas alone would flip Ready=True mid rolling-restart (e.g. a
// config-checksum change), while pods still running the previous revision are
// counted Ready. observedGeneration is stamped onto the condition so the
// umbrella PulsarCluster reconciler can detect a stale (pre-update) Broker
// status.
func brokerReadyCondition(sts *appsv1.StatefulSet, desired int32, observedGeneration int64) metav1.Condition {
	cond := metav1.Condition{Type: conditionTypeReady, ObservedGeneration: observedGeneration}

	switch {
	case desired == 0:
		// Broker is a mandatory data-plane tier: scaled to zero it serves no
		// traffic, so it must never read as healthy to the umbrella rollup.
		// (Optional tiers like Proxy/FunctionsWorker intentionally differ -
		// they report Ready when deliberately parked at zero replicas.)
		cond.Status = metav1.ConditionFalse
		cond.Reason = reasonNoReplicas
		cond.Message = "no broker replicas requested"
	case sts.Status.ObservedGeneration != sts.Generation ||
		sts.Status.UpdatedReplicas != desired ||
		sts.Status.ReadyReplicas != desired:
		cond.Status = metav1.ConditionFalse
		cond.Reason = reasonProgressing
		cond.Message = fmt.Sprintf(
			"rollout in progress: %d/%d replicas updated, %d/%d ready (statefulset generation %d observed %d)",
			sts.Status.UpdatedReplicas, desired, sts.Status.ReadyReplicas, desired,
			sts.Generation, sts.Status.ObservedGeneration)
	default:
		cond.Status = metav1.ConditionTrue
		cond.Reason = reasonAllReady
		cond.Message = fmt.Sprintf("all %d broker replicas ready and up to date", desired)
	}

	return cond
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
	cfg := map[string]string{
		confKeyBrokerServicePort:       strconv.Itoa(int(defaultBrokerServicePort)),
		confKeyWebServicePort:          strconv.Itoa(int(defaultWebServicePort)),
		confKeyLoadManagerClassName:    loadManagerClassName(spec.LoadBalancer),
		confKeyBrokerShutdownTimeoutMs: strconv.FormatInt(defaultBrokerShutdownTimeoutMs, 10),
	}
	if cfg[confKeyLoadManagerClassName] == extensibleLoadManagerClassName {
		// ExtensibleLoadManagerImpl needs a NamespaceUnloadStrategy shedder
		// (TransferShedder) plus loadBalancerTransferEnabled to actually
		// perform live bundle transfer during shedding; see
		// transferShedderClassName above for why the upstream default breaks.
		cfg[confKeyLoadBalancerLoadSheddingStrategy] = transferShedderClassName
		cfg[confKeyLoadBalancerTransferEnabled] = configValTrue
	}
	return cfg
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

// brokerPorts holds the ports the broker actually binds, resolved from the
// merged broker.conf so the container ports, probe ports, and Service ports
// all track a spec.config override in lockstep with the rendered config.
type brokerPorts struct {
	binary int32
	http   int32
}

func resolveBrokerPorts(mergedConfig map[string]string) brokerPorts {
	return brokerPorts{
		binary: resolveConfigPort(mergedConfig, confKeyBrokerServicePort, defaultBrokerServicePort),
		http:   resolveConfigPort(mergedConfig, confKeyWebServicePort, defaultWebServicePort),
	}
}

// resolveConfigPort reads a TCP port out of the merged config, falling back to
// def when the key is absent or the value is not a valid port. The operator
// owns these keys (they are always present in the defaults), so the fallback
// only matters if a user override sets a malformed value.
func resolveConfigPort(mergedConfig map[string]string, key string, def int32) int32 {
	if v, ok := mergedConfig[key]; ok {
		if parsed, err := strconv.ParseInt(v, 10, 32); err == nil && parsed > 0 && parsed <= 65535 {
			return int32(parsed)
		}
	}
	return def
}

func brokerServicePorts(ports brokerPorts) []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: brokerPortName, Port: ports.binary, TargetPort: intstr.FromInt32(ports.binary)},
		{Name: httpPortName, Port: ports.http, TargetPort: intstr.FromInt32(ports.http)},
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
func brokerContainer(broker *clusterv1alpha1.Broker, ports brokerPorts) corev1.Container {
	volumeMounts := append([]corev1.VolumeMount{
		{Name: configVolumeName, MountPath: brokerConfMountPath, SubPath: brokerConfFileName, ReadOnly: true},
	}, broker.Spec.VolumeMounts...)
	if broker.Spec.FunctionsWorkerConfig != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      functionsWorkerConfigVolumeName,
			MountPath: functionsWorkerConfigMountPath,
			SubPath:   functionsWorkerConfigFileName,
			ReadOnly:  true,
		})
	}

	return corev1.Container{
		Name:    brokerComponent,
		Image:   brokerImage(broker.Spec.Image),
		Command: []string{"bin/pulsar", "broker"},
		Ports: []corev1.ContainerPort{
			{Name: brokerPortName, ContainerPort: ports.binary},
			{Name: httpPortName, ContainerPort: ports.http},
		},
		Env:            broker.Spec.Env,
		Resources:      broker.Spec.Resources,
		VolumeMounts:   volumeMounts,
		ReadinessProbe: brokerProbe(ports.http, 10, 10, 3),
		LivenessProbe:  brokerProbe(ports.http, 30, 30, 5),
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf("sleep %d", preStopDrainSeconds)}},
			},
		},
	}
}

func brokerProbe(httpPort, initialDelaySeconds, periodSeconds, failureThreshold int32) *corev1.Probe {
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

// brokerPodAnnotations feeds WithConfigChecksum already-rendered content
// strings rather than ranging over the config maps directly: map iteration
// order is randomized, so hashing key/value pairs individually would make
// the checksum - and therefore whether a config change triggers a rolling
// restart - nondeterministic between reconciles. functionsWorkerRendered is
// "" whenever no colocated worker is mounted, which still participates
// safely in the checksum (Checksum length-frames every part, so an empty
// part can never collide with a change in another part) and so still flips
// the checksum - and triggers a rolling restart - the moment a colocated
// FunctionsWorker is added or removed, not just when its content changes.
func brokerPodAnnotations(renderedConfig, functionsWorkerRendered string) map[string]string {
	return builder.WithConfigChecksum(nil, renderedConfig, functionsWorkerRendered)
}

func desiredBrokerStatefulSetSpec(
	broker *clusterv1alpha1.Broker,
	name string,
	selectorLabels, podLabels map[string]string,
	mergedConfig map[string]string,
	renderedConfig, functionsWorkerRendered string,
	ports brokerPorts,
) appsv1.StatefulSetSpec {
	replicas := brokerReplicas(broker.Spec.Replicas)
	gracePeriod := terminationGracePeriodSeconds(mergedConfig)

	volumes := append([]corev1.Volume{
		{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
				},
			},
		},
	}, broker.Spec.Volumes...)
	if broker.Spec.FunctionsWorkerConfig != nil {
		volumes = append(volumes, corev1.Volume{
			Name: functionsWorkerConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: functionsWorkerBrokerConfigMapName(name)},
				},
			},
		})
	}

	return appsv1.StatefulSetSpec{
		Replicas:    &replicas,
		ServiceName: name,
		Selector:    &metav1.LabelSelector{MatchLabels: selectorLabels},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: brokerPodAnnotations(renderedConfig, functionsWorkerRendered),
			},
			Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: &gracePeriod,
				Affinity:                      brokerAffinity(broker.Spec.Antiaffinity, selectorLabels),
				TopologySpreadConstraints:     builder.ZoneTopologySpreadConstraints(selectorLabels),
				Containers:                    []corev1.Container{brokerContainer(broker, ports)},
				Volumes:                       volumes,
			},
		},
	}
}

// brokerAffinity returns the broker's pod anti-affinity, honoring
// BrokerSpec.Antiaffinity when set. Brokers are stateless (losing a broker
// only migrates its bundles, not data), so the operator default is soft
// (host=false); a caller can opt into hard node anti-affinity via
// antiaffinity.host, or turn anti-affinity off entirely via
// antiaffinity.enabled=false.
func brokerAffinity(spec *clusterv1alpha1.AntiAffinityConfig, selectorLabels map[string]string) *corev1.Affinity {
	if spec != nil && spec.Enabled != nil && !*spec.Enabled {
		return nil
	}
	hard := spec != nil && spec.Host != nil && *spec.Host
	return builder.PodAntiAffinity(hard, selectorLabels)
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
