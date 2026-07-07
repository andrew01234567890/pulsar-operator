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
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

const (
	functionsWorkerComponent = "functionsworker"

	// functionsWorkerNoun is the human-readable component name used in status
	// messages, distinct from the label-safe functionsWorkerComponent.
	functionsWorkerNoun = "functions worker"

	functionsWorkerModeColocated  = "colocated"
	functionsWorkerModeStandalone = "standalone"

	// functionsWorkerClusterPlaceholder is functionsWorkerDefaultConfig's
	// cluster-less fallback for pulsarFunctionsCluster - it happens to share
	// its string value with functionsWorkerModeStandalone (matching upstream
	// Pulsar's own conf/functions_worker.yml, whose shipped default is
	// literally "pulsarFunctionsCluster: standalone"), but names an
	// unrelated concept (a placeholder cluster name, not a reconcile mode),
	// so it is kept as its own constant rather than reusing that one.
	functionsWorkerClusterPlaceholder = "standalone"

	packageStorageFileSystem = "FileSystemPackagesStorage"

	fileSystemPackagesStorageProviderClass = "org.apache.pulsar.packages.management.storage.filesystem.FileSystemPackagesStorageProvider"

	// functionsWorkerDefaultImage is used only when neither
	// FunctionsWorker.spec.image nor a cluster-wide default (applied upstream
	// into FunctionsWorker.spec.image by the PulsarCluster reconciler) is set.
	functionsWorkerDefaultImage = "apachepulsar/pulsar:5.0.0-M1"

	functionsWorkerDefaultReplicas = 1

	functionsWorkerPort = 6750

	functionsWorkerConfigFileName  = "functions_worker.yml"
	functionsWorkerConfigMountPath = "/pulsar/conf/" + functionsWorkerConfigFileName
	functionsWorkerConfigVolume    = "config"

	// functions_worker.yml keys referenced from more than one place (config
	// defaults + tests), named to keep them unambiguous and de-duplicated.
	cfgKeyPulsarServiceURL       = "pulsarServiceUrl"
	cfgKeyPulsarWebServiceURL    = "pulsarWebServiceUrl"
	cfgKeyWorkerPort             = "workerPort"
	cfgKeyPulsarFunctionsCluster = "pulsarFunctionsCluster"
)

// FunctionsWorkerReconciler reconciles a FunctionsWorker object.
//
// FunctionsWorker has two modes. In "standalone" mode it runs
// bin/pulsar functions-worker as its own StatefulSet, fronted by a headless
// Service, rendering functions_worker.yml from operator defaults layered
// with FunctionsWorker.spec.config into a ConfigMap. In "colocated" mode (the
// default) the functions worker runs embedded inside broker pods instead, so
// this reconciler manages no workload at all and only reflects that in
// status - cleaning up any standalone-mode resources left over from a
// previous "standalone" mode.
type FunctionsWorkerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=functionsworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=functionsworkers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=functionsworkers/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=brokers,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a FunctionsWorker toward its desired state for the
// configured mode and reports readiness on its status.
func (r *FunctionsWorkerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	fw := &clusterv1alpha1.FunctionsWorker{}
	if err := r.Get(ctx, req.NamespacedName, fw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var condition metav1.Condition
	if functionsWorkerMode(fw.Spec) == functionsWorkerModeStandalone {
		var err error
		condition, err = r.reconcileStandalone(ctx, fw)
		if err != nil {
			log.Error(err, "failed to reconcile standalone FunctionsWorker workload")
			return ctrl.Result{}, err
		}
	} else {
		var err error
		condition, err = r.reconcileColocated(ctx, fw)
		if err != nil {
			log.Error(err, "failed to clean up standalone FunctionsWorker workload for colocated mode")
			return ctrl.Result{}, err
		}
	}

	fw.Status.ObservedGeneration = fw.Generation
	apimeta.SetStatusCondition(&fw.Status.Conditions, condition)

	if err := r.Status().Update(ctx, fw); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating FunctionsWorker status: %w", err)
	}

	return ctrl.Result{}, nil
}

// reconcileStandalone creates/updates the standalone StatefulSet, Service,
// ConfigMap and PodDisruptionBudget, returning the Ready condition computed
// from the StatefulSet's observed status.
func (r *FunctionsWorkerReconciler) reconcileStandalone(ctx context.Context, fw *clusterv1alpha1.FunctionsWorker) (metav1.Condition, error) {
	labels := builder.Labels(fw.Name, functionsWorkerComponent)
	selector := builder.SelectorLabels(fw.Name, functionsWorkerComponent)
	renderedConf := renderFunctionsWorkerYAML(functionsWorkerMergedConfig(fw.Spec))

	if err := r.reconcileConfigMap(ctx, fw, labels, renderedConf); err != nil {
		return metav1.Condition{}, err
	}

	if err := r.reconcileHeadlessService(ctx, fw, labels, selector); err != nil {
		return metav1.Condition{}, err
	}

	desiredReplicas := functionsWorkerReplicas(fw.Spec)
	sts, err := r.reconcileStatefulSet(ctx, fw, labels, selector, desiredReplicas, renderedConf)
	if err != nil {
		return metav1.Condition{}, err
	}

	if err := r.reconcilePDB(ctx, fw, labels, selector); err != nil {
		return metav1.Condition{}, err
	}

	fw.Status.Replicas = sts.Status.Replicas
	fw.Status.ReadyReplicas = sts.Status.ReadyReplicas

	return workloadReadyCondition(fw.Generation, desiredReplicas, statefulSetRollout(sts), functionsWorkerNoun), nil
}

// reconcileColocated deletes any standalone-mode resources left over from a
// previous "standalone" mode and reports readiness truthfully: since the
// embedded worker runs inside broker pods (see the umbrella PulsarCluster
// reconciler's wireFunctionsWorkerColocated), colocated FunctionsWorker is
// only actually Ready when the sibling Broker is - there is no dedicated
// workload here to observe directly, so an unconditional Ready=True would
// lie whenever the broker itself is down or still rolling out.
func (r *FunctionsWorkerReconciler) reconcileColocated(ctx context.Context, fw *clusterv1alpha1.FunctionsWorker) (metav1.Condition, error) {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: fw.Name, Namespace: fw.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, sts); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale standalone StatefulSet: %w", err)
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: fw.Name, Namespace: fw.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, svc); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale standalone Service: %w", err)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: functionsWorkerConfigMapName(fw), Namespace: fw.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, cm); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale standalone ConfigMap: %w", err)
	}

	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: fw.Name + "-pdb", Namespace: fw.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, pdb); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale standalone PodDisruptionBudget: %w", err)
	}

	fw.Status.Replicas = 0
	fw.Status.ReadyReplicas = 0

	return r.colocatedReadyCondition(ctx, fw)
}

// colocatedReadyCondition mirrors the sibling Broker's own Ready condition
// (the child the umbrella PulsarCluster reconciler names
// "<cluster>-broker", found via fw's controller owner reference - the same
// owner reference every umbrella-managed child carries, see
// applyOrderedChild/writeOrderedChild). It deliberately reports Unknown/False
// rather than True whenever that broker can't be resolved or hasn't reported
// readiness itself, since claiming Ready with no actual signal is exactly
// the false-positive bug this replaces.
func (r *FunctionsWorkerReconciler) colocatedReadyCondition(ctx context.Context, fw *clusterv1alpha1.FunctionsWorker) (metav1.Condition, error) {
	owner := metav1.GetControllerOf(fw)
	if owner == nil || owner.Kind != pulsarClusterOwnerKind {
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionUnknown,
			Reason:             "BrokerOwnerUnknown",
			ObservedGeneration: fw.Generation,
			Message:            "functions worker runs colocated inside broker pods, but this FunctionsWorker has no owning PulsarCluster to look up the broker's readiness from",
		}, nil
	}

	broker := &clusterv1alpha1.Broker{}
	brokerKey := client.ObjectKey{Name: childName(owner.Name, brokerComponent), Namespace: fw.Namespace}
	if err := r.Get(ctx, brokerKey, broker); err != nil {
		if apierrors.IsNotFound(err) {
			return metav1.Condition{
				Type:               conditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             "BrokerMissing",
				ObservedGeneration: fw.Generation,
				Message:            fmt.Sprintf("functions worker runs colocated inside broker pods, but Broker %q does not exist yet", brokerKey.Name),
			}, nil
		}
		return metav1.Condition{}, fmt.Errorf("getting sibling Broker %q for colocated functions worker readiness: %w", brokerKey.Name, err)
	}

	brokerReady := apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeReady)
	if brokerReady == nil {
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "BrokerStatusMissing",
			ObservedGeneration: fw.Generation,
			Message:            fmt.Sprintf("broker %q has not reported a Ready condition yet", brokerKey.Name),
		}, nil
	}

	return metav1.Condition{
		Type:               conditionTypeReady,
		Status:             brokerReady.Status,
		Reason:             "Broker" + brokerReady.Reason,
		ObservedGeneration: fw.Generation,
		Message:            fmt.Sprintf("functions worker runs colocated inside broker pods: %s", brokerReady.Message),
	}, nil
}

func (r *FunctionsWorkerReconciler) reconcileConfigMap(ctx context.Context, fw *clusterv1alpha1.FunctionsWorker, labels map[string]string, renderedConf string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: functionsWorkerConfigMapName(fw), Namespace: fw.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{functionsWorkerConfigFileName: renderedConf}
		return controllerutil.SetControllerReference(fw, cm, r.Scheme)
	})
	return err
}

func (r *FunctionsWorkerReconciler) reconcileHeadlessService(ctx context.Context, fw *clusterv1alpha1.FunctionsWorker, labels, selector map[string]string) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: fw.Name, Namespace: fw.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		desired := builder.HeadlessService(fw.Name, fw.Namespace, labels, selector, functionsWorkerServicePorts())
		svc.Labels = desired.Labels
		svc.Spec.ClusterIP = desired.Spec.ClusterIP
		svc.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return controllerutil.SetControllerReference(fw, svc, r.Scheme)
	})
	return err
}

func (r *FunctionsWorkerReconciler) reconcileStatefulSet(
	ctx context.Context,
	fw *clusterv1alpha1.FunctionsWorker,
	labels, selector map[string]string,
	replicas int32,
	renderedConf string,
) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: fw.Name, Namespace: fw.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = labels
		sts.Spec.Replicas = &replicas
		sts.Spec.ServiceName = fw.Name
		sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
		sts.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      labels,
				Annotations: builder.WithConfigChecksum(sts.Spec.Template.Annotations, renderedConf),
			},
			Spec: corev1.PodSpec{
				// Standalone functions-worker is stateless (function state
				// lives in BookKeeper/metadata, not on the worker pod), so
				// anti-affinity here is soft, not hard.
				Affinity:                  builder.PodAntiAffinity(false, selector),
				TopologySpreadConstraints: builder.ZoneTopologySpreadConstraints(selector),
				Containers: []corev1.Container{{
					Name:           functionsWorkerComponent,
					Image:          functionsWorkerImage(fw.Spec.Image),
					Command:        []string{cmdBinPulsar},
					Args:           []string{"functions-worker"},
					Ports:          []corev1.ContainerPort{{Name: portNameHTTP, ContainerPort: functionsWorkerPort}},
					LivenessProbe:  functionsWorkerProbe(),
					ReadinessProbe: functionsWorkerProbe(),
					VolumeMounts: []corev1.VolumeMount{{
						Name:      functionsWorkerConfigVolume,
						MountPath: functionsWorkerConfigMountPath,
						SubPath:   functionsWorkerConfigFileName,
					}},
				}},
				Volumes: []corev1.Volume{{
					Name: functionsWorkerConfigVolume,
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: functionsWorkerConfigMapName(fw)},
						},
					},
				}},
			},
		}
		return controllerutil.SetControllerReference(fw, sts, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	return sts, nil
}

func (r *FunctionsWorkerReconciler) reconcilePDB(ctx context.Context, fw *clusterv1alpha1.FunctionsWorker, labels, selector map[string]string) error {
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: fw.Name + "-pdb", Namespace: fw.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		desired := builder.PodDisruptionBudget(pdb.Name, fw.Namespace, labels, selector, intstr.FromInt32(1))
		pdb.Labels = desired.Labels
		pdb.Spec = desired.Spec
		return controllerutil.SetControllerReference(fw, pdb, r.Scheme)
	})
	return err
}

func functionsWorkerConfigMapName(fw *clusterv1alpha1.FunctionsWorker) string {
	return fw.Name + "-config"
}

func functionsWorkerMode(spec clusterv1alpha1.FunctionsWorkerSpec) string {
	if spec.Mode == "" {
		return functionsWorkerModeColocated
	}
	return spec.Mode
}

func functionsWorkerImage(specImage string) string {
	if specImage != "" {
		return specImage
	}
	return functionsWorkerDefaultImage
}

func functionsWorkerReplicas(spec clusterv1alpha1.FunctionsWorkerSpec) int32 {
	if spec.Replicas != nil {
		return *spec.Replicas
	}
	return functionsWorkerDefaultReplicas
}

// functionsWorkerDefaultConfig returns the operator's baseline
// functions_worker.yml. Broker connection settings (pulsarServiceUrl,
// pulsarWebServiceUrl, configurationMetadataStoreUrl) are left blank:
// FunctionsWorker has no notion of the cluster's broker or metadata store
// naming, so they are wired in only via spec.config, never invented here.
//
// pulsarFunctionsNamespace/pulsarFunctionsCluster default to the exact
// values upstream Pulsar's own conf/functions_worker.yml ships (see
// /tmp/pulsar-refs/pulsar/conf/functions_worker.yml): "public/functions" has
// no cluster-name dependency, so it is always a safe default; "standalone"
// is a placeholder only relevant when this config is rendered with no
// cluster context (a FunctionsWorker CR applied directly, without an owning
// PulsarCluster) - the umbrella PulsarCluster reconciler overrides
// pulsarFunctionsCluster with the real cluster name via
// withFunctionsWorkerClusterDefault, and colocated mode's own broker
// startup independently overrides it again from the broker's clusterName
// regardless (PulsarService.initializeWorkerConfigFromBrokerConfig), so this
// fallback is only ever visibly used by a truly standalone, cluster-less
// rendering.
//
// schedulerClassName/functionRuntimeFactoryClassName are NOT optional in
// practice despite having no compiled-in Java default (WorkerConfig's fields
// are plain nulls until set): SchedulerManager's constructor and the runtime
// factory's own instantiation both do `Class.forName(name)` against these
// unconditionally during PulsarService.startWorkerService, so a functions
// worker started with either key genuinely absent throws a
// NullPointerException and crashes the whole broker process at startup
// (verified against a real cluster, not just the upstream default file).
// ProcessRuntimeFactory (Pulsar's own upstream default, see the sample file)
// is used here rather than KubernetesRuntimeFactory (what pulsar-helm-chart
// configures for its broker-embedded worker): the Kubernetes runtime needs
// its own broker-granted RBAC to create Pods plus function-instance image
// config, which is a larger, separate follow-up; a user who wants it can
// still set functionRuntimeFactoryClassName via spec.config.
//
// failureCheckFreqMs/instanceLivenessCheckFreqMs/initialBrokerReconnectMaxRetries/
// assignmentWriteMaxRetries share the same no-Java-default gap: WorkerConfig's
// primitive long/int fields are 0 when the key is absent, and
// PulsarWorkerService.start unconditionally schedules its membership-monitor
// task at workerConfig.getFailureCheckFreqMs() via
// ScheduledExecutorService.scheduleAtFixedRate, which requires a period > 0
// - a rendered functions_worker.yml missing failureCheckFreqMs throws
// IllegalArgumentException and crashes the broker at startup, exactly like
// the ClassName keys above (also verified against a real cluster). The
// values here match upstream's own shipped conf/functions_worker.yml
// defaults verbatim.
//
// functionMetadataTopicName/clusterCoordinationTopicName/
// functionAssignmentTopicName have the exact same no-Java-default gap, with
// an especially nasty failure mode: WorkerConfig derives each internal
// topic name as `persistent://<namespace>/<field>` (see e.g.
// getClusterCoordinationTopicName), and String.format renders a null field
// as the literal string "null" rather than failing fast - so with all three
// keys absent, LeaderService's leader-election producer, SchedulerManager's
// producer, and the function-assignment topic all collide on the SAME
// literal topic "persistent://public/functions/null", fencing each other
// out with ProducerFencedException and leaving the worker permanently
// stuck reporting "Leader not yet ready" to every admin call (verified
// against a real cluster: `pulsar-admin functions create` never succeeds
// without these three keys set).
func functionsWorkerDefaultConfig() map[string]string {
	return map[string]string{
		configKeyConfigurationMetadataStoreURL: "",
		cfgKeyPulsarServiceURL:                 "",
		cfgKeyPulsarWebServiceURL:              "",
		cfgKeyWorkerPort:                       strconv.Itoa(functionsWorkerPort),
		"numFunctionPackageReplicas":           "1",
		"downloadDirectory":                    "download/pulsar_functions",
		"connectorsDirectory":                  "./connectors",
		"functionsDirectory":                   "./functions",
		"pulsarFunctionsNamespace":             "public/functions",
		cfgKeyPulsarFunctionsCluster:           functionsWorkerClusterPlaceholder,
		"schedulerClassName":                   "org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler",
		"functionRuntimeFactoryClassName":      "org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory",
		"failureCheckFreqMs":                   "30000",
		"instanceLivenessCheckFreqMs":          "30000",
		"initialBrokerReconnectMaxRetries":     "60",
		"assignmentWriteMaxRetries":            "60",
		"functionMetadataTopicName":            "metadata",
		"clusterCoordinationTopicName":         "coordinate",
		"functionAssignmentTopicName":          "assignments",
	}
}

// functionsWorkerYAMLNestedDefaults is appended, not merged, after every
// rendered functions_worker.yml: it is a nested YAML mapping,
// config.RenderYAML's flat map[string]string can't represent it, so it can
// never be a key in functionsWorkerDefaultConfig/spec.config.
// functionRuntimeFactoryConfigs specifically must be present as at least an
// empty mapping - every runtime factory (ProcessRuntimeFactory,
// ThreadRuntimeFactory, KubernetesRuntimeFactory) calls RuntimeUtils.
// getRuntimeFunctionConfig(workerConfig.getFunctionRuntimeFactoryConfigs(),
// ...), and Jackson's ObjectMapper.convertValue(null, X.class) returns null
// rather than an empty X, so an entirely absent key throws a
// NullPointerException on the very first field the runtime factory reads
// off it. Verified against a real cluster (a rendered functions_worker.yml
// missing this key crashes the broker at startup), not just Pulsar source.
const (
	functionsWorkerConfigsKey         = "functionRuntimeFactoryConfigs"
	functionsWorkerYAMLNestedDefaults = functionsWorkerConfigsKey + ": {}\n"
)

// renderFunctionsWorkerYAML is the one place functions_worker.yml is
// rendered (colocated mode's broker-mounted file and standalone mode's own
// ConfigMap both call this), so functionsWorkerYAMLNestedDefaults is never
// forgotten on either path. The nested default is only appended when the
// user hasn't set functionRuntimeFactoryConfigs themselves via spec.config:
// appending unconditionally would emit the key twice (RenderYAML already
// emitted the user's scalar value), which last-write-wins would silently
// drop - or a strict YAML parser would reject as a duplicate key. A user
// value is therefore honored as-is rather than clobbered.
func renderFunctionsWorkerYAML(cfg map[string]string) string {
	rendered := config.RenderYAML(cfg)
	if _, ok := cfg[functionsWorkerConfigsKey]; !ok {
		rendered += functionsWorkerYAMLNestedDefaults
	}
	return rendered
}

// functionsWorkerPackageStorageConfig translates spec.packageStorage into
// functions_worker.yml keys. FileSystemPackagesStorage is the only backend
// with a provider class built into core Pulsar (pulsar-package-management);
// it needs no ZooKeeper, unlike the default BookKeeperPackagesStorage, which
// is why it is the default for an Oxia-only cluster. S3/GCS package storage
// have no built-in core Pulsar provider class to default to, so for those the
// operator only turns package management on and leaves
// packagesManagementStorageProvider to the user's spec.config.
func functionsWorkerPackageStorageConfig(packageStorage string) map[string]string {
	cfg := map[string]string{"functionsWorkerEnablePackageManagement": configValTrue}
	if packageStorage == "" || packageStorage == packageStorageFileSystem {
		cfg["packagesManagementStorageProvider"] = fileSystemPackagesStorageProviderClass
	}
	return cfg
}

// functionsWorkerMergedConfig layers, lowest to highest precedence: operator
// defaults, package-storage defaults, then the user's spec.config, so a user
// override always wins.
func functionsWorkerMergedConfig(spec clusterv1alpha1.FunctionsWorkerSpec) map[string]string {
	merged := config.Merge(functionsWorkerDefaultConfig(), functionsWorkerPackageStorageConfig(spec.PackageStorage))
	return config.Merge(merged, spec.Config)
}

func functionsWorkerServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: portNameHTTP, Port: functionsWorkerPort, TargetPort: intstr.FromInt32(functionsWorkerPort)},
	}
}

// functionsWorkerProbe uses a plain TCP check rather than an HTTP path: the
// worker's REST API can require authentication, so there is no endpoint
// guaranteed to return a plain 200 in every configuration.
func functionsWorkerProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(functionsWorkerPort),
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *FunctionsWorkerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.FunctionsWorker{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&clusterv1alpha1.Broker{}, handler.EnqueueRequestsFromMapFunc(enqueueFunctionsWorkerForBroker)).
		Named("cluster-functionsworker").
		Complete(r)
}

// enqueueFunctionsWorkerForBroker maps a Broker change to a reconcile
// request for its sibling colocated FunctionsWorker (the child the umbrella
// PulsarCluster reconciler names "<cluster>-functionsworker", same owning
// PulsarCluster as the Broker). Without this watch, colocatedReadyCondition's
// mirrored value only ever refreshes on some UNRELATED trigger for the
// FunctionsWorker itself (a spec edit, or the controller's default resync
// period) - so a broker becoming Ready or regressing could sit unreflected
// on FunctionsWorker.status for a long time, which is its own flavor of the
// false-readiness bug this whole fix targets. FunctionsWorker isn't owned by
// Broker (they're siblings under the same PulsarCluster), so Owns() doesn't
// apply here; a Broker with no owning PulsarCluster - or none matching the
// deterministic child-naming convention - maps to nothing.
func enqueueFunctionsWorkerForBroker(_ context.Context, obj client.Object) []reconcile.Request {
	broker, ok := obj.(*clusterv1alpha1.Broker)
	if !ok {
		return nil
	}
	owner := metav1.GetControllerOf(broker)
	if owner == nil || owner.Kind != pulsarClusterOwnerKind {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Name:      childName(owner.Name, functionsWorkerComponent),
			Namespace: broker.Namespace,
		},
	}}
}
