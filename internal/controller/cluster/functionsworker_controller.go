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
	functionsWorkerComponent = "functionsworker"

	// functionsWorkerNoun is the human-readable component name used in status
	// messages, distinct from the label-safe functionsWorkerComponent.
	functionsWorkerNoun = "functions worker"

	functionsWorkerModeColocated  = "colocated"
	functionsWorkerModeStandalone = "standalone"

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
	renderedConf := config.RenderYAML(functionsWorkerMergedConfig(fw.Spec))

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
// previous "standalone" mode and returns the fixed colocated-mode condition.
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

	return metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "ColocatedMode",
		ObservedGeneration: fw.Generation,
		Message:            "functions worker runs colocated inside broker pods; the operator manages no dedicated workload",
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
func functionsWorkerDefaultConfig() map[string]string {
	return map[string]string{
		configKeyConfigurationMetadataStoreURL: "",
		"pulsarServiceUrl":                     "",
		"pulsarWebServiceUrl":                  "",
		"workerPort":                           strconv.Itoa(functionsWorkerPort),
		"numFunctionPackageReplicas":           "1",
		"downloadDirectory":                    "download/pulsar_functions",
		"connectorsDirectory":                  "./connectors",
		"functionsDirectory":                   "./functions",
	}
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
		Named("cluster-functionsworker").
		Complete(r)
}
