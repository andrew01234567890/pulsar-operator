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
	autoRecoveryComponent = "autorecovery"

	// autoRecoverySubcommand is the bin/bookkeeper subcommand that runs the
	// Auditor + ReplicationWorker daemon. It happens to equal the component
	// name but is a distinct concept (a CLI arg, not a label value).
	autoRecoverySubcommand = "autorecovery"

	autoRecoveryModeEmbedded  = "embedded"
	autoRecoveryModeDedicated = "dedicated"

	// autoRecoveryDefaultImage is used only when neither AutoRecovery.spec.image
	// nor a cluster-wide default (applied upstream into AutoRecovery.spec.image
	// by the PulsarCluster reconciler) is set.
	autoRecoveryDefaultImage = "apachepulsar/pulsar:5.0.0-M1"

	autoRecoveryDefaultReplicas = 1

	autoRecoveryHTTPPort = 8000

	autoRecoveryConfigFileName  = "bookkeeper.conf"
	autoRecoveryConfigMountPath = "/pulsar/conf/" + autoRecoveryConfigFileName
	autoRecoveryConfigVolume    = "config"

	// autoRecoveryKeyMetadataServiceURI is the bookkeeper.conf key pointing at
	// the metadata store. Left blank in the operator defaults - AutoRecovery
	// never invents a store URL; it is supplied only via spec.config.
	autoRecoveryKeyMetadataServiceURI = "metadataServiceUri"
)

// AutoRecoveryReconciler reconciles an AutoRecovery object.
//
// AutoRecovery has two modes. In "dedicated" mode it runs the BookKeeper
// autorecovery daemon (bin/bookkeeper autorecovery: Auditor +
// ReplicationWorker) as its own Deployment - a Deployment rather than a
// StatefulSet because autorecovery's Auditor leader-election happens through
// the metadata store, not through stable per-pod hostnames, so it needs no
// pod identity. In "embedded" mode (the default) autorecovery runs inside
// bookie pods instead, so this reconciler manages no workload at all and only
// reflects that in status - cleaning up any dedicated Deployment/ConfigMap
// left over from a previous "dedicated" mode.
type AutoRecoveryReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=autorecoveries,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=autorecoveries/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=autorecoveries/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives an AutoRecovery toward its desired state for the
// configured mode and reports readiness on its status.
func (r *AutoRecoveryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	autoRecovery := &clusterv1alpha1.AutoRecovery{}
	if err := r.Get(ctx, req.NamespacedName, autoRecovery); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var condition metav1.Condition
	if autoRecoveryMode(autoRecovery.Spec) == autoRecoveryModeDedicated {
		var err error
		condition, err = r.reconcileDedicated(ctx, autoRecovery)
		if err != nil {
			log.Error(err, "failed to reconcile dedicated AutoRecovery workload")
			return ctrl.Result{}, err
		}
	} else {
		var err error
		condition, err = r.reconcileEmbedded(ctx, autoRecovery)
		if err != nil {
			log.Error(err, "failed to clean up dedicated AutoRecovery workload for embedded mode")
			return ctrl.Result{}, err
		}
	}

	autoRecovery.Status.ObservedGeneration = autoRecovery.Generation
	apimeta.SetStatusCondition(&autoRecovery.Status.Conditions, condition)

	if err := r.Status().Update(ctx, autoRecovery); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating AutoRecovery status: %w", err)
	}

	return ctrl.Result{}, nil
}

// reconcileDedicated creates/updates the dedicated Deployment + ConfigMap and
// returns the Ready condition computed from the Deployment's observed status.
func (r *AutoRecoveryReconciler) reconcileDedicated(ctx context.Context, autoRecovery *clusterv1alpha1.AutoRecovery) (metav1.Condition, error) {
	labels := builder.Labels(autoRecovery.Name, autoRecoveryComponent)
	selector := builder.SelectorLabels(autoRecovery.Name, autoRecoveryComponent)
	renderedConf := config.RenderProperties(autoRecoveryMergedConfig(autoRecovery.Spec))

	if err := r.reconcileConfigMap(ctx, autoRecovery, labels, renderedConf); err != nil {
		return metav1.Condition{}, err
	}

	desiredReplicas := autoRecoveryReplicas(autoRecovery.Spec)
	deploy, err := r.reconcileDeployment(ctx, autoRecovery, labels, selector, desiredReplicas, renderedConf)
	if err != nil {
		return metav1.Condition{}, err
	}

	if err := r.reconcilePDB(ctx, autoRecovery, labels, selector); err != nil {
		return metav1.Condition{}, err
	}

	autoRecovery.Status.Replicas = deploy.Status.Replicas
	autoRecovery.Status.ReadyReplicas = deploy.Status.ReadyReplicas

	return workloadReadyCondition(autoRecovery.Generation, desiredReplicas, deploymentRollout(deploy), autoRecoveryComponent), nil
}

// reconcileEmbedded deletes any dedicated Deployment/ConfigMap left over from
// a previous "dedicated" mode and returns the fixed embedded-mode condition.
func (r *AutoRecoveryReconciler) reconcileEmbedded(ctx context.Context, autoRecovery *clusterv1alpha1.AutoRecovery) (metav1.Condition, error) {
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: autoRecovery.Name, Namespace: autoRecovery.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, deploy); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale dedicated Deployment: %w", err)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: autoRecoveryConfigMapName(autoRecovery), Namespace: autoRecovery.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, cm); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale dedicated ConfigMap: %w", err)
	}

	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: autoRecovery.Name, Namespace: autoRecovery.Namespace}}
	if err := deleteChildIfExists(ctx, r.Client, pdb); err != nil {
		return metav1.Condition{}, fmt.Errorf("deleting stale dedicated PodDisruptionBudget: %w", err)
	}

	autoRecovery.Status.Replicas = 0
	autoRecovery.Status.ReadyReplicas = 0

	return metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "EmbeddedMode",
		ObservedGeneration: autoRecovery.Generation,
		Message:            "autorecovery runs embedded in bookie pods; the operator manages no dedicated workload",
	}, nil
}

func (r *AutoRecoveryReconciler) reconcileConfigMap(ctx context.Context, autoRecovery *clusterv1alpha1.AutoRecovery, labels map[string]string, renderedConf string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: autoRecoveryConfigMapName(autoRecovery), Namespace: autoRecovery.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{autoRecoveryConfigFileName: renderedConf}
		return controllerutil.SetControllerReference(autoRecovery, cm, r.Scheme)
	})
	return err
}

func (r *AutoRecoveryReconciler) reconcileDeployment(
	ctx context.Context,
	autoRecovery *clusterv1alpha1.AutoRecovery,
	labels, selector map[string]string,
	replicas int32,
	renderedConf string,
) (*appsv1.Deployment, error) {
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: autoRecovery.Name, Namespace: autoRecovery.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		deploy.Labels = labels
		deploy.Spec.Replicas = &replicas
		deploy.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
		deploy.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      labels,
				Annotations: builder.WithConfigChecksum(deploy.Spec.Template.Annotations, renderedConf),
			},
			Spec: corev1.PodSpec{
				// Dedicated autorecovery's Auditor performs quorum-sensitive
				// leader election through the metadata store: co-locating
				// two replicas on one node would let a single node loss take
				// out enough of that group to disrupt the election, so
				// anti-affinity here is hard, matching the other
				// stateful/quorum tiers (bookie, oxia-server).
				Affinity:                  builder.PodAntiAffinity(true, selector),
				TopologySpreadConstraints: builder.ZoneTopologySpreadConstraints(selector),
				Containers: []corev1.Container{{
					Name:    autoRecoveryComponent,
					Image:   autoRecoveryImage(autoRecovery.Spec.Image),
					Command: []string{"bin/bookkeeper"},
					Args:    []string{autoRecoverySubcommand},
					Ports: []corev1.ContainerPort{
						{Name: portNameHTTP, ContainerPort: autoRecoveryHTTPPort},
					},
					LivenessProbe:  autoRecoveryProbe(),
					ReadinessProbe: autoRecoveryProbe(),
					VolumeMounts: []corev1.VolumeMount{{
						Name:      autoRecoveryConfigVolume,
						MountPath: autoRecoveryConfigMountPath,
						SubPath:   autoRecoveryConfigFileName,
					}},
				}},
				Volumes: []corev1.Volume{{
					Name: autoRecoveryConfigVolume,
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: autoRecoveryConfigMapName(autoRecovery)},
						},
					},
				}},
			},
		}
		return controllerutil.SetControllerReference(autoRecovery, deploy, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	return deploy, nil
}

// reconcilePDB bounds voluntary disruption of the dedicated autorecovery
// replicas to a flat 1, NOT quorum math: the BookKeeper AutoRecovery Auditor
// is leader-elected through the metadata store (ZooKeeper/Oxia), so exactly
// one replica is the active auditor and the rest are idle standbys - there is
// no peer majority-vote among the replicas to protect. Quorum math would
// yield floor((replicas-1)/2)=0 at the default replica count (1) and at 2,
// and a maxUnavailable=0 PDB denies every voluntary eviction, hanging
// kubectl drain / cluster-autoscaler on any node hosting an auditor pod.
func (r *AutoRecoveryReconciler) reconcilePDB(ctx context.Context, autoRecovery *clusterv1alpha1.AutoRecovery, labels, selector map[string]string) error {
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: autoRecovery.Name, Namespace: autoRecovery.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		desired := builder.PodDisruptionBudget(pdb.Name, autoRecovery.Namespace, labels, selector, intstr.FromInt32(1))
		pdb.Labels = desired.Labels
		pdb.Spec = desired.Spec
		return controllerutil.SetControllerReference(autoRecovery, pdb, r.Scheme)
	})
	return err
}

// deleteChildIfExists deletes obj (identified by the Name/Namespace
// already set on it) if it exists, tolerating both "already gone" on Get and
// a concurrent delete racing this one on Delete.
func deleteChildIfExists(ctx context.Context, c client.Client, obj client.Object) error {
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func autoRecoveryConfigMapName(autoRecovery *clusterv1alpha1.AutoRecovery) string {
	return autoRecovery.Name + "-config"
}

func autoRecoveryMode(spec clusterv1alpha1.AutoRecoverySpec) string {
	if spec.Mode == "" {
		return autoRecoveryModeEmbedded
	}
	return spec.Mode
}

func autoRecoveryImage(specImage string) string {
	if specImage != "" {
		return specImage
	}
	return autoRecoveryDefaultImage
}

func autoRecoveryReplicas(spec clusterv1alpha1.AutoRecoverySpec) int32 {
	if spec.Replicas != nil {
		return *spec.Replicas
	}
	return autoRecoveryDefaultReplicas
}

// autoRecoveryDefaultConfig returns the operator's baseline bookkeeper.conf
// for the autorecovery daemon. metadataServiceUri is left blank: AutoRecovery
// has no notion of which metadata store implementation the cluster uses, so
// it is wired in only via spec.config, never invented here.
func autoRecoveryDefaultConfig() map[string]string {
	return map[string]string{
		autoRecoveryKeyMetadataServiceURI: "",
		"httpServerEnabled":               configValTrue,
		"httpServerPort":                  strconv.Itoa(autoRecoveryHTTPPort),
	}
}

func autoRecoveryMergedConfig(spec clusterv1alpha1.AutoRecoverySpec) map[string]string {
	return config.Merge(autoRecoveryDefaultConfig(), spec.Config)
}

func autoRecoveryProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/heartbeat",
				Port: intstr.FromInt32(autoRecoveryHTTPPort),
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *AutoRecoveryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.AutoRecovery{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("cluster-autorecovery").
		Complete(r)
}
