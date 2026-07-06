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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	bookkeeperComponent = "bookkeeper"
	bookieContainerName = "bookie"

	// defaultBookKeeperReplicas matches the HA-by-default replica count
	// documented in docs/docs/high-availability.md: an ensemble of 4 tolerates
	// losing a whole AZ under the operator-recommended 3/3/2 ensemble/write/ack
	// quorum without blocking writes.
	defaultBookKeeperReplicas int32 = 4

	// defaultBookieImage is used only when neither BookKeeper.spec.image nor a
	// propagated PulsarCluster.spec.image is set; it tracks
	// PulsarClusterSpec.PulsarVersion's default so a standalone BookKeeper (no
	// owning PulsarCluster) still gets a working image out of the box.
	defaultBookieImage = "apachepulsar/pulsar:5.0.0-M1"

	// defaultWriteQuorum mirrors BookKeeperEnsembleSpec.WriteQuorum's
	// kubebuilder default, used when computing the PDB's quorum-derived
	// maxUnavailable for a BookKeeper built directly (bypassing CRD defaulting).
	defaultWriteQuorum int32 = 2

	bookiePort      = 3181
	bookieAdminPort = 8000

	keyJournalDirectories = "journalDirectories"
	keyLedgerDirectories  = "ledgerDirectories"
	keyIndexDirectories   = "indexDirectories"

	defaultJournalDir = "/pulsar/data/bookkeeper/journal"
	defaultLedgerDir  = "/pulsar/data/bookkeeper/ledgers"
	defaultIndexDir   = "/pulsar/data/bookkeeper/index"

	volumeNameConfig  = "config"
	volumeNameJournal = "journal"
	volumeNameLedgers = "ledgers"
	volumeNameIndex   = "index"

	configMapKey        = "bookkeeper.conf"
	bookieConfMountPath = "/pulsar/conf/bookkeeper.conf"
	bookieStatePath     = "/api/v1/bookie/state"

	conditionTypeReady = "Ready"
	reasonAllReady     = "AllReplicasReady"
	reasonProgressing  = "ReplicasNotReady"
	reasonNoReplicas   = "NoReplicasDesired"
)

// defaultJournalSize, defaultLedgerSize, and defaultIndexSize mirror
// pulsar-helm-chart's bookkeeper volume defaults (journal/index share a
// smaller, low-latency-oriented size; ledgers get the bulk of the capacity).
var (
	defaultJournalSize = resource.MustParse("10Gi")
	defaultLedgerSize  = resource.MustParse("50Gi")
	defaultIndexSize   = resource.MustParse("10Gi")
)

// BookKeeperReconciler reconciles a BookKeeper object
type BookKeeperReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=bookkeepers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a BookKeeper's bookie StatefulSet, headless Service,
// rendered ConfigMap, and PodDisruptionBudget toward the object's desired
// state, then republishes observed replica counts and a Ready condition onto
// BookKeeper.status.
func (r *BookKeeperReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	bk := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, req.NamespacedName, bk); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	merged, rendered := mergeConfig(bk.Spec)
	image := resolveImage(bk.Spec)

	if err := r.reconcileConfigMap(ctx, bk, rendered); err != nil {
		log.Error(err, "Failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, bk); err != nil {
		log.Error(err, "Failed to reconcile headless Service")
		return ctrl.Result{}, err
	}

	if err := r.reconcilePodDisruptionBudget(ctx, bk); err != nil {
		log.Error(err, "Failed to reconcile PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	sts, err := r.reconcileStatefulSet(ctx, bk, image, merged, rendered)
	if err != nil {
		log.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, req.NamespacedName, sts); err != nil {
		log.Error(err, "Failed to update BookKeeper status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BookKeeperReconciler) reconcileConfigMap(ctx context.Context, bk *clusterv1alpha1.BookKeeper, rendered string) error {
	wanted := desiredConfigMap(bk, rendered)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: bk.Name, Namespace: bk.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = wanted.Labels
		cm.Data = wanted.Data
		return builder.SetControllerOwner(bk, cm, r.Scheme)
	})
	return err
}

func (r *BookKeeperReconciler) reconcileHeadlessService(ctx context.Context, bk *clusterv1alpha1.BookKeeper) error {
	wanted := desiredHeadlessService(bk)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: bk.Name, Namespace: bk.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = wanted.Labels
		svc.Spec.Selector = wanted.Spec.Selector
		svc.Spec.Ports = wanted.Spec.Ports
		svc.Spec.ClusterIP = wanted.Spec.ClusterIP
		svc.Spec.PublishNotReadyAddresses = wanted.Spec.PublishNotReadyAddresses
		return builder.SetControllerOwner(bk, svc, r.Scheme)
	})
	return err
}

func (r *BookKeeperReconciler) reconcilePodDisruptionBudget(ctx context.Context, bk *clusterv1alpha1.BookKeeper) error {
	wanted := desiredPDB(bk)
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: bk.Name, Namespace: bk.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = wanted.Labels
		pdb.Spec = wanted.Spec
		return builder.SetControllerOwner(bk, pdb, r.Scheme)
	})
	return err
}

// reconcileStatefulSet creates the bookie StatefulSet on first reconcile and,
// on subsequent reconciles, only ever updates Replicas and Template: selector,
// serviceName, podManagementPolicy, and volumeClaimTemplates are immutable on
// an existing StatefulSet (podManagementPolicy is documented as such on
// BookKeeperSpec pending a defaulting/validation webhook), so touching them
// here would make every reconcile after the first fail against a real API
// server.
func (r *BookKeeperReconciler) reconcileStatefulSet(ctx context.Context, bk *clusterv1alpha1.BookKeeper, image string, merged map[string]string, rendered string) (*appsv1.StatefulSet, error) {
	desired := desiredStatefulSet(bk, image, merged, rendered)

	existing := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := builder.SetControllerOwner(bk, desired, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	case err != nil:
		return nil, err
	default:
		existing.Labels = desired.Labels
		existing.Spec.Replicas = desired.Spec.Replicas
		existing.Spec.Template = desired.Spec.Template
		if err := builder.SetControllerOwner(bk, existing, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Update(ctx, existing); err != nil {
			return nil, err
		}
		return existing, nil
	}
}

// updateStatus re-fetches BookKeeper immediately before the status write
// (rather than reusing the copy Reconcile started with) to minimize the
// window for a resourceVersion conflict after the several intervening writes
// to child objects.
func (r *BookKeeperReconciler) updateStatus(ctx context.Context, key types.NamespacedName, sts *appsv1.StatefulSet) error {
	latest := &clusterv1alpha1.BookKeeper{}
	if err := r.Get(ctx, key, latest); err != nil {
		return client.IgnoreNotFound(err)
	}

	desiredReplicas := resolveReplicas(latest.Spec)
	latest.Status.Replicas = sts.Status.Replicas
	latest.Status.ReadyReplicas = sts.Status.ReadyReplicas
	latest.Status.ObservedGeneration = latest.Generation
	apimeta.SetStatusCondition(&latest.Status.Conditions, computeReadyCondition(latest.Generation, desiredReplicas, sts.Status.ReadyReplicas))

	return r.Status().Update(ctx, latest)
}

// mergeConfig layers BookKeeper.spec.config over the operator's bookie
// defaults and renders the result as bookkeeper.conf content. metadataServiceUri
// is deliberately absent from the defaults: it must come entirely from
// spec.config so this reconciler never bakes in a particular metadata store
// (Oxia, ZooKeeper, ...).
func mergeConfig(spec clusterv1alpha1.BookKeeperSpec) (merged map[string]string, rendered string) {
	merged = config.Merge(defaultBookKeeperConfig(), spec.Config)
	rendered = config.RenderProperties(merged)
	return merged, rendered
}

func defaultBookKeeperConfig() map[string]string {
	return map[string]string{
		"bookiePort": strconv.Itoa(bookiePort),
		// Pulsar ships httpServerEnabled=false; the operator turns it on
		// because the readiness/liveness probes below depend on the bookie
		// admin API.
		"httpServerEnabled":   "true",
		"httpServerPort":      strconv.Itoa(bookieAdminPort),
		keyJournalDirectories: defaultJournalDir,
		keyLedgerDirectories:  defaultLedgerDir,
		keyIndexDirectories:   defaultIndexDir,
	}
}

func resolveReplicas(spec clusterv1alpha1.BookKeeperSpec) int32 {
	if spec.Replicas != nil {
		return *spec.Replicas
	}
	return defaultBookKeeperReplicas
}

func resolveImage(spec clusterv1alpha1.BookKeeperSpec) string {
	if spec.Image != "" {
		return spec.Image
	}
	return defaultBookieImage
}

func resolvePodManagementPolicy(spec clusterv1alpha1.BookKeeperSpec) appsv1.PodManagementPolicyType {
	if spec.PodManagementPolicy != "" {
		return appsv1.PodManagementPolicyType(spec.PodManagementPolicy)
	}
	return appsv1.ParallelPodManagement
}

func resolveWriteQuorum(spec clusterv1alpha1.BookKeeperSpec) int32 {
	if spec.Ensemble != nil && spec.Ensemble.WriteQuorum != nil {
		return *spec.Ensemble.WriteQuorum
	}
	return defaultWriteQuorum
}

// resolvePDBMaxUnavailable keeps at least writeQuorum bookies available
// through a voluntary disruption, so in-flight ledger writes can still reach
// quorum; it never returns a negative count.
func resolvePDBMaxUnavailable(replicas, writeQuorum int32) intstr.IntOrString {
	return intstr.FromInt32(max(replicas-writeQuorum, 0))
}

func desiredConfigMap(bk *clusterv1alpha1.BookKeeper, rendered string) *corev1.ConfigMap {
	return builder.ConfigMap(bk.Name, bk.Namespace, builder.Labels(bk.Name, bookkeeperComponent), map[string]string{configMapKey: rendered})
}

func desiredHeadlessService(bk *clusterv1alpha1.BookKeeper) *corev1.Service {
	ports := []corev1.ServicePort{
		{Name: bookieContainerName, Port: bookiePort, TargetPort: intstr.FromInt32(bookiePort)},
		{Name: "http", Port: bookieAdminPort, TargetPort: intstr.FromInt32(bookieAdminPort)},
	}
	return builder.HeadlessService(bk.Name, bk.Namespace, builder.Labels(bk.Name, bookkeeperComponent), builder.SelectorLabels(bk.Name, bookkeeperComponent), ports)
}

func desiredPDB(bk *clusterv1alpha1.BookKeeper) *policyv1.PodDisruptionBudget {
	replicas := resolveReplicas(bk.Spec)
	writeQuorum := resolveWriteQuorum(bk.Spec)
	maxUnavailable := resolvePDBMaxUnavailable(replicas, writeQuorum)
	return builder.PodDisruptionBudget(bk.Name, bk.Namespace, builder.Labels(bk.Name, bookkeeperComponent), builder.SelectorLabels(bk.Name, bookkeeperComponent), maxUnavailable)
}

func desiredStatefulSet(bk *clusterv1alpha1.BookKeeper, image string, merged map[string]string, rendered string) *appsv1.StatefulSet {
	replicas := resolveReplicas(bk.Spec)
	labels := builder.Labels(bk.Name, bookkeeperComponent)
	selector := builder.SelectorLabels(bk.Name, bookkeeperComponent)

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bk.Name,
			Namespace: bk.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:             &replicas,
			ServiceName:          bk.Name,
			PodManagementPolicy:  resolvePodManagementPolicy(bk.Spec),
			Selector:             &metav1.LabelSelector{MatchLabels: selector},
			Template:             buildPodTemplate(bk, image, merged, rendered),
			VolumeClaimTemplates: buildVolumeClaimTemplates(bk.Spec, labels),
			// persistentVolumeClaimRetentionPolicy is deliberately left unset
			// (defaults to Retain/Retain): the operator does not own PVC
			// deletion yet, so bookie disks must survive pod/StatefulSet
			// deletion and scale-down without a separate, explicit decision.
		},
	}
}

func buildPodTemplate(bk *clusterv1alpha1.BookKeeper, image string, merged map[string]string, rendered string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      builder.Labels(bk.Name, bookkeeperComponent),
			Annotations: builder.WithConfigChecksum(nil, rendered),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{buildBookieContainer(image, merged)},
			Volumes: []corev1.Volume{
				{
					Name: volumeNameConfig,
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: bk.Name},
						},
					},
				},
			},
		},
	}
}

func buildBookieContainer(image string, merged map[string]string) corev1.Container {
	return corev1.Container{
		Name:    bookieContainerName,
		Image:   image,
		Command: []string{"bin/pulsar", "bookie"},
		Ports: []corev1.ContainerPort{
			{Name: bookieContainerName, ContainerPort: bookiePort},
			{Name: "http", ContainerPort: bookieAdminPort},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: volumeNameConfig, MountPath: bookieConfMountPath, SubPath: configMapKey, ReadOnly: true},
			{Name: volumeNameJournal, MountPath: merged[keyJournalDirectories]},
			{Name: volumeNameLedgers, MountPath: merged[keyLedgerDirectories]},
			{Name: volumeNameIndex, MountPath: merged[keyIndexDirectories]},
		},
		// Liveness only checks that the process accepts bookie-port TCP
		// connections, so a stuck admin HTTP server can't itself trigger a
		// restart loop; readiness additionally requires the bookie admin API
		// to report before the pod joins the ensemble.
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(bookiePort)},
			},
			InitialDelaySeconds: 20,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    6,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: bookieStatePath,
					Port: intstr.FromInt32(bookieAdminPort),
				},
			},
			InitialDelaySeconds: 20,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    6,
		},
	}
}

func buildVolumeClaimTemplates(spec clusterv1alpha1.BookKeeperSpec, labels map[string]string) []corev1.PersistentVolumeClaim {
	var journal, ledgers, index *clusterv1alpha1.VolumeSpec
	if spec.Volumes != nil {
		journal = spec.Volumes.Journal
		ledgers = spec.Volumes.Ledgers
		index = spec.Volumes.Index
	}

	return []corev1.PersistentVolumeClaim{
		volumeClaimTemplate(volumeNameJournal, journal, defaultJournalSize, labels),
		volumeClaimTemplate(volumeNameLedgers, ledgers, defaultLedgerSize, labels),
		volumeClaimTemplate(volumeNameIndex, index, defaultIndexSize, labels),
	}
}

func volumeClaimTemplate(name string, vol *clusterv1alpha1.VolumeSpec, defaultSize resource.Quantity, labels map[string]string) corev1.PersistentVolumeClaim {
	size := defaultSize
	var storageClassName *string
	if vol != nil {
		if !vol.Size.IsZero() {
			size = vol.Size
		}
		storageClassName = vol.StorageClassName
	}

	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
			StorageClassName: storageClassName,
		},
	}
}

// computeReadyCondition treats zero desired replicas as not-Ready rather than
// vacuously Ready (0 == 0): otherwise a freshly created StatefulSet, whose
// status is still all zeros before the controller has observed any pods,
// would read as Ready on the very first reconcile.
func computeReadyCondition(generation int64, desiredReplicas, readyReplicas int32) metav1.Condition {
	switch {
	case desiredReplicas == 0:
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             reasonNoReplicas,
			Message:            "BookKeeper has zero desired replicas",
		}
	case readyReplicas == desiredReplicas:
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			Reason:             reasonAllReady,
			Message:            fmt.Sprintf("%d/%d bookie replicas ready", readyReplicas, desiredReplicas),
		}
	default:
		return metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             reasonProgressing,
			Message:            fmt.Sprintf("%d/%d bookie replicas ready", readyReplicas, desiredReplicas),
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *BookKeeperReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.BookKeeper{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("cluster-bookkeeper").
		Complete(r)
}
