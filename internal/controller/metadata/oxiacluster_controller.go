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

package metadata

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// OxiaClusterReconciler reconciles a OxiaCluster object
type OxiaClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=metadata.pulsaroperator.io,resources=oxiaclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives an OxiaCluster towards two owned workloads: an
// oxia-server StatefulSet (the data plane) and an oxia-coordinator
// Deployment (shard assignment), plus the ConfigMaps, RBAC, and Services
// that connect them. The server is reconciled first because the
// coordinator's static servers list is derived from the server's *desired*
// replica count, so a servers-list change (and the coordinator restart that
// must follow it) takes effect on the same reconcile a server scale
// request does, rather than lagging a reconcile behind.
func (r *OxiaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var oxia metadatav1alpha1.OxiaCluster
	if err := r.Get(ctx, req.NamespacedName, &oxia); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileServer(ctx, &oxia); err != nil {
		log.Error(err, "Failed to reconcile oxia-server")
		return ctrl.Result{}, err
	}

	if _, err := r.reconcileCoordinator(ctx, &oxia); err != nil {
		log.Error(err, "Failed to reconcile oxia-coordinator")
		return ctrl.Result{}, err
	}

	if err := r.reconcileStatus(ctx, &oxia); err != nil {
		log.Error(err, "Failed to update OxiaCluster status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *OxiaClusterReconciler) reconcileStatus(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	coordinator := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Name: coordinatorName(oxia.Name), Namespace: oxia.Namespace}, coordinator); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	server := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: serverName(oxia.Name), Namespace: oxia.Namespace}, server); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	newStatus := aggregateStatus(oxia, coordinator, server)
	if equality.Semantic.DeepEqual(oxia.Status, newStatus) {
		return nil
	}

	oxia.Status = newStatus
	return r.Status().Update(ctx, oxia)
}

// SetupWithManager sets up the controller with the Manager. Owns() on every
// child kind matters for more than drift-correction: without it, a change
// that originates on the child itself — most importantly a StatefulSet/
// Deployment's status.readyReplicas catching up after a scale — never
// re-enqueues the OxiaCluster, so status.conditions[Ready] would go stale
// (stuck reporting "not ready" long after every pod actually turned Ready)
// until something else happened to touch the OxiaCluster's own spec.
func (r *OxiaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&metadatav1alpha1.OxiaCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("metadata-oxiacluster").
		Complete(r)
}
