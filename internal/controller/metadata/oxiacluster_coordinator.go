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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// reconcileCoordinator ensures every coordinator-owned object exists and
// matches oxia's desired spec, returning the rendered config content so the
// caller can fold it into the coordinator pod template's checksum
// annotation.
func (r *OxiaClusterReconciler) reconcileCoordinator(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) (string, error) {
	if err := r.reconcileCoordinatorServiceAccount(ctx, oxia); err != nil {
		return "", fmt.Errorf("coordinator service account: %w", err)
	}
	if err := r.reconcileCoordinatorRole(ctx, oxia); err != nil {
		return "", fmt.Errorf("coordinator role: %w", err)
	}
	if err := r.reconcileCoordinatorRoleBinding(ctx, oxia); err != nil {
		return "", fmt.Errorf("coordinator role binding: %w", err)
	}
	if err := r.reconcileCoordinatorStatusConfigMap(ctx, oxia); err != nil {
		return "", fmt.Errorf("coordinator status config map: %w", err)
	}

	configContent, err := r.reconcileCoordinatorConfigMap(ctx, oxia)
	if err != nil {
		return "", fmt.Errorf("coordinator config map: %w", err)
	}

	if err := r.reconcileCoordinatorService(ctx, oxia); err != nil {
		return "", fmt.Errorf("coordinator service: %w", err)
	}
	if err := r.reconcileCoordinatorDeployment(ctx, oxia, configContent); err != nil {
		return "", fmt.Errorf("coordinator deployment: %w", err)
	}
	if err := r.reconcileCoordinatorPDB(ctx, oxia); err != nil {
		return "", fmt.Errorf("coordinator poddisruptionbudget: %w", err)
	}

	return configContent, nil
}

func (r *OxiaClusterReconciler) reconcileCoordinatorServiceAccount(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name:      coordinatorName(oxia.Name),
		Namespace: oxia.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = builder.Labels(oxia.Name, componentCoordinator)
		return builder.SetControllerOwner(oxia, sa, r.Scheme)
	})
	return err
}

// coordinatorConfigMapVerbs is exactly what the coordinator's configmap
// metadata provider needs — no create/delete, see reconcileCoordinatorRole.
var coordinatorConfigMapVerbs = []string{"get", "list", "watch", "update", "patch"}

// reconcileCoordinatorRole grants exactly the ConfigMap access the
// coordinator's configmap metadata provider needs (get/list/watch/update/
// patch — no create/delete: the operator itself creates both ConfigMaps the
// coordinator touches, see reconcileCoordinatorConfigMap and
// reconcileCoordinatorStatusConfigMap, so the coordinator's own ServiceAccount
// never needs to create one). It additionally grants the coordination.k8s.io
// Leases access oxia's leader-election actually uses (verified against
// oxiad/coordinator/metadata/provider/kubernetes/provider.go, which backs
// WaitToBecomeLeader with a client-go resourcelock.LeaseLock, not the
// ConfigMap itself) — required for coordinator.replicas > 1 to be safe HA
// rather than a split-brain risk.
func (r *OxiaClusterReconciler) reconcileCoordinatorRole(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{
		Name:      coordinatorName(oxia.Name),
		Namespace: oxia.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels = builder.Labels(oxia.Name, componentCoordinator)
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     coordinatorConfigMapVerbs,
			},
			{
				APIGroups: []string{"coordination.k8s.io"},
				Resources: []string{"leases"},
				Verbs:     []string{"get", "create", "update"},
			},
		}
		return builder.SetControllerOwner(oxia, role, r.Scheme)
	})
	return err
}

func (r *OxiaClusterReconciler) reconcileCoordinatorRoleBinding(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	name := coordinatorName(oxia.Name)
	rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: oxia.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, rb, func() error {
		rb.Labels = builder.Labels(oxia.Name, componentCoordinator)
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		}
		rb.Subjects = []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      name,
			Namespace: oxia.Namespace,
		}}
		return builder.SetControllerOwner(oxia, rb, r.Scheme)
	})
	return err
}

// reconcileCoordinatorStatusConfigMap ensures the coordinator's runtime
// status/leader-election ConfigMap exists, without ever touching its Data:
// once created, the coordinator process itself owns writes to it (Store()
// in oxia's kubernetes metadata provider Patches it directly), so the
// operator must not overwrite what the coordinator has written since.
func (r *OxiaClusterReconciler) reconcileCoordinatorStatusConfigMap(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      coordinatorStatusConfigMapName(oxia.Name),
		Namespace: oxia.Namespace,
	}}
	err := r.Get(ctx, client.ObjectKeyFromObject(cm), cm)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	cm.Labels = builder.Labels(oxia.Name, componentCoordinator)
	if err := builder.SetControllerOwner(oxia, cm, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, cm)
}

func (r *OxiaClusterReconciler) reconcileCoordinatorConfigMap(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) (string, error) {
	configContent, err := renderCoordinatorConfig(oxia)
	if err != nil {
		return "", err
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      coordinatorName(oxia.Name),
		Namespace: oxia.Namespace,
	}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = builder.Labels(oxia.Name, componentCoordinator)
		cm.Data = map[string]string{coordinatorConfigFileName: configContent}
		return builder.SetControllerOwner(oxia, cm, r.Scheme)
	})
	if err != nil {
		return "", err
	}
	return configContent, nil
}

func coordinatorPorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: portNameInternal, Port: coordinatorInternalPort, TargetPort: intstr.FromString(portNameInternal)},
		{Name: portNameMetrics, Port: coordinatorMetricsPort, TargetPort: intstr.FromString(portNameMetrics)},
	}
}

func (r *OxiaClusterReconciler) reconcileCoordinatorService(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	labels := builder.Labels(oxia.Name, componentCoordinator)
	selector := builder.SelectorLabels(oxia.Name, componentCoordinator)
	desired := builder.Service(coordinatorName(oxia.Name), oxia.Namespace, labels, selector, coordinatorPorts())

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return builder.SetControllerOwner(oxia, svc, r.Scheme)
	})
	return err
}

func (r *OxiaClusterReconciler) reconcileCoordinatorDeployment(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster, configContent string) error {
	name := coordinatorName(oxia.Name)
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: oxia.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		labels := builder.Labels(oxia.Name, componentCoordinator)
		selector := builder.SelectorLabels(oxia.Name, componentCoordinator)

		// CreateOrUpdate leaves deploy untouched (still the empty struct we
		// built above) when the Get it runs internally comes back
		// NotFound, so an empty ResourceVersion reliably means "about to be
		// created" here.
		isCreate := deploy.ResourceVersion == ""

		deploy.Labels = labels
		replicas := coordinatorReplicas(oxia)
		deploy.Spec.Replicas = &replicas

		// Deployment.spec.selector is immutable after creation: only set it
		// (and the strategy, which the API server also rejects changes to
		// alongside a changed selector in some versions) the first time.
		if isCreate {
			deploy.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
		}

		deploy.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      labels,
				Annotations: builder.WithConfigChecksum(deploy.Spec.Template.Annotations, configContent),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: name,
				// The coordinator is stateless from a scheduling standpoint:
				// shard-assignment state lives in the status ConfigMap, not
				// on the pod, and losing one replica just costs an election,
				// not data - so anti-affinity here is soft, not hard.
				Affinity:                  builder.PodAntiAffinity(false, selector),
				TopologySpreadConstraints: builder.ZoneTopologySpreadConstraints(selector),
				Containers: []corev1.Container{
					coordinatorContainer(oxia),
				},
			},
		}

		return builder.SetControllerOwner(oxia, deploy, r.Scheme)
	})
	return err
}

// reconcileCoordinatorPDB bounds voluntary coordinator disruption to 1 at a
// time: like other stateless tiers (broker, proxy), losing a coordinator
// replica just costs a leader-election handover, not data, so a flat 1 (not
// quorum math) is the right, conservative default.
func (r *OxiaClusterReconciler) reconcileCoordinatorPDB(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	name := coordinatorName(oxia.Name)
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: oxia.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		labels := builder.Labels(oxia.Name, componentCoordinator)
		selector := builder.SelectorLabels(oxia.Name, componentCoordinator)
		desired := builder.PodDisruptionBudget(name, oxia.Namespace, labels, selector, intstr.FromInt32(1))
		pdb.Labels = desired.Labels
		pdb.Spec = desired.Spec
		return builder.SetControllerOwner(oxia, pdb, r.Scheme)
	})
	return err
}

func coordinatorContainer(oxia *metadatav1alpha1.OxiaCluster) corev1.Container {
	name := coordinatorName(oxia.Name)
	statusName := coordinatorStatusConfigMapName(oxia.Name)

	return corev1.Container{
		Name:  "coordinator",
		Image: coordinatorImage(oxia),
		Command: []string{
			oxiaBinary,
			"coordinator",
			fmt.Sprintf("--conf=configmap:%s/%s", oxia.Namespace, name),
			"--log-json",
			"--metadata=configmap",
			fmt.Sprintf("--k8s-namespace=%s", oxia.Namespace),
			fmt.Sprintf("--k8s-configmap-name=%s", statusName),
		},
		Ports: []corev1.ContainerPort{
			{Name: portNameInternal, ContainerPort: coordinatorInternalPort},
			{Name: portNameMetrics, ContainerPort: coordinatorMetricsPort},
		},
		Resources:      coordinatorSpec(oxia).Resources,
		LivenessProbe:  oxiaHealthProbe(false, 10),
		ReadinessProbe: oxiaHealthProbe(false, 10),
	}
}
