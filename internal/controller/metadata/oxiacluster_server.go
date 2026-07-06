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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

const serverDataVolumeName = "data"

// reconcileServer ensures every oxia-server-owned object exists and matches
// oxia's desired spec.
func (r *OxiaClusterReconciler) reconcileServer(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	if err := r.reconcileServerHeadlessService(ctx, oxia); err != nil {
		return fmt.Errorf("server headless service: %w", err)
	}
	if err := r.reconcileServerPublicService(ctx, oxia); err != nil {
		return fmt.Errorf("server public service: %w", err)
	}
	if err := r.reconcileServerStatefulSet(ctx, oxia); err != nil {
		return fmt.Errorf("server statefulset: %w", err)
	}
	return nil
}

func serverPorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: portNamePublic, Port: serverPublicPort, TargetPort: intstr.FromString(portNamePublic)},
		{Name: portNameInternal, Port: serverInternalPort, TargetPort: intstr.FromString(portNameInternal)},
		{Name: portNameMetrics, Port: serverMetricsPort, TargetPort: intstr.FromString(portNameMetrics)},
	}
}

// reconcileServerHeadlessService provides the peer-DNS Service StatefulSet
// pods need to resolve each other, and that the coordinator's static servers
// list addresses. PublishNotReadyAddresses matters here: servers must
// resolve each other during initial cluster formation, before any of them is
// individually Ready.
func (r *OxiaClusterReconciler) reconcileServerHeadlessService(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	labels := builder.Labels(oxia.Name, componentServer)
	selector := builder.SelectorLabels(oxia.Name, componentServer)
	desired := builder.HeadlessService(serverHeadlessServiceName(oxia.Name), oxia.Namespace, labels, selector, serverPorts())

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		svc.Spec.ClusterIP = desired.Spec.ClusterIP
		svc.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		return builder.SetControllerOwner(oxia, svc, r.Scheme)
	})
	return err
}

// reconcileServerPublicService is the OxiaCluster's public entry point
// (oxia://<this-service>:6648/<namespace>): despite targeting oxia-server
// pods rather than the coordinator, this is the Service Pulsar/BookKeeper
// wiring (internal/metadata.MetadataStoreURL) addresses, because the
// coordinator process itself never serves client reads/writes.
func (r *OxiaClusterReconciler) reconcileServerPublicService(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	labels := builder.Labels(oxia.Name, componentServer)
	selector := builder.SelectorLabels(oxia.Name, componentServer)
	desired := builder.Service(publicServiceName(oxia.Name), oxia.Namespace, labels, selector, serverPorts())

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return builder.SetControllerOwner(oxia, svc, r.Scheme)
	})
	return err
}

func (r *OxiaClusterReconciler) reconcileServerStatefulSet(ctx context.Context, oxia *metadatav1alpha1.OxiaCluster) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      serverName(oxia.Name),
		Namespace: oxia.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		labels := builder.Labels(oxia.Name, componentServer)
		selector := builder.SelectorLabels(oxia.Name, componentServer)

		// CreateOrUpdate leaves sts untouched (still the empty struct we
		// built above) when the Get it runs internally comes back
		// NotFound, so an empty ResourceVersion reliably means "about to be
		// created" here.
		isCreate := sts.ResourceVersion == ""

		sts.Labels = labels
		replicas := serverReplicas(oxia)
		sts.Spec.Replicas = &replicas
		sts.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{serverContainer(oxia)},
			},
		}

		// StatefulSet.spec.{selector,serviceName,podManagementPolicy,
		// volumeClaimTemplates} are immutable after creation (the API
		// server rejects any Update that changes them, even to an
		// equivalent value) — set them only once, at creation.
		if isCreate {
			sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
			sts.Spec.ServiceName = serverHeadlessServiceName(oxia.Name)
			sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
			sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{serverVolumeClaimTemplate(oxia)}
		}

		return builder.SetControllerOwner(oxia, sts, r.Scheme)
	})
	return err
}

func serverVolumeClaimTemplate(oxia *metadatav1alpha1.OxiaCluster) corev1.PersistentVolumeClaim {
	spec := serverSpec(oxia)
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   serverDataVolumeName,
			Labels: builder.SelectorLabels(oxia.Name, componentServer),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: spec.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: spec.StorageSize,
				},
			},
		},
	}
}

// serverContainer's data-dir/wal-dir both live under the single "data"
// PVC (subdirectories db/wal): OxiaServerSpec exposes exactly one
// storageClassName/storageSize pair, matching the upstream
// oxia-server-statefulset.yaml, which mounts one PVC at /data rather than
// provisioning data and WAL on separate volumes.
func serverContainer(oxia *metadatav1alpha1.OxiaCluster) corev1.Container {
	command := []string{
		oxiaBinary,
		"server",
		"--log-json",
		"--data-dir=/data/db",
		"--wal-dir=/data/wal",
		fmt.Sprintf("--db-cache-size-mb=%d", dbCacheSizeMb(oxia)),
	}
	if !walSyncData(oxia) {
		command = append(command, "--wal-sync-data=false")
	}

	return corev1.Container{
		Name:    "server",
		Image:   serverImage(oxia),
		Command: command,
		Ports: []corev1.ContainerPort{
			{Name: portNamePublic, ContainerPort: serverPublicPort},
			{Name: portNameInternal, ContainerPort: serverInternalPort},
			{Name: portNameMetrics, ContainerPort: serverMetricsPort},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: serverDataVolumeName, MountPath: "/data"},
		},
		LivenessProbe:  oxiaHealthProbe(false, 10),
		ReadinessProbe: oxiaHealthProbe(true, 10),
		StartupProbe:   oxiaHealthProbe(false, 60),
	}
}
