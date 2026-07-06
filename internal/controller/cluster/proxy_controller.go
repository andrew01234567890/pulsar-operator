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

// Constants shared by the Proxy, AutoRecovery, and FunctionsWorker
// reconcilers (and their tests), collected here to satisfy goconst rather
// than repeating the same literal across all three. The Ready-condition
// reasons and the rollout-readiness helper live in readiness.go.
const (
	portNameHTTP = "http"

	cmdBinPulsar = "bin/pulsar"

	configKeyConfigurationMetadataStoreURL = "configurationMetadataStoreUrl"

	configValTrue = "true"
)

const (
	proxyComponent = "proxy"

	// proxyDefaultImage is used only when neither Proxy.spec.image nor a
	// cluster-wide default (applied upstream by the PulsarCluster reconciler
	// into Proxy.spec.image) is set, e.g. when a Proxy is created directly.
	proxyDefaultImage = "apachepulsar/pulsar:5.0.0-M1"

	proxyDefaultReplicas = 1

	proxyPulsarPort    = 6650
	proxyHTTPPort      = 8080
	proxyPulsarTLSPort = 6651
	proxyHTTPSPort     = 8443

	proxyConfigFileName  = "proxy.conf"
	proxyConfigMountPath = "/pulsar/conf/" + proxyConfigFileName
	proxyConfigVolume    = "config"

	proxyTLSVolume    = "tls-certs"
	proxyTLSMountPath = "/pulsar/certs/proxy"
	proxyTLSCertPath  = proxyTLSMountPath + "/tls.crt"
	proxyTLSKeyPath   = proxyTLSMountPath + "/tls.key"
)

// ProxyReconciler reconciles a Proxy object.
//
// It manages a stateless StatefulSet (for stable per-pod identity/DNS, not
// storage) fronted by a headless Service, rendering proxy.conf from operator
// defaults layered with Proxy.spec.config into a ConfigMap that is mounted
// directly over the image's conf/proxy.conf.
type ProxyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=proxies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=proxies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.pulsaroperator.io,resources=proxies/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a Proxy toward its desired StatefulSet/Service/ConfigMap/
// PodDisruptionBudget state and reports aggregated readiness on its status.
func (r *ProxyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	proxy := &clusterv1alpha1.Proxy{}
	if err := r.Get(ctx, req.NamespacedName, proxy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	labels := builder.Labels(proxy.Name, proxyComponent)
	selector := builder.SelectorLabels(proxy.Name, proxyComponent)
	renderedConf := config.RenderProperties(proxyMergedConfig(proxy.Spec))

	if err := r.reconcileConfigMap(ctx, proxy, labels, renderedConf); err != nil {
		log.Error(err, "failed to reconcile Proxy ConfigMap")
		return ctrl.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, proxy, labels, selector); err != nil {
		log.Error(err, "failed to reconcile Proxy headless Service")
		return ctrl.Result{}, err
	}

	desiredReplicas := proxyReplicas(proxy.Spec)
	sts, err := r.reconcileStatefulSet(ctx, proxy, labels, selector, desiredReplicas, renderedConf)
	if err != nil {
		log.Error(err, "failed to reconcile Proxy StatefulSet")
		return ctrl.Result{}, err
	}

	if err := r.reconcilePDB(ctx, proxy, labels, selector); err != nil {
		log.Error(err, "failed to reconcile Proxy PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	proxy.Status.Replicas = sts.Status.Replicas
	proxy.Status.ReadyReplicas = sts.Status.ReadyReplicas
	proxy.Status.ObservedGeneration = proxy.Generation
	apimeta.SetStatusCondition(&proxy.Status.Conditions, workloadReadyCondition(proxy.Generation, desiredReplicas, statefulSetRollout(sts), proxyComponent))

	if err := r.Status().Update(ctx, proxy); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating Proxy status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *ProxyReconciler) reconcileConfigMap(ctx context.Context, proxy *clusterv1alpha1.Proxy, labels map[string]string, renderedConf string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: proxyConfigMapName(proxy), Namespace: proxy.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{proxyConfigFileName: renderedConf}
		return controllerutil.SetControllerReference(proxy, cm, r.Scheme)
	})
	return err
}

func (r *ProxyReconciler) reconcileHeadlessService(ctx context.Context, proxy *clusterv1alpha1.Proxy, labels, selector map[string]string) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: proxy.Name, Namespace: proxy.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		desired := builder.HeadlessService(proxy.Name, proxy.Namespace, labels, selector, proxyServicePorts(proxy.Spec.Tls))
		svc.Labels = desired.Labels
		svc.Spec.ClusterIP = desired.Spec.ClusterIP
		svc.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return controllerutil.SetControllerReference(proxy, svc, r.Scheme)
	})
	return err
}

func (r *ProxyReconciler) reconcileStatefulSet(
	ctx context.Context,
	proxy *clusterv1alpha1.Proxy,
	labels, selector map[string]string,
	replicas int32,
	renderedConf string,
) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: proxy.Name, Namespace: proxy.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = labels
		sts.Spec.Replicas = &replicas
		sts.Spec.ServiceName = proxy.Name
		sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
		sts.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      labels,
				Annotations: builder.WithConfigChecksum(sts.Spec.Template.Annotations, renderedConf),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:           proxyComponent,
					Image:          proxyImage(proxy.Spec.Image),
					Command:        []string{cmdBinPulsar},
					Args:           []string{"proxy"},
					Ports:          proxyContainerPorts(proxy.Spec.Tls),
					Resources:      proxy.Spec.Resources,
					LivenessProbe:  proxyProbe(),
					ReadinessProbe: proxyProbe(),
					VolumeMounts:   proxyVolumeMounts(proxy.Spec.Tls),
				}},
				Volumes: proxyVolumes(proxy, proxy.Spec.Tls),
			},
		}
		return controllerutil.SetControllerReference(proxy, sts, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	return sts, nil
}

func (r *ProxyReconciler) reconcilePDB(ctx context.Context, proxy *clusterv1alpha1.Proxy, labels, selector map[string]string) error {
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: proxy.Name + "-pdb", Namespace: proxy.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		desired := builder.PodDisruptionBudget(pdb.Name, proxy.Namespace, labels, selector, intstr.FromInt32(1))
		pdb.Labels = desired.Labels
		pdb.Spec = desired.Spec
		return controllerutil.SetControllerReference(proxy, pdb, r.Scheme)
	})
	return err
}

func proxyConfigMapName(proxy *clusterv1alpha1.Proxy) string {
	return proxy.Name + "-config"
}

func proxyImage(specImage string) string {
	if specImage != "" {
		return specImage
	}
	return proxyDefaultImage
}

func proxyReplicas(spec clusterv1alpha1.ProxySpec) int32 {
	if spec.Replicas != nil {
		return *spec.Replicas
	}
	return proxyDefaultReplicas
}

// proxyTLSWired reports whether TLS is enabled and has enough information
// (a cert Secret) to actually be wired into the container/config/ports.
func proxyTLSWired(tls *clusterv1alpha1.ProxyTlsConfig) bool {
	return tls != nil && tls.Enabled && tls.SecretName != ""
}

// proxyDefaultConfig returns the operator's baseline proxy.conf. metadataStoreUrl
// is left blank, matching the upstream reference conf: Proxy has no notion of
// which metadata store implementation the cluster uses (Oxia, or otherwise),
// so it never invents a store URL itself; it flows in only via spec.config.
func proxyDefaultConfig() map[string]string {
	return map[string]string{
		"metadataStoreUrl":                     "",
		configKeyConfigurationMetadataStoreURL: "",
		"bindAddress":                          "0.0.0.0",
		"servicePort":                          strconv.Itoa(proxyPulsarPort),
		"webServicePort":                       strconv.Itoa(proxyHTTPPort),
	}
}

func proxyTLSDefaultConfig(tls *clusterv1alpha1.ProxyTlsConfig) map[string]string {
	if !proxyTLSWired(tls) {
		return nil
	}
	return map[string]string{
		"servicePortTls":         strconv.Itoa(proxyPulsarTLSPort),
		"webServicePortTls":      strconv.Itoa(proxyHTTPSPort),
		"tlsCertificateFilePath": proxyTLSCertPath,
		"tlsKeyFilePath":         proxyTLSKeyPath,
	}
}

// proxyMergedConfig layers, lowest to highest precedence: operator defaults,
// TLS defaults (only when TLS is actually wired), then the user's
// spec.config, so a user override always wins.
func proxyMergedConfig(spec clusterv1alpha1.ProxySpec) map[string]string {
	merged := config.Merge(proxyDefaultConfig(), proxyTLSDefaultConfig(spec.Tls))
	return config.Merge(merged, spec.Config)
}

func proxyContainerPorts(tls *clusterv1alpha1.ProxyTlsConfig) []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{Name: "pulsar", ContainerPort: proxyPulsarPort},
		{Name: portNameHTTP, ContainerPort: proxyHTTPPort},
	}
	if proxyTLSWired(tls) {
		ports = append(ports,
			corev1.ContainerPort{Name: "pulsarssl", ContainerPort: proxyPulsarTLSPort},
			corev1.ContainerPort{Name: "https", ContainerPort: proxyHTTPSPort},
		)
	}
	return ports
}

func proxyServicePorts(tls *clusterv1alpha1.ProxyTlsConfig) []corev1.ServicePort {
	ports := []corev1.ServicePort{
		{Name: "pulsar", Port: proxyPulsarPort, TargetPort: intstr.FromInt32(proxyPulsarPort)},
		{Name: portNameHTTP, Port: proxyHTTPPort, TargetPort: intstr.FromInt32(proxyHTTPPort)},
	}
	if proxyTLSWired(tls) {
		ports = append(ports,
			corev1.ServicePort{Name: "pulsarssl", Port: proxyPulsarTLSPort, TargetPort: intstr.FromInt32(proxyPulsarTLSPort)},
			corev1.ServicePort{Name: "https", Port: proxyHTTPSPort, TargetPort: intstr.FromInt32(proxyHTTPSPort)},
		)
	}
	return ports
}

func proxyProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/status.html",
				Port: intstr.FromInt32(proxyHTTPPort),
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

func proxyVolumes(proxy *clusterv1alpha1.Proxy, tls *clusterv1alpha1.ProxyTlsConfig) []corev1.Volume {
	volumes := []corev1.Volume{{
		Name: proxyConfigVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: proxyConfigMapName(proxy)},
			},
		},
	}}
	if proxyTLSWired(tls) {
		volumes = append(volumes, corev1.Volume{
			Name: proxyTLSVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tls.SecretName},
			},
		})
	}
	return volumes
}

func proxyVolumeMounts(tls *clusterv1alpha1.ProxyTlsConfig) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{{
		Name:      proxyConfigVolume,
		MountPath: proxyConfigMountPath,
		SubPath:   proxyConfigFileName,
	}}
	if proxyTLSWired(tls) {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      proxyTLSVolume,
			MountPath: proxyTLSMountPath,
			ReadOnly:  true,
		})
	}
	return mounts
}

// SetupWithManager sets up the controller with the Manager.
func (r *ProxyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.Proxy{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("cluster-proxy").
		Complete(r)
}
