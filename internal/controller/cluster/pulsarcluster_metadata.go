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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	oxiaurl "github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

const (
	// configKeyMetadataServiceURI is BookKeeper's bookkeeper.conf key for its
	// metadata store connection string (broker/proxy's equivalent,
	// configKeyMetadataStoreURL/configKeyConfigurationMetadataStoreURL, is
	// declared in proxy_controller.go and shared package-wide).
	configKeyMetadataServiceURI = "metadataServiceUri"

	// conditionTypeMetadataInitialized tracks the one-time bin/pulsar
	// initialize-cluster-metadata Job that bootstraps the cluster's root
	// metadata in Oxia (cluster name, default namespace, broker/web service
	// URLs). A PulsarCluster is never Ready until this has succeeded: no
	// broker can serve traffic for a cluster name Oxia has never heard of.
	conditionTypeMetadataInitialized = "MetadataInitialized"

	reasonMetadataInitWaitingForOxia = "WaitingForOxia"
	reasonMetadataInitJobRunning     = "JobRunning"
	reasonMetadataInitJobFailed      = "JobFailed"
	reasonMetadataInitJobSucceeded   = "JobSucceeded"

	metadataInitComponentName = "metadata-init"
	metadataInitContainerName = "initialize-cluster-metadata"
)

// oxiaPublicServiceName returns the Service name Pulsar/BookKeeper/
// FunctionsWorker components address to reach the OxiaCluster child this
// PulsarCluster provisions (see reconcileOxia/childName): the OxiaCluster
// reconciler's own client-facing "public" Service, backed by oxia-server
// pods. It must never be the coordinator's Service, which only assigns
// shards and never serves client reads/writes (see internal/metadata's own
// regression test guarding this).
func oxiaPublicServiceName(clusterName string) string {
	return oxiaurl.PublicServiceName(childName(clusterName, "oxia"))
}

// withBrokerProxyMetadataDefaults sets metadataStoreUrl and
// configurationMetadataStoreUrl to the cluster's Oxia metadata store, unless
// the user already set either. Broker and proxy conventionally point both
// keys at the same store/namespace (there is only one physical cluster here,
// so there is no separate "configuration store" to split them across).
func withBrokerProxyMetadataDefaults(cfg map[string]string, clusterName string) map[string]string {
	url := oxiaurl.MetadataStoreURL(oxiaPublicServiceName(clusterName), oxiaurl.DefaultNamespace)
	cfg = setConfigDefault(cfg, configKeyMetadataStoreURL, url)
	cfg = setConfigDefault(cfg, configKeyConfigurationMetadataStoreURL, url)
	return cfg
}

// withBookKeeperMetadataDefault sets metadataServiceUri to the cluster's Oxia
// metadata store, unless the user already set it.
func withBookKeeperMetadataDefault(cfg map[string]string, clusterName string) map[string]string {
	uri := oxiaurl.BookkeeperMetadataServiceURI(oxiaPublicServiceName(clusterName))
	return setConfigDefault(cfg, configKeyMetadataServiceURI, uri)
}

// withFunctionsWorkerMetadataDefault sets configurationMetadataStoreUrl (the
// only metadata-store key functions_worker.yml has) to the cluster's Oxia
// metadata store, unless the user already set it.
func withFunctionsWorkerMetadataDefault(cfg map[string]string, clusterName string) map[string]string {
	url := oxiaurl.MetadataStoreURL(oxiaPublicServiceName(clusterName), oxiaurl.DefaultNamespace)
	return setConfigDefault(cfg, configKeyConfigurationMetadataStoreURL, url)
}

// setConfigDefault sets cfg[key]=value unless the user already set key,
// allocating cfg if nil. Callers always pass a spec.Config already copied out
// of the PulsarCluster's own sub-spec (via buildXSpec's DeepCopy), so
// mutating it in place never reaches back into the stored PulsarCluster spec.
func setConfigDefault(cfg map[string]string, key, value string) map[string]string {
	if cfg == nil {
		cfg = make(map[string]string, 1)
	}
	if _, ok := cfg[key]; !ok {
		cfg[key] = value
	}
	return cfg
}

// metadataInitJobName deterministically names the cluster-metadata-init Job
// after its owning PulsarCluster.
func metadataInitJobName(clusterName string) string {
	return clusterName + "-metadata-init"
}

// buildMetadataInitJob renders the Job that runs `bin/pulsar
// initialize-cluster-metadata` once against the cluster's Oxia metadata
// store, registering the cluster's name and its broker/web service URLs -
// the one-time bootstrap every Pulsar cluster needs before any broker can
// serve traffic. It is a pure function of cluster so it is unit-testable
// without a client; the caller sets the owner reference and creates it.
func buildMetadataInitJob(cluster *clusterv1alpha1.PulsarCluster) *batchv1.Job {
	labels := builder.Labels(cluster.Name, metadataInitComponentName)

	storeURL := oxiaurl.MetadataStoreURL(oxiaPublicServiceName(cluster.Name), oxiaurl.DefaultNamespace)
	brokerSvc := childName(cluster.Name, brokerComponent)
	webServiceURL := fmt.Sprintf("http://%s:%d", brokerSvc, defaultWebServicePort)
	brokerServiceURL := fmt.Sprintf("pulsar://%s:%d", brokerSvc, defaultBrokerServicePort)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metadataInitJobName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    metadataInitContainerName,
							Image:   clusterDefaultImage(cluster.Spec),
							Command: []string{cmdBinPulsar},
							Args: []string{
								"initialize-cluster-metadata",
								"--cluster", cluster.Name,
								"--metadata-store", storeURL,
								"--configuration-store", storeURL,
								"--web-service-url", webServiceURL,
								"--broker-service-url", brokerServiceURL,
							},
						},
					},
				},
			},
		},
	}
}

// reconcileMetadataInit reconciles the cluster-metadata-init Job and reports
// it as a componentReport so it participates in the umbrella Ready rollup
// exactly like a child component: the cluster is not Ready until the Job has
// succeeded. When a non-Oxia metadata store is ever selected, the Job (like
// the OxiaCluster child) is pruned instead.
func (r *PulsarClusterReconciler) reconcileMetadataInit(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, oxiaReady bool) (componentReport, error) {
	if !oxiaSelected(cluster.Spec) {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: metadataInitJobName(cluster.Name), Namespace: cluster.Namespace},
		}
		return pruneChild(ctx, r, metadataInitComponentName, job)
	}

	job, err := r.reconcileMetadataInitJob(ctx, cluster, oxiaReady)
	if err != nil {
		return componentReport{}, fmt.Errorf("%s: %w", metadataInitComponentName, err)
	}

	cond := metadataInitializedCondition(cluster.Generation, oxiaReady, job)
	apimeta.SetStatusCondition(&cluster.Status.Conditions, cond)

	return componentReport{
		name:    metadataInitComponentName,
		present: true,
		ready:   cond.Status == metav1.ConditionTrue,
		reason:  cond.Reason,
		message: cond.Message,
	}, nil
}

// reconcileMetadataInitJob creates the cluster-metadata-init Job once the
// OxiaCluster child is Ready, and never mutates it afterwards: a Job's pod
// template is immutable, and initialize-cluster-metadata is meant to run
// exactly once per cluster. A nil Job with a nil error means the Job hasn't
// been created yet (Oxia isn't Ready).
func (r *PulsarClusterReconciler) reconcileMetadataInitJob(ctx context.Context, cluster *clusterv1alpha1.PulsarCluster, oxiaReady bool) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	key := types.NamespacedName{Name: metadataInitJobName(cluster.Name), Namespace: cluster.Namespace}
	err := r.Get(ctx, key, job)
	switch {
	case err == nil:
		return job, nil
	case !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("getting cluster-metadata-init job: %w", err)
	case !oxiaReady:
		return nil, nil
	}

	desired := buildMetadataInitJob(cluster)
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return nil, fmt.Errorf("setting owner reference on cluster-metadata-init job: %w", err)
	}
	if err := r.Create(ctx, desired); err != nil {
		return nil, fmt.Errorf("creating cluster-metadata-init job: %w", err)
	}
	return desired, nil
}

// metadataInitializedCondition computes the MetadataInitialized condition
// from whether Oxia is Ready and the observed state of the
// cluster-metadata-init Job (nil until it has been created).
func metadataInitializedCondition(generation int64, oxiaReady bool, job *batchv1.Job) metav1.Condition {
	base := metav1.Condition{Type: conditionTypeMetadataInitialized, ObservedGeneration: generation}

	switch {
	case !oxiaReady:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitWaitingForOxia
		base.Message = "waiting for the OxiaCluster metadata store to become Ready before initializing cluster metadata"
	case job == nil:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitJobRunning
		base.Message = "cluster-metadata-init job has not been created yet"
	case jobSucceeded(job):
		base.Status = metav1.ConditionTrue
		base.Reason = reasonMetadataInitJobSucceeded
		base.Message = "cluster metadata initialized"
	case jobFailedPermanently(job):
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitJobFailed
		base.Message = fmt.Sprintf("cluster-metadata-init job %s failed", job.Name)
	default:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonMetadataInitJobRunning
		base.Message = fmt.Sprintf("cluster-metadata-init job %s is running", job.Name)
	}

	return base
}

func jobSucceeded(job *batchv1.Job) bool {
	if c := findJobCondition(job, batchv1.JobComplete); c != nil {
		return c.Status == corev1.ConditionTrue
	}
	return job.Status.Succeeded > 0
}

func jobFailedPermanently(job *batchv1.Job) bool {
	c := findJobCondition(job, batchv1.JobFailed)
	return c != nil && c.Status == corev1.ConditionTrue
}

func findJobCondition(job *batchv1.Job, condType batchv1.JobConditionType) *batchv1.JobCondition {
	for i := range job.Status.Conditions {
		if job.Status.Conditions[i].Type == condType {
			return &job.Status.Conditions[i]
		}
	}
	return nil
}
