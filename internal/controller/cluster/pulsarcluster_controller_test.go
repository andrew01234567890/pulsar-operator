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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

var _ = Describe("PulsarCluster Controller", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		clusterName string
		req         reconcile.Request
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-test-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		reconciler = &PulsarClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		clusterName = "test-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	It("stamps out configured child CRs with owner references and propagated specs, and aggregates their readiness", func() {
		const clusterImage = "cluster/pulsar:5.0.0"
		const storageClass = "fast-ssd"

		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Image:  clusterImage,
				Global: &clusterv1alpha1.GlobalSpec{StorageClassName: ptr(storageClass)},
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3))},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Volumes: &clusterv1alpha1.BookKeeperVolumes{
						Journal: &clusterv1alpha1.VolumeSpec{},
					},
				},
				Oxia: &metadatav1alpha1.OxiaClusterSpec{
					Server: &metadatav1alpha1.OxiaServerSpec{},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling the PulsarCluster")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		wantOxiaURL := "oxia://" + clusterName + "-oxia-oxia:6648/default"

		By("creating the Broker child with an owner reference, the propagated image, and the injected oxia:// metadata URLs")
		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		Expect(broker.Spec.Image).To(Equal(clusterImage))
		Expect(broker.Spec.Replicas).To(HaveValue(Equal(int32(3))))
		Expect(metav1.IsControlledBy(broker, pulsarCluster)).To(BeTrue())
		Expect(broker.Spec.Config).To(HaveKeyWithValue("metadataStoreUrl", wantOxiaURL))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("configurationMetadataStoreUrl", wantOxiaURL))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("clusterName", clusterName))

		By("creating the BookKeeper child with the propagated global storage class and the injected metadataServiceUri")
		bookKeeper := &clusterv1alpha1.BookKeeper{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-bookkeeper", Namespace: namespace.Name}, bookKeeper)).To(Succeed())
		Expect(bookKeeper.Spec.Volumes.Journal.StorageClassName).To(HaveValue(Equal(storageClass)))
		Expect(metav1.IsControlledBy(bookKeeper, pulsarCluster)).To(BeTrue())
		Expect(bookKeeper.Spec.Config).To(HaveKeyWithValue(
			"metadataServiceUri", "metadata-store:oxia://"+clusterName+"-oxia-oxia:6648/bookkeeper"))

		By("creating the OxiaCluster child with the propagated storage class but NOT the cluster-wide pulsar image")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		// Regression: apachepulsar/pulsar has no oxia binary, so the umbrella
		// must never stamp its own image onto the OxiaCluster child - only the
		// OxiaCluster reconciler's own oxia/oxia default (or an explicit
		// user-set oxia image) may apply.
		Expect(oxia.Spec.Server.Image).NotTo(Equal(clusterImage))
		Expect(oxia.Spec.Server.Image).To(BeEmpty())
		Expect(oxia.Spec.Server.StorageClassName).To(HaveValue(Equal(storageClass)))
		Expect(metav1.IsControlledBy(oxia, pulsarCluster)).To(BeTrue())

		By("not creating children whose sub-spec was left unconfigured")
		proxy := &clusterv1alpha1.Proxy{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-proxy", Namespace: namespace.Name}, proxy)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("reporting Ready=False because no child has reported readiness yet")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Reason).To(Equal(reasonComponentNotReady))
		Expect(pulsarCluster.Status.BrokerPhase).To(Equal(phaseNotReady))
		Expect(pulsarCluster.Status.BookKeeperPhase).To(Equal(phaseNotReady))
		Expect(pulsarCluster.Status.OxiaPhase).To(Equal(phaseNotReady))
		Expect(pulsarCluster.Status.ProxyPhase).To(BeEmpty())
		Expect(pulsarCluster.Status.ObservedGeneration).To(Equal(pulsarCluster.Generation))

		By("marking every configured child Ready for its current generation")
		broker.Status.Conditions = []metav1.Condition{readyConditionForGeneration(broker.Generation, "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())
		bookKeeper.Status.Conditions = []metav1.Condition{readyConditionForGeneration(bookKeeper.Generation, "all bookie pods ready")}
		Expect(k8sClient.Status().Update(ctx, bookKeeper)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("re-reconciling now that Oxia is Ready to create the cluster-metadata-init Job")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("creating the cluster-metadata-init Job with an owner reference")
		metadataInitJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}, metadataInitJob)).To(Succeed())
		Expect(metav1.IsControlledBy(metadataInitJob, pulsarCluster)).To(BeTrue())

		By("still reporting Ready=False: the metadata-init Job hasn't succeeded yet")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(apimeta.IsStatusConditionFalse(pulsarCluster.Status.Conditions, conditionTypeReady)).To(BeTrue())
		Expect(apimeta.IsStatusConditionFalse(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)).To(BeTrue())

		By("marking the cluster-metadata-init Job Succeeded (envtest has no real Job controller to run it)")
		metadataInitJob.Status.Succeeded = 1
		Expect(k8sClient.Status().Update(ctx, metadataInitJob)).To(Succeed())

		By("re-reconciling and observing the aggregated Ready=True condition")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond = apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		Expect(readyCond.Reason).To(Equal(reasonAllComponentsReady))
		Expect(apimeta.IsStatusConditionTrue(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)).To(BeTrue())
		Expect(pulsarCluster.Status.BrokerPhase).To(Equal(phaseReady))
		Expect(pulsarCluster.Status.BookKeeperPhase).To(Equal(phaseReady))
		Expect(pulsarCluster.Status.OxiaPhase).To(Equal(phaseReady))
	})

	It("always provisions a default OxiaCluster even when no sub-specs are configured", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("creating the mandatory OxiaCluster child")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		Expect(metav1.IsControlledBy(oxia, pulsarCluster)).To(BeTrue())

		By("not creating optional Pulsar components")
		broker := &clusterv1alpha1.Broker{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("reporting Ready=False on the still-unready Oxia store, not NoComponentsConfigured")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Reason).To(Equal(reasonComponentNotReady))
		Expect(pulsarCluster.Status.OxiaPhase).To(Equal(phaseNotReady))
	})

	It("prunes a child CR when its sub-spec is removed from the PulsarCluster", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling with the broker configured")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())

		By("removing the broker sub-spec")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.Broker = nil
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		By("reconciling again")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the now-undesired Broker child")
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("clearing the broker phase from status")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(pulsarCluster.Status.BrokerPhase).To(BeEmpty())
	})

	It("does not aggregate a stale child Ready condition after the child's spec changes", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling and marking both broker and the mandatory oxia Ready")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		broker.Status.Conditions = []metav1.Condition{readyConditionForGeneration(broker.Generation, "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("re-reconciling to create the cluster-metadata-init Job now that Oxia is Ready, then marking it Succeeded")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		metadataInitJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}, metadataInitJob)).To(Succeed())
		metadataInitJob.Status.Succeeded = 1
		Expect(k8sClient.Status().Update(ctx, metadataInitJob)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(apimeta.IsStatusConditionTrue(pulsarCluster.Status.Conditions, conditionTypeReady)).To(BeTrue())

		By("bumping the broker's desired spec so its child generation advances past its Ready status")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.Broker.Replicas = ptr(int32(5))
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("reporting the broker as progressing rather than trusting its stale Ready condition")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Message).To(ContainSubstring(reasonComponentProgressing))
		Expect(pulsarCluster.Status.BrokerPhase).To(Equal(phaseNotReady))
	})

	It("gates the cluster-metadata-init Job on OxiaCluster readiness and blocks overall Ready until it succeeds", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling before Oxia is Ready")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("not creating the Job yet")
		metadataInitJob := &batchv1.Job{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}, metadataInitJob)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		initCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)
		Expect(initCond).NotTo(BeNil())
		Expect(initCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(initCond.Reason).To(Equal(reasonMetadataInitWaitingForOxia))

		By("marking Oxia Ready")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("reconciling again to create the Job")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}, metadataInitJob)).To(Succeed())
		Expect(metav1.IsControlledBy(metadataInitJob, pulsarCluster)).To(BeTrue())
		Expect(metadataInitJob.Spec.Template.Spec.Containers[0].Args).To(HaveLen(1))
		script := metadataInitJob.Spec.Template.Spec.Containers[0].Args[0]
		Expect(script).To(ContainSubstring(clusterName))
		Expect(script).To(ContainSubstring("bin/bookkeeper shell initnewcluster"))
		Expect(script).To(ContainSubstring("bin/pulsar initialize-cluster-metadata"))

		By("wiring the BookKeeper metadataServiceUri into the Job's mounted config, same as the BookKeeper reconciler")
		metadataInitConfigMap := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}, metadataInitConfigMap)).To(Succeed())
		Expect(metav1.IsControlledBy(metadataInitConfigMap, pulsarCluster)).To(BeTrue())
		Expect(metadataInitConfigMap.Data[configMapKey]).To(ContainSubstring(withBookKeeperMetadataDefault(nil, clusterName)[configKeyMetadataServiceURI]))

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		readyCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeReady)
		Expect(readyCond).NotTo(BeNil())
		Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(readyCond.Message).To(ContainSubstring(metadataInitComponentName))

		By("marking the Job Succeeded and the Broker Ready")
		metadataInitJob.Status.Succeeded = 1
		Expect(k8sClient.Status().Update(ctx, metadataInitJob)).To(Succeed())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		broker.Status.Conditions = []metav1.Condition{readyConditionForGeneration(broker.Generation, "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

		By("re-reconciling and observing overall Ready=True")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(apimeta.IsStatusConditionTrue(pulsarCluster.Status.Conditions, conditionTypeReady)).To(BeTrue())
		Expect(apimeta.IsStatusConditionTrue(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)).To(BeTrue())
	})

	It("reports a terminally-failed metadata-init Job and retries it instead of wedging the cluster", func() {
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling, marking Oxia Ready, and reconciling again to create the Job")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		metadataInitJob := &batchv1.Job{}
		jobKey := types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}
		Expect(k8sClient.Get(ctx, jobKey, metadataInitJob)).To(Succeed())

		By("marking the Job terminally Failed")
		// The apiserver validates Job status transitions: Failed=True requires
		// a FailureTarget=true condition and a startTime for a finished Job.
		startTime := metav1.Now()
		metadataInitJob.Status.StartTime = &startTime
		metadataInitJob.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
		}
		Expect(k8sClient.Status().Update(ctx, metadataInitJob)).To(Succeed())

		By("re-reconciling and observing MetadataInitialized=False/JobFailed and Ready still False")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		initCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)
		Expect(initCond).NotTo(BeNil())
		Expect(initCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(initCond.Reason).To(Equal(reasonMetadataInitJobFailed))
		Expect(apimeta.IsStatusConditionFalse(pulsarCluster.Status.Conditions, conditionTypeReady)).To(BeTrue())

		By("retrying rather than wedging: the failed Job is deleted and recreated fresh on the next reconcile")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		retried := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, jobKey, retried)).To(Succeed())
		Expect(jobFailedPermanently(retried)).To(BeFalse())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		retriedCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)
		Expect(retriedCond).NotTo(BeNil())
		Expect(retriedCond.Reason).To(Equal(reasonMetadataInitJobRunning))
	})

	It("never recreates the metadata-init Job once MetadataInitialized is True, even if the Job is deleted", func() {
		// Regression: bin/pulsar initialize-cluster-metadata is NOT idempotent
		// (it errors if the cluster's metadata already exists). A deleted
		// succeeded Job (admin kubectl delete job, finished-Job TTL/cleanup)
		// must never be recreated once bootstrap has permanently succeeded, or
		// the re-run fails and wedges the cluster Ready=False forever.
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling, marking Oxia Ready, and reconciling again to create the Job")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		jobKey := types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}
		metadataInitJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, jobKey, metadataInitJob)).To(Succeed())

		By("marking the Job Succeeded and reconciling so MetadataInitialized goes True")
		metadataInitJob.Status.Succeeded = 1
		Expect(k8sClient.Status().Update(ctx, metadataInitJob)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		Expect(apimeta.IsStatusConditionTrue(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)).To(BeTrue())

		By("deleting the succeeded Job out from under the operator")
		// Background propagation removes the Job object immediately - envtest
		// runs no garbage-collector controller to clear a foreground-deletion
		// finalizer, so a plain Delete() would otherwise leave it Get-able
		// forever with a DeletionTimestamp set.
		Expect(k8sClient.Delete(ctx, metadataInitJob, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())

		By("re-reconciling: the non-idempotent init Job must NOT be recreated")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		recreated := &batchv1.Job{}
		err = k8sClient.Get(ctx, jobKey, recreated)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("MetadataInitialized stays True, not regressed to JobRunning/JobFailed")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		initCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeMetadataInitialized)
		Expect(initCond).NotTo(BeNil())
		Expect(initCond.Status).To(Equal(metav1.ConditionTrue))
		Expect(initCond.Reason).To(Equal(reasonMetadataInitJobSucceeded))
	})

	It("wires tiered-storage offload config into the Broker child, and leaves it untouched when unconfigured", func() {
		By("reconciling a cluster with s3 offload configured and an explicit offloader-capable image")
		// spec.image must be set explicitly: the PulsarClusterSpec CEL rule
		// rejects offload without one, since the offloader jars ship only in
		// apachepulsar/pulsar-all and that tag isn't guaranteed to be
		// published for every pulsarVersion (see cel_validation_test.go for
		// the rejection itself).
		const offloadImage = "apachepulsar/pulsar-all:" + testPulsarVersion
		threshold := int64(1073741824)
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				PulsarVersion: testPulsarVersion,
				Image:         offloadImage,
				Broker:        &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
				Offload: &clusterv1alpha1.OffloadSpec{
					Driver:                offloadDriverAWSS3,
					Bucket:                testOffloadBucket,
					Region:                testOffloadRegion,
					OffloadThresholdBytes: &threshold,
					CredentialsSecretRef:  &corev1.LocalObjectReference{Name: "offload-creds"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())

		By("using the explicitly-set image as-is (never fabricating a pulsar-all tag) and setting the S3 offload keys")
		Expect(broker.Spec.Image).To(Equal(offloadImage))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("managedLedgerOffloadDriver", "aws-s3"))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("s3ManagedLedgerOffloadBucket", testOffloadBucket))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("s3ManagedLedgerOffloadRegion", testOffloadRegion))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("managedLedgerOffloadAutoTriggerSizeThresholdBytes", "1073741824"))

		By("wiring the credentials secret in as broker env vars (S3 reads them as literal values)")
		envNames := make([]string, 0, len(broker.Spec.Env))
		for _, e := range broker.Spec.Env {
			envNames = append(envNames, e.Name)
			Expect(e.ValueFrom.SecretKeyRef.Name).To(Equal("offload-creds"))
		}
		Expect(envNames).To(ConsistOf("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"))
		Expect(broker.Spec.Volumes).To(BeEmpty())
		Expect(broker.Spec.VolumeMounts).To(BeEmpty())

		By("marking Oxia Ready so the ordered-rollout gate lets the broker's update through")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("removing offload leaves the explicit image untouched (image selection is independent of offload) and drops the offload keys")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.Offload = nil
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		Expect(broker.Spec.Image).To(Equal(offloadImage))
		Expect(broker.Spec.Config).NotTo(HaveKey("managedLedgerOffloadDriver"))
		Expect(broker.Spec.Env).To(BeEmpty())
	})

	It("mounts the GCS service-account key as a file, not an env var", func() {
		By("reconciling a cluster with google-cloud-storage offload configured and an explicit broker-level offloader image")
		// Uses spec.broker.image rather than spec.image (covered by the s3
		// case above) to exercise both explicit-image paths the CEL rule
		// accepts.
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				PulsarVersion: testPulsarVersion,
				Broker:        &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1)), Image: "apachepulsar/pulsar-all:" + testPulsarVersion},
				Offload: &clusterv1alpha1.OffloadSpec{
					Driver:               offloadDriverGCS,
					Bucket:               testOffloadBucket,
					Region:               testOffloadRegion,
					CredentialsSecretRef: &corev1.LocalObjectReference{Name: testGCSCreds},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())

		By("using the explicitly-set broker-level image as-is")
		Expect(broker.Spec.Image).To(Equal("apachepulsar/pulsar-all:" + testPulsarVersion))

		By("setting the GCS offload keys, including the service-account key FILE path")
		Expect(broker.Spec.Config).To(HaveKeyWithValue("managedLedgerOffloadDriver", "google-cloud-storage"))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("gcsManagedLedgerOffloadBucket", testOffloadBucket))
		Expect(broker.Spec.Config).To(HaveKeyWithValue("gcsManagedLedgerOffloadServiceAccountKeyFile", gcsOffloadKeyPath))

		By("mounting the credentials secret as a file and NOT wiring a credential env var")
		Expect(broker.Spec.Env).To(BeEmpty())
		Expect(broker.Spec.Volumes).To(HaveLen(1))
		Expect(broker.Spec.Volumes[0].Secret).NotTo(BeNil())
		Expect(broker.Spec.Volumes[0].Secret.SecretName).To(Equal(testGCSCreds))
		Expect(broker.Spec.VolumeMounts).To(HaveLen(1))
		Expect(broker.Spec.VolumeMounts[0].MountPath).To(Equal(gcsOffloadKeyDir))
		Expect(broker.Spec.VolumeMounts[0].Name).To(Equal(broker.Spec.Volumes[0].Name))
	})

	It("injects clusterName into the Broker and Proxy child config, without overwriting a user-set value", func() {
		// Regression: Pulsar 5.0.0-M1 refuses to start without clusterName -
		// the broker errors "Required clusterName is null" and the proxy
		// "Cluster name cannot be empty" - so the umbrella must inject it the
		// same way it injects metadataStoreUrl.
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{},
				Proxy:  &clusterv1alpha1.ProxySpec{},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("setting clusterName on the Broker child to the PulsarCluster's own name")
		broker := &clusterv1alpha1.Broker{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		Expect(broker.Spec.Config).To(HaveKeyWithValue("clusterName", clusterName))

		By("setting clusterName on the Proxy child too")
		proxy := &clusterv1alpha1.Proxy{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-proxy", Namespace: namespace.Name}, proxy)).To(Succeed())
		Expect(proxy.Spec.Config).To(HaveKeyWithValue("clusterName", clusterName))

		By("marking Oxia Ready so the ordered-rollout gate lets the Broker/Proxy config change through")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("not overwriting a user-set clusterName on either child")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.Broker.Config = map[string]string{"clusterName": "user-broker-cluster"}
		pulsarCluster.Spec.Proxy.Config = map[string]string{"clusterName": "user-proxy-cluster"}
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("applying the change to the Broker first (its upstream tiers have settled)")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}, broker)).To(Succeed())
		Expect(broker.Spec.Config).To(HaveKeyWithValue("clusterName", "user-broker-cluster"))

		By("settling the Broker so the ordered gate lets the downstream Proxy change through too")
		broker.Status.Conditions = []metav1.Condition{readyConditionForGeneration(broker.Generation, "all broker pods ready")}
		Expect(k8sClient.Status().Update(ctx, broker)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-proxy", Namespace: namespace.Name}, proxy)).To(Succeed())
		Expect(proxy.Spec.Config).To(HaveKeyWithValue("clusterName", "user-proxy-cluster"))
	})

	It("injects the BookKeeper metadataServiceUri into the AutoRecovery child, without overwriting a user-set value", func() {
		// Regression: AutoRecovery shares BookKeeper's own metadata store, but
		// unlike BookKeeper/Broker/Proxy/FunctionsWorker the umbrella used to
		// never inject metadataServiceUri into it, leaving a dedicated-mode
		// AutoRecovery daemon with no metadata store to connect to.
		wantURI := "metadata-store:oxia://" + clusterName + "-oxia-oxia:6648/bookkeeper"

		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace.Name,
			},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				BookKeeper:   &clusterv1alpha1.BookKeeperSpec{},
				AutoRecovery: &clusterv1alpha1.AutoRecoverySpec{Mode: "dedicated"},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("setting the AutoRecovery child's metadataServiceUri to the SAME URI as the BookKeeper child")
		bookKeeper := &clusterv1alpha1.BookKeeper{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-bookkeeper", Namespace: namespace.Name}, bookKeeper)).To(Succeed())
		Expect(bookKeeper.Spec.Config).To(HaveKeyWithValue("metadataServiceUri", wantURI))

		autoRecovery := &clusterv1alpha1.AutoRecovery{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-autorecovery", Namespace: namespace.Name}, autoRecovery)).To(Succeed())
		Expect(metav1.IsControlledBy(autoRecovery, pulsarCluster)).To(BeTrue())
		Expect(autoRecovery.Spec.Config).To(HaveKeyWithValue("metadataServiceUri", wantURI))

		By("marking Oxia Ready so the ordered-rollout gate lets the AutoRecovery config change through")
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "all oxia pods ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		By("not overwriting a user-set metadataServiceUri")
		const userURI = "metadata-store:oxia://user-store:6648/bookkeeper"
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.AutoRecovery.Config = map[string]string{"metadataServiceUri": userURI}
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName + "-autorecovery", Namespace: namespace.Name}, autoRecovery)).To(Succeed())
		Expect(autoRecovery.Spec.Config).To(HaveKeyWithValue("metadataServiceUri", userURI))
	})
})

// readyConditionForGeneration builds a Ready=True metav1.Condition observed
// against the given generation, for stamping onto a child's status in tests.
func readyConditionForGeneration(generation int64, message string) metav1.Condition {
	return metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             testReasonAllPodsReady,
		Message:            message,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
	}
}
