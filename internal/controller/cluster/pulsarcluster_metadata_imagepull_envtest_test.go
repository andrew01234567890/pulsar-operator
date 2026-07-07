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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// This spec is the regression coverage for the metadata-init Job silently
// wedging the whole cluster forever when its pod is stuck in
// ImagePullBackOff/ErrImagePull (e.g. a bad/nonexistent image tag): such a
// pod never fails and is never recreated by the kubelet, so it never trips
// the Job's own Failed condition that reconcileMetadataInitJob otherwise
// relies on to know to recreate it (see jobFailedPermanently). Without the
// fix, MetadataInitialized would sit at JobRunning forever with no
// indication anything is wrong, and BookKeeper (gated on it) would never
// come up. envtest runs no kubelet/Job controller, so this manufactures the
// stuck pod directly, exactly as bookkeeper_rack_controller_test.go and
// bookkeeper_autoscaler_controller_test.go already do for bare bookie pods.
var _ = Describe("PulsarCluster metadata-init Job stuck on image pull", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		recorder    *events.FakeRecorder
		clusterName string
		req         reconcile.Request
		fakeNow     time.Time
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-metadata-imagepull-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		recorder = events.NewFakeRecorder(10)
		fakeNow = time.Now()
		reconciler = &PulsarClusterReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
			Now:      func() time.Time { return fakeNow },
		}
		clusterName = "imagepull-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	// drainEvents returns every event buffered on the FakeRecorder so far,
	// clearing the channel (mirrors pulsarcluster_upgrade_envtest_test.go).
	drainEvents := func() []string {
		var out []string
		for {
			select {
			case e := <-recorder.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}
	anyContains := func(evts []string, subs ...string) bool {
		for _, e := range evts {
			all := true
			for _, s := range subs {
				if !strings.Contains(e, s) {
					all = false
					break
				}
			}
			if all {
				return true
			}
		}
		return false
	}

	It("surfaces MetadataInitialized=False/InitImagePullError with a Warning event, leaves the Job alone within the retry interval, and self-heals by recreating it once stuck past the interval", func() {
		const badImage = "apachepulsar/pulsar:this-tag-does-not-exist"

		By("creating a PulsarCluster with Oxia selected and reconciling: the OxiaCluster child is created")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Oxia: &metadatav1alpha1.OxiaClusterSpec{Server: &metadatav1alpha1.OxiaServerSpec{}},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("marking Oxia Ready: the metadata-init Job gets created")
		oxiaKey := types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}
		oxia := &metadatav1alpha1.OxiaCluster{}
		Expect(k8sClient.Get(ctx, oxiaKey, oxia)).To(Succeed())
		oxia.Status.Conditions = []metav1.Condition{readyConditionForGeneration(oxia.Generation, "ready")}
		Expect(k8sClient.Status().Update(ctx, oxia)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		metadataInitKey := types.NamespacedName{Name: clusterName + "-metadata-init", Namespace: namespace.Name}
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, metadataInitKey, job)).To(Succeed())
		firstJobUID := job.UID

		By("simulating the Job's pod stuck in ImagePullBackOff on a bad image tag (as the Job controller would create it, since envtest runs neither a Job controller nor a kubelet)")
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name + "-abcde",
				Namespace: namespace.Name,
				Labels:    builder.SelectorLabels(clusterName, metadataInitComponentName),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       job.Name,
					UID:        job.UID,
					Controller: ptr(true),
				}},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyOnFailure,
				Containers:    []corev1.Container{{Name: metadataInitContainerName, Image: badImage}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  metadataInitContainerName,
			Image: badImage,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  containerWaitingReasonImagePullBackOff,
				Message: "Back-off pulling image",
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		By("reconciling: MetadataInitialized goes False/InitImagePullError naming the image and reason, and fires a Warning event")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		getCluster := func() *clusterv1alpha1.PulsarCluster {
			c := &clusterv1alpha1.PulsarCluster{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, c)).To(Succeed())
			return c
		}

		cond := apimeta.FindStatusCondition(getCluster().Status.Conditions, conditionTypeMetadataInitialized)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonMetadataInitImagePullError))
		Expect(cond.Message).To(ContainSubstring(badImage))
		Expect(cond.Message).To(ContainSubstring(containerWaitingReasonImagePullBackOff))

		fired := drainEvents()
		Expect(anyContains(fired, "Warning", reasonMetadataInitImagePullError)).To(BeTrue(),
			"expected a Warning/%s event, got %v", reasonMetadataInitImagePullError, fired)

		By("re-reconciling immediately: the Job is left untouched (bounded, not a tight delete/recreate loop) since the retry interval hasn't elapsed, and no duplicate event fires")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		stillStuckJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, metadataInitKey, stillStuckJob)).To(Succeed())
		Expect(stillStuckJob.UID).To(Equal(firstJobUID))
		Expect(drainEvents()).To(BeEmpty())

		By("advancing time past metadataInitRetryInterval and reconciling: the stuck Job is deleted")
		fakeNow = fakeNow.Add(metadataInitRetryInterval + time.Second)
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("reconciling once more: a fresh Job is recreated, self-healing without any human intervention")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		recreatedJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, metadataInitKey, recreatedJob)).To(Succeed())
		Expect(recreatedJob.UID).NotTo(Equal(firstJobUID))
	})
})
