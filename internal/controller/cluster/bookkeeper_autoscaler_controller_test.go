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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	bkautoscaler "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/bookkeeper"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// fakeBookieAdminClient is a bkautoscaler.BookieAdminClient test double: every
// bookie address reports the same canned state/info/error, which is enough
// to drive the strict-priority scale-up decision from this controller's
// integration tests without a live bookie ensemble. The pure decision
// algorithm itself (deficit/HWM/clamp/priority) is covered exhaustively in
// internal/autoscaler/bookkeeper's table-driven tests.
type fakeBookieAdminClient struct {
	state    bkautoscaler.BookieState
	info     bkautoscaler.BookieInfo
	stateErr error
	infoErr  error
}

func (f *fakeBookieAdminClient) State(_ context.Context, _ string) (bkautoscaler.BookieState, error) {
	if f.stateErr != nil {
		return bkautoscaler.BookieState{}, f.stateErr
	}
	return f.state, nil
}

func (f *fakeBookieAdminClient) Info(_ context.Context, _ string) (bkautoscaler.BookieInfo, error) {
	if f.infoErr != nil {
		return bkautoscaler.BookieInfo{}, f.infoErr
	}
	return f.info, nil
}

func (f *fakeBookieAdminClient) UnderReplicatedLedgerCount(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func writableClient(usedBytes, totalBytes int64) *fakeBookieAdminClient {
	return &fakeBookieAdminClient{
		state: bkautoscaler.BookieState{Running: true},
		info:  bkautoscaler.BookieInfo{LedgerDisks: []bkautoscaler.BookieDiskUsage{{UsedBytes: usedBytes, TotalBytes: totalBytes}}},
	}
}

// createBookiePods creates total bare Pods matching bk's bookie selector
// labels. The first addressable pods get a distinct PodIP (set directly via
// the status subresource, since no scheduler/kubelet runs against envtest);
// the remaining total-addressable pods are left without an IP, modelling a
// pod mid-recreate that is not yet reachable. addressable == total is the
// normal "every bookie is up and addressable" case.
func createBookiePods(ctx context.Context, namespace, bkName string, total, addressable int) {
	for i := range total {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-bookie-%d", bkName, i),
				Namespace: namespace,
				Labels:    builder.SelectorLabels(bkName, bookkeeperComponent),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: bookieContainerName, Image: "busybox"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		if i < addressable {
			pod.Status.PodIP = fmt.Sprintf("10.0.0.%d", i+1)
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
	}
}

func deleteBookiePods(ctx context.Context, namespace, bkName string, total int) {
	for i := range total {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-bookie-%d", bkName, i), Namespace: namespace}}
		_ = k8sClient.Delete(ctx, pod)
	}
}

// markStabilized sets the BookKeeper status fields the autoscaler reads to
// decide every pod is Ready and fully rolled out, bypassing the need to run
// BookKeeperReconciler (a different controller, not under test here).
func markStabilized(ctx context.Context, bk *clusterv1alpha1.BookKeeper, replicas int32, lastScaleTime *metav1.Time) {
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
	bk.Status.Replicas = replicas
	bk.Status.ReadyReplicas = replicas
	bk.Status.LastScaleTime = lastScaleTime
	apimeta.SetStatusCondition(&bk.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: bk.Generation,
		Reason:             reasonAllReady,
		Message:            "all replicas ready",
	})
	Expect(k8sClient.Status().Update(ctx, bk)).To(Succeed())
}

var _ = Describe("BookKeeper Autoscaler Controller", func() {
	const namespace = "default"
	ctx := context.Background()

	createBookKeeper := func(name string, replicas int32, autoscaler *clusterv1alpha1.BookKeeperAutoscalerSpec, ensemble *clusterv1alpha1.BookKeeperEnsembleSpec) *clusterv1alpha1.BookKeeper {
		bk := &clusterv1alpha1.BookKeeper{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: clusterv1alpha1.BookKeeperSpec{
				Replicas:   int32Ptr(replicas),
				Autoscaler: autoscaler,
				Ensemble:   ensemble,
			},
		}
		Expect(k8sClient.Create(ctx, bk)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, bk)).To(Succeed())
		})
		return bk
	}

	It("does nothing when the autoscaler is disabled", func() {
		bk := createBookKeeper("autoscaler-disabled", 3, nil, nil)

		reconciler := &BookKeeperAutoscalerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(*bk.Spec.Replicas).To(Equal(int32(3)))
		Expect(findCondition(bk.Status.Conditions, conditionTypeAutoscaling)).To(BeNil())
	})

	It("blocks scaling while pods are not all ready", func() {
		bk := createBookKeeper("autoscaler-not-ready", 3, &clusterv1alpha1.BookKeeperAutoscalerSpec{
			Enabled: true, MinWritableBookies: int32Ptr(3), ScaleUpBy: int32Ptr(1), ScaleUpMaxLimit: int32Ptr(10),
		}, nil)

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		bk.Status.Replicas = 3
		bk.Status.ReadyReplicas = 2 // one pod not yet ready
		Expect(k8sClient.Status().Update(ctx, bk)).To(Succeed())

		reconciler := &BookKeeperAutoscalerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(10 * time.Second))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(*bk.Spec.Replicas).To(Equal(int32(3)))
	})

	It("blocks scaling until the stabilization window has elapsed since the last scale", func() {
		bk := createBookKeeper("autoscaler-cooldown", 3, &clusterv1alpha1.BookKeeperAutoscalerSpec{
			Enabled: true, MinWritableBookies: int32Ptr(3), ScaleUpBy: int32Ptr(1), ScaleUpMaxLimit: int32Ptr(10),
			StabilizationWindowSeconds: int32Ptr(300),
		}, nil)

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		recentScale := metav1.NewTime(fixedNow.Add(-1 * time.Second))
		markStabilized(ctx, bk, 3, &recentScale)

		reconciler := &BookKeeperAutoscalerReconciler{
			Client:      k8sClient,
			Scheme:      k8sClient.Scheme(),
			AdminClient: writableClient(0, 100), // would be a no-op deficit-wise even if reached
			Now:         func() time.Time { return fixedNow },
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(*bk.Spec.Replicas).To(Equal(int32(3)))
		Expect(bk.Status.LastScaleTime.Time).To(BeTemporally("==", recentScale.Time))
	})

	It("scales up on a writable-bookie deficit once stabilized, recording status and an Event", func() {
		bk := createBookKeeper("autoscaler-deficit", 2, &clusterv1alpha1.BookKeeperAutoscalerSpec{
			Enabled: true, MinWritableBookies: int32Ptr(4), ScaleUpBy: int32Ptr(1), ScaleUpMaxLimit: int32Ptr(10),
		}, nil)
		markStabilized(ctx, bk, 2, nil)
		createBookiePods(ctx, namespace, bk.Name, 2, 2)
		DeferCleanup(func() { deleteBookiePods(ctx, namespace, bk.Name, 2) })

		recorder := record.NewFakeRecorder(5)
		reconciler := &BookKeeperAutoscalerReconciler{
			Client:      k8sClient,
			Scheme:      k8sClient.Scheme(),
			Recorder:    recorder,
			AdminClient: writableClient(10, 100), // both bookies writable, 10% used - well under HWM
		}
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(10 * time.Second))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		// 2 writable observed, minWritableBookies=4 -> deficit 2 -> 2+2=4.
		Expect(*bk.Spec.Replicas).To(Equal(int32(4)))
		Expect(bk.Status.WritableBookies).To(Equal(int32(2)))
		Expect(bk.Status.LastScaleTime).NotTo(BeNil())

		cond := findCondition(bk.Status.Conditions, conditionTypeAutoscaling)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(bkautoscaler.ReasonWritableBookieDeficit))

		Eventually(recorder.Events).Should(Receive(ContainSubstring("WritableBookieDeficit")))
	})

	It("skips the tick without scaling when a bookie is not yet addressable", func() {
		// A minWritableBookies of 4 against only 2 addressable (writable)
		// pods would fire the deficit branch and permanently scale up if the
		// incomplete ensemble were evaluated. The completeness guard must
		// skip the tick instead: status is (stale) all-ready, but the third
		// pod has no PodIP yet.
		bk := createBookKeeper("autoscaler-partial-pods", 3, &clusterv1alpha1.BookKeeperAutoscalerSpec{
			Enabled: true, MinWritableBookies: int32Ptr(4), ScaleUpBy: int32Ptr(1), ScaleUpMaxLimit: int32Ptr(10),
		}, nil)
		markStabilized(ctx, bk, 3, nil)
		createBookiePods(ctx, namespace, bk.Name, 3, 2) // 3 desired, only 2 addressable
		DeferCleanup(func() { deleteBookiePods(ctx, namespace, bk.Name, 3) })

		reconciler := &BookKeeperAutoscalerReconciler{
			Client:      k8sClient,
			Scheme:      k8sClient.Scheme(),
			AdminClient: writableClient(10, 100),
		}
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(10 * time.Second))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(*bk.Spec.Replicas).To(Equal(int32(3)))
		Expect(findCondition(bk.Status.Conditions, conditionTypeAutoscaling)).To(BeNil())
	})

	It("does not scale up when a bookie returns a malformed disk-info reading", func() {
		bk := createBookKeeper("autoscaler-malformed-info", 3, &clusterv1alpha1.BookKeeperAutoscalerSpec{
			Enabled: true, MinWritableBookies: int32Ptr(3), ScaleUpBy: int32Ptr(1), ScaleUpMaxLimit: int32Ptr(10),
		}, nil)
		markStabilized(ctx, bk, 3, nil)
		createBookiePods(ctx, namespace, bk.Name, 3, 3)
		DeferCleanup(func() { deleteBookiePods(ctx, namespace, bk.Name, 3) })

		recorder := record.NewFakeRecorder(5)
		reconciler := &BookKeeperAutoscalerReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
			AdminClient: &fakeBookieAdminClient{
				state:   bkautoscaler.BookieState{Running: true},
				infoErr: errors.New("bookie reported non-positive totalSpace 0 from /api/v1/bookie/info"),
			},
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(*bk.Spec.Replicas).To(Equal(int32(3)))
		Expect(findCondition(bk.Status.Conditions, conditionTypeAutoscaling)).To(BeNil())
		Eventually(recorder.Events).Should(Receive(ContainSubstring(reasonBookieAdminPollFailed)))
	})

	It("flags an invalid configuration instead of scaling when minWritableBookies is below the ensemble size", func() {
		bk := createBookKeeper("autoscaler-invalid-config", 3, &clusterv1alpha1.BookKeeperAutoscalerSpec{
			Enabled: true, MinWritableBookies: int32Ptr(2), ScaleUpBy: int32Ptr(1), ScaleUpMaxLimit: int32Ptr(10),
		}, &clusterv1alpha1.BookKeeperEnsembleSpec{EnsembleSize: int32Ptr(5)})

		recorder := record.NewFakeRecorder(5)
		reconciler := &BookKeeperAutoscalerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(*bk.Spec.Replicas).To(Equal(int32(3)))

		cond := findCondition(bk.Status.Conditions, conditionTypeAutoscaling)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonInvalidAutoscalerConfig))

		Eventually(recorder.Events).Should(Receive(ContainSubstring("InvalidAutoscalerConfig")))
	})
})
