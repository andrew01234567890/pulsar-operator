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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// testBookiePodImage is a placeholder image for bare Pods this file creates
// directly (envtest runs no kubelet, so nothing ever actually runs it).
const testBookiePodImage = "busybox"

// fakeRackSetterCall records one SetBookieRack invocation.
type fakeRackSetterCall struct {
	bookieID string
	rack     string
}

// fakeRackSetter is a rackawareness.RackSetter test double recording every
// call it receives, so these integration tests can assert exactly which
// bookies were (and were not) touched without a live cluster. The pure
// diff-only apply logic itself is covered exhaustively by
// bookkeeper_rack_test.go's table-driven tests against applyRackMapping
// directly.
type fakeRackSetter struct {
	calls []fakeRackSetterCall
}

func (f *fakeRackSetter) SetBookieRack(_ context.Context, bookieID, rack string) error {
	f.calls = append(f.calls, fakeRackSetterCall{bookieID: bookieID, rack: rack})
	return nil
}

func createZoneNode(ctx context.Context, name, zone string) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{builder.ZoneTopologyKey: zone},
		},
	}
	Expect(k8sClient.Create(ctx, node)).To(Succeed())
}

func deleteNode(ctx context.Context, name string) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	_ = k8sClient.Delete(ctx, node)
}

// createScheduledBookiePod creates a bare bookie Pod already bound to
// nodeName (as if the scheduler had already placed it, which envtest itself
// never does), matching bk's bookie selector labels.
func createScheduledBookiePod(ctx context.Context, namespace, bkName string, ordinal int, nodeName string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-bookie-%d", bkName, ordinal),
			Namespace: namespace,
			Labels:    builder.SelectorLabels(bkName, bookkeeperComponent),
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: bookieContainerName, Image: testBookiePodImage}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
}

// deleteRackSyncBookiePods removes the bare bookie Pods this file's tests
// create directly (a local helper, rather than reusing the autoscaler
// tests' deleteBookiePods, since every call site here shares the same
// namespace).
func deleteRackSyncBookiePods(ctx context.Context, bkName string, total int) {
	for i := range total {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-bookie-%d", bkName, i), Namespace: "default"}}
		_ = k8sClient.Delete(ctx, pod)
	}
}

var _ = Describe("BookKeeper Rack Controller", func() {
	const namespace = "default"
	ctx := context.Background()

	createRackBookKeeper := func(name string, autoRack *clusterv1alpha1.BookKeeperAutoRackConfig) *clusterv1alpha1.BookKeeper {
		bk := &clusterv1alpha1.BookKeeper{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       clusterv1alpha1.BookKeeperSpec{AutoRackConfig: autoRack},
		}
		Expect(k8sClient.Create(ctx, bk)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, bk)).To(Succeed())
		})
		return bk
	}

	It("does nothing when autoRackConfig is unset", func() {
		bk := createRackBookKeeper("rack-disabled-nil", nil)

		setter := &fakeRackSetter{}
		reconciler := &BookKeeperRackReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RackSetter: setter}
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		Expect(setter.calls).To(BeEmpty())
	})

	It("does nothing when autoRackConfig.enabled is false", func() {
		bk := createRackBookKeeper("rack-disabled-false", &clusterv1alpha1.BookKeeperAutoRackConfig{Enabled: false})

		setter := &fakeRackSetter{}
		reconciler := &BookKeeperRackReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RackSetter: setter}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(setter.calls).To(BeEmpty())
	})

	It("derives each bookie's rack from its node's zone label, applies only what changed, and is idempotent on the next tick", func() {
		nodeA := "rack-sync-node-a"
		nodeB := "rack-sync-node-b"
		createZoneNode(ctx, nodeA, testZoneA)
		createZoneNode(ctx, nodeB, testZoneB)
		DeferCleanup(func() {
			deleteNode(ctx, nodeA)
			deleteNode(ctx, nodeB)
		})

		bk := createRackBookKeeper("rack-sync-basic", &clusterv1alpha1.BookKeeperAutoRackConfig{Enabled: true, PeriodSeconds: int32Ptr(45)})

		createScheduledBookiePod(ctx, namespace, bk.Name, 0, nodeA)
		createScheduledBookiePod(ctx, namespace, bk.Name, 1, nodeB)
		DeferCleanup(func() { deleteRackSyncBookiePods(ctx, bk.Name, 2) })

		bookie0ID := bookieID(bk, fmt.Sprintf("%s-bookie-0", bk.Name))
		bookie1ID := bookieID(bk, fmt.Sprintf("%s-bookie-1", bk.Name))

		// bookie-0 already has the correct rack recorded from a prior tick;
		// only bookie-1 should actually be applied this time.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		bk.Status.BookieRacks = map[string]string{bookie0ID: testRackA}
		Expect(k8sClient.Status().Update(ctx, bk)).To(Succeed())

		setter := &fakeRackSetter{}
		recorder := events.NewFakeRecorder(5)
		reconciler := &BookKeeperRackReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RackSetter: setter, Recorder: recorder}

		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(45 * time.Second))

		Expect(setter.calls).To(HaveLen(1), "bookie-0 is already correct and must be skipped")
		Expect(setter.calls[0].bookieID).To(Equal(bookie1ID))
		Expect(setter.calls[0].rack).To(Equal(testRackB))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		Expect(bk.Status.BookieRacks).To(HaveKeyWithValue(bookie0ID, testRackA))
		Expect(bk.Status.BookieRacks).To(HaveKeyWithValue(bookie1ID, testRackB))

		cond := findCondition(bk.Status.Conditions, conditionTypeRackSync)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(reasonRackSynced))
		Eventually(recorder.Events).Should(Receive(ContainSubstring("RackSynced")))

		// Second tick: every bookie is now recorded with its correct rack,
		// so no further SetBookieRack calls should happen at all.
		result2, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(result2.RequeueAfter).To(Equal(45 * time.Second))
		Expect(setter.calls).To(HaveLen(1), "no new SetBookieRack calls once every bookie is already synced")
	})

	It("skips a bookie pod that has not been scheduled yet and records it as skipped", func() {
		nodeC := "rack-sync-node-c"
		createZoneNode(ctx, nodeC, "us-east-1c")
		DeferCleanup(func() { deleteNode(ctx, nodeC) })

		bk := createRackBookKeeper("rack-sync-unscheduled", &clusterv1alpha1.BookKeeperAutoRackConfig{Enabled: true})

		createScheduledBookiePod(ctx, namespace, bk.Name, 0, nodeC)
		// bookie-1 has no NodeName yet - not scheduled, so it must be
		// skipped rather than failing the whole tick.
		unscheduled := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-bookie-1", bk.Name),
				Namespace: namespace,
				Labels:    builder.SelectorLabels(bk.Name, bookkeeperComponent),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: bookieContainerName, Image: testBookiePodImage}}},
		}
		Expect(k8sClient.Create(ctx, unscheduled)).To(Succeed())
		DeferCleanup(func() { deleteRackSyncBookiePods(ctx, bk.Name, 2) })

		setter := &fakeRackSetter{}
		reconciler := &BookKeeperRackReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), RackSetter: setter}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())

		Expect(setter.calls).To(HaveLen(1))
		Expect(setter.calls[0].bookieID).To(Equal(bookieID(bk, fmt.Sprintf("%s-bookie-0", bk.Name))))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		cond := findCondition(bk.Status.Conditions, conditionTypeRackSync)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Message).To(ContainSubstring("1 skipped"))
	})

	It("surfaces a Warning condition/Event and skips applying when no RackSetter can be resolved", func() {
		bk := createRackBookKeeper("rack-sync-no-setter", &clusterv1alpha1.BookKeeperAutoRackConfig{Enabled: true})

		recorder := events.NewFakeRecorder(5)
		// No RackSetter injected and no RESTConfig/ClientSet wired: the
		// default pod-exec RackSetter can never be constructed.
		reconciler := &BookKeeperRackReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: bk.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(bk), bk)).To(Succeed())
		cond := findCondition(bk.Status.Conditions, conditionTypeRackSync)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonRackSyncUnavailable))
		Expect(bk.Status.BookieRacks).To(BeEmpty())

		Eventually(recorder.Events).Should(Receive(ContainSubstring(reasonRackSyncUnavailable)))
	})
})
