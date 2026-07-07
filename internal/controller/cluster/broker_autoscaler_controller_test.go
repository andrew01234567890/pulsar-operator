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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	autoscalerbroker "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/broker"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
)

// mockLoadClient is a fully-controlled autoscalerbroker.LoadClient, letting
// these tests drive BrokerAutoscalerReconciler's real Reconcile path -
// pod listing, stabilization gating, status/condition writes - without a
// real broker HTTP endpoint or metrics-server.
type mockLoadClient struct {
	percentByPod map[string]int32
	err          error
}

func (m *mockLoadClient) CPUPercentByBroker(context.Context, []corev1.Pod) (map[string]int32, error) {
	return m.percentByPod, m.err
}

var _ = Describe("BrokerAutoscaler Controller", func() {
	ctx := context.Background()
	const resourceNamespace = "default"

	newReconciler := func(load autoscalerbroker.LoadClient) *BrokerAutoscalerReconciler {
		return &BrokerAutoscalerReconciler{
			Client:     k8sClient,
			Recorder:   events.NewFakeRecorder(10),
			LoadClient: load,
		}
	}

	// createReadyBrokerPods simulates a fully converged broker StatefulSet:
	// envtest runs no kubelet/scheduler, so nothing else will create these
	// pods or flip them Ready.
	createReadyBrokerPods := func(brokerName string, count int) []string {
		names := make([]string, 0, count)
		for i := range count {
			name := fmt.Sprintf("%s-%d", brokerName, i)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: resourceNamespace,
					Labels:    builder.SelectorLabels(brokerName, brokerComponent),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "broker", Image: "example/broker"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
			names = append(names, name)
		}
		return names
	}

	deleteBrokerPods := func(names []string) {
		for _, name := range names {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace}}
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		}
	}

	cpuReadings := func(names []string, percents ...int32) map[string]int32 {
		readings := make(map[string]int32, len(names))
		for i, name := range names {
			readings[name] = percents[i]
		}
		return readings
	}

	Context("when the autoscaler is disabled", func() {
		const resourceName = "autoscaler-disabled"
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		BeforeEach(func() {
			resource := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.BrokerSpec{
					Replicas:   int32Ptr(3),
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{Enabled: false},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("never scales and reports the Autoscaling condition as Disabled", func() {
			reconciler := newReconciler(&mockLoadClient{err: fmt.Errorf("must not be called when disabled")})

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			broker := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())
			Expect(*broker.Spec.Replicas).To(Equal(int32(3)))

			cond := apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeAutoscaling)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonAutoscalerDisabled))
		})
	})

	Context("when broker pods are not all Ready", func() {
		const resourceName = "autoscaler-pods-not-ready"
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
		var podNames []string

		BeforeEach(func() {
			resource := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.BrokerSpec{
					Replicas:   int32Ptr(2),
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{Enabled: true},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			// Only one of the two expected pods exists (simulates a
			// rollout still converging), so PodsStable must fail.
			podNames = createReadyBrokerPods(resourceName, 1)
		})

		AfterEach(func() {
			deleteBrokerPods(podNames)
			resource := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("does not scale and reports PodsNotReady", func() {
			reconciler := newReconciler(&mockLoadClient{err: fmt.Errorf("must not be called before pods are stable")})

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			broker := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())
			Expect(*broker.Spec.Replicas).To(Equal(int32(2)))

			cond := apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeAutoscaling)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal(reasonPodsNotReady))
		})
	})

	Context("when the stabilization window has not elapsed since the last scale", func() {
		const resourceName = "autoscaler-stabilizing"
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
		var podNames []string

		BeforeEach(func() {
			resource := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.BrokerSpec{
					Replicas:   int32Ptr(3),
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{Enabled: true},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			podNames = createReadyBrokerPods(resourceName, 3)

			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			recent := metav1.NewTime(time.Now().Add(-1 * time.Second))
			resource.Status.LastScaleTime = &recent
			Expect(k8sClient.Status().Update(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			deleteBrokerPods(podNames)
			resource := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("blocks the decision and reports AwaitingStabilization", func() {
			reconciler := newReconciler(&mockLoadClient{err: fmt.Errorf("must not be called during the stabilization window")})

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			broker := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())
			Expect(*broker.Spec.Replicas).To(Equal(int32(3)))

			cond := apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeAutoscaling)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal(reasonAwaitingStabilization))
		})
	})

	// scenario is a table-driven envtest case exercising the full unanimous
	// vote through Reconcile with a mock LoadClient: the decision-engine
	// math itself is already covered exhaustively (and much faster) by
	// internal/autoscaler/broker's pure unit tests, so this table only has
	// to prove the controller wires pods -> CPU readings -> Decide ->
	// spec.replicas/status correctly for each broad case.
	type scenario struct {
		description     string
		replicas        int32
		min             *int32
		max             *int32
		readingsPercent []int32
		wantReplicas    int32
		wantReason      string
		wantConditionOK bool // true => condition Status=True (a scale happened)
	}

	scenarios := []scenario{
		{
			description:     "all brokers above the higher threshold scale up",
			replicas:        3,
			readingsPercent: []int32{90, 95, 99},
			wantReplicas:    4,
			wantReason:      string(autoscalerbroker.ScaleUp),
			wantConditionOK: true,
		},
		{
			description:     "all brokers below the lower threshold scale down",
			replicas:        3,
			readingsPercent: []int32{5, 10, 15},
			wantReplicas:    2, // default min when unset
			wantReason:      string(autoscalerbroker.ScaleDown),
			wantConditionOK: true,
		},
		{
			description:     "mixed hot and cold brokers is a no-op",
			replicas:        3,
			readingsPercent: []int32{95, 10, 50},
			wantReplicas:    3,
			wantReason:      autoscalerbroker.ReasonMixedSignals,
			wantConditionOK: false,
		},
		{
			description:     "a single hot broker among normal brokers never scales alone",
			replicas:        3,
			readingsPercent: []int32{95, 50, 55},
			wantReplicas:    3,
			wantReason:      autoscalerbroker.ReasonMixedSignals,
			wantConditionOK: false,
		},
		{
			description:     "scale-up clamps to max and no-ops once already there",
			replicas:        5,
			max:             int32Ptr(5),
			readingsPercent: []int32{95, 95, 95, 95, 95},
			wantReplicas:    5,
			wantReason:      autoscalerbroker.ReasonAtReplicaBound,
			wantConditionOK: false,
		},
	}

	for i, s := range scenarios {
		resourceName := fmt.Sprintf("autoscaler-scenario-%d", i)

		Context(s.description, func() {
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
			var podNames []string

			BeforeEach(func() {
				resource := &clusterv1alpha1.Broker{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
					Spec: clusterv1alpha1.BrokerSpec{
						Replicas: int32Ptr(s.replicas),
						Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{
							Enabled: true,
							Min:     s.min,
							Max:     s.max,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
				podNames = createReadyBrokerPods(resourceName, int(s.replicas))
			})

			AfterEach(func() {
				deleteBrokerPods(podNames)
				resource := &clusterv1alpha1.Broker{}
				Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			})

			It("reaches the expected replica count and Autoscaling condition", func() {
				reconciler := newReconciler(&mockLoadClient{percentByPod: cpuReadings(podNames, s.readingsPercent...)})

				result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(BeNumerically(">", 0))

				broker := &clusterv1alpha1.Broker{}
				Expect(k8sClient.Get(ctx, typeNamespacedName, broker)).To(Succeed())
				Expect(*broker.Spec.Replicas).To(Equal(s.wantReplicas))

				cond := apimeta.FindStatusCondition(broker.Status.Conditions, conditionTypeAutoscaling)
				Expect(cond).NotTo(BeNil())
				Expect(cond.Reason).To(Equal(s.wantReason))
				if s.wantConditionOK {
					Expect(cond.Status).To(Equal(metav1.ConditionTrue))
					Expect(broker.Status.LastScaleTime).NotTo(BeNil())
				} else {
					Expect(cond.Status).To(Equal(metav1.ConditionFalse))
					Expect(broker.Status.LastScaleTime).To(BeNil())
				}
			})
		})
	}
})
