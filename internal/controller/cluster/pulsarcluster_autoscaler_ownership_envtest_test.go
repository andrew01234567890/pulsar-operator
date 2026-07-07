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

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// These envtest specs cover the field-ownership boundary between the
// umbrella PulsarCluster reconciler and the broker/bookie autoscaler
// controllers: when a child's autoscaler is enabled, spec.replicas is that
// autoscaler's field to own, and the umbrella must never write or revert it
// (see brokerAutoscalerEnabled/bookKeeperAutoscalerEnabled and the
// onExisting hook wired up in reconcileBroker/reconcileBookKeeper,
// pulsarcluster_controller.go). Neither autoscaler controller is run here;
// each tick is simulated by client-updating the child's spec.replicas
// directly, exactly as BrokerAutoscalerReconciler.applyScale and
// BookKeeperAutoscalerReconciler.scaleUp do. Complements the drift-correction
// coverage in pulsarcluster_upgrade_envtest_test.go, which exercises the same
// umbrella machinery with every autoscaler OFF (the default).
var _ = Describe("PulsarCluster autoscaler replica ownership", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		clusterName string
		req         reconcile.Request
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-autoscaler-ownership-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		reconciler = &PulsarClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		clusterName = "autoscaled-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	getBroker := func() *clusterv1alpha1.Broker {
		b := &clusterv1alpha1.Broker{}
		key := types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}
		Expect(k8sClient.Get(ctx, key, b)).To(Succeed())
		return b
	}
	getBookKeeper := func() *clusterv1alpha1.BookKeeper {
		bk := &clusterv1alpha1.BookKeeper{}
		key := types.NamespacedName{Name: clusterName + "-bookkeeper", Namespace: namespace.Name}
		Expect(k8sClient.Get(ctx, key, bk)).To(Succeed())
		return bk
	}
	getOxia := func() *metadatav1alpha1.OxiaCluster {
		o := &metadatav1alpha1.OxiaCluster{}
		key := types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}
		Expect(k8sClient.Get(ctx, key, o)).To(Succeed())
		return o
	}
	markReady := func(obj client.Object, conditions *[]metav1.Condition, generation int64) {
		*conditions = []metav1.Condition{readyConditionForGeneration(generation, "ready")}
		Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())
	}
	upgrading := func() metav1.Condition {
		cluster := &clusterv1alpha1.PulsarCluster{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, cluster)).To(Succeed())
		c := apimeta.FindStatusCondition(cluster.Status.Conditions, conditionTypeUpgrading)
		Expect(c).NotTo(BeNil())
		return *c
	}

	It("never reverts an autoscaler-set broker replica count, and does not mistake it for an upgrade", func() {
		By("creating a PulsarCluster with the broker autoscaler enabled")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{
					Replicas:   ptr(int32(2)),
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{Enabled: true},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling once: the Broker child is created eagerly at the PulsarCluster's replica count")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Spec.Replicas).To(HaveValue(Equal(int32(2))))

		By("simulating the broker autoscaler scaling the child up, out of band")
		broker := getBroker()
		broker.Spec.Replicas = ptr(int32(5))
		Expect(k8sClient.Update(ctx, broker)).To(Succeed())

		By("reconciling the umbrella again: the autoscaler's replica count must survive, not be reverted")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Spec.Replicas).To(HaveValue(Equal(int32(5))))

		By("the umbrella must not mistake the autoscaler's write for an upgrade")
		cond := upgrading()
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonUpgradeSettled))

		By("a further reconcile is write-free at steady state")
		genBefore := getBroker().Generation
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Generation).To(Equal(genBefore))
		Expect(getBroker().Spec.Replicas).To(HaveValue(Equal(int32(5))))
	})

	It("never reverts an autoscaler-set bookie replica count, and does not mistake it for an upgrade", func() {
		By("creating a PulsarCluster with the bookie autoscaler enabled")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Replicas:   ptr(int32(3)),
					Autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{Enabled: true},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling once: the BookKeeper child is created eagerly at the PulsarCluster's replica count")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBookKeeper().Spec.Replicas).To(HaveValue(Equal(int32(3))))

		By("simulating the bookie autoscaler scaling the child up, out of band")
		bk := getBookKeeper()
		bk.Spec.Replicas = ptr(int32(7))
		Expect(k8sClient.Update(ctx, bk)).To(Succeed())

		By("reconciling the umbrella again: the autoscaler's replica count must survive, not be reverted")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBookKeeper().Spec.Replicas).To(HaveValue(Equal(int32(7))))

		By("the umbrella must not mistake the autoscaler's write for an upgrade")
		cond := upgrading()
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonUpgradeSettled))

		By("a further reconcile is write-free at steady state")
		genBefore := getBookKeeper().Generation
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBookKeeper().Generation).To(Equal(genBefore))
		Expect(getBookKeeper().Spec.Replicas).To(HaveValue(Equal(int32(7))))
	})

	It("rolls a genuine pulsarVersion bump through while preserving the autoscaler's current broker replica count", func() {
		By("creating a PulsarCluster with the broker autoscaler enabled")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				PulsarVersion: "5.0.0-M1",
				Broker: &clusterv1alpha1.BrokerSpec{
					Replicas:   ptr(int32(2)),
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{Enabled: true},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling once: Oxia (mandatory) and Broker are both created")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Spec.Image).To(Equal("apachepulsar/pulsar:5.0.0-M1"))

		By("settling Oxia so Broker's upstream gate is clear and a genuine roll applies immediately")
		oxia := getOxia()
		markReady(oxia, &oxia.Status.Conditions, oxia.Generation)

		By("simulating the broker autoscaler scaling the child up, out of band")
		broker := getBroker()
		broker.Spec.Replicas = ptr(int32(6))
		Expect(k8sClient.Update(ctx, broker)).To(Succeed())

		By("bumping spec.pulsarVersion on the PulsarCluster - a genuine roll")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.PulsarVersion = "5.0.1"
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		By("reconciling: the image rolls to the new version, but replicas is NOT reset")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		rolled := getBroker()
		Expect(rolled.Spec.Image).To(Equal("apachepulsar/pulsar:5.0.1"))
		Expect(rolled.Spec.Replicas).To(HaveValue(Equal(int32(6))),
			"the roll must preserve the autoscaler's current replica count, not reset it to the PulsarCluster's static value")

		cond := upgrading()
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(tierBroker.upgradeReason()))
	})

	It("rolls a genuine pulsarVersion bump through while preserving the autoscaler's current bookie replica count", func() {
		By("creating a PulsarCluster with the bookie autoscaler enabled")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				PulsarVersion: "5.0.0-M1",
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Replicas:   ptr(int32(3)),
					Autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{Enabled: true},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling once: Oxia (mandatory) and BookKeeper are both created")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBookKeeper().Spec.Image).To(Equal("apachepulsar/pulsar:5.0.0-M1"))

		By("settling Oxia so BookKeeper's upstream gate is clear and a genuine roll applies immediately")
		oxia := getOxia()
		markReady(oxia, &oxia.Status.Conditions, oxia.Generation)

		By("simulating the bookie autoscaler scaling the child up, out of band")
		bk := getBookKeeper()
		bk.Spec.Replicas = ptr(int32(7))
		Expect(k8sClient.Update(ctx, bk)).To(Succeed())

		By("bumping spec.pulsarVersion on the PulsarCluster - a genuine roll")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.PulsarVersion = "5.0.1"
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		By("reconciling: the image rolls to the new version, but replicas is NOT reset - a mid-upgrade reset would silently decommission live bookies")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		rolled := getBookKeeper()
		Expect(rolled.Spec.Image).To(Equal("apachepulsar/pulsar:5.0.1"))
		Expect(rolled.Spec.Replicas).To(HaveValue(Equal(int32(7))),
			"the roll must preserve the autoscaler's current replica count, not reset it to the PulsarCluster's static value")

		cond := upgrading()
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(tierBookKeeper.upgradeReason()))
	})

	It("still corrects broker and bookie replica drift when their autoscalers are disabled (regression)", func() {
		By("creating a PulsarCluster with both autoscalers OFF (the default)")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker:     &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(2))},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{Replicas: ptr(int32(3))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("an out-of-band edit to each child's spec.replicas, as would happen without an owning autoscaler")
		broker := getBroker()
		broker.Spec.Replicas = ptr(int32(9))
		Expect(k8sClient.Update(ctx, broker)).To(Succeed())

		bk := getBookKeeper()
		bk.Spec.Replicas = ptr(int32(11))
		Expect(k8sClient.Update(ctx, bk)).To(Succeed())

		By("reconciling: the umbrella still owns replicas on both children and reverts the drift")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Spec.Replicas).To(HaveValue(Equal(int32(2))))
		Expect(getBookKeeper().Spec.Replicas).To(HaveValue(Equal(int32(3))))
	})
})
