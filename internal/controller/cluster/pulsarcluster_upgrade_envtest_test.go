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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// These envtest specs exercise the ordered rolling-upgrade state machine end
// to end: every tier is created and settled once, spec.pulsarVersion is
// bumped (which cascades into every component's image, since none set an
// explicit image of their own), and each tier is unblocked one at a time in
// dependency order - OxiaCluster -> BookKeeper/AutoRecovery -> Broker ->
// Proxy -> FunctionsWorker - proving a downstream tier's spec update stays
// deferred (its child untouched) until every upstream tier has settled, and
// that the Upgrading status condition names whichever tier is currently the
// bottleneck. Complements the pure-function unit tests in
// pulsarcluster_upgrade_test.go.
var _ = Describe("PulsarCluster ordered rolling upgrade", func() {
	var (
		namespace   *corev1.Namespace
		reconciler  *PulsarClusterReconciler
		recorder    *events.FakeRecorder
		clusterName string
		req         reconcile.Request
	)

	BeforeEach(func() {
		namespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "pulsarcluster-upgrade-test-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

		recorder = events.NewFakeRecorder(100)
		reconciler = &PulsarClusterReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}
		clusterName = "upgrade-cluster"
		req = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace.Name},
		}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, namespace)).To(Succeed())
	})

	// drainEvents returns every event buffered on the FakeRecorder so far,
	// clearing the channel. Each entry is "<type> <reason> <note>".
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
	anyContains := func(events []string, subs ...string) bool {
		for _, e := range events {
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

	markReady := func(obj client.Object, generation int64) {
		var conditions *[]metav1.Condition
		switch o := obj.(type) {
		case *metadatav1alpha1.OxiaCluster:
			conditions = &o.Status.Conditions
		case *clusterv1alpha1.BookKeeper:
			conditions = &o.Status.Conditions
		case *clusterv1alpha1.AutoRecovery:
			conditions = &o.Status.Conditions
		case *clusterv1alpha1.Broker:
			conditions = &o.Status.Conditions
		case *clusterv1alpha1.Proxy:
			conditions = &o.Status.Conditions
		case *clusterv1alpha1.FunctionsWorker:
			conditions = &o.Status.Conditions
		}
		*conditions = []metav1.Condition{readyConditionForGeneration(generation, "ready")}
		Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())
	}

	It("rolls a spec change out tier by tier, deferring each downstream tier until its upstream settles", func() {
		const versionBefore = "5.0.0-M1"
		const versionAfter = "5.0.1"
		// Oxia takes its own image, decoupled from pulsarVersion, so the roll is
		// driven by an explicit oxia image change alongside the pulsarVersion bump.
		const oxiaImageBefore = "streamnative/oxia:1.0"
		const oxiaImageAfter = "streamnative/oxia:2.0"
		imageFor := func(version string) string { return "apachepulsar/pulsar:" + version }

		By("creating a cluster with every ordered-rollout tier configured (including AutoRecovery)")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				PulsarVersion:   versionBefore,
				Oxia:            &metadatav1alpha1.OxiaClusterSpec{Server: &metadatav1alpha1.OxiaServerSpec{Image: oxiaImageBefore}},
				Broker:          &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
				BookKeeper:      &clusterv1alpha1.BookKeeperSpec{Volumes: &clusterv1alpha1.BookKeeperVolumes{Journal: &clusterv1alpha1.VolumeSpec{}}},
				AutoRecovery:    &clusterv1alpha1.AutoRecoverySpec{Replicas: ptr(int32(1))},
				Proxy:           &clusterv1alpha1.ProxySpec{},
				FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		By("reconciling once: every child is missing, so all are created eagerly with no gating")
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		oxiaKey := types.NamespacedName{Name: clusterName + "-oxia", Namespace: namespace.Name}
		bookKeeperKey := types.NamespacedName{Name: clusterName + "-bookkeeper", Namespace: namespace.Name}
		autoRecoveryKey := types.NamespacedName{Name: clusterName + "-autorecovery", Namespace: namespace.Name}
		brokerKey := types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}
		proxyKey := types.NamespacedName{Name: clusterName + "-proxy", Namespace: namespace.Name}
		functionsWorkerKey := types.NamespacedName{Name: clusterName + "-functionsworker", Namespace: namespace.Name}

		getOxia := func() *metadatav1alpha1.OxiaCluster {
			o := &metadatav1alpha1.OxiaCluster{}
			Expect(k8sClient.Get(ctx, oxiaKey, o)).To(Succeed())
			return o
		}
		getBookKeeper := func() *clusterv1alpha1.BookKeeper {
			bk := &clusterv1alpha1.BookKeeper{}
			Expect(k8sClient.Get(ctx, bookKeeperKey, bk)).To(Succeed())
			return bk
		}
		getAutoRecovery := func() *clusterv1alpha1.AutoRecovery {
			ar := &clusterv1alpha1.AutoRecovery{}
			Expect(k8sClient.Get(ctx, autoRecoveryKey, ar)).To(Succeed())
			return ar
		}
		getBroker := func() *clusterv1alpha1.Broker {
			b := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, brokerKey, b)).To(Succeed())
			return b
		}
		getProxy := func() *clusterv1alpha1.Proxy {
			p := &clusterv1alpha1.Proxy{}
			Expect(k8sClient.Get(ctx, proxyKey, p)).To(Succeed())
			return p
		}
		getFunctionsWorker := func() *clusterv1alpha1.FunctionsWorker {
			f := &clusterv1alpha1.FunctionsWorker{}
			Expect(k8sClient.Get(ctx, functionsWorkerKey, f)).To(Succeed())
			return f
		}
		upgradingReason := func() (metav1.ConditionStatus, string) {
			Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
			c := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeUpgrading)
			Expect(c).NotTo(BeNil())
			return c.Status, c.Reason
		}

		By("every child starts on the pre-bump image")
		Expect(getOxia().Spec.Server.Image).To(Equal(oxiaImageBefore))
		Expect(getBookKeeper().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getAutoRecovery().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getBroker().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		By("F1: a fresh install whose children are not yet Ready reports Upgrading=False/Settled, NOT rolling")
		status, reason := upgradingReason()
		Expect(status).To(Equal(metav1.ConditionFalse))
		Expect(reason).To(Equal(reasonUpgradeSettled))
		Expect(drainEvents()).To(BeEmpty(), "creating children eagerly emits no rollout events")

		By("marking every tier Ready for its current (creation) generation")
		oxia, bookKeeper, autoRecovery := getOxia(), getBookKeeper(), getAutoRecovery()
		broker, proxy, functionsWorker := getBroker(), getProxy(), getFunctionsWorker()
		markReady(oxia, oxia.Generation)
		markReady(bookKeeper, bookKeeper.Generation)
		markReady(autoRecovery, autoRecovery.Generation)
		markReady(broker, broker.Generation)
		markReady(proxy, proxy.Generation)
		markReady(functionsWorker, functionsWorker.Generation)

		By("reconciling at steady state: nothing to roll, no requeue churn, Upgrading settles")
		result, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		status, reason = upgradingReason()
		Expect(status).To(Equal(metav1.ConditionFalse))
		Expect(reason).To(Equal(reasonUpgradeSettled))

		By("reconciling again with no spec change updates nothing (idempotent steady state)")
		oxiaGenBefore, bkGenBefore, arGenBefore := getOxia().Generation, getBookKeeper().Generation, getAutoRecovery().Generation
		brokerGenBefore, proxyGenBefore, fwGenBefore := getBroker().Generation, getProxy().Generation, getFunctionsWorker().Generation

		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		Expect(getOxia().Generation).To(Equal(oxiaGenBefore))
		Expect(getBookKeeper().Generation).To(Equal(bkGenBefore))
		Expect(getAutoRecovery().Generation).To(Equal(arGenBefore))
		Expect(getBroker().Generation).To(Equal(brokerGenBefore))
		Expect(getProxy().Generation).To(Equal(proxyGenBefore))
		Expect(getFunctionsWorker().Generation).To(Equal(fwGenBefore))
		Expect(drainEvents()).To(BeEmpty(), "steady state emits no rollout events")

		By("bumping the Oxia image and spec.pulsarVersion: the version cascades into every pulsar component's default image")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		pulsarCluster.Spec.PulsarVersion = versionAfter
		pulsarCluster.Spec.Oxia.Server.Image = oxiaImageAfter
		Expect(k8sClient.Update(ctx, pulsarCluster)).To(Succeed())

		By("reconciling: only Oxia (no upstream tier) rolls; every downstream tier is deferred, untouched")
		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(rolloutRequeueInterval))

		Expect(getOxia().Spec.Server.Image).To(Equal(oxiaImageAfter))
		Expect(getBookKeeper().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getAutoRecovery().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getBroker().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		status, reason = upgradingReason()
		Expect(status).To(Equal(metav1.ConditionTrue))
		Expect(reason).To(Equal(tierOxia.upgradeReason()))

		By("F4: Oxia's write emits a RolloutStarted event")
		Expect(anyContains(drainEvents(), "RolloutStarted", string(tierOxia))).To(BeTrue())

		By("F4: requeuing while Oxia is not yet Ready makes BookKeeper the deferred bottleneck and emits RolloutDeferred")
		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(rolloutRequeueInterval))
		status, reason = upgradingReason()
		Expect(status).To(Equal(metav1.ConditionTrue))
		Expect(reason).To(Equal(tierBookKeeper.upgradeReason()))
		deferredEvents := drainEvents()
		Expect(anyContains(deferredEvents, "RolloutDeferred", string(tierBookKeeper))).To(BeTrue())
		Expect(anyContains(deferredEvents, "RolloutStarted")).To(BeFalse(), "nothing applied on a pure requeue")

		By("F4: a second identical requeue does NOT re-emit the deferred event (dedup on unchanged bottleneck)")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(anyContains(drainEvents(), "RolloutDeferred")).To(BeFalse())

		By("F3: settling Oxia unblocks BookKeeper AND AutoRecovery in the SAME reconcile (both gate on Oxia)")
		oxia = getOxia()
		markReady(oxia, oxia.Generation)

		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(rolloutRequeueInterval))

		Expect(getBookKeeper().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getAutoRecovery().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getBroker().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		_, reason = upgradingReason()
		Expect(reason).To(Equal(tierBookKeeper.upgradeReason()))

		By("settling BookKeeper (and AutoRecovery) unblocks Broker only - Proxy/FunctionsWorker stay deferred")
		bookKeeper, autoRecovery = getBookKeeper(), getAutoRecovery()
		markReady(bookKeeper, bookKeeper.Generation)
		markReady(autoRecovery, autoRecovery.Generation)

		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(rolloutRequeueInterval))

		Expect(getBroker().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionBefore)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		_, reason = upgradingReason()
		Expect(reason).To(Equal(tierBroker.upgradeReason()))

		By("settling Broker unblocks Proxy only - FunctionsWorker stays deferred")
		broker = getBroker()
		markReady(broker, broker.Generation)

		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(rolloutRequeueInterval))

		Expect(getProxy().Spec.Image).To(Equal(imageFor(versionAfter)))
		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionBefore)))

		_, reason = upgradingReason()
		Expect(reason).To(Equal(tierProxy.upgradeReason()))

		By("settling Proxy finally unblocks FunctionsWorker, the last tier")
		proxy = getProxy()
		markReady(proxy, proxy.Generation)

		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(rolloutRequeueInterval))

		Expect(getFunctionsWorker().Spec.Image).To(Equal(imageFor(versionAfter)))

		_, reason = upgradingReason()
		Expect(reason).To(Equal(tierFunctionsWorker.upgradeReason()))

		By("settling FunctionsWorker completes the rollout: Upgrading returns to Settled")
		functionsWorker = getFunctionsWorker()
		markReady(functionsWorker, functionsWorker.Generation)

		result, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())

		status, reason = upgradingReason()
		Expect(status).To(Equal(metav1.ConditionFalse))
		Expect(reason).To(Equal(reasonUpgradeSettled))
	})

	It("F2: corrects out-of-band child spec drift ungated, and does not report it as an upgrade", func() {
		By("creating a cluster and reconciling so its children exist")
		pulsarCluster := &clusterv1alpha1.PulsarCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
			Spec: clusterv1alpha1.PulsarClusterSpec{
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, pulsarCluster)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		brokerKey := types.NamespacedName{Name: clusterName + "-broker", Namespace: namespace.Name}
		getBroker := func() *clusterv1alpha1.Broker {
			b := &clusterv1alpha1.Broker{}
			Expect(k8sClient.Get(ctx, brokerKey, b)).To(Succeed())
			return b
		}

		By("mutating the Broker child's live spec out-of-band, as a stray `kubectl edit` would")
		broker := getBroker()
		Expect(broker.Spec.Replicas).To(HaveValue(Equal(int32(1))))
		broker.Spec.Replicas = ptr(int32(5))
		Expect(k8sClient.Update(ctx, broker)).To(Succeed())
		_ = drainEvents()

		By("reconciling with the PulsarCluster spec UNCHANGED")
		result, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		By("the drift is reverted to desired - a regression the pure hash gate would otherwise miss")
		Expect(getBroker().Spec.Replicas).To(HaveValue(Equal(int32(1))))

		By("drift correction is NOT an upgrade: Upgrading stays Settled and no rollout event is emitted")
		Expect(k8sClient.Get(ctx, req.NamespacedName, pulsarCluster)).To(Succeed())
		upgradingCond := apimeta.FindStatusCondition(pulsarCluster.Status.Conditions, conditionTypeUpgrading)
		Expect(upgradingCond).NotTo(BeNil())
		Expect(upgradingCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(upgradingCond.Reason).To(Equal(reasonUpgradeSettled))
		Expect(result.RequeueAfter).To(BeZero(), "drift correction does not schedule a rollout requeue")

		evs := drainEvents()
		Expect(anyContains(evs, "RolloutStarted")).To(BeFalse())
		Expect(anyContains(evs, "RolloutDeferred")).To(BeFalse())

		By("a follow-up reconcile is a clean no-op: the child now matches desired again")
		genBefore := getBroker().Generation
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(getBroker().Generation).To(Equal(genBefore))
	})
})
