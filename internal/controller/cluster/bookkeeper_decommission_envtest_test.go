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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	bkadmin "github.com/andrew01234567890/pulsar-operator/internal/autoscaler/bookkeeper"
)

// These envtest specs exercise BookKeeperDecommissionReconciler against a
// real (envtest) API server, complementing the fake-client unit tests in
// bookkeeper_decommission_controller_test.go. They focus on the one property
// that specifically needs a real Status subresource round trip to prove: a
// decommission that was already in flight (as persisted on
// BookKeeper.status.decommission) resumes from its saved phase across a
// fresh Reconcile call -- exactly what happens after an operator restart --
// instead of restarting the whole guarded sequence from phase 1.
var _ = Describe("BookKeeper Decommission Controller", func() {
	Context("resuming a decommission that was already in flight", func() {
		const (
			resourceName      = "decomm-resume"
			resourceNamespace = "default"
		)

		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

		AfterEach(func() {
			bk := &clusterv1alpha1.BookKeeper{}
			if err := k8sClient.Get(ctx, typeNamespacedName, bk); err == nil {
				Expect(k8sClient.Delete(ctx, bk)).To(Succeed())
			}
		})

		It("continues from the persisted phase instead of restarting the sequence from Verifying", func() {
			By("creating a BookKeeper with the guard enabled and an in-flight decommission already persisted on status")
			replicas := int32(3)
			trueVal := true
			zeroWindow := int32(0)
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Replicas: &replicas,
					Autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{
						Enabled:                    true,
						ScaleDownEnabled:           &trueVal,
						StabilizationWindowSeconds: &zeroWindow,
					},
				},
			}
			Expect(k8sClient.Create(ctx, bk)).To(Succeed())

			Expect(k8sClient.Get(ctx, typeNamespacedName, bk)).To(Succeed())
			ordinal := int32(2)
			started := metav1.NewTime(time.Now().Add(-time.Minute))
			bk.Status.Decommission = &clusterv1alpha1.BookKeeperDecommissionStatus{
				Phase:          clusterv1alpha1.BookKeeperDecommissionPhaseAwaitingReplication,
				TargetOrdinal:  &ordinal,
				TargetBookieID: bookieIDFor(bk, ordinal),
				StartedAt:      &started,
			}
			Expect(k8sClient.Status().Update(ctx, bk)).To(Succeed())

			admin := newMockAdmin()
			admin.writable["decomm-resume-2"] = false // already read-only from the "previous life" before the restart
			admin.hasLedgers["decomm-resume-2"] = false
			admin.noUnderReplicatedLedgers = true

			reconciler := &BookKeeperDecommissionReconciler{
				Client:             k8sClient,
				Scheme:             k8sClient.Scheme(),
				Recorder:           events.NewFakeRecorder(100),
				AdminClientFactory: func(*clusterv1alpha1.BookKeeper) bkadmin.AdminClient { return admin },
			}

			By("reconciling once, simulating a fresh operator process picking the object back up")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("advancing straight to InvalidatingCookie, the next phase after AwaitingReplication")
			Expect(k8sClient.Get(ctx, typeNamespacedName, bk)).To(Succeed())
			Expect(bk.Status.Decommission).NotTo(BeNil())
			Expect(bk.Status.Decommission.Phase).To(Equal(clusterv1alpha1.BookKeeperDecommissionPhaseInvalidatingCookie))

			By("never replaying the earlier MarkingReadOnly/TriggeringRecovery phases")
			Expect(admin.setReadOnlyCalls).To(BeEmpty())
			Expect(admin.triggerCalls).To(BeEmpty())
		})
	})
})
