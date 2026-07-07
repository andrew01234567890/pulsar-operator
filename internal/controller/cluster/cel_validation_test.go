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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
)

// orderedReadyPodManagementPolicy is the non-default PodManagementPolicy enum
// value, kept as a constant so this file doesn't add a third raw-literal
// occurrence of "OrderedReady" (goconst) alongside bookkeeper_build_test.go.
const orderedReadyPodManagementPolicy = "OrderedReady"

// These specs exercise the CEL (x-kubernetes-validations) admission rules
// generated onto the CRDs from +kubebuilder:validation:XValidation markers in
// api/cluster/v1alpha1. They assert against the real envtest apiserver
// (k8sClient/ctx from suite_test.go), not a fake client, since CEL rules are
// evaluated by the apiserver itself and are invisible to a fake client.
var _ = Describe("CEL admission validation", func() {
	celCtx := context.Background()

	Context("BookKeeper", func() {
		It("rejects an Ensemble where ackQuorum > writeQuorum", func() {
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-ensemble-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{
						EnsembleSize: ptr(int32(3)),
						WriteQuorum:  ptr(int32(2)),
						AckQuorum:    ptr(int32(3)),
					},
				},
			}
			err := k8sClient.Create(celCtx, bk)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ackQuorum <= writeQuorum <= ensembleSize"))
		})

		It("rejects an Ensemble where writeQuorum > ensembleSize", func() {
			// Isolates the second conjunct: ackQuorum <= writeQuorum holds
			// (2 <= 3) and ensembleSize <= replicas holds (2 <= 3), so only
			// writeQuorum <= ensembleSize (3 <= 2) can trip the rule.
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-writequorum-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Replicas: ptr(int32(3)),
					Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{
						EnsembleSize: ptr(int32(2)),
						WriteQuorum:  ptr(int32(3)),
						AckQuorum:    ptr(int32(2)),
					},
				},
			}
			err := k8sClient.Create(celCtx, bk)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ackQuorum <= writeQuorum <= ensembleSize"))
		})

		It("rejects an ensembleSize greater than replicas", func() {
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-ensemblesize-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Replicas: ptr(int32(2)),
					Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{
						EnsembleSize: ptr(int32(3)),
						WriteQuorum:  ptr(int32(2)),
						AckQuorum:    ptr(int32(2)),
					},
				},
			}
			err := k8sClient.Create(celCtx, bk)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ensembleSize cannot exceed replicas"))
		})

		It("accepts a valid ensemble/replicas combination", func() {
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-valid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Replicas: ptr(int32(4)),
					Ensemble: &clusterv1alpha1.BookKeeperEnsembleSpec{
						EnsembleSize: ptr(int32(3)),
						WriteQuorum:  ptr(int32(2)),
						AckQuorum:    ptr(int32(2)),
					},
				},
			}
			Expect(k8sClient.Create(celCtx, bk)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, bk)).To(Succeed())
		})

		It("rejects a podManagementPolicy change on update but allows other field updates", func() {
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-immutable", Namespace: testNamespaceDefault},
				Spec:       clusterv1alpha1.BookKeeperSpec{Replicas: ptr(int32(3))},
			}
			Expect(k8sClient.Create(celCtx, bk)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Delete(celCtx, bk)).To(Succeed())
			})

			key := types.NamespacedName{Name: bk.Name, Namespace: bk.Namespace}
			latest := &clusterv1alpha1.BookKeeper{}
			Expect(k8sClient.Get(celCtx, key, latest)).To(Succeed())
			Expect(latest.Spec.PodManagementPolicy).To(Equal("Parallel"))

			latest.Spec.PodManagementPolicy = orderedReadyPodManagementPolicy
			err := k8sClient.Update(celCtx, latest)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("podManagementPolicy is immutable"))

			Expect(k8sClient.Get(celCtx, key, latest)).To(Succeed())
			latest.Spec.Replicas = ptr(int32(5))
			Expect(k8sClient.Update(celCtx, latest)).To(Succeed())
		})

		It("rejects an autoscaler with diskUsageToleranceLwm >= diskUsageToleranceHwm", func() {
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-watermark-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{
						DiskUsageToleranceLwm: ptr(int32(90)),
						DiskUsageToleranceHwm: ptr(int32(80)),
					},
				},
			}
			err := k8sClient.Create(celCtx, bk)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("Lwm must be below Hwm"))
		})

		It("accepts an autoscaler with diskUsageToleranceLwm below diskUsageToleranceHwm", func() {
			bk := &clusterv1alpha1.BookKeeper{
				ObjectMeta: metav1.ObjectMeta{Name: "bk-cel-watermark-valid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BookKeeperSpec{
					Autoscaler: &clusterv1alpha1.BookKeeperAutoscalerSpec{
						DiskUsageToleranceLwm: ptr(int32(80)),
						DiskUsageToleranceHwm: ptr(int32(90)),
					},
				},
			}
			Expect(k8sClient.Create(celCtx, bk)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, bk)).To(Succeed())
		})
	})

	Context("Broker", func() {
		It("rejects an autoscaler with lowerCpuThreshold >= higherCpuThreshold", func() {
			broker := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: "broker-cel-thresholds-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BrokerSpec{
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{
						LowerCpuThreshold:  ptr(int32(90)),
						HigherCpuThreshold: ptr(int32(80)),
					},
				},
			}
			err := k8sClient.Create(celCtx, broker)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("lower threshold must be below higher"))
		})

		It("rejects an autoscaler with min greater than max", func() {
			broker := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: "broker-cel-minmax-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BrokerSpec{
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{
						Min: ptr(int32(5)),
						Max: ptr(int32(3)),
					},
				},
			}
			err := k8sClient.Create(celCtx, broker)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("min must be <= max"))
		})

		It("accepts a valid autoscaler configuration", func() {
			broker := &clusterv1alpha1.Broker{
				ObjectMeta: metav1.ObjectMeta{Name: "broker-cel-valid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.BrokerSpec{
					Autoscaler: &clusterv1alpha1.BrokerAutoscalerSpec{
						Min:                ptr(int32(2)),
						Max:                ptr(int32(5)),
						LowerCpuThreshold:  ptr(int32(30)),
						HigherCpuThreshold: ptr(int32(80)),
					},
				},
			}
			Expect(k8sClient.Create(celCtx, broker)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, broker)).To(Succeed())
		})
	})

	Context("PulsarCluster offload", func() {
		It("rejects an object-store offload driver with no bucket", func() {
			pc := &clusterv1alpha1.PulsarCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "pc-cel-offload-invalid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.PulsarClusterSpec{
					Offload: &clusterv1alpha1.OffloadSpec{Driver: "aws-s3"},
				},
			}
			err := k8sClient.Create(celCtx, pc)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("bucket is required for object-store offload drivers"))
		})

		It("accepts an object-store offload driver with a bucket set", func() {
			pc := &clusterv1alpha1.PulsarCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "pc-cel-offload-valid", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.PulsarClusterSpec{
					Offload: &clusterv1alpha1.OffloadSpec{Driver: "aws-s3", Bucket: "my-bucket"},
				},
			}
			Expect(k8sClient.Create(celCtx, pc)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, pc)).To(Succeed())
		})

		It("accepts the filesystem offload driver with no bucket", func() {
			pc := &clusterv1alpha1.PulsarCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "pc-cel-offload-filesystem", Namespace: testNamespaceDefault},
				Spec: clusterv1alpha1.PulsarClusterSpec{
					Offload: &clusterv1alpha1.OffloadSpec{Driver: "filesystem"},
				},
			}
			Expect(k8sClient.Create(celCtx, pc)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, pc)).To(Succeed())
		})
	})
})
