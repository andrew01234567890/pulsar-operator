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

package metadata

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// This spec exercises the CEL (x-kubernetes-validations) admission rule
// generated from the +kubebuilder:validation:XValidation marker on
// OxiaServerSpec (api/metadata/v1alpha1/oxiacluster_types.go). It asserts
// against the real envtest apiserver (k8sClient/ctx from suite_test.go),
// since CEL rules are evaluated by the apiserver itself and are invisible to
// a fake client.
var _ = Describe("CEL admission validation", func() {
	celCtx := context.Background()

	Context("OxiaCluster server replicas", func() {
		It("rejects an even server replica count", func() {
			oxia := &metadatav1alpha1.OxiaCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "oxia-cel-even", Namespace: testEnvtestNamespace},
				Spec: metadatav1alpha1.OxiaClusterSpec{
					Server: &metadatav1alpha1.OxiaServerSpec{Replicas: ptr(int32(4))},
				},
			}
			err := k8sClient.Create(celCtx, oxia)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("oxia server replicas must be odd (quorum)"))
		})

		It("accepts an explicit odd server replica count", func() {
			oxia := &metadatav1alpha1.OxiaCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "oxia-cel-odd", Namespace: testEnvtestNamespace},
				Spec: metadatav1alpha1.OxiaClusterSpec{
					Server: &metadatav1alpha1.OxiaServerSpec{Replicas: ptr(int32(5))},
				},
			}
			Expect(k8sClient.Create(celCtx, oxia)).To(Succeed())
			Expect(k8sClient.Delete(celCtx, oxia)).To(Succeed())
		})

		It("accepts the default server replica count (3, odd) when unset", func() {
			oxia := &metadatav1alpha1.OxiaCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "oxia-cel-default", Namespace: testEnvtestNamespace},
				Spec: metadatav1alpha1.OxiaClusterSpec{
					Server: &metadatav1alpha1.OxiaServerSpec{},
				},
			}
			Expect(k8sClient.Create(celCtx, oxia)).To(Succeed())

			created := &metadatav1alpha1.OxiaCluster{}
			Expect(k8sClient.Get(celCtx, client.ObjectKeyFromObject(oxia), created)).To(Succeed())
			Expect(*created.Spec.Server.Replicas).To(Equal(int32(3)))

			Expect(k8sClient.Delete(celCtx, oxia)).To(Succeed())
		})
	})
})
