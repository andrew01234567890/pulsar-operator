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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

func ptr[T any](v T) *T { return &v }

const (
	testClusterImage     = "cluster/pulsar:5.0.0"
	testAltImage         = "cluster/image:1"
	testReasonRollingOut = "RollingOut"
	testComponentBroker  = "broker"
	testComponentProxy   = "proxy"
)

func TestChildName(t *testing.T) {
	if got, want := childName("my-cluster", testComponentBroker), "my-cluster-broker"; got != want {
		t.Errorf("childName() = %q, want %q", got, want)
	}
}

func TestEffectiveImage(t *testing.T) {
	cases := []struct {
		name           string
		componentImage string
		clusterImage   string
		want           string
	}{
		{"component overrides cluster", "component/image:1", testAltImage, "component/image:1"},
		{"falls back to cluster default", "", testAltImage, testAltImage},
		{"both empty", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveImage(tc.componentImage, tc.clusterImage); got != tc.want {
				t.Errorf("effectiveImage(%q, %q) = %q, want %q", tc.componentImage, tc.clusterImage, got, tc.want)
			}
		})
	}
}

func TestBuildBrokerSpec(t *testing.T) {
	cases := []struct {
		name      string
		spec      clusterv1alpha1.PulsarClusterSpec
		wantImage string
		wantNil   bool
	}{
		{
			name:    "nil sub-spec yields nil",
			spec:    clusterv1alpha1.PulsarClusterSpec{},
			wantNil: true,
		},
		{
			name: "empty component image falls back to cluster image",
			spec: clusterv1alpha1.PulsarClusterSpec{
				Image:  testClusterImage,
				Broker: &clusterv1alpha1.BrokerSpec{Replicas: ptr(int32(3))},
			},
			wantImage: testClusterImage,
		},
		{
			name: "explicit component image is preserved",
			spec: clusterv1alpha1.PulsarClusterSpec{
				Image:  testClusterImage,
				Broker: &clusterv1alpha1.BrokerSpec{Image: "broker/pulsar:override"},
			},
			wantImage: "broker/pulsar:override",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildBrokerSpec(tc.spec)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("buildBrokerSpec() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("buildBrokerSpec() = nil, want non-nil")
			}
			if got.Image != tc.wantImage {
				t.Errorf("Image = %q, want %q", got.Image, tc.wantImage)
			}
			// The mapping must not mutate the original PulsarCluster sub-spec.
			if tc.spec.Broker != nil && got == tc.spec.Broker {
				t.Error("buildBrokerSpec() returned the input pointer instead of a copy")
			}
		})
	}
}

func TestBuildProxyAutoRecoveryFunctionsWorkerSpec_ImageFallback(t *testing.T) {
	spec := clusterv1alpha1.PulsarClusterSpec{
		Image:           testClusterImage,
		Proxy:           &clusterv1alpha1.ProxySpec{},
		AutoRecovery:    &clusterv1alpha1.AutoRecoverySpec{},
		FunctionsWorker: &clusterv1alpha1.FunctionsWorkerSpec{},
	}

	if got := buildProxySpec(spec).Image; got != spec.Image {
		t.Errorf("Proxy Image = %q, want %q", got, spec.Image)
	}
	if got := buildAutoRecoverySpec(spec).Image; got != spec.Image {
		t.Errorf("AutoRecovery Image = %q, want %q", got, spec.Image)
	}
	if got := buildFunctionsWorkerSpec(spec).Image; got != spec.Image {
		t.Errorf("FunctionsWorker Image = %q, want %q", got, spec.Image)
	}

	// nil sub-specs must yield nil, not panic.
	empty := clusterv1alpha1.PulsarClusterSpec{}
	if buildProxySpec(empty) != nil {
		t.Error("buildProxySpec() with nil sub-spec = non-nil, want nil")
	}
	if buildAutoRecoverySpec(empty) != nil {
		t.Error("buildAutoRecoverySpec() with nil sub-spec = non-nil, want nil")
	}
	if buildFunctionsWorkerSpec(empty) != nil {
		t.Error("buildFunctionsWorkerSpec() with nil sub-spec = non-nil, want nil")
	}
}

func TestBuildBookKeeperSpec(t *testing.T) {
	globalSC := "fast-ssd"
	explicitSC := "already-set"

	cases := []struct {
		name          string
		spec          clusterv1alpha1.PulsarClusterSpec
		wantJournalSC *string
		wantLedgersSC *string
		wantIndexSC   *string
	}{
		{
			name: "propagates global storage class to unset volumes",
			spec: clusterv1alpha1.PulsarClusterSpec{
				Global: &clusterv1alpha1.GlobalSpec{StorageClassName: &globalSC},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Volumes: &clusterv1alpha1.BookKeeperVolumes{
						Journal: &clusterv1alpha1.VolumeSpec{},
						Ledgers: &clusterv1alpha1.VolumeSpec{},
						Index:   &clusterv1alpha1.VolumeSpec{},
					},
				},
			},
			wantJournalSC: &globalSC,
			wantLedgersSC: &globalSC,
			wantIndexSC:   &globalSC,
		},
		{
			name: "does not overwrite an explicit volume storage class",
			spec: clusterv1alpha1.PulsarClusterSpec{
				Global: &clusterv1alpha1.GlobalSpec{StorageClassName: &globalSC},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Volumes: &clusterv1alpha1.BookKeeperVolumes{
						Journal: &clusterv1alpha1.VolumeSpec{StorageClassName: &explicitSC},
					},
				},
			},
			wantJournalSC: &explicitSC,
		},
		{
			name: "no global default leaves volumes untouched",
			spec: clusterv1alpha1.PulsarClusterSpec{
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{
					Volumes: &clusterv1alpha1.BookKeeperVolumes{
						Journal: &clusterv1alpha1.VolumeSpec{},
					},
				},
			},
			wantJournalSC: nil,
		},
		{
			name: "no volumes configured stays nil, nothing fabricated",
			spec: clusterv1alpha1.PulsarClusterSpec{
				Global:     &clusterv1alpha1.GlobalSpec{StorageClassName: &globalSC},
				BookKeeper: &clusterv1alpha1.BookKeeperSpec{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildBookKeeperSpec(tc.spec)
			if got.Volumes == nil {
				if tc.wantJournalSC != nil || tc.wantLedgersSC != nil || tc.wantIndexSC != nil {
					t.Fatal("Volumes = nil, want configured storage classes")
				}
				return
			}
			assertStorageClass(t, "Journal", got.Volumes.Journal, tc.wantJournalSC)
			assertStorageClass(t, "Ledgers", got.Volumes.Ledgers, tc.wantLedgersSC)
			assertStorageClass(t, "Index", got.Volumes.Index, tc.wantIndexSC)
		})
	}
}

func assertStorageClass(t *testing.T, field string, vol *clusterv1alpha1.VolumeSpec, want *string) {
	t.Helper()
	if want == nil {
		if vol != nil && vol.StorageClassName != nil {
			t.Errorf("%s.StorageClassName = %q, want nil", field, *vol.StorageClassName)
		}
		return
	}
	if vol == nil || vol.StorageClassName == nil {
		t.Fatalf("%s.StorageClassName = nil, want %q", field, *want)
	}
	if *vol.StorageClassName != *want {
		t.Errorf("%s.StorageClassName = %q, want %q", field, *vol.StorageClassName, *want)
	}
}

func TestShouldCreateOxia(t *testing.T) {
	oxiaSpec := &metadatav1alpha1.OxiaClusterSpec{}

	cases := []struct {
		name string
		spec clusterv1alpha1.PulsarClusterSpec
		want bool
	}{
		{"no oxia sub-spec", clusterv1alpha1.PulsarClusterSpec{}, false},
		{
			"oxia sub-spec with no metadataStore selector",
			clusterv1alpha1.PulsarClusterSpec{Oxia: oxiaSpec},
			true,
		},
		{
			"oxia sub-spec with explicit oxia metadataStore selector",
			clusterv1alpha1.PulsarClusterSpec{
				Oxia:          oxiaSpec,
				MetadataStore: &clusterv1alpha1.MetadataStoreSpec{Type: "oxia"},
			},
			true,
		},
		{
			"oxia sub-spec but metadataStore selects a different implementation",
			clusterv1alpha1.PulsarClusterSpec{
				Oxia:          oxiaSpec,
				MetadataStore: &clusterv1alpha1.MetadataStoreSpec{Type: "zookeeper"},
			},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldCreateOxia(tc.spec); got != tc.want {
				t.Errorf("shouldCreateOxia() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildOxiaSpec(t *testing.T) {
	globalSC := "fast-ssd"
	explicitSC := "already-set"

	spec := clusterv1alpha1.PulsarClusterSpec{
		Image:  testClusterImage,
		Global: &clusterv1alpha1.GlobalSpec{StorageClassName: &globalSC},
		Oxia: &metadatav1alpha1.OxiaClusterSpec{
			Coordinator: &metadatav1alpha1.OxiaCoordinatorSpec{},
			Server:      &metadatav1alpha1.OxiaServerSpec{},
		},
	}

	got := buildOxiaSpec(spec)
	if got.Coordinator.Image != spec.Image {
		t.Errorf("Coordinator.Image = %q, want %q", got.Coordinator.Image, spec.Image)
	}
	if got.Server.Image != spec.Image {
		t.Errorf("Server.Image = %q, want %q", got.Server.Image, spec.Image)
	}
	if got.Server.StorageClassName == nil || *got.Server.StorageClassName != globalSC {
		t.Errorf("Server.StorageClassName = %v, want %q", got.Server.StorageClassName, globalSC)
	}

	// An explicit server storage class must not be overwritten.
	spec.Oxia.Server.StorageClassName = &explicitSC
	got = buildOxiaSpec(spec)
	if *got.Server.StorageClassName != explicitSC {
		t.Errorf("Server.StorageClassName = %q, want %q (explicit value preserved)", *got.Server.StorageClassName, explicitSC)
	}

	// Nil sub-spec must yield nil, not panic.
	if buildOxiaSpec(clusterv1alpha1.PulsarClusterSpec{}) != nil {
		t.Error("buildOxiaSpec() with nil Oxia = non-nil, want nil")
	}
}

func TestReportFromConditions(t *testing.T) {
	cases := []struct {
		name       string
		conditions []metav1.Condition
		wantReady  bool
		wantReason string
	}{
		{
			name: "ready true",
			conditions: []metav1.Condition{
				{Type: conditionTypeReady, Status: metav1.ConditionTrue, Reason: "AllPodsReady"},
			},
			wantReady:  true,
			wantReason: "AllPodsReady",
		},
		{
			name: "ready false",
			conditions: []metav1.Condition{
				{Type: conditionTypeReady, Status: metav1.ConditionFalse, Reason: testReasonRollingOut},
			},
			wantReady:  false,
			wantReason: testReasonRollingOut,
		},
		{
			name:       "no conditions reported yet",
			conditions: nil,
			wantReady:  false,
			wantReason: reasonComponentStatusMissing,
		},
		{
			name: "ready condition absent among other conditions",
			conditions: []metav1.Condition{
				{Type: "SomethingElse", Status: metav1.ConditionTrue, Reason: "Whatever"},
			},
			wantReady:  false,
			wantReason: reasonComponentStatusMissing,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reportFromConditions(testComponentBroker, tc.conditions)
			if !got.present {
				t.Fatal("present = false, want true")
			}
			if got.ready != tc.wantReady {
				t.Errorf("ready = %v, want %v", got.ready, tc.wantReady)
			}
			if got.reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", got.reason, tc.wantReason)
			}
		})
	}
}

func TestComponentPhase(t *testing.T) {
	cases := []struct {
		name   string
		report componentReport
		want   string
	}{
		{"not present", componentReport{present: false}, ""},
		{"present and ready", componentReport{present: true, ready: true}, phaseReady},
		{"present and not ready", componentReport{present: true, ready: false}, phaseNotReady},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := componentPhase(tc.report); got != tc.want {
				t.Errorf("componentPhase() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAggregateReadyCondition(t *testing.T) {
	cases := []struct {
		name        string
		reports     []componentReport
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string // substring, empty to skip
	}{
		{
			// Regression test: an empty report set must not vacuously report
			// Ready=True just because "every element of the empty set" is
			// trivially Ready. A cluster with nothing configured is not Ready.
			name:       "no components at all is not Ready",
			reports:    nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonNoComponentsConfigured,
		},
		{
			name: "components present but none configured is not Ready",
			reports: []componentReport{
				{name: testComponentProxy, present: false},
				{name: "autorecovery", present: false},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonNoComponentsConfigured,
		},
		{
			name: "all configured components ready",
			reports: []componentReport{
				{name: testComponentBroker, present: true, ready: true},
				{name: "bookkeeper", present: true, ready: true},
				{name: testComponentProxy, present: false},
			},
			wantStatus: metav1.ConditionTrue,
			wantReason: reasonAllComponentsReady,
		},
		{
			name: "one configured component not ready blocks readiness",
			reports: []componentReport{
				{name: testComponentBroker, present: true, ready: true},
				{name: "bookkeeper", present: true, ready: false, reason: testReasonRollingOut},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  reasonComponentNotReady,
			wantMessage: "bookkeeper (RollingOut)",
		},
		{
			name: "multiple configured components not ready are all named",
			reports: []componentReport{
				{name: testComponentBroker, present: true, ready: false, reason: "Pending"},
				{name: testComponentProxy, present: true, ready: false, reason: "Pending"},
			},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  reasonComponentNotReady,
			wantMessage: "broker (Pending), proxy (Pending)",
		},
	}

	const generation = int64(7)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := aggregateReadyCondition(generation, tc.reports)
			if got.Type != conditionTypeReady {
				t.Errorf("Type = %q, want %q", got.Type, conditionTypeReady)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if got.ObservedGeneration != generation {
				t.Errorf("ObservedGeneration = %d, want %d", got.ObservedGeneration, generation)
			}
			if tc.wantMessage != "" && !strings.Contains(got.Message, tc.wantMessage) {
				t.Errorf("Message = %q, want substring %q", got.Message, tc.wantMessage)
			}
		})
	}
}
