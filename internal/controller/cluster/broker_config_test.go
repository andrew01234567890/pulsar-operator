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
	"maps"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	clusterv1alpha1 "github.com/andrew01234567890/pulsar-operator/api/cluster/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/builder"
	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

// testMetadataStoreURL stands in for whatever the PulsarCluster reconciler
// (or a user, for a standalone Broker) puts in spec.Config; its exact shape
// doesn't matter here since defaultBrokerConfig must never hardcode one.
const testMetadataStoreURL = "oxia://pulsar-oxia-coordinator:6648/default"

// TestDefaultBrokerConfig_Regression pins the exact operator-default
// broker.conf keys/values. A silent change here (e.g. a typo'd class name)
// would ship a broken load-manager default without any test failing
// elsewhere, since RenderProperties/Merge only ever see whatever this
// function returns.
func TestDefaultBrokerConfig_Regression(t *testing.T) {
	tests := []struct {
		name         string
		loadBalancer string
		want         map[string]string
	}{
		{
			name:         "unset defaults to extensible",
			loadBalancer: "",
			want: map[string]string{
				confKeyBrokerServicePort:       "6650",
				confKeyWebServicePort:          "8080",
				confKeyLoadManagerClassName:    extensibleLoadManagerClassName,
				confKeyBrokerShutdownTimeoutMs: "60000",
			},
		},
		{
			name:         "explicit extensible",
			loadBalancer: "extensible",
			want: map[string]string{
				confKeyBrokerServicePort:       "6650",
				confKeyWebServicePort:          "8080",
				confKeyLoadManagerClassName:    extensibleLoadManagerClassName,
				confKeyBrokerShutdownTimeoutMs: "60000",
			},
		},
		{
			name:         "simple",
			loadBalancer: loadBalancerSimple,
			want: map[string]string{
				confKeyBrokerServicePort:       "6650",
				confKeyWebServicePort:          "8080",
				confKeyLoadManagerClassName:    simpleLoadManagerClassName,
				confKeyBrokerShutdownTimeoutMs: "60000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := defaultBrokerConfig(clusterv1alpha1.BrokerSpec{LoadBalancer: tt.loadBalancer})
			if !maps.Equal(got, tt.want) {
				t.Errorf("defaultBrokerConfig(LoadBalancer=%q) = %v, want %v", tt.loadBalancer, got, tt.want)
			}
		})
	}
}

func TestMergedBrokerConfig_UserOverridesWin(t *testing.T) {
	broker := &clusterv1alpha1.Broker{
		Spec: clusterv1alpha1.BrokerSpec{
			Config: map[string]string{
				configKeyMetadataStoreURL:              testMetadataStoreURL,
				configKeyConfigurationMetadataStoreURL: testMetadataStoreURL,
				confKeyLoadManagerClassName:            "com.example.CustomLoadManager",
			},
		},
	}

	got := mergedBrokerConfig(broker)

	if got[confKeyLoadManagerClassName] != "com.example.CustomLoadManager" {
		t.Errorf("loadManagerClassName = %q, want spec.Config override to win", got[confKeyLoadManagerClassName])
	}
	if got[configKeyMetadataStoreURL] != testMetadataStoreURL {
		t.Errorf("metadataStoreUrl = %q, want value from spec.Config", got[configKeyMetadataStoreURL])
	}
	if _, present := defaultBrokerConfig(broker.Spec)[configKeyMetadataStoreURL]; present {
		t.Error("metadataStoreUrl must not be an operator default - it must only ever come from spec.Config")
	}
}

// TestRenderBrokerConfigDeterministic guards the "single deterministic
// rendered string, never a raw map range" requirement: hashing a config map
// key-by-key via a range would be nondeterministic since Go randomizes map
// iteration order, which would make the checksum annotation (and therefore
// whether a config change triggers a rolling restart) flap between
// reconciles for no actual config change.
func TestRenderBrokerConfigDeterministic(t *testing.T) {
	broker := &clusterv1alpha1.Broker{
		Spec: clusterv1alpha1.BrokerSpec{
			Config: map[string]string{
				"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6",
			},
		},
	}

	first := brokerPodAnnotations(renderConfig(broker))
	for range 20 {
		got := brokerPodAnnotations(renderConfig(broker))
		if got[builder.ConfigChecksumAnnotation] != first[builder.ConfigChecksumAnnotation] {
			t.Fatalf("checksum annotation is not deterministic across repeated renders: %q != %q", got[builder.ConfigChecksumAnnotation], first[builder.ConfigChecksumAnnotation])
		}
	}
}

func TestLoadManagerClassName(t *testing.T) {
	tests := []struct {
		loadBalancer string
		want         string
	}{
		{loadBalancer: "", want: extensibleLoadManagerClassName},
		{loadBalancer: "extensible", want: extensibleLoadManagerClassName},
		{loadBalancer: loadBalancerSimple, want: simpleLoadManagerClassName},
	}
	for _, tt := range tests {
		if got := loadManagerClassName(tt.loadBalancer); got != tt.want {
			t.Errorf("loadManagerClassName(%q) = %q, want %q", tt.loadBalancer, got, tt.want)
		}
	}
}

func TestBrokerReplicas(t *testing.T) {
	three := int32(3)
	five := int32(5)
	zero := int32(0)

	tests := []struct {
		name string
		in   *int32
		want int32
	}{
		{name: "nil defaults to 3", in: nil, want: defaultBrokerReplicas},
		{name: "explicit value wins", in: &five, want: 5},
		{name: "explicit zero wins", in: &zero, want: 0},
		{name: "explicit default value", in: &three, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := brokerReplicas(tt.in); got != tt.want {
				t.Errorf("brokerReplicas(%v) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestBrokerPDBEnabled(t *testing.T) {
	falseVal := false
	trueVal := true

	tests := []struct {
		name string
		in   *clusterv1alpha1.PodDisruptionBudgetConfig
		want bool
	}{
		{name: "nil defaults enabled", in: nil, want: true},
		{name: "unset Enabled defaults enabled", in: &clusterv1alpha1.PodDisruptionBudgetConfig{}, want: true},
		{name: "explicitly disabled", in: &clusterv1alpha1.PodDisruptionBudgetConfig{Enabled: &falseVal}, want: false},
		{name: "explicitly enabled", in: &clusterv1alpha1.PodDisruptionBudgetConfig{Enabled: &trueVal}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := brokerPDBEnabled(tt.in); got != tt.want {
				t.Errorf("brokerPDBEnabled(%+v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestBrokerMaxUnavailable(t *testing.T) {
	custom := intstr.FromString("25%")

	tests := []struct {
		name string
		in   *clusterv1alpha1.PodDisruptionBudgetConfig
		want intstr.IntOrString
	}{
		{name: "nil defaults to 1", in: nil, want: intstr.FromInt32(1)},
		{name: "unset MaxUnavailable defaults to 1", in: &clusterv1alpha1.PodDisruptionBudgetConfig{}, want: intstr.FromInt32(1)},
		{name: "explicit override wins", in: &clusterv1alpha1.PodDisruptionBudgetConfig{MaxUnavailable: &custom}, want: custom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := brokerMaxUnavailable(tt.in); got != tt.want {
				t.Errorf("brokerMaxUnavailable(%+v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestTerminationGracePeriodSeconds_DrainWiring is the core "graceful drain"
// unit test: terminationGracePeriodSeconds must always exceed
// brokerShutdownTimeoutMs (converted to seconds) by at least the preStop
// drain sleep, or kubelet can SIGKILL the broker mid-bundle-unload.
func TestTerminationGracePeriodSeconds_DrainWiring(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]string
		want   int64
	}{
		{
			name:   "missing key uses the 60s upstream default",
			config: map[string]string{},
			want:   60 + preStopDrainSeconds,
		},
		{
			name:   "custom shutdown timeout",
			config: map[string]string{confKeyBrokerShutdownTimeoutMs: "90000"},
			want:   90 + preStopDrainSeconds,
		},
		{
			name:   "rounds up a sub-second remainder",
			config: map[string]string{confKeyBrokerShutdownTimeoutMs: "1500"},
			want:   2 + preStopDrainSeconds,
		},
		{
			name:   "unparsable value falls back to the default",
			config: map[string]string{confKeyBrokerShutdownTimeoutMs: "not-a-number"},
			want:   60 + preStopDrainSeconds,
		},
		{
			name:   "negative value falls back to the default",
			config: map[string]string{confKeyBrokerShutdownTimeoutMs: "-1"},
			want:   60 + preStopDrainSeconds,
		},
		{
			name:   "zero is honored (not falsy)",
			config: map[string]string{confKeyBrokerShutdownTimeoutMs: "0"},
			want:   0 + preStopDrainSeconds,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := terminationGracePeriodSeconds(tt.config); got != tt.want {
				t.Errorf("terminationGracePeriodSeconds(%v) = %d, want %d", tt.config, got, tt.want)
			}
		})
	}
}

func TestDesiredBrokerStatefulSetSpec_DrainWiring(t *testing.T) {
	broker := &clusterv1alpha1.Broker{
		ObjectMeta: metav1.ObjectMeta{Name: "test-broker", Namespace: "default"},
		Spec: clusterv1alpha1.BrokerSpec{
			Config: map[string]string{confKeyBrokerShutdownTimeoutMs: "30000"},
		},
	}
	mergedConfig := mergedBrokerConfig(broker)
	rendered := renderConfig(broker)
	ports := resolveBrokerPorts(mergedConfig)

	spec := desiredBrokerStatefulSetSpec(broker, broker.Name, map[string]string{"k": "v"}, map[string]string{"k": "v"}, mergedConfig, rendered, ports)

	wantGrace := 30 + preStopDrainSeconds
	if spec.Template.Spec.TerminationGracePeriodSeconds == nil || *spec.Template.Spec.TerminationGracePeriodSeconds != wantGrace {
		t.Fatalf("TerminationGracePeriodSeconds = %v, want %d", spec.Template.Spec.TerminationGracePeriodSeconds, wantGrace)
	}

	if len(spec.Template.Spec.Containers) != 1 {
		t.Fatalf("got %d containers, want 1", len(spec.Template.Spec.Containers))
	}
	container := spec.Template.Spec.Containers[0]

	if container.Lifecycle == nil || container.Lifecycle.PreStop == nil || container.Lifecycle.PreStop.Exec == nil {
		t.Fatal("container missing an exec preStop hook")
	}
	preStopCmd := strings.Join(container.Lifecycle.PreStop.Exec.Command, " ")
	if !strings.Contains(preStopCmd, "sleep 5") {
		t.Errorf("preStop command = %q, want it to sleep %ds before SIGTERM", preStopCmd, preStopDrainSeconds)
	}
}

func TestBrokerReadyCondition(t *testing.T) {
	// stsAt builds a StatefulSet whose spec generation is `generation` and
	// whose observed status is the given (observedGen, updated, ready) triple,
	// so each case can model a precise point in a rollout.
	stsAt := func(generation, observedGen, updated, ready int32) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Generation: int64(generation)},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: int64(observedGen),
				UpdatedReplicas:    updated,
				ReadyReplicas:      ready,
			},
		}
	}

	tests := []struct {
		name       string
		sts        *appsv1.StatefulSet
		desired    int32
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "fully rolled out and ready",
			sts:        stsAt(2, 2, 3, 3),
			desired:    3,
			wantStatus: metav1.ConditionTrue,
			wantReason: reasonAllReady,
		},
		{
			name:       "ready count met but rollout not observed yet",
			sts:        stsAt(2, 1, 3, 3),
			desired:    3,
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			// Revision skew: every pod is Ready but not every pod is on the
			// new revision. Ready=True here would wrongly declare a rolling
			// restart complete while old-revision pods still serve.
			name:       "all ready but not all updated (revision skew)",
			sts:        stsAt(2, 2, 2, 3),
			desired:    3,
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			name:       "fewer replicas ready than desired",
			sts:        stsAt(2, 2, 3, 2),
			desired:    3,
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonProgressing,
		},
		{
			// Broker is mandatory: zero replicas must not read as healthy.
			name:       "zero desired is NotReady",
			sts:        stsAt(1, 1, 0, 0),
			desired:    0,
			wantStatus: metav1.ConditionFalse,
			wantReason: reasonNoReplicas,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const observedGeneration int64 = 7
			cond := brokerReadyCondition(tt.sts, tt.desired, observedGeneration)
			if cond.Type != conditionTypeReady {
				t.Errorf("Type = %q, want %q", cond.Type, conditionTypeReady)
			}
			if cond.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", cond.Status, tt.wantStatus)
			}
			if cond.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", cond.Reason, tt.wantReason)
			}
			if cond.ObservedGeneration != observedGeneration {
				t.Errorf("ObservedGeneration = %d, want %d (needed by the umbrella reconciler's staleness check)", cond.ObservedGeneration, observedGeneration)
			}
		})
	}
}

func TestBrokerContainer_ImageOverride(t *testing.T) {
	const override = "example.com/pulsar:custom-tag"
	broker := &clusterv1alpha1.Broker{Spec: clusterv1alpha1.BrokerSpec{Image: override}}

	got := brokerContainer(broker, resolveBrokerPorts(mergedBrokerConfig(broker)))
	if got.Image != override {
		t.Errorf("container.Image = %q, want spec.Image override %q", got.Image, override)
	}

	fallback := brokerContainer(&clusterv1alpha1.Broker{}, resolveBrokerPorts(nil))
	if fallback.Image != defaultBrokerImage {
		t.Errorf("container.Image = %q, want default %q when spec.Image is empty", fallback.Image, defaultBrokerImage)
	}
}

// TestBrokerConfigChecksumChangesWithConfig proves the pod-template checksum
// annotation is a function of the rendered config: a spec.config change must
// change the checksum, which is what forces the rolling restart.
func TestBrokerConfigChecksumChangesWithConfig(t *testing.T) {
	base := &clusterv1alpha1.Broker{}
	changed := &clusterv1alpha1.Broker{
		Spec: clusterv1alpha1.BrokerSpec{
			Config: map[string]string{"managedLedgerDefaultAckQuorum": "3"},
		},
	}

	baseSum := brokerPodAnnotations(renderConfig(base))[builder.ConfigChecksumAnnotation]
	changedSum := brokerPodAnnotations(renderConfig(changed))[builder.ConfigChecksumAnnotation]

	if baseSum == "" || changedSum == "" {
		t.Fatalf("checksum annotations must be non-empty: base=%q changed=%q", baseSum, changedSum)
	}
	if baseSum == changedSum {
		t.Errorf("checksum did not change when spec.config changed: %q", baseSum)
	}
}

func TestResolveBrokerPorts(t *testing.T) {
	tests := []struct {
		name       string
		config     map[string]string
		wantBinary int32
		wantHTTP   int32
	}{
		{
			name:       "operator defaults",
			config:     defaultBrokerConfig(clusterv1alpha1.BrokerSpec{}),
			wantBinary: defaultBrokerServicePort,
			wantHTTP:   defaultWebServicePort,
		},
		{
			name:       "spec.config override threads through",
			config:     map[string]string{confKeyBrokerServicePort: "16650", confKeyWebServicePort: "18080"},
			wantBinary: 16650,
			wantHTTP:   18080,
		},
		{
			name:       "malformed value falls back to default",
			config:     map[string]string{confKeyBrokerServicePort: "not-a-port", confKeyWebServicePort: "70000"},
			wantBinary: defaultBrokerServicePort,
			wantHTTP:   defaultWebServicePort,
		},
		{
			name:       "nil config falls back to defaults",
			config:     nil,
			wantBinary: defaultBrokerServicePort,
			wantHTTP:   defaultWebServicePort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveBrokerPorts(tt.config)
			if got.binary != tt.wantBinary || got.http != tt.wantHTTP {
				t.Errorf("resolveBrokerPorts(%v) = {binary:%d http:%d}, want {binary:%d http:%d}", tt.config, got.binary, got.http, tt.wantBinary, tt.wantHTTP)
			}
		})
	}
}

// TestBrokerContainerPortsTrackConfig guards against the container/probe ports
// desyncing from a spec.config port override: both the ContainerPorts and the
// probe target port must reflect the resolved config, not a hardcoded literal.
func TestBrokerContainerPortsTrackConfig(t *testing.T) {
	broker := &clusterv1alpha1.Broker{
		Spec: clusterv1alpha1.BrokerSpec{
			Config: map[string]string{confKeyBrokerServicePort: "16650", confKeyWebServicePort: "18080"},
		},
	}
	ports := resolveBrokerPorts(mergedBrokerConfig(broker))
	c := brokerContainer(broker, ports)

	gotPorts := map[string]int32{}
	for _, p := range c.Ports {
		gotPorts[p.Name] = p.ContainerPort
	}
	if gotPorts[brokerPortName] != 16650 {
		t.Errorf("binary ContainerPort = %d, want 16650", gotPorts[brokerPortName])
	}
	if gotPorts[httpPortName] != 18080 {
		t.Errorf("http ContainerPort = %d, want 18080", gotPorts[httpPortName])
	}
	if got := c.ReadinessProbe.HTTPGet.Port.IntValue(); got != 18080 {
		t.Errorf("readiness probe port = %d, want 18080 (must track webServicePort)", got)
	}
	if got := c.LivenessProbe.HTTPGet.Port.IntValue(); got != 18080 {
		t.Errorf("liveness probe port = %d, want 18080 (must track webServicePort)", got)
	}
}

// renderConfig is a small test helper mirroring the Reconcile-internal
// mergedBrokerConfig -> RenderProperties pipeline.
func renderConfig(broker *clusterv1alpha1.Broker) string {
	return config.RenderProperties(mergedBrokerConfig(broker))
}

func TestBrokerAffinity(t *testing.T) {
	selector := builder.SelectorLabels("test-broker", brokerComponent)

	tests := []struct {
		name     string
		spec     *clusterv1alpha1.AntiAffinityConfig
		wantNil  bool
		wantHard bool
	}{
		{name: "unset spec defaults to soft", spec: nil, wantHard: false},
		{name: "explicit host=true selects hard", spec: &clusterv1alpha1.AntiAffinityConfig{Host: boolPtr(true)}, wantHard: true},
		{name: "explicit host=false stays soft", spec: &clusterv1alpha1.AntiAffinityConfig{Host: boolPtr(false)}, wantHard: false},
		{name: "enabled=false disables anti-affinity entirely", spec: &clusterv1alpha1.AntiAffinityConfig{Enabled: boolPtr(false)}, wantNil: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := brokerAffinity(tt.spec, selector)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("brokerAffinity(%+v) = %+v, want nil", tt.spec, got)
				}
				return
			}
			if got == nil || got.PodAntiAffinity == nil {
				t.Fatalf("brokerAffinity(%+v) = %+v, want non-nil PodAntiAffinity", tt.spec, got)
			}
			hasHard := len(got.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
			hasSoft := len(got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0
			if tt.wantHard {
				if !hasHard || hasSoft {
					t.Errorf("brokerAffinity(%+v) = %+v, want hard-only", tt.spec, got)
				}
			} else {
				if !hasSoft || hasHard {
					t.Errorf("brokerAffinity(%+v) = %+v, want soft-only", tt.spec, got)
				}
			}
		})
	}
}
