package builder

import (
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPodAntiAffinityHard(t *testing.T) {
	selector := map[string]string{labelComponent: testComponentBookkeeper, labelInstance: testClusterName}

	affinity := PodAntiAffinity(true, selector)

	if affinity.PodAntiAffinity == nil {
		t.Fatal("PodAntiAffinity is nil")
	}
	required := affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required) != 1 {
		t.Fatalf("RequiredDuringSchedulingIgnoredDuringExecution = %d terms, want 1", len(required))
	}
	if required[0].TopologyKey != HostnameTopologyKey {
		t.Errorf("TopologyKey = %q, want %q", required[0].TopologyKey, HostnameTopologyKey)
	}
	if !maps.Equal(required[0].LabelSelector.MatchLabels, selector) {
		t.Errorf("LabelSelector = %v, want %v", required[0].LabelSelector.MatchLabels, selector)
	}
	if len(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Error("hard anti-affinity must not also set a preferred (soft) term")
	}
}

func TestPodAntiAffinitySoft(t *testing.T) {
	selector := map[string]string{labelComponent: testComponentBookkeeper, labelInstance: testClusterName}

	affinity := PodAntiAffinity(false, selector)

	if affinity.PodAntiAffinity == nil {
		t.Fatal("PodAntiAffinity is nil")
	}
	if len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Error("soft anti-affinity must not also set a required (hard) term")
	}
	preferred := affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(preferred) != 1 {
		t.Fatalf("PreferredDuringSchedulingIgnoredDuringExecution = %d terms, want 1", len(preferred))
	}
	if preferred[0].Weight != 100 {
		t.Errorf("Weight = %d, want 100", preferred[0].Weight)
	}
	if preferred[0].PodAffinityTerm.TopologyKey != HostnameTopologyKey {
		t.Errorf("TopologyKey = %q, want %q", preferred[0].PodAffinityTerm.TopologyKey, HostnameTopologyKey)
	}
	if !maps.Equal(preferred[0].PodAffinityTerm.LabelSelector.MatchLabels, selector) {
		t.Errorf("LabelSelector = %v, want %v", preferred[0].PodAffinityTerm.LabelSelector.MatchLabels, selector)
	}
}

func TestZoneTopologySpreadConstraints(t *testing.T) {
	selector := map[string]string{labelComponent: testComponentBookkeeper}

	constraints := ZoneTopologySpreadConstraints(selector)

	if len(constraints) != 1 {
		t.Fatalf("got %d constraints, want 1", len(constraints))
	}
	c := constraints[0]
	if c.MaxSkew != 1 {
		t.Errorf("MaxSkew = %d, want 1", c.MaxSkew)
	}
	if c.TopologyKey != ZoneTopologyKey {
		t.Errorf("TopologyKey = %q, want %q", c.TopologyKey, ZoneTopologyKey)
	}
	if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
		t.Errorf("WhenUnsatisfiable = %q, want %q", c.WhenUnsatisfiable, corev1.ScheduleAnyway)
	}
	if !maps.Equal(c.LabelSelector.MatchLabels, selector) {
		t.Errorf("LabelSelector = %v, want %v", c.LabelSelector.MatchLabels, selector)
	}
}

func TestQuorumMaxUnavailable(t *testing.T) {
	tests := []struct {
		replicas int32
		want     int32
	}{
		{replicas: 0, want: 0},
		{replicas: 1, want: 0},
		{replicas: 2, want: 0},
		{replicas: 3, want: 1},
		{replicas: 4, want: 1},
		{replicas: 5, want: 2},
		{replicas: -1, want: 0},
	}

	for _, tt := range tests {
		got := QuorumMaxUnavailable(tt.replicas)
		if got.IntValue() != int(tt.want) {
			t.Errorf("QuorumMaxUnavailable(%d) = %d, want %d", tt.replicas, got.IntValue(), tt.want)
		}
	}
}
