package builder

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// HostnameTopologyKey spreads pods across nodes: two pods sharing a node
	// share its failure domain (kernel panic, kubelet crash, node drain).
	HostnameTopologyKey = "kubernetes.io/hostname"
	// ZoneTopologyKey spreads pods across availability zones - the
	// operator's default multi-AZ HA axis. The pulsar-helm-chart ships no
	// zone topologySpreadConstraints at all; this is the gap the operator
	// fills for every component by default.
	ZoneTopologyKey = "topology.kubernetes.io/zone"
)

// PodAntiAffinity returns node anti-affinity keyed on selector.
//
// hard (RequiredDuringSchedulingIgnoredDuringExecution) is for
// stateful/quorum tiers - bookie, oxia-server, dedicated autorecovery -
// where co-locating two replicas on one node defeats the purpose of
// replication: losing that one node can cost quorum outright.
//
// soft (PreferredDuringSchedulingIgnoredDuringExecution, weight 100) is for
// stateless tiers - broker, proxy, functionsworker, oxia-coordinator - where
// losing a node degrades capacity but never correctness, so scheduling must
// still succeed on a small or single-node cluster.
func PodAntiAffinity(hard bool, selector map[string]string) *corev1.Affinity {
	term := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
		TopologyKey:   HostnameTopologyKey,
	}

	antiAffinity := &corev1.PodAntiAffinity{}
	if hard {
		antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{term}
	} else {
		antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{
			{Weight: 100, PodAffinityTerm: term},
		}
	}
	return &corev1.Affinity{PodAntiAffinity: antiAffinity}
}

// ZoneTopologySpreadConstraints returns the operator's default zone-spread
// rule for a component's pods: maxSkew=1, soft (ScheduleAnyway) so a
// single-zone cluster (e.g. a local KIND cluster) still schedules instead of
// sticking pods Pending forever. Applied unconditionally to every tier.
func ZoneTopologySpreadConstraints(selector map[string]string) []corev1.TopologySpreadConstraint {
	return []corev1.TopologySpreadConstraint{
		{
			MaxSkew:           1,
			TopologyKey:       ZoneTopologyKey,
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: selector},
		},
	}
}

// QuorumMaxUnavailable bounds voluntary disruption for a majority-vote tier
// (oxia-server, dedicated autorecovery's Auditor leader election, or any
// future odd-replica quorum tier) to floor((replicas-1)/2): a majority of
// replicas always survives a voluntary disruption, so the tier never loses
// quorum to a node drain or cluster-autoscaler eviction on its own.
//
// With replicas<=2 this floors to 0 - no majority survives losing even one
// member of a one- or two-node group - so the PDB blocks all voluntary
// disruption until the tier is scaled to at least 3. That is the correct,
// conservative answer, not a bug: it is exactly what "protect the quorum"
// means below 3 replicas.
func QuorumMaxUnavailable(replicas int32) intstr.IntOrString {
	if replicas < 0 {
		replicas = 0
	}
	return intstr.FromInt32((replicas - 1) / 2)
}
