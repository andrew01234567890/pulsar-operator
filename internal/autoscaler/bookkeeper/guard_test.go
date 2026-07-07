package bookkeeper

import (
	"testing"
	"time"
)

func TestQuorumHolds(t *testing.T) {
	tests := []struct {
		name                                 string
		ensembleSize, writeQuorum, ackQuorum int32
		want                                 bool
	}{
		{"healthy 3/2/2", 3, 2, 2, true},
		{"healthy 3/3/2 (recommended for 3-AZ)", 3, 3, 2, true},
		{"equal all around", 2, 2, 2, true},
		{"writeQuorum exceeds ensembleSize", 2, 3, 2, false},
		{"ackQuorum exceeds writeQuorum", 3, 2, 3, false},
		{"zero ensembleSize", 0, 2, 2, false},
		{"zero writeQuorum", 3, 0, 2, false},
		{"zero ackQuorum", 3, 2, 0, false},
		{"negative ensembleSize", -1, 2, 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := QuorumHolds(tt.ensembleSize, tt.writeQuorum, tt.ackQuorum); got != tt.want {
				t.Errorf("QuorumHolds(%d,%d,%d) = %v, want %v", tt.ensembleSize, tt.writeQuorum, tt.ackQuorum, got, tt.want)
			}
		})
	}
}

func TestCapacityHoldsAfterRemoval(t *testing.T) {
	tests := []struct {
		name                                            string
		writableAfterRemoval, ensembleSize, minWritable int32
		want                                            bool
	}{
		{"comfortably above both floors", 5, 3, 3, true},
		{"exactly at ensembleSize floor", 3, 3, 3, true},
		{"below ensembleSize floor", 2, 3, 3, false},
		{"above ensembleSize but below minWritableBookies", 3, 3, 4, false},
		{"minWritableBookies is the binding floor", 4, 3, 4, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CapacityHoldsAfterRemoval(tt.writableAfterRemoval, tt.ensembleSize, tt.minWritable)
			if got != tt.want {
				t.Errorf("CapacityHoldsAfterRemoval(%d,%d,%d) = %v, want %v",
					tt.writableAfterRemoval, tt.ensembleSize, tt.minWritable, got, tt.want)
			}
		})
	}
}

func TestPlacementSatisfiable(t *testing.T) {
	tests := []struct {
		name                          string
		distinctZones, zoneLabelsSeen int
		ensembleSize                  int32
		want                          bool
	}{
		{"no zone topology info at all: nothing to enforce", 0, 0, 3, true},
		{"ensembleSize 1: zone spread not meaningful", 1, 5, 1, true},
		{"two distinct zones remain, ensemble 3", 2, 5, 3, true},
		{"three distinct zones remain, ensemble 3", 3, 5, 3, true},
		{"only one zone remains, ensemble 3: unsatisfiable", 1, 5, 3, false},
		{"zero zones remain but labels were seen: unsatisfiable", 0, 3, 3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlacementSatisfiable(tt.distinctZones, tt.zoneLabelsSeen, tt.ensembleSize)
			if got != tt.want {
				t.Errorf("PlacementSatisfiable(%d,%d,%d) = %v, want %v",
					tt.distinctZones, tt.zoneLabelsSeen, tt.ensembleSize, got, tt.want)
			}
		})
	}
}

func TestShouldTriggerScaleDown(t *testing.T) {
	tests := []struct {
		name string
		in   ScaleDownGuardInput
		want bool
	}{
		{
			name: "happy path: surplus writable, all below LWM, no under-replication",
			in: ScaleDownGuardInput{
				WritableBookies: 5, MinWritableBookies: 3, LargestEnsembleSize: 3,
				AllBelowLWM: true, NoUnderReplication: true,
			},
			want: true,
		},
		{
			name: "no surplus writable bookies: floor is ensembleSize",
			in: ScaleDownGuardInput{
				WritableBookies: 3, MinWritableBookies: 3, LargestEnsembleSize: 3,
				AllBelowLWM: true, NoUnderReplication: true,
			},
			want: false,
		},
		{
			name: "surplus over minWritableBookies but not over the larger ensembleSize floor",
			in: ScaleDownGuardInput{
				WritableBookies: 4, MinWritableBookies: 2, LargestEnsembleSize: 4,
				AllBelowLWM: true, NoUnderReplication: true,
			},
			want: false,
		},
		{
			name: "under-replication present: must block regardless of disk usage",
			in: ScaleDownGuardInput{
				WritableBookies: 5, MinWritableBookies: 3, LargestEnsembleSize: 3,
				AllBelowLWM: true, NoUnderReplication: false,
			},
			want: false,
		},
		{
			name: "a writable bookie at/above LWM: must block",
			in: ScaleDownGuardInput{
				WritableBookies: 5, MinWritableBookies: 3, LargestEnsembleSize: 3,
				AllBelowLWM: false, NoUnderReplication: true,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldTriggerScaleDown(tt.in); got != tt.want {
				t.Errorf("ShouldTriggerScaleDown(%+v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestDecommissionTimedOut(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		elapsedSeconds int
		timeoutSeconds int32
		want           bool
	}{
		{"well within timeout", 10, 1800, false},
		{"exactly at timeout: not yet timed out", 1800, 1800, false},
		{"one second past timeout", 1801, 1800, true},
		{"far past timeout", 7200, 1800, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := start.Add(time.Duration(tt.elapsedSeconds) * time.Second)
			if got := DecommissionTimedOut(start, now, tt.timeoutSeconds); got != tt.want {
				t.Errorf("DecommissionTimedOut() = %v, want %v", got, tt.want)
			}
		})
	}
}
