package bookkeeper

import "time"

// QuorumHolds reports whether ensembleSize >= writeQuorum >= ackQuorum, the
// static invariant BookKeeper's ensemble placement depends on. All three
// values must be positive; zero or negative values are always unsatisfied
// (they indicate missing/invalid configuration, never a valid quorum).
func QuorumHolds(ensembleSize, writeQuorum, ackQuorum int32) bool {
	return ensembleSize > 0 && writeQuorum > 0 && ackQuorum > 0 &&
		ensembleSize >= writeQuorum && writeQuorum >= ackQuorum
}

// CapacityHoldsAfterRemoval reports whether the cluster still has enough
// writable bookies -- after removing the decommission target -- to stripe a
// full ensemble and still meet minWritableBookies. writableBookiesAfterRemoval
// must already exclude the target bookie.
func CapacityHoldsAfterRemoval(writableBookiesAfterRemoval, ensembleSize, minWritableBookies int32) bool {
	return writableBookiesAfterRemoval >= ensembleSize && writableBookiesAfterRemoval >= minWritableBookies
}

// PlacementSatisfiable reports whether rack/zone placement can still be
// satisfied after removing the decommission target, given the number of
// distinct zones observed among the remaining bookies (distinctZones) and how
// many of those remaining bookies actually carry a zone label at all
// (zoneLabelsSeen).
//
// When zoneLabelsSeen is zero, the cluster carries no zone topology
// information at all (e.g. a single-node kind cluster, or nodes that predate
// zone labeling): there is nothing to enforce, so placement is treated as
// satisfiable rather than blocking every decommission in clusters that never
// opted into zone-aware placement. When ensembleSize <= 1, zone spread isn't
// meaningful either. Otherwise, at least two distinct zones must remain,
// mirroring the "recommended 3/3/2 across 3 AZs, survive one AZ loss" HA
// guidance documented for BookKeeperEnsembleSpec.
func PlacementSatisfiable(distinctZones, zoneLabelsSeen int, ensembleSize int32) bool {
	if zoneLabelsSeen == 0 || ensembleSize <= 1 {
		return true
	}
	return distinctZones >= 2
}

// ScaleDownGuardInput is the set of cluster signals the automatic scale-down
// trigger evaluates every tick, gathered by the caller from the BookKeeper
// status and AdminClient so the decision itself can be tested without any
// Kubernetes or exec dependency.
type ScaleDownGuardInput struct {
	// WritableBookies is the current number of writable bookies.
	WritableBookies int32
	// MinWritableBookies is the configured (or ensemble-derived) floor.
	MinWritableBookies int32
	// LargestEnsembleSize is the largest ensembleSize configured across the
	// BookKeeper's ledger ensembles.
	LargestEnsembleSize int32
	// AllBelowLWM is true when every writable bookie's ledger-disk usage is
	// below diskUsageToleranceLwm.
	AllBelowLWM bool
	// NoUnderReplication is true when the cluster has zero under-replicated
	// ledgers.
	NoUnderReplication bool
}

// ShouldTriggerScaleDown implements the guarded scale-down trigger: more
// writable bookies than the (minWritableBookies, largestEnsembleSize) floor
// requires, every writable bookie below the low watermark, and zero
// cluster-wide under-replication. All three must hold; any one being false
// means no-op, never a partial/best-effort scale-down.
func ShouldTriggerScaleDown(in ScaleDownGuardInput) bool {
	floor := max(in.MinWritableBookies, in.LargestEnsembleSize)
	return in.WritableBookies > floor && in.AllBelowLWM && in.NoUnderReplication
}

// DecommissionTimedOut reports whether a decommission that started at
// startedAt has been running longer than timeoutSeconds as of now.
func DecommissionTimedOut(startedAt, now time.Time, timeoutSeconds int32) bool {
	return now.Sub(startedAt) > time.Duration(timeoutSeconds)*time.Second
}
