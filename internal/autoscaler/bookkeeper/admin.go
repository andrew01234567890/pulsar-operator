// Package bookkeeper implements the bookie admin operations the guarded
// bookie scale-down state machine
// (internal/controller/cluster/bookkeeper_decommission_controller.go) needs,
// behind an injectable AdminClient interface so every phase of that state
// machine can be unit tested against a mock instead of a real BookKeeper
// cluster. It also holds the state machine's pure decision logic (guard
// checks, timeouts) so those can be table-tested without any Kubernetes API
// dependency at all.
package bookkeeper

import "context"

// AdminClient is every bookie admin operation the guarded decommission state
// machine needs, abstracted behind an interface so tests can inject a mock
// instead of exec-ing into real pods. podName identifies the bookie's pod
// (e.g. "my-bookkeeper-3"); bookieID is the BookKeeper bookie identifier
// (host:port form) used by `bin/bookkeeper shell` commands.
type AdminClient interface {
	// IsWritable reports whether the bookie is currently writable (running,
	// not read-only, not shutting down).
	IsWritable(ctx context.Context, podName string) (bool, error)

	// LedgerDiskUsageBelow reports whether the bookie's ledger-disk usage
	// ratio is below tolerance (a fraction in [0,1]).
	LedgerDiskUsageBelow(ctx context.Context, podName string, tolerance float64) (bool, error)

	// SetReadOnly toggles the bookie's read-only admin flag. Called with
	// readOnly=false both to prepare a fresh bookie and to revert one whose
	// decommission failed or timed out.
	SetReadOnly(ctx context.Context, podName string, readOnly bool) error

	// TriggerDecommission runs `bin/bookkeeper shell decommissionbookie`
	// against the bookie to force re-replication of its ledgers off of it,
	// falling back to `recover -f` if decommissionbookie itself fails.
	TriggerDecommission(ctx context.Context, podName, bookieID string) error

	// HasLedgers reports whether any ledger fragments are still assigned to
	// the bookie.
	HasLedgers(ctx context.Context, podName, bookieID string) (bool, error)

	// NoUnderReplicatedLedgers reports whether the cluster currently has zero
	// under-replicated ledgers (queried from any one bookie's autorecovery
	// endpoint, since under-replication is a cluster-wide, not per-bookie,
	// signal).
	NoUnderReplicatedLedgers(ctx context.Context, podName string) (bool, error)

	// RenameCookie non-destructively invalidates the bookie's on-disk cookie
	// by renaming (never deleting) its VERSION file, so a failed/aborted
	// decommission stays diagnosable. Must be idempotent: it may be called
	// more than once for the same bookie if a prior call's result was
	// ambiguous (e.g. the remote command succeeded but the response was
	// lost).
	RenameCookie(ctx context.Context, podName string) error
}
