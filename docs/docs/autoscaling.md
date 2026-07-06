---
sidebar_position: 3
---

# Autoscaling model

:::caution WIP
The autoscalers described here are designed but not yet implemented (see
[phase 4](./intro.md) of the project plan). This page documents the target
algorithm so operators know what to expect and can review the design before
it ships.
:::

Unlike a generic HPA/KEDA setup, broker and bookie autoscaling are handled by
**dedicated reconcilers** that understand Pulsar- and BookKeeper-specific
signals — CPU alone doesn't tell you whether a broker is actually hot, and
disk usage alone doesn't tell you whether it's safe to remove a bookie.

## Broker autoscaling

- **Loop:** one reconcile pass per broker set every `periodMs` (default 60s).
- **Metric:** Pulsar's own load-report CPU
  (`/admin/v2/broker-stats/load-report`, `resourcesUsageSource=PulsarLBReport`),
  refined to mirror Pulsar's `ThresholdShedder` composite signal —
  `max(cpu, bandwidthIn, bandwidthOut)` smoothed with an EMA of 0.9. This is
  **not** raw kubelet/cgroup CPU; it reflects what Pulsar's own load balancer
  considers "hot."
- **Decision rule — unanimous:**
  - Scale **up** only if **every** broker's CPU signal is above
    `higherCpuThreshold = 0.8`.
  - Scale **down** only if **every** broker's CPU signal is below
    `lowerCpuThreshold = 0.3`.
  - Otherwise, no-op. A single hot or idle broker never triggers a scaling
    event on its own — this avoids oscillation from one noisy neighbor.
- **Step size:** `scaleUpBy` / `scaleDownBy` = 1 broker at a time.
- **Stabilization:** a 5-minute gate after any scaling event (all pods must
  be `Ready`) before another decision is made, plus a `min` replica clamp.
- **Scale-down drain:** the highest-ordinal broker is drained via the
  `ExtensibleLoadManagerImpl` + `TransferShedder`
  (`loadBalancerTransferEnabled=true`) — bundles are **live-transferred**,
  not hard-unloaded, which avoids the reconnect storm a naive unload would
  cause. The operator waits for the broker to own zero bundles before
  terminating the pod, or auto-reverts the scale-down and raises a `Warning`
  condition if it doesn't happen in time.

  Live transfer is a real improvement over unload-based draining, but it is
  **not fully transparent** to clients — in-flight requests can still be
  affected. Don't oversell this as zero-downtime; it's "no reconnect storm,"
  not "no impact."

## Bookie autoscaling

Bookies are polled per-tick via their admin REST API
(`/api/v1/bookie/state`, `/bookie/info`,
`/autorecovery/list_under_replicated_ledger`), and the decision is made with
strict priority, checked in order:

1. **Deficit scale-up:** if `writableBookies < minWritableBookies`
   (computed from the largest configured ensemble size across namespaces,
   not a hardcoded constant), scale up by the deficit immediately.
2. **Watermark scale-up:** else, if any writable bookie's ledger-disk usage
   is at or above the high watermark (**HWM = 0.92**), scale up pre-emptively
   — before the cluster is forced into scenario 1.
3. **Guarded scale-down:** only if **every** writable bookie's disk usage is
   below the low watermark (**LWM = 0.75**) **and** there are zero
   under-replicated ledgers cluster-wide.

Scale-up always wins ties with scale-down.

### Safe bookie scale-down (default OFF)

Bookie scale-down is the highest-risk operation this operator performs,
because a mistake here can lose data. It's a serialized, resumable state
machine, off by default and opt-in per cluster:

1. Re-verify `ensembleSize >= writeQuorum >= ackQuorum` and rack placement
   still hold with one fewer bookie.
2. Mark the target bookie read-only.
3. Run `bin/bookkeeper shell decommissionbookie`.
4. Block until the bookie has zero ledgers **and** cluster-wide
   under-replication is zero — with a timeout that auto-reverts the bookie
   to writable if re-replication doesn't finish in time.
5. **Rename** (never delete) the bookie's cookie `VERSION` file.
6. Scale the StatefulSet down by one ordinal.
7. The operator deletes that bookie's PVC itself — it never relies on
   StatefulSet PVC-retention policy to do this, because that policy's
   defaults and timing aren't a substitute for a verified-safe decommission.

Progress is surfaced as a `Decommissioning` condition plus Kubernetes Events.
The same state machine is exposed as an on-demand manual drain (via an
annotation / subresource) for cases like planned node maintenance.

## Related

- [High availability](./high-availability.md) for the quorum math
  (`ensembleSize`/`writeQuorum`/`ackQuorum`) referenced above.
- [Backup & DR](./backup-and-dr.md) — autoscaling protects capacity and
  headroom, not durability.
