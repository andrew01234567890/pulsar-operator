---
sidebar_position: 3
---

# Autoscaling model

:::caution WIP
Bookie autoscaling is designed but not yet implemented (see
[phase 4](./intro.md) of the project plan) - the bookie section below
documents the target algorithm so operators know what to expect and can
review the design before it ships. **Broker autoscaling is implemented**
(`BrokerAutoscalerReconciler`); its section below describes shipped
behavior, not a design target.
:::

Unlike a generic HPA/KEDA setup, broker and bookie autoscaling are handled by
**dedicated reconcilers** that understand Pulsar- and BookKeeper-specific
signals — CPU alone doesn't tell you whether a broker is actually hot, and
disk usage alone doesn't tell you whether it's safe to remove a bookie.

## Broker autoscaling

A dedicated `BrokerAutoscalerReconciler` watches `Broker` objects
independently of the main Broker reconciler, gated on
`spec.autoscaler.enabled`.

- **Loop:** one reconcile pass per Broker every `periodSeconds` (default 60s).
- **Metric:** pluggable behind a `BrokerLoadClient` interface (unit tests
  substitute a mock), with two built-in sources selected by
  `resourcesUsageSource`:
  - `PulsarLBReport` (default) - each broker's own
    `/admin/v2/broker-stats/load-report` `cpu: {usage, limit}` pair, turned
    into a whole-number percent. This is what Pulsar's own load manager
    considers "hot," not raw kubelet/cgroup CPU.
  - `K8SMetrics` - the metrics-server aggregated API, expressed as a percent
    of the pod's own CPU limit.
- **Decision rule — unanimous:**
  - Scale **up** only if **every** broker's CPU percent is strictly above
    `higherCpuThreshold` (default 80).
  - Scale **down** only if **every** broker's CPU percent is strictly below
    `lowerCpuThreshold` (default 30).
  - Otherwise (a broker in between the thresholds, or a mix of hot and cold
    brokers), no-op. A single hot or idle broker never triggers a scaling
    event on its own — this avoids oscillation from one noisy neighbor.
- **Step size:** `scaleUpBy` / `scaleDownBy` (default 1 broker at a time),
  clamped to `[min, max]` (`min` defaults to 2; `max` is unbounded if unset).
- **Stabilization:** the decision is skipped unless every broker pod is
  currently `Ready` *and* `stabilizationWindowSeconds` (default 300) have
  elapsed since `status.lastScaleTime`.
- **Observability:** every tick (scale or no-op) updates an `Autoscaling`
  status condition explaining the outcome
  (`ScaleUp`/`ScaleDown`/`MixedSignals`/`AtReplicaBound`/`PodsNotReady`/
  `AwaitingStabilization`/`MetricsUnavailable`/`Disabled`); an actual scale
  additionally records `status.lastScaleTime` and emits a Kubernetes Event.

**Scale-down is a plain replica-count decrease** — the autoscaler does not
pick which ordinal drains first or wait for zero owned bundles. It relies
entirely on the Broker reconciler's existing preStop sleep +
`terminationGracePeriodSeconds` (sized to Pulsar's own
`brokerShutdownTimeoutMs` shutdown hook) to unload the terminating pod's
bundles before the process is killed. That is a **graceful shutdown, not a
live bundle-transfer handover** (`ExtensibleLoadManagerImpl`'s
`TransferShedder`) - in-flight requests to the terminating broker can still
be affected. Don't oversell this as zero-downtime; it's "graceful," not
"no impact."

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
