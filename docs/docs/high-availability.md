---
sidebar_position: 4
---

# High availability

:::caution WIP
HA defaults are designed but not yet implemented (see
[phase 5](./intro.md) of the project plan). This page documents the intended
defaults so they can be reviewed ahead of implementation.
:::

`pulsar-operator` aims to bake HA in by default, rather than leaving it as an
exercise for whoever installs the Helm chart. The upstream
`pulsar-helm-chart` ships **no** topology spread constraints and **no**
BookKeeper rack-awareness wiring — both are gaps this operator intends to
fill so a cluster is genuinely multi-AZ-safe out of the box.

## Anti-affinity and zone spread

- **Hostname anti-affinity:** hard (`requiredDuringScheduling`) for stateful
  tiers — bookie, Oxia server, autorecovery. Soft (`preferred`) for stateless
  tiers — broker, proxy.
- **Zone spread:** a default `topologySpreadConstraints` with `maxSkew: 1` on
  every tier, keyed on the node's zone label.

## Quorum-derived PodDisruptionBudgets

Each component gets a PDB whose `maxUnavailable` is computed from quorum math
rather than a fixed number, so voluntary disruption (node drains, cluster
upgrades) can't take out more replicas than the component can tolerate.

## BookKeeper rack-awareness

The operator sets `bookkeeperClientRackawarePolicyEnabled` and runs a small
rack-sync daemon (analogous to KAAP's `bkRackDaemon`) that writes each
bookie's rack — derived from its node's zone label — into BookKeeper's rack
metadata. Without this, ensemble placement has no idea which bookies share a
failure domain.

## Default replica counts

| Component | Default replicas | Notes |
|---|---|---|
| Oxia server | 3 | Odd count enforced by webhook |
| Oxia coordinator | 2 | |
| BookKeeper | 4 | |
| Broker | 3 | |
| Proxy | 3 | |
| AutoRecovery | 1 | Runs as its own StatefulSet, not embedded in bookies |

## Quorum math

The Pulsar production default for BookKeeper ensembles is
`ensembleSize=2, writeQuorum=2, ackQuorum=2` (`2/2/2`). For a genuinely
3-AZ-resilient deployment, the operator recommends `3/3/2` — write to 3
bookies, acknowledge on 2, which tolerates the loss of one entire
availability zone without blocking writes. The operator rejects
configurations where `ensembleSize` exceeds the bookie replica count.

Oxia is configured with `replicationFactor=3` and at least 2 coordinators for
the same reason.

## Stable identity for draining

Brokers and proxies run as `StatefulSet` + headless `Service` rather than a
plain `Deployment`, specifically so the autoscaler and rolling-upgrade logic
can drain a pod **by name** — draining "the highest ordinal" is only
meaningful if ordinals are stable.

## Ordered rolling upgrades

Upgrades follow a strict, dependency-aware order (a state machine modeled on
Strimzi's `KafkaRoller`, with the quorum-holding tier upgraded last among its
peers where applicable):

```
Oxia (metadata) → disable AutoRecovery → BookKeeper (canary, then verify
zero under-replication) → re-enable AutoRecovery → Broker (transfer-drained,
partition-canary) → Proxy → FunctionsWorker (last)
```

A spec-diff step restarts only the components whose configuration actually
changed, rather than rolling every tier on every reconcile.

## Related

- [Autoscaling model](./autoscaling.md) — the quorum checks above gate
  bookie scale-down.
- [Backup & DR](./backup-and-dr.md) — HA protects against losing *a*
  replica, not against losing *all* of them.
