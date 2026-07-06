---
sidebar_position: 1
---

# Introduction

`pulsar-operator` is a Kubernetes operator that manages the full lifecycle of
[Apache Pulsar](https://pulsar.apache.org/) clusters and autoscales them based
on real workload signals — bookie disk usage, broker CPU/throughput. It's
modeled on the ideas behind DataStax [KAAP](https://github.com/datastax/kaap)
("Kubernetes Autoscaling for Apache Pulsar"), rebuilt from scratch in Go and
native to [Oxia](https://github.com/oxia-db/oxia), the metadata store that
replaces ZooKeeper.

:::caution Status: early development
APIs are `v1alpha1` and will change without notice. The operator is **not yet
production-ready** — see the [Quickstart](./quickstart.md) for what actually
works today. This site documents the target design as much as the current
state; pages that describe unbuilt functionality are marked **WIP**.
:::

## Why

Running Pulsar on Kubernetes means operating several stateful subsystems —
bookies (BookKeeper), brokers, a proxy, the autorecovery daemon, and a
metadata store — each with its own scaling, placement, and upgrade rules.
Existing tooling either stops at templating (Helm) or is tied to the JVM and
ZooKeeper. `pulsar-operator` aims to:

- **Manage everything through CRDs** — declarative provisioning, config,
  rolling upgrades, and HA for every component.
- **Autoscale safely** — grow bookies before disks fill, grow brokers under
  load, and *never* lose data on scale-down (guarded, opt-in bookie
  decommission).
- **Be Oxia-native** — deploy and wire Oxia (coordinator + servers) as a
  first-class metadata store, not an afterthought.

It targets the Apache Pulsar `5.0.0-M1` preview, the first release where Oxia
is the default metadata store.

## The CRD graph

One umbrella CRD, `PulsarCluster`, decomposes into per-component child CRDs
that each reconcile independently:

```
PulsarCluster (cluster.pulsaroperator.io)
├── OxiaCluster     (metadata.pulsaroperator.io) — coordinator Deployment + server StatefulSet
├── BookKeeper      — bookie StatefulSet (journal/ledgers/index disks) + disk-usage autoscaler
├── Broker          — broker StatefulSet + CPU/throughput autoscaler
├── Proxy           — optional stateless gateway
├── AutoRecovery    — Auditor + ReplicationWorker
└── FunctionsWorker — Pulsar Functions (co-located or standalone)
```

A `PulsarCluster` reconciler reads the umbrella spec, decomposes global and
per-component configuration into the child custom resources, and aggregates
each child's status back onto `PulsarCluster.status.conditions`. Every child
CRD can also be read (and, carefully, edited) directly, but day-to-day usage
is expected to go through the umbrella resource.

## Feature scope (v1)

- **Broker autoscaling** on Pulsar load-report CPU (a composite CPU/bandwidth
  signal), unanimous thresholds with a stabilization window, and a live
  bundle-transfer drain on scale-down.
- **Bookie autoscaling** on ledger-disk high/low watermarks; scale-up is
  automatic, scale-**down** is a guarded, opt-in, resumable decommission
  state machine.
- **Oxia-native metadata**: coordinator + server split, shard rebalancing on
  scale, quorum-aware rolling restarts.
- **HA by default**: anti-affinity, zone topology spread, quorum-derived
  PodDisruptionBudgets, BookKeeper rack-awareness synced from node zones,
  ordered rolling upgrades.
- **Tiered storage** (S3/GCS/Azure/filesystem offload) configured via CRD —
  this is retention tiering, not a backup mechanism; see
  [Backup & DR](./backup-and-dr.md).

Explicitly out of scope for v1: geo-replication and automated backup (see
[Backup & DR](./backup-and-dr.md) for why that matters and how to mitigate
it yourself in the meantime).

## Where to go next

- [Quickstart](./quickstart.md) — try it on a local `kind` cluster.
- [Autoscaling model](./autoscaling.md) — how broker and bookie autoscaling decide when to act.
- [High availability](./high-availability.md) — anti-affinity, zone spread, PDBs, rolling upgrades.
- [Backup & DR](./backup-and-dr.md) — read this before you trust the cluster with data you care about.
- [Metadata store (Oxia)](./metadata-store-oxia.md) — coordinator/server topology and why Oxia.
- [CRD API reference](./crd-api-reference.md) — placeholder, auto-generated docs land in a later change.
- [KAAP comparison](./kaap-comparison.md) — how this project differs from DataStax KAAP.
