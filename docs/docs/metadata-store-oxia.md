---
sidebar_position: 6
---

# Metadata store (Oxia)

`pulsar-operator` is **Oxia-only** — there is no ZooKeeper CRD, and none is
planned as a first-class option. [Oxia](https://github.com/oxia-db/oxia) is
the metadata store that replaces ZooKeeper for Pulsar (landed as PIP-335 in
Pulsar 3.3.0, and becomes the **default** metadata store in Pulsar 5.0). The
`metadataStore` field on `PulsarCluster` is kept as an abstraction so a
future store could theoretically be added without an API break, but Oxia is
the only implementation today.

## Why Oxia instead of ZooKeeper

ZooKeeper was never designed for Pulsar's access patterns, and running it
well requires its own body of operational knowledge (JVM tuning, ensemble
sizing, `zxid` rollover, etc.) that's mostly orthogonal to running Pulsar
itself. Oxia is purpose-built for Pulsar's metadata workload, is written to
be simpler to operate at Kubernetes-native scale, and — most relevantly for
this project — has a coordinator/server split that maps cleanly onto
Kubernetes primitives (a `Deployment` for coordination, a `StatefulSet` for
data).

## Topology: coordinator + server

Oxia splits into two roles, and `pulsar-operator`'s `OxiaCluster` CRD
provisions both:

- **Coordinator** — a `Deployment` (`replicas >= 2`) that performs leader
  election (via a `ConfigMap` + RBAC `Role`/`RoleBinding`) and owns shard
  assignment. The coordinator does not hold data itself.
- **Server** — a `StatefulSet` with an odd replica count (enforced by
  webhook), each pod with its own PVCs for `data-dir` and `wal-dir`. Servers
  are configured with `db-cache-size-mb` and `wal-sync-data=true` for
  durability, and `allowExtraAuthorities` for in-cluster TLS SAN handling.

Each namespace Oxia manages for Pulsar (`default`, `broker`, `bookkeeper`) is
configured with `initialShardCount=3` and `replicationFactor=3` by default.

## The critical operator duty: keeping the coordinator's server list current

The coordinator's view of `servers` is a **static ConfigMap**, not something
it discovers dynamically. This means the operator has a specific
responsibility on every Oxia server replica-count change: regenerate that
ConfigMap **and** reload/restart the coordinator so shards get rebalanced
across the new server set. Scaling the server `StatefulSet` happens in odd
steps, with a quorum-aware rolling restart — the same category of problem
Strimzi solves for Kafka with `KafkaQuorumCheck`.

## Health and observability

- `oxia health` probes back the coordinator and server pods.
- Metrics are scraped from `:8080/metrics` via a `PodMonitor`.

## Wiring Pulsar to Oxia

Once an `OxiaCluster` is up, the operator wires the rest of the cluster to
it:

```sh
bin/pulsar initialize-cluster-metadata \
  --metadata-store oxia://<coordinator-svc>:6648/<namespace> \
  --configuration-store oxia://<coordinator-svc>:6648/<namespace>
```

- **Broker / Proxy:** `metadataStoreUrl=oxia://<coordinator-svc>:6648/<namespace>`
- **BookKeeper:** `metadataServiceUri=metadata-store:oxia://<coordinator-svc>:6648/bookkeeper`
  (this requires `-Dbookkeeper.metadata.bookie.drivers`, which the
  `bin/bookkeeper` entrypoint sets by default).

## Known risk: BookKeeper's Oxia bridge is unproven at scale

Oxia rides a generic BookKeeper metadata bridge — there is no in-tree,
purpose-built Oxia driver in BookKeeper today. This means BookKeeper's
`Auditor` leader-election and `LedgerUnderreplicationManager` locks/watches
running on the `oxia://` driver need to be **proven end-to-end, not
assumed** to behave like they do on ZooKeeper. This is treated as the single
highest Oxia-specific risk in the project and is planned to be validated via
a full autorecovery + `decommissionbookie` end-to-end test against an
Oxia-backed cluster before the operator is considered scale-ready.

## Related

- [Backup & DR](./backup-and-dr.md) — Oxia's lack of native snapshot/export
  is the most important thing to understand about running this operator.
- [High availability](./high-availability.md) — Oxia's
  `replicationFactor` and coordinator count defaults.
