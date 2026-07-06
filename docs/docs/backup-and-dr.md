---
sidebar_position: 5
---

# Backup & DR

**Read this page before you put data you care about into a cluster managed
by `pulsar-operator`.** It is the single most important caveat in this
project.

## Tiered storage is not backup

`pulsar-operator` supports tiered storage / offload (S3, GCS, Azure Blob, or
filesystem) as a first-class, CRD-configured feature. It is **cost and
retention tiering, not a backup mechanism**:

- Offloaded ledger data is only reachable through Pulsar's own metadata
  (managed-ledger pointers, topic ownership records). If that metadata is
  lost, the offloaded objects are orphaned — there's no independent catalog
  that lets you reconstruct a topic from the object store alone.
- Deleting a topic deletes its offloaded data too. Offload is a storage tier
  for an existing topic, not a point-in-time recovery mechanism.

## Oxia is TIER-0, and it has no native snapshot

This is the load-bearing fact for this whole page: **Oxia holds Pulsar's
authoritative metadata** — managed-ledger pointers, topic ownership, schemas,
cursors, everything BookKeeper and the brokers depend on to make sense of the
ledger data they hold. As of Oxia v0.16.x, **there is no native
snapshot/export operation**. Oxia's internal `Snapshot`/`Restore` machinery
exists purely for follower catch-up during normal replication — it is not a
user-facing backup primitive.

**Consequence:** if you lose all Oxia replicas at once, the cluster is
**bricked, even if every BookKeeper bookie survives intact with all its
ledger data on disk.** There is no supported recovery path for this in v1.
This is not a theoretical edge case — it's the direct result of running a
single metadata store with no export tooling, so treat Oxia's durability as
the durability of the entire cluster.

## What v1 does and does not do

| | v1 status |
|---|---|
| BookKeeper replication (ensemble/quorum) | In HA, built in |
| Tiered storage / offload | In v1, CRD-managed — retention tiering only |
| Oxia replication (`replicationFactor`) | In HA, built in — protects against losing *some* replicas |
| Oxia backup / snapshot export | **Not available** — no upstream primitive to build on yet |
| Automated backup (any component) | **Descoped for v1** |
| Geo-replication | **Deferred to v2** (`PulsarGeoReplication`, not yet designed in detail) |

Durability from replication (BookKeeper quorums, Oxia's
`replicationFactor`) is not the same thing as backup. Replication protects
against losing *some* replicas to routine failures. It does nothing for
correlated failures — an entire Oxia StatefulSet's PVCs deleted by mistake,
a namespace wiped, a storage-class-wide outage — that take out every replica
at once.

## What to do about it today

Until the operator ships its own backup tooling, mitigate this yourself:

1. **Run Oxia with `replicationFactor >= 3` spread across at least 3
   availability zones.** This is the single highest-leverage thing you can
   do — it's what stands between "one AZ had a bad day" and "the cluster is
   gone."
2. **Take your own scheduled volume snapshots of the Oxia server PVCs**
   (CSI `VolumeSnapshot`, on whatever schedule your storage provider
   supports). This is currently a manual/operational responsibility, not
   something the operator automates.
3. Don't treat tiered storage as a substitute for either of the above.

## What's planned for v2

- `PulsarGeoReplication` CRD for cross-cluster replication.
- Operator-managed `VolumeSnapshot` CronJobs for Oxia (and potentially
  BookKeeper) PVCs.
- Optional Velero quiesce hooks for coordinated, application-consistent
  snapshots.

None of this exists yet. If your workload can't tolerate the "one bad day
loses everything" risk above, wait for it, or build your own snapshot
automation now rather than assuming the operator has your back.
