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
ledger data on disk.** There is no supported recovery path for this in v1 —
and, as the next section spells out, losing Oxia is actually a *harder*
problem than "the cluster forgets where its data is." This is not a
theoretical edge case — it's the direct result of running a single metadata
store with no export tooling, so treat Oxia's durability as the durability of
the entire cluster.

## Hazard: losing Oxia bricks bookies at the boot layer, not just the metadata layer

Oxia doesn't just hold "where the data is." Through the generic
`metadata-store:oxia://` bridge (see
[Metadata store (Oxia)](./metadata-store-oxia.md)), Oxia is also
BookKeeper's `RegistrationManager` backing store, and that store holds two
things every bookie checks **before it will start serving**:

- A cluster-wide **`instanceId`**, written once at
  `<ledgersRootPath>/INSTANCEID` when the cluster is first initialized.
- A **per-bookie cookie registration record** — the registration manager's
  copy of the same cookie (bookie ID, journal/ledger directories, and that
  `instanceId`) each bookie also stamps onto its own disks.

On every startup, a bookie fetches the `instanceId` from whatever metadata
store it's pointed at, builds a cookie from its local config, and compares it
against both the on-disk cookie and the registration manager's copy. If
either doesn't match, BookKeeper's cookie validation fails hard with
`InvalidCookieException` and the bookie refuses to start.

**This is exactly what happens if you restore Oxia metadata into a fresh
Oxia instance.** Standing up a new Oxia store and re-running
`bin/pulsar initialize-cluster-metadata` mints a **brand-new** `instanceId`.
Every surviving bookie's on-disk cookie still carries the *old* `instanceId`
— so every bookie now disagrees with the metadata store and fails
`InvalidCookieException` on boot, **fleet-wide**, even though every bookie's
disks and ledger data are completely intact and untouched. Losing Oxia
doesn't just cost you metadata; without care, it bricks a perfectly healthy
BookKeeper fleet at the boot layer, before a single ledger is even opened.

**What this means for restore:** metadata restore must preserve
`instanceId`/cookie **lineage** — the restored (or freshly stood-up) Oxia has
to report the same `instanceId` the surviving bookies were stamped with, not
a new one. Where that isn't achievable, the only supported way through the
mismatch is BookKeeper's own escape hatch, `bin/bookkeeper shell
updatecookie` — a manual, per-bookie, risk-acknowledged override that
rewrites the on-disk and/or registration-manager cookie. It exists precisely
for this class of mismatch, but it's an admin operating a scalpel, not a step
to script into an unattended restore: run it deliberately, one bookie at a
time, understanding exactly what identity you're overriding.

## Hazard: backup ordering — capture BookKeeper at-or-after Oxia, never before

Oxia and BookKeeper have no transactional coupling between them. A broker
appends entries to a bookie's ledger *first*; only afterwards — on ledger
roll-over or periodic checkpoint — does it update that topic's
managed-ledger info (the list of ledgers and last-confirmed positions) in
Oxia. Metadata is always a step behind the data it describes, never ahead of
it — *provided the backup is taken in the right order.*

That gives a strict rule for any two-store snapshot: **the BookKeeper (data)
snapshot must be taken at the same instant as, or strictly after, the Oxia
(metadata) snapshot — never before.** If the Oxia snapshot ends up newer than
the BookKeeper snapshot, the restored metadata can point at ledgers or
entries the older BookKeeper copy never persisted. That failure mode doesn't
announce itself: the topic looks fine, most reads succeed, and only specific
ledgers or offset ranges come back missing or unreadable — discovered
whenever something finally tries to read them. That's arguably worse than
the hard, fleet-wide boot failure above, because it's silent, partial, and
surfaces late.

Always snapshot in this order: **Oxia first, BookKeeper second (or exactly
simultaneously) — never BookKeeper first.**

## Hazard: no point-in-time consistency without a full quiesce — and a real RPO gap

Even respecting the ordering rule above, an unquiesced backup is only
**crash-consistent per store**, not point-in-time consistent:

- Oxia itself has no cross-shard transactions. The `default`, `broker`, and
  `bookkeeper` namespaces `pulsar-operator` provisions — and the shards
  within each — are each independently replicated. Snapshotting them
  independently gives you a set of per-shard-consistent copies, not one
  wall-clock-consistent copy of "Oxia."
- Across Oxia and BookKeeper together, there's even less: no supported way to
  get one consistent instant spanning both stores without a full
  **stop-the-world quiesce** (writers paused across brokers, bookies, and
  Oxia) for the duration of the snapshot.

Without that quiesce, treat any backup — logical or physical — as
crash-consistent-per-store at best: good enough to reconstruct structure,
not guaranteed to reconstruct the exact instant it claims to represent.

**The RPO gap this leaves, stated plainly:** a metadata-only backup, restored
against a fresh BookKeeper, recovers topic/namespace/schema/cursor structure,
and reattaches correctly to any data that was **fully offloaded to tiered
storage** before the snapshot — that data stays reachable through the
restored managed-ledger pointers. It **permanently loses every message that
was not offloaded-and-complete at snapshot time** — in practice, the hot,
recent tail of every topic, since offload only runs after a ledger closes and
crosses `offloadThresholdBytes`. A metadata-only backup recovers the catalog
and the cold tier; it is not a substitute for BookKeeper's own durability
over the hot tail.

## What v1 does and does not do

| | v1 status |
|---|---|
| BookKeeper replication (ensemble/quorum) | In HA, built in |
| Tiered storage / offload | In v1, CRD-managed — retention tiering only |
| Oxia replication (`replicationFactor`) | In HA, built in — protects against losing *some* replicas |
| Oxia backup / snapshot export | **Not available** — no upstream primitive to build on yet |
| Automated backup (any component) | **Descoped for v1** — a metadata-only `Backup`/`Restore`/`BackupSchedule` CRD family is planned; see below |
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
3. **If you rehearse or perform a restore, preserve `instanceId`/cookie
   lineage and respect snapshot ordering.** Restoring metadata into a Oxia
   that mints a fresh `instanceId` bricks every surviving bookie (see the
   boot-layer hazard above); restoring a BookKeeper copy that's older than
   the Oxia copy it's paired with can silently orphan ledger ranges (see the
   ordering hazard above). A restore drill that doesn't check both of these
   is not a validated restore drill.
4. Don't treat tiered storage as a substitute for any of the above.

## What's coming: `Backup` / `Restore` / `BackupSchedule` CRDs

The operator is adding a new CRD family — `Backup`, `Restore`, and
`BackupSchedule` — under a new `backup.pulsaroperator.io` group, following
the same per-concern group split as `cluster.pulsaroperator.io` and
`metadata.pulsaroperator.io`. None of this is implemented yet; it's described
here so the plan is judged on what it will and won't actually fix, not on
what it sounds like it fixes.

- **What it does:** `Backup` performs a **logical export of Oxia's
  metadata** — topics, namespaces, schemas, cursors, managed-ledger/ledger
  pointers — to an object store (S3, GCS, or Azure Blob), reusing the same
  driver/bucket/region/`credentialsSecretRef` wiring as tiered-storage
  `OffloadSpec` rather than inventing a second object-store config surface.
  `BackupSchedule` runs this on a recurring schedule and retains a history of
  prior exports; `Restore` replays a chosen export back into a target Oxia
  instance.
- **Crash-consistent by default.** The export reads Oxia's key-value state
  live; it does not stop-the-world quiesce the cluster. That means it
  inherits the consistency hazard described above: a crash-consistent
  snapshot per store/shard, not a guaranteed point-in-time snapshot across
  all of them.
- **What restoring it recovers — and what it doesn't.** This is
  metadata-centric DR, not full-cluster DR. Restoring a `Backup` into fresh
  Oxia gets back topic/namespace/schema/cursor structure and reattaches to
  fully-offloaded tiered data — exactly the RPO boundary described above.
  The hot tail that hadn't finished offloading at snapshot time is not part
  of this backup and is not recoverable from it. And restoring metadata
  alone does not, by itself, resolve the boot-layer cookie hazard: if
  surviving bookies are meant to reattach to the restored cluster,
  `instanceId`/cookie lineage still has to be preserved or manually
  reconciled — this CRD family exports Oxia's metadata, it does not (yet)
  manage cookie continuity for you.
- **What it isn't:** a substitute for geo-replication. It protects the
  metadata store; it does not give you a live, independent second copy of
  the cluster in another failure domain. Correlated total-cluster loss —
  where you need a genuinely separate cluster to fail over to — is still
  only addressed by `PulsarGeoReplication`, deferred to v2 below.

## What's planned for v2

- `PulsarGeoReplication` CRD for cross-cluster replication — the only
  mitigation planned for correlated total-cluster loss (an entire
  region/cluster gone), which the metadata-only `Backup` family above
  cannot address.
- Physical, data-layer backup: operator-managed `VolumeSnapshot` CronJobs for
  BookKeeper (and potentially Oxia) PVCs, to narrow the hot-tail RPO gap that
  a logical, metadata-only `Backup` cannot close on its own.
- Optional Velero quiesce hooks — the mechanism that would upgrade both the
  physical snapshots above and the logical `Backup` export from
  crash-consistent to genuinely point-in-time consistent.

None of this exists yet. If your workload can't tolerate the "one bad day
loses everything" risk above, wait for it, or build your own snapshot
automation now rather than assuming the operator has your back.
