---
sidebar_position: 8
---

# KAAP comparison

`pulsar-operator` is modeled on the ideas behind DataStax
[KAAP](https://github.com/datastax/kaap) ("Kubernetes Autoscaling for Apache
Pulsar") — the broker CPU-threshold autoscaling logic and the bookie
disk-watermark scaling logic both trace directly back to KAAP's algorithms.
It is a greenfield rewrite, not a fork, and diverges in a few deliberate
ways.

## Language and framework

| | KAAP | pulsar-operator |
|---|---|---|
| Language | Java | Go |
| Framework | Java Operator SDK (JOSDK) | [kubebuilder](https://book.kubebuilder.io/) v4 + `sigs.k8s.io/controller-runtime` |

Go/kubebuilder was chosen for consistency with the broader Kubernetes
operator ecosystem's tooling (controller-gen, envtest, kustomize) and to
avoid running a JVM purely to run the operator itself — the Pulsar
components it manages are JVM-based regardless.

## Metadata store

This is the largest architectural difference. **KAAP has zero Oxia
references** — it's built entirely around ZooKeeper, with no Oxia CRD or
support. `pulsar-operator` is **Oxia-only**: there is no ZooKeeper CRD at
all. See [Metadata store (Oxia)](./metadata-store-oxia.md) for the full
rationale and the topology this implies (coordinator + server, versus
KAAP/ZooKeeper's single ensemble concept).

Practically, this means:

- No ZooKeeper ensemble sizing/tuning surface.
- A coordinator/server split with its own scaling and rebalancing concerns
  (see [Metadata store (Oxia)](./metadata-store-oxia.md#the-critical-operator-duty-keeping-the-coordinators-server-list-current)).
- An open, tracked risk: BookKeeper's Oxia metadata bridge is newer and less
  battle-tested than its ZooKeeper path (see the same page).

## Autoscaling: same algorithms, refined mechanics

The broker and bookie autoscaling **decision logic** (unanimous CPU
thresholds for brokers; HWM/LWM disk watermarks and priority ordering for
bookies) is verified against and consistent with KAAP's approach — see
[Autoscaling model](./autoscaling.md).

The refinement is in **how scale-down drains a broker**: KAAP performs a
hard bundle unload. `pulsar-operator` uses Pulsar's
`ExtensibleLoadManagerImpl` + `TransferShedder`
(`loadBalancerTransferEnabled=true`) to live-transfer bundles instead,
avoiding the reconnect storm a hard unload causes. It's still not fully
transparent to clients — see the caveat in
[Autoscaling model](./autoscaling.md#broker-autoscaling).

## CRD surface

Both projects use an umbrella-plus-children CRD model. `pulsar-operator`
follows the same general shape (`PulsarCluster` decomposing into per-tier
child resources) but adds `OxiaCluster` in place of a ZooKeeper resource,
and folds tiered-storage offload configuration directly into the CRD surface
(`offload` sub-spec) rather than leaving it as manual `pulsar-admin` config.

## What KAAP has that this project doesn't (yet)

- **Production track record.** KAAP has been running in the field; this
  project is pre-v1alpha1.
- **Backup automation** was explicitly descoped for `pulsar-operator` v1
  (see [Backup & DR](./backup-and-dr.md)) — check KAAP's own documentation
  for its current stance if that's a hard requirement for you today.
- **Geo-replication** is deferred to `pulsar-operator` v2.

## What's new here

- Oxia-native metadata (KAAP's largest gap for anyone wanting to leave
  ZooKeeper behind).
- Zone topology spread and BookKeeper rack-awareness sync **by default** —
  gaps in the underlying `pulsar-helm-chart` that this operator fills
  directly (see [High availability](./high-availability.md)).
- Quorum-derived PodDisruptionBudgets rather than fixed values.
- An explicit, resumable bookie decommission state machine with
  cookie-rotation and operator-owned PVC deletion, rather than relying on
  StatefulSet PVC-retention defaults.
