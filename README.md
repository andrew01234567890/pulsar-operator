# pulsar-operator

A Kubernetes operator that manages the full lifecycle of [Apache Pulsar](https://pulsar.apache.org/) clusters and **autoscales** them based on real workload signals — bookie disk usage, broker CPU/throughput — modeled on the ideas behind DataStax [KAAP](https://github.com/datastax/kaap), rebuilt in Go and **native to [Oxia](https://github.com/oxia-db/oxia)**, the metadata store that replaces ZooKeeper.

> **Status: early development.** APIs are `v1alpha1` and will change. Not yet production-ready. Targets the Apache Pulsar `5.0.0-M1` preview.

## Why

Running Pulsar on Kubernetes means operating several stateful subsystems — bookies (BookKeeper), brokers, a proxy, the autorecovery daemon, and a metadata store — each with its own scaling, placement, and upgrade rules. Existing tooling either stops at templating (Helm) or is tied to the JVM and ZooKeeper. `pulsar-operator` aims to:

- **Manage everything through CRDs** — declarative provisioning, config, rolling upgrades, and HA for every component.
- **Autoscale safely** — grow bookies before disks fill, grow brokers under load, and *never* lose data on scale-down (guarded, opt-in bookie decommission).
- **Be Oxia-native** — deploy and wire Oxia (coordinator + servers) as a first-class metadata store, not an afterthought.

## Architecture

One umbrella CRD, `PulsarCluster`, decomposes into per-component child CRDs that each reconcile independently:

```
PulsarCluster (cluster.pulsaroperator.io)
├── OxiaCluster     (metadata.pulsaroperator.io) — coordinator Deployment + server StatefulSet
├── BookKeeper      — bookie StatefulSet (journal/ledgers/index disks) + disk-usage autoscaler
├── Broker          — broker StatefulSet + CPU/throughput autoscaler
├── Proxy           — optional stateless gateway
├── AutoRecovery    — Auditor + ReplicationWorker
└── FunctionsWorker — Pulsar Functions (co-located or standalone)
```

## Features (v1 scope)

- **Broker autoscaling** on Pulsar load-report CPU (composite CPU/bandwidth signal), unanimous thresholds with a stabilization window, live bundle-transfer drain on scale-down.
- **Bookie autoscaling** on ledger-disk high/low watermarks; scale-up is automatic, scale-**down** is a guarded, opt-in, resumable decommission state machine (re-replicate → verify zero under-replication → cookie-rotate → operator-owned PVC delete).
- **Oxia-native** metadata: coordinator + server split, shard rebalancing on scale, quorum-aware rolling restarts.
- **HA by default**: anti-affinity, zone topology spread, quorum-derived PodDisruptionBudgets, BookKeeper rack-awareness synced from node zones, ordered rolling upgrades.
- **Tiered storage** (S3/GCS/Azure/filesystem offload) configured via CRD.

### Backup & DR (read this)

Tiered storage is **cost/retention tiering, not backup**. Oxia holds Pulsar's authoritative metadata and has **no native snapshot/export** today — losing all Oxia replicas can render a cluster unrecoverable even if BookKeeper data survives. Run Oxia with `replicationFactor ≥ 3` across availability zones and take your own volume snapshots. Automated backup and geo-replication are planned for a later release.

## Getting Started

### Prerequisites
- Go v1.24+
- Docker 17.03+
- kubectl v1.11.3+ and access to a Kubernetes v1.11.3+ cluster (a local [kind](https://kind.sigs.k8s.io/) cluster works)

### Deploy

```sh
make docker-build docker-push IMG=<some-registry>/pulsar-operator:tag
make install                       # CRDs
make deploy IMG=<some-registry>/pulsar-operator:tag
```

Apply a sample cluster:

```sh
kubectl apply -k config/samples/
```

### Uninstall

```sh
kubectl delete -k config/samples/
make uninstall
make undeploy
```

Run `make help` for all targets.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Work happens on feature branches via pull requests; every PR is reviewed and must pass CI before merge.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
