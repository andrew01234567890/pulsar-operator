---
sidebar_position: 7
---

# CRD API reference

:::caution Placeholder
This page is a placeholder. A full API reference — generated directly from
the Go types in `api/{cluster,metadata}/<version>/*_types.go` — is planned
via [`elastic/crd-ref-docs`](https://github.com/elastic/crd-ref-docs) as a
CI step (see the `docs-pages` job in the project's CI plan), so the
reference can never drift from the actual schema. That generation step
hasn't landed yet.
:::

## What will live here

Once wired up, this page (or section) will document, for every CRD:

- `PulsarCluster` (group `cluster.pulsaroperator.io`) — the umbrella resource.
- `OxiaCluster` (group `metadata.pulsaroperator.io`) — coordinator + server topology.
- `BookKeeper`, `Broker`, `Proxy`, `AutoRecovery`, `FunctionsWorker` (group `cluster.pulsaroperator.io`).

For each type: every field, its type, default, validation constraints (CEL
rules and webhook-enforced invariants — e.g. odd Oxia server replica counts,
`FileSystemPackagesStorage` being required for FunctionsWorker under Oxia,
`FunctionsWorker.spec.mode: standalone` being rejected outright since it
cannot run against an Oxia-backed metadata store), and a status/condition
reference.

## In the meantime

- The [Introduction](./intro.md#the-crd-graph) page has the high-level CRD
  graph and a short description of each type's responsibility.
- The current source of truth is the Go API types themselves, once they
  land under `api/` — check the repository directly for the latest schema
  ahead of this page being generated.
