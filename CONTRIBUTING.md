# Contributing to pulsar-operator

Thanks for your interest in contributing. This project is in early development; expect APIs and internals to change.

## Ground rules

- **All changes go through pull requests.** No direct pushes to `main`.
- **CI must be green before merge** — lint, unit tests, envtest integration tests, and (where applicable) KIND e2e.
- **Every change ships with tests**, including a regression test that fails without the change.
- **Keep `main` releasable.** Rebase or merge from `main` before requesting review.

## Development setup

Prerequisites: Go 1.24+, Docker, `kubectl`, `kind`, and `make`.

```sh
make manifests generate   # regenerate CRDs, RBAC, deepcopy after changing api/ types
make fmt vet lint         # formatting + static analysis
make test                 # unit + envtest integration tests
make test-e2e             # KIND end-to-end (spins up a kind cluster)
```

## Commit conventions

- Use [Conventional Commits](https://www.conventionalcommits.org/) prefixes (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `ci:`, `chore:`).
- The commit body and PR description carry the **why**; code carries the **what** via clear names.
- Commits are authored with a GitHub noreply email — no personal email in history.

## Pull requests

1. Branch off the latest `main`.
2. Make focused changes; keep unrelated concerns in separate PRs.
3. Ensure `make manifests generate` produced no uncommitted diffs.
4. Fill in the PR description: what changed and why, and how it was tested.
5. A maintainer review + green CI are required to merge.

## Areas that need extra care

- **Bookie scale-down / decommission** touches data durability — changes here get the most rigorous review and must prove zero data loss in e2e against an Oxia-backed cluster.
- **Metadata store wiring (Oxia)** — BookKeeper autorecovery (Auditor election, ledger under-replication) rides a generic Oxia bridge; validate end-to-end, don't assume ZooKeeper semantics.
