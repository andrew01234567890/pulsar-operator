---
sidebar_position: 2
---

# Quickstart

:::caution WIP
`pulsar-operator` is in the bootstrap phase — the Go module and controller
scaffold exist, but the CRDs, reconcilers, and autoscalers described
elsewhere in these docs are not implemented yet. The steps below are the
**intended** path to a working local cluster once those land. Commands that
depend on unbuilt pieces are marked accordingly so you don't waste time
chasing a `make target` or image that doesn't exist yet.
:::

This walks through trying `pulsar-operator` on a local [kind](https://kind.sigs.k8s.io/)
cluster: install the CRDs, run the operator, and apply a sample `PulsarCluster`.

## Prerequisites

- Go v1.24+
- Docker 17.03+
- `kubectl` v1.11.3+
- [`kind`](https://kind.sigs.k8s.io/) for a local Kubernetes cluster

## 1. Create a local cluster

```sh
kind create cluster --name pulsar-operator
```

## 2. Install the CRDs

```sh
git clone https://github.com/andrew01234567890/pulsar-operator.git
cd pulsar-operator
make install   # kubectl apply -k config/crd, via kustomize
```

## 3. Build and load the operator image

:::caution WIP — no release image yet
There is no published `pulsar-operator` image yet. Until one exists, build
from source and load it into `kind` directly.
:::

```sh
make docker-build IMG=pulsar-operator:dev
kind load docker-image pulsar-operator:dev --name pulsar-operator
make deploy IMG=pulsar-operator:dev
```

## 4. Apply a sample `PulsarCluster`

:::caution WIP — sample manifest not yet in the repo
The shape below reflects the planned `PulsarCluster` schema
(see [Introduction](./intro.md#the-crd-graph)). It is illustrative, not a
tested manifest — the API will change before v1alpha1 stabilizes.
:::

```yaml
apiVersion: cluster.pulsaroperator.io/v1alpha1
kind: PulsarCluster
metadata:
  name: pulsar-dev
  namespace: default
spec:
  pulsarVersion: "5.0.0-M1"
  metadataStore:
    oxia:
      coordinatorReplicas: 2
      serverReplicas: 3
  bookkeeper:
    replicas: 4
  broker:
    replicas: 3
  proxy:
    replicas: 1
  autoscaling:
    broker:
      enabled: true
    bookkeeper:
      scaleDown:
        enabled: false   # guarded, opt-in — see Autoscaling model
```

```sh
kubectl apply -f pulsar-dev.yaml
kubectl get pulsarcluster pulsar-dev -o yaml
```

Watch the child resources come up:

```sh
kubectl get oxiacluster,bookkeeper,broker,proxy,autorecovery -n default
```

## 5. Tear down

```sh
kubectl delete -f pulsar-dev.yaml
make undeploy
make uninstall
kind delete cluster --name pulsar-operator
```

## Next steps

- [Autoscaling model](./autoscaling.md) to understand the `autoscaling` block above.
- [High availability](./high-availability.md) for production topology defaults.
- [Backup & DR](./backup-and-dr.md) before you put real data in a cluster.
