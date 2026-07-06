/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metadata

import (
	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	oxiaurl "github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

const (
	oxiaBinary = "oxia"

	componentCoordinator = "oxia-coordinator"
	componentServer      = "oxia-server"

	portNamePublic   = "public"
	portNameInternal = "internal"
	portNameMetrics  = "metrics"

	// oxiaInternalPort is the gRPC port both the coordinator and the server
	// bind for peer/admin traffic and for their "oxia health" probes; ports
	// match the real Oxia coordinator/dataserver default configs
	// (conf/coordinator.yaml, conf/dataserver.yaml upstream). The
	// coordinator never binds a client-facing "public" port, only internal
	// and metrics; the server binds all three.
	oxiaInternalPort        = 6649
	coordinatorInternalPort = oxiaInternalPort
	coordinatorMetricsPort  = 8080

	serverPublicPort   = oxiaurl.ServerPort
	serverInternalPort = oxiaInternalPort
	serverMetricsPort  = 8080

	// defaultOxiaImage matches the pulsar-helm-chart default (images.oxia).
	defaultOxiaImage = "oxia/oxia:0.16.7"

	defaultCoordinatorReplicas = 2
	defaultServerReplicas      = 3
	defaultDbCacheSizeMb       = 512
	defaultWalSyncData         = true

	defaultInitialShardCount = 3
	defaultReplicationFactor = 3

	// clusterDomain is not (yet) exposed on OxiaClusterSpec, so it is fixed
	// to the vanilla Kubernetes default rather than plumbed through from a
	// higher-level cluster-wide setting.
	clusterDomain = "cluster.local"
)

func coordinatorName(oxiaName string) string {
	return oxiaName + "-" + componentCoordinator
}

func coordinatorStatusConfigMapName(oxiaName string) string {
	return coordinatorName(oxiaName) + "-status"
}

func serverName(oxiaName string) string {
	return oxiaName + "-" + componentServer
}

// serverHeadlessServiceName is the peer-DNS Service StatefulSet pods use to
// resolve each other (and that the coordinator's static servers list
// addresses). Deliberately not suffixed "-server": it fronts the whole Oxia
// data plane, mirroring the upstream pulsar-helm-chart "-oxia-svc" naming.
func serverHeadlessServiceName(oxiaName string) string {
	return oxiaName + "-oxia-svc"
}

// publicServiceName is the client-facing Service (oxiaurl.PublicServiceName)
// Pulsar/BookKeeper components address via MetadataStoreURL. It has no
// "-server" or "-coordinator" suffix because it is the OxiaCluster's single
// public identity, even though its selector targets oxia-server pods: the
// coordinator process never serves client reads/writes, only shard
// assignment (verified against oxia's coordinator gRPC server, which only
// registers a health service on its internal port).
func publicServiceName(oxiaName string) string {
	return oxiaurl.PublicServiceName(oxiaName)
}

func coordinatorSpec(oxia *metadatav1alpha1.OxiaCluster) metadatav1alpha1.OxiaCoordinatorSpec {
	if oxia.Spec.Coordinator == nil {
		return metadatav1alpha1.OxiaCoordinatorSpec{}
	}
	return *oxia.Spec.Coordinator
}

func serverSpec(oxia *metadatav1alpha1.OxiaCluster) metadatav1alpha1.OxiaServerSpec {
	if oxia.Spec.Server == nil {
		return metadatav1alpha1.OxiaServerSpec{}
	}
	return *oxia.Spec.Server
}

func coordinatorReplicas(oxia *metadatav1alpha1.OxiaCluster) int32 {
	return derefInt32(coordinatorSpec(oxia).Replicas, defaultCoordinatorReplicas)
}

func serverReplicas(oxia *metadatav1alpha1.OxiaCluster) int32 {
	return derefInt32(serverSpec(oxia).Replicas, defaultServerReplicas)
}

func coordinatorImage(oxia *metadatav1alpha1.OxiaCluster) string {
	if image := coordinatorSpec(oxia).Image; image != "" {
		return image
	}
	return defaultOxiaImage
}

func serverImage(oxia *metadatav1alpha1.OxiaCluster) string {
	if image := serverSpec(oxia).Image; image != "" {
		return image
	}
	return defaultOxiaImage
}

func dbCacheSizeMb(oxia *metadatav1alpha1.OxiaCluster) int32 {
	return derefInt32(serverSpec(oxia).DbCacheSizeMb, defaultDbCacheSizeMb)
}

func walSyncData(oxia *metadatav1alpha1.OxiaCluster) bool {
	if v := serverSpec(oxia).WalSyncData; v != nil {
		return *v
	}
	return defaultWalSyncData
}

func derefInt32(p *int32, def int32) int32 {
	if p == nil {
		return def
	}
	return *p
}
