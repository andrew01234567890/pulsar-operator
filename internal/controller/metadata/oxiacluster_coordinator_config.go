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
	"fmt"

	"sigs.k8s.io/yaml"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
)

// coordinatorConfigFileName is the key the coordinator's --conf=configmap:ns/name
// flag reads (the "config.yaml" data key upstream's oxia-coordinator-configmap
// ConfigMap uses).
const coordinatorConfigFileName = "config.yaml"

// coordinatorConfig mirrors oxia's ClusterConfiguration document (see
// common/proto/metadata.proto): the coordinator loads this once from its
// ConfigMap and never mutates it, so the operator owns it outright.
type coordinatorConfig struct {
	Namespaces            []coordinatorNamespace `json:"namespaces,omitempty"`
	Servers               []coordinatorServer    `json:"servers,omitempty"`
	AllowExtraAuthorities []string               `json:"allowExtraAuthorities,omitempty"`
}

type coordinatorNamespace struct {
	Name              string `json:"name"`
	InitialShardCount int32  `json:"initialShardCount,omitempty"`
	ReplicationFactor int32  `json:"replicationFactor,omitempty"`
}

// coordinatorServer is one static server entry: the pair of addresses (gRPC
// internal port for coordinator<->server/peer traffic, public port for
// client reads/writes) other components use to reach one oxia-server pod.
type coordinatorServer struct {
	Public   string `json:"public"`
	Internal string `json:"internal"`
}

// renderCoordinatorConfig builds the coordinator's config.yaml content from
// oxia's desired spec. It is a pure function of oxia.Spec — deliberately not
// of any live server StatefulSet status — because the servers list must be
// static and cover every desired ordinal immediately on a replica-count
// change, not lag behind until the StatefulSet has actually scaled.
func renderCoordinatorConfig(oxia *metadatav1alpha1.OxiaCluster) (string, error) {
	cfg := coordinatorConfig{
		Namespaces:            renderNamespaces(oxia),
		Servers:               renderServers(oxia),
		AllowExtraAuthorities: renderAllowExtraAuthorities(oxia),
	}

	out, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal coordinator config: %w", err)
	}
	return string(out), nil
}

func renderNamespaces(oxia *metadatav1alpha1.OxiaCluster) []coordinatorNamespace {
	namespaces := make([]coordinatorNamespace, 0, len(oxia.Spec.Namespaces))
	for _, ns := range oxia.Spec.Namespaces {
		namespaces = append(namespaces, coordinatorNamespace{
			Name:              ns.Name,
			InitialShardCount: derefInt32(ns.InitialShardCount, defaultInitialShardCount),
			ReplicationFactor: derefInt32(ns.ReplicationFactor, defaultReplicationFactor),
		})
	}
	return namespaces
}

// renderServers enumerates every desired oxia-server ordinal (0..replicas-1)
// and its two addresses. The internal address is the short (non-FQDN) peer
// DNS name because coordinator and servers always share a namespace; the
// public address is the full FQDN because it must also resolve for Pulsar
// components that could live in a different namespace.
func renderServers(oxia *metadatav1alpha1.OxiaCluster) []coordinatorServer {
	replicas := serverReplicas(oxia)
	headlessSvc := serverHeadlessServiceName(oxia.Name)
	sts := serverName(oxia.Name)

	servers := make([]coordinatorServer, 0, replicas)
	for i := range replicas {
		pod := fmt.Sprintf("%s-%d", sts, i)
		servers = append(servers, coordinatorServer{
			Public:   fmt.Sprintf("%s.%s.%s.svc.%s:%d", pod, headlessSvc, oxia.Namespace, clusterDomain, serverPublicPort),
			Internal: fmt.Sprintf("%s.%s:%d", pod, headlessSvc, serverInternalPort),
		})
	}
	return servers
}

// renderAllowExtraAuthorities gates the coordinator config's
// allowExtraAuthorities list on spec.allowExtraAuthorities: unset/false
// keeps the coordinator strict (oxia-server accepts only the exact bind
// address it presents itself as); true additionally trusts the operator's
// own headless and public Service DNS names — required (oxia >= 0.16.3) for
// clients that connect through either Service rather than a bare pod
// address.
func renderAllowExtraAuthorities(oxia *metadatav1alpha1.OxiaCluster) []string {
	if oxia.Spec.AllowExtraAuthorities == nil || !*oxia.Spec.AllowExtraAuthorities {
		return nil
	}

	public := publicServiceName(oxia.Name)
	headless := serverHeadlessServiceName(oxia.Name)

	return []string{
		fmt.Sprintf("%s:%d", public, serverPublicPort),
		fmt.Sprintf("%s.%s.svc.%s:%d", public, oxia.Namespace, clusterDomain, serverPublicPort),
		fmt.Sprintf("%s:%d", headless, serverPublicPort),
		fmt.Sprintf("%s.%s.svc.%s:%d", headless, oxia.Namespace, clusterDomain, serverPublicPort),
	}
}
