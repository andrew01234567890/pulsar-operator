// Package metadata builds the Pulsar-facing oxia:// URLs that address an
// OxiaCluster, so broker/proxy/bookkeeper reconcilers can wire up
// metadataStoreUrl/metadataServiceUri config values without each duplicating
// Oxia's URL and naming conventions.
package metadata

import "fmt"

// ServerPort is the Oxia server's public client-facing gRPC port, exposed by
// an OxiaCluster's public Service. Pulsar/BookKeeper clients always connect
// here, never to the coordinator: the coordinator only assigns shards, it
// does not serve reads/writes.
const ServerPort = 6648

// BookkeeperNamespace is the fixed Oxia namespace BookKeeper's
// metadataServiceUri always addresses, mirroring the "default"/"broker"
// namespace split Pulsar uses across its own metadata tree.
const BookkeeperNamespace = "bookkeeper"

// PublicServiceName returns the name of the Service that fronts the
// OxiaCluster named oxiaClusterName for client traffic (the Service backed
// by oxia-server pods, reachable on ServerPort).
func PublicServiceName(oxiaClusterName string) string {
	return oxiaClusterName + "-oxia"
}

// MetadataStoreURL returns the oxia:// metadata-store URL for the given Oxia
// namespace (e.g. "default", "broker"), addressing serviceName on
// ServerPort. serviceName is a Service name/DNS name, e.g. the value
// returned by PublicServiceName, optionally namespace-qualified for
// cross-namespace addressing.
func MetadataStoreURL(serviceName, oxiaNamespace string) string {
	return fmt.Sprintf("oxia://%s:%d/%s", serviceName, ServerPort, oxiaNamespace)
}

// BookkeeperMetadataServiceURI returns the metadata-store:oxia://... URI
// BookKeeper's metadataServiceUri config key expects, addressing serviceName
// in the fixed BookkeeperNamespace.
func BookkeeperMetadataServiceURI(serviceName string) string {
	return "metadata-store:" + MetadataStoreURL(serviceName, BookkeeperNamespace)
}
