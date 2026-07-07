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

package backup

import "github.com/andrew01234567890/pulsar-operator/internal/metadata"

// DefaultNamespaces lists every Oxia namespace pulsar-operator provisions
// for a PulsarCluster (see config/samples/metadata_v1alpha1_oxiacluster.yaml).
// Exporter walks each of these in full, including the "bookkeeper" namespace
// verbatim, so no part of Pulsar's metadata surface is silently skipped.
var DefaultNamespaces = []string{
	metadata.DefaultNamespace,
	metadata.BrokerNamespace,
	metadata.BookkeeperNamespace,
}
