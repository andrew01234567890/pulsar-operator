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

import (
	"context"

	"github.com/oxia-db/oxia/oxia"
)

// OxiaClient is the minimal surface the export/import engine needs from an
// Oxia client, scoped to a single namespace: Oxia binds the namespace at
// client-construction time, not per call, so one OxiaClient always talks to
// exactly one namespace. Abstracting it behind this interface lets Exporter
// and Importer be exercised against a fake in tests instead of a live Oxia
// service.
type OxiaClient interface {
	// RangeScanAll streams every record in the client's namespace. The real
	// implementation passes empty min/max bounds to Oxia's RangeScan, which
	// fans the scan out across every shard of the namespace.
	RangeScanAll(ctx context.Context) <-chan ScanResult

	// Put creates or overwrites key with value, unconditionally - re-running
	// an import with the same manifest is idempotent.
	Put(ctx context.Context, key string, value []byte) error

	Close() error
}

// ScanResult is one record (or a terminal error) produced by
// OxiaClient.RangeScanAll.
type ScanResult struct {
	Key     string
	Value   []byte
	Version RecordVersion
	Err     error
}

// ClientFactory returns an OxiaClient scoped to the given Oxia namespace.
// Exporter and Importer both take one of these rather than a concrete
// client, so callers can inject a fake for tests or the real
// NewOxiaClientFactory for production use.
type ClientFactory func(namespace string) (OxiaClient, error)

// NewOxiaClientFactory returns a ClientFactory backed by the real Oxia Go
// client (github.com/oxia-db/oxia/oxia), connecting to serviceAddress and
// binding to a different namespace on each call.
func NewOxiaClientFactory(serviceAddress string, opts ...oxia.ClientOption) ClientFactory {
	return func(namespace string) (OxiaClient, error) {
		clientOpts := append(append([]oxia.ClientOption{}, opts...), oxia.WithNamespace(namespace))
		client, err := oxia.NewSyncClient(serviceAddress, clientOpts...)
		if err != nil {
			return nil, err
		}
		return &syncClientAdapter{client: client}, nil
	}
}

// syncClientAdapter adapts oxia.SyncClient to OxiaClient.
type syncClientAdapter struct {
	client oxia.SyncClient
}

func (a *syncClientAdapter) Close() error {
	return a.client.Close()
}

func (a *syncClientAdapter) Put(ctx context.Context, key string, value []byte) error {
	_, _, err := a.client.Put(ctx, key, value)
	return err
}

func (a *syncClientAdapter) RangeScanAll(ctx context.Context) <-chan ScanResult {
	// The empty min/max bounds are load-bearing: Oxia's server treats an
	// empty bound as "unset" (unbounded), not as "keys < empty-string"
	// (nothing) - kvstore.Pebble.RangeScan only sets the Pebble
	// LowerBound/UpperBound when the string is non-empty - so this is a
	// genuine full-keyspace scan that fans out across every shard (no
	// PartitionKey => clientImpl.RangeScan iterates shardManager.GetAll()).
	// If that ever changed, every export would silently produce an empty
	// manifest; TestExportFullScanAgainstRealOxia (build tag `integration`)
	// guards it against a real Oxia.
	upstream := a.client.RangeScan(ctx, "", "")
	out := make(chan ScanResult)
	go func() {
		defer close(out)
		for r := range upstream {
			result := ScanResult{Key: r.Key, Value: r.Value, Err: r.Err}
			if r.Err == nil {
				result.Version = RecordVersion{
					VersionID:          r.Version.VersionId,
					CreatedTimestamp:   r.Version.CreatedTimestamp,
					ModifiedTimestamp:  r.Version.ModifiedTimestamp,
					ModificationsCount: r.Version.ModificationsCount,
					Ephemeral:          r.Version.Ephemeral,
					SessionID:          r.Version.SessionId,
					ClientIdentity:     r.Version.ClientIdentity,
				}
			}
			out <- result
		}
	}()
	return out
}
