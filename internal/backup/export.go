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
	"fmt"
	"io"
	"time"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

// Exporter walks the entire keyspace of every configured Oxia namespace and
// writes it out as a versioned manifest (see ManifestHeader).
type Exporter struct {
	// OxiaServiceAddress is recorded in the manifest header for
	// traceability; NewClient is what actually connects.
	OxiaServiceAddress string

	// Namespaces are the Oxia namespaces to capture, in the order they'll
	// be walked and listed in the manifest header. Defaults to
	// DefaultNamespaces when empty.
	Namespaces []string

	// NewClient creates a namespace-scoped Oxia client. Required.
	NewClient ClientFactory
}

// Export walks every configured namespace's full keyspace and writes the
// result to w as a versioned manifest, returning the header that was
// written. capturedAt is stamped into the manifest header verbatim - the
// engine never calls time.Now() itself, so exports are deterministic and
// testable.
//
// The manifest header must lead with an accurate RecordCount and Checksum
// (see ManifestHeader), which can only be known once every namespace has
// been walked - so Export accumulates every record in memory before writing
// anything. Namespaces are still walked via Oxia's streaming RangeScan
// channel rather than a bulk List call; given this format only ever carries
// Oxia's metadata keyspace (never bulk ledger bytes), buffering the record
// set is an acceptable trade for a self-describing, truncation-detectable
// format.
func (e *Exporter) Export(ctx context.Context, w io.Writer, capturedAt time.Time) (ManifestHeader, error) {
	namespaces := e.Namespaces
	if len(namespaces) == 0 {
		namespaces = DefaultNamespaces
	}

	var records []ManifestRecord
	var capturedInstanceID string
	var cookieKeys []string

	for _, ns := range namespaces {
		nsRecords, instanceID, nsCookieKeys, err := e.scanNamespace(ctx, ns)
		if err != nil {
			return ManifestHeader{}, err
		}
		records = append(records, nsRecords...)
		if instanceID != "" {
			capturedInstanceID = instanceID
		}
		cookieKeys = append(cookieKeys, nsCookieKeys...)
	}

	checksum, err := checksumRecords(records)
	if err != nil {
		return ManifestHeader{}, fmt.Errorf("backup: checksum records: %w", err)
	}

	header := ManifestHeader{
		SchemaVersion:      SchemaVersion,
		OxiaServiceAddress: e.OxiaServiceAddress,
		Namespaces:         namespaces,
		CapturedInstanceID: capturedInstanceID,
		CookieKeys:         cookieKeys,
		RecordCount:        int64(len(records)),
		Checksum:           checksum,
		CapturedAt:         capturedAt,
	}

	mw := newManifestWriter(w)
	if err := mw.WriteHeader(header); err != nil {
		return header, fmt.Errorf("backup: write manifest header: %w", err)
	}
	for _, rec := range records {
		if err := mw.WriteRecord(rec); err != nil {
			return header, fmt.Errorf("backup: write manifest record for key %q: %w", rec.Key, err)
		}
	}

	return header, nil
}

// scanNamespace walks one namespace's full keyspace, returning its records
// plus any BookKeeper instanceId/cookie keys observed (only meaningful for
// the "bookkeeper" namespace).
func (e *Exporter) scanNamespace(ctx context.Context, ns string) ([]ManifestRecord, string, []string, error) {
	client, err := e.NewClient(ns)
	if err != nil {
		return nil, "", nil, fmt.Errorf("backup: connect to oxia namespace %q: %w", ns, err)
	}
	defer func() { _ = client.Close() }()

	var records []ManifestRecord
	var instanceID string
	var cookieKeys []string

	for res := range client.RangeScanAll(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, "", nil, err
		}
		if res.Err != nil {
			return nil, "", nil, fmt.Errorf("backup: scan oxia namespace %q: %w", ns, res.Err)
		}

		if ns == metadata.BookkeeperNamespace {
			switch {
			case isBookKeeperInstanceIDKey(res.Key):
				instanceID = string(res.Value)
			case isBookKeeperCookieKey(res.Key):
				cookieKeys = append(cookieKeys, res.Key)
			}
		}

		records = append(records, ManifestRecord{
			Namespace: ns,
			Key:       res.Key,
			Value:     res.Value,
			Version:   res.Version,
		})
	}

	return records, instanceID, cookieKeys, nil
}
