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
	"errors"
	"fmt"
	"io"
)

// ImportResult summarizes an Import call, in the shape the Restore
// reconciler (a follow-up) surfaces onto Restore.status.
type ImportResult struct {
	// CapturedInstanceID is the manifest header's BookKeeper instanceId,
	// surfaced so the caller can run its own cookie-lineage check against
	// the target cluster.
	CapturedInstanceID string

	// KeysWritten is the number of non-ephemeral records Put into the
	// target Oxia.
	KeysWritten int64

	// KeysSkippedEphemeral is the number of ephemeral-flagged records
	// skipped (see RecordVersion.Ephemeral).
	KeysSkippedEphemeral int64
}

// Importer reads a manifest and re-applies its records into a target Oxia.
type Importer struct {
	// NewClient creates a namespace-scoped Oxia client for the *target*
	// Oxia the manifest is being replayed into. Required.
	NewClient ClientFactory
}

// Import reads a manifest from r and replays its non-ephemeral records into
// the target Oxia, one Put per key, preserving values. Ephemeral-flagged
// records are skipped: they would inject a stale lock/ownership claim from a
// session that no longer exists.
//
// The manifest is fully read and validated (record count and checksum
// against the header) before a single Put is issued, so a truncated or
// corrupt manifest fails closed rather than partially applying an
// unverified backup to a live cluster.
func (imp *Importer) Import(ctx context.Context, r io.Reader) (ImportResult, error) {
	mr := newManifestReader(r)
	header, err := mr.ReadHeader()
	if err != nil {
		return ImportResult{}, fmt.Errorf("backup: read manifest header: %w", err)
	}

	records, err := readAllRecords(mr)
	if err != nil {
		return ImportResult{}, err
	}

	if int64(len(records)) != header.RecordCount {
		return ImportResult{}, fmt.Errorf("%w: header declares %d records, manifest contains %d",
			ErrTruncatedManifest, header.RecordCount, len(records))
	}
	checksum, err := checksumRecords(records)
	if err != nil {
		return ImportResult{}, fmt.Errorf("backup: checksum records: %w", err)
	}
	if checksum != header.Checksum {
		return ImportResult{}, fmt.Errorf("%w: checksum mismatch (want %s, got %s)",
			ErrTruncatedManifest, header.Checksum, checksum)
	}

	return imp.apply(ctx, header, records)
}

func readAllRecords(mr *manifestReader) ([]ManifestRecord, error) {
	var records []ManifestRecord
	for {
		rec, err := mr.ReadRecord()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: read manifest record %d: %v", ErrTruncatedManifest, len(records), err)
		}
		records = append(records, rec)
	}
}

func (imp *Importer) apply(ctx context.Context, header ManifestHeader, records []ManifestRecord) (ImportResult, error) {
	clients := map[string]OxiaClient{}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	result := ImportResult{CapturedInstanceID: header.CapturedInstanceID}

	for _, rec := range records {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if rec.Version.Ephemeral {
			result.KeysSkippedEphemeral++
			continue
		}

		client, ok := clients[rec.Namespace]
		if !ok {
			var err error
			client, err = imp.NewClient(rec.Namespace)
			if err != nil {
				return result, fmt.Errorf("backup: connect to oxia namespace %q: %w", rec.Namespace, err)
			}
			clients[rec.Namespace] = client
		}

		if err := client.Put(ctx, rec.Key, rec.Value); err != nil {
			return result, fmt.Errorf("backup: put key %q into namespace %q: %w", rec.Key, rec.Namespace, err)
		}
		result.KeysWritten++
	}

	return result, nil
}
