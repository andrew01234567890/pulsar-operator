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

// Package backup implements pulsar-operator's Oxia logical export/import
// engine: a versioned manifest format plus an Exporter/Importer pair that
// the Backup/Restore Jobs (a follow-up) run to capture and replay Oxia's
// entire metadata keyspace.
package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// SchemaVersion is the current manifest format version. Bump it whenever the
// on-disk shape of ManifestHeader or ManifestRecord changes incompatibly;
// Importer refuses to read a manifest with a SchemaVersion it doesn't
// recognize rather than silently misinterpreting it.
const SchemaVersion = 1

var (
	// ErrUnsupportedSchemaVersion is returned when a manifest declares a
	// SchemaVersion the running Importer doesn't know how to read.
	ErrUnsupportedSchemaVersion = errors.New("backup: unsupported manifest schema version")

	// ErrTruncatedManifest is returned when a manifest ends before its
	// header's declared RecordCount is reached, or the records read don't
	// hash to the header's declared Checksum - either way, the manifest is
	// not the complete, uncorrupted stream the exporter produced.
	ErrTruncatedManifest = errors.New("backup: truncated or corrupt manifest")
)

// ManifestHeader is the first value in a manifest stream: a self-describing
// preamble identifying the format version and summarizing what follows, so a
// Restore reconciler can validate a backup (schema version, record count,
// checksum, BookKeeper instanceId lineage) before touching a live cluster.
type ManifestHeader struct {
	// SchemaVersion is the manifest format version; see SchemaVersion.
	SchemaVersion int `json:"schemaVersion"`

	// OxiaServiceAddress is the Oxia service address the export was taken
	// from, recorded for operator traceability.
	OxiaServiceAddress string `json:"oxiaServiceAddress"`

	// Namespaces lists every Oxia namespace this manifest captures, in the
	// order they were walked.
	Namespaces []string `json:"namespaces"`

	// CapturedInstanceID is the BookKeeper cluster instanceId observed in
	// the "bookkeeper" namespace at capture time (empty if that namespace
	// wasn't captured or had no INSTANCEID record yet). A Restore compares
	// this against the target cluster's current instanceId to enforce
	// cookie lineage before replaying ledger metadata into it - see
	// docs/docs/backup-and-dr.md.
	CapturedInstanceID string `json:"capturedInstanceId,omitempty"`

	// CookieKeys lists the BookKeeper per-bookie cookie registration record
	// keys observed in the "bookkeeper" namespace at capture time. The
	// records themselves travel in the record stream like any other key;
	// this is a quick-reference index so a Restore can reason about cookie
	// lineage without decoding the entire manifest.
	CookieKeys []string `json:"cookieKeys,omitempty"`

	// RecordCount is the total number of records in this manifest.
	RecordCount int64 `json:"recordCount"`

	// Checksum is a hex-encoded SHA-256 over the records, computed
	// identically by Exporter and Importer (see checksumRecords), so a
	// truncated or corrupted manifest is detected before anything is
	// replayed into a target Oxia.
	Checksum string `json:"checksum"`

	// CapturedAt is when the export was taken, as supplied by the caller
	// (the engine never calls time.Now() itself, so exports are
	// deterministic and testable).
	CapturedAt time.Time `json:"capturedAt"`
}

// RecordVersion mirrors the fields of an Oxia record's Stat/Version that
// matter for backup/restore: enough to detect ephemeral (session-scoped)
// keys and to retain the original timestamps for audit purposes. Oxia
// assigns VersionId/timestamps server-side on Put, so replaying a record
// into a target Oxia cannot reproduce these exactly - they're informational,
// not restored.
type RecordVersion struct {
	VersionID          int64  `json:"versionId"`
	CreatedTimestamp   uint64 `json:"createdTimestamp"`
	ModifiedTimestamp  uint64 `json:"modifiedTimestamp"`
	ModificationsCount int64  `json:"modificationsCount"`

	// Ephemeral marks a session-scoped record (e.g. an owner/lock key). The
	// Importer skips these: replaying a stale ephemeral record would inject
	// a lock/ownership claim from a session that no longer exists.
	Ephemeral bool `json:"ephemeral"`

	// SessionID is the owning session id for an ephemeral record; always 0
	// for non-ephemeral records.
	SessionID int64 `json:"sessionId"`

	// ClientIdentity is the Oxia client identity that last modified an
	// ephemeral record; empty for non-ephemeral records.
	ClientIdentity string `json:"clientIdentity,omitempty"`
}

// ManifestRecord is one Oxia key/value pair captured by an export, tagged
// with the namespace it came from (a manifest spans every namespace the
// operator provisions) and its version metadata.
type ManifestRecord struct {
	Namespace string        `json:"namespace"`
	Key       string        `json:"key"`
	Value     []byte        `json:"value"`
	Version   RecordVersion `json:"version"`
}

// checksumRecords hashes records in order into a single hex-encoded SHA-256
// digest. Exporter computes this once over the full record set before
// writing the header; Importer recomputes it over the records it read and
// compares, detecting truncation or corruption. json.Marshal on these fixed,
// tag-ordered struct fields (no maps, no floats) is deterministic, so the
// same records always hash the same way on both sides.
func checksumRecords(records []ManifestRecord) (string, error) {
	h := sha256.New()
	for _, r := range records {
		data, err := json.Marshal(r)
		if err != nil {
			return "", err
		}
		h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// manifestWriter encodes a manifest as JSON Lines: one JSON value per line,
// header first, then one line per record. JSON Lines was chosen over a
// length-prefixed binary format for human-debuggability (a manifest can be
// inspected with any JSON tool) at negligible cost given this format only
// ever carries Oxia's metadata keyspace, not bulk ledger bytes.
type manifestWriter struct {
	enc *json.Encoder
}

func newManifestWriter(w io.Writer) *manifestWriter {
	return &manifestWriter{enc: json.NewEncoder(w)}
}

func (mw *manifestWriter) WriteHeader(h ManifestHeader) error {
	return mw.enc.Encode(h)
}

func (mw *manifestWriter) WriteRecord(r ManifestRecord) error {
	return mw.enc.Encode(r)
}

// manifestReader decodes a manifest written by manifestWriter. Position in
// the stream (header always first, then records) disambiguates the two line
// shapes, so no type discriminator field is needed on the wire.
type manifestReader struct {
	dec *json.Decoder
}

func newManifestReader(r io.Reader) *manifestReader {
	return &manifestReader{dec: json.NewDecoder(r)}
}

// ReadHeader decodes the manifest header and rejects a SchemaVersion this
// Importer doesn't understand.
func (mr *manifestReader) ReadHeader() (ManifestHeader, error) {
	var h ManifestHeader
	if err := mr.dec.Decode(&h); err != nil {
		return ManifestHeader{}, err
	}
	if h.SchemaVersion != SchemaVersion {
		return h, fmt.Errorf("%w: manifest is schema version %d, importer supports %d",
			ErrUnsupportedSchemaVersion, h.SchemaVersion, SchemaVersion)
	}
	return h, nil
}

// ReadRecord decodes the next record, or returns io.EOF once the stream is
// exhausted.
func (mr *manifestReader) ReadRecord() (ManifestRecord, error) {
	var r ManifestRecord
	err := mr.dec.Decode(&r)
	return r, err
}
