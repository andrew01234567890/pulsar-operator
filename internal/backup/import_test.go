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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

func targetClients() map[string]*fakeOxiaClient {
	return map[string]*fakeOxiaClient{
		metadata.DefaultNamespace:    {},
		metadata.BrokerNamespace:     {},
		metadata.BookkeeperNamespace: {},
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	exporter := newTestExporter(seedSourceClients())

	var manifest bytes.Buffer
	exportHeader, err := exporter.Export(context.Background(), &manifest, fixedCapturedAt)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	targets := targetClients()
	importer := &Importer{NewClient: fakeClientFactory(targets)}

	result, err := importer.Import(context.Background(), &manifest)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.CapturedInstanceID != exportHeader.CapturedInstanceID {
		t.Errorf("CapturedInstanceID = %q, want %q", result.CapturedInstanceID, exportHeader.CapturedInstanceID)
	}
	if result.KeysSkippedEphemeral != 1 {
		t.Errorf("KeysSkippedEphemeral = %d, want 1", result.KeysSkippedEphemeral)
	}
	if result.KeysWritten != 5 {
		t.Errorf("KeysWritten = %d, want 5", result.KeysWritten)
	}

	// The ephemeral "/owner/broker-1" key must never reach the target.
	def := targets[metadata.DefaultNamespace]
	if len(def.puts) != 1 {
		t.Fatalf("default namespace received %d put(s), want 1 (ephemeral key must be skipped)", len(def.puts))
	}
	if def.puts[0].Key != "/managed-ledgers/public/default/persistent/topic1" {
		t.Errorf("default put key = %q, want the non-ephemeral managed-ledger key", def.puts[0].Key)
	}
	if string(def.puts[0].Value) != "managed-ledger-info" {
		t.Errorf("default put value = %q, want %q", def.puts[0].Value, "managed-ledger-info")
	}

	broker := targets[metadata.BrokerNamespace]
	if len(broker.puts) != 1 || broker.puts[0].Key != "/loadbalance/brokers/broker-1:8080" {
		t.Errorf("broker puts = %+v, want a single put for the load-report key", broker.puts)
	}

	bk := targets[metadata.BookkeeperNamespace]
	if len(bk.puts) != 3 {
		t.Fatalf("bookkeeper namespace received %d put(s), want 3 (instanceId + cookie + ledger metadata)", len(bk.puts))
	}
	for ns, c := range targets {
		if !c.closed {
			t.Errorf("namespace %q target client was not closed", ns)
		}
	}
}

func TestImportRejectsUnsupportedSchemaVersion(t *testing.T) {
	var buf bytes.Buffer
	mw := newManifestWriter(&buf)
	header := ManifestHeader{SchemaVersion: SchemaVersion + 1, RecordCount: 0, Checksum: mustChecksum(t, nil)}
	if err := mw.WriteHeader(header); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}

	importer := &Importer{NewClient: fakeClientFactory(targetClients())}
	_, err := importer.Import(context.Background(), &buf)
	if !errors.Is(err, ErrUnsupportedSchemaVersion) {
		t.Fatalf("Import() error = %v, want wrapping ErrUnsupportedSchemaVersion", err)
	}
}

func TestImportDetectsRecordCountMismatch(t *testing.T) {
	rec := ManifestRecord{Namespace: metadata.DefaultNamespace, Key: "k1", Value: []byte("v1")}

	var buf bytes.Buffer
	mw := newManifestWriter(&buf)
	header := ManifestHeader{
		SchemaVersion: SchemaVersion,
		Namespaces:    []string{metadata.DefaultNamespace},
		RecordCount:   3, // header claims 3 records, but only 2 follow below
		Checksum:      mustChecksum(t, []ManifestRecord{rec, rec, rec}),
	}
	if err := mw.WriteHeader(header); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	if err := mw.WriteRecord(rec); err != nil {
		t.Fatalf("WriteRecord() error = %v", err)
	}
	if err := mw.WriteRecord(rec); err != nil {
		t.Fatalf("WriteRecord() error = %v", err)
	}

	importer := &Importer{NewClient: fakeClientFactory(targetClients())}
	_, err := importer.Import(context.Background(), &buf)
	if !errors.Is(err, ErrTruncatedManifest) {
		t.Fatalf("Import() error = %v, want wrapping ErrTruncatedManifest", err)
	}
}

func TestImportDetectsChecksumMismatch(t *testing.T) {
	rec := ManifestRecord{Namespace: metadata.DefaultNamespace, Key: "k1", Value: []byte("v1")}

	var buf bytes.Buffer
	mw := newManifestWriter(&buf)
	header := ManifestHeader{
		SchemaVersion: SchemaVersion,
		Namespaces:    []string{metadata.DefaultNamespace},
		RecordCount:   1,
		Checksum:      "not-the-real-checksum",
	}
	if err := mw.WriteHeader(header); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	if err := mw.WriteRecord(rec); err != nil {
		t.Fatalf("WriteRecord() error = %v", err)
	}

	importer := &Importer{NewClient: fakeClientFactory(targetClients())}
	_, err := importer.Import(context.Background(), &buf)
	if !errors.Is(err, ErrTruncatedManifest) {
		t.Fatalf("Import() error = %v, want wrapping ErrTruncatedManifest", err)
	}
}

func TestImportDetectsTruncatedStream(t *testing.T) {
	exporter := newTestExporter(seedSourceClients())

	var manifest bytes.Buffer
	if _, err := exporter.Export(context.Background(), &manifest, fixedCapturedAt); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	// Cut the manifest off partway through, guaranteeing the truncation
	// lands mid-record (or mid-header) rather than exactly on a line
	// boundary.
	truncated := manifest.Bytes()[:manifest.Len()/2]

	importer := &Importer{NewClient: fakeClientFactory(targetClients())}
	_, err := importer.Import(context.Background(), bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("Import() error = nil, want a truncation error")
	}
}

func mustChecksum(t *testing.T, records []ManifestRecord) string {
	t.Helper()
	sum, err := checksumRecords(records)
	if err != nil {
		t.Fatalf("checksumRecords() error = %v", err)
	}
	return sum
}
