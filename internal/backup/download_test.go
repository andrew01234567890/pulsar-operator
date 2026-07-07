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
	"os"
	"path/filepath"
	"testing"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

// TestRunImportFromObjectStoreFilesystem exercises the full download -> import
// -> result-write path end-to-end against the filesystem driver, the mirror
// of TestRunExportToObjectStoreFilesystem: a manifest is uploaded once, then
// downloaded and replayed by runImportFromObjectStore, and the ImportResult
// the Restore reconciler reads back is written to the result file with the
// true key/instanceId counts.
func TestRunImportFromObjectStoreFilesystem(t *testing.T) {
	root := t.TempDir()
	dest := objectstore.Config{Driver: objectstore.DriverFilesystem, Bucket: root, Prefix: "cluster-a"}

	store, err := objectstore.New(context.Background(), dest)
	if err != nil {
		t.Fatalf("objectstore.New: %v", err)
	}
	exportHeader, err := exportManifestTo(t, store, testDestManifestKey)
	if err != nil {
		t.Fatalf("exportManifestTo: %v", err)
	}

	resultPath := filepath.Join(t.TempDir(), "termination-log")
	flags := ImportFlags{
		OxiaAddress: testFlagOxiaAddress,
		Src:         dest,
		SrcKey:      testDestManifestKey,
		ResultPath:  resultPath,
	}

	targets := targetClients()
	var stdout, stderr bytes.Buffer
	if err := runImportFromObjectStore(context.Background(), flags, fakeClientFactory(targets), &stdout, &stderr); err != nil {
		t.Fatalf("runImportFromObjectStore: %v", err)
	}

	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read result file: %v", err)
	}
	result, err := ParseImportResult(data)
	if err != nil {
		t.Fatalf("ParseImportResult: %v", err)
	}
	if result.KeysRestored != 5 {
		t.Errorf("KeysRestored = %d, want 5", result.KeysRestored)
	}
	if result.KeysSkippedEphemeral != 1 {
		t.Errorf("KeysSkippedEphemeral = %d, want 1", result.KeysSkippedEphemeral)
	}
	if result.CapturedInstanceID != exportHeader.CapturedInstanceID {
		t.Errorf("CapturedInstanceID = %q, want %q", result.CapturedInstanceID, exportHeader.CapturedInstanceID)
	}

	bk := targets[metadata.BookkeeperNamespace]
	if len(bk.puts) != 3 {
		t.Fatalf("bookkeeper namespace received %d put(s), want 3", len(bk.puts))
	}
}

// TestRunImportFromObjectStoreMissingArtifact confirms a missing manifest
// surfaces as an objectstore.IsNotExist error, the signal the Restore
// reconciler's pre-flight lineage check uses to distinguish "no such backup"
// from a generic download failure.
func TestRunImportFromObjectStoreMissingArtifact(t *testing.T) {
	dest := objectstore.Config{Driver: objectstore.DriverFilesystem, Bucket: t.TempDir()}
	flags := ImportFlags{OxiaAddress: testFlagOxiaAddress, Src: dest, SrcKey: "missing.manifest", ResultPath: filepath.Join(t.TempDir(), "log")}

	var stdout, stderr bytes.Buffer
	err := runImportFromObjectStore(context.Background(), flags, fakeClientFactory(targetClients()), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected an error downloading a missing manifest")
	}
	if !objectstore.IsNotExist(err) {
		t.Fatalf("IsNotExist(%v) = false, want true", err)
	}
}

// TestReadManifestHeaderIgnoresTrailingRecords confirms ReadManifestHeader
// successfully decodes just the header even when a full manifest's records
// follow it in the stream - the property the Restore reconciler's pre-flight
// lineage check relies on to peek at CapturedInstanceID via a plain
// io.Reader (from objectstore.Store.Download) without needing to read or
// validate the rest of the manifest.
func TestReadManifestHeaderIgnoresTrailingRecords(t *testing.T) {
	exporter := newTestExporter(seedSourceClients())

	var manifest bytes.Buffer
	wantHeader, err := exporter.Export(context.Background(), &manifest, fixedCapturedAt)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	gotHeader, err := ReadManifestHeader(&manifest)
	if err != nil {
		t.Fatalf("ReadManifestHeader() error = %v", err)
	}
	if gotHeader.CapturedInstanceID != wantHeader.CapturedInstanceID {
		t.Errorf("CapturedInstanceID = %q, want %q", gotHeader.CapturedInstanceID, wantHeader.CapturedInstanceID)
	}
	if gotHeader.RecordCount != wantHeader.RecordCount {
		t.Errorf("RecordCount = %d, want %d", gotHeader.RecordCount, wantHeader.RecordCount)
	}
}

// exportManifestTo runs a real export against seedSourceClients and uploads
// it to store under key, returning the header that was written.
func exportManifestTo(t *testing.T, store objectstore.Store, key string) (ManifestHeader, error) {
	t.Helper()
	exporter := newTestExporter(seedSourceClients())

	var manifest bytes.Buffer
	header, err := exporter.Export(context.Background(), &manifest, fixedCapturedAt)
	if err != nil {
		return ManifestHeader{}, err
	}
	if err := store.Upload(context.Background(), key, bytes.NewReader(manifest.Bytes())); err != nil {
		return ManifestHeader{}, err
	}
	return header, nil
}
