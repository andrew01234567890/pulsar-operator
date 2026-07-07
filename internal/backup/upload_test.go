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

	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

const testDestManifestKey = "backup-1.manifest"

func TestParseExportFlagsDestRequiresKey(t *testing.T) {
	_, err := parseExportFlags([]string{flagOxia, testFlagOxiaAddress, "--dest-driver", "filesystem"})
	if err == nil {
		t.Fatal("expected an error when --dest-driver is set without --dest-key")
	}
}

func TestParseExportFlagsDest(t *testing.T) {
	flags, err := parseExportFlags([]string{
		flagOxia, testFlagOxiaAddress,
		"--dest-driver", "aws-s3",
		"--dest-bucket", "b",
		"--dest-region", "us-east-1",
		"--dest-endpoint", "https://minio:9000",
		"--dest-prefix", "backups/c1",
		"--dest-key", testDestManifestKey,
	})
	if err != nil {
		t.Fatalf("parseExportFlags: %v", err)
	}
	want := objectstore.Config{
		Driver:   "aws-s3",
		Bucket:   "b",
		Region:   "us-east-1",
		Endpoint: "https://minio:9000",
		Prefix:   "backups/c1",
	}
	if flags.Dest != want {
		t.Errorf("Dest = %+v, want %+v", flags.Dest, want)
	}
	if flags.DestKey != testDestManifestKey {
		t.Errorf("DestKey = %q, want backup-1.manifest", flags.DestKey)
	}
}

// TestRunExportToObjectStoreFilesystem exercises the full export -> upload ->
// result-write path end-to-end against the filesystem driver: the manifest
// lands in the object store and the ExportResult the reconciler reads back is
// written to the result file with the true key count, instanceId, and size.
func TestRunExportToObjectStoreFilesystem(t *testing.T) {
	root := t.TempDir()
	resultPath := filepath.Join(t.TempDir(), "termination-log")

	flags := ExportFlags{
		OxiaAddress: testOxiaServiceAddress,
		Namespaces:  DefaultNamespaces,
		Dest: objectstore.Config{
			Driver: objectstore.DriverFilesystem,
			Bucket: root,
			Prefix: "cluster-a",
		},
		DestKey:    testDestManifestKey,
		ResultPath: resultPath,
	}

	var stdout, stderr bytes.Buffer
	err := runExportToObjectStore(context.Background(), flags, fakeClientFactory(seedSourceClients()), fixedCapturedAt, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runExportToObjectStore: %v", err)
	}

	// The manifest must be readable back through the same store.
	store, err := objectstore.New(context.Background(), flags.Dest)
	if err != nil {
		t.Fatalf("objectstore.New: %v", err)
	}
	rc, err := store.Download(context.Background(), flags.DestKey)
	if err != nil {
		t.Fatalf("Download uploaded manifest: %v", err)
	}
	_ = rc.Close()

	// The ExportResult must have been written to the result file for the
	// reconciler to read back.
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read result file: %v", err)
	}
	result, err := ParseExportResult(data)
	if err != nil {
		t.Fatalf("ParseExportResult: %v", err)
	}
	if result.OxiaKeysCaptured != 6 {
		t.Errorf("OxiaKeysCaptured = %d, want 6", result.OxiaKeysCaptured)
	}
	if result.CapturedInstanceID != "instance-abc-123" {
		t.Errorf("CapturedInstanceID = %q, want instance-abc-123", result.CapturedInstanceID)
	}
	if result.SizeBytes <= 0 {
		t.Errorf("SizeBytes = %d, want > 0", result.SizeBytes)
	}
	if result.ArtifactURI != store.URI(flags.DestKey) {
		t.Errorf("ArtifactURI = %q, want %q", result.ArtifactURI, store.URI(flags.DestKey))
	}
}
