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

package objectstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemRoundTrip(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Driver: DriverFilesystem, Bucket: root, Prefix: "cluster-a/backups"}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const key = "backup-1.manifest"
	payload := []byte("the-quick-brown-fox\nmanifest-line-2\n")

	if err := store.Upload(context.Background(), key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// The blob must physically land under <bucket>/<prefix>/<key>.
	wantPath := filepath.Join(root, "cluster-a/backups", key)
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("expected uploaded file at %s: %v", wantPath, err)
	}

	rc, err := store.Download(context.Background(), key)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, payload)
	}
}

func TestFilesystemDefaultRootURI(t *testing.T) {
	cfg := Config{Driver: DriverFilesystem, Prefix: "p"}
	got := URI(cfg, "k.manifest")
	want := "file://" + filepath.Join(DefaultFilesystemRoot, "p", "k.manifest")
	if got != want {
		t.Fatalf("URI = %q, want %q", got, want)
	}
}

func TestDownloadNotExist(t *testing.T) {
	cfg := Config{Driver: DriverFilesystem, Bucket: t.TempDir()}
	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = store.Download(context.Background(), "missing.manifest")
	if err == nil {
		t.Fatal("expected an error downloading a missing blob")
	}
	if !IsNotExist(err) {
		t.Fatalf("IsNotExist(%v) = false, want true", err)
	}
}

const testManifestKey = "b.manifest"
const testRestoreBucket = "restore-test-bucket"

func TestURI(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		key  string
		want string
	}{
		{
			name: "s3 with prefix",
			cfg:  Config{Driver: DriverAWSS3, Bucket: "my-bucket", Prefix: "backups/c1"},
			key:  testManifestKey,
			want: "s3://my-bucket/backups/c1/b.manifest",
		},
		{
			name: "s3 no prefix",
			cfg:  Config{Driver: DriverAWSS3, Bucket: "my-bucket"},
			key:  testManifestKey,
			want: "s3://my-bucket/b.manifest",
		},
		{
			name: "gcs",
			cfg:  Config{Driver: DriverGCS, Bucket: "gb", Prefix: "p"},
			key:  testManifestKey,
			want: "gs://gb/p/b.manifest",
		},
		{
			name: "azure",
			cfg:  Config{Driver: DriverAzureBlob, Bucket: "container", Prefix: "p"},
			key:  testManifestKey,
			want: "azblob://container/p/b.manifest",
		},
		{
			name: "filesystem with bucket",
			cfg:  Config{Driver: DriverFilesystem, Bucket: "/mnt/backups", Prefix: "p"},
			key:  testManifestKey,
			want: "file:///mnt/backups/p/b.manifest",
		},
		{
			name: "unknown driver",
			cfg:  Config{Driver: "nope"},
			key:  testManifestKey,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := URI(tt.cfg, tt.key); got != tt.want {
				t.Fatalf("URI = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewUnsupportedDriver(t *testing.T) {
	if _, err := New(context.Background(), Config{Driver: "made-up"}); err == nil {
		t.Fatal("expected an error for an unsupported driver")
	}
}

func TestKeyFromURIRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		key  string
	}{
		{name: "s3 with prefix", cfg: Config{Driver: DriverAWSS3, Bucket: testRestoreBucket, Prefix: "backups/c1"}, key: testManifestKey},
		{name: "s3 no prefix", cfg: Config{Driver: DriverAWSS3, Bucket: testRestoreBucket}, key: testManifestKey},
		{name: "gcs", cfg: Config{Driver: DriverGCS, Bucket: "gb", Prefix: "p"}, key: testManifestKey},
		{name: "azure", cfg: Config{Driver: DriverAzureBlob, Bucket: "container", Prefix: "p"}, key: testManifestKey},
		{name: "filesystem with bucket and prefix", cfg: Config{Driver: DriverFilesystem, Bucket: "/mnt/backups", Prefix: "p"}, key: testManifestKey},
		{name: "filesystem default root, no prefix", cfg: Config{Driver: DriverFilesystem}, key: testManifestKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri := URI(tt.cfg, tt.key)
			got, err := KeyFromURI(tt.cfg, uri)
			if err != nil {
				t.Fatalf("KeyFromURI(%q) error = %v", uri, err)
			}
			if got != tt.key {
				t.Fatalf("KeyFromURI(%q) = %q, want %q", uri, got, tt.key)
			}
		})
	}
}

func TestKeyFromURIRejectsWrongBucketOrPrefix(t *testing.T) {
	cfg := Config{Driver: DriverAWSS3, Bucket: testRestoreBucket, Prefix: "restores/c9"}
	tests := []string{
		"s3://other-bucket/restores/c9/b.manifest",
		"s3://my-bucket/wrong-prefix/b.manifest",
		"gs://my-bucket/restores/c9/b.manifest", // wrong scheme for the driver
	}
	for _, uri := range tests {
		if _, err := KeyFromURI(cfg, uri); err == nil {
			t.Errorf("KeyFromURI(%q) error = nil, want an error", uri)
		}
	}
}

func TestKeyFromURIUnsupportedDriver(t *testing.T) {
	if _, err := KeyFromURI(Config{Driver: "made-up"}, "made-up://x"); err == nil {
		t.Fatal("expected an error for an unsupported driver")
	}
}

func TestURIMatchesStore(t *testing.T) {
	cfg := Config{Driver: DriverFilesystem, Bucket: t.TempDir(), Prefix: "p"}
	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if store.URI("k") != URI(cfg, "k") {
		t.Fatalf("store.URI %q != package URI %q", store.URI("k"), URI(cfg, "k"))
	}
}
