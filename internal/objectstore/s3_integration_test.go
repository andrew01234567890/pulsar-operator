//go:build integration

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
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// This test exercises the REAL S3 driver's Upload/Download against a MinIO
// (S3-compatible) server in a throwaway Docker container. It exists because
// the unit tests can only cover the filesystem driver and URI math - the S3
// network path, and specifically the aws-sdk-go-v2 request-checksum behavior
// that breaks PutObject against S3-compatible stores (newExportToObjectStore
// downgrades it to WhenRequired when an endpoint is set), can only be proven
// against a real server.
//
// It is behind the `integration` build tag (like the export tool's real-Oxia
// test) so `make test` / `go test ./...` never run it, and it skips when
// Docker is unavailable so `go test -tags=integration ./...` degrades
// gracefully.

const (
	minioImage     = "minio/minio:RELEASE.2025-04-08T15-41-24Z"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioBucket    = "backup-test"
)

func startMinio(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found on PATH; skipping MinIO S3 integration test")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not available; skipping MinIO S3 integration test")
	}

	runOut, err := exec.Command("docker", "run", "-d",
		"-p", "127.0.0.1:0:9000",
		"-e", "MINIO_ROOT_USER="+minioAccessKey,
		"-e", "MINIO_ROOT_PASSWORD="+minioSecretKey,
		minioImage,
		"server", "/data",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run minio: %v\n%s", err, runOut)
	}
	containerID := strings.TrimSpace(string(runOut))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", containerID).Run() })

	portOut, err := exec.Command("docker", "port", containerID, "9000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port: %v\n%s", err, portOut)
	}
	mapping := strings.TrimSpace(strings.SplitN(string(portOut), "\n", 2)[0])
	idx := strings.LastIndex(mapping, ":")
	if idx < 0 {
		t.Fatalf("unexpected docker port output: %q", mapping)
	}
	return "http://127.0.0.1:" + mapping[idx+1:]
}

// rawS3Client builds an S3 client with the same endpoint/path-style/checksum
// options newS3Store uses, for the test's own bucket setup + readiness poll.
func rawS3Client(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

func TestS3RoundTripAgainstMinio(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", minioAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioSecretKey)

	endpoint := startMinio(t)
	raw := rawS3Client(t, endpoint)

	// Wait for the server, then create the bucket.
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := raw.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(minioBucket)})
		if err == nil || strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("minio never became ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	cfg := Config{
		Driver:   DriverAWSS3,
		Bucket:   minioBucket,
		Region:   "us-east-1",
		Endpoint: endpoint,
		Prefix:   "cluster-a/backups",
	}
	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const key = "backup-1.manifest"
	payload := []byte("manifest-line-1\nmanifest-line-2\n")

	if err := store.Upload(context.Background(), key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Upload: %v", err)
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

	// A missing key must be reported as IsNotExist.
	if _, err := store.Download(context.Background(), "does-not-exist.manifest"); !IsNotExist(err) {
		t.Fatalf("IsNotExist(%v) = false, want true", err)
	}
}
