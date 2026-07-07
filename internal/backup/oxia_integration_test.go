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

package backup

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxia"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

// These tests exercise the export/import engine against a REAL Oxia server,
// not the in-memory fake, because the single highest-risk assumption in the
// whole backup feature - that oxia.RangeScan(ctx, "", "") with empty bounds
// is a genuine FULL-keyspace scan across every shard, not an empty range -
// is a property of Oxia's server (kvstore.Pebble.RangeScan special-cases an
// empty bound as "unset" = unbounded), which a fake OxiaClient can never
// prove. A fake returns whatever it's seeded with regardless of bounds, so
// if empty bounds silently scanned nothing, every export would produce an
// empty manifest and no unit test would notice.
//
// They are behind the `integration` build tag (like the repo's `e2e` suite)
// so `make test` / `go test ./...` never run them, and additionally skip if
// Docker is unavailable, so `go test -tags=integration ./...` degrades
// gracefully on machines without a container runtime.

// oxiaImage matches the image the operator itself deploys
// (internal/controller/metadata.defaultOxiaImage).
const oxiaImage = "oxia/oxia:0.16.7"

// integrationShards forces the standalone server to spread keys across
// several shards, so a passing full-scan assertion actually exercises the
// cross-shard fan-out (shardManager.GetAll) rather than a single shard.
const integrationShards = 4

// startOxiaStandalone runs a single-node, multi-shard Oxia server in a
// throwaway Docker container and returns its host client address. The
// container is removed on test cleanup.
func startOxiaStandalone(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found on PATH; skipping real-Oxia integration test")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not available; skipping real-Oxia integration test")
	}

	runOut, err := exec.Command("docker", "run", "-d",
		"-p", "127.0.0.1:0:6648",
		oxiaImage,
		"oxia", "standalone", "-s", fmt.Sprintf("%d", integrationShards),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run oxia standalone: %v\n%s", err, runOut)
	}
	containerID := strings.TrimSpace(string(runOut))
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", containerID).Run()
	})

	portOut, err := exec.Command("docker", "port", containerID, "6648/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port: %v\n%s", err, portOut)
	}
	// e.g. "127.0.0.1:32768" (take the last colon-delimited field for the port).
	mapping := strings.TrimSpace(strings.SplitN(string(portOut), "\n", 2)[0])
	idx := strings.LastIndex(mapping, ":")
	if idx < 0 {
		t.Fatalf("unexpected docker port output: %q", mapping)
	}
	addr := "127.0.0.1:" + mapping[idx+1:]

	waitForOxiaReady(t, addr)
	return addr
}

// waitForOxiaReady blocks until the server accepts a client Put, or fails
// the test after a deadline.
func waitForOxiaReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		client, err := oxia.NewSyncClient(addr, oxia.WithNamespace(metadata.DefaultNamespace),
			oxia.WithRequestTimeout(2*time.Second))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, _, putErr := client.Put(ctx, "/readiness-probe", []byte("ok"))
			cancel()
			_ = client.Close()
			if putErr == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("oxia standalone at %s never became ready", addr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestExportFullScanAgainstRealOxia is the guard for the empty-bounds
// full-scan semantics: it seeds many keys (enough to land on multiple
// shards) plus one ephemeral key into a real Oxia, runs the real Exporter,
// and asserts EVERY seeded key appears in the manifest - proving
// RangeScan("","") scans the entire keyspace across all shards rather than
// returning an empty range.
func TestExportFullScanAgainstRealOxia(t *testing.T) {
	addr := startOxiaStandalone(t)
	ctx := context.Background()

	// The client that creates the ephemeral key must stay open for the
	// duration of the export: an ephemeral record lives only as long as its
	// creating session.
	writer, err := oxia.NewSyncClient(addr, oxia.WithNamespace(metadata.DefaultNamespace))
	if err != nil {
		t.Fatalf("NewSyncClient(writer): %v", err)
	}
	defer func() { _ = writer.Close() }()

	const seedCount = 60
	seeded := make(map[string]string, seedCount)
	for i := range seedCount {
		key := fmt.Sprintf("/managed-ledgers/topic-%03d", i)
		val := fmt.Sprintf("ledger-info-%d", i)
		if _, _, err := writer.Put(ctx, key, []byte(val)); err != nil {
			t.Fatalf("seed Put %q: %v", key, err)
		}
		seeded[key] = val
	}
	const ephemeralKey = "/owner/broker-lock"
	if _, _, err := writer.Put(ctx, ephemeralKey, []byte("owner"), oxia.Ephemeral()); err != nil {
		t.Fatalf("seed ephemeral Put: %v", err)
	}

	exporter := &Exporter{
		OxiaServiceAddress: addr,
		Namespaces:         []string{metadata.DefaultNamespace},
		NewClient:          NewOxiaClientFactory(addr),
	}
	var manifest bytes.Buffer
	header, err := exporter.Export(ctx, &manifest, fixedCapturedAt)
	if err != nil {
		t.Fatalf("Export() against real Oxia: %v", err)
	}

	// Every seeded regular key must be present with its exact value, and the
	// ephemeral key must be captured AND flagged so import can skip it.
	got := decodeManifestRecords(t, manifest.Bytes())
	var ephemeralFlagged bool
	for _, rec := range got {
		if rec.Key == ephemeralKey {
			if !rec.Version.Ephemeral {
				t.Errorf("ephemeral key %q captured but not flagged Ephemeral", ephemeralKey)
			}
			ephemeralFlagged = true
			continue
		}
		wantVal, ok := seeded[rec.Key]
		if !ok {
			continue // readiness-probe key or other; ignore
		}
		if string(rec.Value) != wantVal {
			t.Errorf("key %q value = %q, want %q", rec.Key, rec.Value, wantVal)
		}
		delete(seeded, rec.Key)
	}

	if len(seeded) != 0 {
		t.Fatalf("empty-bounds RangeScan did NOT return %d seeded key(s) - full-scan assumption is BROKEN: %v",
			len(seeded), keysOf(seeded))
	}
	if !ephemeralFlagged {
		t.Error("ephemeral key was not captured by the full scan")
	}
	if header.RecordCount < seedCount+1 {
		t.Errorf("header RecordCount = %d, want >= %d", header.RecordCount, seedCount+1)
	}
}

// TestExportImportRoundTripAgainstRealOxia proves the whole engine against
// two real Oxia servers: export from a seeded source, import into a fresh
// target, and assert every non-ephemeral key is present in the target with
// its value while the ephemeral key was skipped.
func TestExportImportRoundTripAgainstRealOxia(t *testing.T) {
	sourceAddr := startOxiaStandalone(t)
	targetAddr := startOxiaStandalone(t)
	ctx := context.Background()

	writer, err := oxia.NewSyncClient(sourceAddr, oxia.WithNamespace(metadata.DefaultNamespace))
	if err != nil {
		t.Fatalf("NewSyncClient(source writer): %v", err)
	}
	defer func() { _ = writer.Close() }()

	const seedCount = 40
	seeded := make(map[string]string, seedCount)
	for i := range seedCount {
		key := fmt.Sprintf("/schemas/schema-%03d", i)
		val := fmt.Sprintf("schema-body-%d", i)
		if _, _, err := writer.Put(ctx, key, []byte(val)); err != nil {
			t.Fatalf("seed Put %q: %v", key, err)
		}
		seeded[key] = val
	}
	const ephemeralKey = "/owner/session-lock"
	if _, _, err := writer.Put(ctx, ephemeralKey, []byte("owner"), oxia.Ephemeral()); err != nil {
		t.Fatalf("seed ephemeral Put: %v", err)
	}

	exporter := &Exporter{
		OxiaServiceAddress: sourceAddr,
		Namespaces:         []string{metadata.DefaultNamespace},
		NewClient:          NewOxiaClientFactory(sourceAddr),
	}
	var manifest bytes.Buffer
	if _, err := exporter.Export(ctx, &manifest, fixedCapturedAt); err != nil {
		t.Fatalf("Export(): %v", err)
	}

	importer := &Importer{NewClient: NewOxiaClientFactory(targetAddr)}
	result, err := importer.Import(ctx, &manifest)
	if err != nil {
		t.Fatalf("Import(): %v", err)
	}
	if result.KeysSkippedEphemeral != 1 {
		t.Errorf("KeysSkippedEphemeral = %d, want 1", result.KeysSkippedEphemeral)
	}

	// Read the target back and confirm every seeded key round-tripped and
	// the ephemeral key was not written.
	reader, err := oxia.NewSyncClient(targetAddr, oxia.WithNamespace(metadata.DefaultNamespace))
	if err != nil {
		t.Fatalf("NewSyncClient(target reader): %v", err)
	}
	defer func() { _ = reader.Close() }()

	for key, want := range seeded {
		_, val, _, err := reader.Get(ctx, key)
		if err != nil {
			t.Errorf("target missing key %q: %v", key, err)
			continue
		}
		if string(val) != want {
			t.Errorf("target key %q = %q, want %q", key, val, want)
		}
	}
	if _, _, _, err := reader.Get(ctx, ephemeralKey); err == nil {
		t.Errorf("ephemeral key %q was written to target; it must be skipped", ephemeralKey)
	}
}

func decodeManifestRecords(t *testing.T, manifest []byte) []ManifestRecord {
	t.Helper()
	mr := newManifestReader(bytes.NewReader(manifest))
	if _, err := mr.ReadHeader(); err != nil {
		t.Fatalf("ReadHeader(): %v", err)
	}
	records, err := readAllRecords(mr)
	if err != nil {
		t.Fatalf("readAllRecords(): %v", err)
	}
	return records
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
