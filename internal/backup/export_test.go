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
	"time"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

const testOxiaServiceAddress = "test-oxia:6648"

var fixedCapturedAt = time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

// seedSourceClients returns the fake per-namespace source clients used by
// most of this file's tests: two records in "default" (one of them
// ephemeral), one in "broker", and three in "bookkeeper" - an INSTANCEID
// record, a cookie registration record, and one ordinary ledger-metadata
// record - so a single export exercises every capture rule the Exporter is
// responsible for.
func seedSourceClients() map[string]*fakeOxiaClient {
	return map[string]*fakeOxiaClient{
		metadata.DefaultNamespace: {records: []ScanResult{
			{
				Key:     "/managed-ledgers/public/default/persistent/topic1",
				Value:   []byte("managed-ledger-info"),
				Version: RecordVersion{VersionID: 1, ModifiedTimestamp: 1000},
			},
			{
				Key:     "/owner/broker-1",
				Value:   []byte("broker-1"),
				Version: RecordVersion{VersionID: 1, Ephemeral: true, SessionID: 42},
			},
		}},
		metadata.BrokerNamespace: {records: []ScanResult{
			{
				Key:     "/loadbalance/brokers/broker-1:8080",
				Value:   []byte("load-report"),
				Version: RecordVersion{VersionID: 1},
			},
		}},
		metadata.BookkeeperNamespace: {records: []ScanResult{
			{
				Key:     "/ledgers/INSTANCEID",
				Value:   []byte("instance-abc-123"),
				Version: RecordVersion{VersionID: 1},
			},
			{
				Key:     "/ledgers/cookies/bookie-1",
				Value:   []byte("cookie-bookie-1"),
				Version: RecordVersion{VersionID: 1},
			},
			{
				Key:     "/ledgers/00/L0001",
				Value:   []byte("ledger-metadata"),
				Version: RecordVersion{VersionID: 3, ModifiedTimestamp: 2000},
			},
		}},
	}
}

func newTestExporter(clients map[string]*fakeOxiaClient) *Exporter {
	return &Exporter{
		OxiaServiceAddress: testOxiaServiceAddress,
		Namespaces:         DefaultNamespaces,
		NewClient:          fakeClientFactory(clients),
	}
}

func TestExportCapturesEveryNamespaceAndBookKeeperLineage(t *testing.T) {
	exporter := newTestExporter(seedSourceClients())

	var buf bytes.Buffer
	header, err := exporter.Export(context.Background(), &buf, fixedCapturedAt)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	if header.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", header.SchemaVersion, SchemaVersion)
	}
	if header.OxiaServiceAddress != testOxiaServiceAddress {
		t.Errorf("OxiaServiceAddress = %q, want %q", header.OxiaServiceAddress, testOxiaServiceAddress)
	}
	if !equalStrings(header.Namespaces, DefaultNamespaces) {
		t.Errorf("Namespaces = %v, want %v", header.Namespaces, DefaultNamespaces)
	}
	if header.CapturedInstanceID != "instance-abc-123" {
		t.Errorf("CapturedInstanceID = %q, want %q", header.CapturedInstanceID, "instance-abc-123")
	}
	if !equalStrings(header.CookieKeys, []string{"/ledgers/cookies/bookie-1"}) {
		t.Errorf("CookieKeys = %v, want [/ledgers/cookies/bookie-1]", header.CookieKeys)
	}
	if header.RecordCount != 6 {
		t.Errorf("RecordCount = %d, want 6", header.RecordCount)
	}
	if header.Checksum == "" {
		t.Error("Checksum is empty, want a populated digest")
	}
	if !header.CapturedAt.Equal(fixedCapturedAt) {
		t.Errorf("CapturedAt = %v, want %v (engine must not call time.Now())", header.CapturedAt, fixedCapturedAt)
	}
}

func TestExportClosesEveryNamespaceClient(t *testing.T) {
	clients := seedSourceClients()
	exporter := newTestExporter(clients)

	var buf bytes.Buffer
	if _, err := exporter.Export(context.Background(), &buf, fixedCapturedAt); err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	for ns, c := range clients {
		if !c.closed {
			t.Errorf("namespace %q client was not closed", ns)
		}
	}
}

func TestExportDefaultsNamespaces(t *testing.T) {
	clients := map[string]*fakeOxiaClient{
		metadata.DefaultNamespace:    {},
		metadata.BrokerNamespace:     {},
		metadata.BookkeeperNamespace: {},
	}
	exporter := &Exporter{OxiaServiceAddress: testOxiaServiceAddress, NewClient: fakeClientFactory(clients)}

	var buf bytes.Buffer
	header, err := exporter.Export(context.Background(), &buf, fixedCapturedAt)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if !equalStrings(header.Namespaces, DefaultNamespaces) {
		t.Errorf("Namespaces = %v, want DefaultNamespaces %v", header.Namespaces, DefaultNamespaces)
	}
}

func TestExportSurfacesScanError(t *testing.T) {
	boom := errors.New("boom")
	clients := map[string]*fakeOxiaClient{
		metadata.DefaultNamespace:    {scanErr: boom},
		metadata.BrokerNamespace:     {},
		metadata.BookkeeperNamespace: {},
	}
	exporter := newTestExporter(clients)

	var buf bytes.Buffer
	_, err := exporter.Export(context.Background(), &buf, fixedCapturedAt)
	if !errors.Is(err, boom) {
		t.Fatalf("Export() error = %v, want wrapping %v", err, boom)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
