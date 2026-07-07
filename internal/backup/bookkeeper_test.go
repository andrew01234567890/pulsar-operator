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
	"testing"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

const testTargetInstanceID = "target-instance-777"

func TestReadTargetInstanceIDFound(t *testing.T) {
	clients := map[string]*fakeOxiaClient{
		metadata.BookkeeperNamespace: {records: []ScanResult{
			{Key: "/ledgers/INSTANCEID", Value: []byte(testTargetInstanceID), Version: RecordVersion{VersionID: 1}},
			{Key: "/ledgers/00/L0001", Value: []byte("ledger-metadata"), Version: RecordVersion{VersionID: 1}},
		}},
	}

	instanceID, found, err := ReadTargetInstanceID(context.Background(), fakeClientFactory(clients))
	if err != nil {
		t.Fatalf("ReadTargetInstanceID() error = %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if instanceID != testTargetInstanceID {
		t.Errorf("instanceID = %q, want %q", instanceID, testTargetInstanceID)
	}
	if !clients[metadata.BookkeeperNamespace].closed {
		t.Error("bookkeeper namespace client was not closed")
	}
}

func TestReadTargetInstanceIDFreshCluster(t *testing.T) {
	clients := map[string]*fakeOxiaClient{
		metadata.BookkeeperNamespace: {},
	}

	instanceID, found, err := ReadTargetInstanceID(context.Background(), fakeClientFactory(clients))
	if err != nil {
		t.Fatalf("ReadTargetInstanceID() error = %v", err)
	}
	if found {
		t.Fatal("found = true, want false for a fresh target with no INSTANCEID record")
	}
	if instanceID != "" {
		t.Errorf("instanceID = %q, want empty", instanceID)
	}
}

func TestReadTargetInstanceIDSurfacesScanError(t *testing.T) {
	boom := errors.New("boom")
	clients := map[string]*fakeOxiaClient{
		metadata.BookkeeperNamespace: {scanErr: boom},
	}

	_, _, err := ReadTargetInstanceID(context.Background(), fakeClientFactory(clients))
	if !errors.Is(err, boom) {
		t.Fatalf("ReadTargetInstanceID() error = %v, want wrapping %v", err, boom)
	}
}

func TestReadTargetInstanceIDConnectError(t *testing.T) {
	// fakeClientFactory errors for any namespace it wasn't seeded with.
	_, _, err := ReadTargetInstanceID(context.Background(), fakeClientFactory(map[string]*fakeOxiaClient{}))
	if err == nil {
		t.Fatal("expected an error when the client factory cannot connect")
	}
}
