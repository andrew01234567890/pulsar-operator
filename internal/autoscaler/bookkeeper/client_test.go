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

package bookkeeper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPBookieAdminClient_State(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/bookie/state" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"running":true,"readOnly":false,"shuttingDown":false,"availableForHighPriorityWrites":true}`))
	}))
	defer srv.Close()

	client := NewHTTPBookieAdminClient()
	state, err := client.State(context.Background(), strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("State() error = %v", err)
	}
	if !state.Writable() {
		t.Errorf("expected a running, non-read-only, non-shutting-down bookie to be Writable()")
	}
}

func TestHTTPBookieAdminClient_Info(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"freeSpace":25,"totalSpace":100}`))
	}))
	defer srv.Close()

	client := NewHTTPBookieAdminClient()
	info, err := client.Info(context.Background(), strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("Info() error = %v", err)
	}
	if len(info.LedgerDisks) != 1 {
		t.Fatalf("expected exactly one aggregate ledger disk entry, got %d", len(info.LedgerDisks))
	}
	if got, want := info.LedgerDisks[0].Fraction(), 0.75; got != want {
		t.Errorf("UsedFraction() = %v, want %v", got, want)
	}
}

// A 200 with an empty or drifted body decodes totalSpace=0; without the
// guard in Info that reads as a 100%-full disk and drives an unbounded
// scale-up. Each of these must instead fail the tick with a plain
// (non-ConnectionError) data-integrity error.
func TestHTTPBookieAdminClient_Info_RejectsMalformedBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty body", `{}`},
		{"zero totalSpace", `{"freeSpace":0,"totalSpace":0}`},
		{"missing totalSpace", `{"freeSpace":25}`},
		{"negative totalSpace", `{"freeSpace":0,"totalSpace":-1}`},
		{"unknown/drifted fields only", `{"usedBytes":50,"capacityBytes":100}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			client := NewHTTPBookieAdminClient()
			_, err := client.Info(context.Background(), strings.TrimPrefix(srv.URL, "http://"))
			if err == nil {
				t.Fatalf("Info() with body %q: expected an error, got nil", tt.body)
			}
			if IsConnectionError(err) {
				t.Errorf("Info() malformed-body error must be a data-integrity error, not a ConnectionError: %v", err)
			}
		})
	}
}

// A non-2xx response from a bookie's admin API means it is unhealthy/not
// serving right now; both State and Info must surface that as a
// ConnectionError so Evaluate skips just that one bookie instead of failing
// the whole tick.
func TestHTTPBookieAdminClient_NonOKStatusIsConnectionError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	client := NewHTTPBookieAdminClient()

	if _, err := client.State(context.Background(), addr); !IsConnectionError(err) {
		t.Errorf("State() on a 503 = %v, want a ConnectionError", err)
	}
	if _, err := client.Info(context.Background(), addr); !IsConnectionError(err) {
		t.Errorf("Info() on a 503 = %v, want a ConnectionError", err)
	}
}

func TestHTTPBookieAdminClient_UnderReplicatedLedgerCount(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       int
	}{
		{"none found (404)", http.StatusNotFound, "No under replicated ledgers found", 0},
		{"some found (200)", http.StatusOK, "[1,2,3]", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			client := NewHTTPBookieAdminClient()
			got, err := client.UnderReplicatedLedgerCount(context.Background(), strings.TrimPrefix(srv.URL, "http://"))
			if err != nil {
				t.Fatalf("UnderReplicatedLedgerCount() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("UnderReplicatedLedgerCount() = %d, want %d", got, tt.want)
			}
		})
	}
}
