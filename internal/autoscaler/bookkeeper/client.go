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

// Package bookkeeper implements the bookie disk-watermark scale-up decision
// algorithm and the client used to poll each bookie's admin REST API. The
// algorithm (Evaluate/Decide) depends only on the BookieAdminClient
// interface, never on the concrete HTTP implementation, so callers can unit
// test the decision logic with a mock client instead of a live bookie
// ensemble.
package bookkeeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ConnectionError marks a poll failure that means a single bookie is
// unreachable or unhealthy right now (a transport failure, or a non-2xx
// admin-API response) — as opposed to a data-integrity failure (a 200 whose
// body is unparseable or nonsensical). Evaluate skips a bookie that returns a
// ConnectionError and keeps evaluating the rest, but fails the whole tick on
// a data-integrity error, so one flaky bookie can't block a legitimate
// scale-up while a corrupt reading still can't silently drive one.
type ConnectionError struct {
	BookieAddr string
	Err        error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("bookie %s unreachable: %v", e.BookieAddr, e.Err)
}

func (e *ConnectionError) Unwrap() error { return e.Err }

// IsConnectionError reports whether err is (or wraps) a ConnectionError.
func IsConnectionError(err error) bool {
	var connErr *ConnectionError
	return errors.As(err, &connErr)
}

// BookieState mirrors the response body of the bookie admin REST endpoint
// GET /api/v1/bookie/state.
type BookieState struct {
	Running      bool
	ReadOnly     bool
	ShuttingDown bool
}

// Writable reports whether a bookie in this state accepts new ledger
// writes: it must be running, not read-only, and not shutting down.
func (s BookieState) Writable() bool {
	return s.Running && !s.ReadOnly && !s.ShuttingDown
}

// BookieDiskUsage is one ledger directory's usage, as reported by the bookie
// admin REST endpoint GET /api/v1/bookie/info.
type BookieDiskUsage struct {
	UsedBytes  int64
	TotalBytes int64
}

// Fraction returns the disk used, in [0,1]. The HTTP client rejects a
// non-positive totalSpace upstream in Info (a misread fails the tick rather
// than reaching here), so TotalBytes<=0 only arises from a directly
// constructed value; it is reported as fully used (1.0) as a last-resort
// defensive default.
func (d BookieDiskUsage) Fraction() float64 {
	if d.TotalBytes <= 0 {
		return 1
	}
	used := max(d.UsedBytes, 0)
	used = min(used, d.TotalBytes)
	return float64(used) / float64(d.TotalBytes)
}

// BookieInfo is a bookie's disk usage, one entry per ledger directory. The
// operator always configures exactly one ledger directory per bookie (see
// bookkeeper_controller.go's operatorManagedConfig), and the real
// /api/v1/bookie/info endpoint reports a single aggregate free/total space
// pair across all of a bookie's ledger directories, so the default HTTP
// client always returns a single-element slice. The slice shape is kept
// general so the "ALL ledger dirs" HWM check in Decide is correct if a
// future multi-directory bookie layout reports more than one entry.
type BookieInfo struct {
	LedgerDisks []BookieDiskUsage
}

// BookieAdminClient polls a single bookie's admin REST API. Implementations
// are injected into the autoscaler controller so tests can supply a mock
// instead of talking to a live bookie ensemble.
type BookieAdminClient interface {
	// Info returns bookieAddr's ledger-directory disk usage
	// (GET /api/v1/bookie/info).
	Info(ctx context.Context, bookieAddr string) (BookieInfo, error)

	// State returns bookieAddr's running/read-only/shutting-down state
	// (GET /api/v1/bookie/state).
	State(ctx context.Context, bookieAddr string) (BookieState, error)

	// UnderReplicatedLedgerCount returns the cluster-wide count of
	// under-replicated ledgers, as seen from bookieAddr
	// (GET /api/v1/autorecovery/list_under_replicated_ledger/). It is
	// cluster-wide metadata, so any writable bookie can serve it. The
	// scale-up algorithm in this package does not consult it (strict
	// priority is deficit -> HWM -> no-op), but it is part of this
	// interface so the follow-up guarded bookie scale-down/decommission
	// controller can reuse the same injectable client.
	UnderReplicatedLedgerCount(ctx context.Context, bookieAddr string) (int, error)
}

// DefaultAdminPort is the bookie admin HTTP port. It mirrors
// bookieAdminPort in internal/controller/cluster/bookkeeper_controller.go,
// which the operator always asserts via operatorManagedConfig
// (httpServerPort is not user-overridable), so every bookie's admin API is
// reachable on this port.
const DefaultAdminPort = 8000

// HTTPBookieAdminClient is the default BookieAdminClient, hitting each
// bookie's admin REST API directly over HTTP. It holds no per-bookie state,
// so a single instance is shared across every bookie address and every
// autoscaler tick.
type HTTPBookieAdminClient struct {
	// HTTPClient is the client used for every request. Defaults to a
	// client with a 10s timeout when nil.
	HTTPClient *http.Client
}

// NewHTTPBookieAdminClient returns an HTTPBookieAdminClient with a sane
// request timeout.
func NewHTTPBookieAdminClient() *HTTPBookieAdminClient {
	return &HTTPBookieAdminClient{HTTPClient: &http.Client{Timeout: 10 * time.Second}}
}

func (c *HTTPBookieAdminClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *HTTPBookieAdminClient) get(ctx context.Context, bookieAddr, path string) (*http.Response, error) {
	url := fmt.Sprintf("http://%s%s", bookieAddr, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", url, err)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, &ConnectionError{BookieAddr: bookieAddr, Err: fmt.Errorf("request %s: %w", url, err)}
	}
	return resp, nil
}

// State implements BookieAdminClient.
func (c *HTTPBookieAdminClient) State(ctx context.Context, bookieAddr string) (BookieState, error) {
	resp, err := c.get(ctx, bookieAddr, "/api/v1/bookie/state")
	if err != nil {
		return BookieState{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return BookieState{}, &ConnectionError{BookieAddr: bookieAddr, Err: fmt.Errorf("unexpected status %d from /api/v1/bookie/state", resp.StatusCode)}
	}

	var body struct {
		Running      bool `json:"running"`
		ReadOnly     bool `json:"readOnly"`
		ShuttingDown bool `json:"shuttingDown"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return BookieState{}, fmt.Errorf("bookie %s: decode /api/v1/bookie/state: %w", bookieAddr, err)
	}

	return BookieState{Running: body.Running, ReadOnly: body.ReadOnly, ShuttingDown: body.ShuttingDown}, nil
}

// Info implements BookieAdminClient.
func (c *HTTPBookieAdminClient) Info(ctx context.Context, bookieAddr string) (BookieInfo, error) {
	resp, err := c.get(ctx, bookieAddr, "/api/v1/bookie/info")
	if err != nil {
		return BookieInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return BookieInfo{}, &ConnectionError{BookieAddr: bookieAddr, Err: fmt.Errorf("unexpected status %d from /api/v1/bookie/info", resp.StatusCode)}
	}

	var body struct {
		FreeSpace  int64 `json:"freeSpace"`
		TotalSpace int64 `json:"totalSpace"`
	}
	// DisallowUnknownFields makes a drifted or truncated response an explicit
	// decode error rather than a silent zero-value struct: a body missing the
	// fields we need must fail the tick, not fabricate a reading.
	dec := json.NewDecoder(resp.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return BookieInfo{}, fmt.Errorf("bookie %s: decode /api/v1/bookie/info: %w", bookieAddr, err)
	}

	// A non-positive totalSpace is a nonsense reading (an empty or partial
	// body decodes to totalSpace=0). Returning it would let BookieDiskUsage
	// .Fraction() report the bookie as 100% full and trigger an unbounded
	// scale-up, so treat it as a hard error and fail this tick instead.
	if body.TotalSpace <= 0 {
		return BookieInfo{}, fmt.Errorf("bookie %s reported non-positive totalSpace %d from /api/v1/bookie/info", bookieAddr, body.TotalSpace)
	}

	return BookieInfo{
		LedgerDisks: []BookieDiskUsage{{
			UsedBytes:  body.TotalSpace - body.FreeSpace,
			TotalBytes: body.TotalSpace,
		}},
	}, nil
}

// UnderReplicatedLedgerCount implements BookieAdminClient. The endpoint
// returns 404 with a "No under replicated ledgers found" body when there are
// none, and 200 with a JSON array of ledger IDs otherwise.
func (c *HTTPBookieAdminClient) UnderReplicatedLedgerCount(ctx context.Context, bookieAddr string) (int, error) {
	resp, err := c.get(ctx, bookieAddr, "/api/v1/autorecovery/list_under_replicated_ledger/")
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bookie %s: unexpected status %d from /api/v1/autorecovery/list_under_replicated_ledger/", bookieAddr, resp.StatusCode)
	}

	var ledgerIDs []int64
	if err := json.NewDecoder(resp.Body).Decode(&ledgerIDs); err != nil {
		return 0, fmt.Errorf("bookie %s: decode /api/v1/autorecovery/list_under_replicated_ledger/: %w", bookieAddr, err)
	}
	return len(ledgerIDs), nil
}
