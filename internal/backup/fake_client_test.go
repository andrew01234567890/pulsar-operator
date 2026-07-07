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
	"fmt"
)

// fakeOxiaClient is an in-memory stand-in for a namespace-scoped OxiaClient,
// seeded with a fixed set of records for RangeScanAll and recording every
// Put it receives so tests can assert on them.
type fakeOxiaClient struct {
	records []ScanResult
	scanErr error

	puts   []fakePut
	closed bool
}

type fakePut struct {
	Key   string
	Value []byte
}

func (f *fakeOxiaClient) RangeScanAll(_ context.Context) <-chan ScanResult {
	ch := make(chan ScanResult, len(f.records)+1)
	for _, r := range f.records {
		ch <- r
	}
	if f.scanErr != nil {
		ch <- ScanResult{Err: f.scanErr}
	}
	close(ch)
	return ch
}

func (f *fakeOxiaClient) Put(_ context.Context, key string, value []byte) error {
	f.puts = append(f.puts, fakePut{Key: key, Value: append([]byte(nil), value...)})
	return nil
}

func (f *fakeOxiaClient) Close() error {
	f.closed = true
	return nil
}

// fakeClientFactory builds a ClientFactory over a fixed, per-namespace set
// of fakeOxiaClients, erroring for any namespace it wasn't seeded with.
func fakeClientFactory(byNamespace map[string]*fakeOxiaClient) ClientFactory {
	return func(namespace string) (OxiaClient, error) {
		c, ok := byNamespace[namespace]
		if !ok {
			return nil, fmt.Errorf("fakeClientFactory: no client seeded for namespace %q", namespace)
		}
		return c, nil
	}
}
