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
	"strings"

	"github.com/andrew01234567890/pulsar-operator/internal/metadata"
)

// BookKeeper's RegistrationManager, bridged onto Oxia's "bookkeeper"
// namespace via the generic metadata-store driver, writes the cluster
// instanceId and per-bookie cookie records under whatever ledgersRootPath
// the cluster is configured with (see docs/docs/backup-and-dr.md). Keys are
// therefore matched by suffix/segment rather than an assumed absolute path.
const (
	bookKeeperInstanceIDKeyName = "INSTANCEID"
	bookKeeperCookieNodeSegment = "/cookies/"
)

// isBookKeeperInstanceIDKey reports whether key is the BookKeeper cluster
// instanceId record within the "bookkeeper" namespace.
func isBookKeeperInstanceIDKey(key string) bool {
	return key == bookKeeperInstanceIDKeyName || strings.HasSuffix(key, "/"+bookKeeperInstanceIDKeyName)
}

// isBookKeeperCookieKey reports whether key is a per-bookie cookie
// registration record within the "bookkeeper" namespace.
func isBookKeeperCookieKey(key string) bool {
	return strings.Contains(key, bookKeeperCookieNodeSegment)
}

// ReadTargetInstanceID connects to the "bookkeeper" namespace via newClient
// and returns the BookKeeper cluster instanceId currently recorded there, if
// any. It is Restore's counterpart to Exporter.scanNamespace's capture of the
// same record: the cookie-lineage gate needs the TARGET cluster's existing
// lineage (if the target Oxia has already been initialized) before deciding
// whether replaying a backup's captured lineage into it is safe.
//
// found is false for a fresh, uninitialized Oxia (no INSTANCEID record yet) -
// that is not an error, since a fresh target has no existing lineage to
// conflict with.
func ReadTargetInstanceID(ctx context.Context, newClient ClientFactory) (instanceID string, found bool, err error) {
	client, err := newClient(metadata.BookkeeperNamespace)
	if err != nil {
		return "", false, fmt.Errorf("backup: connect to oxia namespace %q: %w", metadata.BookkeeperNamespace, err)
	}
	defer func() { _ = client.Close() }()

	for res := range client.RangeScanAll(ctx) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", false, ctxErr
		}
		if res.Err != nil {
			return "", false, fmt.Errorf("backup: scan oxia namespace %q: %w", metadata.BookkeeperNamespace, res.Err)
		}
		if isBookKeeperInstanceIDKey(res.Key) {
			return string(res.Value), true, nil
		}
	}
	return "", false, nil
}
