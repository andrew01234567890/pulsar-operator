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

import "strings"

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
