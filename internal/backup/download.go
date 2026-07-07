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
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

// ParseImportResult decodes an ImportResult from the raw bytes of a Job pod's
// termination message. It is the reconciler-side half of the wire contract
// runImportFromObjectStore writes.
func ParseImportResult(data []byte) (ImportResult, error) {
	var r ImportResult
	if err := json.Unmarshal(bytes.TrimSpace(data), &r); err != nil {
		return ImportResult{}, fmt.Errorf("%s: parse import result: %w", ImportCommandName, err)
	}
	return r, nil
}

// runImportFromObjectStore downloads the manifest described by flags.Src/
// flags.SrcKey (the Restore reconciler resolves spec.source.artifactURI down
// to this bare key before building the Job - see objectstore.KeyFromURI),
// replays it into the target Oxia at flags.OxiaAddress, and records an
// ImportResult to flags.ResultPath (and, for human-readable logs, to
// stdout/stderr) - the mirror of runExportToObjectStore.
func runImportFromObjectStore(ctx context.Context, flags ImportFlags, newClient ClientFactory, stdout, stderr io.Writer) error {
	store, err := objectstore.New(ctx, flags.Src)
	if err != nil {
		return fmt.Errorf("%s: %w", ImportCommandName, err)
	}
	rc, err := store.Download(ctx, flags.SrcKey)
	if err != nil {
		return fmt.Errorf("%s: download manifest: %w", ImportCommandName, err)
	}
	defer func() { _ = rc.Close() }()

	result, err := RunImport(ctx, flags, newClient, rc, stderr)
	if err != nil {
		return err
	}

	if err := writeImportResult(flags.ResultPath, result, stdout); err != nil {
		return fmt.Errorf("%s: %w", ImportCommandName, err)
	}
	return nil
}

// writeImportResult serializes result as JSON, writes it to resultPath (the
// container termination-message file the reconciler reads back), and echoes
// it to stdout for operator-visible Job logs. A failure to write resultPath
// is fatal: without it the reconciler cannot learn what was restored, so the
// import must be treated as failed rather than silently completing.
func writeImportResult(resultPath string, result ImportResult, stdout io.Writer) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal import result: %w", err)
	}
	if err := os.WriteFile(resultPath, data, 0o644); err != nil { //nolint:gosec // termination-message file, not a secret
		return fmt.Errorf("write import result to %q: %w", resultPath, err)
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", data); err != nil {
		return fmt.Errorf("echo import result: %w", err)
	}
	return nil
}
