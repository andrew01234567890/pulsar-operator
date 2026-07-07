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
	"time"

	"github.com/andrew01234567890/pulsar-operator/internal/objectstore"
)

// defaultResultPath is a container's default terminationMessagePath. The
// export Job writes its ExportResult here so the Backup reconciler can read it
// back from the pod's terminated-container Message field (no pod-log streaming
// or extra RBAC needed - the result JSON is a few hundred bytes, far under the
// 4KiB termination-message cap).
const defaultResultPath = "/dev/termination-log"

// ExportResult is the machine-readable summary the export Job writes to its
// termination-message file after a successful upload, so the Backup reconciler
// can populate status truthfully (artifactURI, key/instanceId/size read back
// from the completed export rather than guessed). Its JSON encoding is the
// wire contract between the Job and the reconciler; keep the tags stable.
type ExportResult struct {
	ArtifactURI        string `json:"artifactURI"`
	OxiaKeysCaptured   int64  `json:"oxiaKeysCaptured"`
	CapturedInstanceID string `json:"capturedInstanceId"`
	SizeBytes          int64  `json:"sizeBytes"`
}

// ParseExportResult decodes an ExportResult from the raw bytes of a Job pod's
// termination message. It is the reconciler-side half of the wire contract
// runExportToObjectStore writes.
func ParseExportResult(data []byte) (ExportResult, error) {
	var r ExportResult
	if err := json.Unmarshal(bytes.TrimSpace(data), &r); err != nil {
		return ExportResult{}, fmt.Errorf("%s: parse export result: %w", ExportCommandName, err)
	}
	return r, nil
}

// runExportToObjectStore exports the full Oxia keyspace into an in-memory
// manifest, uploads it to the object store described by flags.Dest under
// flags.DestKey, and records an ExportResult to flags.ResultPath (and, for
// human-readable logs, to stderr). The manifest is buffered rather than
// streamed because the exporter already accumulates every record to compute
// the header's leading RecordCount/Checksum, and buffering additionally yields
// the exact SizeBytes to report without a follow-up stat call.
func runExportToObjectStore(ctx context.Context, flags ExportFlags, newClient ClientFactory, capturedAt time.Time, stdout, stderr io.Writer) error {
	var manifest bytes.Buffer
	header, err := RunExport(ctx, flags, newClient, &manifest, capturedAt, stderr)
	if err != nil {
		return err
	}

	store, err := objectstore.New(ctx, flags.Dest)
	if err != nil {
		return fmt.Errorf("%s: %w", ExportCommandName, err)
	}
	if err := store.Upload(ctx, flags.DestKey, bytes.NewReader(manifest.Bytes())); err != nil {
		return fmt.Errorf("%s: upload manifest: %w", ExportCommandName, err)
	}

	result := ExportResult{
		ArtifactURI:        store.URI(flags.DestKey),
		OxiaKeysCaptured:   header.RecordCount,
		CapturedInstanceID: header.CapturedInstanceID,
		SizeBytes:          int64(manifest.Len()),
	}
	if err := writeExportResult(flags.ResultPath, result, stdout); err != nil {
		return fmt.Errorf("%s: %w", ExportCommandName, err)
	}
	return nil
}

// writeExportResult serializes result as JSON, writes it to resultPath (the
// container termination-message file the reconciler reads back), and echoes it
// to stdout for operator-visible Job logs. A failure to write resultPath is
// fatal: without it the reconciler cannot learn what was captured, so the
// export must be treated as failed rather than silently completing.
func writeExportResult(resultPath string, result ExportResult, stdout io.Writer) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal export result: %w", err)
	}
	if err := os.WriteFile(resultPath, data, 0o644); err != nil { //nolint:gosec // termination-message file, not a secret
		return fmt.Errorf("write export result to %q: %w", resultPath, err)
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", data); err != nil {
		return fmt.Errorf("echo export result: %w", err)
	}
	return nil
}
