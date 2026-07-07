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
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// ExportCommandName and ImportCommandName are the manager subcommands
// wired up in cmd/main.go (e.g. "manager backup-export --oxia <addr> --out
// <file>"), and are what the future Backup/Restore Jobs (#40/#41) invoke.
const (
	ExportCommandName = "backup-export"
	ImportCommandName = "backup-import"
)

// stdioPath is the --out/--in value meaning "use stdout/stdin" instead of a
// local file, matching common CLI convention.
const stdioPath = "-"

// ExportFlags are the parsed flags for the backup-export subcommand.
type ExportFlags struct {
	OxiaAddress string
	OutPath     string
	Namespaces  []string
}

func parseExportFlags(args []string) (ExportFlags, error) {
	fs := flag.NewFlagSet(ExportCommandName, flag.ContinueOnError)
	oxiaAddr := fs.String("oxia", "", "Oxia service address (host:port) to export from")
	outPath := fs.String("out", stdioPath, "Output manifest file path, or - for stdout")
	namespaces := fs.String("namespaces", strings.Join(DefaultNamespaces, ","),
		"Comma-separated Oxia namespaces to export")
	if err := fs.Parse(args); err != nil {
		return ExportFlags{}, err
	}
	if *oxiaAddr == "" {
		return ExportFlags{}, fmt.Errorf("%s: --oxia is required", ExportCommandName)
	}

	return ExportFlags{
		OxiaAddress: *oxiaAddr,
		OutPath:     *outPath,
		Namespaces:  splitNamespaces(*namespaces),
	}, nil
}

func splitNamespaces(s string) []string {
	var out []string
	for ns := range strings.SplitSeq(s, ",") {
		ns = strings.TrimSpace(ns)
		if ns != "" {
			out = append(out, ns)
		}
	}
	return out
}

// RunExport runs an export against an already-constructed ClientFactory,
// writing the manifest to out and a one-line human-readable summary to log.
// Split out from RunExportCommand so the flag/wiring logic can be tested
// against a fake ClientFactory without touching the filesystem or a real
// Oxia service.
func RunExport(ctx context.Context, flags ExportFlags, newClient ClientFactory, out io.Writer, capturedAt time.Time, log io.Writer) (ManifestHeader, error) {
	exporter := &Exporter{
		OxiaServiceAddress: flags.OxiaAddress,
		Namespaces:         flags.Namespaces,
		NewClient:          newClient,
	}

	header, err := exporter.Export(ctx, out, capturedAt)
	if err != nil {
		return header, err
	}

	if _, err := fmt.Fprintf(log, "%s: captured %d record(s) across namespaces %v (capturedInstanceId=%q)\n",
		ExportCommandName, header.RecordCount, header.Namespaces, header.CapturedInstanceID); err != nil {
		return header, fmt.Errorf("%s: write summary: %w", ExportCommandName, err)
	}
	return header, nil
}

// RunExportCommand parses args as the backup-export subcommand's flags,
// opens --out (or uses stdout for "-"), connects to the real Oxia client at
// --oxia, and runs the export. capturedAt is supplied by the caller (main.go
// calls time.Now() once, right at the process boundary).
func RunExportCommand(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, capturedAt time.Time) error {
	flags, err := parseExportFlags(args)
	if err != nil {
		return err
	}

	out := stdout
	if flags.OutPath != stdioPath {
		f, err := os.Create(flags.OutPath)
		if err != nil {
			return fmt.Errorf("%s: open output file %q: %w", ExportCommandName, flags.OutPath, err)
		}
		defer func() { _ = f.Close() }()
		out = f
	}

	_, err = RunExport(ctx, flags, NewOxiaClientFactory(flags.OxiaAddress), out, capturedAt, stderr)
	return err
}

// ImportFlags are the parsed flags for the backup-import subcommand.
type ImportFlags struct {
	OxiaAddress string
	InPath      string
}

func parseImportFlags(args []string) (ImportFlags, error) {
	fs := flag.NewFlagSet(ImportCommandName, flag.ContinueOnError)
	oxiaAddr := fs.String("oxia", "", "Oxia service address (host:port) to import into")
	inPath := fs.String("in", stdioPath, "Input manifest file path, or - for stdin")
	if err := fs.Parse(args); err != nil {
		return ImportFlags{}, err
	}
	if *oxiaAddr == "" {
		return ImportFlags{}, fmt.Errorf("%s: --oxia is required", ImportCommandName)
	}

	return ImportFlags{OxiaAddress: *oxiaAddr, InPath: *inPath}, nil
}

// RunImport runs an import against an already-constructed ClientFactory,
// reading the manifest from in and writing a one-line human-readable
// summary to log. Split out from RunImportCommand so the flag/wiring logic
// can be tested against a fake ClientFactory without touching the
// filesystem or a real Oxia service.
func RunImport(ctx context.Context, flags ImportFlags, newClient ClientFactory, in io.Reader, log io.Writer) (ImportResult, error) {
	importer := &Importer{NewClient: newClient}

	result, err := importer.Import(ctx, in)
	if err != nil {
		return result, err
	}

	if _, err := fmt.Fprintf(log, "%s: wrote %d key(s), skipped %d ephemeral key(s) (capturedInstanceId=%q)\n",
		ImportCommandName, result.KeysWritten, result.KeysSkippedEphemeral, result.CapturedInstanceID); err != nil {
		return result, fmt.Errorf("%s: write summary: %w", ImportCommandName, err)
	}
	return result, nil
}

// RunImportCommand parses args as the backup-import subcommand's flags,
// opens --in (or uses stdin for "-"), connects to the real Oxia client at
// --oxia, and runs the import.
func RunImportCommand(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) error {
	flags, err := parseImportFlags(args)
	if err != nil {
		return err
	}

	in := stdin
	if flags.InPath != stdioPath {
		f, err := os.Open(flags.InPath)
		if err != nil {
			return fmt.Errorf("%s: open input file %q: %w", ImportCommandName, flags.InPath, err)
		}
		defer func() { _ = f.Close() }()
		in = f
	}

	_, err = RunImport(ctx, flags, NewOxiaClientFactory(flags.OxiaAddress), in, stderr)
	return err
}
