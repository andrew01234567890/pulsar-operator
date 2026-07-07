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
	"testing"
)

const testFlagOxiaAddress = "my-oxia:6648"
const flagOxia = "--oxia"

func TestParseExportFlags(t *testing.T) {
	flags, err := parseExportFlags([]string{
		flagOxia, testFlagOxiaAddress,
		"--out", "/tmp/manifest.jsonl",
		"--namespaces", "default, broker ,bookkeeper",
	})
	if err != nil {
		t.Fatalf("parseExportFlags() error = %v", err)
	}
	if flags.OxiaAddress != testFlagOxiaAddress {
		t.Errorf("OxiaAddress = %q, want %q", flags.OxiaAddress, testFlagOxiaAddress)
	}
	if flags.OutPath != "/tmp/manifest.jsonl" {
		t.Errorf("OutPath = %q, want %q", flags.OutPath, "/tmp/manifest.jsonl")
	}
	if !equalStrings(flags.Namespaces, DefaultNamespaces) {
		t.Errorf("Namespaces = %v, want %v", flags.Namespaces, DefaultNamespaces)
	}
}

func TestParseExportFlagsDefaults(t *testing.T) {
	flags, err := parseExportFlags([]string{flagOxia, testFlagOxiaAddress})
	if err != nil {
		t.Fatalf("parseExportFlags() error = %v", err)
	}
	if flags.OutPath != stdioPath {
		t.Errorf("OutPath = %q, want %q (stdout)", flags.OutPath, stdioPath)
	}
	if !equalStrings(flags.Namespaces, DefaultNamespaces) {
		t.Errorf("Namespaces = %v, want DefaultNamespaces %v", flags.Namespaces, DefaultNamespaces)
	}
}

func TestParseExportFlagsRequiresOxiaAddress(t *testing.T) {
	if _, err := parseExportFlags([]string{"--out", "-"}); err == nil {
		t.Fatal("parseExportFlags() error = nil, want an error for missing --oxia")
	}
}

func TestParseImportFlagsRequiresOxiaAddress(t *testing.T) {
	if _, err := parseImportFlags([]string{"--in", "-"}); err == nil {
		t.Fatal("parseImportFlags() error = nil, want an error for missing --oxia")
	}
}

func TestRunExportThenRunImportViaFakeFactory(t *testing.T) {
	exportFlags := ExportFlags{
		OxiaAddress: testOxiaServiceAddress,
		Namespaces:  DefaultNamespaces,
	}

	var manifest, log bytes.Buffer
	header, err := RunExport(context.Background(), exportFlags, fakeClientFactory(seedSourceClients()), &manifest, fixedCapturedAt, &log)
	if err != nil {
		t.Fatalf("RunExport() error = %v", err)
	}
	if log.Len() == 0 {
		t.Error("RunExport() wrote no summary to the log writer")
	}

	targets := targetClients()
	importFlags := ImportFlags{OxiaAddress: testFlagOxiaAddress}
	log.Reset()
	result, err := RunImport(context.Background(), importFlags, fakeClientFactory(targets), &manifest, &log)
	if err != nil {
		t.Fatalf("RunImport() error = %v", err)
	}
	if result.CapturedInstanceID != header.CapturedInstanceID {
		t.Errorf("CapturedInstanceID = %q, want %q", result.CapturedInstanceID, header.CapturedInstanceID)
	}
	if log.Len() == 0 {
		t.Error("RunImport() wrote no summary to the log writer")
	}
}
