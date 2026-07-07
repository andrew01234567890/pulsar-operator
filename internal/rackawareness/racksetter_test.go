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

package rackawareness

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPodExecRackSetter_SetBookieRack_BuildsExpectedCommand(t *testing.T) {
	var gotCommand []string
	setter := &PodExecRackSetter{
		execFn: func(_ context.Context, command []string) (string, error) {
			gotCommand = command
			return "", nil
		},
	}

	if err := setter.SetBookieRack(context.Background(), "bk-0.bk.default:3181", "/us-east-1a"); err != nil {
		t.Fatalf("SetBookieRack() error = %v", err)
	}

	want := []string{
		pulsarAdminBin, "bookies", "set-bookie-rack",
		"--bookie", "bk-0.bk.default:3181", "--rack", "/us-east-1a", "--hostname", "bk-0.bk.default:3181",
	}
	if strings.Join(gotCommand, " ") != strings.Join(want, " ") {
		t.Errorf("command = %v, want %v", gotCommand, want)
	}
}

func TestPodExecRackSetter_SetBookieRack_PrependsAdminURL(t *testing.T) {
	var gotCommand []string
	setter := &PodExecRackSetter{
		AdminURL: "http://broker:8080",
		execFn: func(_ context.Context, command []string) (string, error) {
			gotCommand = command
			return "", nil
		},
	}

	if err := setter.SetBookieRack(context.Background(), "bk-0.bk.default:3181", "/us-east-1a"); err != nil {
		t.Fatalf("SetBookieRack() error = %v", err)
	}

	if len(gotCommand) < 3 || gotCommand[1] != "--admin-url" || gotCommand[2] != "http://broker:8080" {
		t.Errorf("command = %v, want --admin-url http://broker:8080 immediately after the binary", gotCommand)
	}
}

func TestPodExecRackSetter_SetBookieRack_WrapsExecErrorWithOutput(t *testing.T) {
	setter := &PodExecRackSetter{
		execFn: func(_ context.Context, _ []string) (string, error) {
			return "Reason: some CLI failure detail", errors.New("command terminated with non-zero exit code")
		},
	}

	err := setter.SetBookieRack(context.Background(), "bk-0.bk.default:3181", "/us-east-1a")
	if err == nil {
		t.Fatal("SetBookieRack() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "some CLI failure detail") {
		t.Errorf("error = %v, want it to include the CLI's own output for debuggability", err)
	}
}
