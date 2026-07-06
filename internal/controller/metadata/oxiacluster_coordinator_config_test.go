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

package metadata

import (
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	metadatav1alpha1 "github.com/andrew01234567890/pulsar-operator/api/metadata/v1alpha1"
	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

func TestRenderServersCoversEveryDesiredOrdinal(t *testing.T) {
	tests := []struct {
		name     string
		replicas int32
	}{
		{name: "default replicas", replicas: defaultServerReplicas},
		{name: "single replica", replicas: 1},
		{name: "five replicas", replicas: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oxia := newTestOxiaCluster(withServerReplicas(tt.replicas))

			servers := renderServers(oxia)
			if len(servers) != int(tt.replicas) {
				t.Fatalf("renderServers() returned %d servers, want %d", len(servers), tt.replicas)
			}

			for i, s := range servers {
				wantPod := "myoxia-oxia-server-" + strconv.Itoa(i)
				if !strings.HasPrefix(s.Internal, wantPod+".myoxia-oxia-svc:") {
					t.Errorf("servers[%d].Internal = %q, want prefix %q", i, s.Internal, wantPod+".myoxia-oxia-svc:")
				}
				if !strings.HasPrefix(s.Public, wantPod+".myoxia-oxia-svc.pulsar-ns.svc.cluster.local:") {
					t.Errorf("servers[%d].Public = %q, want prefix %q", i, s.Public, wantPod+".myoxia-oxia-svc.pulsar-ns.svc.cluster.local:")
				}
				if !strings.HasSuffix(s.Internal, ":6649") {
					t.Errorf("servers[%d].Internal = %q, want suffix %q", i, s.Internal, ":6649")
				}
				if !strings.HasSuffix(s.Public, ":6648") {
					t.Errorf("servers[%d].Public = %q, want suffix %q", i, s.Public, ":6648")
				}
			}
		})
	}
}

func TestRenderNamespacesAppliesDefaultsAndPassesThroughOverrides(t *testing.T) {
	oxia := newTestOxiaCluster(withNamespaces(
		metadatav1alpha1.OxiaNamespaceSpec{Name: testNamespaceDefault},
		metadatav1alpha1.OxiaNamespaceSpec{Name: testNamespaceBroker, InitialShardCount: ptr(int32(8)), ReplicationFactor: ptr(int32(5))},
	))

	got := renderNamespaces(oxia)
	want := []coordinatorNamespace{
		{Name: testNamespaceDefault, InitialShardCount: defaultInitialShardCount, ReplicationFactor: defaultReplicationFactor},
		{Name: testNamespaceBroker, InitialShardCount: 8, ReplicationFactor: 5},
	}

	if len(got) != len(want) {
		t.Fatalf("renderNamespaces() = %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("renderNamespaces()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRenderNamespacesEmptyWhenUnset(t *testing.T) {
	oxia := newTestOxiaCluster()
	if got := renderNamespaces(oxia); len(got) != 0 {
		t.Errorf("renderNamespaces() = %+v, want empty", got)
	}
}

func TestRenderAllowExtraAuthoritiesGatedByFlag(t *testing.T) {
	tests := []struct {
		name    string
		mutator func(*metadatav1alpha1.OxiaCluster)
		want    int
	}{
		{name: "unset", mutator: func(*metadatav1alpha1.OxiaCluster) {}, want: 0},
		{name: "false", mutator: withAllowExtraAuthorities(false), want: 0},
		{name: "true", mutator: withAllowExtraAuthorities(true), want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oxia := newTestOxiaCluster(tt.mutator)
			got := renderAllowExtraAuthorities(oxia)
			if len(got) != tt.want {
				t.Errorf("renderAllowExtraAuthorities() = %v, want %d entries", got, tt.want)
			}
		})
	}
}

func TestRenderCoordinatorConfigIsValidYAML(t *testing.T) {
	oxia := newTestOxiaCluster(
		withServerReplicas(3),
		withNamespaces(metadatav1alpha1.OxiaNamespaceSpec{Name: testNamespaceDefault}),
	)

	content, err := renderCoordinatorConfig(oxia)
	if err != nil {
		t.Fatalf("renderCoordinatorConfig() error = %v", err)
	}

	var parsed coordinatorConfig
	if err := yaml.Unmarshal([]byte(content), &parsed); err != nil {
		t.Fatalf("renderCoordinatorConfig() produced invalid YAML: %v\ncontent:\n%s", err, content)
	}
	if len(parsed.Servers) != 3 {
		t.Errorf("parsed %d servers, want 3", len(parsed.Servers))
	}
	if len(parsed.Namespaces) != 1 || parsed.Namespaces[0].Name != testNamespaceDefault {
		t.Errorf("parsed namespaces = %+v, want [{default ...}]", parsed.Namespaces)
	}
}

func TestRenderCoordinatorConfigIsDeterministic(t *testing.T) {
	oxia := newTestOxiaCluster(withServerReplicas(3), withNamespaces(metadatav1alpha1.OxiaNamespaceSpec{Name: testNamespaceDefault}))

	a, err := renderCoordinatorConfig(oxia)
	if err != nil {
		t.Fatalf("renderCoordinatorConfig() error = %v", err)
	}
	b, err := renderCoordinatorConfig(oxia)
	if err != nil {
		t.Fatalf("renderCoordinatorConfig() error = %v", err)
	}
	if a != b {
		t.Errorf("renderCoordinatorConfig() is not deterministic:\na=%q\nb=%q", a, b)
	}
}

// TestRenderCoordinatorConfigChangesWithServerReplicas is the regression
// proof for the CRITICAL requirement: a server replica-count change must
// regenerate the coordinator's static servers list (and, via
// builder.WithConfigChecksum downstream, roll the coordinator). Reverting
// renderServers to ignore oxia.Spec.Server.Replicas would make this fail.
func TestRenderCoordinatorConfigChangesWithServerReplicas(t *testing.T) {
	three := newTestOxiaCluster(withServerReplicas(3))
	five := newTestOxiaCluster(withServerReplicas(5))

	contentThree, err := renderCoordinatorConfig(three)
	if err != nil {
		t.Fatalf("renderCoordinatorConfig() error = %v", err)
	}
	contentFive, err := renderCoordinatorConfig(five)
	if err != nil {
		t.Fatalf("renderCoordinatorConfig() error = %v", err)
	}

	if contentThree == contentFive {
		t.Fatalf("renderCoordinatorConfig() did not change when server replicas changed from 3 to 5")
	}

	checksumThree := config.Checksum(contentThree)
	checksumFive := config.Checksum(contentFive)
	if checksumThree == checksumFive {
		t.Errorf("config.Checksum() did not change when the rendered coordinator config changed")
	}
}
