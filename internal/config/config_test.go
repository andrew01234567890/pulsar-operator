package config

import (
	"maps"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

const (
	keyBrokerServicePort = "brokerServicePort"
	keyWebServicePort    = "webServicePort"
	keyClusterName       = "clusterName"
	valClusterName       = "pulsar-cluster"
)

func TestMerge(t *testing.T) {
	tests := []struct {
		name      string
		base      map[string]string
		overrides map[string]string
		want      map[string]string
	}{
		{
			name:      "overrides win on conflicting keys",
			base:      map[string]string{"a": "base-a", "b": "base-b"},
			overrides: map[string]string{"b": "override-b"},
			want:      map[string]string{"a": "base-a", "b": "override-b"},
		},
		{
			name:      "disjoint keys are unioned",
			base:      map[string]string{"a": "1"},
			overrides: map[string]string{"b": "2"},
			want:      map[string]string{"a": "1", "b": "2"},
		},
		{
			name:      "nil overrides keeps base",
			base:      map[string]string{"a": "1"},
			overrides: nil,
			want:      map[string]string{"a": "1"},
		},
		{
			name:      "nil base keeps overrides",
			base:      nil,
			overrides: map[string]string{"a": "1"},
			want:      map[string]string{"a": "1"},
		},
		{
			name:      "both nil yields empty map",
			base:      nil,
			overrides: nil,
			want:      map[string]string{},
		},
		{
			name:      "empty string override still wins",
			base:      map[string]string{"a": "1"},
			overrides: map[string]string{"a": ""},
			want:      map[string]string{"a": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Snapshot base/overrides so we can detect mutation.
			baseSnapshot := cloneMap(tt.base)
			overridesSnapshot := cloneMap(tt.overrides)

			got := Merge(tt.base, tt.overrides)

			if !mapsEqual(got, tt.want) {
				t.Errorf("Merge(%v, %v) = %v, want %v", tt.base, tt.overrides, got, tt.want)
			}
			if !mapsEqual(tt.base, baseSnapshot) {
				t.Errorf("Merge mutated base: got %v, want %v", tt.base, baseSnapshot)
			}
			if !mapsEqual(tt.overrides, overridesSnapshot) {
				t.Errorf("Merge mutated overrides: got %v, want %v", tt.overrides, overridesSnapshot)
			}
		})
	}
}

func TestMerge_ResultIndependentOfBase(t *testing.T) {
	base := map[string]string{"a": "1"}
	overrides := map[string]string{"b": "2"}

	got := Merge(base, overrides)
	got["a"] = "mutated"

	if base["a"] != "1" {
		t.Errorf("mutating Merge result affected base: base[\"a\"] = %q, want %q", base["a"], "1")
	}
}

func TestRenderProperties(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
		want string
	}{
		{
			name: "sorted regardless of insertion order",
			cfg:  map[string]string{"zookeeperServers": "zk:2181", keyBrokerServicePort: "6650", "advertisedAddress": "10.0.0.1"},
			want: "advertisedAddress=10.0.0.1\nbrokerServicePort=6650\nzookeeperServers=zk:2181\n",
		},
		{
			name: "single key",
			cfg:  map[string]string{"metadataStoreUrl": "oxia://oxia-coordinator:6648/default"},
			want: "metadataStoreUrl=oxia://oxia-coordinator:6648/default\n",
		},
		{
			name: "empty map yields empty string",
			cfg:  map[string]string{},
			want: "",
		},
		{
			name: "nil map yields empty string",
			cfg:  nil,
			want: "",
		},
		{
			name: "empty value renders trailing equals",
			cfg:  map[string]string{"managedLedgerDefaultEnsembleSize": ""},
			want: "managedLedgerDefaultEnsembleSize=\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RenderProperties(tt.cfg)
			if got != tt.want {
				t.Errorf("RenderProperties(%v) = %q, want %q", tt.cfg, got, tt.want)
			}
		})
	}
}

func TestRenderProperties_Deterministic(t *testing.T) {
	cfg := map[string]string{
		keyBrokerServicePort: "6650",
		keyWebServicePort:    "8080",
		keyClusterName:       valClusterName,
		"managedLedgerCache": "true",
		"advertisedAddress":  "broker-0.broker",
	}

	first := RenderProperties(cfg)
	for i := range 20 {
		if got := RenderProperties(cfg); got != first {
			t.Fatalf("RenderProperties is not deterministic across calls: run %d = %q, want %q", i, got, first)
		}
	}
}

func TestPrefixedEnv(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
		want []corev1.EnvVar
	}{
		{
			name: "sorted, prefixed env vars",
			cfg:  map[string]string{keyWebServicePort: "8080", keyBrokerServicePort: "6650"},
			want: []corev1.EnvVar{
				{Name: "PULSAR_PREFIX_brokerServicePort", Value: "6650"},
				{Name: "PULSAR_PREFIX_webServicePort", Value: "8080"},
			},
		},
		{
			name: "single key",
			cfg:  map[string]string{keyClusterName: valClusterName},
			want: []corev1.EnvVar{
				{Name: "PULSAR_PREFIX_clusterName", Value: valClusterName},
			},
		},
		{
			name: "empty map yields empty (non-nil) slice",
			cfg:  map[string]string{},
			want: []corev1.EnvVar{},
		},
		{
			name: "nil map yields empty slice",
			cfg:  nil,
			want: []corev1.EnvVar{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrefixedEnv(tt.cfg)
			if len(got) != len(tt.want) {
				t.Fatalf("PrefixedEnv(%v) = %v, want %v", tt.cfg, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("PrefixedEnv(%v)[%d] = %+v, want %+v", tt.cfg, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestPrefixedEnv_Deterministic(t *testing.T) {
	cfg := map[string]string{
		keyBrokerServicePort: "6650",
		keyWebServicePort:    "8080",
		keyClusterName:       valClusterName,
		"metadataStoreUrl":   "oxia://oxia-coordinator:6648/default",
	}

	first := PrefixedEnv(cfg)
	for i := range 20 {
		got := PrefixedEnv(cfg)
		if len(got) != len(first) {
			t.Fatalf("run %d: length changed: got %d, want %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("PrefixedEnv is not deterministic across calls: run %d, index %d: got %+v, want %+v", i, j, got[j], first[j])
			}
		}
	}
}

func TestChecksum(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
	}{
		{name: "no parts", parts: nil},
		{name: "single part", parts: []string{"broker.conf content"}},
		{name: "multiple parts", parts: []string{"broker.conf content", "bookkeeper.conf content"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Checksum(tt.parts...)
			if len(got) != 16 {
				t.Errorf("Checksum(%v) length = %d, want 16", tt.parts, len(got))
			}
			for _, r := range got {
				if !strings.ContainsRune("0123456789abcdef", r) {
					t.Errorf("Checksum(%v) = %q contains non-hex rune %q", tt.parts, got, r)
				}
			}
		})
	}
}

func TestChecksum_StableForSameInput(t *testing.T) {
	parts := []string{"metadataStoreUrl=oxia://oxia-coordinator:6648/default", "clusterName=pulsar-cluster"}

	first := Checksum(parts...)
	for i := range 20 {
		if got := Checksum(parts...); got != first {
			t.Fatalf("Checksum is not stable across calls: run %d = %q, want %q", i, got, first)
		}
	}
}

func TestChecksum_SensitiveToInputChanges(t *testing.T) {
	tests := []struct {
		name string
		a, b []string
	}{
		{
			name: "different value",
			a:    []string{"brokerServicePort=6650"},
			b:    []string{"brokerServicePort=6651"},
		},
		{
			name: "different order of same parts",
			a:    []string{"part-a", "part-b"},
			b:    []string{"part-b", "part-a"},
		},
		{
			name: "no boundary collision across adjacent parts",
			a:    []string{"ab", "c"},
			b:    []string{"a", "bc"},
		},
		{
			name: "empty vs no parts",
			a:    []string{""},
			b:    []string{},
		},
		{
			name: "extra empty part still changes hash",
			a:    []string{"x"},
			b:    []string{"x", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotA := Checksum(tt.a...)
			gotB := Checksum(tt.b...)
			if gotA == gotB {
				t.Errorf("Checksum(%v) == Checksum(%v) = %q, want different hashes", tt.a, tt.b, gotA)
			}
		})
	}
}

// TestChecksum_Regression pins the digest for a fixed input. If this ever
// changes, every existing pod-template annotation would flip and trigger an
// unnecessary cluster-wide rolling restart on upgrade — a change to the
// algorithm must be deliberate, not accidental.
func TestChecksum_Regression(t *testing.T) {
	const want = "6ef1918579435763"
	got := Checksum("broker.conf-content", "bookkeeper.conf-content")
	if got != want {
		t.Errorf("Checksum(%q, %q) = %q, want %q (algorithm changed - confirm this is intentional)",
			"broker.conf-content", "bookkeeper.conf-content", got, want)
	}
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	maps.Copy(c, m)
	return c
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
