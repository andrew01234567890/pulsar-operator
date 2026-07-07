package config

import (
	"maps"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	kyaml "sigs.k8s.io/yaml"
)

const (
	keyBrokerServicePort = "brokerServicePort"
	keyWebServicePort    = "webServicePort"
	keyClusterName       = "clusterName"
	keyMetadataStoreURL  = "metadataStoreUrl"
	keyCert              = "cert"
	keyAEqB              = "a=b"
	valClusterName       = "pulsar-cluster"
	valMultiline         = "line1\nline2"
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
			cfg:  map[string]string{keyMetadataStoreURL: "oxia://oxia-coordinator:6648/default"},
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
		{
			name: "value colon is not escaped",
			cfg:  map[string]string{keyMetadataStoreURL: "oxia://svc:6648"},
			want: "metadataStoreUrl=oxia://svc:6648\n",
		},
		{
			name: "value equals is not escaped",
			cfg:  map[string]string{"PULSAR_MEM": "-Xms2g -Dfoo=bar"},
			want: "PULSAR_MEM=-Xms2g -Dfoo=bar\n",
		},
		{
			name: "value newline is escaped",
			cfg:  map[string]string{keyCert: valMultiline},
			want: "cert=line1\\nline2\n",
		},
		{
			name: "value carriage return is escaped",
			cfg:  map[string]string{keyCert: "line1\rline2"},
			want: "cert=line1\\rline2\n",
		},
		{
			name: "value tab is escaped",
			cfg:  map[string]string{keyCert: "a\tb"},
			want: "cert=a\\tb\n",
		},
		{
			name: "value backslash is escaped",
			cfg:  map[string]string{"winPath": `C:\data`},
			want: "winPath=C:\\\\data\n",
		},
		{
			name: "key equals is escaped",
			cfg:  map[string]string{keyAEqB: "v"},
			want: "a\\=b=v\n",
		},
		{
			name: "key colon is escaped",
			cfg:  map[string]string{"a:b": "v"},
			want: "a\\:b=v\n",
		},
		{
			name: "key space is escaped",
			cfg:  map[string]string{"a b": "v"},
			want: "a\\ b=v\n",
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

func TestRenderProperties_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
	}{
		{name: "value with newline", cfg: map[string]string{"k": valMultiline}},
		{name: "value with carriage return", cfg: map[string]string{"k": "line1\rline2"}},
		{name: "value with CRLF", cfg: map[string]string{"k": "line1\r\nline2"}},
		{name: "value with tab", cfg: map[string]string{"k": "a\tb"}},
		{name: "value with equals", cfg: map[string]string{"opts": "a=b=c"}},
		{name: "value with backslash", cfg: map[string]string{"path": `a\b\c`}},
		{name: "key with equals", cfg: map[string]string{keyAEqB: "v"}},
		{name: "key with colon", cfg: map[string]string{"a:b": "v"}},
		{name: "key with space", cfg: map[string]string{"a b": "v"}},
		{name: "empty value", cfg: map[string]string{"k": ""}},
		{
			name: "mixed adversarial keys and values",
			cfg: map[string]string{
				keyAEqB: "1\n2",
				"c:d":   "e\tf",
				"g h":   `back\slash`,
				"i":     "plain",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rendered := RenderProperties(tt.cfg)

			// A control char in a value that leaked unescaped would split
			// one property across multiple lines: entry count must equal
			// the number of keys.
			if lines := nonEmptyLineCount(rendered); lines != len(tt.cfg) {
				t.Errorf("rendered %q has %d entry lines, want %d (layout corrupted by unescaped char)",
					rendered, lines, len(tt.cfg))
			}

			got := parseProperties(rendered)
			if !mapsEqual(got, tt.cfg) {
				t.Errorf("round-trip mismatch: RenderProperties(%v) => %q => parsed %v", tt.cfg, rendered, got)
			}
		})
	}
}

// TestRenderProperties_EscapeRegression pins the exact escaped output for an
// adversarial key/value. If escaping changes, rendered .conf ConfigMaps flip
// and force a cluster-wide rolling restart on upgrade, so the algorithm must
// only change deliberately.
func TestRenderProperties_EscapeRegression(t *testing.T) {
	const want = "a\\=b=line1\\nline2\n"
	got := RenderProperties(map[string]string{keyAEqB: valMultiline})
	if got != want {
		t.Errorf("RenderProperties escaping changed: got %q, want %q (confirm this is intentional)", got, want)
	}
}

func TestRenderYAML(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
		want string
	}{
		{
			name: "sorted keys",
			cfg:  map[string]string{keyWebServicePort: "8080", "workerPort": "6750"},
			want: "webServicePort: \"8080\"\nworkerPort: \"6750\"\n",
		},
		{
			name: "one key",
			cfg:  map[string]string{"workerId": "standalone"},
			want: "workerId: standalone\n",
		},
		{
			name: "empty map",
			cfg:  map[string]string{},
			want: "{}\n",
		},
		{
			name: "nil map",
			cfg:  nil,
			want: "{}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RenderYAML(tt.cfg)
			if got != tt.want {
				t.Errorf("RenderYAML(%v) = %q, want %q", tt.cfg, got, tt.want)
			}
		})
	}
}

func TestRenderYAML_Deterministic(t *testing.T) {
	cfg := map[string]string{
		"workerPort":                    "6750",
		"configurationMetadataStoreUrl": "",
		"pulsarServiceUrl":              "pulsar://broker:6650/",
	}

	first := RenderYAML(cfg)
	for i := range 20 {
		if got := RenderYAML(cfg); got != first {
			t.Fatalf("RenderYAML is not deterministic across calls: run %d = %q, want %q", i, got, first)
		}
	}
}

func TestRenderYAML_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
	}{
		{name: "value with colon", cfg: map[string]string{"k": "a: b"}},
		{name: "value with newline", cfg: map[string]string{"k": valMultiline}},
		{name: "value that looks like a bool", cfg: map[string]string{"k": "true"}},
		{name: "value that looks like a number", cfg: map[string]string{"k": "8080"}},
		{name: "empty value", cfg: map[string]string{"k": ""}},
		{name: "value with quotes", cfg: map[string]string{"k": `say "hi"`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rendered := RenderYAML(tt.cfg)

			var got map[string]string
			if err := kyaml.Unmarshal([]byte(rendered), &got); err != nil {
				t.Fatalf("RenderYAML(%v) = %q, which failed to parse back as YAML: %v", tt.cfg, rendered, err)
			}
			if !mapsEqual(got, tt.cfg) {
				t.Errorf("round-trip mismatch: RenderYAML(%v) => %q => parsed %v", tt.cfg, rendered, got)
			}
		})
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
		keyMetadataStoreURL:  "oxia://oxia-coordinator:6648/default",
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

func nonEmptyLineCount(rendered string) int {
	count := 0
	for line := range strings.SplitSeq(rendered, "\n") {
		if line != "" {
			count++
		}
	}
	return count
}

// parseProperties is an independent, java.util.Properties-style parser used
// to prove RenderProperties output round-trips. Each rendered entry is a
// single line (values never span lines because control chars are escaped);
// the first unescaped separator (`=`, `:`, space, tab) ends the key.
func parseProperties(rendered string) map[string]string {
	out := map[string]string{}
	for line := range strings.SplitSeq(rendered, "\n") {
		if line == "" {
			continue
		}
		key, val := splitPropertyLine(line)
		out[unescapeProperty(key)] = unescapeProperty(val)
	}
	return out
}

func splitPropertyLine(line string) (key, value string) {
	esc := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case esc:
			esc = false
		case c == '\\':
			esc = true
		case c == '=' || c == ':' || c == ' ' || c == '\t':
			return line[:i], line[i+1:]
		}
	}
	return line, ""
}

func unescapeProperty(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'f':
				b.WriteByte('\f')
			default:
				b.WriteByte(s[i])
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
