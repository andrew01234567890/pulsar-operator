package builder

import (
	"maps"
	"testing"

	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

func TestWithConfigChecksum(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		contents    []string
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			contents:    []string{"broker.conf contents"},
		},
		{
			name:        "existing annotations are preserved",
			annotations: map[string]string{"other/annotation": "keep-me"},
			contents:    []string{"broker.conf contents", "PULSAR_PREFIX_x=y"},
		},
		{
			name:        "no contents",
			annotations: map[string]string{},
			contents:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := maps.Clone(tt.annotations)

			got := WithConfigChecksum(tt.annotations, tt.contents...)

			want := config.Checksum(tt.contents...)
			if got[ConfigChecksumAnnotation] != want {
				t.Errorf("WithConfigChecksum(...)[%q] = %q, want %q", ConfigChecksumAnnotation, got[ConfigChecksumAnnotation], want)
			}

			for k, v := range tt.annotations {
				if got[k] != v {
					t.Errorf("WithConfigChecksum dropped/changed existing annotation %q: got %q, want %q", k, got[k], v)
				}
			}

			if !maps.Equal(tt.annotations, before) {
				t.Errorf("WithConfigChecksum mutated its input map: got %v, want unchanged %v", tt.annotations, before)
			}
		})
	}
}

func TestWithConfigChecksumIsStableAndSensitive(t *testing.T) {
	a := WithConfigChecksum(nil, "same-contents")
	b := WithConfigChecksum(nil, "same-contents")
	if a[ConfigChecksumAnnotation] != b[ConfigChecksumAnnotation] {
		t.Errorf("checksum for identical contents is not stable: %q != %q", a[ConfigChecksumAnnotation], b[ConfigChecksumAnnotation])
	}

	c := WithConfigChecksum(nil, "different-contents")
	if a[ConfigChecksumAnnotation] == c[ConfigChecksumAnnotation] {
		t.Errorf("checksum did not change for different contents: %q", a[ConfigChecksumAnnotation])
	}
}

func TestConfigChecksumAnnotationKey(t *testing.T) {
	const want = "pulsaroperator.io/config-checksum"
	if ConfigChecksumAnnotation != want {
		t.Errorf("ConfigChecksumAnnotation = %q, want %q", ConfigChecksumAnnotation, want)
	}
}
