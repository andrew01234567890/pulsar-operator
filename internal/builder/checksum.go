package builder

import (
	"maps"

	"github.com/andrew01234567890/pulsar-operator/internal/config"
)

// ConfigChecksumAnnotation is the pod-template annotation key holding a
// digest of a component's rendered config. Setting it on a pod template
// (rather than only on the ConfigMap) forces a rolling restart when config
// changes, even though the ConfigMap's own name stays the same.
const ConfigChecksumAnnotation = "pulsaroperator.io/config-checksum"

// WithConfigChecksum returns a copy of annotations with
// ConfigChecksumAnnotation set to config.Checksum(contents...). annotations
// is not mutated; pass nil to start from an empty set.
func WithConfigChecksum(annotations map[string]string, contents ...string) map[string]string {
	out := make(map[string]string, len(annotations)+1)
	maps.Copy(out, annotations)
	out[ConfigChecksumAnnotation] = config.Checksum(contents...)
	return out
}
