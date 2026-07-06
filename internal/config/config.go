// Package config implements the shared config-templating engine used by
// every Pulsar component reconciler (broker, bookkeeper, proxy, standalone,
// functions-worker). It merges operator defaults with user overrides into
// flat properties-file ConfigMap data, generates per-key environment
// overrides for the Pulsar image's "apply-config-from-env" entrypoint
// convention, and computes a checksum suitable for a pod-template
// annotation that triggers a rolling restart when config changes.
package config

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// EnvPrefix is the environment-variable prefix recognized by the Pulsar
// container image's apply-config-from-env entrypoint script, which applies
// each PULSAR_PREFIX_<KEY>=<value> env var as a single-key override on top
// of the rendered properties file at container start.
const EnvPrefix = "PULSAR_PREFIX_"

// Merge returns a new map containing base with overrides layered on top;
// keys present in overrides win. Neither base nor overrides is mutated.
func Merge(base, overrides map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(overrides))
	maps.Copy(merged, base)
	maps.Copy(merged, overrides)
	return merged
}

// RenderProperties renders cfg as a flat `key=value` properties file, one
// entry per line with keys sorted lexicographically for deterministic
// output. Suitable for broker.conf, bookkeeper.conf, proxy.conf, and
// standalone.conf ConfigMap data. Keys and values are escaped per
// java.util.Properties semantics so a value containing a newline, CR, or tab
// cannot split into extra property lines and a key containing `=`/`:`/space
// stays unambiguous; the output round-trips through a Properties-style parse.
func RenderProperties(cfg map[string]string) string {
	keys := sortedKeys(cfg)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(propKeyEscaper.Replace(k))
		b.WriteByte('=')
		b.WriteString(propValueEscaper.Replace(cfg[k]))
		b.WriteByte('\n')
	}
	return b.String()
}

// propValueEscaper escapes a property value per java.util.Properties store
// semantics so it round-trips: backslash must come first so escapes we
// introduce aren't re-escaped. `=` / `:` need no escaping in a value (only
// the separator between key and value is significant); leading spaces are
// left as-is (Pulsar conf values are not leading-space-sensitive), so a
// value is only altered when it contains a backslash or a control char that
// would otherwise break the single-line-per-property format.
var propValueEscaper = strings.NewReplacer(
	"\\", "\\\\",
	"\n", "\\n",
	"\r", "\\r",
	"\t", "\\t",
)

// propKeyEscaper escapes a property key: everything the value escaper does,
// plus the token separators (`=`, `:`) and whitespace, each with a leading
// backslash, so the key is unambiguous and terminates at the literal `=` we
// emit rather than at any separator character inside the key itself.
var propKeyEscaper = strings.NewReplacer(
	"\\", "\\\\",
	"\n", "\\n",
	"\r", "\\r",
	"\t", "\\t",
	" ", "\\ ",
	"=", "\\=",
	":", "\\:",
)

// PrefixedEnv converts cfg into a deterministic, sorted-by-key list of
// PULSAR_PREFIX_<key> env vars, the mechanism a per-set override (e.g. one
// StatefulSet replica needing a single differing key) uses to win over the
// shared ConfigMap without forking the whole rendered file.
func PrefixedEnv(cfg map[string]string) []corev1.EnvVar {
	keys := sortedKeys(cfg)

	envVars := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		envVars = append(envVars, corev1.EnvVar{
			Name:  EnvPrefix + k,
			Value: cfg[k],
		})
	}
	return envVars
}

// Checksum returns a stable, order-sensitive hex digest (first 16 hex
// characters of sha256) over parts, suitable as a pod-template annotation
// value: changing it forces a rolling restart when rendered config changes.
func Checksum(parts ...string) string {
	h := sha256.New()

	// Each part is length-framed rather than written raw so that adjacent
	// parts can't shift across a boundary and collide, e.g. ("ab","c") vs
	// ("a","bc") must not hash the same.
	var lenBuf [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(p)))
		h.Write(lenBuf[:])
		h.Write([]byte(p))
	}

	return hex.EncodeToString(h.Sum(nil))[:16]
}

func sortedKeys(cfg map[string]string) []string {
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
