#!/usr/bin/env bash
#
# verify-image-refs.sh collects every apachepulsar/pulsar(-all)? and
# oxia/oxia image tag referenced by this repo's sample manifests, chainsaw
# tests, e2e-pinned workload images, and the operator's own compiled-in
# default-image constants, then verifies each one actually resolves on its
# registry (docker buildx imagetools inspect: the same tool
# test/e2e/pulsarcluster_test.go's pullSinglePlatform already depends on, so
# this reuses infrastructure e2e CI already has rather than adding a new
# dependency).
#
# This is the guard that would have caught the apachepulsar/pulsar-all:5.0.0-M1
# bug (offload's auto-derived image, fixed in #36): a referenced tag that is
# never actually published produces an ImagePullBackOff at deploy time
# instead of a clear failure here, at review time.
#
# Deliberately excludes:
#   - docs/**: prose discussing a tag (e.g. explaining why it does NOT exist)
#     is not a manifest reference.
#   - *_test.go: several tests intentionally construct nonexistent tags
#     (e.g. "apachepulsar/pulsar:bogus-tag", "...:this-tag-does-not-exist") as
#     negative-path fixtures; these must never be treated as real references.
#
# Requires network access to the images' registries, so this is NOT part of
# `make test` (the core unit/envtest suite) - see `make verify-image-refs` and
# .github/workflows/verify-image-refs.yml, a separate, non-required CI job.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# image_pattern matches a literal "repo:tag" for the two image families this
# operator ever deploys workloads from. Kept as a single source of truth so
# the YAML- and Go-scanning passes below can't drift apart.
image_pattern='(apachepulsar/pulsar(-all)?|oxia/oxia):[A-Za-z0-9._-]+'

tmp_images="$(mktemp)"
trap 'rm -f "$tmp_images"' EXIT

# 1. Explicit "image:" fields and "pulsarVersion:" fields in sample/chainsaw
#    manifests. pulsarVersion has no image family of its own - the operator
#    always derives apachepulsar/pulsar:<pulsarVersion> for it (see
#    internal/controller/cluster/pulsarcluster_controller.go's
#    defaultImageRepository/clusterDefaultImage) - so it is translated here
#    the same way.
yaml_dirs=(config/samples test/chainsaw)
for dir in "${yaml_dirs[@]}"; do
  [ -d "$dir" ] || continue

  grep -rhoE "^\s*image:\s*[\"']?${image_pattern}[\"']?" "$dir" --include='*.yaml' --include='*.yml' 2>/dev/null \
    | grep -oE "$image_pattern" >>"$tmp_images" || true

  grep -rhoE '^\s*pulsarVersion:\s*"?[A-Za-z0-9._-]+"?' "$dir" --include='*.yaml' --include='*.yml' 2>/dev/null \
    | grep -oE '[A-Za-z0-9._-]+"?$' | tr -d '"' \
    | sed 's#^#apachepulsar/pulsar:#' >>"$tmp_images" || true
done

# 2. Pinned e2e workload images (test/e2e/pulsarcluster_test.go's
#    pulsarWorkloadImage/oxiaWorkloadImage) - real images `kind load`s onto
#    the e2e cluster, so an unresolvable tag here fails Kind's pull, not just
#    a documentation claim.
if [ -d test/e2e ]; then
  grep -rhoE "\"${image_pattern}\"" test/e2e --include='*.go' 2>/dev/null \
    | grep -oE "$image_pattern" >>"$tmp_images" || true
fi

# 3. The operator's own compiled-in default-image constants (e.g.
#    defaultBrokerImage, functionsWorkerDefaultImage, defaultOxiaImage): what
#    an unmodified PulsarCluster sample actually resolves to and deploys.
#    *_test.go is excluded deliberately - see the file header.
if [ -d internal/controller ]; then
  while IFS= read -r -d '' f; do
    grep -hoE "\"${image_pattern}\"" "$f" 2>/dev/null | grep -oE "$image_pattern" >>"$tmp_images" || true
  done < <(find internal/controller -name '*.go' ! -name '*_test.go' -print0)
fi

mapfile -t images < <(sort -u "$tmp_images")

if [ "${#images[@]}" -eq 0 ]; then
  echo "verify-image-refs: no image references found - nothing to check" >&2
  exit 1
fi

echo "verify-image-refs: checking ${#images[@]} image reference(s):"
printf '  - %s\n' "${images[@]}"

failed=()
for img in "${images[@]}"; do
  if docker buildx imagetools inspect "$img" >/dev/null 2>&1; then
    echo "OK   $img"
  else
    echo "FAIL $img"
    failed+=("$img")
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo >&2
  echo "verify-image-refs: ${#failed[@]} image reference(s) do not resolve on their registry:" >&2
  printf '  - %s\n' "${failed[@]}" >&2
  exit 1
fi

echo
echo "verify-image-refs: all ${#images[@]} image reference(s) resolve"
