#!/bin/sh

# Shared parser and rendering helpers for the pinned runtime-free BuildKit
# artifact. Keep the JSON contract as the only source of versions and hashes.

vz_buildkit_die() {
  echo "buildkit artifact: $*" >&2
  exit 1
}

vz_buildkit_contract_path() {
  if [ -n "${VZ_BUILDKIT_CONTRACT:-}" ]; then
    printf '%s\n' "$VZ_BUILDKIT_CONTRACT"
  else
    printf '%s\n' "$VZ_BUILDKIT_REPO_ROOT/config/buildkit-artifact-v0.19.0.json"
  fi
}

vz_buildkit_contract_get() {
  key=$1
  contract=$(vz_buildkit_contract_path)
  [ -f "$contract" ] || vz_buildkit_die "contract not found: $contract"
  value=$(awk -v wanted="$key" '
    $0 ~ "^[[:space:]]*\\\"" wanted "\\\"[[:space:]]*:" {
      line = $0
      sub("^[[:space:]]*\\\"" wanted "\\\"[[:space:]]*:[[:space:]]*", "", line)
      sub(",[[:space:]]*$", "", line)
      if (line ~ /^\"/) {
        sub(/^\"/, "", line)
        sub(/\"[[:space:]]*$/, "", line)
      }
      print line
      found++
    }
    END { if (found != 1) exit 1 }
  ' "$contract") || vz_buildkit_die "contract key must occur exactly once: $key"
  [ -n "$value" ] || vz_buildkit_die "empty contract value: $key"
  printf '%s\n' "$value"
}

vz_buildkit_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    vz_buildkit_die "sha256sum or shasum is required"
  fi
}

vz_buildkit_json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

vz_buildkit_write_manifest() {
  destination=$1
  version=$(vz_buildkit_contract_get buildkit_version)
  layout=$(vz_buildkit_contract_get layout)
  platform=$(vz_buildkit_contract_get platform)
  commit=$(vz_buildkit_contract_get source_commit)
  buildctl_sha=$(vz_buildkit_contract_get buildctl_sha256)
  buildkitd_sha=$(vz_buildkit_contract_get buildkitd_sha256)
  printf '%s\n' \
    '{' \
    "  \"buildkit\": \"${version}\"," \
    "  \"layout\": ${layout}," \
    "  \"platform\": \"${platform}\"," \
    "  \"source_commit\": \"${commit}\"," \
    '  "binaries": {' \
    "    \"buildctl\": \"${buildctl_sha}\"," \
    "    \"buildkitd\": \"${buildkitd_sha}\"" \
    '  }' \
    '}' > "$destination"
}

vz_buildkit_prepare_fresh_dir() {
  destination=$1
  if [ -e "$destination" ]; then
    [ -d "$destination" ] || vz_buildkit_die "output path is not a directory: $destination"
    [ -z "$(find "$destination" -mindepth 1 -maxdepth 1 -print -quit)" ] || \
      vz_buildkit_die "output directory is not empty: $destination"
  else
    mkdir -p "$destination"
  fi
  chmod 0700 "$destination"
}
