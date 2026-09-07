#!/bin/bash
# vz 0.4 release-candidate builder.
#
# Builds the complete, locally test-signed release directory that the 0.4
# release gate consumes through `--release-dir`, and writes release-manifest.json
# (schemas/vz-0.4-release-manifest.schema.json). Normative source:
# planning/developer-environments/GOAL-0.4.0.md "Release candidate and entry
# point"; implementation plan: RELEASE-GATE-PLAN.md "Release candidate builder".
#
# Usage:
#   scripts/build-vz-0.4-release-candidate.sh --output <new dir> --version <x.y.z[-pre]>
#       [--reuse-guest-bundles <dir>] [--dev-unclean-checkout]
#
# Output layout (all files read-only after the manifest is written):
#   bin/{vz,vz-runtimed,vz-macos-setup,vz-guest-agent,vz-agent-loader}
#   linux/developer/, linux/container/   exact guest bundle file sets
#   buildkit/                            runtime-free BuildKit archive + builder evidence
#   machine-target-catalog.json          written by bin/vz-runtimed
#   entitlements.plist                   copy of entitlements/vz-cli.entitlements.plist
#   codesign/                            per-binary sign/verify logs and `codesign -d` metadata
#   build-evidence/                      cargo JSON artifact log, build logs, source-tree reports,
#                                        BuildKit validation output
#   release-manifest.json, release-manifest.sha256, checksums.sha256
#
# Component decision (verified against crates/, linux/Makefile, release.yml, install.sh):
#   * bin/ holds the five darwin-arm64 host executables that release.yml signs and
#     install.sh installs: `vz` and `vz-macos-setup` (package vz-cli), `vz-runtimed`
#     (package vz-runtimed, Cargo features must be empty), `vz-guest-agent` and
#     `vz-agent-loader` (their own packages). The macOS builds of the two agent
#     binaries serve native macOS Machines and are ad-hoc signed without
#     entitlements, exactly as release.yml does.
#   * The Linux guest agent is a cross target that linux/Makefile builds into each
#     profile's initramfs (`GUEST_AGENT_BINARY` -> initramfs /usr/bin/vz-guest-agent).
#     It is therefore covered by the guest bundle digests, not shipped in bin/.
#   * machine-target-catalog.json embeds absolute bundle_dir paths for this
#     prefix, so it is recorded under `machine_target_catalog` and excluded from
#     the normalized/signed content digests, which must be comparable with the
#     release workflow's distribution.
#
# Signing: vz, vz-runtimed and vz-macos-setup are signed with `--options runtime`
# and the virtualization entitlement (release.yml:93-131 with an ad-hoc identity);
# the agent binaries are ad-hoc signed without entitlements (release.yml:132-137).
# Every binary passes `codesign --verify --strict --verbose=4`. `unsigned_sha256`
# is recorded before signing so the GA distribution can be compared by content.
# GA parity prerequisite: release.yml must also record pre-signing digests.
#
# DEVELOPMENT-ONLY flags (never valid for certification evidence):
#   --dev-unclean-checkout    admit tracked changes/untracked files; the manifest
#                             records source.clean=false and signing_class
#                             local-test-signed so a certification-grade validator
#                             rejects it. crates/Cargo.lock must still be unchanged.
#   --reuse-guest-bundles DIR admit pre-built bundles from DIR/{developer,container}
#                             after verifying every version.json digest; the
#                             manifest records guest_bundles.source=reused and the
#                             directory content digest. The release-grade default
#                             rebuilds both bundles through the Docker builder
#                             (requires LINUX_DOCKER_CONTEXT).
set -euo pipefail

usage() {
    sed -n '2,/^set -euo pipefail/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'
}

err() { printf 'error: %s\n' "$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
# shellcheck source=scripts/lib/guest-bundles.sh
source "$REPO_ROOT/scripts/lib/guest-bundles.sh"

OUTPUT=""
VERSION=""
REUSE_GUEST_BUNDLES=""
DEV_UNCLEAN=false
ORIGINAL_ARGS=("$@")
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output) [[ $# -ge 2 ]] || err "--output requires a value"; OUTPUT="$2"; shift 2 ;;
        --version) [[ $# -ge 2 ]] || err "--version requires a value"; VERSION="$2"; shift 2 ;;
        --reuse-guest-bundles) [[ $# -ge 2 ]] || err "--reuse-guest-bundles requires a value"; REUSE_GUEST_BUNDLES="$2"; shift 2 ;;
        --dev-unclean-checkout) DEV_UNCLEAN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; err "unknown argument: $1" ;;
    esac
done
[[ -n "$OUTPUT" && -n "$VERSION" ]] || { usage >&2; err "--output and --version are required"; }
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]] || err "explicit release version x.y.z[-pre] required, got: $VERSION"

# ---------------------------------------------------------------------------
# 1. Preflight
# ---------------------------------------------------------------------------
[[ "$(uname -s)" == Darwin && "$(uname -m)" == arm64 ]] || err "Apple-silicon macOS required"
for dependency in codesign jq shasum cargo uv git make bsdtar curl perl; do
    command -v "$dependency" >/dev/null || err "missing dependency: $dependency"
done
[[ -x /usr/bin/python3 ]] || err "/usr/bin/python3 required"
if [[ -z "$REUSE_GUEST_BUNDLES" && -z "${LINUX_DOCKER_CONTEXT:-}" ]]; then
    err "set LINUX_DOCKER_CONTEXT to an explicit local Unix-socket Docker build context (or pass --reuse-guest-bundles for a DEV build)"
fi
[[ ! -e "$OUTPUT" ]] || err "output directory must not exist: $OUTPUT"
output_parent="$(cd "$(dirname "$OUTPUT")" 2>/dev/null && pwd -P)" || err "parent of --output must exist"
OUTPUT="$output_parent/$(basename "$OUTPUT")"
if [[ -n "$REUSE_GUEST_BUNDLES" ]]; then
    [[ -d "$REUSE_GUEST_BUNDLES" ]] || err "--reuse-guest-bundles must name a directory"
    REUSE_GUEST_BUNDLES="$(cd "$REUSE_GUEST_BUNDLES" && pwd -P)"
fi

SOURCE_TREE_HELPER="$REPO_ROOT/scripts/helpers/vz04_source_tree.py"
ENTITLEMENTS="$REPO_ROOT/entitlements/vz-cli.entitlements.plist"
[[ -f "$SOURCE_TREE_HELPER" ]] || err "missing $SOURCE_TREE_HELPER"
[[ -f "$ENTITLEMENTS" ]] || err "missing $ENTITLEMENTS"

DEV_FLAGS=()
[[ "$DEV_UNCLEAN" == true ]] && DEV_FLAGS+=(dev-unclean-checkout)
[[ -n "$REUSE_GUEST_BUNDLES" ]] && DEV_FLAGS+=(reuse-guest-bundles)

umask 022
mkdir -m 0700 "$OUTPUT"
EVIDENCE="$OUTPUT/build-evidence"
mkdir -m 0755 "$EVIDENCE" "$OUTPUT/bin" "$OUTPUT/linux" "$OUTPUT/codesign"
FRAGMENTS="$EVIDENCE/.fragments"
mkdir -m 0700 "$FRAGMENTS"
BUILT_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "==> vz 0.4 release candidate $VERSION -> $OUTPUT"

sha256_of() { shasum -a 256 "$1" | cut -d' ' -f1; }
canonical_sha256() { /usr/bin/python3 "$SOURCE_TREE_HELPER" canonical-sha256; }

# ---------------------------------------------------------------------------
# 2. Fresh-checkout assertion and canonical source-tree digest
# ---------------------------------------------------------------------------
echo "==> describing source tree"
source_tree_args=(describe --repo "$REPO_ROOT")
[[ "$DEV_UNCLEAN" == true ]] && source_tree_args+=(--dev-unclean-checkout)
/usr/bin/python3 "$SOURCE_TREE_HELPER" "${source_tree_args[@]}" --json-out "$EVIDENCE/source-tree.pre-build.json"
SOURCE_TREE_SHA256="$(jq -r '.tree_sha256' "$EVIDENCE/source-tree.pre-build.json")"
SOURCE_COMMIT="$(jq -r '.commit' "$EVIDENCE/source-tree.pre-build.json")"
echo "source commit $SOURCE_COMMIT tree_sha256 $SOURCE_TREE_SHA256"

# ---------------------------------------------------------------------------
# 3. Guest bundles (rebuild by default; DEV reuse after digest verification)
# ---------------------------------------------------------------------------
GUEST_SOURCE_MODE=rebuilt
REUSED_DIR_SHA256=null
if [[ -n "$REUSE_GUEST_BUNDLES" ]]; then
    echo "==> DEVELOPMENT ONLY: reusing pre-built guest bundles from $REUSE_GUEST_BUNDLES"
    GUEST_SOURCE_MODE=reused
    REUSED_DIR_SHA256="\"$(vz04_dir_content_sha256 "$REUSE_GUEST_BUNDLES" "$SOURCE_TREE_HELPER")\""
    bundle_source() { printf '%s/%s\n' "$REUSE_GUEST_BUNDLES" "$1"; }
else
    vz04_rebuild_guest_bundles "$REPO_ROOT" "$EVIDENCE"
    bundle_source() { vz04_guest_bundle_source_dir "$REPO_ROOT" "$1"; }
fi
for profile in "${VZ04_GUEST_PROFILES[@]}"; do
    echo "==> staging $profile guest bundle"
    vz04_stage_guest_bundle "$(bundle_source "$profile")" "$profile" "$OUTPUT/linux/$profile"
done

# ---------------------------------------------------------------------------
# 4. Host binaries: one --locked --release JSON build, exact artifact resolution
# ---------------------------------------------------------------------------
echo "==> building host binaries (cargo --locked --release)"
# Bypass optional user-local compiler caches; do not change global settings.
export RUSTC_WRAPPER=
export CC_aarch64_apple_darwin=/usr/bin/clang
export CXX_aarch64_apple_darwin=/usr/bin/clang++
HOST_PACKAGES=(vz-cli vz-runtimed vz-guest-agent vz-agent-loader)
HOST_BINARIES=(vz vz-macos-setup vz-runtimed vz-guest-agent vz-agent-loader)
cargo_package_args=()
for package in "${HOST_PACKAGES[@]}"; do cargo_package_args+=(-p "$package"); done
cargo build --manifest-path "$REPO_ROOT/crates/Cargo.toml" --locked --release \
    "${cargo_package_args[@]}" --bins --message-format=json \
    2> >(tee "$EVIDENCE/cargo-build.log" >&2) | tee "$EVIDENCE/cargo-build.jsonl" >/dev/null

# Copied from scripts/run-sandbox-vm-e2e.sh:743-801: Cargo is the source of truth
# for artifact locations and feature inventories.
resolve_cargo_executable() {
    local artifact_log="$1" target_name="$2" target_kind="$3"
    jq -ers --arg target_name "$target_name" --arg target_kind "$target_kind" '
        [ .[] | select(.reason == "compiler-artifact" and .target.name == $target_name
                       and (.target.kind | index($target_kind)) != null and .executable != null)
          | .executable ] | unique
        | if length == 1 then .[0]
          elif length == 0 then error("Cargo did not report an executable for \($target_kind) target \($target_name)")
          else error("Cargo reported multiple executables for \($target_kind) target \($target_name): \(.)") end
    ' "$artifact_log"
}
resolve_cargo_features() {
    local artifact_log="$1" target_name="$2" target_kind="$3"
    jq -cers --arg target_name "$target_name" --arg target_kind "$target_kind" '
        [ .[] | select(.reason == "compiler-artifact" and .target.name == $target_name
                       and (.target.kind | index($target_kind)) != null and .executable != null)
          | .features ] | unique
        | if length == 1 then .[0]
          elif length == 0 then error("Cargo did not report features for \($target_kind) target \($target_name)")
          else error("Cargo reported multiple feature sets for \($target_kind) target \($target_name): \(.)") end
    ' "$artifact_log"
}
resolve_cargo_package() {
    jq -ers --arg target_name "$2" '
        [ .[] | select(.reason == "compiler-artifact" and .target.name == $target_name and .executable != null)
          | .package_id ] | unique
        | if length == 1 then .[0] else error("ambiguous package for \($target_name)") end
    ' "$1"
}

for binary in "${HOST_BINARIES[@]}"; do
    built="$(resolve_cargo_executable "$EVIDENCE/cargo-build.jsonl" "$binary" bin)" \
        || err "unable to resolve the $binary executable from the Cargo artifact log"
    [[ "$built" == /* && -f "$built" && ! -L "$built" ]] || err "invalid build artifact: $binary"
    features="$(resolve_cargo_features "$EVIDENCE/cargo-build.jsonl" "$binary" bin)"
    if [[ "$binary" == vz-runtimed && "$features" != "[]" ]]; then
        err "installed vz-runtimed must be built with no Cargo features, got $features"
    fi
    package_id="$(resolve_cargo_package "$EVIDENCE/cargo-build.jsonl" "$binary")"
    cp "$built" "$OUTPUT/bin/$binary"
    chmod 0755 "$OUTPUT/bin/$binary"
    jq -n --arg target "$binary" --argjson features "$features" --arg package_id "$package_id" \
        --arg unsigned "$(sha256_of "$OUTPUT/bin/$binary")" \
        '{unsigned_sha256:$unsigned, cargo:{package_id:$package_id, target:$target, features:$features, profile:"release", locked:true}}' \
        > "$FRAGMENTS/bin.$binary.json"
done

# ---------------------------------------------------------------------------
# 5. Sign (release.yml set) and capture codesign metadata
# ---------------------------------------------------------------------------
echo "==> signing host binaries"
codesign_metadata() {
    # Parse `codesign -d --entitlements :- --verbose=4` output written to
    # <stem>.codesign-d.txt (stderr) and <stem>.entitlements.plist (stdout).
    local stem="$1" meta="$1.codesign-d.txt" ents="$1.entitlements.plist"
    local identifier cdhash flags team runtime_version signature ents_sha
    identifier="$(sed -n 's/^Identifier=//p' "$meta" | head -1)"
    cdhash="$(sed -n 's/^CDHash=//p' "$meta" | head -1)"
    flags="$(sed -n 's/^CodeDirectory .*flags=\(0x[0-9a-f]*([^)]*)\).*/\1/p' "$meta" | head -1)"
    team="$(sed -n 's/^TeamIdentifier=//p' "$meta" | head -1)"
    signature="$(sed -n 's/^Signature=//p' "$meta" | head -1)"
    runtime_version="$(sed -n 's/^Runtime Version=//p' "$meta" | head -1)"
    [[ -n "$identifier" && -n "$cdhash" && -n "$flags" ]] || err "could not parse codesign metadata: $meta"
    if [[ -s "$ents" ]]; then ents_sha="$(sha256_of "$ents")"; else ents_sha=""; fi
    jq -n --arg identifier "$identifier" --arg cdhash "$cdhash" --arg flags "$flags" --arg team "$team" \
        --arg signature "$signature" --arg runtime_version "$runtime_version" --arg ents_sha "$ents_sha" \
        --arg stem "$(basename "$stem")" \
        '{identifier:$identifier, cdhash:$cdhash, flags:$flags,
          team_id:(if $team == "" or $team == "not set" then null else $team end),
          signature:(if $signature == "" then "unknown" else $signature end),
          runtime_version:(if $runtime_version == "" then null else $runtime_version end),
          entitlements_sha256:(if $ents_sha == "" then null else $ents_sha end),
          hardened_runtime:($flags | test("runtime")),
          metadata_file:("codesign/" + $stem + ".codesign-d.txt"),
          entitlements_file:(if $ents_sha == "" then null else ("codesign/" + $stem + ".entitlements.plist") end)}'
}
for binary in "${HOST_BINARIES[@]}"; do
    target="$OUTPUT/bin/$binary"
    stem="$OUTPUT/codesign/$binary"
    case "$binary" in
        vz|vz-runtimed|vz-macos-setup)
            sign_args=(--force --sign - --options runtime --entitlements "$ENTITLEMENTS")
            signing_kind=ad-hoc-hardened-runtime-entitled ;;
        *)
            sign_args=(--force --sign -)
            signing_kind=ad-hoc ;;
    esac
    codesign "${sign_args[@]}" "$target" > "$stem.sign.log" 2>&1 || { cat "$stem.sign.log" >&2; err "codesign failed: $binary"; }
    codesign --verify --strict --verbose=4 "$target" > "$stem.verify.log" 2>&1 || { cat "$stem.verify.log" >&2; err "codesign --verify --strict failed: $binary"; }
    codesign -d --entitlements :- --verbose=4 "$target" > "$stem.entitlements.plist" 2> "$stem.codesign-d.txt" \
        || err "codesign -d failed: $binary"
    if [[ ! -s "$stem.entitlements.plist" ]]; then rm -f "$stem.entitlements.plist"; fi
    if [[ "$signing_kind" == ad-hoc-hardened-runtime-entitled ]]; then
        [[ -s "$stem.entitlements.plist" ]] || err "signed $binary carries no entitlements"
        grep -q 'com.apple.security.virtualization' "$stem.entitlements.plist" || err "$binary lacks the virtualization entitlement"
        grep -q 'flags=0x[0-9a-f]*([^)]*runtime' "$stem.codesign-d.txt" || err "$binary was not signed with --options runtime"
    fi
    codesign_metadata "$stem" | jq --arg kind "$signing_kind" --arg signed "$(sha256_of "$target")" \
        '{signed_sha256:$signed, codesign:(. + {signing:$kind})}' > "$FRAGMENTS/sign.$binary.json"
done

# ---------------------------------------------------------------------------
# 6. Runtime-free BuildKit archive via the pinned builder and its validator
# ---------------------------------------------------------------------------
echo "==> building runtime-free BuildKit archive"
"$REPO_ROOT/scripts/build-runtime-free-buildkit.sh" --output-dir "$OUTPUT/buildkit"
BUILDKIT_ARCHIVE_BASENAME="$(jq -er '.archive_name' "$REPO_ROOT/config/buildkit-artifact-v0.19.0.json")"
BUILDKIT_ARCHIVE="$OUTPUT/buildkit/$BUILDKIT_ARCHIVE_BASENAME"
[[ -f "$BUILDKIT_ARCHIVE" && ! -L "$BUILDKIT_ARCHIVE" ]] || err "BuildKit builder did not produce $BUILDKIT_ARCHIVE_BASENAME"
IFS=' ' read -r BUILDKIT_SHA256 sidecar_name sidecar_extra < "$BUILDKIT_ARCHIVE.sha256" || err "BuildKit checksum sidecar unreadable"
[[ "$(wc -l < "$BUILDKIT_ARCHIVE.sha256" | tr -d '[:space:]')" == 1 && -z "$sidecar_extra" && "$sidecar_name" == "$BUILDKIT_ARCHIVE_BASENAME" ]] \
    || err "BuildKit checksum sidecar must contain only the digest and exact archive basename"
[[ "$BUILDKIT_SHA256" =~ ^[0-9a-f]{64}$ ]] || err "BuildKit sidecar digest is not lowercase SHA-256"
[[ "$(sha256_of "$BUILDKIT_ARCHIVE")" == "$BUILDKIT_SHA256" ]] || err "BuildKit archive does not match its sidecar"
"$REPO_ROOT/scripts/validate-runtime-free-buildkit.sh" \
    --archive "$BUILDKIT_ARCHIVE" --expected-sha256 "$BUILDKIT_SHA256" \
    --output-dir "$EVIDENCE/buildkit-validation" --source-mode candidate-build
[[ "$(jq -r '.verdict' "$EVIDENCE/buildkit-validation/buildkit-artifact-verification.json")" == passed ]] \
    || err "BuildKit validator did not pass"

# ---------------------------------------------------------------------------
# 7. Machine target catalog written by the candidate's own daemon
# ---------------------------------------------------------------------------
echo "==> writing machine-target-catalog.json"
catalog_prefix="$(cd "$OUTPUT" && pwd -P)"
catalog_args=(--write-installed-machine-target-catalog "$catalog_prefix" --installed-release-version "$VERSION")
for profile in "${VZ04_GUEST_PROFILES[@]}"; do catalog_args+=(--installed-linux-profile "$profile"); done
"$OUTPUT/bin/vz-runtimed" "${catalog_args[@]}" > "$EVIDENCE/write-installed-catalog.log" 2>&1 \
    || { cat "$EVIDENCE/write-installed-catalog.log" >&2; err "vz-runtimed could not write the installed catalog"; }
[[ -f "$OUTPUT/machine-target-catalog.json" ]] || err "machine-target-catalog.json not written"
[[ "$(jq '.linux | length' "$OUTPUT/machine-target-catalog.json")" == 2 ]] || err "exact two-profile installed catalog required"
# The advisory writer lock is transient installer state, not a release component.
rm -f "$OUTPUT/machine-target-catalog.lock"

cp "$ENTITLEMENTS" "$OUTPUT/entitlements.plist"
chmod 0444 "$OUTPUT/entitlements.plist"

# ---------------------------------------------------------------------------
# 8. Post-build source stability (generated artifacts must not touch tracked files)
# ---------------------------------------------------------------------------
echo "==> re-describing source tree after build"
/usr/bin/python3 "$SOURCE_TREE_HELPER" "${source_tree_args[@]}" --json-out "$EVIDENCE/source-tree.post-build.json"
POST_TREE_SHA256="$(jq -r '.tree_sha256' "$EVIDENCE/source-tree.post-build.json")"
SOURCE_STABLE=true
if [[ "$POST_TREE_SHA256" != "$SOURCE_TREE_SHA256" ]]; then
    SOURCE_STABLE=false
    if [[ "$DEV_UNCLEAN" == true ]]; then
        echo "warning: tracked source changed during the build (DEV build recorded as unstable)" >&2
    else
        err "tracked source changed during the build: $SOURCE_TREE_SHA256 -> $POST_TREE_SHA256"
    fi
fi

# ---------------------------------------------------------------------------
# 9. release-manifest.json
# ---------------------------------------------------------------------------
echo "==> writing release-manifest.json"
component() { # path kind sha256
    # Non-Mach-O components are byte-identical before and after signing.
    jq -n --arg path "$1" --arg kind "$2" --arg sha "$3" \
        '{($path): {kind:$kind, unsigned_sha256:$sha, signed_sha256:$sha, cargo:null, codesign:null}}'
}
{
    for binary in "${HOST_BINARIES[@]}"; do
        jq -s --arg path "bin/$binary" '{($path): ({kind:"host-binary"} + .[0] + .[1])}' \
            "$FRAGMENTS/bin.$binary.json" "$FRAGMENTS/sign.$binary.json"
    done
    for guest_profile in "${VZ04_GUEST_PROFILES[@]}"; do
        while IFS= read -r name; do
            case "$name" in
                vmlinux) kind=guest-kernel ;;
                initramfs.img) kind=guest-initramfs ;;
                youki) kind=guest-oci-runtime ;;
                version.json) kind=guest-version ;;
                developer-probe-rootfs.tar) kind=guest-probe-archive ;;
            esac
            component "linux/$guest_profile/$name" "$kind" "$(sha256_of "$OUTPUT/linux/$guest_profile/$name")"
        done < <(vz04_guest_bundle_files "$guest_profile")
    done
    component "buildkit/$BUILDKIT_ARCHIVE_BASENAME" buildkit-archive "$BUILDKIT_SHA256"
    component entitlements.plist entitlements "$(sha256_of "$OUTPUT/entitlements.plist")"
} | jq -s 'add' > "$FRAGMENTS/components.json"

NORMALIZED_CONTENT_SHA256="$(jq -c 'to_entries | map([.key, .value.unsigned_sha256]) | sort' "$FRAGMENTS/components.json" | canonical_sha256)"
SIGNED_CONTENT_SHA256="$(jq -c 'to_entries | map([.key, .value.signed_sha256]) | sort' "$FRAGMENTS/components.json" | canonical_sha256)"

guest_profile_fragment() {
    local profile="$1"
    local dir="$OUTPUT/linux/$profile"
    jq -n --arg dir "linux/$profile" --arg vsha "$(sha256_of "$dir/version.json")" \
        --slurpfile version "$dir/version.json" \
        '{dir:$dir, version_json_sha256:$vsha, version:$version[0]}'
}
jq -n --arg mode "$GUEST_SOURCE_MODE" --arg context "${LINUX_DOCKER_CONTEXT:-}" \
    --arg reused_from "${REUSE_GUEST_BUNDLES:-}" --argjson reused_sha "$REUSED_DIR_SHA256" \
    --argjson developer "$(guest_profile_fragment developer)" --argjson container "$(guest_profile_fragment container)" \
    '{source:$mode,
      linux_docker_context:(if $mode == "rebuilt" then $context else null end),
      reused_from:(if $mode == "reused" then {path:$reused_from, content_sha256:$reused_sha} else null end),
      build_logs:(if $mode == "rebuilt" then ["build-evidence/linux-developer-build.log","build-evidence/linux-container-build.log"] else [] end),
      profiles:{developer:$developer, container:$container}}' > "$FRAGMENTS/guest_bundles.json"

jq -n --arg archive "buildkit/$BUILDKIT_ARCHIVE_BASENAME" --arg sha "$BUILDKIT_SHA256" \
    --arg contract "config/buildkit-artifact-v0.19.0.json" \
    --arg contract_sha "$(sha256_of "$REPO_ROOT/config/buildkit-artifact-v0.19.0.json")" \
    --arg provenance_sha "$(sha256_of "$OUTPUT/buildkit/buildkit-artifact-provenance.json")" \
    --arg verification_sha "$(sha256_of "$EVIDENCE/buildkit-validation/buildkit-artifact-verification.json")" \
    '{archive:$archive, sha256:$sha, source_mode:"candidate-build", contract:$contract, contract_sha256:$contract_sha,
      provenance:"buildkit/buildkit-artifact-provenance.json", provenance_sha256:$provenance_sha,
      verification:"build-evidence/buildkit-validation/buildkit-artifact-verification.json", verification_sha256:$verification_sha}' \
    > "$FRAGMENTS/buildkit.json"

jq -n --arg rustc "$(rustc --version)" --arg cargo "$(cargo --version)" \
    --arg host "$(rustc -vV | sed -n 's/^host: //p')" --arg git "$(git --version)" \
    --arg product "$(sw_vers -productVersion)" --arg build "$(sw_vers -buildVersion)" \
    --arg jq "$(jq --version)" --arg python "$(/usr/bin/python3 --version 2>&1)" \
    '{rustc:$rustc, cargo:$cargo, host_triple:$host, git:$git, macos_product_version:$product, macos_build_version:$build, jq:$jq, python:$python}' \
    > "$FRAGMENTS/toolchain.json"

dev_flags_json="$(printf '%s\n' "${DEV_FLAGS[@]+"${DEV_FLAGS[@]}"}" | jq -R . | jq -sc 'map(select(length > 0))')"
jq -n --arg schema "../schemas/vz-0.4-release-manifest.schema.json" \
    --arg version "$VERSION" --arg built "$BUILT_AT" \
    --slurpfile source "$EVIDENCE/source-tree.pre-build.json" --arg post_tree "$POST_TREE_SHA256" --argjson stable "$SOURCE_STABLE" \
    --slurpfile toolchain "$FRAGMENTS/toolchain.json" --slurpfile components "$FRAGMENTS/components.json" \
    --slurpfile guest "$FRAGMENTS/guest_bundles.json" --slurpfile buildkit "$FRAGMENTS/buildkit.json" \
    --arg catalog_sha "$(sha256_of "$OUTPUT/machine-target-catalog.json")" --arg prefix "$catalog_prefix" \
    --arg normalized "$NORMALIZED_CONTENT_SHA256" --arg signed "$SIGNED_CONTENT_SHA256" \
    --arg builder_sha "$(sha256_of "${BASH_SOURCE[0]}")" --arg lib_sha "$(sha256_of "$REPO_ROOT/scripts/lib/guest-bundles.sh")" \
    --arg helper_sha "$(sha256_of "$SOURCE_TREE_HELPER")" \
    --argjson args "$(printf '%s\n' "${ORIGINAL_ARGS[@]}" | jq -R . | jq -sc .)" --argjson dev_flags "$dev_flags_json" \
    '{
      "$schema": $schema,
      schema_version: 1,
      kind: "vz-0.4-release-candidate",
      signing_class: "local-test-signed",
      release_version: $version,
      built_at_utc: $built,
      source: ($source[0] + {post_build_tree_sha256:$post_tree, stable_during_build:$stable}),
      toolchain: $toolchain[0],
      components: $components[0],
      guest_bundles: $guest[0],
      buildkit: $buildkit[0],
      machine_target_catalog: {path:"machine-target-catalog.json", sha256:$catalog_sha, prefix:$prefix,
                               writer:"bin/vz-runtimed --write-installed-machine-target-catalog"},
      normalized_content_sha256: $normalized,
      signed_content_sha256: $signed,
      builder: {script:"scripts/build-vz-0.4-release-candidate.sh", script_sha256:$builder_sha,
                guest_bundles_lib_sha256:$lib_sha, source_tree_helper_sha256:$helper_sha,
                arguments:$args, dev_flags:$dev_flags, development_only:($dev_flags | length > 0)}
    }' > "$OUTPUT/release-manifest.json"
rm -rf "$FRAGMENTS"

printf '%s  release-manifest.json\n' "$(sha256_of "$OUTPUT/release-manifest.json")" > "$OUTPUT/release-manifest.sha256"

echo "==> writing checksums.sha256"
(
    cd "$OUTPUT"
    while IFS= read -r -d '' relpath; do
        relpath="${relpath#./}"
        [[ "$relpath" == checksums.sha256 ]] && continue
        [[ -f "$relpath" && ! -L "$relpath" ]] || err "non-regular entry in release directory: $relpath"
        printf '%s  %s\n' "$(sha256_of "$relpath")" "$relpath"
    done < <(find . -mindepth 1 ! -type d -print0 | LC_ALL=C sort -z)
) > "$OUTPUT/checksums.sha256"

chmod -R a-w "$OUTPUT"
echo "==> release candidate complete"
echo "release_dir=$OUTPUT"
echo "release_manifest_sha256=$(cut -d' ' -f1 "$OUTPUT/release-manifest.sha256")"
echo "normalized_content_sha256=$NORMALIZED_CONTENT_SHA256"
echo "signed_content_sha256=$SIGNED_CONTENT_SHA256"
if [[ ${#DEV_FLAGS[@]} -gt 0 ]]; then
    echo "DEVELOPMENT-ONLY candidate (${DEV_FLAGS[*]}); not admissible as certification evidence" >&2
fi
