"""UNIT-TEST-ONLY fixtures for the topology lane: a POSIX-sh stand-in for the
installed `vz` CLI, a compiled stand-in for `vz-runtimed` (which both plays the
autospawned daemon and serves as the fake `up`'s Machine-Docker-endpoint bind
probe), and a fake release directory whose manifest/checksums bind those files.
Behaviour
is switched through a mode file the fake reads at startup (the lane never
passes ambient environment to the CLI, so an env switch would be invisible).

Modes: "" (contract-conformant), mutate (bare vz writes ./discovered),
drift (help gains a line), alias (`create` executes), provisions (`up` writes
state and exits 0 without a definition), hang (`ls` sleeps past the
deadline), autospawn (`status --all` spawns a fake daemon that shuts down
gracefully on SIGTERM), bogus_pid (`status --all` leaves an unattributable
PID file), hardened_docker (the Hardened Machine is given a Docker context and
the Docker capabilities), constant_health (every Machine reads `supervised`
whatever its state), ambiguous_exec_runs (`exec` without `--machine` silently
picks the first Machine and runs), leaky_multi_attach (`up` admits a writable
block volume on two Machines instead of refusing it, and writes state before
failing), edge_shortcut (a published `.test` name resolves to the Machine
behind the edge instead of to the edge), edge_hosts_shortcut (the published
name is also in the guest's /etc/hosts, so no resolver is ever asked),
edge_public_resolver (a Machine on a public-like network keeps the image's
public resolvers), edge_tls_unverified (the guest HTTPS client completes the
handshake without checking the chain, so the image's pinned public bundle is
accepted for an Environment's own edge), edge_foreign_anchor_accepted (the
client accepts any named authority, so another Environment's is accepted), and
edge_origin_shortcut (the origin sees the CLIENT as its peer, i.e. no source
translation happened at the edge).
PID file), import_any_port (the guest relays ANY host loopback port instead of
only its declared ones), import_any_machine (every Machine relays machine-0's
grants), export_wildcard (the export listener binds 0.0.0.0 instead of
127.0.0.1).

The last ten exist to make the criterion 2, 6 and 17 checks falsifiable
offline: each one breaks exactly one claim, and the check has to notice.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_candidate as candidate  # noqa: E402
from developer_environment_checks import FLAG_MIGRATION, ROOT_MIGRATION, TYPED_API_MIGRATION  # noqa: E402
from vz04_common import REPO_ROOT, digest_file, read_regular, sha256_bytes  # noqa: E402

FAKE_VZ = r'''#!/bin/sh
# fake vz (unit tests only)
MODE_FILE=__MODE_FILE__
SNAPSHOT_FILE=__SNAPSHOT_FILE__
mode=""
[ -f "$MODE_FILE" ] && mode=$(cat "$MODE_FILE")
reject() {
  if [ "$2" = root ]; then mig='__ROOT_MIGRATION__'; else mig='__FLAG_MIGRATION__'; fi
  printf '{"error":{"code":"legacy_command_removed","command":"%s","message":"`vz %s` was removed from the 0.4 public CLI","migration":"%s","typed_api_migration":"__TYPED__"}}\n' "$1" "$1" "$mig" >&2
  exit 2
}
verb=""; sawhelp=0; version=0; all=0; endopts=0; command_tail=""; machine=""; wantmachine=0
selected=""; wantenv=0
for arg in "$@"; do
  if [ "$wantmachine" = 1 ]; then machine=$arg; wantmachine=0; continue; fi
  if [ "$wantenv" = 1 ]; then selected=$arg; wantenv=0; continue; fi
  if [ "$arg" = "--machine" ] && [ "$endopts" != 1 ]; then wantmachine=1; continue; fi
  if [ "$arg" = "--environment" ] && [ "$endopts" != 1 ]; then wantenv=1; continue; fi
  # Only `exec` takes a command payload after `--`, so a `-c` there is the
  # shell's flag rather than one of this CLI's removed ones. Everywhere else
  # `--` is just a separator and a removed root after it is still rejected.
  if [ "$endopts" = 1 ]; then command_tail=$arg; continue; fi
  if [ "$arg" = "--" ] && [ "$verb" = exec ]; then endopts=1; continue; fi
  case "$arg" in
    create|ls|rm|inspect|attach|close-shell|init|run|logs|stack|image|diff|checkpoint|vm|self-sign|debug)
      if [ "$mode" = alias ] && [ "$arg" = create ]; then echo created; exit 0; fi
      if [ "$mode" = hang ] && [ "$arg" = ls ]; then sleep 30; fi
      reject "$arg" root ;;
    --continue|--resume|--name|--ephemeral|--cpus|--memory|--base-image|--main-container|--control-plane) reject "$arg" flag ;;
    --continue=*|--resume=*|--name=*|--ephemeral=*|--cpus=*|--memory=*|--base-image=*|--main-container=*|--control-plane=*) reject "${arg%%=*}" flag ;;
    up|exec|status|stop|delete) [ -z "$verb" ] && verb="$arg" ;;
    --all) all=1 ;;
    --help|help) sawhelp=1 ;;
    --version) version=1 ;;
    --json|--quiet|--no-stdin|--) ;;
    --*) ;;
    -*)
      rest="${arg#-}"
      while [ -n "$rest" ]; do
        ch=$(printf %.1s "$rest")
        case "$ch" in c) reject -c flag ;; r) reject -r flag ;; h) sawhelp=1 ;; V) version=1 ;; esac
        rest="${rest#?}"
      done ;;
    *) ;;
  esac
done
if [ -n "$verb" ]; then
  if [ "$sawhelp" = 1 ]; then printf 'Usage: vz %s [OPTIONS]\n\nOptions:\n  -h, --help  Print help\n' "$verb"; exit 0; fi
  if [ "$verb" = up ] && [ "$mode" = provisions ]; then
    mkdir -p "$VZ_RUNTIME_DATA_DIR"; : > "$VZ_RUNTIME_STATE_DB"; echo '{"progress":{"completion":{}}}'; exit 0
  fi
  # A real Up persists topology; `status` succeeds only afterwards, which is
  # what the bootstrap-creates-default check depends on.
  topology="$VZ_RUNTIME_DATA_DIR/topology.json"
  # Up is idempotent reconcile, not recreate. A second Up of an Environment
  # that already exists must hand back the identities it already has --
  # criterion 10 claims stop/up preserves them, and criterion 15's typed
  # channel must name the ones the CLI published. Only a fresh Environment
  # (topology removed by `delete`) mints new ones, which is what criterion 16
  # reads.
  if [ "$verb" = up ] && [ -f "$topology" ]; then
    sed 's/^E stopped$/E ready/' "$topology" > "$topology.next" && mv "$topology.next" "$topology"
    printf '{"schema_version":1,"progress":{"completion":{}}}\n'
    exit 0
  fi
  if [ "$verb" = up ] && [ -f vz.json ]; then
    # This Up applies `offline` egress only, exactly as the installed one does:
    # a non-offline Machine needs the per-Environment gateway that is not built
    # yet. Modelled here so a check that depends on the refusal sees it.
    if grep -q '"egress": *"\(allowed\|restricted\)"' vz.json; then
      printf '{"error":{"code":"unsupported_operation","message":"Machine declares a non-offline egress policy, whose adapter is not implemented; this Up applies `offline` only and performs no admission"},"schema_version":1}\n' >&2
      exit 1
    fi
    pid=$(grep -o '"project_id"[^,]*' vz.json | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
    mkdir -p "$VZ_RUNTIME_DATA_DIR"
    # A Developer Machine's Docker endpoint is an AF_UNIX socket bound in the
    # runtime directory under the longest name the runtime mints
    # (`vzr1-ot-<32 hex>.sock`). Bind it for real rather than trusting a length:
    # an unbindable runtime directory must fail Up here exactly as it does on
    # the installed binaries, whatever the caller believed about its budget.
    ep="$VZ_RUNTIME_DATA_DIR/vzr1-ot-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n').sock"
    if ! "$(dirname "$0")/vz-runtimed" "$ep"; then
      printf '{"error":{"code":"state_conflict","message":"endpoint requires a bounded absolute path without traversal"},"schema_version":1}\n' >&2
      exit 1
    fi
    # Runtime identities are minted per Up, not derived from the definition:
    # recreating one pinned definition must hand out entirely new ones.
    # Declared projections and volumes are admitted and materialised BEFORE any
    # identity is minted, so a refused definition leaves the state root exactly
    # as it was -- which is the ordering the criterion-17 check inventories.
    leak=""
    if [ "$mode" = leaky_multi_attach ]; then leak=--admit-multi-attach; : > "$VZ_RUNTIME_STATE_DB"; fi
    # HOME points at the runtime directory, which is outside the lane state
    # root: the interpreter writes framework caches under HOME on startup, and
    # those would otherwise land inside the state root the criterion-17 check
    # inventories.
    if ! HOME="$VZ_RUNTIME_DATA_DIR" /usr/bin/python3 "$(dirname "$0")/vz-storage-model" vz.json \
        "$VZ_RUNTIME_DATA_DIR/guest" "$PWD" "$VZ_RUNTIME_DATA_DIR/volumes" $leak 2>"$VZ_RUNTIME_DATA_DIR/storage.err"; then
      printf '{"error":{"code":"validation_error","message":"%s"},"schema_version":1}\n' \
        "$(tr -d '\n' < "$VZ_RUNTIME_DATA_DIR/storage.err" | sed 's/"/\\"/g')" >&2
      rm -rf "$VZ_RUNTIME_DATA_DIR/guest" "$VZ_RUNTIME_DATA_DIR/volumes"
      rm -f "$VZ_RUNTIME_STATE_DB" "$VZ_RUNTIME_DATA_DIR/storage.err" "$ep"
      exit 1
    fi
    rm -f "$VZ_RUNTIME_DATA_DIR/storage.err"
    : > "$VZ_RUNTIME_STATE_DB"
    inc=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
    # Status must reflect the topology the definition declares, not a fixed one:
    # every Machine with its own profile and target OS, plus the declared
    # networks and endpoints. Reading it out of vz.json is what stops the fake
    # from agreeing with an invented shape instead of with the definition.
    { printf 'P %s\n' "$pid"; printf 'S %s\n' "$inc"; printf 'E ready\n'
      awk '
        { match($0, /^ */); depth = RLENGTH }
        depth == 4 && /^ *"[a-z_]+": \[/ { split($0, k, "\""); section = k[2]; next }
        section == "machines" && depth == 6 && /\{/ {
          name = ""; profile = ""; os = ""; nets = ""; innets = 0; intarget = 0; next }
        section == "machines" && depth == 6 && /\}/ {
          if (name != "") printf "M %s %s %s %s\n", name, profile, os, (nets == "" ? "-" : nets)
          name = ""; next }
        section == "machines" && depth == 8 {
          innets = ($0 ~ /^ *"networks": \[/); intarget = ($0 ~ /^ *"target": \{/)
          split($0, k, "\"")
          if (k[2] == "name") name = k[4]
          if (k[2] == "profile") profile = k[4]
          next }
        section == "machines" && depth == 10 && innets { split($0, k, "\""); nets = (nets == "" ? k[2] : nets "," k[2]); next }
        section == "machines" && depth == 10 && intarget {
          split($0, k, "\""); if (k[2] == "os") os = k[4]; next }
        section == "networks" && depth == 6 && /\{/ { nname = ""; nkind = ""; next }
        section == "networks" && depth == 6 && /\}/ {
          if (nname != "") printf "N %s %s\n", nname, nkind
          nname = ""; next }
        section == "networks" && depth == 8 {
          split($0, k, "\""); if (k[2] == "name") nname = k[4]; if (k[2] == "kind") nkind = k[4]; next }
        section == "endpoints" && depth == 6 && /\{/ { ename = ""; emach = ""; enet = ""; eproto = ""; eport = ""; next }
        section == "endpoints" && depth == 6 && /\}/ {
          if (ename != "") printf "X %s %s %s %s %s\n", ename, emach, enet, eproto, eport
          ename = ""; next }
        section == "endpoints" && depth == 8 {
          split($0, k, "\"")
          if (k[2] == "name") ename = k[4]
          if (k[2] == "machine") emach = k[4]
          if (k[2] == "network") enet = k[4]
          if (k[2] == "protocol") eproto = k[4]
          if (k[2] == "port") { p = k[3]; gsub(/[^0-9]/, "", p); eport = p }
          next }
      ' vz.json
    } > "$topology"
    grep -q '^M ' "$topology" || printf 'M machine-0 developer linux -\n' >> "$topology"
    # A public-like network is the only declaration that gives an Environment an
    # edge, and the edge is the only thing that publishes a name and an
    # authority. Both are recorded here so the guest stand-in models a Machine
    # that was booted knowing them, rather than one told about them afterwards.
    if grep -q '"kind": *"simulated_public"' vz.json; then
      grep -o '"hostname": *"[^"]*"' vz.json | head -1 | sed 's/.*"\([^"]*\)"$/\1/' > "$VZ_RUNTIME_DATA_DIR/edge"
      anchor="$VZ_RUNTIME_DATA_DIR/environment-edges/env_$inc/net_$inc"
      mkdir -p "$anchor"
      # Bound to this Up's own identity, because the authority IS per
      # Environment: two Environments publishing identical bytes would make the
      # cross-Environment TLS refusal unfalsifiable.
      printf -- '-----BEGIN CERTIFICATE-----\nZmFrZSBhdXRob3JpdHk%s\n-----END CERTIFICATE-----\n' \
        "$inc" > "$anchor/authority.pem"
    fi
    # Declared host imports and exports, read out of the definition the same way
    # the runtime reads them: per Machine for an import (a grant belongs to one
    # Machine) and per host port for an export.
    /usr/bin/python3 "$(dirname "$0")/vz-fake-topology.py" vz.json "$VZ_RUNTIME_DATA_DIR" || exit 1
    if [ -f "$VZ_RUNTIME_DATA_DIR/exports" ]; then
      bind_flag=""
      [ "$mode" = export_wildcard ] && bind_flag="--any"
      while read -r hp mp; do
        # Prove the loopback port is free before claiming it, exactly as
        # `probe_exportable_host_ports` does: a second Environment declaring a
        # port a live one already holds must fail the Up, not share it.
        if ! "$(dirname "$0")/vz-runtimed" --probe-tcp "$hp"; then
          printf '{"error":{"code":"state_conflict","message":"host loopback port %s for export is already held on this host"},"schema_version":1}\n' "$hp" >&2
          rm -f "$topology"
          exit 1
        fi
        VZ_FAKE_EXPORT_FILE="$VZ_RUNTIME_DATA_DIR/www/index.html" \
          "$(dirname "$0")/vz-runtimed" --export "$hp" $bind_flag >/dev/null 2>&1 &
        echo $! >> "$VZ_RUNTIME_DATA_DIR/export-pids"
      done < "$VZ_RUNTIME_DATA_DIR/exports"
    fi
    printf '{"schema_version":1,"progress":{"completion":{}}}\n'
    exit 0
  fi
  if [ "$verb" = stop ] && [ -f "$topology" ]; then
    sed 's/^E ready$/E stopped/' "$topology" > "$topology.next" && mv "$topology.next" "$topology"
    printf '{"schema_version":1,"stopped":["default"]}\n'
    exit 0
  fi
  # A stopped Machine runs nothing. Without this, `stop` would be a no-op that
  # a recovery check could pass straight through.
  if [ "$verb" = exec ] && [ -f "$topology" ] && grep -q '^E stopped$' "$topology"; then
    printf '{"error":{"code":"machine_not_ready","message":"the Environment is stopped"},"schema_version":1}\n' >&2
    exit 2
  fi
  if [ "$verb" = exec ] && [ -f "$topology" ] && [ -z "$machine" ]; then
    # Selection is ambiguous whenever the Environment holds more than one
    # Machine and no default was declared, and it must refuse before running
    # anything. `ambiguous_exec_runs` is the deliberately wrong stand-in: it
    # picks the first Machine instead, which is exactly the failure this must
    # be able to detect.
    count=$(grep -c '^M ' "$topology")
    if [ "$count" -gt 1 ] && [ "$mode" != ambiguous_exec_runs ]; then
      cands=$(awk '$1=="M"{printf "%s%s (mch_x_%s)", sep, $2, $2; sep=", "}' "$topology")
      printf '{"error":{"code":"validation_error","message":"Machine selection is ambiguous; specify --machine (candidates: %s)"},"schema_version":1}\n' "$cands" >&2
      exit 1
    fi
    machine=$(awk '$1=="M"{print $2; exit}' "$topology")
  fi
  if [ "$verb" = exec ] && [ -f "$topology" ]; then
    # Model Machine-local mutable state: run the script with the sentinel path
    # rewritten into this isolated runtime dir, so a recreated Environment with
    # a fresh state directory genuinely has none of it.
    # One script file per exec, not one per Machine: a Machine can be running a
    # held foreground process while another exec probes it, and a shared path
    # would rewrite the running script underneath its own interpreter.
    script="$VZ_RUNTIME_DATA_DIR/script.$$.sh"
    printf '%s' "$command_tail" \
      | sed -e "s#/run/vz-reproducibility-sentinel#$VZ_RUNTIME_DATA_DIR/sentinel#g" \
            -e "s#/usr/local/bin/vz-guest-fetch#$(dirname "$0")/guest-fetch-shim#g" \
            -e "s#/bin/busybox#$(dirname "$0")/busybox-shim#g" \
            -e "s#/vz-storage#$VZ_RUNTIME_DATA_DIR/guest/$machine/vz-storage#g" \
            -e "s#/run/vz-edge#$VZ_RUNTIME_DATA_DIR/vz-edge#g" \
            -e "s#/tmp/vz-fetch-#$VZ_RUNTIME_DATA_DIR/fetch-#g" \
            -e "s#/www#$VZ_RUNTIME_DATA_DIR/www#g" > "$script"
    # The shim needs the Machine identity: every Machine on a declared network
    # gets its OWN derived address, so a fake that keys addressing on the project
    # alone would hand two Machines one address and model the wrong property.
    nets=$(awk -v m="$machine" '$1=="M" && $2==m {print $5}' "$topology")
    [ "$nets" = "-" ] && nets=""
    VZ_FAKE_MACHINE="$machine" VZ_FAKE_NETWORKS="$nets" VZ_FAKE_MODE="$mode" \
      VZ_FAKE_MODE_FILE="$MODE_FILE" /bin/sh "$script"
    code=$?
    rm -f "$script"
    exit $code
  fi
  if [ "$verb" = delete ] && [ -f "$topology" ]; then
    # Delete reclaims the Environment's declared storage as well as its
    # topology. The chmod is needed only because a read_only projection is
    # modelled here as a copy whose owner cannot write it; a VirtioFS read-only
    # share is a mount option over a directory the host still owns writable, so
    # nothing equivalent is needed of the real runtime.
    chmod -R u+rwX "$VZ_RUNTIME_DATA_DIR/guest" 2>/dev/null
    rm -rf "$VZ_RUNTIME_DATA_DIR/guest" "$VZ_RUNTIME_DATA_DIR/volumes"
    if [ -f "$VZ_RUNTIME_DATA_DIR/export-pids" ]; then
      while read -r p; do kill "$p" 2>/dev/null; done < "$VZ_RUNTIME_DATA_DIR/export-pids"
      rm -f "$VZ_RUNTIME_DATA_DIR/export-pids"
    fi
    rm -f "$topology" "$VZ_RUNTIME_DATA_DIR"/imports-* "$VZ_RUNTIME_DATA_DIR/exports"
    printf '{"schema_version":1,"deleted":["default"]}\n'; exit 0
  fi
  # An Environment selector that names nothing is a refusal, not a silent
  # fallback to the only Environment there is: `--environment <absent>` must
  # fail the way the installed CLI fails it.
  if [ -n "$selected" ] && [ "$selected" != default ] && [ -f "$topology" ]; then
    printf '{"error":{"code":"environment_not_found","message":"no Environment named %s in this project"},"schema_version":1}\n' "$selected" >&2
    exit 2
  fi
  if [ "$verb" = status ] && [ -f "$topology" ]; then
    # The real success payload is a pretty-printed document, not one line, and
    # it names its own state source and per-Environment state. Every identity
    # below is derived from this project's own id, so two projects declaring
    # identical names still report distinct Environment, Machine, context and
    # endpoint identities -- which is what the no-collision check reads.
    pid=$(awk '$1=="P"{print $2}' "$topology")
    sfx=$(awk '$1=="S"{print $2}' "$topology")
    estate=$(awk '$1=="E"{print $2}' "$topology")
    dg="sha256:$(printf '%s' "$pid" | shasum -a 256 | cut -c1-64)"
    if [ "$estate" = stopped ]; then mstate=stopped; health=inactive; else mstate=ready; health=supervised; fi
    # `constant_health` is the deliberately wrong stand-in for a health field
    # that is written rather than observed: it never changes with the state.
    [ "$mode" = constant_health ] && health=supervised
    # The full declared success-payload field set, so an exact comparison of it
    # is exercised here and not only against the installed binaries.
    printf '{\n "schema_version": 1,\n "request_id": "req-%s",\n "topology_state_source": "persisted",\n' "$sfx"
    printf ' "definition_path": "%s/vz.json",\n "project_name": "vz04-topology-bootstrap",\n' "$PWD"
    printf ' "host": {"os": "macos", "arch": "aarch64"},\n'
    printf ' "daemon": {"backend_name": "macos-vz", "version": "0.1.0"},\n'
    printf ' "desired_definition_digest": "%s",\n "persisted_definition_digest": "%s",\n' "$dg" "$dg"
    printf ' "definition_drift": false,\n "selection_source": "workspace",\n "project_id": "%s",\n' "$pid"
    printf ' "environments": [\n  {\n   "environment_id": "env_%s",\n   "name": "default",\n   "state": "%s",\n' "$sfx" "$estate"
    printf '   "definition_digest": "%s",\n   "lifecycle_generation": 1,\n' "$dg"
    printf '   "machines": ['
    sep=""
    awk '$1=="M"{print $2, $3, $4, $5}' "$topology" | while read -r m profile os nets; do
      # The complete per-Machine projection, not just the identities the
      # no-collision check reads: the exact-field-set check compares this object
      # against the declared Machine set, so a fake emitting less than the
      # installed binaries do would let that comparison pass on absence.
      #
      # A Docker context and the Docker capabilities belong to a
      # Developer-profile LINUX Machine and to nothing else. `hardened_docker`
      # is the deliberately wrong stand-in that hands them to the restricted
      # profile as well.
      docker=no
      [ "$profile" = developer ] && [ "$os" = linux ] && docker=yes
      [ "$mode" = hardened_docker ] && docker=yes
      printf '%s{"name": "%s", "state": "%s", "health": "%s",' "$sep" "$m" "$mstate" "$health"
      if [ "$docker" = yes ]; then
        printf '"docker_context": {'
        printf '"owner": {"project_id": "%s", "environment_id": "env_%s", "machine_id": "mch_%s_%s"},' "$pid" "$sfx" "$sfx" "$m"
        printf '"name": "vzr1-ctx-%s-%s", "endpoint": "unix:///tmp/vz-%s-%s.sock",' "$sfx" "$m" "$sfx" "$m"
        printf '"engine_id": "eng-%s-%s"},' "$sfx" "$m"
        printf '"docker_context_availability": "persisted_ready_not_live_probed",'
        printf '"requested_capabilities": {"capabilities": ["posix_exec", "docker_engine", "compose", "buildx"]},'
        printf '"negotiated_capabilities": {"capabilities": ["posix_exec", "docker_engine", "compose", "buildx"]},'
      else
        printf '"requested_capabilities": {"capabilities": ["posix_exec"]},'
        printf '"negotiated_capabilities": {"capabilities": ["posix_exec"]},'
      fi
      printf '"machine_id": "mch_%s_%s",' "$sfx" "$m"
      printf '"profile": "%s", "backend": "macos_virtualization_linux",' "$profile"
      printf '"target": {"os": "%s", "arch": "aarch64", "image": "vz-linux",' "$os"
      printf ' "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"},'
      printf '"incarnation_id": "inc_%s_%s", "incarnation_generation": 1}' "$sfx" "$m"
      sep=", "
    done
    printf '],\n'
    printf '   "networks": ['
    sep=""
    awk '$1=="N"{print $2, $3}' "$topology" | while read -r n kind; do
      printf '%s{"network_id": "net_%s_%s", "name": "%s", "kind": "%s", "cidr": "10.85.0.0/24"}' "$sep" "$sfx" "$n" "$n" "$kind"
      sep=", "
    done
    printf '],\n'
    printf '   "network_attachments": ['
    sep=""
    awk '$1=="M" && $5!="-"{print $2, $5}' "$topology" | while read -r m nets; do
      printf '%s{"attachment_id": "att_%s_%s", "machine_id": "mch_%s_%s", "network_id": "net_%s_%s"}' \
        "$sep" "$sfx" "$m" "$sfx" "$m" "$sfx" "${nets%%,*}"
      sep=", "
    done
    printf '],\n'
    printf '   "endpoints": ['
    sep=""
    awk '$1=="X"{print $2, $3, $4, $5, $6}' "$topology" | while read -r e em en proto port; do
      printf '%s{"endpoint_id": "end_%s_%s", "name": "%s", "machine_id": "mch_%s_%s",' "$sep" "$sfx" "$e" "$e" "$sfx" "$em"
      printf ' "network_id": "net_%s_%s", "protocol": "%s", "port": %s}' "$sfx" "$en" "$proto" "$port"
      sep=", "
    done
    printf ']\n'
    printf '  }\n ]\n}\n'
    exit 0
  fi
  if [ ! -f vz.json ]; then
    if [ "$verb" = status ]; then
      printf '{"error":{"code":"definition_not_found","message":"no vz.json project definition found at or above %s"}}\n' "$PWD" >&2
    else
      printf '{"error":{"code":"definition_not_found","details":{},"idempotency_key":"k-%s","message":"no vz.json project definition found at or above %s","request_id":"req-%s"},"schema_version":1}\n' "$verb" "$PWD" "$verb" >&2
    fi
    exit 2
  fi
  if [ "$verb" = status ]; then
    sock="$VZ_RUNTIME_DAEMON_SOCKET"; pidf="${sock%.sock}.pid"; logf="${sock%.sock}.log"
    if [ "$mode" = autospawn ]; then
      mkdir -p "$(dirname "$sock")"
      daemon="$(dirname "$0")/vz-runtimed"
      "$daemon" "$sock" "$pidf" "$logf" </dev/null >/dev/null 2>&1 &
      echo $! > "$pidf"
      while [ ! -S "$sock" ]; do sleep 0.05; done
    fi
    if [ "$mode" = bogus_pid ]; then mkdir -p "$(dirname "$sock")"; echo 99999999 > "$pidf"; fi
    printf '{"error":{"code":"daemon_unavailable","message":"no compatible runtime daemon is listening on the configured socket"}}\n' >&2
    exit 2
  fi
  printf '{"error":{"code":"daemon_unavailable","message":"no compatible runtime daemon is listening on the configured socket"}}\n' >&2
  exit 2
fi
if [ "$version" = 1 ]; then echo "vz 0.4.0-fake"; exit 0; fi
[ "$mode" = mutate ] && : > ./discovered
cat "$SNAPSHOT_FILE"
[ "$mode" = drift ] && echo "extra line"
exit 0
'''

# The typed-channel stand-in. It reads the same persisted topology the fake
# `vz status` reads and projects it as the daemon's own aggregate -- nested
# `incarnation`, and no CLI projection code -- so the agreement check compares
# two spellings of one state exactly as it does against the installed binaries.
# Deriving any identity differently here makes the check FAIL, which is what
# keeps it a comparison rather than a formality.
FAKE_PROBE = r"""#!/bin/sh
# fake vz-runtime-probe (unit tests only)
MODE_FILE=__MODE_FILE__
fmode=""
[ -f "$MODE_FILE" ] && fmode=$(cat "$MODE_FILE")
mode=$1; shift
socket=""; project_id=""; environment="default"
while [ $# -gt 0 ]; do
  case "$1" in
    --socket) socket=$2; shift 2 ;;
    --project-id) project_id=$2; shift 2 ;;
    --environment) environment=$2; shift 2 ;;
    --state-db|--definition|--workspace-root|--timeout-millis|--request-id|--idempotency-key) shift 2 ;;
    *) printf '{"schema_version":1,"kind":"vz-runtime-probe-error","reason":"invalid_arguments","detail":"unknown option %s"}\n' "$1"; exit 1 ;;
  esac
done
fail() {
  printf '{"schema_version":1,"kind":"vz-runtime-probe-error","reason":"%s","detail":"%s"}\n' "$1" "$2"
  exit 1
}
topology="$(dirname "$socket")/topology.json"
[ -f "$topology" ] || fail daemon_unavailable "no daemon state at $topology"
pid=$(awk '$1=="P"{print $2}' "$topology")
sfx=$(awk '$1=="S"{print $2}' "$topology")
estate=$(awk '$1=="E"{print $2}' "$topology")
if [ "$estate" = stopped ]; then mstate=stopped; else mstate=ready; fi
dg="sha256:$(printf '%s' "$pid" | shasum -a 256 | cut -c1-64)"
[ -z "$project_id" ] || [ "$project_id" = "$pid" ] || fail get_project_state_failed "no project $project_id"
# probe_drift makes the typed channel mint an Environment identity the CLI
# never published. Everything else about the two documents still matches, so a
# check that passed here would be reading field presence rather than agreement.
esfx="$sfx"
[ "$fmode" = probe_drift ] && esfx="${sfx}drift"
# The typed aggregate, not the CLI's projection of it: `incarnation` is nested
# here and flattened there, and health is absent because it rides beside the
# aggregate on the response rather than inside it.
machines() {
  sep=""
  awk '$1=="M"{print $2, $3, $4, $5}' "$topology" | while read -r m profile os nets; do
    docker=no
    [ "$profile" = developer ] && [ "$os" = linux ] && docker=yes
    [ "$fmode" = hardened_docker ] && docker=yes
    printf '%s{"schema_version":1,"machine_id":"mch_%s_%s","environment_id":"env_%s","name":"%s",' "$sep" "$sfx" "$m" "$esfx" "$m"
    printf '"profile":"%s","target":{"os":"%s","arch":"aarch64","image":"vz-linux",' "$profile" "$os"
    printf '"digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000"},'
    if [ "$docker" = yes ]; then
      printf '"requested_capabilities":{"capabilities":["posix_exec","docker_engine","compose","buildx"]},'
      printf '"negotiated_capabilities":{"capabilities":["posix_exec","docker_engine","compose","buildx"]},'
      printf '"docker_context":{"owner":{"project_id":"%s","environment_id":"env_%s","machine_id":"mch_%s_%s"},' "$pid" "$sfx" "$sfx" "$m"
      printf '"name":"vzr1-ctx-%s-%s","endpoint":"unix:///tmp/vz-%s-%s.sock","engine_id":"eng-%s-%s"},' "$sfx" "$m" "$sfx" "$m" "$sfx" "$m"
    else
      printf '"requested_capabilities":{"capabilities":["posix_exec"]},'
      printf '"negotiated_capabilities":{"capabilities":["posix_exec"]},'
    fi
    printf '"backend":"macos_virtualization_linux",'
    printf '"incarnation":{"schema_version":1,"incarnation_id":"inc_%s_%s","machine_id":"mch_%s_%s","generation":1,"created_at":1},' "$sfx" "$m" "$sfx" "$m"
    printf '"state":"%s"}' "$mstate"
    sep=","
  done
}
networks() {
  sep=""
  awk '$1=="N"{print $2, $3}' "$topology" | while read -r n kind; do
    printf '%s{"network_id":"net_%s_%s","name":"%s","kind":"%s","cidr":"10.85.0.0/24"}' "$sep" "$sfx" "$n" "$n" "$kind"
    sep=","
  done
}
attachments() {
  sep=""
  awk '$1=="M" && $5!="-"{print $2, $5}' "$topology" | while read -r m nets; do
    printf '%s{"attachment_id":"att_%s_%s","machine_id":"mch_%s_%s","network_id":"net_%s_%s"}' \
      "$sep" "$sfx" "$m" "$sfx" "$m" "$sfx" "${nets%%,*}"
    sep=","
  done
}
endpoints() {
  sep=""
  awk '$1=="X"{print $2, $3, $4, $5, $6}' "$topology" | while read -r e em en proto port; do
    printf '%s{"endpoint_id":"end_%s_%s","name":"%s","machine_id":"mch_%s_%s",' "$sep" "$sfx" "$e" "$e" "$sfx" "$em"
    printf '"network_id":"net_%s_%s","protocol":"%s","port":%s}' "$sfx" "$en" "$proto" "$port"
    sep=","
  done
}
machine_ids() {
  sep=""
  awk '$1=="M"{print $2}' "$topology" | while read -r m; do printf '%s"mch_%s_%s"' "$sep" "$sfx" "$m"; sep=","; done
}
if [ "$mode" = state ]; then
  printf '{"schema_version":1,"kind":"vz-runtime-probe-state","request_id":"req-%s","project":' "$sfx"
  printf '{"schema_version":1,"definition":{"schema_version":1,"project_id":"%s","name":"vz04-topology-bootstrap"},' "$pid"
  printf '"environments":[{"schema_version":1,"environment_id":"env_%s","project_id":"%s","name":"%s",' "$esfx" "$pid" "$environment"
  printf '"definition_digest":"%s","state":"%s","lifecycle_generation":1,"machines":[' "$dg" "$estate"
  machines
  printf '],"networks":['
  networks
  printf '],"network_attachments":['
  attachments
  printf '],"endpoints":['
  endpoints
  printf ']}]}}\n'
  exit 0
fi
if [ "$mode" = up ]; then
  admission=$(printf '{"schema_version":1,"project_id":"%s","environment_id":"env_%s","machine_ids":[%s],"definition_digest":"%s","request_id":"req-up-%s","idempotency_key":"key-up-%s","request_hash":"sha256:%s","workspace_key":null,"created_at":1}' "$pid" "$esfx" "$(machine_ids)" "$dg" "$sfx" "$sfx" "$(printf '%064d' 0)")
  seq=0
  for phase in admitted preparing ready; do
    printf '{"schema_version":1,"kind":"vz-runtime-probe-up-event","event":{"schema_version":1,"sequence":%s,"admission":%s,"phase":"%s","operation":null,' "$seq" "$admission" "$phase"
    if [ "$phase" = ready ]; then
      printf '"completion":{"schema_version":1,"environment_id":"env_%s","state":"ready"}}}\n' "$esfx"
    else
      printf '"completion":null}}\n'
    fi
    seq=$((seq + 1))
  done
  exit 0
fi
fail invalid_arguments "unknown mode $mode"
"""

# The storage model is Python rather than `sh` because a POSIX-sh stand-in
# cannot parse a nested JSON definition, and a grep-shaped parse would decide
# the policy questions this check exists to ask. It is a MODEL of the observable
# semantics only: what a Machine can read, what it can write, and what the host
# sees afterwards. The real carriers (VirtioFS shares, a virtio-block image) are
# exercised by the installed binaries in the gate, never here.
STORAGE_MODEL = r"""#!/usr/bin/env python3
import json
import os
import shutil
import sys
from pathlib import Path


def refuse(message):
    print(message, file=sys.stderr)
    raise SystemExit(3)


def main():
    definition_path, guest_root, worktree, volumes_root = (Path(a) for a in sys.argv[1:5])
    # The vacuity switch. With it, a writable block volume on two Machines is
    # ADMITTED and both Machines are given the one backing directory -- exactly
    # the silent multi-attach the policy exists to prevent. The criterion-17
    # check must fail on this input, and a check that could not tell this apart
    # from the correct behaviour would be asserting nothing.
    admit_multi_attach = "--admit-multi-attach" in sys.argv[5:]
    environment = json.loads(definition_path.read_text())["environment"]
    machines = {m["name"]: m for m in environment["machines"]}
    volumes = environment.get("volumes") or []

    # ADMISSION, before anything is created. A writable block volume attached to
    # more than one Machine is refused here and the function returns without
    # having made a single directory, which is what the check's before/after
    # inventory of the state root observes.
    for volume in volumes:
        if volume["kind"] != "block":
            continue
        attachments = volume["attachments"]
        writers = [a for a in attachments if a["mode"] == "read_write"]
        if writers and len(attachments) > 1 and not admit_multi_attach:
            first, second = attachments[0]["machine"], attachments[1]["machine"]
            refuse(
                "volume `%s` is a writable block device attached to Machines `%s` and `%s`; "
                "a block volume carries one ext4 filesystem, which has exactly one writer"
                % (volume["name"], first, second))
        for attachment in attachments:
            if attachment["machine"] not in machines:
                refuse("volume `%s` names Machine `%s`, which this Environment does not have"
                       % (volume["name"], attachment["machine"]))
    for name, machine in machines.items():
        projection = machine.get("workspace")
        if projection is None:
            continue
        source = worktree / projection["source_path"]
        if not source.is_dir():
            refuse("Machine `%s` workspace source `%s` is not resolvable"
                   % (name, projection["source_path"]))

    def place(machine, target_path, maker):
        destination = guest_root / machine / target_path.lstrip("/")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.is_symlink() or destination.exists():
            return
        maker(destination)

    for name, machine in machines.items():
        projection = machine.get("workspace")
        if projection is None:
            continue
        source = (worktree / projection["source_path"]).resolve()
        mode = projection["mode"]
        if mode == "read_write":
            # The share IS the source: a write inside the Machine lands on the
            # host file, which is the property that separates read_write from a
            # copy.
            place(name, projection["target_path"], lambda d: d.symlink_to(source))
        elif mode == "read_only":
            # A copy the Machine cannot write. The real carrier answers EROFS
            # and this answers EACCES; the check asserts the write fails and the
            # host file survives, never a particular errno.
            def read_only(destination, source=source):
                shutil.copytree(source, destination, symlinks=True)
                for path in sorted(destination.rglob("*"), reverse=True):
                    path.chmod(path.stat().st_mode & ~0o222)
                destination.chmod(destination.stat().st_mode & ~0o222)
            place(name, projection["target_path"], read_only)
        elif mode == "snapshot":
            # A private writable copy: the Machine may write, and nothing it
            # writes reaches the worktree.
            place(name, projection["target_path"],
                  lambda d, source=source: shutil.copytree(source, d, symlinks=True))
        else:
            refuse("Machine `%s` declares unknown workspace mode `%s`" % (name, mode))

    for volume in volumes:
        backing = volumes_root / volume["name"]
        backing.mkdir(parents=True, exist_ok=True)
        for attachment in volume["attachments"]:
            # One backing directory for every attachment, so a shared cache is
            # genuinely one directory two Machines see. A per-Machine copy would
            # pass a naive consistency fixture and model the wrong thing.
            place(attachment["machine"], attachment["target_path"],
                  lambda d, backing=backing: d.symlink_to(backing))


main()
"""
FAKE_TOPOLOGY_READER = r'''#!/usr/bin/env python3
"""Read the declared host imports/exports out of a fake project definition.

The fake `vz` is POSIX sh, and a nested JSON array is not something `grep` reads
honestly. This writes the two tables the stand-in needs:

  <state>/imports-<machine>   "<guest_port> <host_port>" per declared import,
                              keyed by the Machine that was granted it, because
                              a grant belongs to one Machine and to no other.
  <state>/exports             "<host_port> <machine_port>" per declared export.
"""
import json
import sys

definition = json.loads(open(sys.argv[1]).read())
state = sys.argv[2]
environment = definition.get("environment", {})
tables = {}
for entry in environment.get("host_imports", []):
    guest_port = entry.get("guest_port") or entry["host_port"]
    tables.setdefault(entry["machine"], []).append(f"{guest_port} {entry['host_port']}")
for machine, rows in tables.items():
    with open(f"{state}/imports-{machine}", "w") as handle:
        handle.write("\n".join(rows) + "\n")
rows = [f"{entry.get('host_port')} {entry['machine_port']}"
        for entry in environment.get("host_exports", []) if entry.get("host_port")]
if rows:
    with open(f"{state}/exports", "w") as handle:
        handle.write("\n".join(rows) + "\n")
'''


CATALOG = {"schema_version": 1, "linux": [
    {"image": "vz-linux-appliance", "version": "0.4.0-fake", "profile": "developer", "bundle_dir": "/nonexistent/developer",
     "digest": "sha256:" + "1" * 64, "channels": []},
    {"image": "vz-linux-appliance", "version": "0.4.0-fake", "profile": "hardened", "bundle_dir": "/nonexistent/container",
     "digest": "sha256:" + "2" * 64, "channels": []}], "macos": []}


def fake_probe_script(mode_file: Path) -> str:
    """The typed stand-in, pointed at the same mode file the fake CLI reads."""
    return FAKE_PROBE.replace("__MODE_FILE__", json.dumps(str(mode_file)))


def fake_vz_script(mode_file: Path, snapshot_file: Path) -> bytes:
    text = (FAKE_VZ.replace("__MODE_FILE__", json.dumps(str(mode_file))).replace("__SNAPSHOT_FILE__", json.dumps(str(snapshot_file)))
            .replace("__ROOT_MIGRATION__", ROOT_MIGRATION).replace("__FLAG_MIGRATION__", FLAG_MIGRATION)
            .replace("__TYPED__", TYPED_API_MIGRATION))
    return text.encode()


# A spawned daemon must report the release `bin/vz-runtimed` path as its own
# executable, exactly as `daemon_fingerprint` requires of the real daemon. A
# copied Apple platform binary (/bin/sh) is refused by macOS even after ad-hoc
# signing, and a shebang script reports its interpreter, so the fixture builds
# one tiny Mach-O that binds the socket and shuts down gracefully on SIGTERM.
FAKE_DAEMON_SOURCE = r"""
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/stat.h>

static char socket_path[1024], pid_path[1024], log_path[1024];

/* Bind one TCP port on 127.0.0.1 and release it: the fake `vz up`'s collision
   proof, deciding by binding rather than by believing a table. */
static int probe_tcp(int port) {
    struct sockaddr_in address;
    int descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (descriptor < 0) {
        return 4;
    }
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((unsigned short)port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(descriptor, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(descriptor);
        return 4;
    }
    close(descriptor);
    return 0;
}

/* The host half of a declared export: a listener that serves the Machine's own
   file over HTTP. Bound to 127.0.0.1 unless `--any` asks for the wildcard the
   `export_wildcard` mode exists to be caught by, so the listener evidence the
   check reads with lsof is a real bind and not a claim. */
static int serve_export(int port, int wildcard) {
    struct sockaddr_in address;
    int descriptor = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    const char *file = getenv("VZ_FAKE_EXPORT_FILE");
    if (descriptor < 0 || file == NULL) {
        return 4;
    }
    setsockopt(descriptor, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((unsigned short)port);
    address.sin_addr.s_addr = htonl(wildcard ? INADDR_ANY : INADDR_LOOPBACK);
    if (bind(descriptor, (struct sockaddr *)&address, sizeof(address)) != 0) {
        return 4;
    }
    if (listen(descriptor, 8) != 0) {
        return 5;
    }
    /* A fixture must not outlive its test even when the test fails before it
       could delete the Environment that started it: a failing test never runs
       `vz delete`, and a listener still holding a port when the next test
       starts is a leak that reads as that test's flakiness. */
    alarm(60);
    for (;;) {
        char request[2048], body[65536], header[256];
        FILE *source;
        size_t length = 0;
        int client = accept(descriptor, NULL, NULL);
        if (client < 0) {
            continue;
        }
        (void)read(client, request, sizeof(request));
        source = fopen(file, "rb");
        if (source != NULL) {
            length = fread(body, 1, sizeof(body), source);
            fclose(source);
        }
        snprintf(header, sizeof(header),
                 "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", length);
        (void)write(client, header, strlen(header));
        if (length > 0) {
            (void)write(client, body, length);
        }
        close(client);
    }
}

static void on_term(int signal_number) {
    (void)signal_number;
    unlink(socket_path);
    unlink(pid_path);
    FILE *log = fopen(log_path, "a");
    if (log != NULL) {
        fputs("runtime daemon shutting down\n", log);
        fclose(log);
    }
    _exit(0);
}

static const char *MIGRATE_SHIM = __SHIM__;

/* Delegate the daemon's own flag form to the embedded sh stand-in. The daemon
   that migrates a state store has to be scriptable (it reads and rewrites
   SQLite), while the daemon a `vz status` autospawns has to BE this Mach-O so
   its executable path is the release `bin/vz-runtimed`. One binary satisfies
   both, and embedding the script rather than shipping it beside the binary
   keeps `install.sh`'s exact five-name install honest. `$0` is this binary, so
   the script can exec it back for the socket-binding form. */
static void delegate_flag_form(int argc, char **argv) {
    char **shell;
    int index;
    for (index = 1; index < argc; index++) {
        if (strcmp(argv[index], "--state-store-path") == 0) {
            break;
        }
    }
    if (index == argc) {
        return;
    }
    shell = calloc((size_t)argc + 4, sizeof(char *));
    if (shell == NULL) {
        return;
    }
    shell[0] = (char *)"/bin/sh";
    shell[1] = (char *)"-c";
    shell[2] = (char *)MIGRATE_SHIM;
    for (index = 0; index < argc; index++) {
        shell[index + 3] = argv[index];
    }
    execv("/bin/sh", shell);
    free(shell);
}

/* argv[1] alone: bind that path, release it, exit. This is the Machine Docker
   endpoint probe the fake `vz up` uses -- it decides bindability by binding,
   not by measuring. argv[1..3]: run as the autospawned daemon stand-in. */
int main(int argc, char **argv) {
    struct sockaddr_un address;
    int descriptor;
    if (argc >= 3 && strcmp(argv[1], "--probe-tcp") == 0) {
        return probe_tcp(atoi(argv[2]));
    }
    if (argc >= 3 && strcmp(argv[1], "--export") == 0) {
        return serve_export(atoi(argv[2]), argc >= 4 && strcmp(argv[3], "--any") == 0);
    }
    delegate_flag_form(argc, argv);
    if (argc != 2 && argc != 4) {
        return 2;
    }
    snprintf(socket_path, sizeof(socket_path), "%s", argv[1]);
    if (argc == 4) {
        snprintf(pid_path, sizeof(pid_path), "%s", argv[2]);
        snprintf(log_path, sizeof(log_path), "%s", argv[3]);
    }
    memset(&address, 0, sizeof(address));
    address.sun_family = AF_UNIX;
    if (strlen(socket_path) >= sizeof(address.sun_path)) {
        return 3;
    }
    snprintf(address.sun_path, sizeof(address.sun_path), "%s", socket_path);
    descriptor = socket(AF_UNIX, SOCK_STREAM, 0);
    if (descriptor < 0 || bind(descriptor, (struct sockaddr *)&address, sizeof(address)) != 0) {
        return 4;
    }
    if (listen(descriptor, 1) != 0) {
        return 5;
    }
    if (argc == 2) {
        close(descriptor);
        unlink(socket_path);
        return 0;
    }
    signal(SIGTERM, on_term);
    for (;;) {
        pause();
    }
}
"""


# The daemon stand-in's flag form: what an installed `vz-runtimed` does to a
# state store older than its own schema. It models exactly the properties
# criterion 19 reads -- a byte-identical pre-migration backup, one
# Project/Environment/Machine per legacy DEVELOPER record, legacy rows left
# where they are, and restoration of the backup when a migration failure is
# injected -- and the `migration_widens`/`migration_no_backup` modes break one
# of them on purpose so the check can be shown to fail on a wrong runtime.
MIGRATE_SHIM = r'''#!/bin/sh
set -u
MODE_FILE=__MODE_FILE__
mode=""
[ -f "$MODE_FILE" ] && mode=$(cat "$MODE_FILE")
db=""; rt=""; sock=""
while [ $# -gt 0 ]; do
  case "$1" in
    --state-store-path) db=$2; shift 2 ;;
    --runtime-data-dir) rt=$2; shift 2 ;;
    --socket-path) sock=$2; shift 2 ;;
    *) shift ;;
  esac
done
[ -n "$db" ] && [ -n "$rt" ] && [ -n "$sock" ] || exit 2
mkdir -p "$rt"
serve() { exec "$0" "$sock" "$sock.pid" "$sock.log"; }

version=$(sqlite3 "$db" "select value from control_metadata where key='schema_version';" 2>/dev/null || echo "")
[ "$version" = 1 ] || serve

if [ "$mode" != migration_no_backup ]; then
  mkdir -p "$rt/state-store-backups"
  bak="$rt/state-store-backups/$(basename "$db").v1.$(date +%s)000000000.bak"
  cp "$db" "$bak"
  sha=$(shasum -a 256 "$bak" | cut -d' ' -f1)
  printf '{"from_schema_version":1,"to_schema_version":12,"sha256":"%s","state_store_path":"%s","backup_path":"%s","created_unix_ns":%s,"migration_completed":%s,"restored":false}\n' \
    "$sha" "$db" "$bak" "$(date +%s)000000000" false > "$bak.json"
fi

developer="json_extract(labels_json,'\$.\"vz.run.workspace\"') IS NOT NULL AND coalesce(json_extract(labels_json,'\$.\"vz.space.mode\"'),'') <> 'required'"
migrated="$developer"
# A runtime that also migrated the Hardened record, and gave it Developer,
# Docker, a host import and an egress default.
[ "$mode" = migration_widens ] && migrated="1=1"

sqlite3 "$db" <<SQL
CREATE TABLE project_definitions (project_id TEXT PRIMARY KEY, schema_version INTEGER, name TEXT, definition_json TEXT, created_at INTEGER, updated_at INTEGER);
CREATE TABLE environment_instances (environment_id TEXT PRIMARY KEY, project_id TEXT, schema_version INTEGER, name TEXT, definition_digest TEXT, state TEXT, instance_json TEXT, created_at INTEGER, updated_at INTEGER, legacy_sandbox_id TEXT, lifecycle_generation INTEGER, active_operation_id TEXT);
CREATE TABLE machine_instances (machine_id TEXT PRIMARY KEY, environment_id TEXT, schema_version INTEGER, name TEXT, state TEXT, instance_json TEXT, legacy_sandbox_id TEXT);
CREATE TABLE workspace_bindings (binding_id TEXT PRIMARY KEY, project_id TEXT, environment_id TEXT, name TEXT, binding_json TEXT);
CREATE TABLE environment_network_attachments (environment_id TEXT, name TEXT, machine_id TEXT);
CREATE TABLE environment_host_exports (environment_id TEXT, name TEXT, machine_id TEXT);
CREATE TABLE environment_host_imports (environment_id TEXT, name TEXT, machine_id TEXT);
CREATE TABLE environment_machine_egress (environment_id TEXT, machine_id TEXT, policy TEXT);

INSERT INTO project_definitions
 SELECT 'prj_'||substr(replace(sandbox_id,'-',''),1,24), 1, sandbox_id, '{}', created_at, updated_at
 FROM sandbox_state WHERE $migrated;
INSERT INTO environment_instances
 SELECT 'env_'||substr(replace(sandbox_id,'-',''),1,24), 'prj_'||substr(replace(sandbox_id,'-',''),1,24), 1,
        'default', 'sha256:0', state, '{}', created_at, updated_at, sandbox_id, 0, NULL
 FROM sandbox_state WHERE $migrated;
INSERT INTO machine_instances
 SELECT 'mac_'||substr(replace(sandbox_id,'-',''),1,24), 'env_'||substr(replace(sandbox_id,'-',''),1,24), 1, 'linux', state,
        json_object('schema_version',1,'name','linux','profile','developer',
                    'target',json_object('os','linux','arch','aarch64','image',json_extract(spec_json,'\$.base_image_ref')),
                    'resources',json_object('cpus',json_extract(spec_json,'\$.cpus'),'memory_mb',json_extract(spec_json,'\$.memory_mb')),
                    'negotiated_capabilities',json_object('capabilities',json_array('posix_exec','docker_engine','compose','buildx')),
                    'legacy_sandbox_id',sandbox_id),
        sandbox_id
 FROM sandbox_state WHERE $migrated;
INSERT INTO workspace_bindings
 SELECT 'wsp_'||substr(replace(sandbox_id,'-',''),1,24), 'prj_'||substr(replace(sandbox_id,'-',''),1,24),
        'env_'||substr(replace(sandbox_id,'-',''),1,24), 'workspace', '{"slots":["workspace"]}'
 FROM sandbox_state WHERE $migrated;
SQL
if [ "$mode" = migration_widens ]; then
  sqlite3 "$db" "
    INSERT INTO environment_host_imports
      SELECT 'env_'||substr(replace(sandbox_id,'-',''),1,24), 'legacy-import', 'mac_'||substr(replace(sandbox_id,'-',''),1,24)
      FROM sandbox_state WHERE json_extract(labels_json,'\$.\"vz.space.mode\"') = 'required';
    INSERT INTO environment_machine_egress
      SELECT 'env_'||substr(replace(sandbox_id,'-',''),1,24), 'mac_'||substr(replace(sandbox_id,'-',''),1,24), 'allowed'
      FROM sandbox_state WHERE json_extract(labels_json,'\$.\"vz.space.mode\"') = 'required';"
fi
sqlite3 "$db" "UPDATE control_metadata SET value='12' WHERE key='schema_version';"

if [ -n "${VZ_STATE_STORE_MIGRATION_FAILPOINT:-}" ]; then
  if [ "$mode" != migration_no_backup ]; then
    rm -f "$db-wal" "$db-shm" "$db-journal"
    cp "$bak" "$db"
    sed 's/"restored":false/"restored":true/' "$bak.json" > "$bak.json.tmp" && mv "$bak.json.tmp" "$bak.json"
    echo "state store migration from schema version 1 failed; the pre-migration backup at $bak was restored over $db byte-for-byte" >&2
  else
    echo "state store migration from schema version 1 failed; no backup was taken" >&2
  fi
  exit 1
fi
[ "$mode" = migration_no_backup ] || {
  sed 's/"migration_completed":false/"migration_completed":true/' "$bak.json" > "$bak.json.tmp" && mv "$bak.json.tmp" "$bak.json"
}
serve
'''

FAKE_NET_IDENTITY = r'''# One Environment's addressing, sourced by every guest stand-in.
#
# Kept in one file rather than copied into each: the BusyBox stand-in and the
# HTTPS-client stand-in have to agree about which address is the edge and which
# is the origin, and two copies that drifted would let a check pass against one
# fake's idea of the topology and fail against the other's.
#
# `state` (the project's runtime directory) and `machine` must already be set.

# One identity per (Environment, Machine), from which BOTH the address and the
# MAC are derived. That is the invariant the real derivation has to hold: the
# switch assigns a port one address and refuses any frame whose source is not
# it, so the MAC on the cmdline and the MAC on the NIC can only ever agree by
# coming from one identity. A fake that made up an unrelated NIC address would
# make the pairing claim unfalsifiable here.
net_seed=$(printf '%s' "$state" | cksum | cut -d' ' -f1)
host_seed=$(printf '%s' "$machine" | cksum | cut -d' ' -f1)
octet_a=$(( (net_seed / 256) % 254 + 1 ))
octet_b=$(( net_seed % 254 + 1 ))
octet_c=$(( host_seed % 200 + 2 ))
fabric_iface=enp0s5
fabric_addr="10.$octet_a.$octet_b.$octet_c"
fabric_mac=$(printf '02:00:00:%02x:%02x:%02x' "$octet_a" "$octet_b" "$octet_c")
# A Machine that declares no Environment network is given no port on the switch:
# the Hardened profile may not declare one, and the runtime writes it no
# `vz.net.N` cmdline entry and configures it no fabric NIC. Modelled by having
# no fabric address at all rather than by filtering traffic, because that is the
# shape of the real denial.
on_fabric=1
[ -n "${VZ_FAKE_NETWORKS:-}" ] || on_fabric=0
nat_addr="192.168.64.$(( host_seed % 200 + 20 ))"
nat_mac=$(printf '02:00:01:%02x:%02x:%02x' "$octet_a" "$octet_b" "$octet_c")

# The Environment's edge, on the reserved first offset of the same range every
# Machine on this network holds an address in. It exists only where the
# definition declared a public-like network, which is exactly when the runtime
# starts one, and it is never any Machine's own address.
edge_addr="10.$octet_a.$octet_b.1"
edge_name=""
[ -f "$state/edge" ] && edge_name=$(cat "$state/edge")
# The Machine the declared endpoint is on. A name that answered with this
# instead of the edge would be a private shortcut wearing a public name, which
# is the distinction criterion 6 exists to make.
origin_seed=$(printf '%s' "machine-0" | cksum | cut -d' ' -f1)
origin_addr="10.$octet_a.$octet_b.$(( origin_seed % 200 + 2 ))"
'''


GUEST_FETCH_SHIM = r'''#!/bin/sh
# Stand-in for the Developer image's certificate-verifying HTTPS client.
#
# It models exactly the properties the criterion-6 check reads off the real
# one, and nothing else: a name that only this Environment's resolver answers,
# a chain that verifies against this Environment's published authority and
# against no other trust store, an origin that must actually be listening, and
# a body whose PEER line is the ORIGIN's view of who connected to it.
#
# The three vacuity modes live here because this is where a defeated claim
# would hide: a client that skipped verification, or accepted any anchor it was
# handed, or a path that never translated the source address, all still produce
# a 200 and a body.
state="$VZ_RUNTIME_DATA_DIR"
machine="${VZ_FAKE_MACHINE:-machine-0}"
mode="${VZ_FAKE_MODE:-}"
. "$(dirname "$0")/fake-net.sh"

# Every failure names its own reason and its own status, because the check
# asserts on the STATUS: a negative TLS claim that accepted "it failed somehow"
# would pass when the name merely failed to resolve.
fail() {
  printf '{"schema_version":1,"kind":"vz-guest-fetch-error","reason":"%s","detail":"stand-in"}\n' "$1" >&2
  exit "$2"
}

[ "$1" = get ] || fail invalid_arguments 2
shift
url=""; ca=""
while [ $# -gt 0 ]; do
  case "$1" in
    --url) [ $# -ge 2 ] || fail invalid_arguments 2; url=$2; shift 2 ;;
    --ca-file) [ $# -ge 2 ] || fail invalid_arguments 2; ca=$2; shift 2 ;;
    --timeout-millis) [ $# -ge 2 ] || fail invalid_arguments 2; shift 2 ;;
    # There is no verification escape hatch in the real client, so there is
    # none here: an option this does not know is a refusal, never a no-op.
    *) fail invalid_arguments 2 ;;
  esac
done
case "$url" in https://*) ;; *) fail invalid_arguments 2 ;; esac
rest=${url#https://}
host=${rest%%/*}
# Resolution goes through the Environment's own resolver, which answers this
# Environment's declared name and nothing else.
{ [ -n "$edge_name" ] && [ "$host" = "$edge_name" ] ; } || fail resolve_failed 5

# The authority this Environment's daemon published. The client trusts what it
# was pointed at and nothing else; with no --ca-file it is pointed at the
# image's pinned PUBLIC bundle, which cannot contain an Environment authority.
own=$(cat "$state"/environment-edges/*/*/authority.pem 2>/dev/null)
[ -n "$own" ] || fail trust_bundle_unreadable 3
presented=""
if [ -n "$ca" ] && [ -f "$ca" ]; then presented=$(cat "$ca"); fi

trusted=0
if [ -n "$presented" ] && [ "$presented" = "$own" ]; then trusted=1; fi
# A client that checks nothing: the public bundle now "verifies" this edge.
[ "$mode" = edge_tls_unverified ] && trusted=1
# A client that accepts whatever anchor it is handed, including another
# Environment's.
if [ "$mode" = edge_foreign_anchor_accepted ] && [ -n "$presented" ]; then trusted=1; fi
[ "$trusted" = 1 ] || fail certificate_rejected 7

# A listener lives exactly as long as the invocation holding it; a marker left
# by a process that is gone is not a service. Criterion 2 runs one listener per
# Machine at once, so the markers are per-Machine and carry the address served;
# the edge reaches whichever Machine is serving the declared origin.
marker=""
for candidate in "$state"/httpd-*; do
  [ -f "$candidate" ] || continue
  marker=$candidate
  break
done
[ -n "$marker" ] || fail connect_failed 6
pid=$(cut -d' ' -f1 < "$marker")
root=$(cut -d' ' -f3- < "$marker")
kill -0 "$pid" 2>/dev/null || fail connect_failed 6

# The edge opens the origin connection itself, so the ORIGIN's peer is the
# edge and the client's address appears nowhere on it. The shortcut mode is
# what a path with no translation looks like from the origin's side.
peer=$edge_addr
[ "$mode" = edge_origin_shortcut ] && peer=$fabric_addr

printf 'TOKEN %s\n' "$(cat "$root/token")"
# The IPv4-mapped literal BusyBox httpd's CGI actually receives.
printf 'PEER [::ffff:%s]\n' "$peer"
# The receipt reports the guest path it was pointed at, not the host path this
# stand-in was rewritten to use.
guest_ca=$(printf '%s' "$ca" | sed "s#^$state/vz-edge#/run/vz-edge#")
printf '{"schema_version":1,"kind":"vz-guest-fetch-response","url":"%s","host":"%s","port":443,"peer":"%s","peer_port":443,"status":200,"protocol":"TLSv1_3","body_bytes":0,"trust_bundle":"%s","trust_anchors":1,"verified":true}\n' \
  "$url" "$host" "$edge_addr" "$guest_ca" >&2
exit 0
'''


BUSYBOX_SHIM = r'''#!/bin/sh
# Stand-in for the guest BusyBox. Applets that only touch files delegate to the
# host; `httpd`, `wget`, `ip` and the `/proc`+`/sys` reads model one
# Environment's private reachability: an address belongs to the project whose
# runtime directory serves it, so a probe from another project's directory
# cannot reach it. That is the property under test, modelled at the granularity
# this fake has (project == Environment).
state="$VZ_RUNTIME_DATA_DIR"
machine="${VZ_FAKE_MACHINE:-machine-0}"
applet=$1
shift
. "$(dirname "$0")/fake-net.sh"

# A peer's MAC is derived from its address by the same rule, so an ARP row names
# the address the peer genuinely carries rather than an invented one.
peer_mac() {
  printf '02:00:00:%02x:%02x:%02x' "$(echo "$1" | cut -d. -f2)" \
    "$(echo "$1" | cut -d. -f3)" "$(echo "$1" | cut -d. -f4)"
}

# Counters belong to a PORT, not to whoever is running: a frame that crossed
# left one port and arrived at the other, so both ends count it. Keying them on
# the Machine that happened to run the probe would let one side's silence read
# as the other side's traffic, which is the very distinction they exist to make.
counter() {
  file="$state/stat-$1-$2"
  value=0
  [ -f "$file" ] && value=$(cat "$file")
  value=$(( value + ${3:-0} ))
  printf '%s' "$value" > "$file"
  printf '%s' "$value"
}

case "$applet" in
  --list)
    # What this BusyBox carries. A check whose clause needs an applet this
    # image lacks has to be able to see that it lacks it, rather than reading
    # an exit code that means several things.
    for bs_applet in sh mount umount mkdir cp cat ls ip hostname chroot switch_root \
      udhcpc echo sleep dmesg tail head wget nsenter unshare mke2fs blkid awk grep \
      nslookup httpd ssl_client; do
      echo "$bs_applet"
    done
    exit 0 ;;
  nslookup)
    # Resolution through the Environment's own resolver, which answers the
    # Environment's declared name and nothing else. There is no upstream: a name
    # this Environment did not declare is not looked for anywhere.
    [ -n "$edge_name" ] || { printf 'nslookup: no resolver\n'; exit 1; }
    target=$edge_addr
    [ "${VZ_FAKE_MODE:-}" = edge_shortcut ] && target=$origin_addr
    if [ "$1" = "$edge_name" ]; then
      printf 'Server:\t%s\nAddress:\t%s:53\n\nName:\t%s\nAddress: %s\n' \
        "$edge_addr" "$edge_addr" "$1" "$target"
      exit 0
    fi
    printf "Server:\t%s\nAddress:\t%s:53\n\nnslookup: can't resolve '%s'\n" \
      "$edge_addr" "$edge_addr" "$1"
    exit 1 ;;
  httpd)
    root=""; foreground=0
    while [ $# -gt 0 ]; do
      case "$1" in -h) root=$2; shift 2 ;; -f) foreground=1; shift ;; *) shift ;; esac
    done
    # A listener lives exactly as long as the invocation that started it. A
    # Machine exec supervises its command as a child subreaper and SIGKILLs
    # every descendant before it reports, so a backgrounded httpd leaves nothing
    # behind -- modelled by refusing to record one at all.
    [ "$foreground" = 1 ] || exit 0
    # One marker per Machine, naming the address it answers on. A single shared
    # marker could not tell two Machines' listeners apart, and criterion 2 runs
    # one on a Developer Machine and one on the Hardened Machine at once.
    serve_addr=127.0.0.1
    [ "$on_fabric" = 1 ] && serve_addr="$fabric_addr"
    printf '%s %s %s' "$$" "$serve_addr" "$root" > "$state/httpd-$machine"
    trap 'rm -f "$state/httpd-$machine"; exit 0' TERM INT HUP EXIT
    while : ; do sleep 1; done ;;
  cat)
    case "${1:-}" in
      /proc/cmdline)
        # The host writes vz.net.N=<mac>,<ipv4>/<prefix>[,<gateway>] for each
        # fabric port, and vz.dns.N=<ipv4> for each edge that answers names.
        # A private network has neither: it has no route off itself and its
        # names travel as a static table.
        # A Machine that declares no Environment network gets no `vz.net.N` at
        # all -- that is how the runtime denies the Hardened profile a fabric
        # port, and criterion 2 reads exactly this absence. Criterion 6's
        # gateway suffix only applies to a Machine that has a port to begin
        # with; the merge of the two lanes briefly lost this gate and gave the
        # Hardened Machine a fabric address.
        if [ "$on_fabric" != 1 ]; then
          printf 'console=hvc0\n'
        elif [ -n "$edge_name" ]; then
          printf 'console=hvc0 vz.net.0=%s,%s/24,%s vz.dns.0=%s\n' \
            "$fabric_mac" "$fabric_addr" "$edge_addr" "$edge_addr"
        else
          printf 'console=hvc0 vz.net.0=%s,%s/24\n' "$fabric_mac" "$fabric_addr"
        fi
        exit 0 ;;
      /etc/resolv.conf)
        # The image ships public resolvers; a Machine on a public-like network
        # is booted with its Environment's resolver in their place.
        if [ -n "$edge_name" ] && [ "${VZ_FAKE_MODE:-}" != edge_public_resolver ]; then
          printf 'nameserver %s\n' "$edge_addr"
        else
          printf 'nameserver 1.1.1.1\nnameserver 8.8.8.8\n'
        fi
        exit 0 ;;
      /etc/hosts)
        printf '127.0.0.1 localhost\n::1 localhost\n'
        # A published name in the static table would mean the resolver is never
        # asked, so the mode that puts one there must break the DNS claims.
        if [ -n "$edge_name" ] && [ "${VZ_FAKE_MODE:-}" = edge_hosts_shortcut ]; then
          printf '%s %s\n' "$origin_addr" "$edge_name"
        fi
        exit 0 ;;
      /proc/net/arp)
        # L2 resolves for peers on this Machine's own fabric network and for
        # nothing else, which is the same boundary the switch enforces.
        printf 'IP address       HW type     Flags       HW address            Mask     Device\n'
        [ -f "$state/arp-$machine" ] || exit 0
        sort -u "$state/arp-$machine" | while read -r peer; do
          printf '%-16s0x1         0x2         %s     *        %s\n' \
            "$peer" "$(peer_mac "$peer")" "$fabric_iface"
        done
        exit 0 ;;
      /sys/class/net/"$fabric_iface"/address) printf '%s\n' "$fabric_mac"; exit 0 ;;
      /sys/class/net/eth0/address) printf '%s\n' "$nat_mac"; exit 0 ;;
      /sys/class/net/*/operstate) printf 'up\n'; exit 0 ;;
      /sys/class/net/*/carrier) printf '1\n'; exit 0 ;;
      # Counted where traffic actually crossed: a fetch this Machine attempted
      # is transmitted, and one it was answered on is received.
      /sys/class/net/"$fabric_iface"/statistics/tx_packets) counter "$fabric_addr" tx 0; printf '\n'; exit 0 ;;
      /sys/class/net/"$fabric_iface"/statistics/rx_packets) counter "$fabric_addr" rx 0; printf '\n'; exit 0 ;;
      /sys/class/net/*/statistics/*) printf '0\n'; exit 0 ;;
    esac
    exec "$applet" "$@" ;;
  ip)
    # A Machine attached to a declared network has TWO IPv4 interfaces: Apple's
    # NAT eth0, which every Machine gets whether or not it declares a network,
    # and the fabric NIC the declared network gives it. Modelling only one hid
    # that distinction and let a probe over the host-shared NAT segment look
    # like a private-fabric proof. `ip -o -4 addr show` field layout, so the
    # caller parses the shim exactly the way it parses the real tool.
    printf '2: eth0    inet %s/24 brd 192.168.64.255 scope global eth0\n' "$nat_addr"
    [ "$on_fabric" = 1 ] && printf '3: %s    inet %s/24 brd 10.%s.%s.255 scope global %s\n' \
      "$fabric_iface" "$fabric_addr" "$octet_a" "$octet_b" "$fabric_iface"
    exit 0 ;;
  nc)
    # Nothing in a Machine serves UDP on an import's guest port: the grant is
    # for the declared stream protocol and nothing else.
    exit 1 ;;
  wget)
    url=""
    while [ $# -gt 0 ]; do case "$1" in http://*) url=$1 ;; esac; shift; done
    hostpart=${url#http://}; hostpart=${hostpart%%/*}
    target=${hostpart%%:*}
    port=${hostpart#*:}; [ "$port" = "$hostpart" ] && port=80
    # The guest's OWN loopback. Reaching a host service through it is a declared
    # host import and nothing else: the guest holds a table of (guest port ->
    # host port) grants for THIS Machine, and a port that is not in it has
    # nothing bound. The guest never names the host destination -- the table
    # does -- so an undeclared host port, another Machine's grant, and a sibling
    # Environment's grant are all simply absent from this table.
    if [ "$target" = "127.0.0.1" ]; then
      table="$state/imports-$machine"
      # The whole point of the grant being per-Machine.
      [ "$mode" = import_any_machine ] && table="$state/imports-machine-0"
      # No table is not a refusal on its own: a Machine with no import grant can
      # still reach its OWN listener below, which is criterion 5's control. A
      # bare `exit 1` here made that control fail for the Hardened Machine and
      # so reported a missing applet as a routing fact -- the exact confusion
      # the control exists to rule out.
      hostport=""
      if [ -f "$table" ]; then
        hostport=$(awk -v p="$port" '$1==p {print $2}' "$table")
      fi
      # The whole point of the guest not choosing the destination: a Machine
      # that HAS a grant also reaches every other host loopback port. Its
      # declared grant still resolves normally and a Machine with no grant still
      # reaches nothing, so this mode breaks only the "cannot choose a host
      # destination" and "undeclared port" denials -- a mode that also broke the
      # positive or "absent by default" would not say which clause caught it.
      if [ -z "$hostport" ] && [ -f "$table" ] && [ "$mode" = import_any_port ]; then hostport=$port; fi
      if [ -n "$hostport" ]; then
        exec /usr/bin/curl -s -m 5 "http://127.0.0.1:$hostport/"
      fi
      # No grant names this port, so this is the Machine's OWN listener rather
      # than a host import: criterion 5's control that its HTTP client and
      # server work at all, which is what makes failing to reach a sibling a
      # routing fact rather than a missing applet. Trying the grant table first
      # keeps both claims falsifiable -- an undeclared host port still reaches
      # nothing, and a local listener is still reachable.
      [ -f "$state/httpd-$machine" ] || exit 1
      pid=$(cut -d' ' -f1 < "$state/httpd-$machine")
      kill -0 "$pid" 2>/dev/null || exit 1
      cat "$(cut -d' ' -f3- < "$state/httpd-$machine")/index.html"
      exit 0
    fi
    # No port on the switch means no route to any Environment address,
    # whatever is listening on it.
    [ "$on_fabric" = 1 ] || exit 1
    # Served on the FABRIC address only. A request to the NAT address must not
    # be answered: the declared private path serves inside its Environment, and
    # the host-shared NAT segment is not that path.
    #
    # Reachability is the SWITCH, not one Machine's own address: every Machine on
    # one Environment's declared network shares that network's /24, so a sibling
    # reaches it and a different Environment (a different /24) does not. Matching
    # only the caller's own address modelled a Machine talking to itself, which
    # passed by accident while every Machine shared one address.
    [ "${target%.*}" = "${fabric_addr%.*}" ] || exit 1
    # On-network is a link-layer fact and does not depend on anything listening:
    # ARP resolves for a peer whose port exists, and the fetch still fails when
    # no listener answers. Collapsing the two is what let a dead listener read
    # as a fabric that does not forward.
    # A Machine reaching its OWN address never leaves the Machine, so it resolves
    # nothing and counts nothing on the wire. That is what makes the self-fetch
    # a statement about the listener alone.
    if [ "$target" != "$fabric_addr" ]; then
      printf '%s\n' "$target" >> "$state/arp-$machine"
      counter "$fabric_addr" tx 1 > /dev/null
      counter "$target" rx 1 > /dev/null
    fi
    # Which Machine answers is decided by the address, not by who asked: the
    # sibling reaches the listener on the server's address, and nothing else.
    pid=""; root=""
    for marker in "$state"/httpd-*; do
      [ -f "$marker" ] || continue
      [ "$(cut -d' ' -f2 < "$marker")" = "$target" ] || continue
      pid=$(cut -d' ' -f1 < "$marker"); root=$(cut -d' ' -f3- < "$marker")
    done
    [ -n "$pid" ] || exit 1
    # The listener is only there while its own invocation is: a marker left by a
    # process that is gone is not a service.
    kill -0 "$pid" 2>/dev/null || exit 1
    if [ "$target" != "$fabric_addr" ]; then
      counter "$target" tx 1 > /dev/null
      counter "$fabric_addr" rx 1 > /dev/null
    fi
    # The origin's CGI reports the peer IT saw. Reached directly like this the
    # peer is the caller, which is the control the translation claim needs: if
    # this reported the edge too, "the origin's peer is the edge" would be a
    # property of the CGI rather than of the path.
    # Written the way BusyBox httpd writes it: it accepts on an IPv6 socket, so
    # an IPv4 peer reaches CGI as a bracketed IPv4-mapped literal. Modelling the
    # bare address would let the caller's unwrapping go unexercised here.
    case "$url" in
      */cgi-bin/peer)
        printf 'TOKEN %s\n' "$(cat "$root/token")"
        printf 'PEER [::ffff:%s]\n' "$fabric_addr" ;;
      *) cat "$root/index.html" ;;
    esac
    exit 0 ;;
  *) exec "$applet" "$@" ;;
esac
'''


def build_fake_daemon(destination: Path, mode_file: Path) -> None:
    """Compile the daemon stand-in, or skip the caller when no compiler exists."""
    compiler = shutil.which("cc") or shutil.which("clang")
    if compiler is None:
        raise unittest.SkipTest("no C compiler for the fake vz-runtimed stand-in")
    source = destination.parent / "fake-vz-runtimed.c"
    shim = MIGRATE_SHIM.replace("__MODE_FILE__", json.dumps(str(mode_file)))
    source.write_text(FAKE_DAEMON_SOURCE.replace("__SHIM__", json.dumps(shim)))
    completed = subprocess.run([compiler, "-O0", "-o", str(destination), str(source)], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=120, check=False)
    source.unlink()
    if completed.returncode != 0:
        raise unittest.SkipTest("cannot build the fake vz-runtimed stand-in: " +
                                completed.stdout.decode("utf-8", "replace")[-200:])


def build_fake_release(root: Path, *, mode_file: Path, snapshot_file: Path = None) -> Path:
    """A read-only fake release dir whose bin/vz is the sh stand-in and whose
    bin/vz-runtimed is a compiled stand-in (so a spawned fake daemon has the
    release path as its executable)."""
    snapshot_file = snapshot_file or (REPO_ROOT / "tests/fixtures/vz-0.4/cli/help-snapshot.txt")
    fixtures.build_fake_release_dir(root)
    fixtures.make_writable(root)
    (root / "bin/vz").write_bytes(fake_vz_script(mode_file, snapshot_file))
    (root / "bin/vz").chmod(0o755)
    build_fake_daemon(root / "bin/vz-runtimed", mode_file)
    (root / "bin/vz-runtimed").chmod(0o755)
    (root / "bin/vz-runtime-probe").write_text(fake_probe_script(mode_file))
    (root / "bin/vz-runtime-probe").chmod(0o755)
    # The guest stand-ins every `vz exec` script addresses, and the one file
    # they both source so they cannot disagree about the Environment's shape.
    (root / "bin/fake-net.sh").write_text(FAKE_NET_IDENTITY)
    (root / "bin/fake-net.sh").chmod(0o644)
    (root / "bin/busybox-shim").write_text(BUSYBOX_SHIM)
    (root / "bin/busybox-shim").chmod(0o755)
    (root / "bin/guest-fetch-shim").write_text(GUEST_FETCH_SHIM)
    (root / "bin/guest-fetch-shim").chmod(0o755)
    # Declared-storage admission and materialisation, which the fake `up` runs.
    (root / "bin/vz-storage-model").write_text(STORAGE_MODEL)
    (root / "bin/vz-storage-model").chmod(0o755)
    # The declared-topology reader the fake `up` calls: a nested JSON array is
    # not something the sh stand-in can read honestly with `grep`.
    (root / "bin/vz-fake-topology.py").write_text(FAKE_TOPOLOGY_READER)
    (root / "bin/vz-fake-topology.py").chmod(0o755)
    catalog = json.dumps(CATALOG, indent=2, sort_keys=True).encode() + b"\n"
    (root / "machine-target-catalog.json").write_bytes(catalog)
    manifest = json.loads(read_regular(root / "release-manifest.json"))
    for relative in ("bin/vz", "bin/vz-runtimed", "bin/vz-runtime-probe"):
        manifest["components"][relative]["signed_sha256"] = digest_file(root / relative)
    components = manifest["components"]
    manifest["normalized_content_sha256"] = candidate.line_digest(sorted([p, c["unsigned_sha256"]] for p, c in components.items()))
    manifest["signed_content_sha256"] = candidate.line_digest(sorted([p, c["signed_sha256"]] for p, c in components.items()))
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    (root / "release-manifest.json").write_bytes(manifest_bytes)
    (root / "release-manifest.sha256").write_bytes(f"{sha256_bytes(manifest_bytes)}  release-manifest.json\n".encode())
    (root / "checksums.sha256").unlink()
    rows = [f"{digest_file(path)}  {path.relative_to(root).as_posix()}\n" for path in sorted(p for p in root.rglob("*") if p.is_file())]
    (root / "checksums.sha256").write_bytes("".join(rows).encode())
    for path in root.rglob("*"):
        path.chmod(stat.S_IMODE(path.lstat().st_mode) & ~0o222)
    return root
