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

Criterion 8 adds five more of the same kind, one per verb of the
cross-Environment isolation claim: cross_environment_resolve (a Machine's
static table names every other Environment's Machine), cross_environment_route
(the route domains are merged, so another Environment's listener answers),
cross_environment_read (`status` also reports every sibling Environment, with
the identities that sibling's own Up minted), cross_environment_control (an
Environment selector naming another Environment is honoured and the verb acts
on it), and cross_environment_events (the daemon fans every Machine event out
to every other Environment's observers). Each breaks exactly one of the five
denials and leaves that sub-check's positive control intact.
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
# Has `establish_recovery_environments` written its sentinel yet?
#
# This ARMS criterion 8's cross_environment_read leak, which must not fire until
# after the three Environments are established -- the leak falsifies the read
# denial, and firing it earlier would break the precondition that creates the
# Environments to hold apart. The check decides where the sentinel lives:
# criterion 11 uses the `/run` ramdisk (rewritten to $VZ_RUNTIME_DATA_DIR/sentinel
# below), criterion 10 uses a DECLARED VOLUME because a ramdisk cannot survive
# the stop/up its clause performs. Testing only the first path silently
# disarmed the leak when the sentinel moved, and four fault-injection tests
# started passing a runtime that leaks -- which is why this asks about both.
sentinel_written() {
  [ -f "$VZ_RUNTIME_DATA_DIR/sentinel" ] && return 0
  for __s in "$VZ_RUNTIME_DATA_DIR"/guest/*/vz-storage/recovery/sentinel; do
    [ -f "$__s" ] && return 0
  done
  return 1
}
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
# The daemon a lifecycle verb needs, spawned if it is not already serving.
#
# This exists for criterion 10's crash clause, which SIGKILLs the daemon hosting
# an Environment and requires the next Up to bring everything back. Without a
# real process holding a real socket there is nothing to kill and the clause
# cannot be exercised at all -- and the compiled `bin/vz-runtimed` stand-in
# already binds a socket, writes a PID file and removes both on SIGTERM, so a
# SIGKILL leaves exactly what a crash leaves: an inert socket inode and a PID
# file naming a process that is gone.
#
# Liveness is decided by signalling the recorded PID, not by the socket file:
# after a SIGKILL the inode is still there and testing for it would report a
# dead daemon as healthy, which is the very confusion the clause is about.
vz_daemon_live() {
  sock="$VZ_RUNTIME_DAEMON_SOCKET"; pidf="${sock%.sock}.pid"
  [ -f "$pidf" ] || return 1
  dpid=$(cat "$pidf" 2>/dev/null)
  [ -n "$dpid" ] || return 1
  kill -0 "$dpid" 2>/dev/null
}
vz_daemon_start() {
  sock="$VZ_RUNTIME_DAEMON_SOCKET"; pidf="${sock%.sock}.pid"; logf="${sock%.sock}.log"
  vz_daemon_live && return 0
  mkdir -p "$(dirname "$sock")"
  rm -f "$sock" "$pidf"
  "$(dirname "$0")/vz-runtimed" "$sock" "$pidf" "$logf" </dev/null >/dev/null 2>&1 &
  echo $! > "$pidf"
  n=0
  while [ ! -S "$sock" ] && [ "$n" -lt 200 ]; do sleep 0.05; n=$((n + 1)); done
  [ -S "$sock" ]
}
if [ -n "$verb" ]; then
  if [ "$sawhelp" = 1 ]; then printf 'Usage: vz %s [OPTIONS]\n\nOptions:\n  -h, --help  Print help\n' "$verb"; exit 0; fi
  if [ "$verb" = up ] && [ "$mode" = provisions ]; then
    mkdir -p "$VZ_RUNTIME_DATA_DIR"; : > "$VZ_RUNTIME_STATE_DB"; echo '{"progress":{"completion":{}}}'; exit 0
  fi
  # A real Up persists topology; `status` succeeds only afterwards, which is
  # what the bootstrap-creates-default check depends on.
  topology="$VZ_RUNTIME_DATA_DIR/topology.json"
  # `cross_environment_control` is criterion 8's deliberately wrong stand-in for
  # a control plane with no Environment boundary: a selector naming ANOTHER
  # Environment's identity is honoured, and the verb acts on that Environment.
  # It is the only thing that can make the fail-closed and target-unchanged
  # claims falsifiable -- a mode that merely ran the verb locally would leave
  # the target unchanged for the wrong reason.
  if [ "$mode" = cross_environment_control ] && [ -n "$selected" ] && [ "$selected" != default ]; then
    for other in "$(dirname "$VZ_RUNTIME_DATA_DIR")"/*/topology.json; do
      [ -f "$other" ] || continue
      osfx=$(awk '$1=="S"{print $2}' "$other")
      # The identity the caller named stays addressable after the wreck below
      # rewrote it, so a second verb aimed at the same Environment still lands
      # on it. Without this the first verb would make every later one refuse
      # honestly, and half the target-unchanged claims would never be exercised.
      alias_file="$(dirname "$other")/cross-alias"
      aliased=""
      [ -f "$alias_file" ] && aliased=$(cat "$alias_file")
      { [ "$selected" = "env_$osfx" ] || { [ -n "$aliased" ] && [ "$selected" = "env_$aliased" ]; }; } || continue
      [ -f "$alias_file" ] || printf '%s' "$osfx" > "$alias_file"
      VZ_RUNTIME_DATA_DIR=$(dirname "$other"); export VZ_RUNTIME_DATA_DIR
      topology=$other; selected=default
      # And it does to that Environment what a Machine driven from outside its
      # own Environment would: a fresh incarnation and no Machine-local state.
      # Without this the mode would falsify "the verb is refused" but leave
      # "the target is unchanged" unable to fail on its identity or sentinel.
      awk '$1=="S"{print "S ffffffffffffffff"; next} {print}' "$topology" > "$topology.x" \
        && mv "$topology.x" "$topology"
      rm -f "$VZ_RUNTIME_DATA_DIR/sentinel"
      break
    done
  fi
  # An Environment selector naming an Environment this project does not have is
  # refused for EVERY verb, before any of them acts. The fallthrough refusal
  # further down is reached only by `status`, so without this `stop`, `delete`
  # and `exec` would silently act on the local Environment instead -- which is
  # precisely the cross-Environment control criterion 8 forbids.
  if [ -n "$selected" ] && [ "$selected" != default ] && [ -f "$topology" ]; then
    printf '{"error":{"code":"environment_not_found","message":"no Environment named %s in this project"},"schema_version":1}\n' "$selected" >&2
    exit 2
  fi
  # Up is idempotent reconcile, not recreate. A second Up of an Environment
  # that already exists must hand back the identities it already has --
  # criterion 10 claims stop/up preserves them, and criterion 15's typed
  # channel must name the ones the CLI published. Only a fresh Environment
  # (topology removed by `delete`) mints new ones, which is what criterion 16
  # reads.
  if [ "$verb" = up ] && [ -f "$topology" ]; then
    vz_daemon_start || { printf '{"error":{"code":"backend_unavailable","message":"no daemon"},"schema_version":1}\n' >&2; exit 2; }
    sed 's/^E stopped$/E ready/' "$topology" > "$topology.next" && mv "$topology.next" "$topology"
    printf '{"schema_version":1,"progress":{"completion":{}}}\n'
    exit 0
  fi
  if [ "$verb" = up ] && [ -f vz.json ]; then
    # `offline` and `allowed` are both applied, exactly as the installed one
    # does: `offline` attaches no external NIC and `allowed` attaches Apple's
    # NAT, decided in `external_nic_required`. This fake used to refuse every
    # non-offline policy because the installed Up did; that refusal is gone,
    # and a fake that kept it would make criterion 7's enabled-egress clause
    # look unexercisable when it is not.
    #
    # `restricted` is still refused, and by the SCHEMA rather than here: the
    # project definition spells `offline` and `allowed` only, so a CIDR or
    # domain policy has no way to be written down. That is the honest state of
    # criterion 20's two blank cells.
    if grep -q '"egress": *"restricted"' vz.json; then
      printf '{"error":{"code":"unsupported_operation","message":"Machine declares a `restricted` egress policy, which the project definition schema does not spell; `offline` and `allowed` are the two it does"},"schema_version":1}\n' >&2
      exit 1
    fi
    pid=$(grep -o '"project_id"[^,]*' vz.json | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
    mkdir -p "$VZ_RUNTIME_DATA_DIR"
    vz_daemon_start || { printf '{"error":{"code":"backend_unavailable","message":"no daemon"},"schema_version":1}\n' >&2; exit 2; }
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
    # This Environment's identity suffix, minted here rather than below because
    # the SecretBinding refusal needs to compare `from_environment` against it.
    inc=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
    # Declared SecretBindings, delivered into the BOUND Machine's guest root and
    # no other. Criterion 18 asks whether the bound Machine can read the value
    # and its sibling cannot, and per-Machine guest roots are what make that a
    # real answer rather than an arranged one.
    if ! HOME="$VZ_RUNTIME_DATA_DIR" /usr/bin/python3 -c '
import json, os, pathlib, sys
guest, runtime = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
declaration = json.load(open("vz.json"))
environment_id = "env_" + sys.argv[3]
declared = declaration.get("environment", {}).get("secret_bindings") or []
for binding in declared:
    # A binding naming ANOTHER Environment is refused, and refused before any
    # Environment exists: the criterion asks that the request fail closed and
    # leave nothing behind, so this cannot be a cleanup afterwards.
    foreign = binding.get("from_environment")
    if foreign and foreign != environment_id:
        json.dump({"schema_version": 1, "error": {"code": "cross_environment_denied", "message":
                   "SecretBinding %r names Environment %s, which is not this Environment; "
                   "cross-Environment access requires an explicit directional grant"
                   % (binding.get("name"), foreign)}}, sys.stdout)
        raise SystemExit(3)
    source = binding.get("source_env") or ""
    if source not in os.environ:
        sys.stderr.write("secret source %s is not set" % source)
        raise SystemExit(1)
    target = guest / binding["machine"] / binding["target_path"].lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(os.environ[source])
    target.chmod(0o600)
(runtime / "bindings.json").write_text(json.dumps(
    [{"name": b["name"], "machine": b["machine"], "target_path": b["target_path"]} for b in declared]))
' "$VZ_RUNTIME_DATA_DIR/guest" "$VZ_RUNTIME_DATA_DIR" "$inc" \
        >"$VZ_RUNTIME_DATA_DIR/secret.out" 2>"$VZ_RUNTIME_DATA_DIR/secret.err"; then
      code=$?
      if [ "$code" = 3 ]; then
        cat "$VZ_RUNTIME_DATA_DIR/secret.out" >&2; echo >&2
      else
        printf '{"error":{"code":"validation_error","message":"%s"},"schema_version":1}\n' \
          "$(tr -d '\n' < "$VZ_RUNTIME_DATA_DIR/secret.err" | sed 's/"/\\"/g')" >&2
      fi
      rm -rf "$VZ_RUNTIME_DATA_DIR/guest" "$VZ_RUNTIME_DATA_DIR/volumes"
      rm -f "$VZ_RUNTIME_DATA_DIR/secret.err" "$VZ_RUNTIME_DATA_DIR/secret.out" "$VZ_RUNTIME_STATE_DB" "$ep"
      exit 2
    fi
    rm -f "$VZ_RUNTIME_DATA_DIR/secret.err" "$VZ_RUNTIME_DATA_DIR/secret.out"
    : > "$VZ_RUNTIME_STATE_DB"
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
    # `cross_environment_events` is criterion 8's deliberately wrong stand-in
    # for a daemon that fans every Machine event out to every Environment's
    # observers instead of only to the Environment that produced it. It never
    # writes into the Environment that produced the event, so an observer still
    # sees its OWN Environment's events exactly as it does conformantly and only
    # the cross-Environment silence can break.
    if [ "$mode" = cross_environment_events ]; then
      for other in "$(dirname "$VZ_RUNTIME_DATA_DIR")"/*/; do
        case "$other" in "$VZ_RUNTIME_DATA_DIR"/) continue ;; esac
        [ -d "$other/www" ] || continue
        printf 'EVENT %s\n' "$command_tail" >> "$other/www/vz-cross-events"
      done
    fi
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
            -e "s#/run/vz-secrets#$VZ_RUNTIME_DATA_DIR/guest/$machine/run/vz-secrets#g" \
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
    # Using a declared SecretBinding is auditable. One record per use, naming
    # the Environment, the Machine that read it and the binding -- and never
    # the value, which is the whole point of the record existing.
    if [ -f "$VZ_RUNTIME_DATA_DIR/bindings.json" ]; then
      HOME="$VZ_RUNTIME_DATA_DIR" /usr/bin/python3 -c '
import json, os, pathlib, sys, time
runtime, machine, environment = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
command = sys.argv[4]
guest = runtime / "guest" / machine
audit = runtime / "audit.jsonl"
for binding in json.loads((runtime / "bindings.json").read_text()):
    if binding["target_path"] not in command:
        continue
    if not (guest / binding["target_path"].lstrip("/")).is_file():
        continue
    record = {"event": "secret_binding_used", "environment_id": environment,
              "machine": machine, "machine_id": "mch_%s_%s" % (environment[4:], machine),
              "binding": binding["name"],
              "binding_id": "sbn_" + binding["name"], "unix_ns": time.time_ns()}
    with open(audit, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
    os.chmod(audit, 0o600)
' "$VZ_RUNTIME_DATA_DIR" "$machine" "env_$(awk '$1=="S"{print $2}' "$topology")" "$command_tail" 2>/dev/null
    fi
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
  if [ "$verb" = status ] && [ -f "$topology" ] && ! vz_daemon_live; then
    # Exactly what the installed CLI answers when the socket it was told to use
    # has no daemon behind it. Answering from the persisted record instead
    # would report a crashed Environment as healthy.
    printf '{"error":{"code":"daemon_unavailable","message":"no compatible runtime daemon is listening on the configured socket"}}\n' >&2
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
    # Under `cross_environment_read` this project also names ANOTHER
    # Environment's directory as the source of its own definition, so the claim
    # that a status names its OWN definition can fail as well as pass. Armed by
    # the same sentinel gate as the leaked Environment objects below.
    dpath="$PWD"
    if [ "$mode" = cross_environment_read ] && sentinel_written; then
      for other in "$(dirname "$VZ_RUNTIME_DATA_DIR")"/*/topology.json; do
        [ -f "$other" ] || continue
        [ "$other" = "$topology" ] && continue
        dpath=$(dirname "$other"); break
      done
    fi
    printf ' "definition_path": "%s/vz.json",\n "project_name": "vz04-topology-bootstrap",\n' "$dpath"
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
    printf '  }'
    # `cross_environment_read` is criterion 8's deliberately wrong stand-in for
    # a control plane that exposes another Environment's state: this project's
    # status also reports every sibling Environment, with the identities that
    # sibling's own Up minted. Reading them out of the sibling's persisted
    # topology is what makes it a real leak -- identities invented here would
    # match nothing the isolation check recorded, and the claim would be
    # unfalsifiable.
    # Armed only once this Environment holds the sentinel that
    # `establish_recovery_environments` writes AFTER it has read this status,
    # so the leak falsifies criterion 8's read denial without breaking the
    # precondition that establishes the three Environments to hold apart.
    if [ "$mode" = cross_environment_read ] && sentinel_written; then
      for other in "$(dirname "$VZ_RUNTIME_DATA_DIR")"/*/topology.json; do
        [ -f "$other" ] || continue
        [ "$other" = "$topology" ] && continue
        osfx=$(awk '$1=="S"{print $2}' "$other")
        opid=$(awk '$1=="P"{print $2}' "$other")
        printf ',\n  {"environment_id": "env_%s", "name": "default", "state": "ready",' "$osfx"
        printf ' "definition_digest": "sha256:%s", "lifecycle_generation": 1, "project_id": "%s",' "$osfx" "$opid"
        # The sibling's runtime directory, so the leak is a readable PATH into
        # another Environment's state as well as a readable identity.
        printf ' "runtime_directory": "%s",' "$(dirname "$other")"
        printf ' "machines": [{"name": "machine-0", "state": "ready", "health": "supervised",'
        printf ' "machine_id": "mch_%s_machine-0", "incarnation_id": "inc_%s_machine-0",' "$osfx" "$osfx"
        printf ' "incarnation_generation": 1, "profile": "developer",'
        printf ' "docker_context": {"name": "vzr1-ctx-%s-machine-0"}}],' "$osfx"
        printf ' "networks": [], "network_attachments": [], "endpoints": []}'
      done
    fi
    printf '\n ]\n}\n'
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
      # Through the shared helper, which returns early when a daemon is already
      # serving this isolate. Spawning unconditionally wrote the PID of a second
      # daemon that then failed to bind and exited, so the PID file named a dead
      # process while the live one kept running -- and the cleanup sweep
      # reported an artifact it could not attribute rather than the read-only
      # violation this mode exists to produce.
      vz_daemon_start
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
  <state>/egress-<machine>    written only for a Machine that declared
                              `allowed`, because that is the only Machine the
                              runtime gives an external NIC. Its ABSENCE is
                              what `offline` means, exactly as in the runtime:
                              not a filter over a shared gateway, but no
                              attachment at all.
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
for machine in environment.get("machines", []):
    if machine.get("egress") == "allowed":
        with open(f"{state}/egress-{machine['name']}", "w") as handle:
            handle.write("allowed\n")
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
    /* A fixture must not outlive its test session, for the same reason
       `serve_export` sets an alarm: a stand-in daemon whose test never reached
       its cleanup is a process nobody will ever stop, and one unit-test run
       leaves dozens. Far longer than any stand-in lane phase, so nothing in a
       running test can trip over it. */
    alarm(1800);
    /* A started daemon says so in its own log. Criterion 18's redaction sweep
       reads the daemon log as one of its declared artifact groups, and a group
       that never exists is a group the sweep cannot fail on. */
    {
        FILE *log = fopen(log_path, "a");
        if (log != NULL) {
            fprintf(log, "runtime daemon listening on %s\n", socket_path);
            fclose(log);
        }
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
# A public Internet address, reached over TLS on 443 because that is what 443
# serves. The matrix used to probe these with plaintext `wget http://host:443/`,
# which opens the connection, sends HTTP to a TLS listener and exits non-zero --
# recording `deny` for a host the Machine reached perfectly well. Every deny
# cell passed anyway for the wrong reason, so the confusion only surfaced when
# an `allowed` Machine could finally be built and its one `allow` cell failed.
#
# Answered from the Machine's declared egress, exactly as the plaintext path
# is, and against the image's PUBLIC bundle with no --ca-file: 1.1.1.1 and
# 8.8.8.8 present certificates for their own addresses, so no Environment
# authority is involved.
case "$host" in
  1.1.1.1|8.8.8.8)
    [ -z "$ca" ] || fail invalid_arguments 2
    [ -f "$state/egress-$machine" ] || fail connect_failed 6
    printf '{"schema_version":1,"kind":"vz-guest-fetch-response","url":"%s","host":"%s","port":443,"peer":"%s","peer_port":443,"status":200,"protocol":"TLSv1_3","body_bytes":0,"trust_bundle":"public","trust_anchors":1,"verified":true}\n' \
      "$url" "$host" "$host"
    exit 0
    ;;
esac
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
# Criterion 7's falsifying modes (import_any_port, import_any_machine,
# export_wildcard) are read from here. The merge that brought them in kept the
# environment pass-through but not this assignment, so `$mode` was empty and all
# three modes were silently inert -- the check passed in every mode, which is
# exactly the shape of a vacuity test that cannot fail.
mode="${VZ_FAKE_MODE:-}"
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
    # `cross_environment_resolve` is criterion 8's deliberately wrong stand-in
    # for a merged name space: this resolver also ANSWERS for a Machine that
    # belongs to another Environment, not merely lists it in a static table.
    if [ "$mode" = cross_environment_resolve ]; then
      for other in "$(dirname "$state")"/*/topology.json; do
        [ -f "$other" ] || continue
        [ "$(dirname "$other")" = "$state" ] && continue
        osfx=$(awk '$1=="S"{print $2}' "$other")
        { [ "$1" = "env_$osfx" ] || [ "$1" = "mch_${osfx}_machine-0" ] ; } || continue
        printf 'Server:\t10.0.0.1\nAddress:\t10.0.0.1:53\n\nName:\t%s\nAddress: 10.0.0.9\n' "$1"
        exit 0
      done
    fi
    # `cross_environment_resolve_name` answers a name a SIBLING Environment
    # declared. This is the resolve clause's own falsifier: the name is read out
    # of the sibling's own edge file, so it is the name that Environment really
    # published and not one invented here, which is what makes the refusal
    # assertion able to fail.
    if [ "$mode" = cross_environment_resolve_name ]; then
      for other in "$(dirname "$state")"/*/edge; do
        [ -f "$other" ] || continue
        [ "$(dirname "$other")" = "$state" ] && continue
        [ "$1" = "$(cat "$other")" ] || continue
        printf 'Server:\t10.0.0.1\nAddress:\t10.0.0.1:53\n\nName:\t%s\nAddress: 10.0.0.9\n' "$1"
        exit 0
      done
    fi
    # `no_local_resolver` is the VACUITY guard for that clause: a Machine with
    # no resolver at all refuses every name, including a sibling's, so the
    # refusals alone would be satisfied by a Machine that can resolve nothing.
    # The control -- each Environment resolving the name IT declared -- is what
    # this mode has to break.
    if [ "$mode" = no_local_resolver ]; then
      printf 'nslookup: no resolver\n'; exit 1
    fi
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
        # `cross_environment_resolve` is criterion 8's deliberately wrong
        # stand-in for a merged name space: this Machine's static table names
        # every OTHER Environment's Machine, so its resolver view answers for a
        # Machine that belongs to a different Environment. The identity is read
        # from that Environment's own persisted topology, so it is the identity
        # criterion 8's record holds rather than an invented string.
        if [ "$mode" = cross_environment_resolve ]; then
          for other in "$(dirname "$state")"/*/topology.json; do
            [ -f "$other" ] || continue
            [ "$(dirname "$other")" = "$state" ] && continue
            printf '10.0.0.9 mch_%s_machine-0\n' "$(awk '$1=="S"{print $2}' "$other")"
          done
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
    # Model REAL BusyBox, not the answer the check wants. BusyBox 1.37.0 is built
    # CONFIG_NC_110_COMPAT=y: without `-z` it never runs `udptest()` and returns
    # 0 whether the port is refused, unbound or live. This shim returned 1
    # unconditionally, so criterion 7's wrong-protocol clause passed here and
    # could not pass on hardware -- a stand-in that models the desired behaviour
    # instead of the real one makes its check unfalsifiable.
    case " $* " in
      *" -z "*) exit 1 ;;
      *) exit 0 ;;
    esac ;;
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
    # An address on the public Internet. This is the only destination whose
    # answer is decided by the Machine's declared egress, and it is decided the
    # way the runtime decides it: `allowed` attaches an external NIC and
    # `offline` attaches none, so the same probe from two Machines of ONE
    # Environment answers differently. Modelled rather than really dialled,
    # because a fake that depended on the build host's own connectivity would
    # report a flaky network as a policy result.
    #
    # Only these two addresses are modelled, and deliberately: LAN and
    # control-plane destinations are separate cells with their own denials, and
    # folding them in here would make one rule stand for three.
    case "$target" in
      1.1.1.1|8.8.8.8)
        [ -f "$state/egress-$machine" ] || exit 1
        printf 'vz04-internet-reachable\n'
        exit 0
        ;;
    esac
    # `cross_environment_route` is criterion 8's deliberately wrong stand-in for
    # a merged route domain: an address belonging to ANOTHER Environment's
    # Machine is reachable and its listener answers. It never serves the
    # caller's own Environment, so the loopback control that proves the client
    # and server work is untouched and only the cross-Environment denial breaks.
    if [ "$mode" = cross_environment_route ]; then
      for marker in "$(dirname "$state")"/*/httpd-*; do
        [ -f "$marker" ] || continue
        case "$marker" in "$state"/*) continue ;; esac
        mpid=$(cut -d' ' -f1 < "$marker"); mroot=$(cut -d' ' -f3- < "$marker")
        kill -0 "$mpid" 2>/dev/null || continue
        cat "$mroot/index.html"
        exit 0
      done
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


# --------------------------------------------------------------------- criterion 22
#
# A second, purpose-built stand-in for `vz`, used ONLY by the criterion-22 tests.
# `FAKE_VZ` above models a CLI whose definition digest is derived from the
# project id, so editing `vz.json` never moves it and there is no reconciliation
# decision to observe. Rather than teach that stand-in a second job -- five
# criteria's checks depend on its exact current behaviour -- this one models the
# one surface criterion 22 reads and nothing else: the desired/persisted
# definition digests, `definition_drift`, and what `vz up` does when they differ.
#
# Its default behaviour is the installed runtime's: every ProjectDefinition
# change is refused before admission with a `validation_error` naming both
# digests (`resolve_or_reserve_environment_for_up_in_transaction` in
# crates/vz-stack/src/state_store/topology.rs). Each named mode below produces
# exactly one wrong value so one assertion of one sub-check has to notice it, and
# `recon_reconciles` models the runtime the criterion describes -- one that
# reconciles mutable changes and classifies immutable ones -- so the checks are
# exercised on the accepting path too.
RECONCILE_MODES = ("", "recon_reconciles", "recon_identity_drift", "recon_nondeterministic",
                   "recon_mutates_on_refusal", "recon_incidental_refusal", "recon_no_code",
                   "recon_accepts_stale_replay", "recon_orphan", "recon_cross_owner",
                   "recon_consumes_generation", "recon_bumps_incarnation", "recon_accepts_immutable",
                   "recon_activation_digest_differs", "recon_digest_ignores_value",
                   "recon_digest_over_bytes", "recon_publishes_snapshot_keys",
                   "recon_half_reconciles")

RECONCILE_VZ = r'''#!/usr/bin/python3
"""UNIT-TEST-ONLY stand-in for `vz`: the definition-digest surface only."""
import hashlib
import json
import os
import sys
import uuid

MODE_FILE = __MODE_FILE__
SHARED_NETWORK = "net_adopted_shared"


def mode():
    try:
        with open(MODE_FILE, encoding="utf-8") as stream:
            return stream.read().strip()
    except OSError:
        return ""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def state_path():
    return os.path.join(os.environ["VZ_RUNTIME_DATA_DIR"], "recon-state.json")


def load_state():
    try:
        with open(state_path(), encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError):
        return None


def save_state(state):
    # Atomic: the concurrent-Up sub-check runs two of these at once, and a
    # reader that saw a truncated file would decide there is no topology at all
    # and mint a second Environment.
    os.makedirs(os.path.dirname(state_path()), exist_ok=True)
    temporary = state_path() + ".%d.tmp" % os.getpid()
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(state, stream, sort_keys=True)
    os.replace(temporary, state_path())


def key_path(key):
    """One file per idempotency key, so two concurrent Ups cannot lose one."""
    return os.path.join(os.environ["VZ_RUNTIME_DATA_DIR"],
                        "recon-key-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16] + ".txt")


def seen_key(key):
    try:
        with open(key_path(key), encoding="utf-8") as stream:
            return stream.read().strip()
    except OSError:
        return None


def remember_key(key, digest):
    with open(key_path(key), "w", encoding="utf-8") as stream:
        stream.write(digest)


def read_definition():
    with open("vz.json", "rb") as stream:
        raw = stream.read()
    return raw, json.loads(raw.decode("utf-8"))


def desired_digest(raw, definition):
    """The identity planning consumes. Canonical over the VALUE by default."""
    if mode() == "recon_digest_ignores_value":
        return "sha256:" + hashlib.sha256(definition["project_id"].encode()).hexdigest()
    if mode() == "recon_digest_over_bytes":
        return "sha256:" + hashlib.sha256(raw).hexdigest()
    return "sha256:" + hashlib.sha256(canonical(definition)).hexdigest()


def persisted_digest(state):
    """The identity activation was admitted under, spelled the same way."""
    if mode() == "recon_digest_ignores_value":
        return "sha256:" + hashlib.sha256(state["definition"]["project_id"].encode()).hexdigest()
    if mode() == "recon_digest_over_bytes":
        return "sha256:" + hashlib.sha256(state["raw"].encode("utf-8")).hexdigest()
    return "sha256:" + hashlib.sha256(canonical(state["definition"])).hexdigest()


def create(raw, definition):
    suffix = uuid.uuid4().hex[:12]
    return {
        "definition": definition,
        "raw": raw.decode("utf-8"),
        "environment_id": "env_" + suffix,
        "name": "default",
        "lifecycle_generation": 1,
        "machines": [{"machine_id": "mch_%s_%s" % (suffix, machine["name"]),
                      "name": machine["name"], "profile": machine["profile"],
                      "target": machine["target"],
                      "incarnation_id": "inc_%s_%s" % (suffix, machine["name"]),
                      "incarnation_generation": 1}
                     for machine in definition["environment"]["machines"]],
        "networks": [{"network_id": "net_%s_%s" % (suffix, index), "name": network["name"],
                      "kind": network.get("kind", "private"), "cidr": "10.85.0.0/24"}
                     for index, network in enumerate(definition["environment"].get("networks") or [])],
        "ups": 0,
    }


def networks_of(state):
    rows = list(state["networks"])
    if mode() == "recon_cross_owner":
        # One id every Environment claims: two owners for one resource.
        rows.append({"network_id": SHARED_NETWORK, "name": "shared", "kind": "private", "cidr": "10.99.0.0/24"})
    return rows


def status_document(state, raw, definition):
    desired = desired_digest(raw, definition)
    persisted = persisted_digest(state)
    machines = []
    for machine in state["machines"]:
        machines.append({
            "machine_id": machine["machine_id"], "name": machine["name"], "state": "ready",
            "profile": machine["profile"], "target": machine["target"], "health": "supervised",
            "requested_capabilities": {"capabilities": ["posix_exec"]},
            "negotiated_capabilities": {"capabilities": ["posix_exec"]},
            "backend": "macos_virtualization_linux",
            "incarnation_id": machine["incarnation_id"],
            "incarnation_generation": machine["incarnation_generation"],
            "docker_context": {"name": "vzr1-ctx-" + machine["machine_id"],
                               "endpoint": "unix:///tmp/" + machine["machine_id"] + ".sock",
                               "engine_id": "eng-" + machine["machine_id"],
                               "owner": {"project_id": state["definition"]["project_id"],
                                         "environment_id": state["environment_id"],
                                         "machine_id": machine["machine_id"]}},
            "docker_context_availability": "persisted_ready_not_live_probed"})
    # What activation was admitted under. `recon_activation_digest_differs`
    # makes it a digest planning never recorded.
    activation = "sha256:" + "a" * 64 if mode() == "recon_activation_digest_differs" else persisted
    if mode() == "recon_half_reconciles" and state.get("stale_activation"):
        activation = state["stale_activation"]
    document = {
        "schema_version": 1, "request_id": "req-" + uuid.uuid4().hex[:12],
        "topology_state_source": "persisted", "definition_path": os.path.join(os.getcwd(), "vz.json"),
        "project_id": definition["project_id"], "project_name": state["definition"]["name"],
        "host": {"os": "macos", "arch": "aarch64"},
        "daemon": {"backend_name": "macos-vz", "version": "0.1.0"},
        "desired_definition_digest": desired, "persisted_definition_digest": persisted,
        "definition_drift": desired != persisted, "selection_source": "workspace",
        "environments": [{"environment_id": state["environment_id"], "name": state["name"], "state": "ready",
                          "definition_digest": activation,
                          "lifecycle_generation": state["lifecycle_generation"],
                          "machines": machines, "networks": networks_of(state),
                          "network_attachments": [], "endpoints": []}]}
    if mode() == "recon_publishes_snapshot_keys":
        document["manifest_digest"] = "sha256:" + "0" * 64
    return document


def refuse(code, message, request_id, key):
    envelope = {"message": message, "request_id": request_id, "idempotency_key": key,
                "details": {"reason": message}}
    if mode() != "recon_no_code":
        envelope["code"] = code
    sys.stderr.write(json.dumps({"schema_version": 1, "error": envelope}) + "\n")
    return 2


def admission_of(state, digest, request_id, key):
    return {"schema_version": 1, "project_id": state["definition"]["project_id"],
            "environment_id": state["environment_id"],
            "machine_ids": sorted(machine["machine_id"] for machine in state["machines"]),
            "definition_digest": digest, "request_id": request_id, "idempotency_key": key,
            "request_hash": "sha256:" + "1" * 64, "workspace_key": "wk-fake", "created_at": 1}


def emit(record):
    sys.stdout.write(json.dumps(record) + "\n")


def progress(state, digest, request_id, key, phase, completion, attempt):
    event = {"schema_version": 1, "sequence": 1 if phase == "admitted" else 2,
             "admission": admission_of(state, digest, request_id, key), "phase": phase,
             "preparation": None,
             "operation": {"operation_id": "op-" + uuid.uuid4().hex[:8], "kind": "up",
                           "generation": state["lifecycle_generation"]},
             "completion": completion}
    if mode() == "recon_nondeterministic":
        # A plan that is not a function of its inputs: two identical requests
        # announce different work.
        event["attempt"] = attempt
    emit({"schema_version": 1, "record_type": "operation_progress", "progress": event})


def accept(state, digest, request_id, key, raw, definition, attempt):
    progress(state, digest, request_id, key, "admitted", None, attempt)
    state["definition"] = definition
    state["raw"] = raw.decode("utf-8")
    state["lifecycle_generation"] += 1
    save_state(state)
    completion = {"admission": admission_of(state, digest, request_id, key), "error": None}
    progress(state, digest, request_id, key, "ready", completion, attempt)
    return 0


def only_resources_changed(before, after):
    """True when the two definitions differ only in Machine `resources`."""
    def stripped(value):
        copy = json.loads(json.dumps(value))
        for machine in copy["environment"]["machines"]:
            machine.pop("resources", None)
        return canonical(copy)
    return stripped(before) == stripped(after) and canonical(before) != canonical(after)


def run_up(argv):
    request_id, key = "req-" + uuid.uuid4().hex[:12], "up-" + uuid.uuid4().hex[:12]
    for index, argument in enumerate(argv):
        if argument == "--request-id" and index + 1 < len(argv):
            request_id = argv[index + 1]
        if argument == "--idempotency-key" and index + 1 < len(argv):
            key = argv[index + 1]
    started = {"schema_version": 1, "record_type": "request_started", "operation": "up_environment",
               "request_id": request_id, "idempotency_key": key}
    raw, definition = read_definition()
    state = load_state()
    if state is None:
        emit(started)
        state = create(raw, definition)
        state["ups"] = 1
        save_state(state)
        return accept(state, desired_digest(raw, definition), request_id, key, raw, definition, 1)
    state["ups"] += 1
    save_state(state)
    attempt = state["ups"]
    if mode() == "recon_nondeterministic":
        # A plan that is not a function of its inputs: two identical requests
        # announce different work. Carried on the first record so it shows up on
        # the refusing path too, which is the one today's runtime takes.
        started["attempt"] = attempt
    emit(started)
    desired, persisted = desired_digest(raw, definition), persisted_digest(state)
    seen = seen_key(key)
    remember_key(key, desired)
    if seen is not None and seen != desired and mode() == "recon_accepts_stale_replay":
        # A stale client's request identity replayed against inputs that moved,
        # accepted as if it were the same request.
        return accept(state, desired, request_id, key, raw, definition, attempt)
    if seen is not None and seen != desired:
        return refuse("state_conflict",
                      "Up idempotency key belongs to a different immutable request; "
                      "persisted digest=%s, requested digest=%s" % (persisted, desired),
                      request_id, key)
    if desired == persisted:
        return accept(state, desired, request_id, key, raw, definition, attempt)
    if mode() == "recon_accepts_immutable":
        # A change to a field the persisted instance is compared against,
        # applied in place instead of refused.
        return accept(state, desired, request_id, key, raw, definition, attempt)
    if mode() == "recon_half_reconciles":
        # Accepts like `recon_reconciles`, but pins the Environment's activation
        # digest to the value it had BEFORE the change. The project then reports
        # the new definition while the Environment still names the old one:
        # exactly the between-versions state a concurrent pair may not leave,
        # and the one the concurrency sub-check's state grading exists to catch.
        if only_resources_changed(state["definition"], definition):
            state["stale_activation"] = persisted
            return accept(state, desired, request_id, key, raw, definition, attempt)
        return refuse("immutable_field_change",
                      "Machine `%s` target differs and cannot be reconciled in place; "
                      "persisted digest=%s, requested digest=%s"
                      % (definition["environment"]["machines"][0]["name"], persisted, desired),
                      request_id, key)
    if mode() == "recon_reconciles":
        if only_resources_changed(state["definition"], definition):
            return accept(state, desired, request_id, key, raw, definition, attempt)
        return refuse("immutable_field_change",
                      "Machine `%s` target differs and cannot be reconciled in place; "
                      "persisted digest=%s, requested digest=%s"
                      % (definition["environment"]["machines"][0]["name"], persisted, desired),
                      request_id, key)
    if mode() == "recon_identity_drift":
        state["environment_id"] = "env_" + uuid.uuid4().hex[:12]
        save_state(state)
    if mode() == "recon_mutates_on_refusal":
        marker = os.path.join(os.path.dirname(os.environ["VZ_RUNTIME_STATE_DB"]), "refused-marker")
        with open(marker, "w", encoding="utf-8") as stream:
            stream.write("a refused Up wrote this\n")
    if mode() == "recon_orphan":
        state["networks"].append({"network_id": "net_orphan_" + uuid.uuid4().hex[:8], "name": "orphan",
                                  "kind": "private", "cidr": "10.77.0.0/24"})
        save_state(state)
    if mode() == "recon_consumes_generation":
        state["lifecycle_generation"] += 1
        save_state(state)
    if mode() == "recon_bumps_incarnation":
        state["machines"][0]["incarnation_generation"] += 1
        save_state(state)
    if mode() == "recon_incidental_refusal":
        return refuse("validation_error", "reconcile failed", request_id, key)
    return refuse("validation_error",
                  "invalid stack spec: project definition drift for `%s`; persisted digest=%s, "
                  "requested digest=%s" % (definition["project_id"], persisted, desired),
                  request_id, key)


def run_status():
    raw, definition = read_definition()
    state = load_state()
    if state is None:
        sys.stderr.write(json.dumps({"schema_version": 1, "error": {
            "code": "daemon_unavailable", "message": "no topology has been created"}}) + "\n")
        return 2
    sys.stdout.write(json.dumps(status_document(state, raw, definition), indent=1) + "\n")
    return 0


def main(argv):
    verb = next((argument for argument in argv if argument in ("up", "status", "stop", "delete", "exec")), None)
    if verb == "up":
        return run_up(argv)
    if verb == "status":
        return run_status()
    sys.stderr.write(json.dumps({"schema_version": 1, "error": {
        "code": "unsupported_operation", "message": "this stand-in serves only up and status"}}) + "\n")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
'''


def build_reconcile_release(root: Path, *, mode_file: Path) -> Path:
    """A minimal release directory whose `bin/vz` is the reconciliation stand-in.

    Not `build_fake_release`: that one compiles a Mach-O daemon stand-in (so a
    test skips where no compiler exists) and rewrites checksums for
    `admit_release_dir`. The criterion-22 tests call the check functions directly
    with a `CheckContext`, so neither is needed and neither should gate them.
    `machine-target-catalog.json` is present because `minimal_definition` reads
    it; `bin/vz-runtimed` is never executed by this stand-in and never spawned.
    """
    (root / "bin").mkdir(parents=True)
    (root / "bin/vz").write_text(RECONCILE_VZ.replace("__MODE_FILE__", json.dumps(str(mode_file))))
    (root / "bin/vz").chmod(0o755)
    (root / "bin/vz-runtimed").write_text("#!/bin/sh\nexit 1\n")
    (root / "bin/vz-runtimed").chmod(0o755)
    (root / "machine-target-catalog.json").write_bytes(
        json.dumps(CATALOG, indent=2, sort_keys=True).encode() + b"\n")
    return root


def establish_reconcile_environments(ctx, names) -> dict:
    """`establish_recovery_environments`' record, over the reconciliation stand-in.

    The real one writes a Machine-local sentinel through `vz exec` and commits
    the definition to git, neither of which this stand-in serves or the
    criterion-22 checks read. What they do read is the isolate layout, the
    Environment/Machine identities and the persisted definition digest, and those
    are recorded here exactly as `establish_recovery_environments` records them.
    """
    import uuid as _uuid

    import developer_environment_checks as _checks

    check = _checks.SubCheck("gate.definition.reconciliation_fencing", "establish_reconcile_environments")
    environments = []
    for name in names:
        definition = _checks.minimal_definition(ctx.release_dir)
        definition["project_id"] = "prj_" + _uuid.uuid4().hex
        data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
        iso = ctx.isolated(name, project_files={"vz.json": data}, provision=True)
        started = ctx.run(check, name + "-up", ["--json", "up"], cwd=iso["project"], env=iso["env"], timeout=120)
        if started.exit_code != 0:
            raise AssertionError(f"{name}: stand-in up exited {started.exit_code}: {started.stderr[:400]!r}")
        payload = _checks.read_status(ctx, check, name, project=iso["project"], env=iso["env"])
        environment = (payload.get("environments") or [{}])[0]
        environments.append({
            "isolate": name, "token": "vzrec-fake", "project_id": payload.get("project_id"),
            "definition_digest": payload.get("persisted_definition_digest"),
            "environment_id": environment.get("environment_id"), "environment_name": environment.get("name"),
            "state": environment.get("state"), "lifecycle_generation": environment.get("lifecycle_generation"),
            "machines": [{"name": machine.get("name"), "machine_id": machine.get("machine_id"),
                          "incarnation_id": machine.get("incarnation_id"),
                          "incarnation_generation": machine.get("incarnation_generation"),
                          "state": machine.get("state"),
                          "docker_context": (machine.get("docker_context") or {}).get("name")}
                         for machine in environment.get("machines") or []]})
    return {"schema_version": 1, "kind": _checks.RECOVERY_RECORD_KIND, "environments": environments}


# ------------------------------------------------------------------ criterion 18
#
# A second, self-contained stand-in release. `FAKE_VZ` deliberately models no
# SecretBinding and no capability negotiation -- neither exists in the shipped
# definition schema or the runtime contract -- so against it the criterion-18
# checks report `not_implemented`, which is the honest verdict and exactly what
# the lane test asserts. These fixtures exist so the checks are FALSIFIABLE
# anyway: they model a runtime that does implement both, and each mode below
# breaks exactly one claim so the check has to notice.
#
# Modes: "" (conformant), leak_status_json / leak_status_human / leak_daemon_log
# / leak_exec_stderr / leak_state_root (the value reaches one swept artifact),
# no_audit (using the binding records nothing), audit_leaks_value (the record
# carries the value), audit_wrong_machine (the record names the sibling),
# sibling_machine_reads (the Machine that declared nothing gets the value too),
# foreign_env_reads (every Environment's Machines get it), cross_env_empty (a
# cross-Environment binding request comes up with an empty result instead of
# failing closed), cross_env_unstructured (it fails without a machine-readable
# code), snapshot_granted (an unadvertised capability is negotiated), and
# snapshot_silent (a requested capability is neither granted nor accounted),
# snapshot_refuse / snapshot_refuse_generic (Up refuses, structured or not),
# drop_request (the Machine's declared request is not projected at all),
# restore_noop (restore reports success and rewinds nothing).

SECRET_VZ = r'''#!/usr/bin/env python3
"""UNIT-TEST-ONLY `vz` stand-in with SecretBinding and capability semantics."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

MODE_FILE = Path(__MODE_FILE__)
MODE = MODE_FILE.read_text().strip() if MODE_FILE.is_file() else ""
RUNTIME = Path(os.environ["VZ_RUNTIME_DATA_DIR"])
TOPOLOGY = RUNTIME / "topology.json"
AUDIT = RUNTIME / "audit.jsonl"
LOG = RUNTIME / "d.log"
SOURCE_ENV = "VZ_GATE_SECRET_VALUE"
SECRET = os.environ.get(SOURCE_ENV, "")
BUSYBOX = str(Path(sys.argv[0]).resolve().parent / "busybox-secret-shim")


def refuse(code, message):
    sys.stderr.write(json.dumps({"schema_version": 1, "error": {"code": code, "message": message}}) + "\n")
    raise SystemExit(1)


def parse(argv):
    verb, machine, environment, tail, want = None, None, None, [], None
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--":
            tail = argv[index + 1:]
            break
        if want:
            if want == "machine":
                machine = item
            elif want == "environment":
                environment = item
            want = None
        elif item in ("--machine", "--environment", "--timeout"):
            want = item[2:] if item != "--timeout" else "timeout"
        elif item in ("up", "status", "exec", "stop", "delete") and verb is None:
            verb = item
        index += 1
    return verb, machine, environment, tail


def machine_id(suffix, name):
    return "mch_%s_%s" % (suffix, name)


def do_up():
    definition = json.loads(Path("vz.json").read_text())
    declaration = definition["environment"]
    machines = declaration["machines"]
    suffix = hashlib.sha256(definition["project_id"].encode()).hexdigest()[:16]
    environment_id = "env_" + suffix
    bindings = list(declaration.get("secret_bindings") or [])
    kept = []
    for binding in bindings:
        foreign = binding.get("from_environment")
        if foreign and foreign != environment_id:
            if MODE == "cross_env_empty":
                continue
            if MODE == "cross_env_unstructured":
                sys.stderr.write("cross-environment secret request denied\n")
                raise SystemExit(1)
            refuse("cross_environment_denied",
                   "SecretBinding %r names Environment %s, which is not this Environment; cross-Environment "
                   "access requires an explicit directional grant" % (binding.get("name"), foreign))
        kept.append(binding)
    rows = []
    for entry in machines:
        requested = list(((entry.get("requested_capabilities") or {}).get("capabilities")) or ["posix_exec"])
        reported = ["posix_exec"] if MODE == "drop_request" else requested
        granted, unsupported = [c for c in reported if c != "snapshot"], {}
        if "snapshot" in reported:
            if "snapshot_granted" in MODE:
                granted = list(reported)
            elif MODE == "snapshot_silent":
                pass
            elif MODE == "snapshot_refuse":
                refuse("unsupported_capability",
                       "this backend cannot provide the snapshot capability this Machine requested")
            elif MODE == "snapshot_refuse_generic":
                sys.stderr.write("up failed\n")
                raise SystemExit(1)
            else:
                unsupported["snapshot"] = ("the macos_virtualization_linux backend implements no Machine "
                                           "snapshot")
        rows.append({"name": entry["name"], "machine_id": machine_id(suffix, entry["name"]),
                     "requested": reported, "granted": granted, "unsupported": unsupported})
    guest = RUNTIME / "guest"
    for entry in machines:
        (guest / entry["name"] / "run").mkdir(parents=True, exist_ok=True)
    for binding in kept:
        holders = [binding["machine"]]
        if MODE == "sibling_machine_reads":
            holders = [entry["name"] for entry in machines]
        for name in holders:
            target = guest / name / binding["target_path"].lstrip("/")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(os.environ.get(binding.get("source_env", ""), ""))
    if MODE == "foreign_env_reads" and SECRET:
        for entry in machines:
            target = guest / entry["name"] / "run/vz-secrets/gate-secret"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(SECRET)
    TOPOLOGY.write_text(json.dumps({
        "project_id": definition["project_id"], "suffix": suffix, "environment_id": environment_id,
        "state": "ready", "machines": rows,
        "bindings": [{"name": b["name"], "machine": b["machine"], "target_path": b["target_path"]} for b in kept]}))
    log = "vz-runtimed stand-in: %s ready with %d Machine(s)\n" % (environment_id, len(rows))
    if MODE == "leak_daemon_log":
        log += "secret material %s\n" % SECRET
    LOG.write_text(log)
    state = "stand-in state store for %s\n" % environment_id
    if MODE == "leak_state_root":
        state += "secret material %s\n" % SECRET
    Path(os.environ["VZ_RUNTIME_STATE_DB"]).write_text(state)
    sys.stdout.write(json.dumps({"schema_version": 1, "progress": {"completion": {}}}) + "\n")
    return 0


def topology():
    if not TOPOLOGY.is_file():
        refuse("daemon_unavailable", "no compatible runtime daemon is listening on the configured socket")
    return json.loads(TOPOLOGY.read_text())


def status_payload(state):
    suffix = state["suffix"]
    machines = []
    for row in state["machines"]:
        negotiated = {"capabilities": row["granted"]}
        if row["unsupported"]:
            negotiated["unsupported"] = row["unsupported"]
        machines.append({"name": row["name"], "machine_id": row["machine_id"], "state": "ready",
                         "profile": "developer", "target": {"os": "linux", "arch": "aarch64", "image": "vz-linux"},
                         "requested_capabilities": {"capabilities": row["requested"]},
                         "negotiated_capabilities": negotiated,
                         "incarnation_id": "inc_%s_%s" % (suffix, row["name"]), "incarnation_generation": 1})
    payload = {"schema_version": 1, "request_id": "req-" + suffix, "topology_state_source": "persisted",
               "definition_path": str(Path.cwd() / "vz.json"), "project_id": state["project_id"],
               "persisted_definition_digest": "sha256:" + suffix * 4,
               "environments": [{"environment_id": state["environment_id"], "name": "default", "state": "ready",
                                 "lifecycle_generation": 1, "machines": machines}]}
    if MODE == "leak_status_json":
        payload["secret_material"] = SECRET
    return payload


def do_status(as_json):
    state = topology()
    if as_json:
        sys.stdout.write(json.dumps(status_payload(state), indent=1) + "\n")
        return 0
    lines = ["Environment %s (default) ready" % state["environment_id"]]
    for row in state["machines"]:
        lines.append("  Machine %s (%s) ready caps=%s" % (row["name"], row["machine_id"], ",".join(row["granted"])))
    for binding in state["bindings"]:
        value = SECRET if MODE == "leak_status_human" else "<redacted>"
        lines.append("  SecretBinding %s -> %s on %s = %s"
                     % (binding["name"], binding["target_path"], binding["machine"], value))
    sys.stdout.write("\n".join(lines) + "\n")
    return 0


def record_use(state, name, binding):
    if MODE == "no_audit":
        return
    rows = {row["name"]: row for row in state["machines"]}
    identity = rows[name]["machine_id"]
    if MODE == "audit_wrong_machine":
        other = [row for row in state["machines"] if row["name"] != name]
        if other:
            identity = other[0]["machine_id"]
    record = {"event": "secret_binding_used", "environment_id": state["environment_id"],
              "machine_id": identity, "machine": name, "binding": binding["name"],
              "binding_id": "sbn_" + hashlib.sha256(binding["name"].encode()).hexdigest()[:16],
              "unix_ns": time.time_ns()}
    if MODE == "audit_leaks_value":
        record["value"] = SECRET
    with open(AUDIT, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(record) + "\n")


def do_exec(name, tail):
    state = topology()
    rows = {row["name"]: row for row in state["machines"]}
    if name is None:
        if len(rows) != 1:
            refuse("validation_error", "Machine selection is ambiguous; specify --machine")
        name = sorted(rows)[0]
    if name not in rows:
        refuse("machine_not_found", "no Machine named %s in this Environment" % name)
    guest = RUNTIME / "guest" / name
    guest.mkdir(parents=True, exist_ok=True)
    rewritten = [item.replace("/bin/busybox", BUSYBOX).replace("/run/", str(guest) + "/run/") for item in tail]
    completed = subprocess.run(rewritten, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    sys.stdout.buffer.write(completed.stdout)
    sys.stderr.buffer.write(completed.stderr)
    if MODE == "leak_exec_stderr":
        sys.stderr.write("secret material %s\n" % SECRET)
    joined = " ".join(tail)
    for binding in state["bindings"]:
        if binding["target_path"] in joined and (guest / binding["target_path"].lstrip("/")).is_file():
            record_use(state, name, binding)
    return completed.returncode


def do_delete():
    if TOPOLOGY.is_file():
        TOPOLOGY.unlink()
    shutil.rmtree(RUNTIME / "guest", ignore_errors=True)
    sys.stdout.write(json.dumps({"schema_version": 1, "deleted": ["default"]}) + "\n")
    return 0


def main():
    argv = sys.argv[1:]
    as_json = "--json" in argv
    verb, machine, _environment, tail = parse(argv)
    if verb == "up":
        return do_up()
    if verb == "status":
        return do_status(as_json)
    if verb == "exec":
        return do_exec(machine, tail)
    if verb == "delete":
        return do_delete()
    if verb == "stop":
        sys.stdout.write(json.dumps({"schema_version": 1, "stopped": ["default"]}) + "\n")
        return 0
    refuse("definition_not_found", "no vz.json project definition found at or above %s" % Path.cwd())
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
'''

# The typed client the snapshot/restore clause is reached through: snapshot and
# restore are not among the five public lifecycle verbs, so the contract puts
# them on the typed API, and criterion 15 already reads the daemon this way.
SECRET_PROBE = r'''#!/usr/bin/env python3
"""UNIT-TEST-ONLY `vz-runtime-probe` stand-in: Machine snapshot and restore."""
import json
import os
from pathlib import Path
import shutil
import sys

MODE_FILE = Path(__MODE_FILE__)
MODE = MODE_FILE.read_text().strip() if MODE_FILE.is_file() else ""


def main():
    argv = sys.argv[1:]
    operation = argv[0] if argv else ""
    options, index = {}, 1
    while index < len(argv):
        if argv[index].startswith("--") and index + 1 < len(argv):
            options[argv[index][2:]] = argv[index + 1]
            index += 2
        else:
            index += 1
    runtime = Path(options.get("socket", "/nonexistent")).parent
    guest = runtime / "guest" / options.get("machine", "machine-0")
    snapshots = runtime / "snapshots"
    if operation == "snapshot":
        identity = "snp_" + os.urandom(8).hex()
        snapshots.mkdir(parents=True, exist_ok=True)
        shutil.copytree(guest, snapshots / identity)
        sys.stdout.write(json.dumps({"schema_version": 1, "kind": "vz-runtime-probe-snapshot",
                                     "snapshot_id": identity}) + "\n")
        return 0
    if operation == "restore":
        identity = options.get("snapshot-id", "")
        source = snapshots / identity
        if not source.is_dir():
            sys.stdout.write(json.dumps({"schema_version": 1, "kind": "vz-runtime-probe-error",
                                         "reason": "snapshot_not_found", "detail": identity}) + "\n")
            return 1
        if "restore_noop" not in MODE:
            shutil.rmtree(guest, ignore_errors=True)
            shutil.copytree(source, guest)
        sys.stdout.write(json.dumps({"schema_version": 1, "kind": "vz-runtime-probe-restore",
                                     "snapshot_id": identity}) + "\n")
        return 0
    sys.stdout.write(json.dumps({"schema_version": 1, "kind": "vz-runtime-probe-error",
                                 "reason": "invalid_arguments", "detail": operation}) + "\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
'''

BUSYBOX_SECRET_SHIM = r'''#!/bin/sh
# The guest BusyBox applets criterion 18's probes use, and nothing else: a probe
# whose applet is missing must fail loudly rather than look like a denial.
applet=$1
shift
case "$applet" in
  sh) exec /bin/sh "$@" ;;
  sha256sum) exec /usr/bin/shasum -a 256 "$@" ;;
  cat) exec /bin/cat "$@" ;;
  *) echo "applet not found: $applet" >&2; exit 127 ;;
esac
'''

# One SecretBinding declaration surface, imposed on a copy of the shipped
# schema. The shipped schema now declares its own; this is the fixture's, kept
# separate so a test can hold the surface fixed while the shipped one changes,
# and so `schema_secrets=False` has something definite to take away.
SECRET_BINDING_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["schema_version", "name", "machine", "target_path", "source_env"],
    "properties": {
        "schema_version": {"const": 1},
        "name": {"$ref": "#/$defs/name"},
        "machine": {"$ref": "#/$defs/name"},
        "target_path": {"type": "string", "minLength": 1, "maxLength": 1024, "pattern": "^/"},
        "source_env": {"type": "string", "minLength": 1, "maxLength": 128},
        "from_environment": {"anyOf": [{"$ref": "#/$defs/id"}, {"type": "null"}]},
    },
}


def build_secret_release(root: Path, *, mode_file: Path) -> Path:
    """A release directory whose `vz` and typed probe model criterion 18.

    Not a signed candidate: these tests call the checks directly rather than
    through lane admission, because the claim under test is what the check
    asserts about a runtime, not how the lane admits a release.
    """
    binaries = root / "bin"
    binaries.mkdir(mode=0o700, parents=True)
    for name, text in (("vz", SECRET_VZ), ("vz-runtime-probe", SECRET_PROBE)):
        path = binaries / name
        path.write_text(text.replace("__MODE_FILE__", json.dumps(str(mode_file))))
        path.chmod(0o755)
    (binaries / "busybox-secret-shim").write_text(BUSYBOX_SECRET_SHIM)
    (binaries / "busybox-secret-shim").chmod(0o755)
    # `ctx.isolated(provision=True)` points VZ at this as the daemon it may
    # spawn; the stand-in CLI never spawns one, so it only has to exist.
    (binaries / "vz-runtimed").write_text("#!/bin/sh\nexit 0\n")
    (binaries / "vz-runtimed").chmod(0o755)
    (root / "machine-target-catalog.json").write_bytes(json.dumps(CATALOG, indent=2, sort_keys=True).encode() + b"\n")
    return root


def build_secret_repo_root(root: Path, *, secret_status: str = "DEV", snapshot_status: str = "PLANNED",
                           schema_secrets: bool = True) -> Path:
    """A repository root carrying the two versioned inputs criterion 18 reads.

    Both are copies of the checked-in files with one field changed, so a test
    that advertises a capability is testing the real matrix shape rather than an
    invented one.

    `schema_secrets=False` REMOVES the SecretBinding surface rather than leaving
    the shipped schema alone. It used to leave it alone, and the docstring said
    "a schema without the SecretBinding surface is the shipped schema exactly" --
    which was true only while the shipped schema declared none. It now declares
    one, so that spelling silently handed every "no way to declare a binding"
    test a schema that declares one, and the test that exists to catch a vacuous
    PASS became the vacuous PASS.
    """
    (root / "config").mkdir(mode=0o700, parents=True)
    (root / "schemas").mkdir(mode=0o700, parents=True)
    matrix = json.loads(read_regular(REPO_ROOT / "config/host-target-capabilities-v0.4.json").decode())
    for pair in matrix["pairs"]:
        pair["topology_capabilities"]["secret_bindings"]["status"] = secret_status
        pair["machine_capabilities"]["snapshot"]["status"] = snapshot_status
    (root / "config/host-target-capabilities-v0.4.json").write_bytes(
        json.dumps(matrix, indent=2, sort_keys=True).encode() + b"\n")
    schema = json.loads(read_regular(REPO_ROOT / "schemas/vz-project-definition-v1.schema.json").decode())
    if schema_secrets:
        schema["$defs"]["secretBinding"] = json.loads(json.dumps(SECRET_BINDING_SCHEMA))
        schema["$defs"]["environment"]["properties"]["secret_bindings"] = {
            "type": "array", "items": {"$ref": "#/$defs/secretBinding"}}
    else:
        schema["$defs"].pop("secretBinding", None)
        schema["$defs"]["environment"]["properties"].pop("secret_bindings", None)
    (root / "schemas/vz-project-definition-v1.schema.json").write_bytes(
        json.dumps(schema, indent=2, sort_keys=True).encode() + b"\n")
    return root


# ---------------------------------------------------------------- criterion 12
#
# The stand-ins criterion 12's check needs, and the deliberately wrong ones that
# make each of its assertions falsifiable offline.
#
# `vz --json exec` is the criterion's whole attribution surface: it opens a
# request, reports the execution ready, streams the guest's bytes, and files a
# terminal receipt, and every one of those records carries a scope naming the
# project, Environment, Machine, request and idempotency key. The sh stand-in
# above models none of that, so this wraps it: anything without `--request-id`
# is handed straight to it, and an execution that carries one is run through it
# and dressed in the record stream the installed CLI emits. Identities are
# derived from the SAME persisted topology the sh stand-in's `status` reads, so
# the check comparing a scope against `vz status` is comparing two spellings of
# one state rather than two copies of one constant.
#
# The wrong modes each break exactly one claim:
#   agent_scope_one_environment  every scope names one Environment, whatever ran
#   agent_scope_machine    a scope's machine_id varies per request instead of
#                          naming the Machine that ran it
#   agent_scope_request    the request id is keyed by Machine, so the twin pair
#                          becomes indistinguishable -- the exact defect the
#                          twin round exists to catch
#   agent_env_constant     the guest environment delivered into the Machine is
#                          not this execution's
#   agent_pty_constant     the same, but only over a terminal
#   agent_cross_token      another execution's token appears in this one's stream
#   agent_exit_status_zero every receipt reports 0 whatever the guest returned
#   agent_cancel_unreported a cancelled execution is filed as a clean completion
#   agent_cancel_machine_wide a deadline takes every execution on the Machine
#   agent_receipt_dropped  one execution files no terminal receipt
#   agent_receipt_missing  no execution files one (the not_implemented path)
#   agent_writer_leaks     a read_only projection is materialised writable and
#                          shared, so a forbidden write lands on the worktree
#   agent_writer_private   a read_write projection is materialised as a private
#                          copy, so an admitted write never reaches the worktree
AGENT_EXEC_CLI = r'''#!/usr/bin/env python3
"""Record-stream stand-in for `vz --json exec` (unit tests only)."""
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

MODE_FILE = __MODE_FILE__
HERE = Path(__file__).resolve().parent
BASE = HERE / "vz-base"
CONSTANT_REQUEST = "req-constant-not-this-execution"
CONSTANT_TOKEN = "tok-constant-not-this-execution"
DEADLINE_POLL_SECONDS = 3.0


def mode():
    try:
        return Path(MODE_FILE).read_text().strip()
    except OSError:
        return ""


def delegate():
    os.execv(str(BASE), [str(BASE)] + sys.argv[1:])


def refuse(code, message, request, idem):
    sys.stderr.write(json.dumps({"schema_version": 1, "record_type": "execution_error",
                                 "error": {"code": code, "message": message, "request_id": request,
                                           "idempotency_key": idem, "details": {}}}) + "\n")
    raise SystemExit(2)


def topology(state):
    rows = {"machines": {}}
    path = state / "topology.json"
    if not path.is_file():
        return None
    for line in path.read_text().splitlines():
        parts = line.split()
        if parts[:1] == ["P"] and len(parts) == 2:
            rows["project_id"] = parts[1]
        elif parts[:1] == ["S"] and len(parts) == 2:
            rows["suffix"] = parts[1]
        elif parts[:1] == ["E"] and len(parts) == 2:
            rows["state"] = parts[1]
        elif parts[:1] == ["M"] and len(parts) >= 3:
            rows["machines"][parts[1]] = {"profile": parts[2], "os": parts[3] if len(parts) > 3 else "linux"}
    return rows


def main():
    argv = sys.argv[1:]
    if "--request-id" not in argv or "exec" not in argv:
        delegate()
    switch = mode()
    json_output = False
    tty = False
    environment = machine = request = idem = None
    timeout = None
    guest_env = {}
    command = []
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--":
            command = argv[index + 1:]
            break
        if item == "--json":
            json_output = True
        elif item in ("--tty", "-t"):
            tty = True
        elif item == "--env":
            key, _, value = argv[index + 1].partition("=")
            guest_env[key] = value
            index += 1
        elif item in ("--environment", "--machine", "--request-id", "--idempotency-key", "--timeout"):
            value = argv[index + 1]
            index += 1
            if item == "--environment":
                environment = value
            elif item == "--machine":
                machine = value
            elif item == "--request-id":
                request = value
            elif item == "--idempotency-key":
                idem = value
            else:
                timeout = int(value)
        index += 1
    if tty and (json_output or not sys.stdin.isatty()):
        # Exactly the installed CLI's own refusal: --tty needs a local terminal
        # and cannot be combined with --json.
        refuse("validation_error", "--tty requires a local terminal and is incompatible with --json",
               request or "", idem or "")
    state = Path(os.environ["VZ_RUNTIME_DATA_DIR"])
    rows = topology(state)
    if rows is None:
        refuse("daemon_unavailable", "no compatible runtime daemon is listening on the configured socket",
               request, idem)
    if machine not in rows["machines"]:
        refuse("validation_error", "no Machine named %s in the selected Environment" % machine, request, idem)
    suffix = rows.get("suffix", "0")
    environment_id = "env_%s" % suffix
    machine_id = "mch_%s_%s" % (suffix, machine)
    scope_request = request
    if switch == "agent_scope_one_environment":
        environment_id = "env_one_for_every_environment"
    if switch == "agent_scope_machine":
        machine_id = "mch_%s_%s" % (suffix, hashlib.sha256((request or "").encode()).hexdigest()[:8])
    if switch == "agent_scope_request":
        # Keyed by Machine rather than by request: two concurrent executions on
        # one Machine become one identity.
        scope_request = "req-%s" % machine
    scope = {"schema_version": 1, "execution_id": "exe-" + uuid.uuid4().hex,
             "request_id": scope_request, "idempotency_key": idem,
             "request_hash": hashlib.sha256(json.dumps(command).encode()).hexdigest(),
             "project_id": rows.get("project_id", "prj_unknown"), "environment_id": environment_id,
             "machine_id": machine_id, "environment_generation": 1,
             "incarnation": {"incarnation_id": "inc_%s_%s" % (suffix, machine), "generation": 1},
             "runtime_identity": {"backend": "macos_virtualization_linux"},
             "definition_digest": "sha256:" + "0" * 64}
    sequence = [0]

    def emit(record):
        if not json_output:
            return
        sequence[0] += 1
        record.setdefault("schema_version", 1)
        record["sequence"] = sequence[0]
        sys.stdout.write(json.dumps(record) + "\n")
        sys.stdout.flush()

    emit({"record_type": "request_started", "operation": "exec_machine",
          "request_id": scope_request, "idempotency_key": idem})
    emit({"record_type": "execution_ready", "scope": scope})

    child_env = dict(os.environ)
    delivered = dict(guest_env)
    if switch == "agent_env_constant" and not tty:
        delivered = {"VZ_AGENT_REQUEST": CONSTANT_REQUEST, "VZ_AGENT_TOKEN": CONSTANT_TOKEN}
    if switch == "agent_pty_constant" and tty:
        delivered = {"VZ_AGENT_REQUEST": CONSTANT_REQUEST, "VZ_AGENT_TOKEN": CONSTANT_TOKEN}
    child_env.update(delivered)

    guest_root = state / "guest" / machine / "vz-storage"
    projected = guest_root / "ro"
    if switch == "agent_writer_leaks" and projected.exists() and not projected.is_symlink():
        # A read_only projection materialised as a writable share of the source.
        # The copy it replaces was made unwritable on purpose, so its own mode
        # has to be lifted before it can be removed.
        for path in sorted(projected.rglob("*"), reverse=True) + [projected]:
            try:
                path.chmod(0o700)
            except OSError:
                pass
        shutil.rmtree(projected, ignore_errors=True)
        if not projected.exists():
            projected.symlink_to(Path(os.getcwd()) / "ro")
    if switch == "agent_writer_private" and (guest_root / "rw").is_symlink():
        # A read_write projection materialised as a private copy.
        source = (guest_root / "rw").resolve()
        (guest_root / "rw").unlink()
        shutil.copytree(source, guest_root / "rw")

    marker = state / ("agent-deadline-" + machine)
    if switch == "agent_cancel_machine_wide" and timeout is not None:
        marker.write_text("deadline\n")
    started = time.time_ns()
    inner = [str(BASE), "exec", "--environment", environment or "default", "--machine", machine, "--", *command]
    cancelled = False
    process = subprocess.Popen(inner, env=child_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        exit_code = process.returncode
    except subprocess.TimeoutExpired:
        cancelled = True
        try:
            os.killpg(process.pid, 9)
        except (ProcessLookupError, PermissionError):
            process.kill()
        stdout, stderr = process.communicate()
        exit_code = None
    if switch == "agent_cancel_machine_wide" and timeout is None:
        deadline = time.monotonic() + DEADLINE_POLL_SECONDS
        while time.monotonic() < deadline and not marker.is_file():
            time.sleep(0.05)
        if marker.is_file():
            cancelled = True
            exit_code = None
    first = state / "agent-first-execution.json"
    if switch == "agent_cross_token":
        if first.is_file():
            stderr += ("leaked %s\n" % json.loads(first.read_text())["token"]).encode()
        else:
            first.write_text(json.dumps({"token": guest_env.get("VZ_AGENT_TOKEN", "")}))
    if switch == "agent_exit_status_zero":
        exit_code = 0
    state_name = "completed"
    failure = None
    if cancelled:
        state_name = "quiesced"
        failure = "execution deadline expired; the guest process group was reaped"
        if switch == "agent_cancel_unreported":
            state_name, failure, exit_code = "completed", None, 0
    if json_output:
        if stdout:
            emit({"record_type": "execution_output", "scope": scope, "stream": "stdout",
                  "base64": base64.b64encode(stdout).decode("ascii")})
        if stderr:
            emit({"record_type": "execution_output", "scope": scope, "stream": "stderr",
                  "base64": base64.b64encode(stderr).decode("ascii")})
    else:
        sys.stdout.buffer.write(stdout)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(stderr)
    receipt = {"schema_version": 1, "scope": scope, "execution_id": scope["execution_id"],
               "state": state_name, "exit_code": exit_code, "failure": failure,
               "started_unix_ns": started, "ended_unix_ns": time.time_ns()}
    drop = switch == "agent_receipt_missing" or (
        switch == "agent_receipt_dropped" and (request or "").endswith("_writer_ro"))
    if not drop:
        emit({"record_type": "execution_receipt", "replayed": False, "receipt": receipt})
    return 5 if exit_code is None else exit_code


if __name__ == "__main__":
    raise SystemExit(main())
'''


# The tamper driver: it runs the real checked-in driver and then breaks exactly
# one property of the transcript. Those properties are the driver's own -- which
# steps ran, in what order, against which binding, whether a round's steps
# overlapped -- so no CLI stand-in can falsify them, and an assertion nothing
# can falsify is an assertion that is not being made.
AGENT_TAMPER_DRIVER = r'''#!/usr/bin/env python3
"""Run the real agent driver, then break one property of its transcript."""
import json
from pathlib import Path
import subprocess
import sys

MODE_FILE = __MODE_FILE__
REAL = __REAL_DRIVER__


def mode():
    try:
        return Path(MODE_FILE).read_text().strip()
    except OSError:
        return ""


def main():
    argv = sys.argv[1:]
    completed = subprocess.run([sys.executable, "-B", REAL, *argv], check=False)
    if completed.returncode != 0:
        return completed.returncode
    transcript_path = Path(argv[argv.index("--transcript") + 1])
    transcript = json.loads(transcript_path.read_text())
    switch = mode()
    steps = transcript["steps"]
    ran = [row for row in steps if not row.get("skipped")]
    if switch == "agent_tamper_digest":
        transcript["schedule_sha256"] = "0" * 64
    elif switch == "agent_tamper_order":
        transcript["steps"] = list(reversed(steps))
    elif switch == "agent_tamper_argv":
        row = ran[0]
        row["argv"][row["argv"].index("--machine") + 1] = "machine-somewhere-else"
    elif switch == "agent_tamper_cwd":
        ran[0]["intent"]["cwd"] = "/not/this/worker/project"
    elif switch == "agent_tamper_identity":
        ran[1]["intent"]["request_id"] = ran[0]["intent"]["request_id"]
        ran[1]["intent"]["idempotency_key"] = ran[0]["intent"]["idempotency_key"]
        ran[1]["intent"]["token"] = ran[0]["intent"]["token"]
    elif switch == "agent_tamper_execution_id":
        first = None
        for row in ran:
            for record in row.get("records") or []:
                receipt = record.get("receipt")
                if not isinstance(receipt, dict):
                    continue
                if first is None:
                    first = receipt.get("execution_id")
                else:
                    receipt["execution_id"] = first
                    if isinstance(receipt.get("scope"), dict):
                        receipt["scope"]["execution_id"] = first
    elif switch == "agent_tamper_overlap":
        # Rewrite one round's spans so its steps ran strictly one after another:
        # a barrier that did not hold looks exactly like this.
        moment = 1_000_000_000
        for row in ran:
            if row.get("round") != 0:
                continue
            row["started_unix_ns"], row["ended_unix_ns"] = moment, moment + 10
            moment += 100
    elif switch == "agent_tamper_terminal":
        for row in ran:
            if row.get("channel") == "pty":
                row["terminal"] = False
    transcript_path.write_text(json.dumps(transcript, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''


def build_tamper_driver(destination: Path, *, mode_file: Path, real_driver: Path = None) -> Path:
    """The tamper driver, pointed at the checked-in one it wraps."""
    real_driver = real_driver or (REPO_ROOT / "tests/fixtures/vz-0.4/agent-driver/driver.py")
    destination.write_text(AGENT_TAMPER_DRIVER
                           .replace("__MODE_FILE__", json.dumps(str(mode_file)))
                           .replace("__REAL_DRIVER__", json.dumps(str(real_driver))))
    destination.chmod(0o755)
    return destination


def seal_fake_release(root: Path) -> Path:
    """Re-derive the release manifest and checksums after an overlay."""
    manifest = json.loads(read_regular(root / "release-manifest.json"))
    for relative in ("bin/vz", "bin/vz-runtimed", "bin/vz-runtime-probe"):
        manifest["components"][relative]["signed_sha256"] = digest_file(root / relative)
    components = manifest["components"]
    manifest["normalized_content_sha256"] = candidate.line_digest(
        sorted([p, c["unsigned_sha256"]] for p, c in components.items()))
    manifest["signed_content_sha256"] = candidate.line_digest(
        sorted([p, c["signed_sha256"]] for p, c in components.items()))
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    (root / "release-manifest.json").write_bytes(manifest_bytes)
    (root / "release-manifest.sha256").write_bytes(
        f"{sha256_bytes(manifest_bytes)}  release-manifest.json\n".encode())
    (root / "checksums.sha256").unlink()
    rows = [f"{digest_file(path)}  {path.relative_to(root).as_posix()}\n"
            for path in sorted(p for p in root.rglob("*") if p.is_file())]
    (root / "checksums.sha256").write_bytes("".join(rows).encode())
    for path in root.rglob("*"):
        path.chmod(stat.S_IMODE(path.lstat().st_mode) & ~0o222)
    return root


def build_agent_fake_release(root: Path, *, mode_file: Path, snapshot_file: Path = None) -> Path:
    """`build_fake_release`, with a `vz` that speaks the exec record stream.

    The sh stand-in is kept, unchanged, as `bin/vz-base`: every verb but a
    request-identified `exec` is handed straight to it, so the topology,
    identities and storage model criterion 12 reads are the same ones every
    other check reads.
    """
    build_fake_release(root, mode_file=mode_file, snapshot_file=snapshot_file)
    fixtures.make_writable(root)
    shutil.move(str(root / "bin/vz"), str(root / "bin/vz-base"))
    (root / "bin/vz-base").chmod(0o755)
    (root / "bin/vz").write_text(AGENT_EXEC_CLI.replace("__MODE_FILE__", json.dumps(str(mode_file))))
    (root / "bin/vz").chmod(0o755)
    return seal_fake_release(root)


# ---------------------------------------------------------------- criterion 23
#
# The stand-ins criterion 23's check needs, and the deliberately wrong ones that
# make each of its assertions falsifiable offline.
#
# Forking is physical: a disk is cloned copy-on-write, a Docker image store
# arrives with it, and the cost of both is read off the VOLUME rather than off
# the file. None of that can be modelled by a shell script that prints a status
# document, so this stand-in does the real filesystem work — it writes a real
# 32 MiB Machine disk, clones it with `cp -c` (clonefile(2)), and keeps each
# engine's image and volume list INSIDE that disk's header, so a fork inherits
# its parent's images for the same reason the product does: the bytes came with
# the disk.
#
# That is what makes the wrong modes worth having. `fork_deep_copy` clones with
# `cp` instead of `cp -c` and nothing else changes: same file, same size, same
# per-file allocated size, same image store — and the check must still catch it,
# because the volume lost 32 MiB. `fork_sparse_stub` is its mirror: a disk that
# costs nothing and contains nothing, which a free-space measurement alone would
# happily accept.
#
# One mode breaks exactly one claim:
#   fork_label_ignored      the default label is the raw branch, not the
#                           normalised one the check computed in advance
#   fork_lineage_absent     the fork reports no `fork` object
#   fork_parent_lineage     the declared Machine reports one
#   fork_shared_machine_id  the fork carries its parent's machine_id
#   fork_shared_incarnation the fork carries its parent's incarnation_id
#   fork_shared_context     the fork carries its parent's Docker context,
#                           endpoint and engine_id
#   fork_reincarnates_parent  the fork re-mints the PARENT's incarnation
#   fork_not_ready          the fork never reaches ready
#   fork_mints_endpoint     the fork republishes its parent's declared endpoint
#   fork_same_address       the fork answers on its parent's fabric address
#   fork_same_mac           the fork carries its parent's MAC
#   fork_other_subnet       the fork lands on a different /24
#   fork_no_seed            the fork gets no Docker data disk at all
#   fork_stub_disk          the fork's disk is smaller than its parent's
#   fork_sparse_stub        the fork's disk is the parent's size but allocates
#                           nothing, so it cannot hold the parent's bytes
#   fork_deep_copy          the clone is a byte copy, so the volume pays for it
#   fork_slow               the fork costs a cold boot (reported, never a failure)
#   fork_cold_image_store   the fork's image store comes up empty
#   fork_pulls              the fork's engine pulled the images it holds
#   fork_shared_engine      parent and fork are one engine wearing two names
#   fork_wipes_parent_sentinel  the fork takes the parent's Machine-local state
#   fork_pruned_by_up       reconcile removes the Machines it does not declare
#   fork_reidentified_by_up reconcile keeps the fork but re-mints its identity
#   fork_shared_guest       two forks share one guest, so their state is one
#   fork_exec_falls_back    ambiguous `exec` picks the first Machine and runs
#   fork_ambiguous_unlisted ambiguous `exec` refuses without naming candidates
#   fork_delete_declared    `delete --machine <declared>` is honoured
#   fork_delete_blanket     `delete --machine` refuses without resolving, so an
#                           unknown label answers exactly like a known fork
#   fork_delete_generic     the fork refusal carries no code and names nothing
#   fork_delete_reclaims    a spelling of the default, kept so the test that
#                           named it still reads: reclaiming one fork IS the
#                           default answer now
#   fork_delete_leaks       `delete --machine` reports success and leaves the
#                           fork's disk behind
#   fork_env_delete_leaks   deleting the Environment leaves a Machine disk
#   fork_context_unavailable  the Machine reports a context that its own
#                           `docker_context_availability` says is unusable
#   fork_context_unresolvable  `vz status` names a context the client cannot
#                           resolve out of the Machine's config directory --
#                           the exact shape of the first real-Machine run, where
#                           the check searched the lane's VZ_DOCKER_CONFIG
#                           instead of the Machine's own private config
#   fork_engine_never_ready the context resolves but no Engine ever answers
#                           `docker version`, so the readiness poll expires

# One Machine disk, big enough that a byte copy of it is unmistakable against
# the noise of a `statvfs` window and small enough to write in milliseconds.
#
# 128 MiB, raised from 32 MiB after the first full-suite run: ambient volume
# movement on this host reached ±25 MB, which at 32 MiB is the same order as the
# signal, so the cost assertion decided nothing and failed at random. At 128 MiB
# a deep copy is five times the noise floor and a clone is a thousandth of it.
FORK_DISK_BYTES = 128 * 1024 * 1024
# The disk's first block is its engine's image and volume list. Rewritten in
# place, so recording an image does not change what the file allocates.
FORK_DISK_HEADER = 4096

FORK_BUSYBOX = r'''#!/bin/sh
# Stand-in for the guest BusyBox, for criterion 23 only.
#
# The fork check's scripts are rewritten by the `vz` stand-in so that /proc,
# /sys and /run point into the Machine's own guest tree before they run, which
# leaves exactly one applet that cannot be a file read: `ip`, whose output is
# generated per Machine and staged beside that tree. Everything else is the
# host's own tool, because a Machine-local file read is what the check is
# actually making a claim about.
applet=$1
shift
case "$applet" in
  ip) cat "$VZ_FORK_GUEST/ip-addr.txt"; exit 0 ;;
  sh) exec /bin/sh "$@" ;;
  *) exec "$applet" "$@" ;;
esac
'''

FORK_DOCKER = r'''#!/usr/bin/env python3
"""UNIT-TEST-ONLY `docker` stand-in whose state lives on the Machine's disk.

The point of the fork is that a Docker image store arrives WITH the cloned
disk, so this stand-in keeps each engine's images and volumes in the first
block of that disk's image file and reads them back from wherever `--context`
points. A fork therefore inherits its parent's images because the bytes were
copied, not because anything told it to, and an engine that shared its parent's
disk would be caught by the same read.
"""
import json
import hashlib
import os
from pathlib import Path
import sys


def fail(message):
    sys.stderr.write("docker stand-in: %s\n" % message)
    raise SystemExit(1)


def parse(argv):
    config, context, rest = None, None, []
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--config" and index + 1 < len(argv):
            config = argv[index + 1]
            index += 2
        elif item == "--context" and index + 1 < len(argv):
            context = argv[index + 1]
            index += 2
        else:
            rest = argv[index:]
            break
    return config, context, rest


def engine(config, context):
    path = Path(config) / "vzfork-contexts" / (context + ".json")
    if not path.is_file():
        fail("context %r is not known to this client config" % context)
    return json.loads(path.read_text())


def read_disk(entry):
    disk = Path(entry["disk"])
    if not disk.is_file():
        fail("engine %s has no data disk at %s" % (entry["engine_id"], disk))
    with open(disk, "rb") as stream:
        header = stream.read(__HEADER__)
    text = header.split(b"\0", 1)[0].decode("utf-8").strip()
    return json.loads(text) if text else {"images": {}, "volumes": []}


def write_disk(entry, state):
    disk = Path(entry["disk"])
    payload = json.dumps(state, sort_keys=True).encode("utf-8")
    if len(payload) >= __HEADER__:
        fail("engine state outgrew the disk header")
    with open(disk, "r+b") as stream:
        stream.write(payload + b"\0" * (__HEADER__ - len(payload)))


def events(entry):
    path = Path(entry["events"])
    if not path.is_file():
        return []
    return [line.split(" ", 2) for line in path.read_text().splitlines() if line.strip()]


def main(argv):
    config, context, rest = parse(argv)
    if config is None or context is None:
        fail("every invocation is scoped by --config and --context")
    entry = engine(config, context)
    if rest[:2] == ["context", "inspect"]:
        # A pure client-side read of the config directory. It does not touch the
        # engine, which is what lets the check tell "the context is not where I
        # looked" from "the engine is not answering".
        sys.stdout.write(entry["name"] + "\n")
        return 0
    if rest[:1] == ["version"]:
        # The server half is only reported by an engine that is actually
        # serving; `docker version` against a dead endpoint exits non-zero with
        # no server line, which is the condition poll.docker.engine_ready waits
        # on.
        if not entry.get("serving"):
            sys.stderr.write("Cannot connect to the Docker daemon. Is the docker daemon running?\n")
            return 1
        sys.stdout.write("27.4.0\n")
        return 0
    if rest[:2] == ["image", "import"]:
        source, tag = rest[2], rest[3]
        digest = "sha256:" + hashlib.sha256(Path(source).read_bytes()).hexdigest()
        state = read_disk(entry)
        state.setdefault("images", {})[tag] = digest
        write_disk(entry, state)
        sys.stdout.write(digest + "\n")
        return 0
    if rest[:2] == ["image", "inspect"]:
        tag = rest[-1]
        digest = read_disk(entry).get("images", {}).get(tag)
        if digest is None:
            sys.stderr.write("Error: No such image: %s\n" % tag)
            return 1
        sys.stdout.write(digest + "\n")
        return 0
    if rest[:2] == ["image", "ls"]:
        for digest in sorted(set(read_disk(entry).get("images", {}).values())):
            sys.stdout.write(digest + "\n")
        return 0
    if rest[:1] == ["events"]:
        wanted = [item.split("=", 1)[1] for item in rest if item.startswith("event=")]
        for row in events(entry):
            if len(row) >= 3 and row[1] in wanted:
                sys.stdout.write(row[2] + "\n")
        return 0
    if rest[:2] == ["volume", "create"]:
        state = read_disk(entry)
        state.setdefault("volumes", [])
        if rest[2] not in state["volumes"]:
            state["volumes"].append(rest[2])
        write_disk(entry, state)
        sys.stdout.write(rest[2] + "\n")
        return 0
    if rest[:2] == ["volume", "ls"]:
        for name in read_disk(entry).get("volumes", []):
            sys.stdout.write(name + "\n")
        return 0
    fail("unsupported invocation %r" % (rest,))
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
'''

FORK_VZ = r'''#!/usr/bin/env python3
"""UNIT-TEST-ONLY `vz` stand-in with Machine-fork semantics.

Real filesystem work, because criterion 23's claims are physical: a 32 MiB
Machine disk per Machine, cloned with `cp -c` so the volume pays metadata
rather than bytes, one guest tree per Machine so Machine-local state is
genuinely per Machine, and a derived fabric identity per (Environment, Machine)
so a fork lands on its own address of its parent's subnet by construction.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

MODE_FILE = Path(__MODE_FILE__)
MODE = MODE_FILE.read_text().strip() if MODE_FILE.is_file() else ""
HERE = Path(sys.argv[0]).resolve().parent
RUNTIME = Path(os.environ["VZ_RUNTIME_DATA_DIR"])
TOPOLOGY = RUNTIME / "topology.json"
DOCKER_CONFIG = Path(os.environ["VZ_DOCKER_CONFIG"])
DISK_BYTES = __DISK_BYTES__
HEADER = __HEADER__
COLD_SECONDS = 0.4
FORK_SECONDS = 0.02
MAX_LABEL = 64


def refuse(code, message):
    sys.stderr.write(json.dumps({"schema_version": 1,
                                 "error": {"code": code, "message": message}}) + "\n")
    raise SystemExit(1)


def digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def label_from_branch(branch):
    label = ""
    for character in branch:
        if character.isascii() and (character.isalnum() or character in "._-"):
            label += character
        elif not label.endswith("-"):
            label += "-"
        if len(label) >= MAX_LABEL:
            break
    start, end = 0, len(label)
    while start < end and not (label[start].isascii() and label[start].isalnum()):
        start += 1
    while end > start and not (label[end - 1].isascii() and label[end - 1].isalnum()):
        end -= 1
    return label[start:end][:MAX_LABEL]


def parse(argv):
    options = {"json": False, "verb": None, "tail": [], "fork_from": None, "fork_as": None,
               "machine": None, "environment": None}
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--":
            options["tail"] = argv[index + 1:]
            break
        if item == "--json":
            options["json"] = True
        elif item in ("--fork-from", "--as", "--machine", "--environment", "--timeout",
                      "--request-id", "--idempotency-key") and index + 1 < len(argv):
            key = {"--fork-from": "fork_from", "--as": "fork_as", "--machine": "machine",
                   "--environment": "environment"}.get(item)
            if key:
                options[key] = argv[index + 1]
            index += 1
        elif item in ("up", "status", "exec", "stop", "delete") and options["verb"] is None:
            options["verb"] = item
        index += 1
    return options


def load():
    if not TOPOLOGY.is_file():
        refuse("daemon_unavailable",
               "no compatible runtime daemon is listening on the configured socket")
    return json.loads(TOPOLOGY.read_text())


def save(state):
    TOPOLOGY.write_text(json.dumps(state, indent=1, sort_keys=True))


def fabric(environment_id, machine_id, subnet_salt=""):
    net = int(digest(environment_id + subnet_salt)[:8], 16)
    host = int(digest(machine_id)[:8], 16)
    octet_a, octet_b = net // 256 % 254 + 1, net % 254 + 1
    octet_c = host % 200 + 2
    return ("10.%d.%d.%d" % (octet_a, octet_b, octet_c),
            "02:00:00:%02x:%02x:%02x" % (octet_a, octet_b, octet_c),
            "192.168.64.%d" % (host % 200 + 20))


def stage_guest(machine):
    """One Machine's own /proc, /sys and /run, as files the check's scripts read."""
    guest = Path(machine["guest"])
    (guest / "run").mkdir(parents=True, exist_ok=True)
    (guest / "proc" / "net").mkdir(parents=True, exist_ok=True)
    interface = guest / "sys" / "class" / "net" / "enp0s5" / "statistics"
    interface.mkdir(parents=True, exist_ok=True)
    (guest / "proc" / "cmdline").write_text(
        "console=hvc0 vz.net.0=%s,%s/24\n" % (machine["mac"], machine["address"]))
    (guest / "proc" / "net" / "arp").write_text(
        "IP address       HW type     Flags       HW address            Mask     Device\n")
    (interface.parent / "address").write_text(machine["mac"] + "\n")
    (interface.parent / "operstate").write_text("up\n")
    (interface.parent / "carrier").write_text("1\n")
    for counter in ("rx_packets", "tx_packets"):
        (interface / counter).write_text("0\n")
    (guest / "ip-addr.txt").write_text(
        "2: eth0    inet %s/24 brd 192.168.64.255 scope global eth0\n"
        "3: enp0s5    inet %s/24 brd 10.255.255.255 scope global enp0s5\n"
        % (machine["nat"], machine["address"]))


def store_root(machine):
    """One Machine's private runtime store.

    Keyed by the Machine's NAME rather than its id, only so that
    `fork_shared_machine_id` -- which hands a fork its parent's identity -- still
    produces two stores to compare. Keying it on the id would make that mode
    collide on the disk path and fail for a reason that is about the fixture
    rather than about the claim it exists to break.
    """
    return RUNTIME / "store" / digest(machine["name"])[:16]


def disk_path(machine):
    return (store_root(machine) / "data" / "docker-machines"
            / digest(machine["stack_id"]) / "data.img")


def create_disk(machine):
    """A Machine disk with real bytes, so cloning it is measurable.

    fsync'd before returning, and this is load-bearing rather than tidy. The
    check measures the VOLUME's free space across the fork, and unflushed writes
    are not charged to the volume until writeback runs -- so a disk written here
    and left dirty is charged to whatever window happens to be open when the
    kernel gets round to it, which is the fork's. Leaving 64 MiB of these in
    flight made the fork appear to cost between 24 MB and 280 MB of a 32 MiB
    parent. Flushing here charges each disk to its own creation, where it
    belongs.
    """
    path = disk_path(machine)
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk = os.urandom(1024 * 1024)
    with open(path, "wb") as stream:
        stream.write(b"\0" * HEADER)
        written = HEADER
        while written < DISK_BYTES:
            stream.write(chunk[:min(len(chunk), DISK_BYTES - written)])
            written += len(chunk)
        stream.flush()
        os.fsync(stream.fileno())
    write_engine_state(machine, {"images": {}, "volumes": []})


def read_engine_state(machine):
    with open(disk_path(machine), "rb") as stream:
        header = stream.read(HEADER)
    text = header.split(b"\0", 1)[0].decode("utf-8").strip()
    return json.loads(text) if text else {"images": {}, "volumes": []}


def write_engine_state(machine, state):
    payload = json.dumps(state, sort_keys=True).encode("utf-8")
    with open(disk_path(machine), "r+b") as stream:
        stream.write(payload + b"\0" * (HEADER - len(payload)))
        stream.flush()
        os.fsync(stream.fileno())


def clone_disk(parent, machine):
    """Copy-on-write, unless a mode says otherwise."""
    source, destination = disk_path(parent), disk_path(machine)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if MODE == "fork_no_seed":
        return
    if MODE == "fork_stub_disk":
        with open(destination, "wb") as stream:
            stream.write(b"\0" * HEADER)
        return
    if MODE == "fork_sparse_stub":
        with open(destination, "wb") as stream:
            stream.write(b"\0" * HEADER)
            stream.truncate(DISK_BYTES)
        return
    argv = ["/bin/cp", "-c", str(source), str(destination)]
    if MODE == "fork_deep_copy":
        argv = ["/bin/cp", str(source), str(destination)]
    subprocess.run(argv, check=True)
    if MODE == "fork_cold_image_store":
        write_engine_state(machine, {"images": {}, "volumes": []})


def publish_context(machine, share_with=None):
    """Write the context into the MACHINE's own private client config.

    Modelled where the product puts it, which is not where the lane exports
    `VZ_DOCKER_CONFIG`: vz reads that directory only for the host's CLI plugins
    and then mints a private, Machine-owned config under the Machine's runtime
    store. A check that pointed `--config` at the lane's directory would find no
    context here either, which is what the first run against real Machines hit.
    """
    directory = Path(machine["context"]["config_dir"]) / "vzfork-contexts"
    if MODE == "fork_context_unresolvable":
        # Named by `vz status`, never written for the client: the shape of a
        # context that exists as a record and not as something addressable.
        return
    directory.mkdir(parents=True, exist_ok=True)
    holder = share_with or machine
    (directory / (machine["context"]["name"] + ".json")).write_text(json.dumps({
        "engine_id": machine["context"]["engine_id"],
        "name": machine["context"]["name"],
        "serving": MODE != "fork_engine_never_ready",
        "disk": str(disk_path(holder)),
        "events": str(RUNTIME / "events" / (machine["context"]["engine_id"] + ".log"))}))
    (RUNTIME / "events").mkdir(parents=True, exist_ok=True)


def mint(state, name, fork_origin=None, parent=None):
    suffix = digest(state["environment_id"] + name)[:16]
    machine_id = "mch_" + suffix
    if fork_origin and MODE == "fork_shared_machine_id":
        machine_id = parent["machine_id"]
    incarnation = "inc_" + digest(machine_id + str(time.time_ns()))[:16]
    if fork_origin and MODE == "fork_shared_incarnation":
        incarnation = parent["incarnation_id"]
    address, mac, nat = fabric(state["environment_id"], machine_id)
    if fork_origin and MODE == "fork_same_address":
        address = fabric(state["environment_id"], parent["machine_id"])[0]
    if fork_origin and MODE == "fork_same_mac":
        mac = parent["mac"]
    if fork_origin and MODE == "fork_other_subnet":
        address = fabric(state["environment_id"], machine_id, "-elsewhere")[0]
    # The private config directory is per Machine and lives under that Machine's
    # own runtime store, exactly as `ManagedMachineDockerConfig` puts it, and
    # deliberately NOT under VZ_DOCKER_CONFIG.
    context = {"name": "vz-" + suffix, "endpoint": "unix:///tmp/vzfork-" + suffix + ".sock",
               "engine_id": "eng_" + suffix,
               "config_dir": str(RUNTIME / "topology-machines" / suffix / "data" / "docker-client")}
    if fork_origin and MODE == "fork_shared_context":
        context = dict(parent["context"])
    guest = str(RUNTIME / "guest" / machine_id)
    return {"name": name, "machine_id": machine_id, "incarnation_id": incarnation,
            "context": context, "address": address, "mac": mac, "nat": nat,
            "state": "creating" if (fork_origin and MODE == "fork_not_ready") else "ready",
            "fork": fork_origin, "guest": guest,
            "stack_id": "stk_" + digest(state["project_id"] + state["environment_id"] + machine_id)[:24]}


def create(options):
    definition = json.loads(Path("vz.json").read_text())
    declared = definition["environment"]["machines"][0]
    project_id = definition["project_id"]
    state = {"project_id": project_id, "environment_id": "env_" + digest(project_id)[:16],
             "definition_digest": "sha256:" + digest(json.dumps(definition, sort_keys=True)),
             "networks": definition["environment"].get("networks") or [],
             "endpoints": definition["environment"].get("endpoints") or [], "machines": []}
    machine = mint(state, declared["name"])
    state["machines"].append(machine)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    stage_guest(machine)
    create_disk(machine)
    publish_context(machine)
    save(state)
    time.sleep(COLD_SECONDS)
    return 0


def do_fork(state, options):
    parents = [row for row in state["machines"]
               if row["name"] == options["fork_from"] or row["machine_id"] == options["fork_from"]]
    if not parents:
        refuse("not_found", "no Machine named `%s` to fork from" % options["fork_from"])
    parent = parents[0]
    # `fork_parent_lineage` gives the DECLARED Machine a bogus lineage record so
    # that the "a declared Machine reports none" assertion can fail. It must not
    # also make this Machine unforkable, or that one mode would break two
    # unrelated claims and stop saying which one it caught.
    if parent.get("fork") and parent["fork"].get("label") != "self":
        refuse("unsupported_operation",
               "a fork is seeded from a declared Machine, never from another fork")
    if options["fork_as"]:
        if "@" not in options["fork_as"]:
            refuse("validation_error", "`--as` names a fork address `<machine>@<label>`")
        machine_name, _, label = options["fork_as"].partition("@")
        if machine_name != parent["name"]:
            refuse("validation_error", "`--as %s` does not name a fork of `%s`"
                   % (options["fork_as"], parent["name"]))
    else:
        head = subprocess.run(["/usr/bin/git", "symbolic-ref", "--quiet", "--short", "HEAD"],
                              capture_output=True, text=True, check=False)
        branch = head.stdout.strip()
        if not branch:
            refuse("validation_error", "this worktree has no checked-out branch to name the fork after")
        label = branch if MODE == "fork_label_ignored" else label_from_branch(branch)
    name = "%s@%s" % (parent["name"], label)
    if any(row["name"] == name for row in state["machines"]):
        time.sleep(FORK_SECONDS)
        return 0
    origin = None if MODE == "fork_lineage_absent" else {
        "parent_machine_id": parent["machine_id"], "parent_name": parent["name"], "label": label}
    machine = mint(state, name, fork_origin=origin or {"parent_machine_id": parent["machine_id"],
                                                       "parent_name": parent["name"], "label": label},
                   parent=parent)
    machine["fork"] = origin
    state["machines"].append(machine)
    if MODE == "fork_parent_lineage":
        parent["fork"] = {"parent_machine_id": parent["machine_id"],
                          "parent_name": parent["name"], "label": "self"}
    if MODE == "fork_reincarnates_parent":
        parent["incarnation_id"] = "inc_" + digest(parent["machine_id"] + str(time.time_ns()))[:16]
    if MODE == "fork_wipes_parent_sentinel":
        shutil.rmtree(Path(parent["guest"]) / "run", ignore_errors=True)
        (Path(parent["guest"]) / "run").mkdir(parents=True, exist_ok=True)
    if MODE == "fork_mints_endpoint":
        for endpoint in list(state["endpoints"]):
            state["endpoints"].append(dict(endpoint, machine=machine["name"]))
    stage_guest(machine)
    if MODE == "fork_shared_guest":
        # Only the mutable tree, so this mode breaks the isolation claim and
        # leaves the fabric identity it stages above alone.
        run = Path(machine["guest"]) / "run"
        shutil.rmtree(run, ignore_errors=True)
        run.symlink_to(Path(parent["guest"]) / "run")
    clone_disk(parent, machine)
    publish_context(machine, share_with=parent if MODE == "fork_shared_engine" else None)
    if MODE == "fork_pulls":
        log = RUNTIME / "events" / (machine["context"]["engine_id"] + ".log")
        log.parent.mkdir(parents=True, exist_ok=True)
        state_on_disk = read_engine_state(machine) if disk_path(machine).is_file() else {"images": {}}
        for image in sorted(set(state_on_disk.get("images", {}).values())):
            log.write_text("%d pull %s\n" % (int(time.time()), image))
    save(state)
    time.sleep(COLD_SECONDS if MODE == "fork_slow" else FORK_SECONDS)
    return 0


def reconcile(state):
    if MODE == "fork_pruned_by_up":
        for machine in [row for row in state["machines"] if row.get("fork")]:
            shutil.rmtree(store_root(machine), ignore_errors=True)
        state["machines"] = [row for row in state["machines"] if not row.get("fork")]
    elif MODE == "fork_reidentified_by_up":
        for machine in state["machines"]:
            if machine.get("fork"):
                machine["machine_id"] = "mch_" + digest(machine["name"] + str(time.time_ns()))[:16]
    save(state)
    time.sleep(FORK_SECONDS)
    return 0


def do_up(options):
    if not TOPOLOGY.is_file():
        if options["fork_from"]:
            refuse("not_found", "a fork needs an Environment to fork inside; run `vz up` once first")
        code = create(options)
    else:
        state = load()
        code = do_fork(state, options) if options["fork_from"] else reconcile(state)
    sys.stdout.write(json.dumps({"schema_version": 1, "progress": {"completion": {}}}) + "\n")
    return code


def status_document(state):
    network = (state["networks"] or [{}])[0]
    network_id = "net_" + digest(state["environment_id"] + (network.get("name") or ""))[:16]
    machines = []
    for row in state["machines"]:
        entry = {"name": row["name"], "machine_id": row["machine_id"], "state": row["state"],
                 "profile": "developer",
                 "target": {"os": "linux", "arch": "aarch64", "image": "vz-linux"},
                 "requested_capabilities": {"capabilities": ["posix_exec"]},
                 "negotiated_capabilities": {"capabilities": ["posix_exec"]},
                 "health": "supervised", "incarnation_id": row["incarnation_id"],
                 "incarnation_generation": 1,
                 "docker_context": dict(row["context"]),
                 "docker_context_availability":
                     "persisted_unavailable" if MODE == "fork_context_unavailable"
                     else "persisted_ready_not_live_probed"}
        if row.get("fork"):
            entry["fork"] = dict(row["fork"])
        machines.append(entry)
    endpoints = []
    for endpoint in state["endpoints"]:
        holder = next((row for row in state["machines"] if row["name"] == endpoint["machine"]), None)
        if holder is None:
            continue
        endpoints.append({"name": endpoint["name"], "machine_id": holder["machine_id"],
                          "network_id": network_id, "protocol": endpoint["protocol"],
                          "port": endpoint["port"]})
    return {"schema_version": 1, "request_id": "req-" + digest(state["environment_id"])[:16],
            "topology_state_source": "persisted", "definition_path": str(Path.cwd() / "vz.json"),
            "project_id": state["project_id"],
            "persisted_definition_digest": state["definition_digest"],
            "environments": [{"environment_id": state["environment_id"], "name": "default",
                              "state": "ready", "lifecycle_generation": 1, "machines": machines,
                              "networks": ([{"network_id": network_id, "name": network.get("name"),
                                             "kind": network.get("kind")}] if network else []),
                              "network_attachments": [
                                  {"machine_id": row["machine_id"], "network_id": network_id}
                                  for row in state["machines"]],
                              "endpoints": endpoints}]}


def do_status(options):
    state = load()
    if options["json"]:
        sys.stdout.write(json.dumps(status_document(state), indent=1) + "\n")
    else:
        sys.stdout.write("Environment %s (default) ready\n" % state["environment_id"])
    return 0


def do_exec(options):
    state = load()
    rows = state["machines"]
    selector = options["machine"]
    if selector is None:
        if len(rows) > 1 and MODE != "fork_exec_falls_back":
            listed = ", ".join("%s (%s)" % (row["name"], row["machine_id"]) for row in rows)
            if MODE == "fork_ambiguous_unlisted":
                refuse("validation_error", "Machine selection is ambiguous; specify --machine")
            refuse("validation_error",
                   "Machine selection is ambiguous; specify --machine (candidates: %s)" % listed)
        machine = rows[0]
    else:
        matched = [row for row in rows
                   if row["name"] == selector or row["machine_id"] == selector]
        if not matched:
            refuse("not_found", "no Machine matches the selected Environment and Machine selectors")
        if len(matched) > 1:
            refuse("validation_error", "Machine selection is ambiguous; specify --machine")
        machine = matched[0]
    guest = Path(machine["guest"])
    for relative in ("run", "proc", "sys"):
        (guest / relative).mkdir(parents=True, exist_ok=True)
    shim = str(HERE / "busybox-fork-shim")
    rewritten = []
    for item in options["tail"]:
        item = item.replace("/bin/busybox", shim)
        for relative in ("/run/", "/proc/", "/sys/"):
            item = item.replace(relative, str(guest) + relative)
        rewritten.append(item)
    environment = dict(os.environ, VZ_FORK_GUEST=str(guest))
    completed = subprocess.run(rewritten, capture_output=True, env=environment, check=False)
    sys.stdout.buffer.write(completed.stdout)
    sys.stderr.buffer.write(completed.stderr)
    return completed.returncode


def do_delete(options):
    state = load()
    selector = options["machine"]
    if selector is not None:
        if "@" not in selector:
            if MODE == "fork_delete_declared":
                state["machines"] = [row for row in state["machines"] if row["name"] != selector]
                save(state)
                sys.stdout.write(json.dumps({"schema_version": 1, "deleted": [selector]}) + "\n")
                return 0
            refuse("invalid_selector",
                   "`%s` names a declared Machine; only a fork `<machine>@<label>` can be deleted "
                   "on its own" % selector)
        if MODE == "fork_delete_blanket":
            refuse("unsupported_operation", "reclaiming one fork is not implemented")
        machine = next((row for row in state["machines"] if row["name"] == selector), None)
        if machine is None:
            refuse("not_found", "no Machine `%s` in Environment `%s`; `vz status` lists forks with "
                   "their labels" % (selector, state["environment_id"]))
        if not machine.get("fork"):
            refuse("unsupported_operation", "Machine `%s` is declared by the project definition; "
                   "only a fork can be deleted on its own" % machine["name"])
        if MODE == "fork_delete_generic":
            refuse("internal_error", "delete failed")
        # Reclaiming one fork is the DEFAULT answer now, not a mode. The
        # Machine-scoped lifecycle operation landed in 94ea34c9 and is proved
        # on hardware: `vz delete --machine machine-0@feat-y` removes exactly
        # that fork and its Docker data disk, with the parent and the sibling
        # fork still present. A fake that kept refusing would make criterion
        # 23's delete clause look unimplemented when it is not.
        #
        # `fork_delete_leaks` is still a mode, because "removed the rows and
        # left the store behind" is the failure the check has to be able to
        # catch, and it is not something the real runtime does.
        state["machines"] = [row for row in state["machines"] if row["name"] != selector]
        if MODE != "fork_delete_leaks":
            shutil.rmtree(store_root(machine), ignore_errors=True)
        save(state)
        sys.stdout.write(json.dumps({"schema_version": 1, "deleted": [selector]}) + "\n")
        return 0
    keep = None
    if MODE == "fork_env_delete_leaks" and state["machines"]:
        keep = state["machines"][0]["machine_id"]
    TOPOLOGY.unlink()
    for machine in state["machines"]:
        if machine["machine_id"] != keep:
            shutil.rmtree(store_root(machine), ignore_errors=True)
    shutil.rmtree(RUNTIME / "guest", ignore_errors=True)
    shutil.rmtree(DOCKER_CONFIG / "vzfork-contexts", ignore_errors=True)
    sys.stdout.write(json.dumps({"schema_version": 1, "deleted": ["default"]}) + "\n")
    return 0


def main(argv):
    options = parse(argv)
    if options["environment"] not in (None, "default"):
        refuse("environment_not_found",
               "no Environment named %s in this project" % options["environment"])
    if options["verb"] == "up":
        return do_up(options)
    if options["verb"] == "status":
        return do_status(options)
    if options["verb"] == "exec":
        return do_exec(options)
    if options["verb"] == "delete":
        return do_delete(options)
    if options["verb"] == "stop":
        sys.stdout.write(json.dumps({"schema_version": 1, "stopped": ["default"]}) + "\n")
        return 0
    refuse("definition_not_found", "no vz.json project definition found at or above %s" % Path.cwd())
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
'''


def build_fork_release(root: Path, *, mode_file: Path) -> Path:
    """A release directory whose `vz` and `docker` model criterion 23.

    Not a signed candidate: these tests call the check directly rather than
    through lane admission, because the claim under test is what the check
    asserts about a runtime, not how the lane admits a release.
    """
    binaries = root / "bin"
    binaries.mkdir(mode=0o700, parents=True)
    substitutions = {"__MODE_FILE__": json.dumps(str(mode_file)),
                     "__DISK_BYTES__": str(FORK_DISK_BYTES),
                     "__HEADER__": str(FORK_DISK_HEADER)}
    for name, text in (("vz", FORK_VZ), ("docker-fork-stand-in", FORK_DOCKER)):
        for token, value in substitutions.items():
            text = text.replace(token, value)
        path = binaries / name
        path.write_text(text)
        path.chmod(0o755)
    (binaries / "busybox-fork-shim").write_text(FORK_BUSYBOX)
    (binaries / "busybox-fork-shim").chmod(0o755)
    # `ctx.isolated(provision=True)` points VZ at this as the daemon it may
    # spawn; the stand-in CLI never spawns one, so it only has to exist.
    (binaries / "vz-runtimed").write_text("#!/bin/sh\nexit 0\n")
    (binaries / "vz-runtimed").chmod(0o755)
    (root / "machine-target-catalog.json").write_bytes(
        json.dumps(CATALOG, indent=2, sort_keys=True).encode() + b"\n")
    return root
