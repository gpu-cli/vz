"""Physical sub-checks of the `topology` lane against the installed 0.4 binaries.

Most checks prove what the binaries do without provisioning; the bootstrap
Environment check provisions one Developer Machine and removes it again, and is
the lane's only mutating path.

Criterion 21 (`gate.cli.legacy_removal_and_bootstrap`):
  bare_help            bare `vz` == pinned snapshot, exit 0, no discovery/mutation
  legacy_rejection     every removed root/flag/nested path in the CLI-removal
                       inventory returns exit 2 + one-line structured JSON
  clean_up_refuses     `vz up` in a clean directory fails `definition_not_found`
                       with zero state mutation and no daemon
  bootstrap_read_only  a schema-valid minimal vz.json is read by `vz status`
                       without spawning a daemon or creating state (the runtime
                       directory it would spawn into is asserted absent)
  bootstrap_creates_default   a real `vz up` from a definition naming no
                       Environment creates `default`, then deletes it again
Criterion 15 (`gate.cli_api.agreement`):
  help_surface_exact   root/subcommand help is exactly the five verbs
  error_envelope_agreement   `up` and `status` agree on `definition_not_found`
  status_json_field_set      `vz --json status` over a live topology emits
                       exactly its declared field set, at the top level and for
                       the Environment and Machine objects under it
  grpc_api_live_agreement    NOT IMPLEMENTED
Criterion 17 (`gate.storage.workspace_projection_policy`):
  workspace_storage_policy   read_write/read_only/snapshot projections prove
                       their own target-qualified semantics against the host
                       worktree, a write into a read_only projection fails, a
                       writable block volume on two Machines is refused with no
                       storage allocated and no Environment persisted, and two
                       Machines writing one declared shared cache concurrently
                       both observe every write within its declared staleness
                       bound

Every AF_UNIX path the installed binaries bind lives in the lane's short socket
root, not under `--state-root`; see `developer_environment_recorder`.

Sub-check ids are `<top-level id>__<slug>` (the lane-result schema allows
exactly three dot-separated `[a-z_]+` segments, so a fourth `.slug` segment
is not schema-valid). Every assertion is recorded from raw receipts.
"""
from __future__ import annotations

import copy
import json
import shutil
import os
import re
from pathlib import Path
import socket
import stat
import time
import uuid

from jsonschema import Draft202012Validator

import frozen_tree

from developer_environment_recorder import (ENDPOINT_NAME_BYTES, SOCKET_PATH_LIMIT, LaneState, Recorder, inventory,
                                            inventory_diff, processes_referencing, write_inventory)
from vz04_common import digest_file, load_json, now_ns, read_regular, write_exclusive
import vz04_host

HELP_SNAPSHOT = "tests/fixtures/vz-0.4/cli/help-snapshot.txt"
PROJECT_DEFINITION_SCHEMA = "schemas/vz-project-definition-v1.schema.json"
LEGACY_CODE = "legacy_command_removed"
LEGACY_EXIT = 2
ROOT_MIGRATION = ("Declare Developer Environment topology in vz.json. Use vz up to create it, vz status to inspect it, "
                  "vz exec for Machine execution, vz stop to preserve it, and vz delete to remove owned state. "
                  "Consult installed help for implemented DEV capabilities.")
FLAG_MIGRATION = ("The implicit sandbox mode was removed. Declare Developer Environment configuration in vz.json. "
                  "The 0.4 public CLI is converging on explicit vz up, vz exec, vz status, vz stop, and vz delete lifecycle verbs.")
TYPED_API_MIGRATION = "Use the topology-scoped typed API for operations outside the five lifecycle verbs."
FIVE_VERBS = ("up", "exec", "status", "stop", "delete")
INVALID_DEFINITION = b"deliberately invalid project definition\n"
SENTINEL = b"project unchanged\n"
STATE_SENTINEL = b"not a SQLite database; must remain byte-identical"
FLAG_SPELLINGS = ((["-c"], "-c"), (["-hc"], "-c"), (["-vc"], "-c"), (["-qc"], "-c"), (["--continue"], "--continue"),
                  (["-r", "target"], "-r"), (["-vrtarget"], "-r"), (["-vrcandidate"], "-r"), (["-Vcr"], "-c"),
                  (["--resume=target"], "--resume"), (["--name", "target"], "--name"), (["--ephemeral"], "--ephemeral"),
                  (["--cpus", "4"], "--cpus"), (["--memory=4096"], "--memory"), (["--base-image", "alpine"], "--base-image"),
                  (["--main-container=app"], "--main-container"), (["--control-plane", "daemon-grpc"], "--control-plane"),
                  (["help", "--name", "stack"], "--name"))
MAX_REPORTED_FAILURES = 25


class ReattachError(Exception):
    """An earlier phase left no isolate for this one to address.

    Raised rather than returned so a post-wake check cannot mistake an
    Environment that never survived for one that came back empty.
    """


class SubCheck:
    def __init__(self, top: str, slug: str):
        self.id = f"{top}__{slug}"
        self.slug = slug
        self.started = now_ns()
        self.ended = None
        self.assertions = []
        self.failures = []
        self.evidence = []
        self.not_implemented = None

    def ok(self, text: str) -> None:
        self.assertions.append(text)

    def fail(self, text: str) -> None:
        if len(self.failures) < MAX_REPORTED_FAILURES:
            self.failures.append(text)
        elif len(self.failures) == MAX_REPORTED_FAILURES:
            self.failures.append("further failures elided")

    def check(self, condition, text: str) -> bool:
        (self.ok if condition else self.fail)(text)
        return bool(condition)

    def finish(self) -> "SubCheck":
        self.ended = now_ns()
        return self

    @property
    def status(self) -> str:
        return "PASS" if not self.failures and self.not_implemented is None else "FAIL"

    def scenario(self) -> dict:
        assertions = list(self.assertions)
        if self.not_implemented:
            assertions.append(f"not_implemented: {self.not_implemented}")
        assertions.extend(f"FAILED: {text}" for text in self.failures)
        return {"id": self.id, "status": self.status, "started_unix_ns": self.started, "ended_unix_ns": self.ended or now_ns(),
                "assertions": assertions, "evidence": sorted(set(self.evidence)), "readiness_polls": []}


class CheckContext:
    def __init__(self, *, repo_root: Path, release_dir: Path, state: LaneState, recorder: Recorder, evidence_dir: Path,
                 cli_removal: dict, docker_client: str = "none", plugins: dict = None):
        self.repo_root = Path(repo_root)
        self.release_dir = Path(release_dir)
        self.state = state
        self.recorder = recorder
        self.evidence_dir = Path(evidence_dir)
        self.cli_removal = cli_removal
        # The gate passes the Mac's own unmodified client; a Developer Machine
        # cannot reach `ready` without one, and vz will not guess a path.
        self.docker_client = docker_client
        # `docker compose` is a CLI plugin, not a subcommand: without it staged
        # beside the client config the Engine handshake fails with
        # "unknown command: docker compose" after the Machine has been created.
        self.plugins = {name: path for name, path in (plugins or {}).items() if path and path != "none"}

    def run(self, check: SubCheck, label: str, argv: list, *, cwd: Path, env: dict, timeout: int = 5):
        receipt = self.recorder.run(label, [self.state.cli, *argv], cwd=cwd, env=env, scenario_id=check.id, timeout=timeout)
        check.evidence.extend(self.recorder.receipt_paths(receipt))
        return receipt

    def start(self, check: SubCheck, label: str, argv: list, *, cwd: Path, env: dict, timeout: int = 120):
        """Start a CLI invocation this check holds open until it releases it."""
        return self.recorder.start(label, [self.state.cli, *argv], cwd=cwd, env=env, scenario_id=check.id,
                                   timeout=timeout)

    def release(self, check: SubCheck, held):
        receipt = self.recorder.release(held)
        check.evidence.extend(self.recorder.receipt_paths(receipt))
        return receipt

    def run_tool(self, check: SubCheck, label: str, argv: list, *, cwd: Path, env: dict, timeout: int = 60):
        """Record a non-vz host tool the same way the CLI is recorded."""
        receipt = self.recorder.run(label, argv, cwd=cwd, env=env, scenario_id=check.id, timeout=timeout)
        check.evidence.extend(self.recorder.receipt_paths(receipt))
        return receipt

    def isolated(self, name: str, *, project_files: dict, provision: bool = False) -> dict:
        """`<lane state>/<name>/{state,project}`, an absent HOME, and a runtime
        directory in the lane's short AF_UNIX root.

        The daemon is absent by default so a read-only check can never spawn
        one. `provision=True` points at the release daemon instead, which is
        what a real `vz up` needs; only a check that also removes what it
        creates may ask for it.

        Runtime sockets never live under `--state-root`: macOS cannot bind a
        103+ byte path and a real state root is already longer than the budget
        (see `developer_environment_recorder.socket_root_for`). A read-only
        isolate's runtime directory is still not created here, so "no daemon
        socket, PID file or runtime directory appeared" stays an assertion the
        read-only checks make about a real path.
        """
        root = self.state.root / name
        root.mkdir(mode=0o700)
        state_dir = root / "state"
        state_dir.mkdir(mode=0o700)
        project = root / "project"
        project.mkdir(mode=0o700)
        self.state.socket_root.mkdir(mode=0o700, exist_ok=True)
        # `isolate_paths` derives this same path from the same function below;
        # the provisioning branch needs it here to create the directory first.
        runtime = self.state.isolate_runtime(name)
        overrides = {"CARGO_BIN_EXE_vz-runtimed": self.state.daemon if provision else self.state.absent_daemon}
        if provision:
            # vz requires its runtime and Docker endpoint directories to be
            # effective-user-owned mode 0700. Left to the daemon they inherit
            # the ambient umask, and Up then fails with an ownership conflict
            # after it has already admitted the request and allocated
            # identities. A Developer Machine also cannot reach `ready` without
            # a Docker client, and vz will not guess a path for one.
            runtime.mkdir(mode=0o700)
            (state_dir / "docker").mkdir(mode=0o700)
            if self.docker_client and self.docker_client != "none":
                overrides["VZ_DOCKER_CLIENT"] = self.docker_client
            if self.plugins:
                plugin_dir = state_dir / "docker" / "cli-plugins"
                plugin_dir.mkdir(mode=0o700)
                for plugin, source in sorted(self.plugins.items()):
                    destination = plugin_dir / ("docker-" + plugin)
                    shutil.copyfile(source, destination)
                    destination.chmod(0o500)
        for filename, data in project_files.items():
            write_exclusive(project / filename, data)
        return self.isolate_paths(name, overrides)

    def isolate_paths(self, name: str, overrides: dict) -> dict:
        """One isolate's paths and environment, derived and never stored.

        `isolated` and `reattach` both come through here so the two spellings of
        one isolate cannot drift: a phase that rebuilt the environment slightly
        differently would address a different state database or socket and read
        the absence as a recovery failure.
        """
        root = self.state.root / name
        state_dir, project = root / "state", root / "project"
        runtime = self.state.isolate_runtime(name)
        env = self.state.env(HOME=root / "absent-home", VZ_RUNTIME_STATE_DB=state_dir / "stack-state.db",
                             VZ_RUNTIME_DATA_DIR=runtime, VZ_RUNTIME_DAEMON_SOCKET=runtime / "d.sock",
                             VZ_DOCKER_CONFIG=state_dir / "docker", **overrides)
        return {"root": root, "state": state_dir, "project": project, "runtime": runtime, "env": env}

    def reattach(self, name: str) -> dict:
        """The isolate an earlier phase created, addressed again and unchanged.

        persisted-recovery spans two lane invocations either side of a hardware
        sleep/wake checkpoint. The Environments pre-sleep leaves running are
        exactly the ones post-wake has to find, so this creates nothing: a
        missing directory is the phase's finding, not something to repair.
        `developer_environment_recorder.socket_root_for` derives the AF_UNIX
        root from the state root for the same reason -- both phases of one run
        address the same one rather than a random per-invocation path.
        """
        iso = self.isolate_paths(name, {"CARGO_BIN_EXE_vz-runtimed": self.state.daemon})
        absent = [str(path) for path in (iso["root"], iso["state"], iso["project"])
                  if not path.is_dir() or path.is_symlink()]
        if absent:
            raise ReattachError(f"{name}: no isolate survived the earlier phase at " + ", ".join(absent))
        return iso


def _single_json_line(data: bytes):
    text = data.decode("utf-8")
    lines = text.splitlines()
    if len(lines) != 1 or not text.endswith("\n"):
        return None
    return json.loads(lines[0])


def _unchanged(check: SubCheck, what: str, before: list, root: Path) -> bool:
    diff = inventory_diff(before, inventory(root))
    return check.check(not diff, f"{what} unchanged" if not diff else f"{what} changed: " + "; ".join(diff[:6]))


def check_bare_help(ctx: CheckContext, top: str) -> SubCheck:
    check = SubCheck(top, "bare_help")
    snapshot_path = ctx.repo_root / HELP_SNAPSHOT
    if not snapshot_path.is_file():
        check.fail(f"help snapshot fixture absent: {HELP_SNAPSHOT}")
        return check.finish()
    snapshot = read_regular(snapshot_path)
    check.ok(f"snapshot {HELP_SNAPSHOT} sha256={digest_file(snapshot_path)}")
    check.ok(f"installed cli {ctx.state.cli} sha256={digest_file(ctx.state.cli)}")
    iso = ctx.isolated("bare", project_files={"vz.json": INVALID_DEFINITION, "sentinel": SENTINEL})
    # Empty, read-only isolated state; absent HOME; poisoned daemon controls so
    # any discovery attempt would fail loudly rather than silently succeed.
    iso["state"].chmod(0o500)
    env = dict(iso["env"], VZ_RUNTIME_DAEMON_AUTOSTART="definitely-not-a-boolean",
               VZ_CONTROL_PLANE_TRANSPORT="definitely-not-a-transport")
    before, path = write_inventory(ctx.evidence_dir, "bare-isolated-before", iso["root"])
    check.evidence.append(path)
    observed = None
    for argv in ([], ["--help"], ["help"], ["--json"], ["--quiet"], ["-v"], ["-vvq"]):
        receipt = ctx.run(check, "bare-" + ("-".join(argv) or "vz"), argv, cwd=iso["project"], env=env)
        spelled = "vz " + " ".join(argv) if argv else "bare vz"
        check.check(receipt.exit_code == 0, f"{spelled}: exit {receipt.exit_code} (expected 0)")
        check.check(receipt.stderr == b"", f"{spelled}: stderr empty" if not receipt.stderr else f"{spelled}: stderr not empty")
        check.check(receipt.stdout == snapshot, f"{spelled}: stdout == snapshot ({len(receipt.stdout)} bytes)")
        if observed is None:
            observed = receipt.stdout
    write_exclusive(ctx.evidence_dir / "bare-help-observed.txt", observed or b"")
    check.evidence.append("bare-help-observed.txt")
    after, path = write_inventory(ctx.evidence_dir, "bare-isolated-after", iso["root"])
    check.evidence.append(path)
    diff = inventory_diff(before, after)
    check.check(not diff, "isolated root (read-only empty state, project, absent HOME) byte-identical after every invocation"
                if not diff else "isolated root changed: " + "; ".join(diff[:6]))
    # The runtime directory is outside the isolated root (AF_UNIX budget), so the
    # inventory above cannot speak for it: assert it directly.
    check.check(not os.path.lexists(iso["runtime"]),
                f"no runtime directory, daemon socket or PID file created at {iso['runtime']}")
    check.check(not os.path.lexists(iso["env"]["HOME"]), "absent HOME not created")
    iso["state"].chmod(0o700)
    return check.finish()


def _legacy_payload(check: SubCheck, spelled: str, receipt, command: str, migration: str) -> bool:
    ok = True
    if receipt.exit_code != LEGACY_EXIT:
        check.fail(f"{spelled}: exit {receipt.exit_code} (expected {LEGACY_EXIT})")
        ok = False
    if receipt.stdout:
        check.fail(f"{spelled}: stdout not empty")
        ok = False
    try:
        payload = _single_json_line(receipt.stderr)
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if not isinstance(payload, dict):
        check.fail(f"{spelled}: stderr is not exactly one JSON line")
        return False
    error = payload.get("error")
    expected = {"code": LEGACY_CODE, "command": command, "message": f"`vz {command}` was removed from the 0.4 public CLI",
                "migration": migration, "typed_api_migration": TYPED_API_MIGRATION}
    if set(payload) != {"error"} or error != expected:
        check.fail(f"{spelled}: structured payload differs from contract: {json.dumps(payload)[:300]}")
        ok = False
    return ok


def check_legacy_rejection(ctx: CheckContext, top: str) -> SubCheck:
    check = SubCheck(top, "legacy_rejection")
    inventory_cfg = ctx.cli_removal
    rejection = inventory_cfg["rejection"]
    check.check(rejection == {"exit_code": LEGACY_EXIT, "code": LEGACY_CODE, "stream": "stderr", "format": "single_line_json",
                              "stdout": "empty", "state_effects": "none"}, "cli-removal inventory rejection contract as expected")
    normative = ctx.repo_root / inventory_cfg["normative_source"]
    if normative.is_file():
        blocks = [rest.split("\n```", 1)[0] for rest in read_regular(normative).decode("utf-8").split("```text\n")[1:]]
        check.check(len(blocks) >= 2 and blocks[0].splitlines() == inventory_cfg["removed_roots"],
                    "removed_roots equal the normative legacy-cli-removal.md block")
        flags = [flag for line in blocks[1].splitlines() for flag in line.split(", ")] if len(blocks) >= 2 else []
        check.check(flags == inventory_cfg["removed_root_flags"], "removed_root_flags equal the normative block")
    else:
        check.fail(f"normative source absent: {inventory_cfg['normative_source']}")
    iso = ctx.isolated("legacy", project_files={"vz.json": INVALID_DEFINITION, "sentinel": SENTINEL})
    env = dict(iso["env"], VZ_RUNTIME_DAEMON_AUTOSTART="1", VZ_ENVIRONMENT_ID="invalid-selector-must-not-be-read",
               VZ_MACHINE_ID="invalid-selector-must-not-be-read")
    before, path = write_inventory(ctx.evidence_dir, "legacy-isolated-before", iso["root"])
    check.evidence.append(path)
    invocations = 0
    rejected = 0
    roots = inventory_cfg["removed_roots"]
    for root in roots:
        for argv in ([root], [root, "--help"], ["help", root], ["--json", root, "--help"], ["--", root], ["--help", root],
                     ["--version", root], ["help", "--", root], ["-vvq", root, "unknown"]):
            receipt = ctx.run(check, "root-" + "-".join(argv), argv, cwd=iso["project"], env=env)
            invocations += 1
            rejected += _legacy_payload(check, "vz " + " ".join(argv), receipt, root, ROOT_MIGRATION)
    for argv, flag in FLAG_SPELLINGS:
        receipt = ctx.run(check, "flag-" + "-".join(argv), argv, cwd=iso["project"], env=env)
        invocations += 1
        rejected += _legacy_payload(check, "vz " + " ".join(argv), receipt, flag, FLAG_MIGRATION)
    paths = [entry["path"] for entry in inventory_cfg["dev_baseline"]["help_paths"]] + list(inventory_cfg["normative_only_paths"])
    for path_parts in paths:
        root = path_parts[0]
        if root not in roots:
            check.fail(f"inventoried path {' '.join(path_parts)} does not start with a removed root")
        for argv in (list(path_parts), [*path_parts, "--help"], [*path_parts, "arbitrary", "--unknown"], ["help", *path_parts]):
            receipt = ctx.run(check, "path-" + "-".join(argv), argv, cwd=iso["project"], env=env)
            invocations += 1
            rejected += _legacy_payload(check, "vz " + " ".join(argv), receipt, root, ROOT_MIGRATION)
    check.check(rejected == invocations, f"{rejected}/{invocations} invocations rejected with exit {LEGACY_EXIT} and one-line "
                f"{LEGACY_CODE} JSON: {len(roots)} roots x 9 spellings, {len(FLAG_SPELLINGS)} removed-flag spellings, "
                f"{len(paths)} inventoried nested paths x 4 spellings")
    after, path = write_inventory(ctx.evidence_dir, "legacy-isolated-after", iso["root"])
    check.evidence.append(path)
    diff = inventory_diff(before, after)
    check.check(not diff, "isolated state/project byte-identical after every rejected invocation" if not diff
                else "isolated root changed: " + "; ".join(diff[:6]))
    # Existing socket + sentinel state DB: retired roots must neither connect nor write.
    runtime_dir = Path(iso["env"]["VZ_RUNTIME_DATA_DIR"])
    runtime_dir.mkdir(mode=0o700)
    write_exclusive(Path(iso["env"]["VZ_RUNTIME_STATE_DB"]), STATE_SENTINEL)
    socket_path = iso["env"]["VZ_RUNTIME_DAEMON_SOCKET"]
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(socket_path)
        listener.listen(8)
        listener.setblocking(False)
        bound = True
    except OSError as error:
        bound = False
        check.ok(f"existing-socket probe not performed: cannot bind {socket_path} ({error.strerror}); "
                 "path exceeds the macOS sun_path limit under this state root")
    if bound:
        connections = 0
        for root in roots:
            receipt = ctx.run(check, "socket-" + root, [root, "arbitrary", "--unknown"], cwd=iso["project"], env=env)
            _legacy_payload(check, f"vz {root} arbitrary --unknown (existing socket)", receipt, root, ROOT_MIGRATION)
            try:
                while True:
                    connection, _ = listener.accept()
                    connection.close()
                    connections += 1
            except BlockingIOError:
                pass
        listener.close()
        os.unlink(socket_path)
        check.check(connections == 0, f"retired roots opened {connections} connections to an existing daemon socket (expected 0)")
        check.check(read_regular(Path(iso["env"]["VZ_RUNTIME_STATE_DB"])) == STATE_SENTINEL, "sentinel state DB byte-identical")
        check.check(sorted(os.listdir(runtime_dir)) == [], "runtime dir gained no files beside the probe socket")
    return check.finish()


def check_clean_up(ctx: CheckContext, top: str) -> SubCheck:
    check = SubCheck(top, "clean_up_refuses")
    iso = ctx.isolated("clean", project_files={})
    env = iso["env"]
    before, path = write_inventory(ctx.evidence_dir, "clean-state-root-before", ctx.state.root)
    check.evidence.append(path)
    for argv in (["--json", "up"], ["up"]):
        receipt = ctx.run(check, "clean-" + "-".join(argv), argv, cwd=iso["project"], env=env, timeout=30)
        spelled = "vz " + " ".join(argv)
        check.check(receipt.exit_code == 2, f"{spelled}: exit {receipt.exit_code} (expected 2)")
        check.check(receipt.stdout == b"", f"{spelled}: stdout empty" if not receipt.stdout else f"{spelled}: stdout not empty")
        try:
            payload = _single_json_line(receipt.stderr)
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = None
        code = payload.get("error", {}).get("code") if isinstance(payload, dict) else None
        check.check(code == "definition_not_found", f"{spelled}: stderr error.code={code!r} (expected definition_not_found)")
        if argv[0] == "--json" and isinstance(payload, dict):
            check.check(payload.get("schema_version") == 1 and {"code", "message", "request_id", "idempotency_key", "details"}
                        <= set(payload.get("error", {})), f"{spelled}: structured error carries schema_version, request_id, idempotency_key")
    after, path = write_inventory(ctx.evidence_dir, "clean-state-root-after", ctx.state.root)
    check.evidence.append(path)
    diff = inventory_diff(before, after)
    check.check(not diff, "lane state root inventory identical before/after (zero mutation)" if not diff
                else "lane state root changed: " + "; ".join(diff[:6]))
    check.check(os.listdir(iso["project"]) == [], "clean project directory still empty")
    # Runtime directories live in the lane's short AF_UNIX root, which the state
    # root inventory above does not cover: name both of them explicitly.
    check.check(not os.path.lexists(iso["runtime"]) and not os.path.lexists(ctx.state.runtime),
                f"no runtime directory or daemon socket created ({iso['runtime']}, {ctx.state.runtime})")
    live = processes_referencing(ctx.state)
    check.check(not live, "no live process references either lane root" if not live else f"live processes: {live[:3]}")
    return check.finish()


def minimal_definition(release_dir: Path) -> dict:
    catalog = load_json(release_dir / "machine-target-catalog.json")
    entry = next(item for item in catalog["linux"] if item["profile"] == "developer")
    return {"schema_version": 1, "project_id": "prj_" + uuid.uuid4().hex, "name": "vz04-topology-bootstrap",
            "environment": {"schema_version": 1, "machines": [
                {"schema_version": 1, "name": "machine-0", "profile": "developer",
                 "target": {"os": "linux", "arch": "aarch64", "image": entry["image"], "digest": entry["digest"]},
                 "resources": {"cpus": 2, "memory_mb": 4096}}]}}


def check_bootstrap_read_only(ctx: CheckContext, top: str) -> SubCheck:
    check = SubCheck(top, "bootstrap_read_only")
    try:
        definition = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if schema_path.is_file():
        problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(definition), key=lambda e: list(map(str, e.absolute_path)))
        check.check(not problems, f"minimal vz.json validates against {PROJECT_DEFINITION_SCHEMA}" if not problems
                    else f"minimal vz.json invalid: {problems[0].message[:200]}")
    else:
        check.fail(f"project definition schema absent: {PROJECT_DEFINITION_SCHEMA}")
    data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
    iso = ctx.isolated("bootstrap", project_files={"vz.json": data})
    write_exclusive(ctx.evidence_dir / "bootstrap-vz.json.txt", data)
    check.evidence.append("bootstrap-vz.json.txt")
    env = iso["env"]
    before, path = write_inventory(ctx.evidence_dir, "bootstrap-state-root-before", ctx.state.root)
    check.evidence.append(path)
    receipt = ctx.run(check, "bootstrap-status-all", ["--json", "status", "--all"], cwd=iso["project"], env=env, timeout=30)
    check.check(receipt.exit_code == 2 and receipt.stdout == b"", f"vz --json status --all: exit {receipt.exit_code}, stdout {len(receipt.stdout)} bytes (expected 2, empty)")
    try:
        payload = _single_json_line(receipt.stderr)
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    code = payload.get("error", {}).get("code") if isinstance(payload, dict) else None
    check.check(code == "daemon_unavailable", f"definition read, then error.code={code!r} (expected daemon_unavailable: read-only status never spawns)")
    after, path = write_inventory(ctx.evidence_dir, "bootstrap-state-root-after", ctx.state.root)
    check.evidence.append(path)
    diff = inventory_diff(before, after)
    check.check(not diff, "lane state root identical: no state DB created" if not diff
                else "lane state root changed: " + "; ".join(diff[:6]))
    # The runtime directory (socket, PID file, log) is in the lane's short
    # AF_UNIX root, outside the inventory above: a spawn would show here.
    created = inventory(iso["runtime"])
    check.check(not os.path.lexists(iso["runtime"]),
                f"no runtime directory, daemon socket or PID file created at {iso['runtime']}"
                if not os.path.lexists(iso["runtime"]) else
                f"read-only status created {iso['runtime']}: " + "; ".join(row[0] for row in created[:6]))
    live = processes_referencing(ctx.state)
    check.check(not live, "no live process references either lane root" if not live else f"live processes: {live[:3]}")
    return check.finish()


UP_TIMEOUT = 900
DELETE_TIMEOUT = 300
GIT = "/usr/bin/git"


CONCURRENT = 3


def provision(ctx: CheckContext, check: SubCheck, name: str, definition: dict, *, timeout: int = UP_TIMEOUT) -> dict:
    """Bring one Environment up in its own isolated project; never clean up here."""
    data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
    iso = ctx.isolated(name, project_files={"vz.json": data}, provision=True)
    env, project = iso["env"], iso["project"]
    socket = Path(env["VZ_RUNTIME_DAEMON_SOCKET"])
    length = len(str(socket).encode())
    check.check(length <= SOCKET_PATH_LIMIT,
                f"{name}: socket path is {length} bytes (limit {SOCKET_PATH_LIMIT})")
    # Each Machine's Docker endpoint is another AF_UNIX socket in the runtime
    # directory, named `vzr1-ot-<32 hex>.sock`. Over budget, Up reports a
    # state_conflict about a "bounded absolute path" that says nothing about
    # length, so the budget is asserted here instead.
    endpoint = len(str(Path(env["VZ_RUNTIME_DATA_DIR"]) / ("x" * ENDPOINT_NAME_BYTES)).encode())
    check.check(endpoint <= SOCKET_PATH_LIMIT,
                f"{name}: Docker endpoint path would be {endpoint} bytes (limit {SOCKET_PATH_LIMIT})")
    if check.status != "PASS":
        return {"env": env, "project": project, "status": None}
    for label, argv in ((name + "-git-init", [GIT, "init", "--quiet", "--initial-branch", "main"]),
                        (name + "-git-add", [GIT, "add", "vz.json"]),
                        (name + "-git-commit", [GIT, "-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                                                "commit", "--quiet", "-m", "definition"])):
        receipt = ctx.run_tool(check, label, argv, cwd=project, env=env)
        check.check(receipt.exit_code == 0, f"{label}: exit {receipt.exit_code} (expected 0)")
    if check.status != "PASS":
        return {"env": env, "project": project, "status": None}
    up = ctx.run(check, name + "-up", ["--json", "up"], cwd=project, env=env, timeout=timeout)
    if up.exit_code != 0:
        # A refusal naming an unimplemented adapter is reported to the caller
        # rather than asserted against: it is a statement about the runtime.
        detail = ""
        try:
            detail = json.loads(up.stderr.decode("utf-8")).get("error", {}).get("message", "")
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            detail = ""
        if "adapters remain required" in detail:
            return {"env": env, "project": project, "status": None, "unsupported": detail}
        check.check(False, f"{name}: vz --json up exit {up.exit_code} (expected 0)")
        return {"env": env, "project": project, "status": None}
    check.check(True, f"{name}: vz --json up exit 0 (expected 0)")
    return {"env": env, "project": project,
            "status": read_status(ctx, check, name, project=project, env=env)}


def read_status(ctx: CheckContext, check: SubCheck, name: str, *, project: Path, env: dict):
    row = ctx.run(check, name + "-status", ["--json", "status"], cwd=project, env=env, timeout=60)
    if row.exit_code != 0:
        return None
    try:
        return json.loads(row.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        check.fail(f"{name}: status is not a JSON document")
        return None


def check_three_concurrent_environments(ctx: CheckContext, top: str) -> SubCheck:
    """Three Environments alive at once, every declared name deliberately repeated.

    Each project declares the same Machine name and the same resources, so any
    identity the Engine hands out must be its own doing. Collision would show as
    a shared project, Environment, Machine, Docker context or endpoint. They are
    brought up one at a time but all three are required to be ready together,
    which is what `concurrent` means for this criterion.
    """
    check = SubCheck(top, "three_concurrent_no_collision")
    try:
        base = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    instances = []
    for index in range(CONCURRENT):
        definition = copy.deepcopy(base)
        # Only the project id differs; the name, Machine name and resources are
        # deliberately identical across all three.
        definition["project_id"] = "prj_" + uuid.uuid4().hex
        instances.append(provision(ctx, check, f"c{index}", definition))
        if check.status != "PASS":
            break
    live = [row for row in instances if row["status"]]
    check.check(len(live) == CONCURRENT, f"{len(live)} of {CONCURRENT} Environments reached a readable status")
    if len(live) == CONCURRENT:
        identities = {"project": [], "environment": [], "machine": [], "context": [], "endpoint": [], "engine": []}
        for row in live:
            payload = row["status"]
            environments = payload.get("environments") or []
            check.check(len(environments) == 1, f"each project reports exactly one Environment (observed {len(environments)})")
            if len(environments) != 1:
                break
            environment = environments[0]
            machines = environment.get("machines") or []
            check.check(len(machines) == 1 and environment.get("state") == "ready",
                        f"one ready Machine per Environment (observed {len(machines)}, state {environment.get('state')!r})")
            if len(machines) != 1:
                break
            context = machines[0].get("docker_context") or {}
            identities["project"].append(payload.get("project_id"))
            identities["environment"].append(environment.get("environment_id"))
            identities["machine"].append(machines[0].get("machine_id"))
            identities["context"].append(context.get("name"))
            identities["endpoint"].append(context.get("endpoint"))
            # A shared Engine would be the most direct collision of all.
            identities["engine"].append(context.get("engine_id"))
            # The repeated declarations are the point: names collide by design.
            check.check(environment.get("name") == "default" and machines[0].get("name") == "machine-0",
                        f"declared names repeat across instances (observed {environment.get('name')!r}/{machines[0].get('name')!r})")
        for kind, values in identities.items():
            present = [value for value in values if value]
            check.check(len(present) == CONCURRENT and len(set(present)) == CONCURRENT,
                        f"{kind} identities are distinct across the three Environments ({len(set(present))} of {CONCURRENT})")
    if check.status == "PASS":
        for index, row in enumerate(instances):
            removed = ctx.run(check, f"concurrent-{index}-delete",
                              ["--json", "delete", "--environment", "default", "--timeout", "120"],
                              cwd=row["project"], env=row["env"], timeout=DELETE_TIMEOUT)
            check.check(removed.exit_code == 0, f"concurrent-{index}: delete exit {removed.exit_code} (expected 0)")
    return check.finish()


SENTINEL_PATH = "/run/vz-reproducibility-sentinel"


def resolved_shape(payload: dict) -> dict:
    """The configuration a definition resolves to, with identities removed.

    Two Ups of one pinned definition must agree here exactly, which is only
    meaningful because every runtime identity is deliberately excluded.
    """
    environments = payload.get("environments") or []
    shape = {"definition_digest": payload.get("desired_definition_digest"),
             "project_name": payload.get("project_name"), "environments": []}
    for environment in environments:
        machines = []
        for machine in environment.get("machines") or []:
            machines.append({"name": machine.get("name"), "profile": machine.get("profile"),
                             "target": machine.get("target"), "state": machine.get("state")})
        shape["environments"].append({"name": environment.get("name"), "state": environment.get("state"),
                                      "machines": machines})
    return shape


def runtime_identities(payload: dict) -> dict:
    identities = {"environment": [], "machine": [], "incarnation": [], "engine": []}
    for environment in payload.get("environments") or []:
        identities["environment"].append(environment.get("environment_id"))
        for machine in environment.get("machines") or []:
            identities["machine"].append(machine.get("machine_id"))
            identities["incarnation"].append(machine.get("incarnation_id"))
            identities["engine"].append((machine.get("docker_context") or {}).get("engine_id"))
    return identities


def check_recreate_from_definition(ctx: CheckContext, top: str) -> SubCheck:
    """The pinned definition, recreated from fresh state, resolves the same.

    One Up writes a mutable sentinel into its Machine. After deletion a second
    Up of the very same definition bytes must resolve the same configuration and
    artifact digests, hand out entirely new runtime identities, and carry none
    of the sentinel data the first one left behind.
    """
    check = SubCheck(top, "recreate_from_definition")
    try:
        definition = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    token = "vzrepro-" + uuid.uuid4().hex[:16]
    first = provision(ctx, check, "rep-a", definition)
    if check.status != "PASS" or not first["status"]:
        return check.finish()
    written = ctx.run(check, "rep-sentinel-write",
                      ["exec", "--environment", "default", "--", "/bin/busybox", "sh", "-c",
                       f"printf %s {token} > {SENTINEL_PATH}; cat {SENTINEL_PATH}"],
                      cwd=first["project"], env=first["env"], timeout=120)
    check.check(written.exit_code == 0 and written.stdout.strip() == token.encode(),
                f"mutable sentinel written into the first Machine (exit {written.exit_code})")
    if check.status != "PASS":
        return check.finish()
    removed = ctx.run(check, "rep-a-delete",
                      ["--json", "delete", "--environment", "default", "--timeout", "120"],
                      cwd=first["project"], env=first["env"], timeout=DELETE_TIMEOUT)
    check.check(removed.exit_code == 0, f"first Environment deleted (exit {removed.exit_code})")
    if check.status != "PASS":
        return check.finish()
    # Same definition bytes, fresh state directory: nothing of the first Up's
    # runtime survives except what the definition itself pins.
    second = provision(ctx, check, "rep-b", definition)
    if check.status != "PASS" or not second["status"]:
        return check.finish()
    before, after = resolved_shape(first["status"]), resolved_shape(second["status"])
    check.check(before == after, "the recreated definition resolves the same topology, configuration and artifact digests"
                if before == after else f"resolved configuration differs: {json.dumps(before)[:200]} vs {json.dumps(after)[:200]}")
    old_identities, new_identities = runtime_identities(first["status"]), runtime_identities(second["status"])
    for kind in sorted(old_identities):
        old_values = [value for value in old_identities[kind] if value]
        new_values = [value for value in new_identities[kind] if value]
        check.check(old_values and new_values and not (set(old_values) & set(new_values)),
                    f"{kind} identities are entirely new after recreation "
                    f"({len(old_values)} before, {len(new_values)} after, {len(set(old_values) & set(new_values))} shared)")
    survivor = ctx.run(check, "rep-sentinel-absent",
                       ["exec", "--environment", "default", "--", "/bin/busybox", "sh", "-c",
                        f"cat {SENTINEL_PATH} 2>/dev/null; printf END"],
                       cwd=second["project"], env=second["env"], timeout=120)
    check.check(survivor.exit_code == 0 and survivor.stdout.strip() == b"END",
                f"no deleted sentinel data survives into the recreated Machine (observed {survivor.stdout[:60]!r})")
    if check.status == "PASS":
        final = ctx.run(check, "rep-b-delete",
                        ["--json", "delete", "--environment", "default", "--timeout", "120"],
                        cwd=second["project"], env=second["env"], timeout=DELETE_TIMEOUT)
        check.check(final.exit_code == 0, f"second Environment deleted (exit {final.exit_code})")
    return check.finish()


def sentinel_write(ctx: CheckContext, check: SubCheck, name: str, instance: dict, token: str) -> bool:
    row = ctx.run(check, name + "-sentinel-write",
                  ["exec", "--environment", "default", "--", "/bin/busybox", "sh", "-c",
                   f"printf %s {token} > {SENTINEL_PATH}; cat {SENTINEL_PATH}"],
                  cwd=instance["project"], env=instance["env"], timeout=120)
    check.check(row.exit_code == 0 and row.stdout.strip() == token.encode(),
                f"{name}: sentinel written into its Machine (exit {row.exit_code})")
    return row.exit_code == 0 and row.stdout.strip() == token.encode()


def sentinel_read(ctx: CheckContext, check: SubCheck, name: str, instance: dict):
    row = ctx.run(check, name + "-sentinel-read",
                  ["exec", "--environment", "default", "--", "/bin/busybox", "sh", "-c",
                   f"cat {SENTINEL_PATH} 2>/dev/null; printf END"],
                  cwd=instance["project"], env=instance["env"], timeout=120)
    return row


def check_delete_single_environment_safety(ctx: CheckContext, top: str) -> SubCheck:
    """Deleting one Environment leaves its neighbour serving and byte-identical.

    Two Environments are brought up and each writes its own sentinel. One is
    deleted; the survivor must still answer an exec (so it is serving, not
    merely recorded as ready), keep every runtime identity it had, and return
    its sentinel bytes unchanged. The scope is these two Environments and the
    surfaces named here, not a host-wide sweep.
    """
    check = SubCheck(top, "single_environment_safety")
    try:
        base = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    tokens, instances = {}, {}
    for name in ("del-a", "del-b"):
        definition = copy.deepcopy(base)
        definition["project_id"] = "prj_" + uuid.uuid4().hex
        instances[name] = provision(ctx, check, name, definition)
        if check.status != "PASS" or not instances[name]["status"]:
            return check.finish()
        tokens[name] = "vzdel-" + uuid.uuid4().hex[:16]
        if not sentinel_write(ctx, check, name, instances[name], tokens[name]):
            return check.finish()
    victim, survivor = instances["del-a"], instances["del-b"]
    before = runtime_identities(survivor["status"])
    removed = ctx.run(check, "del-a-delete", ["--json", "delete", "--environment", "default", "--timeout", "120"],
                      cwd=victim["project"], env=victim["env"], timeout=DELETE_TIMEOUT)
    check.check(removed.exit_code == 0, f"the selected Environment was deleted (exit {removed.exit_code})")
    if check.status != "PASS":
        return check.finish()
    gone = read_status(ctx, check, "del-a-after", project=victim["project"], env=victim["env"])
    check.check(not (gone or {}).get("environments"),
                f"the deleted Environment is gone (observed {[e.get('name') for e in (gone or {}).get('environments') or []]})")
    # The survivor must still answer, not merely be recorded as ready: an exec
    # exercises its daemon, its Machine and its runtime end to end.
    survived = read_status(ctx, check, "del-b-after", project=survivor["project"], env=survivor["env"])
    check.check(bool((survived or {}).get("environments")), "the other Environment is still reported")
    if not survived or not survived.get("environments"):
        return check.finish()
    check.check(all(row.get("state") == "ready" for row in survived["environments"]),
                f"the other Environment is still ready (observed {[r.get('state') for r in survived['environments']]})")
    after = runtime_identities(survived)
    for kind in sorted(before):
        old_values = [value for value in before[kind] if value]
        new_values = [value for value in after[kind] if value]
        check.check(old_values and old_values == new_values,
                    f"the other Environment kept its {kind} identity ({len(old_values)} before, {len(new_values)} after)")
    reread = sentinel_read(ctx, check, "del-b", survivor)
    check.check(reread.exit_code == 0 and reread.stdout.strip() == (tokens["del-b"] + "END").encode(),
                f"the other Environment returns byte-identical sentinel data (observed {reread.stdout[:60]!r})")
    if check.status == "PASS":
        final = ctx.run(check, "del-b-delete", ["--json", "delete", "--environment", "default", "--timeout", "120"],
                        cwd=survivor["project"], env=survivor["env"], timeout=DELETE_TIMEOUT)
        check.check(final.exit_code == 0, f"the other Environment deleted afterwards (exit {final.exit_code})")
    return check.finish()


PRIVATE_NETWORK = "backend"
# One probe, run on EVERY Machine on the network, before and after the fetch.
#
# Every line is a fact about ONE link in the chain, because the whole chain
# failing looks the same from the outside whichever link is broken:
#
#   CMDLINE <mac> <cidr>   what the HOST derived and wrote to this Machine.
#   IFACE <name> <cidr> <mac> <operstate> <carrier> <rx> <tx>
#                          what the GUEST configured. The MAC decides the pairing
#                          question: the switch assigns one address per port and
#                          drops every frame whose source is not it, so a NIC
#                          carrying a different MAC than its cmdline declared is
#                          a mis-paired descriptor and not a forwarding fault.
#                          operstate/carrier decide whether the link came up at
#                          all -- an address configured on a dead link prints
#                          identically to one on a live link. rx/tx decide the
#                          direction: frames counted out with none counted back
#                          is the host switch; none counted out is the guest.
#   ARP <ip> <flags> <mac> <dev>
#                          whether L2 resolved. ARP must resolve before IPv4
#                          flows, so an unresolved peer is a link-layer fact and
#                          a resolved one moves the question above the fabric.
#
# Everything reads a kernel file or one applet; nothing here needs a raw socket,
# so no applet beyond what the initramfs already relies on has to exist.
#
# `printf` is the shell's own, never an applet: the probe is run by `sh -c`, so
# the builtin is there by construction, while an applet is only there if this
# BusyBox was compiled with it. `ip`, `awk` and `cat` are addressed as
# `/bin/busybox <applet>` for the reason the initramfs already does -- a
# VirtioFS-backed overlay does not expose the applet symlinks.
FABRIC_PROBE = (
    'for p in $(/bin/busybox cat /proc/cmdline); do '
    '  case "$p" in vz.net.*=*) printf "CMDLINE %s\\n" "${p#*=}" ;; esac; '
    'done; '
    '/bin/busybox ip -o -4 addr show | /bin/busybox awk \'$2!="lo"{print $2, $4}\' '
    '| while read -r i c; do '
    '    s=/sys/class/net/"$i"; '
    '    m=$(/bin/busybox cat "$s"/address 2>/dev/null) || m="?"; '
    '    o=$(/bin/busybox cat "$s"/operstate 2>/dev/null) || o="?"; '
    '    k=$(/bin/busybox cat "$s"/carrier 2>/dev/null) || k="?"; '
    '    rx=$(/bin/busybox cat "$s"/statistics/rx_packets 2>/dev/null) || rx="?"; '
    '    tx=$(/bin/busybox cat "$s"/statistics/tx_packets 2>/dev/null) || tx="?"; '
    '    printf "IFACE %s %s %s %s %s %s %s\\n" "$i" "$c" "$m" "$o" "$k" "$rx" "$tx"; '
    '  done; '
    '/bin/busybox cat /proc/net/arp 2>/dev/null '
    '| /bin/busybox awk \'NR>1{print "ARP", $1, $3, $4, $6}\''
)


class FabricState:
    """One Machine's whole fabric-port state, as the probe reported it."""

    def __init__(self, receipt):
        self.declared = []   # [(mac, address, prefix), ...] from the kernel cmdline
        self.ifaces = []     # [{name, address, mac, operstate, carrier, rx, tx}, ...]
        self.arp = []        # [(address, flags, mac, device), ...]
        for line in receipt.stdout.decode("ascii", "replace").splitlines():
            row = line.split()
            if row[:1] == ["CMDLINE"] and len(row) == 2 and "," in row[1]:
                mac, _, cidr = row[1].partition(",")
                address, _, prefix = cidr.partition("/")
                self.declared.append((mac.lower(), address, prefix))
            elif row[:1] == ["IFACE"] and len(row) == 8:
                self.ifaces.append(dict(zip(("name", "address", "mac", "operstate", "carrier", "rx", "tx"),
                                            [row[1], row[2].split("/")[0], row[3].lower(), *row[4:]])))
            elif row[:1] == ["ARP"] and len(row) == 5:
                self.arp.append(tuple(row[1:]))

    def port(self):
        """The one interface carrying the one address the host derived, or None.

        Matched by address and never by name: a Machine carries Apple's NAT NIC,
        the fabric NIC and Docker's bridge, and which name the fabric NIC gets is
        not something the host can predict.
        """
        if len(self.declared) != 1:
            return None
        _, address, _ = self.declared[0]
        return next((row for row in self.ifaces if row["address"] == address), None)

    def evidence(self) -> str:
        return f"cmdline {self.declared!r}, interfaces {self.ifaces!r}, arp {self.arp!r}"


PRIVATE_PORT = 8080
WGET_TIMEOUT = 5
# How long the sibling's fetch may take to be attempted at all, and how long the
# listener is given to answer its own address before the fabric is blamed for a
# service that was never up.
LISTENER_ATTEMPTS = 10
LISTENER_INTERVAL = 1.0


def two_machine_definition(release_dir: Path) -> dict:
    """One Environment, two Machines on one declared private network."""
    definition = minimal_definition(release_dir)
    environment = definition["environment"]
    first = environment["machines"][0]
    second = copy.deepcopy(first)
    second["name"] = "machine-1"
    for machine in (first, second):
        machine["networks"] = [PRIVATE_NETWORK]
    environment["machines"] = [first, second]
    environment["networks"] = [{"schema_version": 1, "name": PRIVATE_NETWORK, "kind": "private"}]
    environment["endpoints"] = [{"schema_version": 1, "name": "probe", "machine": first["name"],
                                 "network": PRIVATE_NETWORK, "protocol": "tcp", "port": PRIVATE_PORT}]
    return definition


# Criterion 4 needs the Swift toolchain, so the crossing resolves the xcode
# channel rather than `latest`, which may point at a clean template.
MACOS_CHANNEL = "xcode"


def macos_target(release_dir: Path):
    """The Developer macOS target this release registers, or None.

    Criterion 5 requires a service path crossing between a Linux Machine and a
    native macOS Machine. A host with no registered macOS template cannot build
    that Machine at all, which is a fact about the host rather than about the
    runtime, so it is reported separately from a crossing that was attempted and
    failed.
    """
    try:
        catalog = load_json(release_dir / "machine-target-catalog.json")
    except (OSError, ValueError):
        return None
    entries = catalog.get("macos")
    if not isinstance(entries, list):
        return None
    # A macOS catalog entry is shaped unlike a Linux one: it carries `image`,
    # `version` and `channels` naming a registered template bundle, and no
    # `profile` or `digest` at all. `TargetSpec.digest` is optional precisely so
    # a native target can be resolved by image/version/channel instead.
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("image") != "vz-macos":
            continue
        channels = entry.get("channels")
        if isinstance(channels, list) and MACOS_CHANNEL in channels:
            return entry
    return None


def crossing_definition(release_dir: Path, macos_entry: dict) -> dict:
    """One Environment whose declared private path crosses Linux to macOS.

    This is the shape criterion 5 asks for and `two_machine_definition` cannot
    express: two Developer Linux Machines and one Developer native macOS
    Machine on one declared private network, with an endpoint on each side so
    the required path is declared in both directions. Until native macOS
    Machines were admitted to the fabric this definition could not even be
    written -- the schema capped a macOS Machine's `networks` at zero -- so a
    definition that validates is itself evidence that the declaration half of
    the crossing landed.
    """
    definition = two_machine_definition(release_dir)
    environment = definition["environment"]
    target = {"os": "macos", "arch": "aarch64", "image": macos_entry["image"],
              "channel": MACOS_CHANNEL}
    if macos_entry.get("version"):
        target["version"] = macos_entry["version"]
    native = {"schema_version": 1, "name": "machine-mac", "profile": "developer",
              "target": target,
              "resources": {"cpus": 2, "memory_mb": 4096},
              "networks": [PRIVATE_NETWORK]}
    environment["machines"] = [*environment["machines"], native]
    environment["endpoints"] = [*environment["endpoints"],
                                {"schema_version": 1, "name": "probe-mac", "machine": "machine-mac",
                                 "network": PRIVATE_NETWORK, "protocol": "tcp", "port": PRIVATE_PORT}]
    return definition


def machine_exec_argv(machine: str, script: str) -> list:
    return ["exec", "--environment", "default", "--machine", machine, "--", "/bin/busybox", "sh", "-c", script]


def machine_exec(ctx, check, label, instance, machine, script, *, timeout=120):
    return ctx.run(check, label, machine_exec_argv(machine, script),
                   cwd=instance["project"], env=instance["env"], timeout=timeout)


def hold_machine_exec(ctx, check, label, instance, machine, script, *, timeout=120):
    """Run a foreground process on a Machine for as long as the caller needs it.

    A Machine `exec` is bounded on purpose: the guest supervises the command as
    a child subreaper and SIGKILLs its whole process group and every descendant
    it adopted before the invocation reports, so `httpd` backgrounding itself is
    dead before `vz exec` prints `0`. A listener a sibling has to reach is
    therefore a foreground process held open across the sibling's fetch, and its
    death when the hold is released is the same mechanism doing its job.
    """
    return ctx.start(check, label, machine_exec_argv(machine, script),
                     cwd=instance["project"], env=instance["env"], timeout=timeout)


def check_private_topology_paths(ctx: CheckContext, top: str) -> SubCheck:
    """A declared private path serves inside its Environment and nowhere else.

    One Machine serves a token on a declared private network; its sibling in the
    same Environment must read exactly that token, and a Machine in a different
    Environment must fail to reach the very same address. The foreign probe uses
    the address rather than a name, so its failure is a routing fact and not an
    unresolved hostname.

    The claims are ordered so that the first one to fail names the broken link
    rather than the whole chain. Each Machine's port must carry the MAC its own
    cmdline declared, on a link that is up with carrier, before either is asked
    to carry traffic; the server must answer its OWN fabric address before the
    sibling is asked to reach it, because a listener that is not there presents
    exactly as a fabric that does not forward; and every fetch is bracketed by
    the probe, so a failure carries the ARP table and the interface counters
    that say whether frames crossed.
    """
    check = SubCheck(top, "private_topology_paths")
    try:
        # Criterion 5 wants the crossing, so the Environment carries the macOS
        # Machine whenever this release registers one. The Linux-to-Linux claims
        # below are unchanged either way: they are the same two Machines, on the
        # same declared network, and a third Machine on it does not weaken them.
        native = macos_target(ctx.release_dir)
        definition = (crossing_definition(ctx.release_dir, native) if native
                      else two_machine_definition(ctx.release_dir))
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if schema_path.is_file():
        problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(definition),
                          key=lambda e: list(map(str, e.absolute_path)))
        check.check(not problems, "the two-Machine private-network definition validates"
                    if not problems else f"definition invalid: {problems[0].message[:200]}")
        if problems:
            return check.finish()
    token = "vznet-" + uuid.uuid4().hex[:16]
    inside = provision(ctx, check, "net-a", definition)
    # vz 0.4 refuses a definition that declares networks, endpoints or workspace
    # projections: the adapters that would apply them are not implemented, and
    # Up performs no admission at all. That is a runtime gap, not a gap in this
    # check, so it is reported as not_implemented with the runtime's own words.
    # The check is otherwise complete and starts passing when the adapters land.
    if inside.get("unsupported"):
        check.not_implemented = ("declared networks and endpoints are not applied by this runtime: " +
                                 inside["unsupported"][:300])
        return check.finish()
    if check.status != "PASS" or not inside["status"]:
        return check.finish()
    names = [m.get("name") for e in inside["status"]["environments"] for m in e.get("machines") or []]
    expected = sorted(m["name"] for m in definition["environment"]["machines"])
    check.check(sorted(names) == expected, f"every declared Machine is present (observed {names}, declared {expected})")
    if check.status != "PASS":
        return check.finish()
    # Both ports are judged BEFORE anything is asked to carry traffic. Every
    # value compared here is one the host derived and wrote to that Machine's
    # own kernel cmdline, so a mismatch names the mis-paired descriptor rather
    # than leaving it to look like a forwarding fault. Name-based selection is
    # not sound -- a Machine carries Apple's NAT eth0, the fabric NIC and
    # Docker's bridge, and the host cannot predict which name the fabric NIC
    # gets -- so the interface is found by the address the cmdline declared.
    ports = {}
    for machine in ("machine-0", "machine-1"):
        probed = machine_exec(ctx, check, "net-port-" + machine, inside, machine, FABRIC_PROBE)
        state = FabricState(probed)
        port = state.port()
        check.check(probed.exit_code == 0 and port is not None,
                    f"{machine} carries the one fabric address the host derived ({state.evidence()})")
        if port is None:
            return check.finish()
        declared_mac = state.declared[0][0]
        # The switch assigns one address per port and refuses every frame whose
        # source is not it, so a NIC holding a different MAC than the cmdline
        # declared is handed a port the switch assigned to somebody else.
        check.check(port["mac"] == declared_mac,
                    f"{machine}'s fabric NIC {port['name']} carries the MAC the host planned "
                    f"(cmdline {declared_mac}, interface {port['mac']})")
        # An address on a link that never came up prints exactly like an address
        # on a live one, which is why this is asserted and not merely reported.
        check.check(port["operstate"] == "up" and port["carrier"] == "1",
                    f"{machine}'s fabric NIC {port['name']} is up with carrier "
                    f"(operstate {port['operstate']}, carrier {port['carrier']})")
        ports[machine] = port
    address, sibling_address = ports["machine-0"]["address"], ports["machine-1"]["address"]
    check.check(address != sibling_address,
                f"the two Machines hold distinct fabric addresses ({address}, {sibling_address})")
    macs = {machine: port["mac"] for machine, port in ports.items()}
    check.check(len(set(macs.values())) == 2, f"the two fabric ports hold distinct MACs ({macs})")
    if check.status != "PASS":
        return check.finish()
    # Held open, not backgrounded. A Machine exec supervises its command as a
    # child subreaper and SIGKILLs every descendant before it reports, so a
    # daemonised `httpd` is already dead when the sibling fetches -- which reads
    # exactly like a fabric that does not forward. The listener lives for as
    # long as this invocation is held and no longer.
    server = hold_machine_exec(ctx, check, "net-serve", inside, "machine-0",
                               f"/bin/busybox mkdir -p /www; printf %s {token} > /www/index.html; "
                               f"/bin/busybox httpd -f -p {PRIVATE_PORT} -h /www")
    try:
        # The server answering its OWN fabric address settles the listener
        # before the fabric is asked to carry anything: this fetch never leaves
        # machine-0, so it fails only if nothing is bound.
        for attempt in range(1, LISTENER_ATTEMPTS + 1):
            local = machine_exec(ctx, check, "net-serve-local", inside, "machine-0",
                                 f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{PRIVATE_PORT}/")
            if local.exit_code == 0 and local.stdout.strip() == token.encode():
                break
            time.sleep(LISTENER_INTERVAL)
        check.check(local.exit_code == 0 and local.stdout.strip() == token.encode(),
                    f"the private endpoint answers on machine-0's own fabric address after {attempt} "
                    f"attempt(s) (exit {local.exit_code}, {local.stdout[:80]!r})")
        if check.status != "PASS":
            return check.finish()
        sibling = machine_exec(ctx, check, "net-sibling", inside, "machine-1",
                               f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{PRIVATE_PORT}/")
        # Read back on both sides whatever the fetch did or did not do. Counted
        # frames and a resolved ARP entry are the difference between a fabric
        # that never carried the frame and a guest that refused it, so they are
        # recorded on the pass as well: the numbers are the claim's evidence,
        # not only its post-mortem.
        after = {machine: FabricState(machine_exec(ctx, check, "net-after-" + machine, inside, machine, FABRIC_PROBE))
                 for machine in ("machine-0", "machine-1")}
        for machine, state in after.items():
            check.ok(f"{machine} after the sibling fetch: {state.evidence()}")
        check.check(sibling.exit_code == 0 and sibling.stdout.strip() == token.encode(),
                    f"the sibling Machine reads the declared private path (exit {sibling.exit_code})")
        outside = provision(ctx, check, "net-b", minimal_definition(ctx.release_dir))
        if check.status != "PASS" or not outside["status"]:
            return check.finish()
        foreign = machine_exec(ctx, check, "net-foreign", outside, "machine-0",
                               f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{PRIVATE_PORT}/; "
                               "printf ':%s' $?")
        check.check(foreign.exit_code == 0 and token.encode() not in foreign.stdout and
                    not foreign.stdout.strip().endswith(b":0"),
                    f"a Machine in another Environment cannot reach that address (observed {foreign.stdout[:80]!r})")
        # The crossing runs last, on the same Environment and the same declared
        # network the Linux half just proved, so a failure here is about the
        # macOS Machine and not about the fabric existing.
        if check.status == "PASS" and native is not None:
            check_macos_crossing(ctx, check, inside, token, "machine-0", address)
    finally:
        released = ctx.release(check, server)
        # `None` is the one uncertain outcome: the invocation outlived SIGKILL,
        # so something this lane started may still be running.
        check.check(released.exit_code is not None,
                    f"the held listener was released (exit {released.exit_code})")
    if check.status == "PASS":
        for name, instance in (("net-a", inside), ("net-b", outside)):
            removed = ctx.run(check, name + "-delete",
                              ["--json", "delete", "--environment", "default", "--timeout", "120"],
                              cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
            check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")
    # Everything above proves the Linux half. Criterion 5 also requires that "at
    # least one required service path crosses between a Linux Machine and a
    # native macOS Machine in both directions permitted by its declarations",
    # and none of it attempted that. Reporting PASS here would certify the
    # criterion on evidence that never touched its macOS clause, so the crossing
    # is claimed last and only when it was actually exercised.
    #
    # This deliberately cannot pass on a host with no registered macOS template.
    # The declaration half landed -- `crossing_definition` is a definition the
    # schema now accepts, which it did not before native macOS Machines were
    # admitted to the fabric -- but a definition that validates is not a path
    # that carries traffic.
    if check.status == "PASS" and native is None:
        check.not_implemented = (
            "criterion 5 requires a required service path crossing between a Linux Machine and a "
            "native macOS Machine in both directions; this release registers no Developer macOS "
            "target, so no macOS Machine could be built and the crossing was never attempted. "
            "The Linux-to-Linux half above passed. Register a template with vz-macos-setup "
            "(planning/developer-environments/macos-local-setup.md); this check never provisions one.")
    return check.finish()


# The macOS guest is addressed by `native_macos::fabric` over the agent channel
# rather than by a kernel cmdline, so its port is read from the interface list
# and not from `/proc/cmdline`. `ifconfig -a` is used because it is present on a
# stock macOS with no developer tools, which the clean template is.
MACOS_FABRIC_PROBE = "/sbin/ifconfig -a"


def macos_fabric_port(output: bytes, network: str):
    """(interface, mac, address) for the macOS NIC on `network`, or None.

    The interface is identified by the address it holds, never by name: a macOS
    guest enumerates en0/en1/en2 in an order the host does not control, and the
    fabric NIC is whichever one came up on the Environment's subnet.
    """
    interface = mac = None
    loopback = False
    for raw in output.decode("utf-8", "replace").splitlines():
        if raw and not raw[0].isspace():
            interface, mac = raw.split(":", 1)[0], None
            # A fabric port is never the loopback, so it is refused here rather
            # than relying on every caller to pass a subnet that cannot match
            # it. `lo0` holding 127.0.0.1 is otherwise a perfectly good match
            # for the shape of this search.
            loopback = "LOOPBACK" in raw
        elif raw.strip().startswith("ether "):
            mac = raw.split()[1]
        elif raw.strip().startswith("inet ") and interface is not None and not loopback:
            address = raw.split()[1]
            if address.rsplit(".", 1)[0] == network:
                return interface, mac, address
    return None


def check_macos_crossing(ctx: CheckContext, check: SubCheck, inside: dict, token: str,
                         linux_machine: str, linux_address: str) -> None:
    """A required service path crosses Linux to macOS in BOTH declared directions.

    GOAL-0.4.0.md criterion 5. Each direction is served by a foreground listener
    held open across the peer's fetch, for the same reason the Linux half holds
    one: `vz exec` reaps the whole process group, so a backgrounded listener is
    dead before the invocation reports.

    Both directions are required. One of them passing would prove the switch
    forwards, but not that the macOS Machine is a peer on the fabric rather than
    a client of it, which is what "in both directions permitted by its
    declarations" asks for.
    """
    probed = machine_exec(ctx, check, "net-port-machine-mac", inside, "machine-mac", MACOS_FABRIC_PROBE)
    network = linux_address.rsplit(".", 1)[0]
    port = macos_fabric_port(probed.stdout, network) if probed.exit_code == 0 else None
    check.check(port is not None,
                f"the macOS Machine holds an address on the Environment's fabric subnet {network}.0/24 "
                f"(exit {probed.exit_code})")
    if port is None:
        return
    interface, mac, mac_address = port
    check.ok(f"macOS fabric port: {interface} {mac} {mac_address}")
    check.check(mac_address != linux_address,
                f"the macOS Machine holds its own address, distinct from {linux_machine}'s "
                f"({mac_address}, {linux_address})")

    # Direction 1: the macOS Machine serves, a Linux Machine reads.
    served = hold_machine_exec(ctx, check, "net-serve-mac", inside, "machine-mac",
                               f"printf %s {token} > /tmp/vz-crossing; "
                               f"while true; do /usr/bin/nc -l {PRIVATE_PORT} < /tmp/vz-crossing; done")
    try:
        fetched = None
        for _ in range(LISTENER_ATTEMPTS):
            fetched = machine_exec(ctx, check, "net-cross-to-mac", inside, linux_machine,
                                   f"/bin/busybox nc -w 5 {mac_address} {PRIVATE_PORT}")
            if fetched.exit_code == 0 and fetched.stdout.strip() == token.encode():
                break
            time.sleep(LISTENER_INTERVAL)
        check.check(fetched is not None and fetched.exit_code == 0
                    and fetched.stdout.strip() == token.encode(),
                    f"{linux_machine} reads the declared path served by the macOS Machine "
                    f"(exit {None if fetched is None else fetched.exit_code}, "
                    f"{b'' if fetched is None else fetched.stdout[:80]!r})")
    finally:
        released = ctx.release(check, served)
        check.check(released.exit_code is not None,
                    f"the macOS listener was released (exit {released.exit_code})")

    # Direction 2: a Linux Machine serves, the macOS Machine reads.
    reverse = hold_machine_exec(ctx, check, "net-serve-linux-cross", inside, linux_machine,
                                f"/bin/busybox sh -c 'printf %s {token} > /tmp/vz-crossing; "
                                f"while true; do /bin/busybox nc -l -p {PRIVATE_PORT} < /tmp/vz-crossing; done'")
    try:
        back = None
        for _ in range(LISTENER_ATTEMPTS):
            back = machine_exec(ctx, check, "net-cross-from-mac", inside, "machine-mac",
                                f"/usr/bin/nc -w 5 {linux_address} {PRIVATE_PORT}")
            if back.exit_code == 0 and back.stdout.strip() == token.encode():
                break
            time.sleep(LISTENER_INTERVAL)
        check.check(back is not None and back.exit_code == 0 and back.stdout.strip() == token.encode(),
                    f"the macOS Machine reads the declared path served by {linux_machine} "
                    f"(exit {None if back is None else back.exit_code}, "
                    f"{b'' if back is None else back.stdout[:80]!r})")
    finally:
        released = ctx.release(check, reverse)
        check.check(released.exit_code is not None,
                    f"the Linux listener was released (exit {released.exit_code})")


def check_bootstrap_creates_default(ctx: CheckContext, top: str) -> SubCheck:
    """A real `vz up` from a bare definition must create the `default` Environment.

    This is the lane's first provisioning check, so it owns the whole lifecycle:
    it brings one Developer Machine up from a definition that names no
    Environment, reads the persisted topology back, and deletes what it created.
    A failure never deletes: the Machines stay for inspection and the lane's own
    leak detection reports them.
    """
    check = SubCheck(top, "bootstrap_creates_default")
    try:
        definition = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    # The definition names machines and no Environment: `default` must be the
    # Engine's own doing, not something this check asked for by name.
    check.check("environments" not in definition["environment"] and "name" not in definition["environment"],
                "the bootstrap definition names no Environment, so `default` can only come from Up")
    data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
    iso = ctx.isolated("boot", project_files={"vz.json": data}, provision=True)
    env, project = iso["env"], iso["project"]
    # AF_UNIX truncates silently past 103 bytes, and the daemon then reports a
    # bare daemon_unavailable that looks like a provisioning defect. Say what it
    # actually is: this lane's state root is too deep to host a socket.
    socket = Path(env["VZ_RUNTIME_DAEMON_SOCKET"])
    length = len(str(socket).encode())
    check.check(length <= SOCKET_PATH_LIMIT,
                f"provisioning socket path is {length} bytes (limit {SOCKET_PATH_LIMIT}): {socket}")
    if check.status != "PASS":
        return check.finish()
    # A workspace is resolved through git, so an Up outside a repository fails
    # with workspace_read_failed before any Machine is provisioned. The lane's
    # env deliberately has no global or system git config, so identity is
    # supplied per invocation rather than read from the host.
    for label, argv in (("bootstrap-git-init", [GIT, "init", "--quiet", "--initial-branch", "main"]),
                        ("bootstrap-git-add", [GIT, "add", "vz.json"]),
                        ("bootstrap-git-commit", [GIT, "-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                                                  "commit", "--quiet", "-m", "bootstrap definition"])):
        receipt = ctx.run_tool(check, label, argv, cwd=project, env=env)
        check.check(receipt.exit_code == 0, f"{label}: exit {receipt.exit_code} (expected 0)")
    if check.status != "PASS":
        return check.finish()
    up = ctx.run(check, "bootstrap-up", ["--json", "up"], cwd=project, env=env, timeout=UP_TIMEOUT)
    check.check(up.exit_code == 0, f"vz --json up: exit {up.exit_code} (expected 0)")
    if up.exit_code != 0:
        return check.finish()
    status = ctx.run(check, "bootstrap-status", ["--json", "status"], cwd=project, env=env, timeout=60)
    check.check(status.exit_code == 0, f"vz --json status: exit {status.exit_code} (expected 0)")
    payload = None
    try:
        # A success payload is pretty-printed across many lines; only the error
        # envelope is a single line, so this is a whole-document parse.
        payload = json.loads(status.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        check.fail(f"vz --json status did not emit a JSON document: {error}")
    if not isinstance(payload, dict):
        return check.finish()
    environments = payload.get("environments") or []
    names = sorted(row.get("name") for row in environments if isinstance(row, dict))
    check.check(names == ["default"], f"Up created exactly the `default` Environment (observed {names})")
    machines = [m for row in environments for m in (row.get("machines") or []) if isinstance(m, dict)]
    check.check(len(machines) == 1 and machines[0].get("name") == "machine-0",
                f"the definition's single Machine is present (observed {[m.get('name') for m in machines]})")
    check.check(payload.get("topology_state_source") == "persisted",
                f"topology read from persisted state (observed {payload.get('topology_state_source')!r})")
    check.check(all(row.get("state") == "ready" for row in environments),
                f"every Environment reached ready (observed {[row.get('state') for row in environments]})")
    # Delete only after the claim is decided, and only if it held: a failed
    # check leaves the Machines for inspection.
    if check.status == "PASS":
        removed = ctx.run(check, "bootstrap-delete", ["--json", "delete", "--environment", "default", "--timeout", "120"],
                          cwd=project, env=env, timeout=DELETE_TIMEOUT)
        check.check(removed.exit_code == 0, f"vz --json delete --environment default: exit {removed.exit_code} (expected 0)")
        after = ctx.run(check, "bootstrap-status-after-delete", ["--json", "status"], cwd=project, env=env, timeout=60)
        surviving = None
        if after.exit_code == 0:
            try:
                surviving = [row.get("name") for row in json.loads(after.stdout.decode("utf-8")).get("environments") or []]
            except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
                surviving = ["<unparsable status>"]
        check.check(not surviving, f"no Environment survives the delete (observed {surviving})")
    return check.finish()


def _commands_section(help_text: str) -> list:
    if "Commands:\n" not in help_text:
        return []
    section = help_text.split("Commands:\n", 1)[1].split("\n\n", 1)[0]
    return [line.split()[0] for line in section.splitlines() if line.strip()]


def _tokens(text: str) -> set:
    out = set()
    for raw in text.replace(",", " ").replace("=", " ").split():
        out.add(raw)
    return out


def check_help_surface(ctx: CheckContext, top: str) -> SubCheck:
    check = SubCheck(top, "help_surface_exact")
    iso = ctx.isolated("help", project_files={"vz.json": INVALID_DEFINITION, "sentinel": SENTINEL})
    env = dict(iso["env"], VZ_RUNTIME_DAEMON_AUTOSTART="definitely-not-a-boolean")
    removed_roots = set(ctx.cli_removal["removed_roots"])
    removed_flags = set(ctx.cli_removal["removed_root_flags"])
    before = inventory(iso["root"])
    root_help = None
    for argv in (["--help"], ["help"]):
        receipt = ctx.run(check, "help-" + "-".join(argv), argv, cwd=iso["project"], env=env)
        text = receipt.stdout.decode("utf-8", "replace")
        check.check(receipt.exit_code == 0 and receipt.stderr == b"", f"vz {' '.join(argv)}: exit 0, empty stderr")
        commands = [name for name in _commands_section(text) if name != "help"]
        check.check(set(commands) == set(FIVE_VERBS) and len(commands) == 5,
                    f"vz {' '.join(argv)}: Commands section is exactly {sorted(FIVE_VERBS)} (+help): {commands}")
        check.check(set(commands) == set(ctx.cli_removal["required_release_roots"]), "Commands equal cli-removal required_release_roots")
        exposed = sorted(removed_roots & set(_commands_section(text)))
        check.check(not exposed, "no removed root listed as a command" if not exposed else f"removed roots listed: {exposed}")
        flags = sorted(removed_flags & _tokens(text))
        check.check(not flags, "no removed root flag token in root help" if not flags else f"removed flags exposed: {flags}")
        if root_help is None:
            root_help = receipt.stdout
        else:
            check.check(receipt.stdout == root_help, "`vz help` output identical to `vz --help`")
    for verb in FIVE_VERBS:
        receipt = ctx.run(check, f"help-{verb}", [verb, "--help"], cwd=iso["project"], env=env)
        text = receipt.stdout.decode("utf-8", "replace")
        check.check(receipt.exit_code == 0 and receipt.stderr == b"" and f"Usage: vz {verb}" in text,
                    f"vz {verb} --help: exit 0, empty stderr, names `vz {verb}` usage")
        nested = sorted(removed_roots & set(_commands_section(text)))
        check.check(not nested, f"vz {verb} --help exposes no removed root as a nested command" if not nested else f"vz {verb}: nested {nested}")
        flags = sorted(removed_flags & _tokens(text))
        check.check(not flags, f"vz {verb} --help exposes no removed root flag" if not flags else f"vz {verb}: flags {flags}")
    receipt = ctx.run(check, "version", ["--version"], cwd=iso["project"], env=env)
    check.check(receipt.exit_code == 0 and receipt.stdout.startswith(b"vz ") and receipt.stderr == b"", f"vz --version: {receipt.stdout.decode('utf-8', 'replace').strip()!r}")
    _unchanged(check, "help isolated root", before, iso["root"])
    return check.finish()


def check_error_envelope(ctx: CheckContext, top: str) -> SubCheck:
    check = SubCheck(top, "error_envelope_agreement")
    iso = ctx.isolated("envelope", project_files={})
    env = iso["env"]
    before = inventory(ctx.state.root)
    codes = {}
    for label, argv in (("up-json", ["--json", "up"]), ("up-text", ["up"]), ("status-json", ["--json", "status"]),
                        ("status-text", ["status"]), ("exec", ["exec", "--no-stdin", "--", "/usr/bin/true"]),
                        ("stop", ["--json", "stop"]), ("delete", ["--json", "delete"])):
        receipt = ctx.run(check, "envelope-" + label, argv, cwd=iso["project"], env=env, timeout=30)
        try:
            payload = _single_json_line(receipt.stderr)
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = None
        error = payload.get("error") if isinstance(payload, dict) else None
        code = error.get("code") if isinstance(error, dict) else None
        codes[label] = code
        check.check(receipt.exit_code == 2 and receipt.stdout == b"" and isinstance(error, dict) and {"code", "message"} <= set(error)
                    and isinstance(error.get("message"), str) and error["message"],
                    f"vz {' '.join(argv)}: exit 2, empty stdout, one-line stderr JSON {{error:{{code,message,...}}}} code={code!r}")
    check.check(len(set(codes.values())) == 1 and codes["up-json"] == "definition_not_found",
                f"all five verbs agree on the same structured code in a clean directory: {codes}")
    _unchanged(check, "lane state root", before, ctx.state.root)
    check.check(os.listdir(iso["project"]) == [], "clean project directory still empty")
    return check.finish()


# The exact success-payload field set of `vz status --json` over a live
# topology, observed on the installed 0.4 binaries. The nested Environment and
# Machine sets are declared here too: comparing only the top level accepts any
# projection at all under `environments`, so the per-Machine identity, profile,
# target and capability projection that criterion 2 reads was unpinned.
# `*_OPTIONAL` names the fields the Rust structs mark `skip_serializing_if`;
# every other field must appear. `test_developer_environment_e2e` reads all
# twelve sets out of `crates/vz-cli/src/commands/dev_status.rs` rather than
# restating them, so the next drift fails offline instead of on the gate host.
#
# The network, attachment and endpoint sets are declared for the same reason the
# Machine set is: criterion 2 reads topology and endpoints out of this document,
# and a comparison that stopped at `environments` would accept a `networks` list
# of empty objects.
STATUS_FIELDS = {"schema_version", "request_id", "topology_state_source", "definition_path", "project_id",
                 "project_name", "host", "daemon", "desired_definition_digest", "persisted_definition_digest",
                 "definition_drift", "selection_source", "environments"}
STATUS_OPTIONAL_FIELDS = {"selection_source"}
ENVIRONMENT_FIELDS = {"environment_id", "name", "state", "definition_digest", "lifecycle_generation", "machines",
                      "networks", "network_attachments", "endpoints"}
ENVIRONMENT_OPTIONAL_FIELDS: set[str] = set()
MACHINE_FIELDS = {"machine_id", "name", "state", "profile", "target", "requested_capabilities",
                  "negotiated_capabilities", "health", "backend", "incarnation_id", "incarnation_generation",
                  "docker_context", "docker_context_availability", "fork"}
# `fork` is present exactly when this Machine is a fork of another in the same
# Environment, carrying the parent's id and name and the fork's label. It is how
# an agent tells "a fork of the Machine I wanted" from "a Machine whose name
# happens to contain an @", so the lineage is reported rather than only the
# composed name.
MACHINE_OPTIONAL_FIELDS = {"backend", "incarnation_id", "incarnation_generation", "docker_context",
                           "docker_context_availability", "fork"}
NETWORK_FIELDS = {"network_id", "name", "kind", "cidr"}
NETWORK_OPTIONAL_FIELDS = {"cidr"}
ATTACHMENT_FIELDS = {"attachment_id", "machine_id", "network_id"}
ATTACHMENT_OPTIONAL_FIELDS: set[str] = set()
ENDPOINT_FIELDS = {"endpoint_id", "name", "machine_id", "network_id", "protocol", "port", "hostname"}
ENDPOINT_OPTIONAL_FIELDS = {"hostname"}
# Every reading `MachineHealth` can take. `supervised` is the only one a Machine
# this lane just brought up may report; the rest exist so a check can say which
# wrong answer it saw instead of only that the answer was wrong.
MACHINE_HEALTH_READINGS = {"supervised", "unsupervised", "diverged", "inactive", "unobservable"}


def _compare_field_set(check: SubCheck, label: str, observed: set, declared: set, optional: set) -> None:
    """One object's keys against its declared set, honouring skipped optionals.

    A skipped optional may be absent, but nothing may be present that is not
    declared: an undeclared key is exactly how a field reaches users without the
    harness ever having been told about it.
    """
    missing = sorted((declared - optional) - observed)
    unexpected = sorted(observed - declared)
    check.check(not missing and not unexpected,
                f"{label} emits exactly its declared field set" if not missing and not unexpected else
                f"{label} field set differs: missing {missing}, unexpected {unexpected}")


def check_status_field_set(ctx: CheckContext, top: str) -> SubCheck:
    """`vz status --json` over a live topology emits exactly its declared fields.

    Extra fields are as much a contract break as missing ones, so the set is
    compared exactly rather than by presence, at every level of the document and
    not only the top: criterion 2 reads per-Machine profile, target, capabilities
    and Docker context, and a top-level-only comparison would accept a Machine
    projection missing all four. The digests must agree with each other and with
    an undrifted definition, which is what makes them evidence rather than two
    unrelated strings.
    """
    check = SubCheck(top, "status_json_field_set")
    try:
        definition = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    instance = provision(ctx, check, "stat", definition)
    if instance.get("unsupported"):
        check.not_implemented = "this runtime refused the definition: " + instance["unsupported"][:200]
        return check.finish()
    if check.status != "PASS" or not instance["status"]:
        return check.finish()
    payload = instance["status"]
    _compare_field_set(check, "status", set(payload), STATUS_FIELDS, STATUS_OPTIONAL_FIELDS)
    environments = payload.get("environments")
    check.check(isinstance(environments, list) and len(environments) == 1,
                f"one Environment is reported (observed {len(environments) if isinstance(environments, list) else None})")
    machines = None
    if check.status == "PASS":
        environment = environments[0]
        _compare_field_set(check, "environment", set(environment), ENVIRONMENT_FIELDS, ENVIRONMENT_OPTIONAL_FIELDS)
        machines = environment.get("machines")
        check.check(isinstance(machines, list) and len(machines) == 1,
                    f"one Machine is reported (observed {len(machines) if isinstance(machines, list) else None})")
    if check.status == "PASS" and machines:
        machine = machines[0]
        _compare_field_set(check, "machine", set(machine), MACHINE_FIELDS, MACHINE_OPTIONAL_FIELDS)
        # A Ready Machine is one activation produced, and activation records a
        # backend and an incarnation together (`machine_matches_activation`);
        # a persisted Ready Machine without a current incarnation is refused
        # outright. So for this Machine the two are not optional, and asserting
        # that is what stops the optional allowance above from excusing a
        # projection that simply dropped them.
        check.check(machine.get("state") == "ready", f"the Machine is Ready (observed {machine.get('state')!r})")
        check.check(isinstance(machine.get("backend"), str) and isinstance(machine.get("incarnation_id"), str)
                    and isinstance(machine.get("incarnation_generation"), int),
                    "a Ready Machine names its backend and current incarnation (observed "
                    f"{machine.get('backend')!r}, {machine.get('incarnation_id')!r}, "
                    f"{machine.get('incarnation_generation')!r})")
        # Target-qualified, per criterion 2: the profile and the target OS are
        # reported for this Machine and are the ones the definition declared,
        # not a daemon-wide or sibling-derived value.
        declared = definition["environment"]["machines"][0]
        check.check(machine.get("profile") == declared["profile"] and
                    isinstance(machine.get("target"), dict) and
                    machine["target"].get("os") == declared["target"]["os"] and
                    machine["target"].get("arch") == declared["target"]["arch"],
                    f"the Machine is target-qualified as declared (observed {machine.get('profile')!r} "
                    f"{(machine.get('target') or {}).get('os')!r}/{(machine.get('target') or {}).get('arch')!r})")
        for field in ("requested_capabilities", "negotiated_capabilities"):
            value = machine.get(field)
            check.check(isinstance(value, dict) and isinstance(value.get("capabilities"), list),
                        f"{field} is a capability set (observed {value!r})")
    check.check(payload.get("schema_version") == 1 and isinstance(payload.get("request_id"), str) and
                payload["request_id"], "schema_version 1 and a request_id")
    check.check(payload.get("definition_path", "").endswith("/vz.json"),
                f"definition_path names the read definition (observed {payload.get('definition_path')!r})")
    check.check(isinstance(payload.get("host"), dict) and set(payload["host"]) == {"os", "arch"},
                f"host carries exactly os and arch (observed {sorted(payload.get('host') or {})})")
    check.check(isinstance(payload.get("daemon"), dict) and {"backend_name", "version"} <= set(payload["daemon"]),
                f"daemon names its backend and version (observed {sorted(payload.get('daemon') or {})})")
    desired, persisted = payload.get("desired_definition_digest"), payload.get("persisted_definition_digest")
    check.check(isinstance(desired, str) and desired.startswith("sha256:") and desired == persisted and
                payload.get("definition_drift") is False,
                "an undrifted topology reports one digest as both desired and persisted "
                f"(desired {str(desired)[:16]}, persisted {str(persisted)[:16]}, drift {payload.get('definition_drift')})")
    check.check(payload.get("topology_state_source") == "persisted" and payload.get("selection_source") == "workspace",
                f"state and selection sources are named (observed {payload.get('topology_state_source')!r}, "
                f"{payload.get('selection_source')!r})")
    if check.status == "PASS":
        removed = ctx.run(check, "stat-delete", ["--json", "delete", "--environment", "default", "--timeout", "120"],
                          cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
        check.check(removed.exit_code == 0, f"deleted (exit {removed.exit_code})")
    return check.finish()


PROBE = "vz-runtime-probe"


def probe_documents(receipt) -> list:
    """Every JSON document the probe wrote, in order.

    The probe prints one document per line, including its error envelope, so a
    caller never has to distinguish "failed" from "said nothing".
    """
    documents = []
    for line in receipt.stdout.decode("utf-8", "replace").splitlines():
        if line.strip():
            documents.append(json.loads(line))
    return documents


def _typed_machine(machine: dict) -> dict:
    """A typed MachineInstance in the shape the CLI projects it.

    The CLI flattens `incarnation` into two scalars; comparing the nested and
    flattened spellings directly would report a disagreement that is only a
    difference in serialization.
    """
    incarnation = machine.get("incarnation")
    return {
        "machine_id": machine.get("machine_id"),
        "name": machine.get("name"),
        "state": machine.get("state"),
        "profile": machine.get("profile"),
        "target": machine.get("target"),
        "requested_capabilities": machine.get("requested_capabilities"),
        "negotiated_capabilities": machine.get("negotiated_capabilities"),
        "backend": machine.get("backend"),
        "incarnation_id": (incarnation or {}).get("incarnation_id"),
        "incarnation_generation": (incarnation or {}).get("generation"),
        "docker_context": machine.get("docker_context"),
    }


def _cli_machine(machine: dict) -> dict:
    """One `vz status --json` Machine reduced to the same comparable fields."""
    return {key: machine.get(key) for key in _typed_machine({})}


def check_grpc_agreement(ctx: CheckContext, top: str) -> SubCheck:
    """The CLI's account of an Environment and the daemon's typed channel agree.

    Criterion 15 is an agreement claim, and agreement needs two independent
    speakers. `vz-runtime-probe` is the second: a shipped release component
    that speaks the daemon's own gRPC channel, decodes the contract types off
    the wire, and shares no projection code with `vz`. Comparing the CLI's JSON
    against the CLI's own state store would prove only that the CLI is
    self-consistent.

    The CLI brings an Environment up. The probe then reads the same aggregate
    (identities, topology, capabilities) and reconciles the same definition
    bytes over the typed channel (admission identities, transitions, terminal
    receipt). The identities must be the ones the CLI already published --
    a typed Up that minted new ones would mean the two channels disagree about
    what the Environment is.
    """
    check = SubCheck(top, "grpc_api_live_agreement")
    probe = ctx.release_dir / "bin" / PROBE
    if not probe.is_file() or probe.is_symlink():
        check.fail(f"the release ships no {PROBE}; CLI/API agreement has no typed channel to observe")
        return check.finish()
    check.ok(f"typed client bin/{PROBE} sha256={digest_file(probe)}")
    try:
        definition = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    instance = provision(ctx, check, "agree", definition)
    if instance.get("unsupported"):
        check.not_implemented = "this runtime refused the definition: " + instance["unsupported"][:200]
        return check.finish()
    if check.status != "PASS" or not instance["status"]:
        return check.finish()
    env, project, status = instance["env"], instance["project"], instance["status"]
    state_db, socket = env["VZ_RUNTIME_STATE_DB"], env["VZ_RUNTIME_DAEMON_SOCKET"]
    environments = status.get("environments") or []
    if not check.check(len(environments) == 1, f"the CLI reports one Environment (observed {len(environments)})"):
        return check.finish()
    cli_environment = environments[0]
    cli_machines = {machine["name"]: _cli_machine(machine) for machine in cli_environment.get("machines") or []}

    read = ctx.run_tool(check, "agree-probe-state",
                        [str(probe), "state", "--state-db", state_db, "--socket", socket,
                         "--project-id", definition["project_id"]],
                        cwd=project, env=env, timeout=120)
    if not check.check(read.exit_code == 0,
                       f"the typed channel returned the aggregate (exit {read.exit_code}, "
                       f"stdout {read.stdout[:200]!r})"):
        return check.finish()
    try:
        documents = probe_documents(read)
    except json.JSONDecodeError as error:
        check.fail(f"the typed client did not emit JSON documents: {error}")
        return check.finish()
    if not check.check(len(documents) == 1 and documents[0].get("kind") == "vz-runtime-probe-state",
                       f"one typed state document (observed {[d.get('kind') for d in documents]})"):
        return check.finish()
    typed = documents[0]["project"]
    check.check(typed.get("definition", {}).get("project_id") == status.get("project_id"),
                f"both channels name project {status.get('project_id')!r} "
                f"(typed {typed.get('definition', {}).get('project_id')!r})")
    typed_environments = typed.get("environments") or []
    if not check.check(len(typed_environments) == len(environments),
                       f"both channels report {len(environments)} Environment(s) "
                       f"(typed {len(typed_environments)})"):
        return check.finish()
    typed_environment = typed_environments[0]
    # Criterion 2 put the declared topology into `vz status --json`. Both
    # channels now carry it, so agreement has to cover it: a CLI that dropped a
    # network or renamed an endpoint would otherwise pass here unnoticed. These
    # are compared whole rather than by key, because the values are the claim.
    for collection in ("networks", "network_attachments", "endpoints"):
        observed, typed = cli_environment.get(collection), typed_environment.get(collection)
        check.check(observed == typed,
                    f"the two channels report the same {collection} "
                    f"({json.dumps(observed, sort_keys=True)[:160]} vs typed "
                    f"{json.dumps(typed, sort_keys=True)[:160]})")
    for field in ("environment_id", "name", "state", "definition_digest", "lifecycle_generation"):
        check.check(typed_environment.get(field) == cli_environment.get(field),
                    f"Environment {field} agrees ({cli_environment.get(field)!r} vs typed "
                    f"{typed_environment.get(field)!r})")
    check.check(cli_environment.get("definition_digest") == status.get("persisted_definition_digest"),
                "the CLI's Environment digest is the persisted definition digest it reports")
    typed_machines = {machine["name"]: _typed_machine(machine) for machine in typed_environment.get("machines") or []}
    if not check.check(set(typed_machines) == set(cli_machines),
                       f"both channels report the same Machines ({sorted(cli_machines)} vs typed "
                       f"{sorted(typed_machines)})"):
        return check.finish()
    for name in sorted(cli_machines):
        for field, observed in sorted(cli_machines[name].items()):
            check.check(typed_machines[name][field] == observed,
                        f"Machine {name} {field} agrees ({observed!r} vs typed "
                        f"{typed_machines[name][field]!r})")
    # `health` is deliberately absent from both mappings above. It is not a
    # field of the persisted aggregate -- a MachineInstance is a durable record
    # and supervision belongs to the process that answered -- so the typed
    # channel carries it beside the aggregate, joined by machine_id. Comparing
    # it as though it lived inside would assert the CLI invented it.
    check.check(all(machine.get("health") for machine in cli_environment.get("machines") or []),
                "every Machine the CLI reports carries a health reading")
    if check.status != "PASS":
        return check.finish()

    # Reconciling the very same definition bytes over the typed channel. An Up
    # that minted new identities would mean the channels disagree about what
    # this Environment is; the admission must name the ones the CLI published.
    reconcile = ctx.run_tool(check, "agree-probe-up",
                             [str(probe), "up", "--state-db", state_db, "--socket", socket,
                              "--definition", str(project / "vz.json"), "--environment", cli_environment["name"],
                              # The CLI resolves its worktree root; /tmp is a symlink on macOS and an
                              # unresolved spelling is a different authorizing path, not the same one.
                              "--workspace-root", os.path.realpath(project),
                              "--timeout-millis", str(UP_TIMEOUT * 1000)],
                             cwd=project, env=env, timeout=UP_TIMEOUT + 60)
    if not check.check(reconcile.exit_code == 0,
                       f"the typed channel reconciled the same definition (exit {reconcile.exit_code}, "
                       f"stdout {reconcile.stdout[:300]!r})"):
        return check.finish()
    try:
        events = [document["event"] for document in probe_documents(reconcile)
                  if document.get("kind") == "vz-runtime-probe-up-event"]
    except json.JSONDecodeError as error:
        check.fail(f"the typed Up stream did not emit JSON documents: {error}")
        return check.finish()
    if not check.check(events, "the typed Up emitted at least one event"):
        return check.finish()
    sequences = [event.get("sequence") for event in events]
    check.check(all(isinstance(value, int) for value in sequences) and sequences == sorted(set(sequences)),
                f"event sequences are strictly increasing (observed {sequences[:12]})")
    admissions = {json.dumps(event.get("admission"), sort_keys=True) for event in events}
    check.check(len(admissions) == 1, f"every event carries one admission (observed {len(admissions)})")
    admission = events[0].get("admission") or {}
    check.check(admission.get("environment_id") == cli_environment.get("environment_id"),
                f"the typed admission names the Environment the CLI published "
                f"({cli_environment.get('environment_id')!r} vs {admission.get('environment_id')!r})")
    check.check(admission.get("project_id") == status.get("project_id"),
                f"the typed admission names the same project ({admission.get('project_id')!r})")
    check.check(admission.get("definition_digest") == status.get("desired_definition_digest"),
                f"the typed admission carries the same definition digest "
                f"({status.get('desired_definition_digest')!r} vs {admission.get('definition_digest')!r})")
    cli_machine_ids = {machine["machine_id"] for machine in cli_environment.get("machines") or []}
    check.check(set(admission.get("machine_ids") or []) == cli_machine_ids,
                f"the typed admission names the Machines the CLI published ({sorted(cli_machine_ids)} vs "
                f"{sorted(admission.get('machine_ids') or [])})")
    phases = [event.get("phase") for event in events]
    check.ok(f"typed transitions observed: {phases}")
    terminal = [event for event in events if event.get("completion") is not None]
    check.check(terminal, f"the typed stream carried a terminal receipt (phases {phases})")

    # The CLI reads the same Environment after the typed reconcile. Identities
    # that moved would mean one channel's Up is invisible to the other.
    after = read_status(ctx, check, "agree-after", project=project, env=env)
    if check.status == "PASS" and after:
        after_environment = (after.get("environments") or [{}])[0]
        check.check(after_environment.get("environment_id") == cli_environment.get("environment_id"),
                    "the CLI reports the same Environment identity after the typed reconcile")
        check.check({machine["machine_id"] for machine in after_environment.get("machines") or []} == cli_machine_ids,
                    "the CLI reports the same Machine identities after the typed reconcile")

    # Failure agreement: an Environment that does not exist is refused by both
    # channels, each in its own envelope, and the typed refusal names what it
    # could not find rather than failing silently.
    absent = "prj_" + "0" * 32
    denied = ctx.run_tool(check, "agree-probe-absent",
                          [str(probe), "state", "--state-db", state_db, "--socket", socket,
                           "--project-id", absent],
                          cwd=project, env=env, timeout=120)
    check.check(denied.exit_code != 0, f"the typed channel refuses an absent project (exit {denied.exit_code})")
    try:
        refusals = probe_documents(denied)
    except json.JSONDecodeError:
        refusals = []
    check.check(len(refusals) == 1 and refusals[0].get("kind") == "vz-runtime-probe-error"
                and isinstance(refusals[0].get("reason"), str) and refusals[0]["reason"]
                and isinstance(refusals[0].get("detail"), str) and refusals[0]["detail"],
                f"the typed refusal is one named error envelope (observed {refusals[:1]})")
    missing = ctx.run(check, "agree-cli-absent",
                      ["--json", "status", "--environment", "definitely-not-an-environment"],
                      cwd=project, env=env, timeout=60)
    check.check(missing.exit_code != 0,
                f"the CLI refuses an absent Environment too (exit {missing.exit_code})")

    if check.status == "PASS":
        removed = ctx.run(check, "agree-delete",
                          ["--json", "delete", "--environment", "default", "--timeout", "120"],
                          cwd=project, env=env, timeout=DELETE_TIMEOUT)
        check.check(removed.exit_code == 0, f"deleted (exit {removed.exit_code})")
    return check.finish()


# ---------------------------------------------------------------------------
# persisted-recovery: the Environments that must survive the sleep/wake edge
# ---------------------------------------------------------------------------
RECOVERY_RECORD_KIND = "vz-0.4-persisted-recovery-environments"


def establish_recovery_environments(ctx: CheckContext, names: tuple) -> tuple:
    """Bring up the Environments the post-wake phase has to find again.

    Every other phase deletes what it creates. This one deliberately leaves its
    Environments running: they are the subject of criterion 10, and an
    Environment that was torn down cannot demonstrate that its identity and
    state survived anything. Each gets its own project so criterion 8's
    isolation claims have three mutually foreign Environments to make, and each
    carries a sentinel so post-wake can prove Machine-local state came back
    rather than merely that a record still exists.

    Returns (record, check). The record is written into the retained state root,
    because the post-wake phase is a separate lane invocation with its own
    evidence directory and needs to be told what it is looking for.
    """
    check = SubCheck("gate.lifecycle.recovery_including_sleep_wake", "establish_recovery_environments")
    try:
        base = minimal_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return None, check.finish()
    environments = []
    for name in names:
        definition = copy.deepcopy(base)
        definition["project_id"] = "prj_" + uuid.uuid4().hex
        instance = provision(ctx, check, name, definition)
        if instance.get("unsupported"):
            check.not_implemented = "this runtime refused the definition: " + instance["unsupported"][:200]
            return None, check.finish()
        if check.status != "PASS" or not instance["status"]:
            return None, check.finish()
        token = "vzrec-" + uuid.uuid4().hex[:16]
        if not sentinel_write(ctx, check, name, instance, token):
            return None, check.finish()
        payload = instance["status"]
        reported = payload.get("environments") or []
        if not check.check(len(reported) == 1, f"{name}: one Environment (observed {len(reported)})"):
            return None, check.finish()
        environment = reported[0]
        environments.append({
            "isolate": name, "token": token, "project_id": payload.get("project_id"),
            "definition_digest": payload.get("persisted_definition_digest"),
            "environment_id": environment.get("environment_id"), "environment_name": environment.get("name"),
            "state": environment.get("state"), "lifecycle_generation": environment.get("lifecycle_generation"),
            "machines": [{"name": machine.get("name"), "machine_id": machine.get("machine_id"),
                          "incarnation_id": machine.get("incarnation_id"),
                          "incarnation_generation": machine.get("incarnation_generation"),
                          "state": machine.get("state"),
                          "docker_context": (machine.get("docker_context") or {}).get("name")}
                         for machine in environment.get("machines") or []],
        })
    identities = [environment["environment_id"] for environment in environments]
    check.check(len(set(identities)) == len(identities),
                f"the {len(environments)} Environments have distinct identities ({identities})")
    record = {"schema_version": 1, "kind": RECOVERY_RECORD_KIND, "environments": environments}
    return record, check.finish()


# Clauses of criterion 10 that nothing in this repository can exercise yet. They
# are named here rather than left out so the check reports what it did not
# observe instead of passing on the part that worked.
RECOVERY_UNEXERCISED = (
    "declared volumes (no Volume resource exists in schemas/vz-project-definition-v1.schema.json)",
    "DNS reconstruction (environment-local split DNS arrives with criterion 6's gateway)",
    "daemon, adapter and guest crash recovery (no crash injection exists in this lane)",
    "manifest recovery deadlines (the contract pins none for this lane)",
)


def check_lifecycle_recovery(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """The Environments pre-sleep left are the same ones, still serving.

    Criterion 10 claims stop/up preserves identity and declared state, and that
    a sleep/wake reconstructs routes, sockets and port state. What is proven
    here is the part that has a subject: each Environment pre-sleep established
    is found again with the identity it had, answers an exec (so it is serving,
    not merely recorded ready), returns its sentinel bytes unchanged, and keeps
    all of that across an explicit stop/up. Identity is read from the record
    written before the checkpoint, so a Machine silently recreated during the
    wake would read as a new incarnation and fail here rather than pass as a
    Machine that "came back".
    """
    check = SubCheck(top, "lifecycle_recovery")
    expected = established.get("environments") or []
    if not check.check(expected, "pre-sleep recorded at least one Environment to recover"):
        return check.finish()
    for entry in expected:
        name = entry["isolate"]
        try:
            instance = ctx.reattach(name)
        except ReattachError as error:
            check.fail(str(error))
            return check.finish()
        payload = read_status(ctx, check, "wake-" + name, project=instance["project"], env=instance["env"])
        if check.status != "PASS" or not payload:
            return check.finish()
        reported = payload.get("environments") or []
        if not check.check(len(reported) == 1, f"{name}: one Environment after wake (observed {len(reported)})"):
            return check.finish()
        environment = reported[0]
        for field, want in (("environment_id", entry["environment_id"]), ("name", entry["environment_name"]),
                            ("lifecycle_generation", entry["lifecycle_generation"])):
            check.check(environment.get(field) == want,
                        f"{name}: Environment {field} survived the checkpoint ({want!r} observed "
                        f"{environment.get(field)!r})")
        check.check(payload.get("persisted_definition_digest") == entry["definition_digest"],
                    f"{name}: the persisted definition digest is unchanged")
        observed = {machine.get("name"): machine for machine in environment.get("machines") or []}
        check.check(set(observed) == {machine["name"] for machine in entry["machines"]},
                    f"{name}: the same Machines are reported ({sorted(observed)})")
        for machine in entry["machines"]:
            after = observed.get(machine["name"]) or {}
            for field in ("machine_id", "incarnation_id", "incarnation_generation"):
                check.check(after.get(field) == machine[field],
                            f"{name}/{machine['name']}: {field} survived ({machine[field]!r} observed "
                            f"{after.get(field)!r})")
            check.check((after.get("docker_context") or {}).get("name") == machine["docker_context"],
                        f"{name}/{machine['name']}: the Docker context is the one it had")
        if check.status != "PASS":
            return check.finish()
        row = sentinel_read(ctx, check, "wake-" + name, instance)
        check.check(row.exit_code == 0 and row.stdout.strip() == (entry["token"] + "END").encode(),
                    f"{name}: Machine-local state came back byte-identical (observed {row.stdout[:60]!r})")
    if check.status != "PASS":
        return check.finish()
    # Stop/up is the criterion's own words, and it is a separate claim from
    # surviving the checkpoint: a Machine can come back from sleep and still
    # lose its identity when deliberately stopped and started.
    for entry in expected:
        name = entry["isolate"]
        instance = ctx.reattach(name)
        stopped = ctx.run(check, "wake-" + name + "-stop",
                          ["--json", "stop", "--environment", entry["environment_name"], "--timeout", "120"],
                          cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
        if not check.check(stopped.exit_code == 0, f"{name}: stopped (exit {stopped.exit_code})"):
            return check.finish()
        started = ctx.run(check, "wake-" + name + "-up", ["--json", "up"],
                          cwd=instance["project"], env=instance["env"], timeout=UP_TIMEOUT)
        if not check.check(started.exit_code == 0, f"{name}: up again (exit {started.exit_code})"):
            return check.finish()
        payload = read_status(ctx, check, "wake-" + name + "-after", project=instance["project"], env=instance["env"])
        if check.status != "PASS" or not payload:
            return check.finish()
        environment = (payload.get("environments") or [{}])[0]
        check.check(environment.get("environment_id") == entry["environment_id"],
                    f"{name}: stop/up preserved the Environment identity")
        check.check({machine.get("machine_id") for machine in environment.get("machines") or []} ==
                    {machine["machine_id"] for machine in entry["machines"]},
                    f"{name}: stop/up preserved every Machine identity")
        row = sentinel_read(ctx, check, "wake-" + name + "-after", instance)
        check.check(row.exit_code == 0 and row.stdout.strip() == (entry["token"] + "END").encode(),
                    f"{name}: declared state survived stop/up (observed {row.stdout[:60]!r})")
    if check.status == "PASS":
        check.not_implemented = ("criterion 10 also claims " + "; ".join(RECOVERY_UNEXERCISED) +
                                 ". The recovery of Environment and Machine identity, Docker context, "
                                 "Machine-local state and stop/up above did pass; these clauses were "
                                 "not exercised and are not claimed.")
    return check.finish()


# --------------------------------------------------------------------- criterion 19
#
# `gate.migration.install_upgrade_rollback_uninstall`: the installed-flow
# clauses of criterion 19, each proved separately.
#
# Everything here happens inside a disposable prefix and a disposable HOME under
# the lane state root. The uninstaller removes things, so it is never pointed at
# the developer's own `~/.vz`: `VZ_INSTALL_DIR`, `HOME`, `VZ_RUNTIME_DATA_DIR`,
# `VZ_RUNTIME_STATE_DB` and `VZ_DOCKER_CONFIG` are all lane-owned paths and the
# environment the installer sees is built from scratch rather than inherited.
MIGRATION_FIXTURE = "tests/fixtures/vz-0.4/migration/v0.3.20-state.db"
MIGRATION_PROJECT = "tests/fixtures/vz-0.4/migration/project"
INSTALLER = "scripts/install.sh"
E2E_CONTRACT = "config/vz-0.4-e2e-contract.json"
INSTALLED_BINARIES = ("vz", "vz-runtimed", "vz-guest-agent", "vz-agent-loader", "vz-macos-setup")
# The legacy classification markers, from
# `crates/vz-runtime-contract/src/types/{sandbox,topology}.rs`.
LEGACY_DEVELOPER_LABEL = "vz.run.workspace"
LEGACY_SPACE_MODE_LABEL = "vz.space.mode"
LEGACY_SPACE_MODE_REQUIRED = "required"
MIGRATION_FAILPOINT_ENV = "VZ_STATE_STORE_MIGRATION_FAILPOINT"
MIGRATION_FAILPOINT = "after_schema_migration"
# The migrated topology's own tables, and the four declared-topology projections
# a legacy Hardened/generic record must not appear in.
MIGRATED_TABLES = ("project_definitions", "environment_instances", "machine_instances", "workspace_bindings")
DEFAULTED_TABLES = ("environment_host_imports", "environment_host_exports", "environment_machine_egress",
                    "environment_network_attachments")
DOCKER_CAPABILITIES = ("docker_engine", "compose", "buildx")
MIGRATION_DEADLINE = 180
INSTALL_TIMEOUT = 300
RC_PREAMBLE = b"# a line this user had before vz\nexport EDITOR=vi\n"
FOREIGN_BYTES = b"a file the user keeps under the vz prefix\n"
# Where the pinned v0.3.20 daemon is looked for. It is ~22 MiB, so it is neither
# committed nor downloaded from inside a check; the operator stages it once and
# its digest is verified against the contract pin before it is executed.
LEGACY_ARTIFACT_ENV = "VZ04_LEGACY_V0320_RUNTIMED"
LEGACY_ARTIFACT_CACHE = ".cache/vz-0.4-legacy-v0.3.20/vz-runtimed-v0.3.20-darwin-arm64"


def _sqlite_query(path: Path, sql: str, *, immutable: bool = False) -> list:
    """Query a state store without writing to it.

    `immutable` is for the checked-in fixture and nothing else: opening a store
    in WAL mode the ordinary way creates `-wal`/`-shm` beside it, and a check
    must not leave those in the source tree next to a digest-pinned file.
    """
    import sqlite3

    if immutable:
        connection = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    else:
        connection = sqlite3.connect(str(path))
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


def _schema_version(path: Path, *, immutable: bool = False):
    try:
        rows = _sqlite_query(path, "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                             immutable=immutable)
    except Exception:  # noqa: BLE001 - a store mid-migration or absent is not an error here
        return None
    return rows[0][0] if rows else None


def _table_rows(path: Path, table: str):
    """Every row of `table`, or `None` when the table does not exist."""
    try:
        return _sqlite_query(path, f"SELECT * FROM {table}")
    except Exception:  # noqa: BLE001 - an absent table is absent, not empty
        return None


def _machine_rows(path: Path) -> list:
    return _sqlite_query(path, "SELECT machine_id, legacy_sandbox_id, instance_json FROM machine_instances")


def _legacy_records(path: Path, *, immutable: bool = False) -> dict:
    """`{sandbox_id: {...}}` from a legacy `sandbox_state` table."""
    records = {}
    for sandbox_id, state, backend, spec, labels in _sqlite_query(
            path, "SELECT sandbox_id, state, backend, spec_json, labels_json FROM sandbox_state ORDER BY sandbox_id",
            immutable=immutable):
        parsed = json.loads(labels)
        records[sandbox_id] = {
            "state": state, "backend": backend, "spec": json.loads(spec), "labels": parsed,
            "developer": LEGACY_DEVELOPER_LABEL in parsed,
            "hardened": parsed.get(LEGACY_SPACE_MODE_LABEL) == LEGACY_SPACE_MODE_REQUIRED,
        }
    return records


def _classify(records: dict) -> dict:
    """Legacy ids by the classification 0.4 migration gives them."""
    return {
        "developer": sorted(i for i, r in records.items() if r["developer"] and not r["hardened"]),
        "hardened": sorted(i for i, r in records.items() if r["hardened"]),
        "generic": sorted(i for i, r in records.items() if not r["developer"] and not r["hardened"]),
    }


def _await_daemon(socket_path: Path, deadline: float, held) -> None:
    """Wait for the daemon to finish opening the store: it serves or it dies.

    Not "the schema version moved": the migrations commit one step at a time, so
    an intermediate version is visible long before the store is migrated, and
    stopping the daemon there would leave it half-migrated and judge that.
    """
    while time.monotonic() < deadline:
        if socket_path.is_socket() or held.process.poll() is not None:
            return
        time.sleep(0.2)


def _open_store(ctx: CheckContext, check: SubCheck, label: str, prefix: Path, database: Path, runtime: Path,
                socket_path: Path, home: Path, *, failpoint: bool):
    """Run the installed daemon over `database` until it migrates or dies."""
    runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C", "NO_COLOR": "1", "HOME": str(home),
           "TMPDIR": str(ctx.state.tmp), "VZ_RUNTIME_STATE_DB": str(database), "VZ_RUNTIME_DATA_DIR": str(runtime),
           "VZ_RUNTIME_DAEMON_SOCKET": str(socket_path), "VZ_DOCKER_CONFIG": str(ctx.state.docker_config)}
    if failpoint:
        env[MIGRATION_FAILPOINT_ENV] = MIGRATION_FAILPOINT
    held = ctx.recorder.start(label, [prefix / "bin/vz-runtimed", "--state-store-path", database,
                                      "--runtime-data-dir", runtime, "--socket-path", socket_path],
                              cwd=prefix, env=env, scenario_id=check.id, timeout=MIGRATION_DEADLINE)
    _await_daemon(socket_path, time.monotonic() + MIGRATION_DEADLINE, held)
    receipt = ctx.recorder.release(held)
    check.evidence.extend(ctx.recorder.receipt_paths(receipt))
    # Read after the daemon is gone: the store may be in WAL mode, so its schema
    # version lives in the write-ahead log until the last connection closes.
    return _schema_version(database), receipt


def _backup_records(runtime: Path) -> list:
    """`[(backup path, decoded sidecar or None)]` under one runtime data dir."""
    directory = runtime / "state-store-backups"
    if not directory.is_dir():
        return []
    rows = []
    for path in sorted(directory.glob("*.bak.json")):
        try:
            rows.append((Path(str(path)[:-len(".json")]), json.loads(read_regular(path))))
        except (OSError, json.JSONDecodeError):
            rows.append((Path(str(path)[:-len(".json")]), None))
    return rows


def _legacy_artifact(ctx: CheckContext, check: SubCheck, pinned_digest: str, url: str):
    """The pinned v0.3.20 daemon, or `None` with the reason recorded."""
    if check.failures:
        # `not_implemented` claims everything else ran and passed. Something has
        # already failed, so this stays a failure and does not become a gap.
        check.ok("the pinned v0.3.20 daemon was not consulted: earlier assertions in this check already failed")
        return None
    # The cache is gitignored, so it exists in the working checkout and never in
    # a frozen tree -- resolving it against `ctx.repo_root` would look inside the
    # freeze and always miss, and the staging instruction below would name a
    # directory that disappears when the run ends.
    cache_root = frozen_tree.live_root(ctx.repo_root)
    staged = os.environ.get(LEGACY_ARTIFACT_ENV) or str(cache_root / LEGACY_ARTIFACT_CACHE)
    path = Path(staged)
    if not path.is_file():
        check.not_implemented = (
            "criterion 19 requires the restored store to be usable by v0.3.20 itself, and the pinned v0.3.20 daemon "
            f"was not staged, so the restored store was never opened by v0.3.20. Stage it once with: "
            f"curl -sSfL --create-dirs -o {cache_root / LEGACY_ARTIFACT_CACHE} {url} "
            f"(sha256 {pinned_digest}), or point {LEGACY_ARTIFACT_ENV} at a copy. Every other clause of this "
            "check ran; see its assertions.")
        return None
    observed = digest_file(path)
    if observed != pinned_digest:
        check.fail(f"the staged v0.3.20 daemon at {path} is not the pinned artifact "
                   f"(pinned {pinned_digest}, staged {observed})")
        return None
    # This clause EXECUTES the v0.3.20 daemon, and the staging instruction above
    # is a plain `curl -o`, which writes 0644. Executing the operator's own file
    # therefore raised PermissionError and crashed the whole lane -- following
    # the printed instruction exactly was the way to reproduce it. Run an
    # executable copy in the lane's own scratch instead: the cache is not
    # mutated, the cache directory need not be writable, and the clause no
    # longer depends on how the artifact happened to be staged. The digest above
    # is checked against the ORIGINAL, and the copy is compared to it again so
    # the thing that runs is the thing that was verified.
    runnable = ctx.state.tmp / "vz-runtimed-v0.3.20-darwin-arm64"
    try:
        ctx.state.tmp.mkdir(mode=0o700, parents=True, exist_ok=True)
        shutil.copyfile(path, runnable)
        runnable.chmod(0o700)
    except OSError as error:
        check.fail(f"the pinned v0.3.20 daemon at {path} could not be staged as executable in the lane scratch: {error}")
        return None
    copied = digest_file(runnable)
    if copied != pinned_digest:
        check.fail(f"the executable copy of the v0.3.20 daemon does not match the pinned artifact "
                   f"(pinned {pinned_digest}, copied {copied})")
        return None
    check.ok(f"the pinned v0.3.20 daemon is staged at {path} with the contract's digest {pinned_digest}, "
             f"and runs from an executable copy at {runnable}")
    return runnable


def check_migration_install_upgrade_rollback_uninstall(ctx: CheckContext, top: str) -> SubCheck:
    """Clean install, upgrade from the pinned v0.3.20 fixture, injected failure, uninstall.

    Every clause of criterion 19 is separated, because each fails for its own
    reason and one verdict over the four would hide which:

    * a clean 0.4 install places exactly the release's own binaries under a
      disposable prefix and records the release version;
    * the pinned v0.3.20 fixture is upgraded to exactly one Project, one
      Environment and one Machine, and that Machine is compared field by field
      against the legacy record it came from rather than merely counted;
    * the same fixture, upgraded with a migration failure injected through the
      installed daemon, ends byte-identical to the fixture and is opened again
      by the pinned v0.3.20 daemon;
    * the legacy Hardened and generic records acquire none of the four things
      the criterion names -- Developer, Docker, host imports, egress -- each
      checked on its own; and
    * uninstall removes what vz installed and created and nothing else: a
      foreign file under the prefix, the legacy project, the user's shell rc and
      the user's Docker configuration are all compared before and after.

    The fixture is deliberately substantive -- three legacy records, one of each
    classification, and a Developer record carrying an image and resources -- so
    that none of the counts below can be satisfied by an empty legacy store.
    """
    check = SubCheck(top, "install_upgrade_rollback_uninstall")
    root = ctx.state.root / "migration"
    prefix, home, project = root / "prefix", root / "home", root / "project"
    docker_config, rc_file = home / ".docker", home / ".zshrc"
    foreign = prefix / "keep-me.txt"
    runtime = ctx.state.isolate_runtime("mig")
    socket_path = runtime / "d.sock"
    database = prefix / "stack-state.db"

    fixture = ctx.repo_root / MIGRATION_FIXTURE
    contract = load_json(ctx.repo_root / E2E_CONTRACT)["migration"]
    if not fixture.is_file():
        check.fail(f"the pinned v0.3.20 state fixture is missing: {fixture}")
        return check.finish()
    fixture_digest = digest_file(fixture)
    check.check(contract["legacy_state_fixture"] == MIGRATION_FIXTURE and
                contract["legacy_state_fixture_sha256"] == fixture_digest,
                f"the fixture on disk is the one the contract pins (contract {contract['legacy_state_fixture']} "
                f"{str(contract['legacy_state_fixture_sha256'])[:16]}, observed {MIGRATION_FIXTURE} {fixture_digest[:16]})")
    check.check(contract["legacy_release_tag"] == "v0.3.20",
                f"the pinned legacy tag is v0.3.20 (observed {contract['legacy_release_tag']!r})")
    check.check(_schema_version(fixture, immutable=True) == "1",
                f"the fixture is at the legacy schema version "
                f"(observed {_schema_version(fixture, immutable=True)!r})")

    legacy = _legacy_records(fixture, immutable=True)
    kinds = _classify(legacy)
    check.check(len(kinds["developer"]) == 1 and len(kinds["hardened"]) == 1 and len(kinds["generic"]) == 1,
                f"the fixture carries one legacy record of each classification (observed {kinds})")
    if check.status != "PASS":
        return check.finish()
    developer_id, hardened_id, generic_id = kinds["developer"][0], kinds["hardened"][0], kinds["generic"][0]
    developer_spec = legacy[developer_id]["spec"]
    check.check(bool(developer_spec.get("base_image_ref")) and bool(developer_spec.get("cpus")) and
                bool(developer_spec.get("memory_mb")),
                f"the legacy Developer record carries the data migration must preserve (spec {developer_spec})")

    # -- clean 0.4 installation ---------------------------------------------------
    for directory in (root, prefix, home, docker_config / "contexts/meta/vz04-unrelated"):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    shutil.copytree(ctx.repo_root / MIGRATION_PROJECT, project)
    foreign.write_bytes(FOREIGN_BYTES)
    rc_file.write_bytes(RC_PREAMBLE)
    (docker_config / "config.json").write_bytes(b'{"currentContext":"desktop-linux"}\n')
    (docker_config / "contexts/meta/vz04-unrelated/meta.json").write_bytes(b'{"Name":"vz04-unrelated"}\n')
    before = {"docker": inventory(docker_config), "project": inventory(project)}

    installer = ctx.repo_root / INSTALLER
    install_env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C", "NO_COLOR": "1", "HOME": str(home),
                   "TMPDIR": str(ctx.state.tmp), "SHELL": "/bin/zsh", "VZ_INSTALL_DIR": str(prefix),
                   "VZ_LOCAL_RELEASE_DIR": str(ctx.release_dir)}
    # A candidate carrying no guest bundles cannot have them installed. Say which
    # half ran rather than letting an absent bundle read as a passing install.
    guest_bundles = (ctx.release_dir / "linux").is_dir()
    if not guest_bundles:
        install_env["VZ_NO_LINUX"] = "1"
    installed = ctx.recorder.run("migration-install", ["/bin/bash", installer], cwd=root, env=install_env,
                                 scenario_id=check.id, timeout=INSTALL_TIMEOUT)
    check.evidence.extend(ctx.recorder.receipt_paths(installed))
    check.check(installed.exit_code == 0, f"a clean 0.4 installation into {prefix} succeeds "
                f"(exit {installed.exit_code}, {installed.stderr[-200:]!r})")
    if installed.exit_code != 0:
        return check.finish()
    for name in INSTALLED_BINARIES:
        target, source = prefix / "bin" / name, ctx.release_dir / "bin" / name
        check.check(target.is_file() and os.access(target, os.X_OK) and digest_file(target) == digest_file(source),
                    f"installed bin/{name} is the release's own executable")
    recorded = (read_regular(prefix / ".installed-version").decode().strip()
                if (prefix / ".installed-version").is_file() else None)
    declared = load_json(ctx.release_dir / "release-manifest.json")["release_version"]
    check.check(recorded == declared,
                f"the installation records the release version (recorded {recorded!r}, release {declared!r})")
    rc_installed = rc_file.read_bytes()
    check.check(rc_installed.startswith(RC_PREAMBLE) and
                f'export PATH="{prefix / "bin"}:$PATH"'.encode() in rc_installed,
                "the installer added its PATH entry to the shell rc and kept what was already there")
    if guest_bundles:
        check.check((prefix / "linux/developer/version.json").is_file() and
                    (prefix / "machine-target-catalog.json").is_file(),
                    "the installation placed the release's guest bundle and wrote the installed machine-target catalog")
    else:
        check.ok("this candidate carries no linux/ guest bundles, so the installation ran with VZ_NO_LINUX=1 and the "
                 "guest-bundle and machine-target-catalog half of installation was not exercised")

    # -- upgrade from the pinned v0.3.20 fixture ----------------------------------
    shutil.copyfile(fixture, database)
    upgraded, upgrade_receipt = _open_store(ctx, check, "migration-upgrade", prefix, database, runtime, socket_path,
                                            home, failpoint=False)
    check.check(upgraded not in (None, "1") and str(upgraded).isdigit() and int(upgraded) > 1,
                f"opening the v0.3.20 store with the installed daemon migrates it off the legacy schema "
                f"(observed schema_version {upgraded!r}, daemon exit {upgrade_receipt.exit_code}, "
                f"{upgrade_receipt.stderr[-200:]!r})")
    if upgraded in (None, "1"):
        return check.finish()
    counts = {table: (None if _table_rows(database, table) is None else len(_table_rows(database, table)))
              for table in MIGRATED_TABLES}
    check.check(counts == {table: 1 for table in MIGRATED_TABLES},
                f"the upgrade produced exactly one Project, Environment, Machine and WorkspaceBinding (observed {counts})")

    # No early return on a wrong Machine count: the Hardened/generic claims below
    # are exactly what a migration that adopted too much would break, and they
    # have to be reached to say so.
    machines = _machine_rows(database)
    developer_rows = [row for row in machines if row[1] == developer_id]
    check.check(len(developer_rows) == 1 and len(machines) == 1,
                f"exactly one Machine was migrated and it is the legacy Developer record "
                f"(all {[(row[0], row[1]) for row in machines]})")
    for _machine_id, _legacy_id, instance_json in developer_rows:
        instance = json.loads(instance_json)
        check.check(instance.get("legacy_sandbox_id") == developer_id,
                    f"the migrated Machine names the legacy sandbox it came from "
                    f"(instance {instance.get('legacy_sandbox_id')!r}, legacy {developer_id!r})")
        survived = {"image": (instance.get("target") or {}).get("image"),
                    "cpus": (instance.get("resources") or {}).get("cpus"),
                    "memory_mb": (instance.get("resources") or {}).get("memory_mb")}
        expected = {"image": developer_spec.get("base_image_ref"), "cpus": developer_spec.get("cpus"),
                    "memory_mb": developer_spec.get("memory_mb")}
        check.check(survived == expected,
                    f"the legacy record's image and resources survived the upgrade (legacy {expected}, "
                    f"migrated {survived})")
    environments = sorted(_sqlite_query(database, "SELECT name, legacy_sandbox_id FROM environment_instances"))
    check.check(environments == [("default", developer_id)],
                f"the one Environment is the legacy record's own (observed {environments})")

    # -- legacy Hardened/generic records keep their meaning -----------------------
    after_upgrade = _legacy_records(database)
    for name, identifier in (("Hardened", hardened_id), ("generic", generic_id)):
        original, current = legacy[identifier], after_upgrade.get(identifier)
        check.check(current is not None and current["labels"] == original["labels"] and
                    current["spec"] == original["spec"] and current["backend"] == original["backend"],
                    f"the legacy {name} record {identifier} keeps its markers, spec and backend "
                    f"(labels {None if current is None else current['labels']})")
    check.check(after_upgrade.get(hardened_id, {}).get("hardened") is True,
                f"the legacy Hardened record still reads as Hardened ({LEGACY_SPACE_MODE_LABEL}="
                f"{after_upgrade.get(hardened_id, {}).get('labels', {}).get(LEGACY_SPACE_MODE_LABEL)!r})")
    check.check(after_upgrade.get(generic_id, {}).get("developer") is False and
                after_upgrade.get(generic_id, {}).get("hardened") is False,
                f"the legacy generic record still carries neither marker "
                f"(labels {sorted(after_upgrade.get(generic_id, {}).get('labels', {}))})")
    # The four things the criterion says they must not acquire, one at a time.
    foreign_ids = (hardened_id, generic_id)
    adopted = [(row[0], row[1]) for row in _machine_rows(database) if row[1] in foreign_ids]
    check.check(not adopted, f"no legacy Hardened or generic record acquired a Machine, so none acquired a Developer "
                f"profile (observed {adopted})")
    profiles = sorted({json.loads(row[2]).get("profile") for row in _machine_rows(database) if row[1] in foreign_ids})
    check.check(not profiles, f"no Machine carrying a Hardened/generic legacy id declares a profile (observed {profiles})")
    docker_granted = [row[1] for row in _machine_rows(database) if row[1] in foreign_ids and
                      set(DOCKER_CAPABILITIES) &
                      set((json.loads(row[2]).get("negotiated_capabilities") or {}).get("capabilities") or [])]
    check.check(not docker_granted,
                f"no legacy Hardened or generic record acquired Docker capabilities (observed {docker_granted})")
    for table in DEFAULTED_TABLES:
        rows = _table_rows(database, table)
        check.check(rows == [], f"the upgrade created no {table} row for any legacy record, migrated or refused "
                    f"(observed {'no such table' if rows is None else len(rows)})")
    check.check(all(json.loads(row[2]).get("docker_context") in (None, {}) for row in _machine_rows(database)),
                "the upgrade materialised no Docker context for any migrated Machine")

    # -- a pre-migration backup exists, byte-identical to the fixture --------------
    retained = _backup_records(runtime)
    check.check(any(record and record.get("sha256") == fixture_digest and record.get("from_schema_version") == 1 and
                    record.get("migration_completed") for _path, record in retained),
                "the successful upgrade retained a completed pre-migration backup of the fixture's exact bytes "
                f"(records {[r for _p, r in retained]})")
    backup_digests = [digest_file(path) for path, _record in retained if path.is_file()]
    check.check(fixture_digest in backup_digests, f"the retained backup file is byte-identical to the fixture "
                f"(fixture {fixture_digest[:16]}, backups {[d[:16] for d in backup_digests]})")

    # -- injected migration failure restores the backup ---------------------------
    failed_runtime = ctx.state.isolate_runtime("migf")
    shutil.copyfile(fixture, database)
    failed_version, failed_receipt = _open_store(ctx, check, "migration-injected-failure", prefix, database,
                                                 failed_runtime, failed_runtime / "d.sock", home, failpoint=True)
    check.check(failed_receipt.exit_code not in (0, None),
                f"the injected migration failure fails the installed daemon's start (exit {failed_receipt.exit_code})")
    # Byte-identity of the store file is not enough on its own: this store is in
    # WAL mode, so a half-migrated store can have identical main-file bytes and
    # its newest committed state in the sidecar. The schema version is therefore
    # read back through SQLite, which sees whatever the write-ahead log holds --
    # a restore that left a v12 log behind reads as v12 and fails here.
    sidecars = [suffix for suffix in ("-wal", "-shm", "-journal") if os.path.lexists(str(database) + suffix)]
    check.check(failed_version == "1" and digest_file(database) == fixture_digest,
                f"the failed migration left the store byte-identical to the v0.3.20 fixture and reading it through "
                f"SQLite still answers the legacy schema version (schema_version {failed_version!r}, "
                f"sha256 {digest_file(database)[:16]}, fixture {fixture_digest[:16]}, sidecars present {sidecars})")
    restored = [record for _path, record in _backup_records(failed_runtime) if record]
    check.check(any(record.get("restored") and record.get("sha256") == fixture_digest for record in restored),
                f"the daemon records that it restored the backup (records {restored})")
    check.check(_classify(_legacy_records(database)) == kinds,
                f"every legacy record survived the failed migration (observed {_classify(_legacy_records(database))})")

    # v0.3.20 must still be able to open what was restored. The pinned daemon is
    # the component that owns the store, so it is the one run here; the v0.3.20
    # CLI is not pinned by the contract and is not run.
    legacy_daemon = _legacy_artifact(ctx, check, contract["legacy_artifact_sha256"], contract["legacy_artifact_url"])
    if legacy_daemon is not None:
        rollback_runtime = ctx.state.isolate_runtime("migr")
        rollback_runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
        rollback_socket = rollback_runtime / "d.sock"
        held = ctx.recorder.start("migration-v0320-rollback",
                                  [legacy_daemon, "--state-store-path", database,
                                   "--runtime-data-dir", rollback_runtime, "--socket-path", rollback_socket],
                                  cwd=prefix, env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C",
                                                   "HOME": str(home), "TMPDIR": str(ctx.state.tmp)},
                                  scenario_id=check.id, timeout=MIGRATION_DEADLINE)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not rollback_socket.is_socket() and held.process.poll() is None:
            time.sleep(0.2)
        served = rollback_socket.is_socket()
        rollback = ctx.recorder.release(held)
        check.evidence.extend(ctx.recorder.receipt_paths(rollback))
        check.check(served, f"the pinned v0.3.20 daemon opened the restored store and served its socket "
                    f"(exit {rollback.exit_code}, {rollback.stderr[-200:]!r})")
        # v0.3.20 owns the store it opened and reconciles it, so its bytes are
        # allowed to change here. What rollback means is that it still reads its
        # own schema and still holds its own records afterwards.
        after_rollback = _legacy_records(database)
        check.check(_schema_version(database) == "1" and _classify(after_rollback) == kinds,
                    f"after v0.3.20 owned the restored store it is still the legacy schema holding every legacy "
                    f"record (schema_version {_schema_version(database)!r}, {_classify(after_rollback)})")
        check.check(all(after_rollback[identifier]["labels"] == legacy[identifier]["labels"] and
                        after_rollback[identifier]["spec"] == legacy[identifier]["spec"]
                        for identifier in legacy if identifier in after_rollback),
                    "every legacy record kept its markers and spec across the v0.3.20 rollback")

    # -- uninstall removes only vz-owned resources --------------------------------
    uninstall_env = dict(install_env)
    uninstall_env.pop("VZ_LOCAL_RELEASE_DIR")
    uninstall_env.update({"VZ_RUNTIME_STATE_DB": str(database), "VZ_RUNTIME_DATA_DIR": str(runtime)})
    owned = (prefix / "bin/vz", prefix / ".installed-version", database, runtime)
    present_before = [path for path in owned if os.path.lexists(path)]
    check.check(len(present_before) == len(owned),
                f"every vz-owned path exists before uninstall (present {[str(p) for p in present_before]})")
    removed = ctx.recorder.run("migration-uninstall", ["/bin/bash", installer, "--uninstall"], cwd=root,
                               env=uninstall_env, scenario_id=check.id, timeout=INSTALL_TIMEOUT)
    check.evidence.extend(ctx.recorder.receipt_paths(removed))
    check.check(removed.exit_code == 0, f"uninstall succeeds (exit {removed.exit_code}, {removed.stderr[-200:]!r})")
    reported = [line for line in removed.stdout.decode("utf-8", "replace").splitlines()
                if line.strip().startswith("removed:")]
    check.check(len(reported) >= len(INSTALLED_BINARIES),
                f"uninstall reports what it removed rather than finding nothing to remove ({len(reported)} path(s))")
    survivors = [str(path) for path in owned if os.path.lexists(path)]
    check.check(not survivors, f"uninstall removed the installed software and the vz-owned runtime resources "
                f"(surviving {survivors})")
    check.check(foreign.is_file() and foreign.read_bytes() == FOREIGN_BYTES,
                f"a file the user keeps under the prefix survives uninstall ({foreign})")
    check.check(inventory(project) == before["project"],
                "the legacy project directory is byte-identical after uninstall")
    check.check(inventory(docker_config) == before["docker"],
                "unrelated Docker configuration is byte-identical after uninstall")
    rc_text = rc_file.read_bytes()
    check.check(rc_text.startswith(RC_PREAMBLE), "uninstall kept the shell rc lines that were not vz's")
    check.check(f'export PATH="{prefix / "bin"}:$PATH"'.encode() not in rc_text and b"# vz\n" not in rc_text,
                f"uninstall removed its own PATH entry from the shell rc (rc now {rc_text!r})")
    return check.finish()


# ── Criterion 2: mixed-profile topology status ────────────────────────────────
#
# One Environment holding two Developer Linux Machines, one Hardened Linux
# Machine, and one native macOS Machine, read back through `vz status --json`.

MIXED_NETWORK = "backend"
MIXED_PORT = PRIVATE_PORT
# The three capabilities a Developer-profile Linux Machine implicitly acquires,
# and that nothing else in the product may hold.
DOCKER_CAPABILITIES = {"docker_engine", "compose", "buildx"}
AMBIGUOUS_SENTINEL = "vz04-ambiguous-exec-must-not-run"


def mixed_profile_definition(release_dir: Path, macos_entry) -> dict:
    """Criterion 2's topology: two Developer Linux, one Hardened Linux, one macOS.

    The Hardened Machine declares no `networks`, because it may not: both the
    project-definition schema (`machine.allOf[2]`) and
    `validate_machine_network_support` refuse Environment-network membership to
    the restricted profile. That is what makes the endpoint denial below
    structural rather than a firewall rule -- the Machine is given no port on
    the switch at all -- and it is why the denial is proved against a Developer
    endpoint that a Developer sibling really can reach.

    `macos_entry` is None on a host with no registered Developer macOS template.
    The Linux Machines are still exercised; the criterion's macOS clause is then
    reported unimplemented by name rather than quietly dropped.
    """
    catalog = load_json(release_dir / "machine-target-catalog.json")
    developer = next(item for item in catalog["linux"] if item["profile"] == "developer")
    hardened = next(item for item in catalog["linux"] if item["profile"] == "hardened")

    def linux(name: str, profile: str, entry: dict, networks: list) -> dict:
        machine = {"schema_version": 1, "name": name, "profile": profile,
                   "target": {"os": "linux", "arch": "aarch64", "image": entry["image"],
                              "digest": entry["digest"]},
                   "resources": {"cpus": 2, "memory_mb": 4096}}
        if networks:
            machine["networks"] = networks
        return machine

    machines = [linux("dev-0", "developer", developer, [MIXED_NETWORK]),
                linux("dev-1", "developer", developer, [MIXED_NETWORK]),
                linux("hardened-0", "hardened", hardened, [])]
    if macos_entry is not None:
        target = {"os": "macos", "arch": "aarch64", "image": macos_entry["image"], "channel": MACOS_CHANNEL}
        if macos_entry.get("version"):
            target["version"] = macos_entry["version"]
        machines.append({"schema_version": 1, "name": "mac-0", "profile": "developer", "target": target,
                         "resources": {"cpus": 2, "memory_mb": 4096}, "networks": [MIXED_NETWORK]})
    return {"schema_version": 1, "project_id": "prj_" + uuid.uuid4().hex,
            "name": "vz04-mixed-profile-topology",
            "environment": {"schema_version": 1, "machines": machines,
                            "networks": [{"schema_version": 1, "name": MIXED_NETWORK, "kind": "private"}],
                            "endpoints": [{"schema_version": 1, "name": "probe", "machine": "dev-0",
                                           "network": MIXED_NETWORK, "protocol": "tcp", "port": MIXED_PORT}]}}


def _capabilities(machine: dict, field: str) -> set:
    value = machine.get(field)
    if not isinstance(value, dict) or not isinstance(value.get("capabilities"), list):
        return set()
    return {row for row in value["capabilities"] if isinstance(row, str)}


def _machines_by_name(environment: dict) -> dict:
    return {machine.get("name"): machine for machine in environment.get("machines") or []
            if isinstance(machine, dict)}


def _identity_map(payload) -> dict:
    """`{environment name: (environment id, {machine name: machine id})}`.

    Reduced to identity alone so that two reads can be compared for stability
    without a difference in some unrelated live field looking like an identity
    change.
    """
    if not isinstance(payload, dict):
        return {}
    identities = {}
    for environment in payload.get("environments") or []:
        if not isinstance(environment, dict):
            continue
        identities[environment.get("name")] = (
            environment.get("environment_id"),
            {machine.get("name"): machine.get("machine_id")
             for machine in environment.get("machines") or [] if isinstance(machine, dict)})
    return identities


def check_mixed_profile_topology_status(ctx: CheckContext, top: str) -> SubCheck:
    """Criterion 2, read out of one live mixed-profile Environment.

    The claims are ordered so the first failure names the broken link: the
    Environment must exist and hold exactly the declared Machines before their
    projections are judged; each Machine's profile, target, capabilities and
    Docker context are compared against what the definition declared rather than
    checked for presence; the denial is proved only after the endpoint it is
    denied has been shown to work from a Developer sibling; and health is proved
    to be an observation by making it change.

    Nothing here accepts a key as evidence of a value. Every comparison is an
    equality against something the definition, the profile rule or a previous
    read already fixed.
    """
    check = SubCheck(top, "mixed_profile_topology_status")
    macos_entry = macos_target(ctx.release_dir)
    try:
        definition = mixed_profile_definition(ctx.release_dir, macos_entry)
    except (StopIteration, KeyError, OSError) as error:
        check.fail("cannot derive both Linux profiles from the release machine-target-catalog: "
                   f"{error}")
        return check.finish()
    declared = {machine["name"]: machine for machine in definition["environment"]["machines"]}
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if schema_path.is_file():
        problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(definition),
                          key=lambda error: list(map(str, error.absolute_path)))
        check.check(not problems, f"the mixed-profile definition validates ({len(declared)} Machines)"
                    if not problems else f"definition invalid: {problems[0].message[:200]}")
        if problems:
            return check.finish()
    else:
        check.fail(f"project definition schema absent: {PROJECT_DEFINITION_SCHEMA}")
        return check.finish()

    instance = provision(ctx, check, "mix", definition)
    if instance.get("unsupported"):
        check.not_implemented = ("this runtime refused the mixed-profile definition: " +
                                 instance["unsupported"][:300])
        return check.finish()
    if check.status != "PASS" or not instance["status"]:
        return check.finish()
    payload = instance["status"]

    # ── the document's own shape, at every level ──────────────────────────────
    _compare_field_set(check, "status", set(payload), STATUS_FIELDS, STATUS_OPTIONAL_FIELDS)
    environments = payload.get("environments")
    if not check.check(isinstance(environments, list) and len(environments) == 1,
                       "exactly one Environment is reported (observed "
                       f"{len(environments) if isinstance(environments, list) else None})"):
        return check.finish()
    environment = environments[0]
    _compare_field_set(check, "environment", set(environment), ENVIRONMENT_FIELDS,
                       ENVIRONMENT_OPTIONAL_FIELDS)
    machines = _machines_by_name(environment)
    if not check.check(sorted(machines) == sorted(declared),
                       f"every declared Machine is reported (declared {sorted(declared)}, "
                       f"observed {sorted(machines)})"):
        return check.finish()
    for name in sorted(machines):
        _compare_field_set(check, f"machine {name}", set(machines[name]), MACHINE_FIELDS,
                           MACHINE_OPTIONAL_FIELDS)
    for network in environment.get("networks") or []:
        _compare_field_set(check, "network", set(network), NETWORK_FIELDS, NETWORK_OPTIONAL_FIELDS)
    for attachment in environment.get("network_attachments") or []:
        _compare_field_set(check, "attachment", set(attachment), ATTACHMENT_FIELDS,
                           ATTACHMENT_OPTIONAL_FIELDS)
    for endpoint in environment.get("endpoints") or []:
        _compare_field_set(check, "endpoint", set(endpoint), ENDPOINT_FIELDS, ENDPOINT_OPTIONAL_FIELDS)
    if check.status != "PASS":
        return check.finish()

    # ── stable Environment and Machine IDs ────────────────────────────────────
    environment_id = environment.get("environment_id")
    identities = {name: machine.get("machine_id") for name, machine in machines.items()}
    check.check(isinstance(environment_id, str) and environment_id != "",
                f"the Environment names a non-empty immutable ID ({environment_id!r})")
    check.check(all(isinstance(value, str) and value for value in identities.values()) and
                len(set(identities.values())) == len(identities),
                f"every Machine names a distinct non-empty immutable ID ({identities})")
    # Stability is only meaningful across reads, and across two ways of naming
    # the same Environment: an ID minted per answer would pass a single read.
    first = _identity_map(payload)
    again = read_status(ctx, check, "mix-again", project=instance["project"], env=instance["env"])
    by_name = ctx.run(check, "mix-status-by-name", ["--json", "status", "--environment", "default"],
                      cwd=instance["project"], env=instance["env"], timeout=60)
    try:
        selected = json.loads(by_name.stdout.decode("utf-8")) if by_name.exit_code == 0 else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        selected = None
    check.check(_identity_map(again) == first,
                f"a second read reports the same identities ({_identity_map(again) == first})")
    check.check(selected is not None and _identity_map(selected) == first,
                "selecting the Environment by name reports the same identities "
                f"(exit {by_name.exit_code})")
    if check.status != "PASS":
        return check.finish()

    # ── target-qualified profiles and capabilities ────────────────────────────
    for name in sorted(declared):
        want, machine = declared[name], machines[name]
        target = machine.get("target") if isinstance(machine.get("target"), dict) else {}
        check.check(machine.get("profile") == want["profile"] and
                    target.get("os") == want["target"]["os"] and
                    target.get("arch") == want["target"]["arch"],
                    f"{name} is target-qualified as declared (want {want['profile']}/"
                    f"{want['target']['os']}/{want['target']['arch']}, observed "
                    f"{machine.get('profile')!r}/{target.get('os')!r}/{target.get('arch')!r})")
        negotiated = _capabilities(machine, "negotiated_capabilities")
        requested = _capabilities(machine, "requested_capabilities")
        developer_linux = want["profile"] == "developer" and want["target"]["os"] == "linux"
        if developer_linux:
            check.check(DOCKER_CAPABILITIES <= negotiated,
                        f"{name} implicitly negotiated the private Docker capabilities "
                        f"(missing {sorted(DOCKER_CAPABILITIES - negotiated)})")
        else:
            # Criterion 2's Hardened clause, and the product rule that native
            # Machines never acquire Docker. Asserted on requested as well:
            # a capability that was asked for and refused still tells the reader
            # this Machine was meant to have Docker.
            check.check(not (DOCKER_CAPABILITIES & (negotiated | requested)),
                        f"{name} holds no Docker capability at all (observed negotiated "
                        f"{sorted(DOCKER_CAPABILITIES & negotiated)}, requested "
                        f"{sorted(DOCKER_CAPABILITIES & requested)})")

    # ── Docker contexts, present exactly where the profile grants them ────────
    contexts = {}
    for name in sorted(declared):
        want, machine = declared[name], machines[name]
        developer_linux = want["profile"] == "developer" and want["target"]["os"] == "linux"
        context = machine.get("docker_context")
        if developer_linux:
            if not check.check(isinstance(context, dict), f"{name} reports a Docker context "
                               f"(observed {context!r})"):
                continue
            contexts[name] = tuple(context.get(field) for field in ("name", "endpoint", "engine_id"))
            check.check(all(isinstance(value, str) and value for value in contexts[name]),
                        f"{name}'s Docker context names itself, its endpoint and its engine "
                        f"({contexts[name]})")
            check.check(machine.get("docker_context_availability") == "persisted_ready_not_live_probed",
                        f"{name}'s Docker context is a persisted Ready projection (observed "
                        f"{machine.get('docker_context_availability')!r})")
        else:
            check.check("docker_context" not in machine and
                        "docker_context_availability" not in machine,
                        f"{name} omits every Docker context field (observed "
                        f"{sorted(set(machine) & {'docker_context', 'docker_context_availability'})})")
    check.check(len(set(contexts.values())) == len(contexts),
                f"each Developer Machine's Docker context is its own ({contexts})")

    # ── topology and endpoints ────────────────────────────────────────────────
    networks = environment.get("networks") or []
    if not check.check(len(networks) == 1 and networks[0].get("name") == MIXED_NETWORK and
                       networks[0].get("kind") == "private",
                       f"the declared private network is reported (observed {networks})"):
        return check.finish()
    network_id = networks[0].get("network_id")
    check.check(isinstance(network_id, str) and network_id != "",
                f"the network names an immutable ID ({network_id!r})")
    attached = {name for name, machine in declared.items() if machine.get("networks")}
    observed_attached = {}
    for attachment in environment.get("network_attachments") or []:
        observed_attached[attachment.get("machine_id")] = attachment.get("network_id")
    expected_attached = {identities[name]: network_id for name in attached}
    check.check(observed_attached == expected_attached,
                f"exactly the network-declaring Machines hold a port ({sorted(attached)}); "
                f"observed {observed_attached}, expected {expected_attached}")
    endpoints = environment.get("endpoints") or []
    declared_endpoint = definition["environment"]["endpoints"][0]
    check.check(len(endpoints) == 1 and endpoints[0].get("name") == declared_endpoint["name"] and
                endpoints[0].get("machine_id") == identities[declared_endpoint["machine"]] and
                endpoints[0].get("network_id") == network_id and
                endpoints[0].get("protocol") == declared_endpoint["protocol"] and
                endpoints[0].get("port") == declared_endpoint["port"],
                f"the declared endpoint is reported on its own Machine and network (observed "
                f"{endpoints})")

    # ── health, as this daemon observed it ────────────────────────────────────
    readings = {name: machine.get("health") for name, machine in machines.items()}
    states = {name: machine.get("state") for name, machine in machines.items()}
    check.check(set(readings.values()) <= MACHINE_HEALTH_READINGS,
                f"every health reading is one this contract defines ({readings})")
    check.check(all(state == "ready" for state in states.values()) and
                all(reading == "supervised" for reading in readings.values()),
                f"every Machine is Ready and supervised by the daemon that answered "
                f"(states {states}, health {readings})")
    if check.status != "PASS":
        return check.finish()

    # ── ambiguous exec fails closed ───────────────────────────────────────────
    # Proved in both directions: the ambiguous form must refuse AND must not
    # have run, and the same command with `--machine` must succeed, or "fails
    # closed" is indistinguishable from "exec is broken".
    # Deliberately the same command shape `machine_exec_argv` uses, minus
    # `--machine`: a different shape could refuse for a different reason.
    ambiguous = ctx.run(check, "mix-exec-ambiguous",
                        ["--json", "exec", "--environment", "default", "--", "/bin/busybox", "sh",
                         "-c", f"/bin/busybox echo {AMBIGUOUS_SENTINEL}"],
                        cwd=instance["project"], env=instance["env"], timeout=120)
    detail = ambiguous.stderr.decode("utf-8", "replace")
    check.check(ambiguous.exit_code != 0 and AMBIGUOUS_SENTINEL.encode() not in ambiguous.stdout,
                f"`vz exec` without --machine refuses and runs nothing (exit "
                f"{ambiguous.exit_code}, stdout {ambiguous.stdout[:80]!r})")
    check.check("ambiguous" in detail and all(name in detail for name in declared),
                "the refusal says the selection is ambiguous and lists every candidate "
                f"({detail[:300]!r})")
    resolved = machine_exec(ctx, check, "mix-exec-resolved", instance, "dev-0",
                            f"/bin/busybox echo {AMBIGUOUS_SENTINEL}")
    check.check(resolved.exit_code == 0 and AMBIGUOUS_SENTINEL.encode() in resolved.stdout,
                f"the same command with --machine runs (exit {resolved.exit_code}, "
                f"stdout {resolved.stdout[:80]!r})")
    if check.status != "PASS":
        return check.finish()

    # ── the Hardened Machine cannot use a sibling Developer endpoint ──────────
    # First the structural fact: the restricted profile is handed no port on the
    # switch, so there is no fabric NIC on its kernel cmdline at all.
    hardened_fabric = FabricState(machine_exec(ctx, check, "mix-hardened-fabric", instance,
                                               "hardened-0", FABRIC_PROBE))
    check.check(hardened_fabric.declared == [],
                f"the Hardened Machine holds no Environment fabric port ({hardened_fabric.evidence()})")
    server_state = FabricState(machine_exec(ctx, check, "mix-dev0-fabric", instance, "dev-0",
                                            FABRIC_PROBE))
    port = server_state.port()
    if not check.check(port is not None, "dev-0 carries the fabric address the host derived "
                       f"({server_state.evidence()})"):
        return check.finish()
    address = port["address"]
    token = "vzmix-" + uuid.uuid4().hex[:16]
    # Held open, not backgrounded: a Machine exec SIGKILLs its whole process
    # group before reporting, so a daemonised httpd is already dead when the
    # sibling fetches -- which reads exactly like a denial.
    server = hold_machine_exec(ctx, check, "mix-serve", instance, "dev-0",
                               f"/bin/busybox mkdir -p /www; printf %s {token} > /www/index.html; "
                               f"/bin/busybox httpd -f -p {MIXED_PORT} -h /www")
    try:
        local = None
        for attempt in range(1, LISTENER_ATTEMPTS + 1):
            local = machine_exec(ctx, check, "mix-serve-local", instance, "dev-0",
                                 f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{MIXED_PORT}/")
            if local.exit_code == 0 and local.stdout.strip() == token.encode():
                break
            time.sleep(LISTENER_INTERVAL)
        if not check.check(local is not None and local.exit_code == 0 and
                           local.stdout.strip() == token.encode(),
                           f"the endpoint answers on dev-0's own fabric address after {attempt} "
                           f"attempt(s) (exit {None if local is None else local.exit_code})"):
            return check.finish()
        sibling = machine_exec(ctx, check, "mix-sibling", instance, "dev-1",
                               f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{MIXED_PORT}/")
        if not check.check(sibling.exit_code == 0 and sibling.stdout.strip() == token.encode(),
                           f"a Developer sibling reads the endpoint (exit {sibling.exit_code}, "
                           f"{sibling.stdout[:80]!r})"):
            return check.finish()
        # The denial must not rest on a missing applet. Prove the Hardened
        # Machine's own wget works by fetching a listener it holds itself; only
        # then is its failure against the sibling a routing fact.
        control_token = "vzmix-hardened-" + uuid.uuid4().hex[:8]
        control = hold_machine_exec(ctx, check, "mix-hardened-serve", instance, "hardened-0",
                                    f"/bin/busybox mkdir -p /www; printf %s {control_token} > /www/index.html; "
                                    f"/bin/busybox httpd -f -p {MIXED_PORT} -h /www")
        try:
            loopback = None
            for attempt in range(1, LISTENER_ATTEMPTS + 1):
                loopback = machine_exec(ctx, check, "mix-hardened-loopback", instance, "hardened-0",
                                        f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - "
                                        f"http://127.0.0.1:{MIXED_PORT}/")
                if loopback.exit_code == 0 and loopback.stdout.strip() == control_token.encode():
                    break
                time.sleep(LISTENER_INTERVAL)
            if not check.check(loopback is not None and loopback.exit_code == 0 and
                               loopback.stdout.strip() == control_token.encode(),
                               "the Hardened Machine's own HTTP client and server work, so its "
                               "failure below is a routing fact (exit "
                               f"{None if loopback is None else loopback.exit_code})"):
                return check.finish()
            denied = machine_exec(ctx, check, "mix-hardened-denied", instance, "hardened-0",
                                  f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - "
                                  f"http://{address}:{MIXED_PORT}/; printf ':%s' $?")
            check.check(denied.exit_code == 0 and token.encode() not in denied.stdout and
                        not denied.stdout.strip().endswith(b":0"),
                        "the Hardened Machine cannot reach the sibling Developer endpoint "
                        f"(observed {denied.stdout[:120]!r})")
        finally:
            released = ctx.release(check, control)
            check.check(released.exit_code is not None,
                        f"the Hardened control listener was released (exit {released.exit_code})")
    finally:
        released = ctx.release(check, server)
        check.check(released.exit_code is not None,
                    f"the held endpoint listener was released (exit {released.exit_code})")
    if check.status != "PASS":
        return check.finish()

    # ── health is an observation, not a constant ──────────────────────────────
    # Every reading above was `supervised`, which a hard-coded field would also
    # produce. Stop retires the daemon's sessions, so the same Machines must now
    # read `inactive` -- and the identities must not have moved.
    stopped = ctx.run(check, "mix-stop", ["--json", "stop", "--environment", "default",
                                          "--timeout", "120"],
                      cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
    check.check(stopped.exit_code == 0, f"the Environment stopped (exit {stopped.exit_code})")
    after = read_status(ctx, check, "mix-after-stop", project=instance["project"], env=instance["env"])
    if check.check(after is not None, "status is readable after Stop"):
        rows = _machines_by_name(after["environments"][0]) if after.get("environments") else {}
        after_states = {name: machine.get("state") for name, machine in rows.items()}
        after_health = {name: machine.get("health") for name, machine in rows.items()}
        check.check(sorted(rows) == sorted(declared) and _identity_map(after) == first,
                    f"Stop preserved every Machine and its identity ({sorted(rows)})")
        check.check(all(state == "stopped" for state in after_states.values()),
                    f"every Machine is Stopped ({after_states})")
        check.check(all(reading == "inactive" for reading in after_health.values()),
                    f"every Machine now reads inactive, so health tracked the change "
                    f"({after_health})")

    if check.status == "PASS":
        removed = ctx.run(check, "mix-delete", ["--json", "delete", "--environment", "default",
                                                "--timeout", "120"],
                          cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
        check.check(removed.exit_code == 0, f"deleted (exit {removed.exit_code})")

    # Everything above is the Linux half plus, when a template is registered,
    # the native macOS Machine. Criterion 2 names "one native macOS Machine"
    # explicitly, so a host that cannot build one has not satisfied the
    # criterion, and saying so by name is the only honest close.
    if check.status == "PASS" and macos_entry is None:
        check.not_implemented = (
            "criterion 2 requires the Environment to hold one native macOS Machine alongside the "
            "two Developer Linux Machines and the Hardened Linux Machine. This release's "
            "machine-target-catalog.json registers no Developer macOS target, so no macOS Machine "
            "was declared, built, or reported, and none of the macOS clause -- its target-qualified "
            "profile and capabilities, its health, its absence of a Docker context -- was "
            "exercised. The three Linux Machines above passed in full. The catalog a "
            "release candidate ships is written by its own vz-runtimed with the Linux profiles "
            "only (scripts/build-vz-0.4-release-candidate.sh step 7); a locally installed template "
            "registered by vz-macos-setup lives in a separate installation prefix and does not "
            "reach it. This check never provisions or registers a template.")
    return check.finish()


# -- criterion 17: workspace and storage policy --------------------------------

# Every declared projection and volume appears under one guest prefix, so a
# `vz exec` script names a path the definition chose rather than one this check
# assumed. `/vz-storage` is not special to the runtime; it is simply a prefix no
# Machine image already occupies.
STORAGE_ROOT = "/vz-storage"
RW_TARGET = STORAGE_ROOT + "/rw"
RO_TARGET = STORAGE_ROOT + "/ro"
SNAPSHOT_TARGET = STORAGE_ROOT + "/snapshot"
CACHE_TARGET = STORAGE_ROOT + "/cache"
BLOCK_TARGET = STORAGE_ROOT + "/block"
# Declared in the definition and read back out of it, never restated: the
# fixture below polls to exactly this bound, so a definition that declared a
# different one would change what the fixture proves.
STALENESS_BOUND_MILLIS = 4000
BLOCK_VOLUME_BYTES = 16 * 1024 * 1024
# Files each Machine writes into the shared cache. Small enough to stay well
# inside an exec deadline, large enough that a lost update is visible as a
# count rather than as one missing name.
CACHE_WRITES = 8
BARRIER_ATTEMPTS = 60
BARRIER_INTERVAL = 0.5
CONSISTENCY_POLL_INTERVAL = 0.1
# How long the held writer is given to finish once its peer has. This is a
# liveness bound on the exec, deliberately not the consistency bound: it is
# waited out by asking that Machine about a file IT wrote, so no cross-Machine
# visibility is involved and the declared staleness bound is not being spent
# here. Releasing a still-running writer would SIGTERM it mid-write, which is
# how this check first came to fail intermittently for a reason that had
# nothing to do with the storage policy.
WRITER_COMPLETION_ATTEMPTS = 60
WRITER_COMPLETION_INTERVAL = 0.5
# Host-side seed content, written into the worktree before Up so every read
# below has something to observe that this check did not write from inside.
SEED = b"seed written on the host before Up\n"


def storage_definition(release_dir: Path, *, block_attachments) -> dict:
    """Three Developer Linux Machines, one of each projection mode, plus volumes.

    A Machine carries at most one workspace projection, so proving all three
    modes needs three Machines. `block_attachments` is the one thing that
    varies: the accepted definition attaches the block volume to one Machine,
    and the refused one attaches the same writable volume to two.
    """
    definition = minimal_definition(release_dir)
    environment = definition["environment"]
    base = environment["machines"][0]
    machines = []
    for index, (mode, source, target) in enumerate((
            ("read_write", "rw", RW_TARGET),
            ("read_only", "ro", RO_TARGET),
            ("snapshot", "snapshot", SNAPSHOT_TARGET))):
        machine = copy.deepcopy(base)
        machine["name"] = f"machine-{index}"
        machine["workspace"] = {"binding": "source", "target_path": target, "mode": mode,
                                "source_path": source}
        machines.append(machine)
    environment["machines"] = machines
    environment["volumes"] = [
        # Multi-attached and writable on both sides. This is the case a block
        # volume may not be, and it is exactly why the two kinds are separate.
        {"schema_version": 1, "name": "cache", "kind": "shared_cache",
         "consistency": {"model": "bounded_staleness",
                         "staleness_bound_millis": STALENESS_BOUND_MILLIS},
         "attachments": [{"machine": "machine-0", "target_path": CACHE_TARGET, "mode": "read_write"},
                         {"machine": "machine-1", "target_path": CACHE_TARGET, "mode": "read_write"}]},
        {"schema_version": 1, "name": "data", "kind": "block", "size_bytes": BLOCK_VOLUME_BYTES,
         "attachments": list(block_attachments)},
    ]
    return definition


def block_attachment(machine: str, mode: str) -> dict:
    return {"machine": machine, "target_path": BLOCK_TARGET, "mode": mode}


def seed_worktree(project: Path) -> None:
    """The three declared sources, each holding one host-written file."""
    for source in ("rw", "ro", "snapshot"):
        directory = project / source
        directory.mkdir(mode=0o700)
        write_exclusive(directory / "seed.txt", SEED)


def guest_read(ctx, check, label, instance, machine, path):
    return machine_exec(ctx, check, label, instance, machine,
                        f"/bin/busybox cat {path}")


def guest_write(ctx, check, label, instance, machine, path, payload):
    """Write from INSIDE the Machine and report the guest's own exit status.

    `printf` is the shell's builtin, so a write that fails failed because the
    filesystem refused it and not because an applet was missing.
    """
    return machine_exec(ctx, check, label, instance, machine,
                        f"printf %s {payload} > {path}; printf ':%s' $?")


def refused_write(receipt) -> bool:
    """Whether the guest's redirection was refused.

    The shell reports a refused redirection with a non-zero status appended by
    the probe itself, so this reads the guest's own verdict rather than the
    exec's transport status. A read-only VirtioFS share answers `EROFS` and a
    mode-refused copy answers `EACCES`; either is the forbidden write failing,
    and pinning one errno would make this check pass or fail on which carrier
    the runtime chose rather than on the policy.
    """
    return receipt.exit_code == 0 and not receipt.stdout.strip().endswith(b":0")


def cache_writer_script(machine: str, peer: str) -> str:
    """Write `CACHE_WRITES` files, but only once the peer has started.

    The barrier is what makes this concurrent rather than merely sequential:
    each Machine waits for the other's start marker before writing any of its
    own files, so the two write phases genuinely overlap. Without it the first
    exec could finish before the second was even issued, and the fixture would
    prove nothing about two writers.
    """
    return (
        f"/bin/busybox mkdir -p {CACHE_TARGET}; "
        f"printf started > {CACHE_TARGET}/start-{machine}; "
        f"i=0; while [ $i -lt {BARRIER_ATTEMPTS} ]; do "
        f"  [ -f {CACHE_TARGET}/start-{peer} ] && break; "
        f"  /bin/busybox sleep {BARRIER_INTERVAL}; i=$((i+1)); done; "
        f"[ -f {CACHE_TARGET}/start-{peer} ] || exit 3; "
        f"i=0; while [ $i -lt {CACHE_WRITES} ]; do "
        f"  printf %s {machine}-$i > {CACHE_TARGET}/{machine}-$i; i=$((i+1)); done; "
        f"printf done > {CACHE_TARGET}/done-{machine}"
    )


def cache_entries(ctx, check, label, instance, machine) -> set:
    receipt = machine_exec(ctx, check, label, instance, machine,
                           f"/bin/busybox ls {CACHE_TARGET}")
    if receipt.exit_code != 0:
        return set()
    return set(receipt.stdout.decode("utf-8", "replace").split())


def check_workspace_projection_policy(ctx: CheckContext, top: str) -> SubCheck:
    """Criterion 17: workspace projections, forbidden writes, and storage policy.

    Four clauses, and the check is ordered so that each is proved by something
    only that clause could produce.

    *Projection semantics* are target-qualified: every read and write below is
    issued by `vz exec` on a named Machine, and the corresponding host-side
    observation is made on the worktree file. `read_write` is proved by a write
    inside the Machine appearing on the HOST, which a private copy could not do.
    `snapshot` is proved by the opposite: the guest's write succeeds and the
    host file is unchanged, which a share could not do. Asserting only that each
    mode mounted would have passed for all three carriers being the same one.

    *Forbidden writes fail* is asserted against the guest's own exit status and
    then confirmed on the host file, because a write that silently went nowhere
    and a write that was refused look identical from inside if only the byte
    content is read back.

    *Rejected before mutation* is an ordering claim, so it is proved by
    inventorying the isolate's whole state root before the refused `vz up` and
    again after, and requiring the two to be identical. Nothing is asserted
    about what the runtime "would have" written.

    *Shared-cache consistency* polls to exactly the bound the definition
    declares. Two Machines write concurrently behind a barrier, and each must
    then observe every one of the other's files within `staleness_bound_millis`
    of both writers finishing. A fixture that polled without a deadline would
    prove the writes landed eventually and nothing about the declared model.
    """
    check = SubCheck(top, "workspace_storage_policy")
    try:
        accepted = storage_definition(
            ctx.release_dir, block_attachments=[block_attachment("machine-0", "read_write")])
        refused = storage_definition(
            ctx.release_dir,
            block_attachments=[block_attachment("machine-0", "read_write"),
                               block_attachment("machine-1", "read_only")])
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()

    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if not schema_path.is_file():
        check.fail(f"project definition schema absent: {PROJECT_DEFINITION_SCHEMA}")
        return check.finish()
    validator = Draft202012Validator(load_json(schema_path))
    for label, definition in (("accepted", accepted), ("refused", refused)):
        problems = sorted(validator.iter_errors(definition), key=lambda e: list(map(str, e.absolute_path)))
        check.check(not problems, f"the {label} storage definition validates against the authoring schema"
                    if not problems else f"the {label} definition is invalid: {problems[0].message[:200]}")
    # The refused definition must be SCHEMA-VALID: a multi-attach the schema
    # rejected would never reach the runtime, and the refusal below would be
    # proving that the authoring schema works rather than that Up refuses
    # before mutation.
    if check.status != "PASS":
        return check.finish()

    # -- clause: a writable block volume on two Machines, refused before mutation
    deny = ctx.isolated("store-deny", project_files={
        "vz.json": json.dumps(refused, indent=2, sort_keys=True).encode() + b"\n"},
        provision=True)
    seed_worktree(deny["project"])
    for argv in (["init", "--quiet", "--initial-branch", "main"],
                 ["add", "-A"],
                 ["-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                  "commit", "--quiet", "-m", "definition"]):
        ctx.run_tool(check, "store-deny-git", [GIT, *argv], cwd=deny["project"], env=deny["env"])
    # Taken after the repository exists and immediately before the refused Up,
    # so the comparison spans that invocation and nothing else. An inventory
    # taken earlier would also carry this check's own `git init`.
    before, path = write_inventory(ctx.evidence_dir, "storage-deny-before", deny["project"])
    check.evidence.append(path)
    denied = ctx.run(check, "store-deny-up", ["--json", "up"], cwd=deny["project"], env=deny["env"],
                     timeout=UP_TIMEOUT)
    detail = ""
    try:
        detail = json.loads(denied.stderr.decode("utf-8")).get("error", {}).get("message", "")
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        detail = ""
    if "adapters remain required" in detail:
        check.not_implemented = ("declared volumes are not applied by this runtime: " + detail[:300])
        return check.finish()
    check.check(denied.exit_code != 0,
                f"a writable block volume on two Machines is refused (vz up exit {denied.exit_code})")
    # Named, not merely non-zero: an Up that failed for an unrelated reason
    # would satisfy a bare exit-code assertion and prove nothing about the rule.
    check.check("data" in detail and "machine-0" in detail and "machine-1" in detail,
                f"the refusal names the volume and both Machines (message {detail[:200]!r})")
    _after, path = write_inventory(ctx.evidence_dir, "storage-deny-after", deny["project"])
    check.evidence.append(path)

    # "Rejected before mutation" is an ordering claim, so it is proved by
    # observing what is NOT there rather than by asserting an intention. Three
    # separate things must all still be absent, because each could be true while
    # another was violated:
    #
    #   * the worktree, byte for byte -- the refusal must not have touched the
    #     user's own files;
    #   * the volume storage root -- no image and no cache directory was
    #     allocated for a declaration that was refused;
    #   * the persisted topology -- no Environment exists to be deleted later.
    #
    # The lane state root as a whole is deliberately NOT compared: an autospawned
    # daemon legitimately creates its own database and runtime directory on
    # startup, and folding that in would make this assertion about daemon
    # start-up rather than about the declaration.
    _unchanged(check, "the worktree across the refused Up", before, deny["project"])
    volume_root = Path(deny["runtime"]) / "volumes"
    survivors = sorted(p.name for p in volume_root.iterdir()) if volume_root.is_dir() else []
    check.check(not survivors,
                "no volume storage was allocated for the refused declaration"
                if not survivors else
                f"the refused Up allocated storage under {volume_root}: {survivors[:6]}")
    persisted = ctx.run(check, "store-deny-status", ["--json", "status"], cwd=deny["project"],
                        env=deny["env"], timeout=60)
    environments = None
    if persisted.exit_code == 0:
        try:
            environments = json.loads(persisted.stdout.decode("utf-8")).get("environments")
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            environments = "unreadable"
    check.check(persisted.exit_code != 0 or environments == [],
                f"no Environment was persisted by the refused Up (status exit "
                f"{persisted.exit_code}, environments {environments!r})")
    if check.status != "PASS":
        return check.finish()

    # -- the accepted topology
    data = json.dumps(accepted, indent=2, sort_keys=True).encode() + b"\n"
    write_exclusive(ctx.evidence_dir / "storage-vz.json.txt", data)
    check.evidence.append("storage-vz.json.txt")
    inside = ctx.isolated("store-ok", project_files={"vz.json": data}, provision=True)
    seed_worktree(inside["project"])
    for argv in (["init", "--quiet", "--initial-branch", "main"],
                 ["add", "-A"],
                 ["-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                  "commit", "--quiet", "-m", "definition"]):
        receipt = ctx.run_tool(check, "store-ok-git", [GIT, *argv], cwd=inside["project"], env=inside["env"])
        check.check(receipt.exit_code == 0, f"store-ok git {argv[0]}: exit {receipt.exit_code} (expected 0)")
    if check.status != "PASS":
        return check.finish()
    up = ctx.run(check, "store-ok-up", ["--json", "up"], cwd=inside["project"], env=inside["env"],
                 timeout=UP_TIMEOUT)
    if up.exit_code != 0:
        try:
            message = json.loads(up.stderr.decode("utf-8")).get("error", {}).get("message", "")
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            message = ""
        if "adapters remain required" in message:
            check.not_implemented = ("declared projections and volumes are not applied by this runtime: " +
                                     message[:300])
            return check.finish()
        check.check(False, f"store-ok: vz --json up exit {up.exit_code} (expected 0): {message[:200]}")
        return check.finish()
    check.check(True, "the three-mode storage topology comes up (vz --json up exit 0)")

    try:
        # -- clause: target-qualified file semantics, one mode at a time
        # read_write: written inside machine-0, observed on the HOST.
        token = "vzrw-" + uuid.uuid4().hex[:16]
        wrote = guest_write(ctx, check, "storage-rw-write", inside, "machine-0",
                            f"{RW_TARGET}/guest.txt", token)
        check.check(wrote.exit_code == 0 and wrote.stdout.strip().endswith(b":0"),
                    f"machine-0 writes into its read_write projection (guest status {wrote.stdout[-8:]!r})")
        host_copy = inside["project"] / "rw" / "guest.txt"
        observed = host_copy.read_bytes() if host_copy.is_file() else b""
        check.check(observed == token.encode(),
                    f"the read_write projection is the worktree itself: the host file holds the "
                    f"Machine's bytes (observed {observed[:40]!r})")
        # And the seed the host wrote before Up is visible inside the Machine,
        # so the share is the source in both directions.
        seen = guest_read(ctx, check, "storage-rw-read", inside, "machine-0", f"{RW_TARGET}/seed.txt")
        check.check(seen.exit_code == 0 and seen.stdout == SEED,
                    f"machine-0 reads the host-written seed through its projection (exit {seen.exit_code})")

        # read_only: reads work, writes are refused, and the host file survives.
        seen = guest_read(ctx, check, "storage-ro-read", inside, "machine-1", f"{RO_TARGET}/seed.txt")
        check.check(seen.exit_code == 0 and seen.stdout == SEED,
                    f"machine-1 reads through its read_only projection (exit {seen.exit_code})")
        forbidden = guest_write(ctx, check, "storage-ro-write", inside, "machine-1",
                                f"{RO_TARGET}/seed.txt", "overwritten")
        check.check(refused_write(forbidden),
                    f"a write into a read_only projection fails (guest status {forbidden.stdout[-8:]!r})")
        survived = (inside["project"] / "ro" / "seed.txt").read_bytes()
        check.check(survived == SEED,
                    f"the read_only source is byte-identical after the refused write (observed {survived[:40]!r})")
        created = guest_write(ctx, check, "storage-ro-create", inside, "machine-1",
                              f"{RO_TARGET}/new.txt", "created")
        check.check(refused_write(created),
                    f"creating a file in a read_only projection fails too (guest status {created.stdout[-8:]!r})")
        check.check(not (inside["project"] / "ro" / "new.txt").exists(),
                    "no file appeared in the read_only source")

        # snapshot: the guest's write SUCCEEDS and the host is untouched. Both
        # halves are needed: success alone is read_write, and an unchanged host
        # alone is read_only.
        seen = guest_read(ctx, check, "storage-snap-read", inside, "machine-2",
                          f"{SNAPSHOT_TARGET}/seed.txt")
        check.check(seen.exit_code == 0 and seen.stdout == SEED,
                    f"machine-2's snapshot carries the source's content (exit {seen.exit_code})")
        private = "vzsnap-" + uuid.uuid4().hex[:16]
        wrote = guest_write(ctx, check, "storage-snap-write", inside, "machine-2",
                            f"{SNAPSHOT_TARGET}/seed.txt", private)
        check.check(wrote.exit_code == 0 and wrote.stdout.strip().endswith(b":0"),
                    f"machine-2 may write into its own snapshot (guest status {wrote.stdout[-8:]!r})")
        readback = guest_read(ctx, check, "storage-snap-verify", inside, "machine-2",
                              f"{SNAPSHOT_TARGET}/seed.txt")
        check.check(readback.stdout.strip() == private.encode(),
                    f"the snapshot keeps the Machine's own write (observed {readback.stdout[:40]!r})")
        untouched = (inside["project"] / "snapshot" / "seed.txt").read_bytes()
        check.check(untouched == SEED,
                    f"the snapshot source is byte-identical on the host: the copy is private "
                    f"(observed {untouched[:40]!r})")

        # -- clause: the block volume is the attached Machine's alone
        block_token = "vzblk-" + uuid.uuid4().hex[:16]
        wrote = guest_write(ctx, check, "storage-block-write", inside, "machine-0",
                            f"{BLOCK_TARGET}/payload", block_token)
        check.check(wrote.exit_code == 0 and wrote.stdout.strip().endswith(b":0"),
                    f"machine-0 writes into its block volume (guest status {wrote.stdout[-8:]!r})")
        readback = guest_read(ctx, check, "storage-block-read", inside, "machine-0",
                              f"{BLOCK_TARGET}/payload")
        check.check(readback.stdout.strip() == block_token.encode(),
                    f"the block volume reads back what was written (observed {readback.stdout[:40]!r})")
        absent = machine_exec(ctx, check, "storage-block-absent", inside, "machine-1",
                              f"/bin/busybox cat {BLOCK_TARGET}/payload; printf ':%s' $?")
        check.check(block_token.encode() not in absent.stdout and
                    not absent.stdout.strip().endswith(b":0"),
                    f"the Machine the block volume is NOT attached to cannot read it "
                    f"(observed {absent.stdout[:60]!r})")

        # -- clause: declared shared-cache consistency, concurrent fixture
        writer = hold_machine_exec(ctx, check, "storage-cache-write-0", inside, "machine-0",
                                   cache_writer_script("machine-0", "machine-1"),
                                   timeout=BARRIER_ATTEMPTS * 2)
        finished = False
        try:
            peer = machine_exec(ctx, check, "storage-cache-write-1", inside, "machine-1",
                                cache_writer_script("machine-1", "machine-0"),
                                timeout=int(BARRIER_ATTEMPTS * BARRIER_INTERVAL) + 60)
            check.check(peer.exit_code == 0,
                        f"machine-1's concurrent cache writer completed (exit {peer.exit_code}); "
                        "exit 3 means it never saw machine-0 start, so the two never overlapped")
            # machine-1 returning does not mean machine-0 has finished: it was
            # released from the barrier at most one poll interval ago and still
            # has its own files to write. Waiting for machine-0's own marker, on
            # machine-0, is what makes the release below a no-op on a process
            # that has already exited.
            for attempt in range(1, WRITER_COMPLETION_ATTEMPTS + 1):
                marker = machine_exec(ctx, check, "storage-cache-settle", inside, "machine-0",
                                      f"/bin/busybox cat {CACHE_TARGET}/done-machine-0")
                if marker.exit_code == 0 and marker.stdout.strip() == b"done":
                    finished = True
                    break
                time.sleep(WRITER_COMPLETION_INTERVAL)
            check.check(finished,
                        f"machine-0's writer finished its own writes after {attempt} poll(s)"
                        if finished else
                        f"machine-0's writer never wrote its own completion marker within "
                        f"{WRITER_COMPLETION_ATTEMPTS * WRITER_COMPLETION_INTERVAL:.0f}s")
        finally:
            released = ctx.release(check, writer)
        check.check(released.exit_code == 0,
                    f"machine-0's concurrent cache writer completed (exit {released.exit_code}); "
                    "a negative status is the release signal, meaning it was still running")
        if check.status != "PASS":
            return check.finish()
        expected = ({f"machine-0-{index}" for index in range(CACHE_WRITES)} |
                    {f"machine-1-{index}" for index in range(CACHE_WRITES)} |
                    {"start-machine-0", "start-machine-1", "done-machine-0", "done-machine-1"})
        # Both writers have exited, so every write above is closed. The declared
        # model gives each Machine `staleness_bound_millis` from that moment to
        # observe them all; the deadline is what makes this an assertion about
        # the declaration rather than about eventual convergence.
        deadline = time.monotonic() + STALENESS_BOUND_MILLIS / 1000.0
        converged, observed, polls = {}, {}, 0
        while time.monotonic() < deadline and len(converged) < 2:
            polls += 1
            for machine in ("machine-0", "machine-1"):
                if machine in converged:
                    continue
                entries = cache_entries(ctx, check, "storage-cache-poll-" + machine, inside, machine)
                observed[machine] = entries
                if expected <= entries:
                    converged[machine] = time.monotonic()
            if len(converged) < 2:
                time.sleep(CONSISTENCY_POLL_INTERVAL)
        for machine in ("machine-0", "machine-1"):
            missing = sorted(expected - observed.get(machine, set()))
            check.check(machine in converged,
                        f"{machine} observed every concurrent write within the declared "
                        f"{STALENESS_BOUND_MILLIS} ms staleness bound after {polls} poll(s)"
                        if machine in converged else
                        f"{machine} still missing {missing[:6]} after the declared "
                        f"{STALENESS_BOUND_MILLIS} ms staleness bound")
        # Convergence on names alone would pass if two writers had overwritten
        # each other's files, so the contents are compared too: a lost update
        # shows here and nowhere above.
        for machine, peer in (("machine-0", "machine-1"), ("machine-1", "machine-0")):
            sample = machine_exec(ctx, check, "storage-cache-verify-" + machine, inside, machine,
                                  f"/bin/busybox cat {CACHE_TARGET}/{peer}-0")
            check.check(sample.stdout.strip() == f"{peer}-0".encode(),
                        f"{machine} reads {peer}'s own bytes out of the shared cache, not a "
                        f"clobbered copy (observed {sample.stdout[:40]!r})")
    finally:
        if check.status == "PASS":
            for name, instance in (("store-ok", inside), ("store-deny", deny)):
                removed = ctx.run(check, name + "-delete",
                                  ["--json", "delete", "--environment", "default", "--timeout", "120"],
                                  cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
                # The refused Environment was never created, so `delete` legitimately
                # has nothing to remove; only the one that came up must delete cleanly.
                if name == "store-ok":
                    check.check(removed.exit_code == 0,
                                f"{name}: deleted (exit {removed.exit_code})")
    return check.finish()


# --------------------------------------------------------------- criterion 6

# The public-like network's name, the endpoint port behind its edge, and the
# `.test` names two separate Environments publish. `.test` is reserved for
# exactly this: a name that must never resolve or validate outside the context
# that defined it.
PUBLIC_NETWORK = "edge"
PUBLIC_ORIGIN_PORT = 8080
PUBLIC_NAMES = ("api.one.test", "api.two.test")
UNDECLARED_NAME = "admin.one.test"

# The Developer image's certificate-verifying HTTPS client, and where a client
# inside a Machine is handed the authority its own Environment published.
#
# The client is deliberately rigid: it has no `--insecure`, and its DEFAULT
# trust store is the pinned public bundle the image already ships at
# /etc/vz/ca-certificates.crt. That default is what makes the negative claims
# below mean something -- an Environment's own authority is not in any public
# bundle, so a fetch that omits `--ca-file` MUST be refused, and one that
# names another Environment's authority must be refused too.
GUEST_FETCH = "/usr/local/bin/vz-guest-fetch"
GUEST_ANCHOR_DIR = "/run/vz-edge"
# `vz-guest-fetch` gives every failure its own status; 7 is "the peer's chain
# was refused". Asserting the exact status is what keeps a negative TLS claim
# from passing because the name failed to resolve or nothing was listening.
FETCH_CERTIFICATE_REJECTED = "7"
FETCH_TIMEOUT_MILLIS = 20000
# The path the origin serves its own view of the connection on. BusyBox `httpd`
# runs anything under `cgi-bin/` as CGI and hands it `REMOTE_ADDR`, which is the
# only way the ORIGIN (rather than the client, or the host) gets to say which
# peer it saw -- and therefore the only first-hand evidence of the translation.
ORIGIN_CGI_PATH = "/cgi-bin/peer"

# What a Machine on a public-like network knows about its own edge, read from
# the guest rather than assumed from the host.
#
#   APPLET <name>          every applet this BusyBox actually carries. The guest
#                          image is not a promise; a clause that needs an applet
#                          this build lacks must be reported unexercised rather
#                          than failed or quietly skipped.
#   CMDLINE <value>        `vz.net.N=<mac>,<cidr>[,<gateway>]` as the HOST wrote
#                          it, so the guest's resolver can be compared against
#                          the address the host planned instead of against
#                          itself.
#   DNSARG <value>         `vz.dns.N=<ipv4>`, likewise.
#   RESOLV <line>          `/etc/resolv.conf` as the guest is actually running
#                          with. This is the file every resolver call reads, so
#                          it is the only thing that makes "resolved through the
#                          Environment" a fact rather than an intention.
#   HOSTS <line>           `/etc/hosts`. A published name appearing here would
#                          mean the resolver was never asked, and every DNS
#                          claim below would pass without the resolver existing.
#   ADDR <iface> <cidr>    the Machine's own IPv4 addresses.
#   FETCH <yes|no>         whether this Machine actually carries the HTTPS
#                          client. Read off the running root rather than
#                          assumed from the image having been built with it:
#                          the binary is copied across the overlay/chroot
#                          boundary by `init`, and a copy that did not happen
#                          leaves a Machine that cannot fetch anything.
PUBLIC_EDGE_PROBE = (
    '/bin/busybox --list | /bin/busybox awk \'{print "APPLET", $0}\'; '
    'for p in $(/bin/busybox cat /proc/cmdline); do '
    '  case "$p" in '
    '    vz.net.*=*) printf "CMDLINE %s\\n" "${p#*=}" ;; '
    '    vz.dns.*=*) printf "DNSARG %s\\n" "${p#*=}" ;; '
    '  esac; '
    'done; '
    '/bin/busybox cat /etc/resolv.conf 2>/dev/null | /bin/busybox awk \'{print "RESOLV", $0}\'; '
    '/bin/busybox cat /etc/hosts 2>/dev/null | /bin/busybox awk \'{print "HOSTS", $0}\'; '
    '/bin/busybox ip -o -4 addr show | /bin/busybox awk \'$2!="lo"{print "ADDR", $2, $4}\'; '
    f'if [ -x {GUEST_FETCH} ]; then printf "FETCH yes\\n"; else printf "FETCH no\\n"; fi'
)


class EdgeState:
    """One Machine's view of its Environment's edge, as the probe reported it."""

    def __init__(self, receipt):
        self.applets = set()
        self.gateways = []      # gateways named on the kernel cmdline
        self.resolver_args = []  # `vz.dns.N` values
        self.resolv_conf = []   # nameserver addresses in /etc/resolv.conf
        self.hosts = []         # raw /etc/hosts lines
        self.addresses = []     # this Machine's own IPv4 addresses
        self.https_client = False  # whether GUEST_FETCH is present and executable
        for line in receipt.stdout.decode("ascii", "replace").splitlines():
            row = line.split()
            if row[:1] == ["APPLET"] and len(row) == 2:
                self.applets.add(row[1])
            elif row[:1] == ["CMDLINE"] and len(row) == 2:
                fields = row[1].split(",")
                if len(fields) >= 3 and fields[2]:
                    self.gateways.append(fields[2])
            elif row[:1] == ["DNSARG"] and len(row) == 2:
                self.resolver_args.append(row[1])
            elif row[:1] == ["RESOLV"] and len(row) == 3 and row[1] == "nameserver":
                self.resolv_conf.append(row[2])
            elif row[:1] == ["HOSTS"]:
                self.hosts.append(" ".join(row[1:]))
            elif row[:1] == ["ADDR"] and len(row) == 3:
                self.addresses.append(row[2].split("/")[0])
            elif row[:2] == ["FETCH", "yes"]:
                self.https_client = True

    def fabric_address(self, edge: str):
        """This Machine's address on the edge's own network, and no other.

        A Machine carries Apple's NAT address as well as its fabric one, and the
        origin the edge dials is the fabric one. Picking by /24 rather than by
        interface name is the same rule the fabric checks use: the host cannot
        predict which name the NIC gets, but it derived the range.
        """
        prefix = edge.rsplit(".", 1)[0] + "."
        matching = [address for address in self.addresses if address.startswith(prefix)]
        return matching[0] if len(matching) == 1 else None

    def evidence(self) -> str:
        return (f"cmdline gateways {self.gateways!r}, vz.dns {self.resolver_args!r}, "
                f"resolv.conf {self.resolv_conf!r}, addresses {self.addresses!r}, "
                f"hosts {self.hosts!r}, https client {self.https_client}")


def public_like_definition(release_dir: Path, hostname: str) -> dict:
    """One Environment, two Machines, one public-like network, one `https` name.

    The endpoint is declared `https` because that is what the edge terminates;
    a `tcp` or `http` endpoint on a public-like network is refused at plan time
    rather than published behind a listener that does not exist.
    """
    definition = minimal_definition(release_dir)
    environment = definition["environment"]
    first = environment["machines"][0]
    second = copy.deepcopy(first)
    second["name"] = "machine-1"
    for machine in (first, second):
        machine["networks"] = [PUBLIC_NETWORK]
    environment["machines"] = [first, second]
    environment["networks"] = [{"schema_version": 1, "name": PUBLIC_NETWORK, "kind": "simulated_public"}]
    environment["endpoints"] = [{"schema_version": 1, "name": "api", "machine": first["name"],
                                 "network": PUBLIC_NETWORK, "protocol": "https",
                                 "port": PUBLIC_ORIGIN_PORT, "hostname": hostname}]
    return definition


def edge_anchor(instance: dict) -> Path:
    """The Environment authority the daemon published for this Environment.

    One file per Environment per network, under the daemon's own runtime
    directory. Reading it from the host is what lets a client inside the
    Environment be handed the one certificate that verifies its edge; finding
    exactly one is also how this check learns which Environment and network the
    edge belongs to without being told.
    """
    root = Path(instance["env"]["VZ_RUNTIME_DATA_DIR"]) / "environment-edges"
    return sorted(root.glob("*/*/authority.pem")) if root.is_dir() else []


def _lookup(ctx, check, label, instance, machine, name):
    """Resolve one name from inside a Machine, through whatever resolver it has.

    Deliberately not given a server argument. The claim is that the Machine
    resolves the Environment's names through its Environment, and a lookup that
    named the resolver on the command line would prove only that the resolver
    answers -- not that the Machine is pointed at it.
    """
    return machine_exec(ctx, check, label, instance, machine,
                        f'/bin/busybox nslookup {name} 2>&1; printf "EXIT:%s\\n" $?')


def _resolved(receipt) -> tuple:
    """(exit code, every address the lookup reported after the server line)."""
    text = receipt.stdout.decode("ascii", "replace")
    code, addresses, seen_name = None, [], False
    for line in text.splitlines():
        if line.startswith("EXIT:"):
            code = line.split(":", 1)[1].strip()
        elif line.startswith("Name:"):
            seen_name = True
        elif seen_name and line.startswith("Address"):
            # `Address: 10.0.0.1` and `Address 1: 10.0.0.1 name` both occur.
            fields = line.replace(":", " ").split()
            addresses.extend(field for field in fields[1:] if _is_ipv4(field))
    return code, addresses


def _is_ipv4(text: str) -> bool:
    parts = text.split(".")
    return len(parts) == 4 and all(part.isdigit() and 0 <= int(part) <= 255 for part in parts)


def install_anchor(ctx, check, label, instance, machine, name, pem: bytes):
    """Put one published certificate authority inside a Machine, verbatim.

    The daemon publishes an Environment's authority on the HOST, under its own
    runtime directory; a client inside the Environment has to be handed it. It
    is written here as the exact bytes the daemon published -- not a re-encoded
    or re-derived copy -- because what the client then verifies against is the
    whole meaning of the TLS claim, and the byte count is read back so a
    truncated write cannot look like a refused certificate later.
    """
    body = pem.decode("ascii").rstrip("\n")
    script = (f"/bin/busybox mkdir -p {GUEST_ANCHOR_DIR}; "
              f"/bin/busybox cat > {GUEST_ANCHOR_DIR}/{name} <<'VZPEM'\n{body}\nVZPEM\n"
              f"printf 'BYTES '; /bin/busybox wc -c < {GUEST_ANCHOR_DIR}/{name}")
    receipt = machine_exec(ctx, check, label, instance, machine, script)
    written = None
    for line in receipt.stdout.decode("ascii", "replace").splitlines():
        if line.startswith("BYTES"):
            fields = line.split()
            written = int(fields[1]) if len(fields) == 2 and fields[1].isdigit() else None
    check.check(receipt.exit_code == 0 and written == len(body) + 1,
                f"{label}: the published authority is inside {machine} byte for byte "
                f"(exit {receipt.exit_code}, wrote {written}, published {len(body) + 1})")
    return f"{GUEST_ANCHOR_DIR}/{name}"


# The origin behind the edge: a token that says the response came from the
# declared Machine, and the peer address that Machine's own kernel reports for
# the connection. `REMOTE_ADDR` is the origin's first-hand account of who
# connected to it, which is the only place the source translation is visible as
# a fact rather than as an intention.
def origin_script(token: str) -> str:
    return (
        "/bin/busybox mkdir -p /www/cgi-bin; "
        f"printf %s {token} > /www/token; "
        "/bin/busybox cat > /www/cgi-bin/peer <<'VZCGI'\n"
        "#!/bin/busybox sh\n"
        "printf 'Content-Type: text/plain\\r\\n\\r\\n'\n"
        "printf 'TOKEN %s\\n' \"$(/bin/busybox cat /www/token)\"\n"
        "printf 'PEER %s\\n' \"$REMOTE_ADDR\"\n"
        "VZCGI\n"
        "/bin/busybox chmod 755 /www/cgi-bin/peer; "
        f"printf %s {token} > /www/index.html; "
        f"/bin/busybox httpd -f -p {PUBLIC_ORIGIN_PORT} -h /www"
    )


class FetchResult:
    """One `vz-guest-fetch` invocation, as the Machine reported it.

    Three separate streams, kept separate on purpose: the exit status names the
    KIND of failure, stdout is the response body byte for byte, and the JSON
    receipt on stderr is the client's own account of the exchange -- the address
    it reached, the protocol it negotiated, and the trust store it verified
    against.
    """

    def __init__(self, receipt):
        self.exit = None
        self.body = []
        self.receipt = {}
        self.receipt_raw = ""
        for line in receipt.stdout.decode("utf-8", "replace").splitlines():
            if line.startswith("EXIT "):
                self.exit = line[len("EXIT "):].strip()
            elif line.startswith("BODY "):
                self.body.append(line[len("BODY "):])
            elif line.startswith("RECEIPT "):
                self.receipt_raw = line[len("RECEIPT "):]
                try:
                    self.receipt = json.loads(self.receipt_raw)
                except ValueError:
                    self.receipt = {}

    def field(self, name):
        return self.receipt.get(name)

    def reported(self, key: str):
        """A `KEY value` line the origin's CGI printed into the body."""
        for line in self.body:
            fields = line.split()
            if fields[:1] == [key] and len(fields) >= 2:
                return fields[1]
        return None

    def evidence(self) -> str:
        return f"exit {self.exit}, body {self.body!r}, receipt {self.receipt_raw[:300]!r}"


def peer_address(text):
    """One address as the ORIGIN wrote it, in the form the host planned it.

    BusyBox `httpd` accepts on an IPv6 socket, so `REMOTE_ADDR` for an IPv4 peer
    arrives as the bracketed IPv4-mapped literal `[::ffff:10.0.0.2]`. That is
    the same address written another way, and unwrapping it is exact rather
    than lenient: only the brackets and the `::ffff:` mapping prefix are
    removed, and anything that is not then a dotted quad is returned untouched
    so a comparison against it still fails.
    """
    if not isinstance(text, str):
        return text
    unwrapped = text.strip()
    if unwrapped.startswith("[") and unwrapped.endswith("]"):
        unwrapped = unwrapped[1:-1]
    lowered = unwrapped.lower()
    if lowered.startswith("::ffff:"):
        unwrapped = unwrapped[len("::ffff:"):]
    return unwrapped if _is_ipv4(unwrapped) else text


def _reported(receipt, key: str):
    """A `KEY value` line out of a plaintext CGI response on stdout."""
    if receipt is None:
        return None
    for line in receipt.stdout.decode("utf-8", "replace").splitlines():
        fields = line.split()
        if fields[:1] == [key] and len(fields) >= 2:
            return fields[1]
    return None


def guest_fetch(ctx, check, label, instance, machine, url, *, ca_file=None):
    """One HTTPS GET from inside a Machine, with its three streams kept apart.

    `ca_file` is omitted deliberately in the negative cases: with no `--ca-file`
    the client verifies against the image's pinned PUBLIC bundle, which cannot
    contain an Environment's own authority, so the fetch must be refused.
    """
    anchor = f" --ca-file {ca_file}" if ca_file else ""
    script = (f"{GUEST_FETCH} get --url {url}{anchor} --timeout-millis {FETCH_TIMEOUT_MILLIS} "
              "> /tmp/vz-fetch-body 2> /tmp/vz-fetch-receipt; code=$?; "
              "printf 'EXIT %s\\n' \"$code\"; "
              "/bin/busybox awk '{print \"BODY\", $0}' /tmp/vz-fetch-body; "
              "/bin/busybox awk '{print \"RECEIPT\", $0}' /tmp/vz-fetch-receipt")
    return FetchResult(machine_exec(ctx, check, label, instance, machine, script))


def check_public_like_ingress(ctx: CheckContext, top: str) -> SubCheck:
    """Criterion 6: the Environment's own public-like edge, and nothing on the host.

    What this proves, in the order the claims depend on each other:

    * a `simulated_public` declaration is applied at all -- it was refused
      outright until the edge existed, so an Up that succeeds here is the first
      fact;
    * the daemon published exactly one Environment certificate authority for it,
      which is the trust anchor a client inside the Environment would verify
      against, and which is a certificate and never a key;
    * every Machine on that network is booted pointing at its Environment's own
      resolver, and at nothing else -- read out of the `/etc/resolv.conf` the
      guest is actually running with, and agreeing with the address the host
      derived and wrote to that Machine's kernel cmdline;
    * the declared `.test` name resolves, through that resolver, to the EDGE's
      address and never to the address of the Machine behind it. That is the
      difference between ingress and a private shortcut, and it is asserted on
      the values rather than on the lookup having succeeded;
    * the name is not in `/etc/hosts`, so the resolver was genuinely asked;
    * an undeclared name in the same Environment does not resolve, and a second
      Environment's Machine cannot resolve this Environment's name nor this one
      the other's -- which is what makes the DNS view split rather than shared;
    * the declared API answers, from inside a Machine, over a TLS session that
      Machine's own client verified against its Environment's published
      authority -- and the SAME request is refused against the image's pinned
      public CA bundle and against a second Environment's authority. The
      refusals are what make the acceptance mean something: they are the same
      request over the same path with only the trust store changed, so the
      difference can be nothing but a verification result;
    * the response body carries the token the DECLARED origin Machine wrote, on
      the declared port, so the ingress was routed rather than answered by the
      edge for itself;
    * the origin's own `REMOTE_ADDR` for that connection is the EDGE. The same
      origin, spoken to directly by its sibling, reports the caller instead --
      which is what makes the translation a property of the path rather than of
      the CGI;
    * and no listener attributable to this run appeared on the host's LAN or on
      any wildcard address while all of it was running, while the edge's own
      address is bound nowhere on the host at all -- both read off the host's
      real listener table before and after rather than inferred from the
      absence of a bind in the source.

    What it does NOT prove, and says so rather than passing on the rest: the
    controlled-egress, host-import/export and fault-control clauses. See the
    `not_implemented` text at the end for exactly why.
    """
    check = SubCheck(top, "public_like_ingress")
    try:
        one = public_like_definition(ctx.release_dir, PUBLIC_NAMES[0])
        two = public_like_definition(ctx.release_dir, PUBLIC_NAMES[1])
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if schema_path.is_file():
        problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(one),
                          key=lambda e: list(map(str, e.absolute_path)))
        check.check(not problems, "the public-like definition validates"
                    if not problems else f"definition invalid: {problems[0].message[:200]}")
        if problems:
            return check.finish()

    # The host, before anything of this Environment exists. Everything below is
    # judged against this snapshot, because a listener that was already there is
    # not this Environment's and a claim that ignored that would be a claim
    # about the whole machine.
    scope = vz04_host.HostScope(run_id=ctx.recorder.run_id, state_root=ctx.state.root,
                                release_dir=ctx.release_dir, clients={})
    before = vz04_host.capture(scope, "public_like_ingress_before")
    check.check(before["capture_state"] == "captured",
                f"the host listener table was read before the Environment existed "
                f"({before['capture_state']}, {len(before['listeners'])} listeners)")

    inside = provision(ctx, check, "edge-a", one)
    if inside.get("unsupported"):
        check.not_implemented = ("a public-like network is not applied by this runtime: " +
                                 inside["unsupported"][:300])
        return check.finish()
    if check.status != "PASS" or not inside["status"]:
        return check.finish()
    check.ok("a `simulated_public` network with an `https` endpoint was applied by Up")

    anchors = edge_anchor(inside)
    check.check(len(anchors) == 1,
                f"the daemon published exactly one Environment authority for this edge ({anchors})")
    if len(anchors) != 1:
        return check.finish()
    anchor = read_regular(anchors[0])
    # A trust anchor and nothing else: a reader can verify the edge and cannot
    # issue under it.
    check.check(anchor.startswith(b"-----BEGIN CERTIFICATE-----") and b"PRIVATE KEY" not in anchor,
                f"the published authority is a certificate and carries no key ({len(anchor)} bytes)")

    states = {}
    for machine in ("machine-0", "machine-1"):
        probed = machine_exec(ctx, check, "edge-probe-" + machine, inside, machine, PUBLIC_EDGE_PROBE)
        state = EdgeState(probed)
        check.check(probed.exit_code == 0 and len(state.resolver_args) == 1,
                    f"{machine} was booted with exactly one Environment resolver "
                    f"({state.evidence()})")
        if probed.exit_code != 0 or len(state.resolver_args) != 1:
            return check.finish()
        # The route and the resolver are separate declarations that must name
        # the same edge; a Machine whose default route and resolver disagreed
        # would reach one thing and ask another.
        check.check(state.gateways == state.resolver_args,
                    f"{machine}'s declared route and resolver are the same edge "
                    f"(route {state.gateways}, resolver {state.resolver_args})")
        # The file every resolver call in this Machine actually reads. The
        # image ships public resolvers; finding them here would mean the
        # Environment's resolver was never installed and every lookup below
        # would have left the fabric.
        check.check(state.resolv_conf == state.resolver_args,
                    f"{machine} resolves through its Environment alone "
                    f"(resolv.conf {state.resolv_conf}, declared {state.resolver_args})")
        check.check(state.resolver_args[0] not in state.addresses,
                    f"{machine}'s resolver is the edge and not the Machine itself "
                    f"(resolver {state.resolver_args}, own addresses {state.addresses})")
        published = [line for line in state.hosts if PUBLIC_NAMES[0] in line]
        # If the name were also in the static table the resolver would never be
        # asked, and every DNS assertion below would pass with no resolver at
        # all.
        check.check(not published,
                    f"{machine} has no static /etc/hosts entry for the published name "
                    f"(matching lines {published})")
        states[machine] = state
    if check.status != "PASS":
        return check.finish()

    edge = states["machine-0"].resolver_args[0]
    origin = states["machine-0"].addresses
    check.check(edge not in states["machine-1"].addresses,
                f"the edge address is no Machine's address (edge {edge}, machine-1 {states['machine-1'].addresses})")

    if "nslookup" not in states["machine-1"].applets:
        # Reported, never assumed away: without a resolver client in the guest
        # the DNS clauses are unexercised, and passing the rest would certify
        # the criterion on evidence that never asked a resolver anything.
        check.not_implemented = (
            "criterion 6 requires a client to reach an API through environment-local split DNS; "
            "this Developer Linux guest image carries no `nslookup` applet, so no lookup could be "
            f"issued from inside a Machine at all (applets present: {len(states['machine-1'].applets)}). "
            "The edge, its published authority and the resolver configuration every Machine booted "
            "with were verified above.")
        return check.finish()

    # The declared name, resolved from the client Machine through the resolver
    # that Machine is actually configured with.
    code, addresses = _resolved(_lookup(ctx, check, "edge-lookup", inside, "machine-1", PUBLIC_NAMES[0]))
    check.check(code == "0" and addresses == [edge],
                f"the declared `.test` name resolves to the edge inside the Environment "
                f"(exit {code}, addresses {addresses}, edge {edge})")
    # The claim that makes this ingress rather than a private shortcut: the
    # client is told the edge, never the Machine that serves behind it.
    check.check(all(address not in origin for address in addresses),
                f"the published name never resolves to the origin Machine "
                f"(addresses {addresses}, machine-0 {origin})")
    undeclared = _resolved(_lookup(ctx, check, "edge-lookup-undeclared", inside, "machine-1", UNDECLARED_NAME))
    check.check(undeclared[0] != "0" and not undeclared[1],
                f"an undeclared name in the same Environment does not resolve "
                f"(exit {undeclared[0]}, addresses {undeclared[1]})")

    # Two Environments, two views. Each resolver is the whole name space its own
    # Machines can see, so neither Environment's name exists in the other.
    outside = provision(ctx, check, "edge-b", two)
    if check.status != "PASS" or not outside["status"]:
        return check.finish()
    for label, instance, name in (("edge-b-cannot-see-a", outside, PUBLIC_NAMES[0]),
                                  ("edge-a-cannot-see-b", inside, PUBLIC_NAMES[1])):
        code, addresses = _resolved(_lookup(ctx, check, label, instance, "machine-1", name))
        check.check(code != "0" and not addresses,
                    f"{label}: `{name}` does not resolve in the other Environment "
                    f"(exit {code}, addresses {addresses})")
    if check.status != "PASS":
        return check.finish()

    # ---- TLS, routed ingress and the source translation --------------------
    #
    # Everything above is about names. These are the criterion's other three
    # clauses, and none of them could be exercised until the Developer image
    # carried a client that can both complete this edge's handshake and check a
    # certificate: BusyBox `ssl_client` can do neither. `vz-guest-fetch` is that
    # client, and it has no way to disable verification, so a fetch that
    # succeeds is a fetch whose chain was checked.
    #
    # The order below is chosen so each claim rests on the one before it:
    # the client exists; the two Environments published DIFFERENT authorities;
    # the origin answers on its own address and reports the client's address
    # when spoken to directly; the same origin, reached through the edge by
    # name over TLS, reports the EDGE instead; and the same request is refused
    # against the image's public bundle and against the other Environment's
    # authority. Without those last two, "the handshake succeeded" would be
    # compatible with a client that verifies nothing.
    for machine, state in states.items():
        check.check(state.https_client,
                    f"{machine} carries the Developer image's HTTPS client at {GUEST_FETCH} "
                    f"({state.evidence()})")
    foreign_anchors = edge_anchor(outside)
    check.check(len(foreign_anchors) == 1,
                f"the second Environment published exactly one authority of its own ({foreign_anchors})")
    if check.status != "PASS":
        return check.finish()
    foreign = read_regular(foreign_anchors[0])
    # Two Environments, two authorities. If they were the same bytes, the
    # cross-Environment refusal below would prove nothing at all.
    check.check(foreign != anchor,
                f"the two Environments minted different authorities "
                f"({len(anchor)} and {len(foreign)} bytes, identical: {foreign == anchor})")
    origin_address = states["machine-0"].fabric_address(edge)
    client_address = states["machine-1"].fabric_address(edge)
    check.check(origin_address is not None and client_address is not None
                and origin_address != client_address,
                f"origin and client hold distinct addresses on the edge's own network "
                f"(machine-0 {origin_address}, machine-1 {client_address}, edge {edge})")
    if check.status != "PASS":
        return check.finish()

    own_anchor = install_anchor(ctx, check, "edge-anchor-own", inside, "machine-1",
                                "authority.pem", anchor)
    other_anchor = install_anchor(ctx, check, "edge-anchor-foreign", inside, "machine-1",
                                  "foreign.pem", foreign)
    if check.status != "PASS":
        return check.finish()

    token = "vzedge-" + uuid.uuid4().hex[:16]
    # Held open for exactly as long as the fetches need it, for the same reason
    # the private-path listener is: a Machine `exec` SIGKILLs its whole process
    # group before reporting, so a backgrounded `httpd` is already dead when the
    # sibling fetches -- which reads exactly like an edge that does not route.
    server = hold_machine_exec(ctx, check, "edge-serve", inside, "machine-0", origin_script(token))
    try:
        # The origin answering its OWN address settles the listener before the
        # edge is asked to carry anything, and it is also the control for the
        # translation claim: spoken to directly, this same CGI reports the
        # caller. If it reported the edge here too, the claim below would be an
        # artefact of the CGI rather than a fact about the path.
        direct = None
        for attempt in range(1, LISTENER_ATTEMPTS + 1):
            direct = machine_exec(ctx, check, "edge-serve-local", inside, "machine-0",
                                  f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - "
                                  f"http://{origin_address}:{PUBLIC_ORIGIN_PORT}{ORIGIN_CGI_PATH}")
            if direct.exit_code == 0 and token.encode() in direct.stdout:
                break
            time.sleep(LISTENER_INTERVAL)
        direct_reported = _reported(direct, "PEER")
        direct_peer = peer_address(direct_reported)
        check.check(direct.exit_code == 0 and token.encode() in direct.stdout,
                    f"the declared origin answers on its own fabric address after {attempt} "
                    f"attempt(s) (exit {direct.exit_code}, {direct.stdout[:120]!r})")
        check.check(direct_peer == origin_address,
                    f"spoken to directly, the origin reports the caller as its peer "
                    f"(reported {direct_reported!r} -> {direct_peer}, caller {origin_address})")
        if check.status != "PASS":
            return check.finish()

        # The criterion's own path: a client inside a Machine, reaching a
        # declared API by its published name, over TLS it verified against its
        # Environment's authority.
        url = f"https://{PUBLIC_NAMES[0]}{ORIGIN_CGI_PATH}"
        through = guest_fetch(ctx, check, "edge-fetch", inside, "machine-1", url, ca_file=own_anchor)
        check.check(through.exit == "0",
                    f"the declared `.test` API answers over verified TLS from inside a Machine "
                    f"({through.evidence()})")
        if through.exit != "0":
            return check.finish()
        # The client's own account: which address the NAME led it to, and what
        # it negotiated there. The edge terminates TLS, so this address is the
        # edge's and never the origin's.
        check.check(through.field("peer") == edge,
                    f"the TLS session terminated at the edge, not at the origin "
                    f"(client reached {through.field('peer')}, edge {edge}, origin {origin_address})")
        check.check(through.field("verified") is True and through.field("trust_anchors") == 1
                    and through.field("trust_bundle") == own_anchor,
                    f"the chain was verified against the Environment's own published authority "
                    f"and nothing else ({through.receipt_raw[:200]!r})")
        check.check(str(through.field("protocol") or "").startswith("TLSv1")
                    and through.field("status") == 200,
                    f"the exchange was TLS 1.x and the origin returned 200 "
                    f"(protocol {through.field('protocol')!r}, status {through.field('status')!r})")
        # Routed ingress: the bytes came from the DECLARED origin, on the
        # declared port, and not from the edge answering for itself.
        check.check(through.reported("TOKEN") == token,
                    f"the response body came from the declared origin Machine "
                    f"(token {through.reported('TOKEN')!r}, expected {token!r})")
        # The translation. The origin's own kernel says its peer was the edge;
        # the client's address appears nowhere on that connection.
        through_reported = through.reported("PEER")
        through_peer = peer_address(through_reported)
        check.check(through_peer == edge,
                    f"the origin's peer on the ingress path is the edge "
                    f"(origin reported {through_reported!r} -> {through_peer}, edge {edge})")
        check.check(through_peer != client_address,
                    f"the client's own address never reached the origin "
                    f"(origin reported {through_reported!r} -> {through_peer}, client {client_address})")

        # The negatives, without which the positive proves only that bytes
        # moved. Both are the SAME request over the SAME path; only the trust
        # store changes, so a refusal can be nothing but a verification result.
        public_bundle = guest_fetch(ctx, check, "edge-fetch-public-bundle", inside, "machine-1", url)
        check.check(public_bundle.exit == FETCH_CERTIFICATE_REJECTED,
                    f"the same request is REFUSED against the image's pinned public CA bundle "
                    f"(exit {public_bundle.exit}, expected {FETCH_CERTIFICATE_REJECTED}; "
                    f"{public_bundle.evidence()})")
        check.check(not public_bundle.reported("TOKEN"),
                    f"the refused fetch returned no body from the origin "
                    f"(body {public_bundle.body!r})")
        cross = guest_fetch(ctx, check, "edge-fetch-foreign-anchor", inside, "machine-1", url,
                            ca_file=other_anchor)
        check.check(cross.exit == FETCH_CERTIFICATE_REJECTED,
                    f"the same request is REFUSED against the OTHER Environment's authority "
                    f"(exit {cross.exit}, expected {FETCH_CERTIFICATE_REJECTED}; {cross.evidence()})")
        check.check(not cross.reported("TOKEN"),
                    f"the cross-Environment fetch returned no body from the origin "
                    f"(body {cross.body!r})")
    finally:
        released = ctx.release(check, server)
        # `None` is the one uncertain outcome: the invocation outlived SIGKILL,
        # so something this lane started may still be running.
        check.check(released.exit_code is not None,
                    f"the held origin was released (exit {released.exit_code})")

    # Nothing on the host LAN or the public Internet. A negative claim about the
    # host has to be read off the host: this is the real listener table while
    # both Environments and both edges are running, compared against the table
    # from before either existed.
    after = vz04_host.capture(scope, "public_like_ingress_after")
    check.check(after["capture_state"] == "captured",
                f"the host listener table was read while both edges ran ({after['capture_state']})")
    known = {vz04_host.listener_key(row) for row in before["listeners"]}
    appeared = [row for row in after["listeners"] if vz04_host.listener_key(row) not in known]
    # New, and attributable. A listener that predates the Environment is not
    # this Environment's doing, and an unrelated application opening a LAN
    # listener mid-run is not either; failing on those would make the claim
    # about the whole machine rather than about vz.
    #
    # An unprivileged `lsof` cannot name every process, so some rows arrive with
    # no pid. Counting those as this run's -- which this did -- makes the
    # criterion fail for listeners vz demonstrably did not create, and makes it
    # do so non-deterministically: on this machine it caught a `*:53564`
    # ephemeral row and, in two hardware runs, `*:53` and OrbStack's IPv6 rows,
    # none of them present when nothing was running and none attributable to
    # any process. A check that fails on the machine's own background noise
    # stops being read.
    #
    # Dropping the unattributable rows costs this clause nothing it was
    # actually providing. The edge binds nothing on the host at all -- the whole
    # path is a datagram socket pair inside one Environment's fabric -- so vz
    # declares no host port here for an unattributable row to have taken. The
    # claim that matters, that the edge address is bound nowhere on the host, is
    # asserted separately and directly above.
    scoped = {row["pid"] for row in after["processes"]}
    exposed, unattributed = [], []
    for row in appeared:
        if row.get("scope") == "loopback":
            continue
        (exposed if row.get("pid") in scoped else unattributed).append(row)
    check.check(not exposed,
                f"no listener on the host LAN or a wildcard address appeared while both edges ran "
                f"({len(appeared)} new listeners, {len(scoped)} processes attributable to this run, "
                f"exposed {exposed[:5]})")
    if unattributed:
        check.ok(f"{len(unattributed)} new non-loopback listener(s) this run could not attribute to any "
                 f"process and which bind no port it declared, recorded rather than charged to vz: "
                 f"{[(row.get('address'), row.get('port')) for row in unattributed][:5]}")
    check.ok(f"every listener that appeared during the run, with attribution: "
             f"{sorted((row.get('scope'), row.get('command'), row.get('pid') in scoped) for row in appeared)[:10]}")
    # And the edge itself is not on the host at all. It is a station on one
    # Environment's fabric; an address of it appearing in the host's own
    # listener table would mean it had been given a second, unowned identity.
    on_host = [row for row in after["listeners"] if row.get("address", "").strip("[]") == edge]
    check.check(not on_host, f"the edge address {edge} is bound nowhere on the host ({on_host})")

    if check.status == "PASS":
        for name, instance in (("edge-a", inside), ("edge-b", outside)):
            removed = ctx.run(check, name + "-delete",
                              ["--json", "delete", "--environment", "default", "--timeout", "120"],
                              cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
            check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")

    # Everything above is real, and it is not the whole criterion. Criterion 6
    # is one long sentence, and this check now exercises most of it against
    # installed binaries: isolated routing and DNS views, environment-local
    # public-like ingress, the firewall/NAT translation, TLS, and the negative
    # host claim that no shared NAT address or wildcard listener stands in for
    # an authorization boundary.
    #
    # Three of its clauses are still untouched here, and they are named rather
    # than left to look covered:
    #
    #   * controlled egress. `EgressPolicy` admits `Offline` alone in this
    #     runtime -- reaching a host off the fabric needs a translation towards
    #     the host's own network and a policy deciding which hosts, and neither
    #     exists. The edge translates only between an Environment's own client
    #     and its own declared origin, which is what was proved above.
    #   * explicit host imports and exports. Criterion 6 requires host imports
    #     to reach exact stored loopback services through authenticated
    #     Environment/Machine-owned relays; no such relay is implemented, so
    #     nothing here imports or exports anything.
    #   * the deterministic latency/loss/bandwidth/partition/DNS fault controls.
    #     No `Fault` is declared, applied or measured by this lane.
    #
    # Reporting PASS would certify the criterion on evidence that never touched
    # those three, so the sub-check stays not_implemented and says which.
    if check.status == "PASS":
        check.not_implemented = (
            "criterion 6's controlled-egress, host-import/export and fault-control clauses were "
            "not exercised. `EgressPolicy` admits only `Offline` in this runtime, and this check "
            "declares no host import or export of its own, so nothing reached a host off the "
            "fabric through the edge; and no latency/loss/bandwidth/partition/DNS fault was "
            "declared, "
            "applied or measured. Everything else in the criterion did run from inside real "
            "Machines and passed: the split-DNS and `.test`-hostname views, the edge-versus-origin "
            "distinction, cross-Environment name isolation, a verified TLS session from a Machine "
            "to the declared API through the edge, routed ingress to the declared origin on its "
            "declared port, the source translation (the origin's own `REMOTE_ADDR` is the edge, "
            "and is the caller when that same origin is spoken to directly), the refusal of the "
            "same request against the image's pinned public CA bundle and against another "
            "Environment's authority, and the absence of any attributable host LAN or wildcard "
            "listener while both edges ran.")
    return check.finish()
# ── Criterion 7: host import and export boundaries ─────────────────────────────
#
# The two directions are deliberately not symmetric, and the check is built
# around that asymmetry:
#
#   EXPORT  host 127.0.0.1:<host_port>  ──▶  Machine's own loopback service
#   IMPORT  Machine 127.0.0.1:<guest_port>  ──▶  host 127.0.0.1:<host_port>
#
# An export is host-initiated and its listener is loopback-only by construction
# (`HostExportSpec` has no host-address field at all). An import is
# guest-initiated, and the contract's rule for it is stricter: "Host imports
# require exact authenticated Environment/Machine grants to a declared
# host-loopback service, independently of external egress. NAT aliases and
# wildcard/LAN listeners are not authorization."
#
# So every denial below is measured against a WORKING import in the same run. A
# denial that passed because imports were unimplemented would prove nothing, and
# that is exactly the failure mode this criterion has to avoid.

# The guest loopback port the granted import binds, and one deliberately never
# declared. Guest-local, so they cannot collide with anything on the host.
GRANTED_GUEST_PORT = 15432
UNDECLARED_GUEST_PORT = 15433
# The Machine port the export forwards to, served by a held BusyBox httpd.
EXPORT_MACHINE_PORT = 8080
# Wall-clock budget for one recorded guest probe. Generous on purpose: the
# network deadline that actually decides these claims is WGET_TIMEOUT inside the
# Machine, and this only has to cover process launch on a loaded host. Too tight
# and a probe is killed mid-flight, which the recorder reports as uncertain
# effects -- a lane failure that says nothing about the boundary under test.
HOST_BOUNDARY_TIMEOUT = 60
# The address a Machine sees the host as over Apple's shared NAT segment. The
# contract names this case explicitly: a NAT alias is not authorization.
NAT_GATEWAY_ADDRESS = "192.168.64.1"


def free_host_port() -> int:
    """A loopback port free at this instant.

    Bound and released rather than guessed. Racy in principle -- the port can be
    taken between the probe and the declaration -- but a hard-coded port
    collides with whatever else this Mac is running, which is worse and silent.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class LoopbackHostService:
    """A host HTTP service bound to 127.0.0.1 and to nothing else.

    This is the thing an import is supposed to reach and every denial is
    supposed not to reach. It binds the loopback address explicitly rather than
    a wildcard, so "the Machine reached the host service" cannot be satisfied by
    a service that was reachable from the LAN anyway.
    """

    def __init__(self, token: str):
        import http.server
        import threading

        body = token.encode()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server's required spelling
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                return

        self.token = token
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.address, self.port = self.server.server_address[0], self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def attempt_up(ctx: CheckContext, check: SubCheck, name: str, definition: dict, *, timeout: int = UP_TIMEOUT) -> dict:
    """Bring one Environment up and REPORT the outcome instead of asserting it.

    `provision` asserts `vz up` exit 0, which is right for every check whose
    subject is a running Environment. Two clauses of this criterion have a
    refusal as their subject -- a colliding export host port, and a Machine
    whose egress policy this runtime does not implement -- so a helper that
    fails the check on a nonzero exit could not express them. Everything before
    the Up is asserted exactly as `provision` asserts it, because a fixture that
    could not have started is not a refusal.
    """
    data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
    iso = ctx.isolated(name, project_files={"vz.json": data}, provision=True)
    env, project = iso["env"], iso["project"]
    for label, argv in ((name + "-git-init", [GIT, "init", "--quiet", "--initial-branch", "main"]),
                        (name + "-git-add", [GIT, "add", "vz.json"]),
                        (name + "-git-commit", [GIT, "-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                                                "commit", "--quiet", "-m", "definition"])):
        receipt = ctx.run_tool(check, label, argv, cwd=project, env=env)
        check.check(receipt.exit_code == 0, f"{label}: exit {receipt.exit_code} (expected 0)")
    up = ctx.run(check, name + "-up", ["--json", "up"], cwd=project, env=env, timeout=timeout)
    message = ""
    try:
        message = json.loads(up.stderr.decode("utf-8")).get("error", {}).get("message", "")
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        message = up.stderr.decode("utf-8", "replace")[:400]
    return {"env": env, "project": project, "exit_code": up.exit_code, "message": message,
            "status": read_status(ctx, check, name, project=project, env=env) if up.exit_code == 0 else None}


def host_boundary_definition(release_dir: Path, *, host_port: int, export_host_port: int) -> dict:
    """Two Developer Linux Machines; only the first is granted anything.

    machine-1 exists solely so "the wrong Machine is denied" is a claim about a
    real sibling in the same Environment rather than about a Machine that does
    not exist. Neither Machine declares a network: an import terminates on the
    host and an export starts there, so the Environment fabric is not on either
    path and adding one would only blur which boundary refused.
    """
    definition = minimal_definition(release_dir)
    environment = definition["environment"]
    first = environment["machines"][0]
    second = copy.deepcopy(first)
    second["name"] = "machine-1"
    environment["machines"] = [first, second]
    environment["host_imports"] = [{
        "schema_version": 1, "name": "hostsvc", "machine": first["name"], "protocol": "tcp",
        "host_port": host_port, "guest_port": GRANTED_GUEST_PORT}]
    environment["host_exports"] = [{
        "schema_version": 1, "name": "api", "machine": first["name"], "protocol": "tcp",
        "machine_port": EXPORT_MACHINE_PORT, "host_port": export_host_port}]
    return definition


def guest_fetch_script(address: str, port: int) -> str:
    """Fetch one URL from inside a Machine and always report the exit status.

    `printf ':%s' $?` makes an empty body and a failed fetch distinguishable:
    without it, "denied" and "answered with nothing" print identically, and a
    denial that cannot be told from an empty success is not evidence.
    """
    return (f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{port}/; "
            "printf ':%s' $?")


def reached(receipt, token: str) -> bool:
    """Whether a guest fetch actually returned the host service's token."""
    return receipt.exit_code == 0 and token.encode() in receipt.stdout


def denied(receipt, token: str) -> bool:
    """Whether a guest fetch was refused rather than served.

    Both halves matter. The token must be absent, and the fetch must have
    reported a nonzero status: a wget that printed nothing and exited 0 would
    satisfy an absence test while having been served an empty body.
    """
    return token.encode() not in receipt.stdout and not receipt.stdout.strip().endswith(b":0")


LISTENER_ROW = re.compile(r"(?P<address>\[?[0-9a-fA-F.:*]+\]?):(?P<port>\d+)$")


def host_tcp_listeners(ctx: CheckContext, check: SubCheck, label: str):
    """Every TCP listener on this host, as (address, port) pairs.

    Read from `lsof`, not asserted from the code that binds. The criterion asks
    for listener evidence, and evidence means inspecting the machine: a claim
    that "the export binds 127.0.0.1 by construction" is exactly the kind of
    by-construction reasoning this clause exists to refuse.

    Returns `None` when the tool could not be run at all, so the caller can say
    the clause was not exercised instead of reading an empty list as proof that
    nothing listens.
    """
    receipt = ctx.run_tool(check, label, ["/usr/sbin/lsof", "-nP", "-iTCP", "-sTCP:LISTEN"],
                           cwd=ctx.state.root, env=ctx.state.env(), timeout=60)
    text = receipt.stdout.decode("utf-8", "replace")
    if receipt.exit_code not in (0, 1) or not text.strip():
        return None
    listeners = []
    for line in text.splitlines()[1:]:
        columns = line.split()
        if not columns:
            continue
        name = columns[-1]
        if name == "(LISTEN)" and len(columns) >= 2:
            name = columns[-2]
        match = LISTENER_ROW.search(name)
        if match:
            listeners.append((match.group("address").strip("[]"), int(match.group("port"))))
    return listeners


def is_loopback_listener(address: str) -> bool:
    """Whether a listener address is loopback and not a wildcard or a LAN address.

    `*` and `0.0.0.0`/`::` are wildcards, which listen on every interface
    including the LAN, and the contract states plainly that a wildcard listener
    is not authorization.
    """
    return address in ("127.0.0.1", "::1")


def host_non_loopback_addresses(ctx: CheckContext, check: SubCheck):
    """This host's own non-loopback IPv4 addresses, as the guest could reach them.

    Includes the vmnet gateway a Machine sees the host as. These are the
    addresses the LAN clause denies, and they are read from the host rather than
    assumed, so the clause fails honestly on a host that has none.
    """
    receipt = ctx.run_tool(check, "host-interfaces", ["/sbin/ifconfig", "-a"],
                           cwd=ctx.state.root, env=ctx.state.env(), timeout=30)
    addresses = []
    for line in receipt.stdout.decode("utf-8", "replace").splitlines():
        columns = line.split()
        if len(columns) >= 2 and columns[0] == "inet":
            address = columns[1]
            if not address.startswith("127.") and address != "0.0.0.0":
                addresses.append(address)
    return addresses


def check_host_import_export_boundaries(ctx: CheckContext, top: str) -> SubCheck:
    """Criterion 7, every clause, with the denials measured against a live import.

    The order is the criterion's own, and it is an order rather than a set
    because each claim is only meaningful once the one before it holds:

      1. absent by default -- an Environment that declares nothing reaches
         nothing, proved before anything is granted, so "denied" later cannot be
         confused with "was never possible";
      2. the authorized Machine reaches a 127.0.0.1-only host service through
         its one declared import -- the positive every denial is measured
         against;
      3. the denials: undeclared port, wrong protocol, wrong Machine, sibling
         Environment, arbitrary host destination, LAN;
      4. offline egress does not break the import, and enabled egress does not
         create one;
      5. loopback exports serve, and a colliding host port is refused;
      6. listener evidence, read from `lsof`, proving no wildcard or LAN
         listener exists for any port this check put in play.
    """
    from vz04_common import GateError

    check = SubCheck(top, "host_import_export_boundaries")
    granted_service = foil_service = None
    try:
        granted_service = LoopbackHostService("vzhostsvc-" + uuid.uuid4().hex[:16])
        # Never declared to anybody. It exists so "the guest cannot pick a host
        # destination" is tested against a host service that really is there and
        # really is listening -- an unreachable port would deny by absence.
        foil_service = LoopbackHostService("vzfoil-" + uuid.uuid4().hex[:16])
    except OSError as error:
        # Whichever half came up is closed again: a listener this check started
        # and then abandoned is exactly the leak it exists to detect.
        if granted_service is not None:
            granted_service.close()
        check.fail(f"cannot bind a loopback host service for the import to terminate against: {error}")
        return check.finish()
    export_host_port = free_host_port()
    export_token = "vzexport-" + uuid.uuid4().hex[:16]
    granted = sibling = egress_probe = collision = None
    try:
        check.check(granted_service.address == "127.0.0.1" and foil_service.address == "127.0.0.1",
                    f"both host services bound loopback only (granted {granted_service.address}, "
                    f"foil {foil_service.address})")
        try:
            definition = host_boundary_definition(ctx.release_dir, host_port=granted_service.port,
                                                  export_host_port=export_host_port)
        except (StopIteration, KeyError, OSError) as error:
            check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
            return check.finish()
        schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
        if schema_path.is_file():
            problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(definition),
                              key=lambda e: list(map(str, e.absolute_path)))
            check.check(not problems, "the host import/export definition validates"
                        if not problems else f"definition invalid: {problems[0].message[:200]}")
            if problems:
                return check.finish()

        # 1. Absent by default. A bare Environment declares no import and no
        #    export, and must reach neither the granted host service nor the
        #    foil, on any port. Proved FIRST: a denial observed only after a
        #    grant exists cannot distinguish "refused" from "never possible".
        sibling = provision(ctx, check, "hb-bare", minimal_definition(ctx.release_dir))
        if sibling.get("unsupported"):
            check.not_implemented = ("this runtime refuses the bare Environment this check starts from: " +
                                     sibling["unsupported"][:300])
            return check.finish()
        # An unreadable status is a FAILURE, never an early return: a check that
        # stopped here quietly would report PASS having exercised no clause of
        # the criterion at all.
        check.check(sibling["status"] is not None,
                    "the bare Environment reports a readable status before any denial is claimed against it")
        if check.status != "PASS":
            return check.finish()
        for label, port, token in (("granted-service", granted_service.port, granted_service.token),
                                   ("foil-service", foil_service.port, foil_service.token),
                                   ("granted-guest-port", GRANTED_GUEST_PORT, granted_service.token)):
            receipt = machine_exec(ctx, check, "hb-absent-" + label, sibling, "machine-0",
                                   guest_fetch_script("127.0.0.1", port), timeout=HOST_BOUNDARY_TIMEOUT)
            check.check(denied(receipt, token),
                        f"an Environment that declares no import cannot reach {label} on 127.0.0.1:{port} "
                        f"(observed {receipt.stdout[:80]!r})")
        before = host_tcp_listeners(ctx, check, "hb-listeners-before")
        if before is None:
            check.fail("cannot enumerate host TCP listeners with lsof; the listener-evidence clause "
                       "cannot be read as proof of absence")
            return check.finish()
        check.check(not any(port == export_host_port for _address, port in before),
                    f"no host listener holds the export port {export_host_port} before anything is declared")
        if check.status != "PASS":
            return check.finish()

        # 2. The granted Environment. One import on machine-0 to the granted
        #    host service, one export on machine-0, and a sibling Machine that
        #    declares neither.
        granted = provision(ctx, check, "hb-granted", definition)
        if granted.get("unsupported"):
            check.not_implemented = ("declared host imports/exports are not applied by this runtime: " +
                                     granted["unsupported"][:300])
            return check.finish()
        check.check(granted["status"] is not None,
                    "the granted Environment reports a readable status; without it no clause below was exercised")
        if check.status != "PASS":
            return check.finish()
        names = sorted(m.get("name") for e in granted["status"]["environments"] for m in e.get("machines") or [])
        check.check(names == ["machine-0", "machine-1"], f"both declared Machines are present (observed {names})")
        if check.status != "PASS":
            return check.finish()
        served = machine_exec(ctx, check, "hb-granted-import", granted, "machine-0",
                              guest_fetch_script("127.0.0.1", GRANTED_GUEST_PORT), timeout=HOST_BOUNDARY_TIMEOUT)
        check.check(reached(served, granted_service.token),
                    "the authorized Machine reaches the 127.0.0.1-only host service through its one declared "
                    f"import on guest loopback {GRANTED_GUEST_PORT} (exit {served.exit_code}, "
                    f"{served.stdout[:80]!r})")
        if check.status != "PASS":
            return check.finish()

        # 3. The denials, every one of them against that live import.
        undeclared = machine_exec(ctx, check, "hb-deny-undeclared-port", granted, "machine-0",
                                  guest_fetch_script("127.0.0.1", UNDECLARED_GUEST_PORT),
                                  timeout=HOST_BOUNDARY_TIMEOUT)
        check.check(denied(undeclared, granted_service.token),
                    f"the undeclared guest port {UNDECLARED_GUEST_PORT} is denied on the same Machine whose "
                    f"declared port {GRANTED_GUEST_PORT} just worked (observed {undeclared.stdout[:80]!r})")

        # An import is a stream grant. The same port addressed as UDP is not the
        # declared protocol and nothing answers it.
        udp = machine_exec(ctx, check, "hb-deny-wrong-protocol", granted, "machine-0",
                           f"printf probe | /bin/busybox nc -u -w 2 127.0.0.1 {GRANTED_GUEST_PORT}; "
                           "printf ':%s' $?", timeout=HOST_BOUNDARY_TIMEOUT)
        if b"applet not found" in udp.stderr or udp.exit_code == 127:
            check.not_implemented = (
                "the wrong-protocol denial was not exercised: this Machine's BusyBox has no `nc` applet, so a "
                f"UDP datagram could not be sent to the declared guest port {GRANTED_GUEST_PORT}. Every other "
                "clause of criterion 7 above and below did run.")
        else:
            check.check(denied(udp, granted_service.token),
                        "a UDP datagram to the declared guest port is not served; the grant is for the declared "
                        f"stream protocol only (observed {udp.stdout[:80]!r})")

        sibling_machine = machine_exec(ctx, check, "hb-deny-wrong-machine", granted, "machine-1",
                                       guest_fetch_script("127.0.0.1", GRANTED_GUEST_PORT),
                                       timeout=HOST_BOUNDARY_TIMEOUT)
        check.check(denied(sibling_machine, granted_service.token),
                    "the sibling Machine in the SAME Environment, which declares no import, cannot reach the "
                    f"host service on the granted port (observed {sibling_machine.stdout[:80]!r})")

        foreign = machine_exec(ctx, check, "hb-deny-sibling-environment", sibling, "machine-0",
                               guest_fetch_script("127.0.0.1", GRANTED_GUEST_PORT),
                               timeout=HOST_BOUNDARY_TIMEOUT)
        check.check(denied(foreign, granted_service.token),
                    "a Machine in a sibling Environment cannot reach the granted port, which is now live in the "
                    f"granted Environment (observed {foreign.stdout[:80]!r})")

        # Arbitrary host destination. Two shapes, both from the Machine that
        # DOES hold a grant: the host port of its own declared service, and an
        # undeclared host service. Neither is nameable on the wire, so neither
        # may answer.
        for label, port, token in (("own-host-port", granted_service.port, granted_service.token),
                                   ("undeclared-host-service", foil_service.port, foil_service.token)):
            receipt = machine_exec(ctx, check, "hb-deny-arbitrary-" + label, granted, "machine-0",
                                   guest_fetch_script("127.0.0.1", port), timeout=HOST_BOUNDARY_TIMEOUT)
            check.check(denied(receipt, token),
                        f"the granted Machine cannot choose a host destination ({label} on 127.0.0.1:{port}); "
                        f"only its declared guest port relays (observed {receipt.stdout[:80]!r})")

        # LAN. Every non-loopback address this host actually holds, plus the
        # vmnet gateway a Machine sees the host as -- the NAT alias the contract
        # names as explicitly not authorization.
        lan_addresses = [a for a in host_non_loopback_addresses(ctx, check) if a != NAT_GATEWAY_ADDRESS]
        probed = []
        for address in [NAT_GATEWAY_ADDRESS, *lan_addresses[:3]]:
            receipt = machine_exec(ctx, check, "hb-deny-lan-" + address.replace(".", "-"), granted, "machine-0",
                                   guest_fetch_script(address, granted_service.port),
                                   timeout=HOST_BOUNDARY_TIMEOUT)
            probed.append(address)
            check.check(denied(receipt, granted_service.token),
                        f"the granted Machine cannot reach the host service at {address}:{granted_service.port}; "
                        f"a NAT alias or LAN address is not the grant (observed {receipt.stdout[:80]!r})")
        check.ok(f"non-loopback host addresses probed: {probed}")

        # 4. Egress. Every Machine here is offline -- no `egress` key means the
        #    default, and this Up applies `offline` only -- and the import above
        #    worked, which is the "offline egress does not break the declared
        #    import" clause proved rather than asserted.
        offline = all("egress" not in machine or machine["egress"] == "offline"
                      for machine in definition["environment"]["machines"])
        check.check(offline, "every Machine of the granted Environment declares offline egress, and its declared "
                    "import served anyway")
        enabled = copy.deepcopy(minimal_definition(ctx.release_dir))
        enabled["environment"]["machines"][0]["egress"] = "allowed"
        egress_probe = attempt_up(ctx, check, "hb-egress", enabled, timeout=DELETE_TIMEOUT)
        if egress_probe["exit_code"] == 0 and egress_probe["status"] is not None:
            # Enabled egress is available: prove it creates no import.
            for label, port, token in (("granted-service", granted_service.port, granted_service.token),
                                       ("granted-guest-port", GRANTED_GUEST_PORT, granted_service.token)):
                receipt = machine_exec(ctx, check, "hb-egress-no-import-" + label, egress_probe, "machine-0",
                                       guest_fetch_script("127.0.0.1", port), timeout=HOST_BOUNDARY_TIMEOUT)
                check.check(denied(receipt, token),
                            f"a Machine with enabled egress and no declared import cannot reach {label} "
                            f"(observed {receipt.stdout[:80]!r})")
        else:
            check.not_implemented = (
                "the \"enabled egress does not create one\" clause of criterion 7 was not exercised: this runtime "
                "refuses a Machine with non-offline egress, so no Machine with enabled egress could be built to "
                "prove it gains no host import. Every other clause -- absent by default, the granted import, the "
                "undeclared port, wrong protocol, wrong Machine, sibling Environment, arbitrary host destination "
                "and LAN denials, offline egress, loopback exports without collisions, and the listener evidence "
                "-- did run above. Runtime refusal: "
                f"{(egress_probe['message'] or 'vz up failed without naming a reason')[:250]}")

        # 5. Exports. The host reaches the Machine's own loopback service through
        #    the declared loopback export, and a second Environment declaring the
        #    same host port is refused rather than silently sharing it.
        server = hold_machine_exec(ctx, check, "hb-export-serve", granted, "machine-0",
                                   f"/bin/busybox mkdir -p /www; printf %s {export_token} > /www/index.html; "
                                   f"/bin/busybox httpd -f -p {EXPORT_MACHINE_PORT} -h /www")
        try:
            body = None
            for attempt in range(1, LISTENER_ATTEMPTS + 1):
                fetch = ctx.run_tool(check, f"hb-export-fetch-{attempt}",
                                     ["/usr/bin/curl", "--silent", "--show-error", "--max-time", str(WGET_TIMEOUT),
                                      f"http://127.0.0.1:{export_host_port}/"],
                                     cwd=ctx.state.root, env=ctx.state.env(), timeout=HOST_BOUNDARY_TIMEOUT)
                body = fetch.stdout
                if fetch.exit_code == 0 and export_token.encode() in body:
                    break
                time.sleep(LISTENER_INTERVAL)
            check.check(body is not None and export_token.encode() in body,
                        f"the declared loopback export serves the Machine's own service on 127.0.0.1:"
                        f"{export_host_port} after {attempt} attempt(s) (observed {(body or b'')[:80]!r})")
            during = host_tcp_listeners(ctx, check, "hb-listeners-during")
            if during is None:
                check.fail("cannot enumerate host TCP listeners with lsof while the export is live")
            else:
                holders = [address for address, port in during if port == export_host_port]
                check.check(holders and all(is_loopback_listener(address) for address in holders),
                            f"the live export listener on {export_host_port} is loopback and nothing else "
                            f"(observed {holders})")
                # Every port this check put in play, not only the export: an
                # import's guest port must have no host listener at all, and no
                # port of ours may be held on a wildcard or LAN address.
                ours = {export_host_port, granted_service.port, foil_service.port,
                        GRANTED_GUEST_PORT, UNDECLARED_GUEST_PORT}
                wide = sorted({(address, port) for address, port in during
                               if port in ours and not is_loopback_listener(address)})
                check.check(not wide,
                            "no wildcard or LAN host listener holds any port this check declared "
                            f"(observed {wide})")
                guest_side = sorted({(address, port) for address, port in during
                                     if port in (GRANTED_GUEST_PORT, UNDECLARED_GUEST_PORT)})
                check.check(not guest_side,
                            "an import's guest loopback port has no host listener at all; the host half of an "
                            f"import is a vsock terminator, not a TCP port (observed {guest_side})")
                check.ok(f"host TCP listeners inspected while the export was live: {len(during)}")
        finally:
            released = ctx.release(check, server)
            check.check(released.exit_code is not None,
                        f"the held export listener was released (exit {released.exit_code})")

        collider = copy.deepcopy(minimal_definition(ctx.release_dir))
        collider["environment"]["host_exports"] = [{
            "schema_version": 1, "name": "api", "machine": "machine-0", "protocol": "tcp",
            "machine_port": EXPORT_MACHINE_PORT, "host_port": export_host_port}]
        collision = attempt_up(ctx, check, "hb-collide", collider, timeout=DELETE_TIMEOUT)
        check.check(collision["exit_code"] != 0 and "already held on this host" in collision["message"],
                    "a second Environment declaring the export host port already held is refused rather than "
                    f"silently sharing the loopback port (exit {collision['exit_code']}, "
                    f"{collision['message'][:160]!r})")

        after = host_tcp_listeners(ctx, check, "hb-listeners-after")
        if after is None:
            check.fail("cannot enumerate host TCP listeners with lsof after the export was released")
        else:
            check.ok(f"host TCP listeners after release: "
                     f"{sorted({(a, p) for a, p in after if p == export_host_port})}")
    except (OSError, GateError) as error:
        # A path or process this check needed went away underneath it. That is a
        # failure of this check, recorded with what it was doing -- not a lane
        # crash, which would abort every sub-check after this one and report
        # nothing about any of them.
        check.fail(f"the host-boundary check could not complete: {type(error).__name__}: {error}")
    finally:
        granted_service.close()
        foil_service.close()
        # Unconditionally, including on failure. An Environment left running by
        # this check is not left for inspection, it is left holding a host
        # loopback listener and a daemon inside a state root the next check is
        # about to allocate under -- which shows up as that check failing for
        # reasons of its own. What this check needs kept is its receipts and its
        # assertions, and both are already recorded by the time we get here.
        for name, instance in (("hb-granted", granted), ("hb-bare", sibling),
                               ("hb-egress", egress_probe), ("hb-collide", collision)):
            if instance is None or instance.get("status") is None:
                continue
            try:
                removed = ctx.run(check, name + "-delete",
                                  ["--json", "delete", "--environment", "default", "--timeout", "120"],
                                  cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
                check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")
            except OSError as error:
                check.fail(f"{name}: could not be deleted: {type(error).__name__}: {error}")
    return check.finish()


# --------------------------------------------------------------------- criterion 22
#
# `gate.definition.reconciliation_fencing`: the definition-reconciliation and
# generation-fencing clauses of criterion 22, each proved against one of the
# three Environments `establish_recovery_environments` left running.
#
# The criterion cites two normative sub-documents by name, and they -- not the
# summary paragraph -- are the contract:
#
#   planning/developer-environments/reconcile-generation-fencing.md
#     "Action schema v3": every ServiceCreate/ServiceRecreate/ServiceRemove
#     carries a `ReplicaPrecondition` naming the exact workload scope,
#     `environment_generation` and journal head it was planned against.
#     "Durable claim and exact StateStore CAS": the exact batch audit row is the
#     durable claim, and `start_reconcile_batch` revalidates every precondition
#     in one transaction before any effect.
#     "Strict mutation ordering": nothing but the inert pre-claim planning
#     manifest may touch state before the claims are acquired.
#
#   planning/developer-environments/reconcile-effective-inputs.md
#     "Snapshot model": one immutable operation-owned `ReconcileInputSnapshot`
#     with a `vz-reconcile-input-manifest-v1` manifest digest and per-service
#     `vz-effective-service-input-v1` effective digests.
#     "Capture and atomic admission": `admit_reconcile_round` finalizes that
#     snapshot and derives the plan in one transaction.
#     "Planning and execution": execution resolves its inputs through the
#     persisted manifest and never rereads the caller's current spec.
#     "Recovery, retention, and cleanup": any digest/inventory disagreement is a
#     state conflict before mutation.
#
# Both sub-documents are written about SERVICE REPLICAS inside a Machine. The
# 0.4 public CLI has five verbs and the 0.4 ProjectDefinition
# (schemas/vz-project-definition-v1.schema.json) declares no services, secrets or
# volumes, so a black-box gate has no scoped service create/recreate/remove to
# fence and no effective-service input to snapshot. What it does have is the
# Environment-level reconciliation the criterion's own paragraph is about. Every
# clause below is either proved at that layer against exact values, or reported
# `not_implemented` by name with the runtime's own refusal quoted.
#
# One hard constraint on all of it: these sub-checks run in
# `persisted-recovery/pre-sleep`, and the post-wake phase must find these exact
# Environments with the identity `establish_recovery_environments` recorded --
# including `lifecycle_generation`. Nothing here may consume a generation. Every
# Up attempted below therefore carries a CHANGED definition, which is precisely
# the input this criterion is about; each sub-check restores the exact
# definition bytes it found and re-asserts the recorded identity afterwards, on
# every exit path including failure.
RECONCILE_FENCING_DOC = "planning/developer-environments/reconcile-generation-fencing.md"
RECONCILE_INPUTS_DOC = "planning/developer-environments/reconcile-effective-inputs.md"
DEFINITION_FILE = "vz.json"
# `validate_definition_instance` (crates/vz-runtime-contract/src/types/topology.rs)
# is the function that decides which ProjectDefinition fields a live Environment
# is compared against: Machine name/count, `target`, `profile` and
# `requested_capabilities`, plus the declared networks and endpoints. `resources`
# is not among them, so `memory_mb` is a mutable field and `target.digest` is an
# immutable one. Both spellings below stay schema-valid, so what is under test is
# the reconciliation decision and not a rejected document.
MUTABLE_FIELD = "environment.machines[0].resources.memory_mb"
IMMUTABLE_FIELD = "environment.machines[0].target.digest"
MUTATED_MEMORY_MB = 6144
FOREIGN_ARTIFACT_DIGEST = "sha256:" + "9" * 64
DIGEST_SPELLING = re.compile(r"^sha256:[0-9a-f]{64}$")
ERROR_CODE_SPELLING = re.compile(r"^[a-z][a-z0-9_]*$")
RECONCILE_UP_TIMEOUT = UP_TIMEOUT
RECONCILE_STATUS_TIMEOUT = 60
# Removed from a plan value by name before two runs are compared: everything
# that identifies THIS invocation rather than the plan it announced. A lifecycle
# `generation` is excluded because a second accepted Up legitimately consumes the
# next one; it is asserted separately, and exactly, instead.
PLAN_VOLATILE = frozenset(("request_id", "idempotency_key", "request_hash", "trace_id", "created_at",
                           "started_at", "completed_at", "finished_at", "updated_at", "operation_id",
                           "generation", "elapsed_millis", "elapsed_nanos"))
# Every key by which a runtime that had implemented `reconcile-effective-inputs.md`
# would publish its operation-owned snapshot identity on a public interface.
# Their absence is what makes that contract's clauses unexercisable here, so the
# absence is asserted rather than assumed.
EFFECTIVE_INPUT_KEYS = ("manifest_id", "manifest_digest", "effective_digest", "snapshot_id",
                        "applied_config_digest", "plan_hash", "secret_blobs", "stack_projection",
                        "reconcile_actions", "replica_precondition")


def _strip_volatile(value):
    if isinstance(value, dict):
        return {key: _strip_volatile(item) for key, item in value.items() if key not in PLAN_VOLATILE}
    if isinstance(value, list):
        return [_strip_volatile(item) for item in value]
    return value


def reconcile_plan(receipt) -> dict:
    """One `vz --json up` reduced to the plan it announced, as a comparable value.

    `vz --json up` writes one JSON document per line on stdout (a
    `request_started` record, then one `operation_progress` per transition, each
    carrying the admission's project/Environment/Machine identities and the
    definition digest it was admitted under) and, when it refuses, one error
    envelope on stderr. `PLAN_VOLATILE` removes by name everything that
    identifies the invocation rather than the plan. What survives is what two
    runs of one definition change against one Environment must agree on exactly.
    """
    records, unparsed = [], []
    for line in receipt.stdout.decode("utf-8", "replace").splitlines():
        if not line.strip():
            continue
        try:
            records.append(_strip_volatile(json.loads(line)))
        except json.JSONDecodeError:
            unparsed.append(line[:160])
    envelope = None
    if receipt.stderr:
        try:
            envelope = _strip_volatile(json.loads(receipt.stderr.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError):
            envelope = None
    return {"exit_code": receipt.exit_code, "records": records, "error_envelope": envelope,
            "unparsed_stdout_lines": unparsed,
            "unparsed_stderr": None if envelope is not None else receipt.stderr.decode("utf-8", "replace")[:400]}


def reconcile_error(receipt):
    """The structured refusal `vz --json` writes to stderr, or None."""
    if not receipt.stderr:
        return None
    try:
        payload = json.loads(receipt.stderr.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    error = payload.get("error") if isinstance(payload, dict) else None
    return error if isinstance(error, dict) else None


def reconcile_refusal_text(error) -> str:
    """Every string the runtime put in one refusal, joined for exact search."""
    if not error:
        return ""
    parts = [str(error.get("code") or ""), str(error.get("message") or "")]
    details = error.get("details")
    if isinstance(details, dict):
        parts.extend(f"{key}={value}" for key, value in sorted(details.items()))
    return " ".join(parts)


def reconcile_identity(payload) -> dict:
    """Every stable identity `vz status --json` publishes, as one comparable value.

    Criterion 22 claims a reconcile does not change stable identity, so the
    comparison has to be of identities and not of a success flag: Environment and
    Machine IDs, the Machine incarnation that would change if a Machine were
    silently replaced, the lifecycle generation a refused plan must not consume,
    and the declared-fabric IDs an orphan would show up as.
    """
    environments = []
    for environment in payload.get("environments") or []:
        environments.append({
            "environment_id": environment.get("environment_id"),
            "name": environment.get("name"),
            "state": environment.get("state"),
            "definition_digest": environment.get("definition_digest"),
            "lifecycle_generation": environment.get("lifecycle_generation"),
            "machines": [{"machine_id": machine.get("machine_id"), "name": machine.get("name"),
                          "state": machine.get("state"), "profile": machine.get("profile"),
                          "target": machine.get("target"),
                          "incarnation_id": machine.get("incarnation_id"),
                          "incarnation_generation": machine.get("incarnation_generation")}
                         for machine in environment.get("machines") or []],
            "networks": sorted(str(row.get("network_id")) for row in environment.get("networks") or []),
            "network_attachments": sorted(str(row.get("attachment_id"))
                                          for row in environment.get("network_attachments") or []),
            "endpoints": sorted(str(row.get("endpoint_id")) for row in environment.get("endpoints") or []),
        })
    return {"project_id": payload.get("project_id"),
            "persisted_definition_digest": payload.get("persisted_definition_digest"),
            "environments": environments}


def reconcile_owned_resources(payload) -> dict:
    """`<kind>:<resource id>` -> the Environment id that owns it, from one status.

    Deliberately scoped to the resources the status document itself attributes to
    an owner, and compared before against after. It is NOT compared against the
    definition's Machine list: a forked Machine is a runtime object the definition
    does not declare, reconcile must leave it alone, and only `vz delete` removes
    it. A plan that created nothing must leave this value byte-identical, which is
    what "no orphaned resources" means for a refused or converged reconcile.
    """
    owned = {}
    for environment in payload.get("environments") or []:
        owner = str(environment.get("environment_id"))
        owned[f"environment:{owner}"] = owner
        for machine in environment.get("machines") or []:
            owned[f"machine:{machine.get('machine_id')}"] = owner
            context = machine.get("docker_context") or {}
            if context.get("name"):
                owned[f"docker_context:{context.get('name')}"] = str((context.get("owner") or {}).get("environment_id"))
        for row in environment.get("networks") or []:
            owned[f"network:{row.get('network_id')}"] = owner
        for row in environment.get("network_attachments") or []:
            owned[f"network_attachment:{row.get('attachment_id')}"] = owner
        for row in environment.get("endpoints") or []:
            owned[f"endpoint:{row.get('endpoint_id')}"] = owner
    return owned


def definition_bytes(definition: dict) -> bytes:
    """The spelling `provision` writes, so a restored file is byte-identical."""
    return json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"


def replace_definition(project: Path, data: bytes) -> None:
    """Rewrite `vz.json` in place, atomically, leaving no temporary behind.

    In place because the CLI reads the NEAREST definition: a copy beside it would
    not be the file Up discovers, and a half-written one would be refused as an
    invalid document rather than as the changed field under test.
    """
    target = project / DEFINITION_FILE
    temporary = target.with_name(DEFINITION_FILE + ".reconcile-tmp")
    if os.path.lexists(temporary):
        os.unlink(temporary)
    write_exclusive(temporary, data)
    os.replace(temporary, target)
    descriptor = os.open(project, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def mutable_change(definition: dict) -> dict:
    """The same definition with one field no instance record is compared against."""
    changed = copy.deepcopy(definition)
    machine = changed["environment"]["machines"][0]
    machine.setdefault("resources", {})["memory_mb"] = MUTATED_MEMORY_MB
    return changed


def immutable_change(definition: dict) -> dict:
    """The same definition with one field the persisted instance IS compared against.

    `actual.target != desired.target` is a definition/topology mismatch, so a
    Machine's pinned artifact digest cannot be reconciled in place: honouring it
    would mean replacing the Machine, which is exactly what "immutable/unsafe
    changes fail before mutation" forbids doing silently.
    """
    changed = copy.deepcopy(definition)
    changed["environment"]["machines"][0]["target"]["digest"] = FOREIGN_ARTIFACT_DIGEST
    return changed


def reordered_bytes(definition: dict) -> bytes:
    """The same definition value, serialized to different bytes.

    Reversed key order and a different separator style. A canonical desired digest
    must be identical over these bytes; a digest over the file would not be, which
    is the difference this distinguishes.
    """
    def reorder(value):
        if isinstance(value, dict):
            return {key: reorder(value[key]) for key in reversed(list(value))}
        if isinstance(value, list):
            return [reorder(item) for item in value]
        return value
    return json.dumps(reorder(definition), separators=(", ", ": ")).encode() + b"\n"


def reconcile_status(ctx: CheckContext, check: SubCheck, label: str, instance: dict):
    row = ctx.run(check, label, ["--json", "status"], cwd=instance["project"], env=instance["env"],
                  timeout=RECONCILE_STATUS_TIMEOUT)
    if row.exit_code != 0:
        check.fail(f"{label}: vz --json status exit {row.exit_code} (expected 0); stderr {row.stderr[:200]!r}")
        return None
    try:
        return json.loads(row.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        check.fail(f"{label}: status is not a JSON document: {error}")
        return None


class ReconcileSubject:
    """One pre-sleep Environment, its recorded identity, and its definition bytes.

    `restore` puts the exact bytes back and is called on every exit path,
    including failure: the post-wake phase has to find these Environments as
    `establish_recovery_environments` recorded them.
    """

    def __init__(self, entry: dict, instance: dict, original: bytes, definition: dict, before: dict):
        self.entry = entry
        self.name = entry["isolate"]
        self.instance = instance
        self.project = instance["project"]
        self.env = instance["env"]
        self.original = original
        self.definition = definition
        self.before = before
        self.identity = reconcile_identity(before)
        self.owned = reconcile_owned_resources(before)
        self.desired = before.get("desired_definition_digest")
        self.persisted = before.get("persisted_definition_digest")

    def write(self, data: bytes) -> None:
        replace_definition(self.project, data)

    def restore(self) -> None:
        replace_definition(self.project, self.original)


def reconcile_subject(ctx: CheckContext, check: SubCheck, established, index: int):
    """Reattach pre-sleep's Environment `index` and record what must not change."""
    entries = (established or {}).get("environments") or []
    if not check.check(len(entries) > index,
                       f"pre-sleep recorded {len(entries)} Environment(s); this sub-check addresses index {index}"):
        return None
    entry = entries[index]
    try:
        instance = ctx.reattach(entry["isolate"])
    except ReattachError as error:
        check.fail(str(error))
        return None
    definition_path = instance["project"] / DEFINITION_FILE
    if not check.check(definition_path.is_file() and not definition_path.is_symlink(),
                       f"{entry['isolate']}: a regular {DEFINITION_FILE} to change at {definition_path}"):
        return None
    original = read_regular(definition_path)
    try:
        definition = json.loads(original.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        check.fail(f"{entry['isolate']}: {DEFINITION_FILE} is not a JSON document: {error}")
        return None
    before = reconcile_status(ctx, check, entry["isolate"] + "-recon-before", instance)
    if before is None:
        return None
    subject = ReconcileSubject(entry, instance, original, definition, before)
    check.check(subject.desired == subject.persisted and DIGEST_SPELLING.match(str(subject.desired or "")) is not None,
                f"{subject.name}: before any change the desired and persisted definition digests are one canonical "
                f"value (desired {subject.desired!r}, persisted {subject.persisted!r})")
    check.check(before.get("definition_drift") is False,
                f"{subject.name}: before any change definition_drift is False (observed "
                f"{before.get('definition_drift')!r})")
    check.check(bool(subject.identity["environments"]) and
                subject.identity["environments"][0]["environment_id"] == subject.entry["environment_id"],
                f"{subject.name}: the Environment pre-sleep recorded is the one addressed here "
                f"({subject.entry['environment_id']!r} observed "
                f"{(subject.identity['environments'] or [{}])[0].get('environment_id')!r})")
    return subject


def desired_digest_responds(ctx: CheckContext, check: SubCheck, subject: "ReconcileSubject", changed: dict):
    """Write `changed` and report the desired digest the CLI then publishes.

    Returns `(status, responded)`. A runtime whose desired digest does not move
    when a declared field moves has no desired-input identity for planning to
    consume, and every clause after it would be asserted against a value the
    definition cannot influence -- so the caller reports that by name instead of
    continuing.
    """
    subject.write(definition_bytes(changed))
    after = reconcile_status(ctx, check, subject.name + "-recon-changed", subject.instance)
    if after is None:
        return None, False
    desired = after.get("desired_definition_digest")
    return after, bool(desired) and desired != subject.desired


def reconcile_unimplemented(check: SubCheck, clause: str, quote: str) -> None:
    """Name the sub-document clause with no subject, in the runtime's own words."""
    check.not_implemented = (clause + " The runtime's own answer to the definition change this sub-check made "
                             "was: " + (quote or "<no structured refusal and no reconcile: the CLI said nothing "
                                                 "about the changed definition>")[:600])


def _reconcile_generation(identity) -> object:
    return ((identity or {}).get("environments") or [{}])[0].get("lifecycle_generation")


def _reconcile_without_generation(identity):
    """One identity value with the lifecycle counter removed, and nothing else."""
    stripped = copy.deepcopy(identity) if identity is not None else None
    for environment in (stripped or {}).get("environments") or []:
        environment.pop("lifecycle_generation", None)
    return stripped


def _reconcile_restored(ctx: CheckContext, check: SubCheck, subject: "ReconcileSubject") -> None:
    """Put the definition back and prove pre-sleep's record still describes reality.

    Post-wake compares Environment id, name, `lifecycle_generation`, every
    Machine id/incarnation and the persisted definition digest against the record
    `establish_recovery_environments` wrote before the checkpoint. If anything
    here moved them, that has to be said in this sub-check rather than left for
    criterion 10 to fail over a change that belongs to criterion 22.

    A runtime that ACCEPTS the definition change is still activated under it once
    the file is put back, so the file alone is not a restore: it is reconciled
    back, which legitimately consumes lifecycle generations. Only that counter is
    refreshed in pre-sleep's record, and only after the reconcile back has been
    asserted -- every identity post-wake reads, and the digest, must come back on
    their own.
    """
    subject.restore()
    restored = reconcile_status(ctx, check, subject.name + "-recon-restored", subject.instance)
    reconverged = False
    if restored is not None and restored.get("definition_drift") is True:
        reconverged = True
        again = ctx.run(check, subject.name + "-recon-reconverge", ["--json", "up"], cwd=subject.project,
                        env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        check.check(again.exit_code == 0,
                    f"{subject.name}: this runtime accepted the change, so the Environment is reconciled back to "
                    f"the definition pre-sleep recorded (exit {again.exit_code}, expected 0)")
        restored = reconcile_status(ctx, check, subject.name + "-recon-reconverged", subject.instance)
    identity = reconcile_identity(restored) if restored is not None else None
    comparable = (_reconcile_without_generation(identity), _reconcile_without_generation(subject.identity))
    check.check(comparable[0] == comparable[1],
                f"{subject.name}: pre-sleep's recorded identity is byte-identical after this sub-check "
                f"(persisted digest {subject.persisted!r} observed "
                f"{(restored or {}).get('persisted_definition_digest')!r})"
                if comparable[0] == comparable[1] else
                f"{subject.name}: this sub-check changed pre-sleep's recorded identity: "
                f"{json.dumps(comparable[1])[:260]} vs {json.dumps(comparable[0])[:260]}")
    before_generation, after_generation = _reconcile_generation(subject.identity), _reconcile_generation(identity)
    if reconverged:
        # Named in the assertions and written back into pre-sleep's record, so
        # post-wake compares against what pre-sleep actually left running rather
        # than against a counter this sub-check advanced.
        check.check(isinstance(after_generation, int) and isinstance(before_generation, int)
                    and after_generation > before_generation,
                    f"{subject.name}: reconciling back advanced the lifecycle generation "
                    f"({before_generation!r} -> {after_generation!r}); pre-sleep's record is updated to the "
                    f"generation it is leaving running")
        subject.entry["lifecycle_generation"] = after_generation
    else:
        check.check(after_generation == before_generation,
                    f"{subject.name}: the lifecycle generation pre-sleep recorded is unchanged "
                    f"({before_generation!r} observed {after_generation!r})")
    check.check(restored is not None and restored.get("desired_definition_digest") == subject.desired
                and restored.get("definition_drift") is False,
                f"{subject.name}: the definition file is restored byte-identical (desired {subject.desired!r} "
                f"observed {(restored or {}).get('desired_definition_digest')!r}, drift "
                f"{(restored or {}).get('definition_drift')!r})")


def check_definition_change_plan_determinism(ctx: CheckContext, top: str, established) -> SubCheck:
    """One mutable-field change, applied twice, producing the same plan twice.

    Criterion 22's first clause. The two plans are compared as values, not as two
    exit codes: `reconcile_plan` keeps every record `vz --json up` emitted and the
    structured refusal it ended with, and removes only the fields that name the
    invocation. Stable identity is read before and after and compared field by
    field, because "reconciles without changing its stable identity" is a claim
    about IDs and not about a state word.
    """
    check = SubCheck(top, "definition_change_plan_determinism")
    subject = reconcile_subject(ctx, check, established, 0)
    if subject is None:
        return check.finish()
    try:
        changed = mutable_change(subject.definition)
        original_memory = (subject.definition["environment"]["machines"][0].get("resources") or {}).get("memory_mb")
        if not check.check(original_memory != MUTATED_MEMORY_MB,
                           f"{subject.name}: {MUTABLE_FIELD} changes value ({original_memory!r} -> "
                           f"{MUTATED_MEMORY_MB!r})"):
            return check.finish()
        after_write, responded = desired_digest_responds(ctx, check, subject, changed)
        if after_write is None:
            return check.finish()
        desired_now = after_write.get("desired_definition_digest")
        if not responded:
            check.check(False, f"{subject.name}: the desired definition digest must follow {MUTABLE_FIELD} "
                               f"(before {subject.desired!r}, after the change {desired_now!r})")
            reconcile_unimplemented(check,
                                    f"{RECONCILE_INPUTS_DOC} 'Snapshot model' requires the desired inputs a "
                                    f"reconcile plans from to be an identity that changes when the inputs change; "
                                    f"this runtime published the same desired_definition_digest {desired_now!r} "
                                    f"before and after {MUTABLE_FIELD} changed, so there is no desired-input "
                                    f"identity for planning to consume and no plan to compare.", "")
            return check.finish()
        check.check(True, f"{subject.name}: the desired definition digest followed {MUTABLE_FIELD} "
                          f"({subject.desired} -> {desired_now})")
        check.check(after_write.get("persisted_definition_digest") == subject.persisted,
                    f"{subject.name}: reading the changed definition did not persist it "
                    f"({subject.persisted!r} observed {after_write.get('persisted_definition_digest')!r})")
        check.check(after_write.get("definition_drift") is True,
                    f"{subject.name}: the changed definition is reported as drift (observed "
                    f"{after_write.get('definition_drift')!r})")
        first = ctx.run(check, subject.name + "-recon-up-1", ["--json", "up"], cwd=subject.project,
                        env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        second = ctx.run(check, subject.name + "-recon-up-2", ["--json", "up"], cwd=subject.project,
                         env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        plans = (reconcile_plan(first), reconcile_plan(second))
        check.check(plans[0] == plans[1],
                    f"{subject.name}: the same definition change produced the same plan twice (exit "
                    f"{plans[0]['exit_code']}/{plans[1]['exit_code']}, {len(plans[0]['records'])}/"
                    f"{len(plans[1]['records'])} records; fields removed by name: {sorted(PLAN_VOLATILE)})"
                    if plans[0] == plans[1] else
                    f"{subject.name}: the two plans differ: {json.dumps(plans[0])[:300]} vs "
                    f"{json.dumps(plans[1])[:300]}")
        after = reconcile_status(ctx, check, subject.name + "-recon-after-up", subject.instance)
        if after is None:
            return check.finish()
        identity = reconcile_identity(after)
        observed_environment = (identity["environments"] or [{}])[0]
        expected_environment = (subject.identity["environments"] or [{}])[0]
        for field in ("environment_id", "name", "machines", "networks", "network_attachments", "endpoints"):
            check.check(observed_environment.get(field) == expected_environment.get(field),
                        f"{subject.name}: the Environment kept its {field} across the reconcile "
                        f"({json.dumps(expected_environment.get(field))[:160]} observed "
                        f"{json.dumps(observed_environment.get(field))[:160]})")
        if first.exit_code == 0 and second.exit_code == 0:
            check.check(after.get("persisted_definition_digest") == desired_now,
                        f"{subject.name}: an accepted reconcile persisted the definition it planned from "
                        f"({desired_now!r} observed {after.get('persisted_definition_digest')!r})")
            check.check(after.get("definition_drift") is False,
                        f"{subject.name}: an accepted reconcile converged (definition_drift observed "
                        f"{after.get('definition_drift')!r})")
        else:
            check.check(after.get("persisted_definition_digest") == subject.persisted,
                        f"{subject.name}: a refused reconcile persisted nothing ({subject.persisted!r} observed "
                        f"{after.get('persisted_definition_digest')!r})")
            check.check(observed_environment.get("lifecycle_generation") ==
                        expected_environment.get("lifecycle_generation"),
                        f"{subject.name}: a refused reconcile consumed no lifecycle generation "
                        f"({expected_environment.get('lifecycle_generation')!r} observed "
                        f"{observed_environment.get('lifecycle_generation')!r})")
            reconcile_unimplemented(check,
                                    f"{RECONCILE_INPUTS_DOC} 'Capture and atomic admission' requires "
                                    f"`admit_reconcile_round` to derive a plan from the changed inputs and return "
                                    f"`BatchCreated`, and {RECONCILE_FENCING_DOC} 'Action schema v3' requires that "
                                    f"plan's create/recreate/remove actions to carry a `ReplicaPrecondition`. This "
                                    f"runtime derives no plan at all from a changed mutable field: it refuses the "
                                    f"Up before admission, so the deterministic-plan and reconcile clauses have "
                                    f"no subject and only the identity-preservation half is proved.",
                                    reconcile_refusal_text(reconcile_error(first)))
    finally:
        _reconcile_restored(ctx, check, subject)
    return check.finish()


def check_immutable_change_refused_before_mutation(ctx: CheckContext, top: str, established) -> SubCheck:
    """An immutable-field change is refused, and nothing moved while it was.

    Criterion 22's second clause has two halves and both are proved. "Fails" is
    the structured envelope: exactly one machine-readable code, a nonempty
    explanation, and the exact persisted and requested digests the refusal was
    decided against -- which is what makes it a decision at the fencing boundary
    rather than an incidental error. "Before mutation" is the isolate's
    state-root inventory taken immediately either side of the refused Up, plus
    every identity `vz status --json` publishes.
    """
    check = SubCheck(top, "immutable_change_refused_before_mutation")
    subject = reconcile_subject(ctx, check, established, 1)
    if subject is None:
        return check.finish()
    try:
        changed = immutable_change(subject.definition)
        original_digest = subject.definition["environment"]["machines"][0]["target"].get("digest")
        if not check.check(original_digest != FOREIGN_ARTIFACT_DIGEST,
                           f"{subject.name}: {IMMUTABLE_FIELD} changes value ({original_digest!r} -> "
                           f"{FOREIGN_ARTIFACT_DIGEST!r})"):
            return check.finish()
        after_write, responded = desired_digest_responds(ctx, check, subject, changed)
        if after_write is None:
            return check.finish()
        desired_now = after_write.get("desired_definition_digest")
        if not responded:
            check.check(False, f"{subject.name}: the desired definition digest must follow {IMMUTABLE_FIELD} "
                               f"(before {subject.desired!r}, after the change {desired_now!r})")
            reconcile_unimplemented(check,
                                    f"{RECONCILE_INPUTS_DOC} 'Snapshot model' requires a desired-input identity "
                                    f"that changes when the inputs change; this runtime published the same "
                                    f"desired_definition_digest {desired_now!r} before and after "
                                    f"{IMMUTABLE_FIELD} changed, so an immutable change is indistinguishable from "
                                    f"no change and there is nothing for it to refuse.", "")
            return check.finish()
        before_rows, path = write_inventory(ctx.evidence_dir, f"recon-{subject.name}-state-before",
                                            subject.instance["state"])
        check.evidence.append(path)
        refused = ctx.run(check, subject.name + "-recon-immutable-up", ["--json", "up"], cwd=subject.project,
                          env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        after_rows, path = write_inventory(ctx.evidence_dir, f"recon-{subject.name}-state-after",
                                           subject.instance["state"])
        check.evidence.append(path)
        check.check(refused.exit_code not in (0, None),
                    f"{subject.name}: the immutable change to {IMMUTABLE_FIELD} was refused (exit "
                    f"{refused.exit_code}, expected non-zero)")
        error = reconcile_error(refused)
        code = (error or {}).get("code")
        check.check(isinstance(code, str) and ERROR_CODE_SPELLING.match(code) is not None,
                    f"{subject.name}: the refusal carries a machine-readable code (observed {code!r})")
        check.check(isinstance((error or {}).get("message"), str) and str((error or {}).get("message")).strip() != "",
                    f"{subject.name}: the refusal carries a structured explanation (observed "
                    f"{str((error or {}).get('message'))[:160]!r})")
        spoken = reconcile_refusal_text(error)
        check.check(str(subject.persisted) in spoken and str(desired_now) in spoken,
                    f"{subject.name}: the refusal names the exact precondition it was decided against -- persisted "
                    f"digest {subject.persisted!r} against requested digest {desired_now!r} -- rather than failing "
                    f"incidentally (refusal: {spoken[:300]!r})")
        diff = inventory_diff(before_rows, after_rows)
        check.check(not diff,
                    f"{subject.name}: the isolate state root is byte-identical across the refused Up "
                    f"({len(before_rows)} entries)" if not diff else
                    f"{subject.name}: the refused Up mutated the state root: " + "; ".join(diff[:6]))
        after = reconcile_status(ctx, check, subject.name + "-recon-immutable-after", subject.instance)
        if after is None:
            return check.finish()
        identity = reconcile_identity(after)
        check.check(identity == subject.identity,
                    f"{subject.name}: every published identity is byte-identical after the refusal"
                    if identity == subject.identity else
                    f"{subject.name}: identities changed despite the refusal: "
                    f"{json.dumps(subject.identity)[:260]} vs {json.dumps(identity)[:260]}")
        check.check(after.get("persisted_definition_digest") == subject.persisted,
                    f"{subject.name}: the persisted definition digest is unchanged ({subject.persisted!r} observed "
                    f"{after.get('persisted_definition_digest')!r})")
        # The criterion distinguishes mutable changes from immutable ones. A
        # runtime that answers both with one code has classified nothing, and
        # that is a finding about the runtime rather than an assertion it passes.
        subject.write(definition_bytes(mutable_change(subject.definition)))
        mutable_receipt = ctx.run(check, subject.name + "-recon-mutable-up", ["--json", "up"], cwd=subject.project,
                                  env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        mutable_code = (reconcile_error(mutable_receipt) or {}).get("code")
        classified = mutable_receipt.exit_code == 0 or mutable_code != code
        check.check(classified,
                    f"{subject.name}: the immutable change is classified apart from the mutable one (immutable "
                    f"{code!r} exit {refused.exit_code}, mutable {mutable_code!r} exit "
                    f"{mutable_receipt.exit_code})")
        if not classified:
            reconcile_unimplemented(check,
                                    f"{RECONCILE_INPUTS_DOC} 'Planning and execution' requires the planner to "
                                    f"decide per change what converges and what must be recreated, and criterion "
                                    f"22 requires immutable/unsafe changes to fail with a structured explanation "
                                    f"of what makes them unsafe. This runtime answers every definition change -- "
                                    f"mutable {MUTABLE_FIELD} and immutable {IMMUTABLE_FIELD} alike -- with the "
                                    f"same {code!r} refusal, so what is proved above is that a changed definition "
                                    f"fails before mutation, not that the change was classified.",
                                    reconcile_refusal_text(error))
    finally:
        _reconcile_restored(ctx, check, subject)
    return check.finish()


def check_concurrent_stale_reconcile_fail_closed(ctx: CheckContext, top: str, established) -> SubCheck:
    """Concurrent, interrupted and stale reconciles converge or fail closed.

    Criterion 22's third clause names three negative properties and each is
    asserted here as a value: no mixed-version topology (every Machine still on
    the incarnation generation pre-sleep recorded FOR IT -- "they all agree"
    would be vacuous with one Machine -- plus one lifecycle generation and one
    definition digest shared by the project and the Environment), no cross-owner
    adoption
    (no resource id appears under two owners, across all three of pre-sleep's
    mutually foreign Environments), and no orphaned resources (the owned-resource
    map compared before against after, scoped to what the plan itself would have
    created -- a forked Machine is a runtime object reconcile must leave alone,
    so nothing here requires an undeclared Machine to disappear).
    """
    check = SubCheck(top, "concurrent_stale_reconcile_fail_closed")
    subject = reconcile_subject(ctx, check, established, 2)
    if subject is None:
        return check.finish()
    siblings, entries = [], (established or {}).get("environments") or []
    for index in (0, 1):
        if len(entries) > index:
            try:
                siblings.append((entries[index]["isolate"], ctx.reattach(entries[index]["isolate"])))
            except ReattachError as error:
                check.fail(str(error))
                return check.finish()
    try:
        changed = mutable_change(subject.definition)
        after_write, responded = desired_digest_responds(ctx, check, subject, changed)
        if after_write is None:
            return check.finish()
        desired_now = after_write.get("desired_definition_digest")
        if not responded:
            check.check(False, f"{subject.name}: the desired definition digest must follow {MUTABLE_FIELD} "
                               f"(before {subject.desired!r}, after the change {desired_now!r})")
            reconcile_unimplemented(check,
                                    f"{RECONCILE_FENCING_DOC} 'Authoritative planning snapshot' requires two "
                                    f"controllers to plan against one consistent snapshot of the same desired "
                                    f"inputs; this runtime published the same desired_definition_digest "
                                    f"{desired_now!r} before and after the change, so concurrent updates have no "
                                    f"competing version to be fenced against.", "")
            return check.finish()
        token = uuid.uuid4().hex[:12]
        identities = ((f"req-recon-{token}-a", f"recon-{token}-a"), (f"req-recon-{token}-b", f"recon-{token}-b"))
        held = ctx.start(check, subject.name + "-recon-concurrent-a",
                         ["--json", "up", "--request-id", identities[0][0], "--idempotency-key", identities[0][1]],
                         cwd=subject.project, env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        second = ctx.run(check, subject.name + "-recon-concurrent-b",
                         ["--json", "up", "--request-id", identities[1][0], "--idempotency-key", identities[1][1]],
                         cwd=subject.project, env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        # Releasing signals the held invocation. Whether it had already answered
        # or was still mid-flight, this is also the criterion's "interrupted
        # reconciliation"; the assertions below are about the state either
        # outcome leaves behind.
        first = ctx.release(check, held)
        codes = (first.exit_code, second.exit_code)
        converged = codes == (0, 0)
        closed = all(code is not None and code > 0 for code in codes)
        interrupted = [code for code in codes if code is not None and code < 0]
        check.check(converged or closed,
                    f"{subject.name}: the two concurrent Ups both converged or both failed closed (exit codes "
                    f"{list(codes)}; signalled {interrupted})")
        after = reconcile_status(ctx, check, subject.name + "-recon-concurrent-after", subject.instance)
        if after is None:
            return check.finish()
        identity = reconcile_identity(after)
        environments = identity["environments"]
        if not check.check(len(environments) == 1,
                           f"{subject.name}: exactly one Environment after the concurrent pair (observed "
                           f"{[row.get('environment_id') for row in environments]})"):
            return check.finish()
        environment = environments[0]
        expected = subject.identity["environments"][0]
        # No mixed-version topology. With one Machine "they all agree" would be
        # vacuous, so each Machine is compared against the incarnation generation
        # pre-sleep recorded for it by name, and the set is reported beside it.
        recorded = {machine["name"]: machine["incarnation_generation"] for machine in subject.entry["machines"]}
        observed = {machine["name"]: machine["incarnation_generation"] for machine in environment["machines"]}
        check.check(observed == recorded,
                    f"{subject.name}: no mixed-version topology -- every Machine still reports the incarnation "
                    f"generation pre-sleep recorded ({recorded} observed {observed}; distinct values "
                    f"{sorted(set(observed.values()), key=str)})")
        if converged:
            # Two accepted reconciles legitimately consume generations; what may
            # not happen is the pair leaving two versions behind.
            check.check(isinstance(environment["lifecycle_generation"], int)
                        and isinstance(expected["lifecycle_generation"], int)
                        and environment["lifecycle_generation"] > expected["lifecycle_generation"],
                        f"{subject.name}: the accepted pair advanced one lifecycle counter monotonically "
                        f"({expected['lifecycle_generation']!r} -> {environment['lifecycle_generation']!r})")
            check.check(environment["definition_digest"] == after.get("persisted_definition_digest") == desired_now,
                        f"{subject.name}: one definition version across the project and the Environment, and it is "
                        f"the one the pair planned from (Environment {environment['definition_digest']!r}, project "
                        f"{after.get('persisted_definition_digest')!r}, planned {desired_now!r})")
        else:
            check.check(environment["lifecycle_generation"] == expected["lifecycle_generation"],
                        f"{subject.name}: the refused pair consumed no lifecycle generation "
                        f"({expected['lifecycle_generation']!r} observed "
                        f"{environment['lifecycle_generation']!r})")
            check.check(environment["definition_digest"] == expected["definition_digest"] ==
                        after.get("persisted_definition_digest"),
                        f"{subject.name}: one definition version across the project and the Environment "
                        f"(Environment {environment['definition_digest']!r}, project "
                        f"{after.get('persisted_definition_digest')!r}, expected "
                        f"{expected['definition_digest']!r})")
        owned_after = reconcile_owned_resources(after)
        changed_owners = sorted(f"{key}: {subject.owned.get(key)!r} -> {owned_after.get(key)!r}"
                                for key in set(subject.owned) | set(owned_after)
                                if subject.owned.get(key) != owned_after.get(key))
        check.check(not changed_owners,
                    f"{subject.name}: every owned resource kept its owner ({len(subject.owned)} resources)"
                    if not changed_owners else
                    f"{subject.name}: ownership changed: " + "; ".join(changed_owners[:6]))
        adopted = []
        for sibling_name, sibling in siblings:
            payload = reconcile_status(ctx, check, sibling_name + "-recon-foreign", sibling)
            if payload is None:
                return check.finish()
            foreign = reconcile_owned_resources(payload)
            adopted.extend(f"{key} is claimed by both {sibling_name} ({foreign[key]!r}) and {subject.name} "
                           f"({owned_after[key]!r})" for key in sorted(set(foreign) & set(owned_after)))
        check.check(not adopted,
                    f"{subject.name}: no cross-owner adoption -- none of the {len(siblings)} foreign "
                    f"Environment(s) shares a resource id with this one"
                    if not adopted else f"{subject.name}: cross-owner adoption: " + "; ".join(adopted[:6]))
        terminal = [record for receipt in (first, second) for record in reconcile_plan(receipt)["records"]
                    if isinstance(record, dict) and (record.get("progress") or {}).get("completion")]
        check.check(set(owned_after) == set(subject.owned),
                    f"{subject.name}: no orphaned resources -- the plan created and removed nothing and no owned "
                    f"resource appeared or vanished ({len(subject.owned)} before, {len(owned_after)} after; "
                    f"{len(terminal)} terminal plan record(s))"
                    if set(owned_after) == set(subject.owned) else
                    f"{subject.name}: orphaned/extra resources: appeared "
                    f"{sorted(set(owned_after) - set(subject.owned))[:6]}, vanished "
                    f"{sorted(set(subject.owned) - set(owned_after))[:6]}")
        # The stale client: the exact request identity of the second Up, replayed
        # against a definition that has moved again since it was issued.
        subject.write(definition_bytes(immutable_change(changed)))
        stale = ctx.run(check, subject.name + "-recon-stale-replay",
                        ["--json", "up", "--request-id", identities[1][0], "--idempotency-key", identities[1][1]],
                        cwd=subject.project, env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        stale_code = (reconcile_error(stale) or {}).get("code")
        check.check(stale.exit_code not in (0, None),
                    f"{subject.name}: a stale client replaying request {identities[1][0]!r} against a definition "
                    f"that moved again is refused (exit {stale.exit_code}, expected non-zero)")
        check.check(isinstance(stale_code, str) and ERROR_CODE_SPELLING.match(stale_code) is not None,
                    f"{subject.name}: the stale replay carries a machine-readable code (observed {stale_code!r})")
        stale_after = reconcile_status(ctx, check, subject.name + "-recon-stale-after", subject.instance)
        if stale_after is None:
            return check.finish()
        # Against the state the concurrent pair left, not against the state
        # before it: an accepted pair legitimately moved the digest and the
        # counter, and what the stale replay may not do is move them again.
        stale_identity = reconcile_identity(stale_after)
        check.check(stale_identity == identity,
                    f"{subject.name}: the stale replay changed no identity and consumed no generation "
                    f"(lifecycle generation {_reconcile_generation(identity)!r})"
                    if stale_identity == identity else
                    f"{subject.name}: the stale replay changed state: {json.dumps(identity)[:260]} vs "
                    f"{json.dumps(stale_identity)[:260]}")
        if not converged:
            reconcile_unimplemented(check,
                                    f"{RECONCILE_FENCING_DOC} 'Durable claim and exact StateStore CAS' requires "
                                    f"the loser of a concurrent claim to be refused by `start_reconcile_batch` "
                                    f"after the winner has taken the exact `started` audit row, and 'Strict "
                                    f"mutation ordering' requires an interrupted reconcile to resume that same "
                                    f"claim. Both need an admitted reconcile to fence. This runtime refuses every "
                                    f"definition change before admission, so no claim is ever taken: what is "
                                    f"proved above is that concurrent, interrupted and stale reconciles all fail "
                                    f"closed with no mixed-version topology, no cross-owner adoption and no "
                                    f"orphaned resources -- not that the exact-generation claim contract holds.",
                                    reconcile_refusal_text(reconcile_error(second)))
    finally:
        _reconcile_restored(ctx, check, subject)
    return check.finish()


def check_effective_input_snapshot_identity(ctx: CheckContext, top: str, established) -> SubCheck:
    """One canonical desired-input identity, shared by planning and activation.

    Criterion 22's last clause, reduced to what the 0.4 public surface can carry.
    `reconcile-effective-inputs.md` requires one immutable operation-owned
    snapshot whose canonical digest binds planning, the persisted session, the
    audit and execution, so activation cannot consume different bytes from the
    ones planning recorded. The public equivalent of that digest is the
    definition digest, and it is asserted exactly: the digest the project
    persisted and the digest the Environment was activated under must be one
    value; it must be canonical over the definition VALUE rather than over its
    bytes; and it must move when the value moves. The per-service
    `vz-effective-service-input-v1` digests, the `vz-reconcile-input-manifest-v1`
    manifest and the tamper rules over staged secret/image blobs have no subject
    on this surface, and their absence is asserted rather than assumed.
    """
    check = SubCheck(top, "effective_input_snapshot_identity")
    subject = reconcile_subject(ctx, check, established, 0)
    if subject is None:
        return check.finish()
    try:
        environments = subject.identity["environments"]
        if not check.check(len(environments) == 1,
                           f"{subject.name}: one Environment to read the activation digest from (observed "
                           f"{len(environments)})"):
            return check.finish()
        activation = environments[0]["definition_digest"]
        check.check(activation == subject.persisted == subject.desired,
                    f"{subject.name}: planning and activation name one desired-input identity (desired "
                    f"{subject.desired!r}, persisted {subject.persisted!r}, Environment {activation!r})")
        check.check(DIGEST_SPELLING.match(str(activation or "")) is not None,
                    f"{subject.name}: that identity is a canonical sha256 digest (observed {activation!r})")
        # Canonical over the value, not over the file: the same definition
        # serialized differently must digest to the same thing.
        subject.write(reordered_bytes(subject.definition))
        reordered = reconcile_status(ctx, check, subject.name + "-recon-reordered", subject.instance)
        if reordered is None:
            return check.finish()
        check.check(reordered.get("desired_definition_digest") == subject.desired,
                    f"{subject.name}: reserializing the same definition does not change its digest "
                    f"({subject.desired!r} observed {reordered.get('desired_definition_digest')!r})")
        check.check(reordered.get("definition_drift") is False,
                    f"{subject.name}: reserializing the same definition is not drift (observed "
                    f"{reordered.get('definition_drift')!r})")
        # And it must move when the value moves.
        subject.write(definition_bytes(mutable_change(subject.definition)))
        moved = reconcile_status(ctx, check, subject.name + "-recon-moved", subject.instance)
        if moved is None:
            return check.finish()
        check.check(moved.get("desired_definition_digest") not in (None, "", subject.desired),
                    f"{subject.name}: changing {MUTABLE_FIELD} changes the desired-input digest "
                    f"({subject.desired!r} -> {moved.get('desired_definition_digest')!r})")
        check.check(moved.get("persisted_definition_digest") == subject.persisted,
                    f"{subject.name}: activation still names the digest it was activated under "
                    f"({subject.persisted!r} observed {moved.get('persisted_definition_digest')!r})")
        refused = ctx.run(check, subject.name + "-recon-inputs-up", ["--json", "up"], cwd=subject.project,
                          env=subject.env, timeout=RECONCILE_UP_TIMEOUT)
        # Everything the public surface said, searched for a snapshot identity.
        spoken = json.dumps([moved, reconcile_plan(refused)])
        published = sorted(key for key in EFFECTIVE_INPUT_KEYS if f'"{key}"' in spoken)
        check.check(not published,
                    f"{subject.name}: the public surface publishes no effective-input snapshot identity, so this "
                    f"contract's own digests are named as absent rather than assumed (searched for "
                    f"{list(EFFECTIVE_INPUT_KEYS)})"
                    if not published else
                    f"{subject.name}: the public surface publishes {published}; this sub-check does not yet assert "
                    f"the snapshot contract those keys carry")
        reconcile_unimplemented(check,
                                f"{RECONCILE_INPUTS_DOC} 'Snapshot model' requires an immutable operation-owned "
                                f"`ReconcileInputSnapshot` with a `vz-reconcile-input-manifest-v1` manifest digest "
                                f"and per-service `vz-effective-service-input-v1` effective digests over each "
                                f"service's referenced networks, volumes, secret bytes and resolved image content; "
                                f"'Recovery, retention, and cleanup' requires a tampered or reordered manifest to "
                                f"be a state conflict before mutation. The 0.4 ProjectDefinition "
                                f"({PROJECT_DEFINITION_SCHEMA}) declares no services, secrets or volumes and the "
                                f"public CLI has no scoped service create/recreate/remove, so those digests and "
                                f"the tamper rules over their staged blobs have no subject here: none of "
                                f"{list(EFFECTIVE_INPUT_KEYS)} appears on any public interface. What is proved "
                                f"above is the definition-level identity -- one canonical digest, shared by "
                                f"planning and activation, canonical over the value and responsive to it.",
                                reconcile_refusal_text(reconcile_error(refused)))
    finally:
        _reconcile_restored(ctx, check, subject)
    return check.finish()


def check_definition_reconciliation_fencing(ctx: CheckContext, top: str, established) -> list:
    """Criterion 22's four sub-checks, over pre-sleep's three Environments.

    Each addresses one isolate and restores it before returning, so the order
    below is the order their evidence is recorded in and not a dependency.
    """
    return [check_definition_change_plan_determinism(ctx, top, established),
            check_immutable_change_refused_before_mutation(ctx, top, established),
            check_concurrent_stale_reconcile_fail_closed(ctx, top, established),
            check_effective_input_snapshot_identity(ctx, top, established)]


# --------------------------------------------------------------------- criterion 18
#
# `gate.secrets.snapshots_scoped_redacted`, in the persisted-recovery/post-wake
# phase. Two sub-checks, because the criterion is two claims that fail for
# unrelated reasons and a single verdict could not say which:
#
#   secret_bindings_scoped_redacted   a SecretBinding declared for one
#                       Environment/Machine is readable from that Machine and
#                       from nowhere else -- not from its SIBLING in the same
#                       Environment, not from a Machine in another Environment
#                       -- its use is audited by identity and never by value,
#                       a sibling Environment's attempt to bind it is refused
#                       with a structured error rather than served an empty
#                       result, and the value itself occurs zero times across
#                       status (JSON and human), the daemon log, the audit log,
#                       the whole evidence directory, the state root and its
#                       inventory.
#   snapshot_restore_capability   snapshot/restore passes where
#                       `config/host-target-capabilities-v0.4.json` advertises
#                       the capability for the host x Machine-target pair under
#                       test, and where it does not, the runtime accounts the
#                       request with an EXPLICIT unsupported capability -- a
#                       machine-readable reason against the capability itself --
#                       rather than granting it, refusing generically, or saying
#                       nothing at all.
#
# The value planted here never reaches this process's own artifacts by the
# gate's own hand: the host passes it to the CLI in the environment (never on a
# command line, which the recorder writes into every receipt), and the guest
# reports a SHA-256 of what it read rather than the bytes. Any occurrence the
# sweep finds is therefore the runtime's, which is the only thing that makes a
# redaction claim mean anything.

CAPABILITY_MATRIX = "config/host-target-capabilities-v0.4.json"
# The two statuses `status_definitions` defines as shipped-or-demonstrated.
# PLANNED and NA are the two that are not advertised.
ADVERTISED_STATUSES = ("ACTIVE", "DEV")
SECRET_BINDING = "gate-secret"
SECRET_TARGET_PATH = "/run/vz-secrets/gate-secret"
# The host-side name the binding reads its value from. An environment variable
# rather than a file or a literal in vz.json: a literal would put the value into
# the project the state-root sweep reads back, and the leak would be the gate's
# own rather than the runtime's.
SECRET_SOURCE_ENV = "VZ_GATE_SECRET_VALUE"
SECRET_AUDIT_LOG = "audit.jsonl"
SECRET_USE_EVENT = "secret_binding_used"
SNAPSHOT_SENTINEL_PATH = "/run/vz-snapshot-sentinel"
SNAPSHOT_CAPABILITY = "snapshot"
BASE_CAPABILITY = "posix_exec"
SWEEP_MAX_FILES = 20000
SWEEP_GROUPS = ("audit-log", "daemon-log", "evidence", "state-root", "state-root-inventory",
                "status-human", "status-json")


def host_matrix_key(matrix: dict):
    """The `hosts` key of the machine this gate is running on, or None.

    Derived from the host rather than passed in: the criterion is qualified by
    host x Machine-target pair, and a check that had to be told which pair to
    read could be told the wrong one.
    """
    import platform

    system = {"Darwin": "macos", "Linux": "linux", "Windows": "windows"}.get(platform.system())
    arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64", "AMD64": "x86_64"}.get(platform.machine())
    for key, entry in sorted((matrix.get("hosts") or {}).items()):
        if entry.get("os") == system and entry.get("arch") == arch:
            return key
    return None


def capability_pair(repo_root: Path, *, target: str = "linux", profile: str = "developer") -> tuple:
    """(matrix, host key, pair) for this host and the named Machine target."""
    matrix = load_json(Path(repo_root) / CAPABILITY_MATRIX)
    key = host_matrix_key(matrix)
    for pair in matrix.get("pairs") or []:
        if pair.get("host") == key and pair.get("target") == target and pair.get("profile") == profile:
            return matrix, key, pair
    return matrix, key, None


def capability_entry(pair: dict, group: str, name: str) -> tuple:
    """(status, entry) for one capability of one pair; (None, {}) when absent."""
    entry = ((pair or {}).get(group) or {}).get(name)
    if not isinstance(entry, dict):
        return None, {}
    return entry.get("status"), entry


def regular_files(root: Path, limit: int = SWEEP_MAX_FILES) -> list:
    """Every regular non-symlink file under `root`, sorted and bounded."""
    found = []
    root = Path(root)
    if not root.is_dir():
        return found
    for path in sorted(root.rglob("*")):
        if len(found) >= limit:
            break
        if path.is_file() and not path.is_symlink():
            found.append(path)
    return found


def sweep_group(label: str, paths, skipped: list = None) -> list:
    """`[(label, path, bytes)]` for the files read, and what could not be.

    An artifact the sweep could not read is a HOLE in the redaction claim, not a
    file to pass over: `read_regular` refuses anything over its two-gigabyte
    bound and anything that changed underneath it, and either would otherwise
    turn "the value is not in this file" into "this file was never looked at".
    A file that moved during the read is retried once, because a daemon log
    being appended to is the ordinary case and not evidence of anything; what
    still cannot be read is reported to the caller by path and reason.
    """
    from vz04_common import GateError

    rows = []
    for path in paths:
        path = Path(path)
        for attempt in (1, 2):
            try:
                rows.append((label, path, read_regular(path)))
                break
            except (GateError, OSError) as error:
                if attempt == 2 and skipped is not None:
                    skipped.append(f"{label} {path}: {type(error).__name__}: {error}")
    return rows


def secret_occurrences(artifacts: list, needle: str) -> list:
    """`[(label, path, count)]` for every artifact carrying the exact bytes.

    Byte-exact and case-sensitive on purpose: the sentinel is minted at check
    time, so any occurrence at all is the value itself and never a coincidence.
    """
    raw = needle.encode()
    hits = []
    for label, path, data in artifacts:
        count = data.count(raw)
        if count:
            hits.append((label, str(path), count))
    return hits


def secret_definition(release_dir: Path, *, from_environment: str = None) -> dict:
    """Two Developer Linux Machines; only machine-0 is bound to the secret.

    machine-1 exists so "the sibling Machine in the same Environment cannot read
    it" is a claim about a real sibling rather than about a Machine that is not
    there. `from_environment` names another Environment's identity, which is the
    cross-boundary request that has to fail closed.
    """
    definition = minimal_definition(release_dir)
    environment = definition["environment"]
    first = environment["machines"][0]
    second = copy.deepcopy(first)
    second["name"] = "machine-1"
    environment["machines"] = [first, second]
    binding = {"schema_version": 1, "name": SECRET_BINDING, "machine": first["name"],
               "target_path": SECRET_TARGET_PATH, "source_env": SECRET_SOURCE_ENV}
    if from_environment is not None:
        binding["from_environment"] = from_environment
    environment["secret_bindings"] = [binding]
    return definition


def up_reporting_envelope(ctx: CheckContext, check: SubCheck, name: str, definition: dict, *, secret: str = None,
                          timeout: int = UP_TIMEOUT) -> dict:
    """`attempt_up`, plus the structured error envelope and the isolate runtime.

    Both criterion-18 sub-checks have a REFUSAL as the subject of one of their
    claims -- a cross-boundary secret request, and an unadvertised capability --
    and a refusal is only evidence if its machine-readable code can be read, so
    the envelope is returned rather than flattened to a message.

    `secret`, when given, is planted in the CLI's environment and never in an
    argv element: the recorder writes every argv into a receipt under the
    evidence directory, and the redaction sweep reads those receipts back. A
    secret placed on a command line would be found by this check's own plumbing,
    and a leak the gate itself planted says nothing about the runtime.
    """
    data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
    iso = ctx.isolated(name, project_files={"vz.json": data}, provision=True)
    env, project = iso["env"], iso["project"]
    if secret is not None:
        env[SECRET_SOURCE_ENV] = secret
    for label, argv in ((name + "-git-init", [GIT, "init", "--quiet", "--initial-branch", "main"]),
                        (name + "-git-add", [GIT, "add", "vz.json"]),
                        (name + "-git-commit", [GIT, "-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                                                "commit", "--quiet", "-m", "definition"])):
        receipt = ctx.run_tool(check, label, argv, cwd=project, env=env)
        check.check(receipt.exit_code == 0, f"{label}: exit {receipt.exit_code} (expected 0)")
    up = ctx.run(check, name + "-up", ["--json", "up"], cwd=project, env=env, timeout=timeout)
    try:
        envelope = _single_json_line(up.stderr)
    except (UnicodeDecodeError, json.JSONDecodeError):
        envelope = None
    if isinstance(envelope, dict):
        error = envelope.get("error") or {}
        message, code = error.get("message", ""), error.get("code")
    else:
        message, code = up.stderr.decode("utf-8", "replace")[:400], None
    return {"env": env, "project": project, "runtime": iso["runtime"], "exit_code": up.exit_code,
            "envelope": envelope, "message": message, "code": code, "stderr": up.stderr,
            "status": read_status(ctx, check, name, project=project, env=env) if up.exit_code == 0 else None}


def secret_digest_script(path: str) -> str:
    """Report a SHA-256 of what the Machine can read, plus its own exit status.

    The digest, never the bytes: a probe that printed the secret would put it
    into this check's own receipts, and the redaction sweep would then find the
    gate's leak instead of the runtime's. `printf ':%s'` keeps "refused" and
    "read an empty file" distinguishable, exactly as `guest_fetch_script` does.
    """
    return (f"out=$(/bin/busybox sha256sum {path} 2>/dev/null); rc=$?; "
            "printf '%s' \"${out%% *}\"; printf ':%s' \"$rc\"")


def digest_probe(receipt) -> tuple:
    """(digest, status) a `secret_digest_script` probe reported."""
    text = receipt.stdout.decode("utf-8", "replace").strip()
    if ":" not in text:
        return "", None
    digest, _colon, status = text.rpartition(":")
    return digest.strip(), status.strip()


def audit_records(path: Path) -> tuple:
    """(records, raw bytes, problem) from one JSON-lines audit log."""
    from vz04_common import GateError

    try:
        raw = read_regular(Path(path))
    except (GateError, OSError) as error:
        return [], b"", f"no readable audit log at {path}: {error}"
    records, problems = [], []
    for number, line in enumerate(raw.decode("utf-8", "replace").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as error:
            problems.append(f"line {number}: {error}")
    return records, raw, ("; ".join(problems[:3]) if problems else None)


def check_secret_bindings_scoped_redacted(ctx: CheckContext, top: str) -> SubCheck:
    """A SecretBinding is readable from its own Machine and from nowhere else.

    The claims are ordered so the first one to fail names the broken one rather
    than the whole criterion:

      1. the capability matrix is read for the host x Machine-target pair under
         test, because "advertised" is what decides whether an absent adapter is
         a gap or a failure;
      2. the declaration surface exists at all -- a criterion whose subject
         cannot be written down in a ProjectDefinition has nothing to prove, and
         says so rather than passing;
      3. scope: the bound Machine reads exactly the planted value (compared as a
         SHA-256, so the value never enters this check's own receipts), and its
         SIBLING in the same Environment does not;
      4. denial: a Machine in another Environment cannot read the path, and a
         sibling Environment that declares the same binding is REFUSED with a
         structured error rather than brought up holding an empty one;
      5. audit: using it produced a record naming this Environment, this Machine
         and this binding, carrying the binding's identity and not its value;
      6. redaction, LAST, so the sweep covers every artifact the claims above
         produced: zero occurrences of the exact planted bytes across status
         JSON, human status, the daemon log, the audit log, the evidence
         directory, the state root, and the state-root inventory.
    """
    import hashlib

    check = SubCheck(top, "secret_bindings_scoped_redacted")
    matrix, host, pair = capability_pair(ctx.repo_root)
    if not check.check(pair is not None, f"{CAPABILITY_MATRIX} declares the pair under test "
                       f"(host {host!r} x target 'linux' x profile 'developer')"):
        return check.finish()
    status, entry = capability_entry(pair, "topology_capabilities", "secret_bindings")
    known = sorted(matrix.get("status_definitions") or {})
    if not check.check(status in known,
                       f"{host}/linux/developer advertises secret_bindings as {status!r} (one of {known})"):
        return check.finish()
    advertised = status in ADVERTISED_STATUSES
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if not check.check(schema_path.is_file() and not schema_path.is_symlink(),
                       f"the definition schema is present at {PROJECT_DEFINITION_SCHEMA}"):
        return check.finish()
    schema_document = load_json(schema_path)
    properties = sorted((((schema_document.get("$defs") or {}).get("environment") or {}).get("properties") or {}))
    if "secret_bindings" not in properties:
        # An advertised capability with no way to declare it is a FAILURE; a
        # capability the matrix does not advertise is the runtime's own stated
        # gap, reported in its own words. The two must never collapse.
        if advertised:
            check.fail(f"{CAPABILITY_MATRIX} advertises secret_bindings as {status} for {host}/linux/developer, but "
                       f"{PROJECT_DEFINITION_SCHEMA} declares no way to bind one: the environment properties it "
                       f"names are {properties}")
            return check.finish()
        check.not_implemented = (
            f"no SecretBinding can be declared: {PROJECT_DEFINITION_SCHEMA} names environment properties "
            f"{properties}, and {CAPABILITY_MATRIX} reports secret_bindings {status} for {host}/linux/developer "
            f"-- {entry.get('note') or 'no note'}. Nothing was planted, so nothing about scope, redaction, audit "
            "or cross-boundary denial is claimed here.")
        return check.finish()
    check.ok(f"{PROJECT_DEFINITION_SCHEMA} declares environment.secret_bindings (properties {properties})")
    try:
        definition = secret_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    problems = sorted(Draft202012Validator(schema_document).iter_errors(definition),
                      key=lambda e: list(map(str, e.absolute_path)))
    if not check.check(not problems, "the secret-binding definition validates against the shipped schema"
                       if not problems else f"definition invalid: {problems[0].message[:200]}"):
        return check.finish()
    # 64 hex characters minted here and nowhere else. A fixed canary could be
    # left in an artifact by an earlier run and a short one could occur by
    # chance; neither would make "occurs zero times" mean anything.
    secret = "vzsec-" + uuid.uuid4().hex + uuid.uuid4().hex
    expected_digest = hashlib.sha256(secret.encode()).hexdigest()
    holder = sibling = refused = None
    try:
        holder = up_reporting_envelope(ctx, check, "sec-a", definition, secret=secret)
        if holder["exit_code"] != 0:
            if advertised:
                check.fail(f"{CAPABILITY_MATRIX} advertises secret_bindings as {status} for "
                           f"{host}/linux/developer, but Up refused the declaration: exit "
                           f"{holder['exit_code']}, error.code {holder['code']!r}, {holder['message'][:200]!r}")
            else:
                check.not_implemented = ("this runtime refuses a declared SecretBinding: exit "
                                         f"{holder['exit_code']}, error.code {holder['code']!r}, "
                                         f"{holder['message'][:300]!r}")
            return check.finish()
        payload = holder["status"]
        if not check.check(payload is not None, "the Environment holding the binding reports a readable status"):
            return check.finish()
        environments = payload.get("environments") or []
        if not check.check(len(environments) == 1, f"one Environment holds the binding (observed {len(environments)})"):
            return check.finish()
        environment = environments[0]
        machines = {machine.get("name"): machine for machine in environment.get("machines") or []}
        if not check.check(sorted(machines) == ["machine-0", "machine-1"],
                           f"both declared Machines are present (observed {sorted(machines)})"):
            return check.finish()
        environment_id = environment.get("environment_id")
        holder_machine_id = (machines.get("machine-0") or {}).get("machine_id")

        # The detector itself, proved against a control the sweep cannot have
        # produced. A sweep whose comparison never matches anything reports zero
        # occurrences of everything, which is indistinguishable from redaction.
        control = [("control", Path("<synthetic control>"), b"leading " + secret.encode() + b" trailing")]
        found = secret_occurrences(control, secret)
        if not check.check(found == [("control", "<synthetic control>", 1)],
                           f"the redaction sweep finds the sentinel in a control buffer (observed {found}, "
                           "expected exactly one occurrence)"):
            return check.finish()

        # 3. Scope. The bound Machine, then its sibling in the SAME Environment.
        read = machine_exec(ctx, check, "sec-a-holder-read", holder, "machine-0",
                            secret_digest_script(SECRET_TARGET_PATH))
        digest, reported = digest_probe(read)
        if not check.check(digest == expected_digest and reported == "0",
                           f"machine-0 reads the binding at {SECRET_TARGET_PATH}: sha256 {digest!r} status "
                           f"{reported!r} (expected {expected_digest!r} status '0')"):
            return check.finish()
        sibling_read = machine_exec(ctx, check, "sec-a-sibling-machine-read", holder, "machine-1",
                                    secret_digest_script(SECRET_TARGET_PATH))
        sibling_digest, sibling_reported = digest_probe(sibling_read)
        check.check(sibling_digest != expected_digest and sibling_reported not in ("0", None),
                    "machine-1, the sibling in the SAME Environment that declares no binding, cannot read it: "
                    f"sha256 {sibling_digest!r} status {sibling_reported!r} (expected neither {expected_digest!r} "
                    "nor status '0')")

        # 4. Denial across the Environment boundary, in both its shapes.
        sibling = up_reporting_envelope(ctx, check, "sec-b", minimal_definition(ctx.release_dir), secret=secret)
        if not check.check(sibling["exit_code"] == 0 and sibling["status"] is not None,
                           f"a sibling Environment that declares no binding comes up (exit {sibling['exit_code']}); "
                           "without it no cross-boundary denial was measured"):
            return check.finish()
        foreign = machine_exec(ctx, check, "sec-b-foreign-read", sibling, "machine-0",
                               secret_digest_script(SECRET_TARGET_PATH))
        foreign_digest, foreign_reported = digest_probe(foreign)
        check.check(foreign_digest != expected_digest and foreign_reported not in ("0", None),
                    "a Machine in a sibling Environment cannot read the binding's path: sha256 "
                    f"{foreign_digest!r} status {foreign_reported!r} (expected neither {expected_digest!r} "
                    "nor status '0')")
        crossing = up_reporting_envelope(ctx, check, "sec-c",
                                         secret_definition(ctx.release_dir, from_environment=environment_id),
                                         secret=secret)
        refused = crossing
        crossing_code = crossing.get("code")
        check.check(crossing["exit_code"] not in (0, None) and isinstance(crossing_code, str)
                    and bool(crossing_code.strip()),
                    f"a sibling Environment requesting Environment {environment_id}'s binding fails CLOSED with a "
                    f"structured error: exit {crossing['exit_code']}, error.code {crossing_code!r} (expected a "
                    "nonzero exit and a machine-readable code, never an empty result)")
        check.check(crossing["status"] is None,
                    "the refused cross-boundary request left no Environment behind (a readable status after it: "
                    f"{crossing['status'] is not None})")
        check.check(secret.encode() not in crossing["stderr"],
                    f"the cross-boundary refusal names no secret value ({len(crossing['stderr'])} bytes of stderr)")

        # 5. Audit on use: by identity, never by value.
        audit_path = Path(holder["runtime"]) / SECRET_AUDIT_LOG
        records, audit_raw, problem = audit_records(audit_path)
        if not check.check(problem is None and bool(records),
                           f"using the binding wrote an audit log at {audit_path}: {len(records)} record(s)"
                           + (f", problem {problem}" if problem else "")):
            return check.finish()
        uses = [record for record in records if record.get("event") == SECRET_USE_EVENT]
        observed = sorted({(record.get("environment_id"), record.get("machine_id"), record.get("binding"))
                           for record in uses})
        expected = [(environment_id, holder_machine_id, SECRET_BINDING)]
        check.check(observed == expected,
                    f"every {SECRET_USE_EVENT} record names exactly this Environment, Machine and binding: "
                    f"observed {observed}, expected {expected}")
        check.check(secret.encode() not in audit_raw,
                    f"the audit log carries the binding's identity and not its value ({len(audit_raw)} bytes)")

        # 6. Redaction, last, over everything the claims above produced.
        status_json = ctx.run(check, "sec-a-status-json", ["--json", "status"], cwd=holder["project"],
                              env=holder["env"], timeout=60)
        status_human = ctx.run(check, "sec-a-status-human", ["status"], cwd=holder["project"],
                               env=holder["env"], timeout=60)
        check.check(status_json.exit_code == 0 and status_human.exit_code == 0,
                    f"both status spellings answer (json exit {status_json.exit_code}, human exit "
                    f"{status_human.exit_code})")
        artifacts = [("status-json", Path("sec-a-status-json.stdout"), status_json.stdout),
                     ("status-json", Path("sec-a-status-json.stderr"), status_json.stderr),
                     ("status-human", Path("sec-a-status-human.stdout"), status_human.stdout),
                     ("status-human", Path("sec-a-status-human.stderr"), status_human.stderr)]
        logs = [path for path in regular_files(Path(holder["runtime"])) if path.suffix == ".log"]
        check.check(bool(logs), f"the Environment's daemon wrote a log under {holder['runtime']} "
                    f"(observed {[path.name for path in logs]})")
        skipped = []
        artifacts += sweep_group("daemon-log", logs, skipped)
        artifacts += sweep_group("audit-log", [audit_path], skipped)
        _rows, inventory_relative = write_inventory(ctx.evidence_dir, "secret-lane-state-root", ctx.state.root)
        check.evidence.append(inventory_relative)
        artifacts += sweep_group("state-root-inventory", [ctx.evidence_dir / inventory_relative], skipped)
        artifacts += sweep_group("state-root", regular_files(ctx.state.root), skipped)
        artifacts += sweep_group("evidence", regular_files(ctx.evidence_dir), skipped)
        check.check(not skipped, "every artifact the sweep addressed could be read (0 unreadable)" if not skipped
                    else f"{len(skipped)} artifact(s) could not be read, so the value was not looked for in them: "
                         + "; ".join(skipped[:4]))
        covered = sorted({label for label, _path, _data in artifacts})
        check.check(covered == sorted(SWEEP_GROUPS),
                    f"the sweep covers every declared artifact group (observed {covered}, declared "
                    f"{sorted(SWEEP_GROUPS)})")
        total = sum(len(data) for _label, _path, data in artifacts)
        check.check(len(artifacts) >= len(SWEEP_GROUPS) and total > 0,
                    f"the sweep read {len(artifacts)} artifacts totalling {total} bytes")
        hits = secret_occurrences(artifacts, secret)
        check.check(not hits, f"the secret value occurs 0 times across {len(artifacts)} artifacts in "
                    f"{len(covered)} groups" if not hits else
                    "the secret value leaked: " +
                    "; ".join(f"observed {count} in {path} ({label})" for label, path, count in hits[:6]))
    finally:
        # Unconditionally, including on failure: an Environment this check left
        # running holds a daemon inside a state root the phase is about to
        # inventory for leaks. What it needs kept is its receipts and its
        # assertions, and both are recorded by the time we get here.
        for name, instance in (("sec-a", holder), ("sec-b", sibling), ("sec-c", refused)):
            if instance is None or instance.get("status") is None:
                continue
            try:
                removed = ctx.run(check, name + "-delete",
                                  ["--json", "delete", "--environment", "default", "--timeout", "120"],
                                  cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
                check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")
            except OSError as error:
                check.fail(f"{name}: could not be deleted: {type(error).__name__}: {error}")
    return check.finish()


def snapshot_definition(release_dir: Path) -> dict:
    """One Developer Linux Machine that requests `snapshot` alongside exec."""
    definition = minimal_definition(release_dir)
    machine = definition["environment"]["machines"][0]
    machine["requested_capabilities"] = {"capabilities": [BASE_CAPABILITY, SNAPSHOT_CAPABILITY]}
    return definition


def snapshot_accounting(machine: dict) -> tuple:
    """(requested, granted, reason) for `snapshot` on one status Machine.

    `reason` is the runtime's own stable string from
    `negotiated_capabilities.unsupported`, which is where the contract type
    (`CapabilitySet`) says a request the backend could not negotiate is
    accounted. None means it accounted nothing, which is silence rather than an
    explicit unsupported capability.
    """
    requested = set(((machine.get("requested_capabilities") or {}).get("capabilities")) or [])
    negotiated = machine.get("negotiated_capabilities") or {}
    granted = SNAPSHOT_CAPABILITY in set(negotiated.get("capabilities") or [])
    reason = (negotiated.get("unsupported") or {}).get(SNAPSHOT_CAPABILITY)
    return requested, granted, reason


def probe_snapshot(ctx: CheckContext, check: SubCheck, label: str, instance: dict, argv: list):
    """One typed `vz-runtime-probe` call, recorded like any other observer.

    Snapshot and restore are not among the five public lifecycle verbs, so the
    typed API is where the contract puts them; the probe is the release's own
    typed client and criterion 15 already reads the daemon through it.
    """
    probe = ctx.release_dir / "bin" / PROBE
    return ctx.run_tool(check, label, [str(probe), *argv], cwd=instance["project"], env=instance["env"], timeout=120)


def check_snapshot_restore_capability(ctx: CheckContext, top: str) -> SubCheck:
    """Snapshot/restore passes where advertised, and is explicit where it is not.

    The capability matrix decides which branch is under test, and the two are
    deliberately never collapsed: a capability the matrix ADVERTISES and the
    runtime does not provide is a FAILURE, while one it does not advertise must
    come back as an explicit unsupported capability -- the request accounted
    against the capability itself, with the backend's own stable reason -- and
    that is a PASS. Granting an unadvertised capability, refusing without a
    machine-readable code, and saying nothing at all are three separate ways to
    fail the second branch, and each is asserted on its own.
    """
    check = SubCheck(top, "snapshot_restore_capability")
    matrix, host, pair = capability_pair(ctx.repo_root)
    if not check.check(pair is not None, f"{CAPABILITY_MATRIX} declares the pair under test "
                       f"(host {host!r} x target 'linux' x profile 'developer')"):
        return check.finish()
    status, _entry = capability_entry(pair, "machine_capabilities", SNAPSHOT_CAPABILITY)
    known = sorted(matrix.get("status_definitions") or {})
    if not check.check(status in known,
                       f"{host}/linux/developer advertises snapshot as {status!r} (one of {known})"):
        return check.finish()
    advertised = status in ADVERTISED_STATUSES
    try:
        definition = snapshot_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    if schema_path.is_file() and not schema_path.is_symlink():
        problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(definition),
                          key=lambda e: list(map(str, e.absolute_path)))
        if not check.check(not problems, "a Machine may request the snapshot capability in a valid definition"
                           if not problems else f"definition invalid: {problems[0].message[:200]}"):
            return check.finish()
    instance = None
    try:
        instance = up_reporting_envelope(ctx, check, "snap-a", definition)
        if instance["exit_code"] != 0:
            # Refusing the Up is a legitimate way to be explicit, so long as the
            # refusal is machine-readable and names the capability it refused.
            envelope, code = instance["envelope"], instance["code"]
            if advertised:
                check.fail(f"{CAPABILITY_MATRIX} advertises snapshot as {status} for {host}/linux/developer, but Up "
                           f"refused a Machine that requested it: exit {instance['exit_code']}, error.code "
                           f"{code!r}, {instance['message'][:200]!r}")
                return check.finish()
            check.check(isinstance(code, str) and bool(code.strip()),
                        f"the refusal is a structured error envelope: error.code {code!r} (expected a "
                        "machine-readable code, not a bare nonzero exit)")
            check.check(SNAPSHOT_CAPABILITY in json.dumps(envelope or {}),
                        f"the refusal names the {SNAPSHOT_CAPABILITY!r} capability it could not provide "
                        f"(envelope {json.dumps(envelope or {})[:200]!r})")
            return check.finish()
        payload = instance["status"]
        if not check.check(payload is not None,
                           "the Environment declaring the capability reports a readable status"):
            return check.finish()
        machines = {machine.get("name"): machine for environment in payload.get("environments") or []
                    for machine in environment.get("machines") or []}
        machine = machines.get("machine-0")
        if not check.check(machine is not None, f"machine-0 is present (observed {sorted(machines)})"):
            return check.finish()
        requested, granted, reason = snapshot_accounting(machine)
        declared = {BASE_CAPABILITY, SNAPSHOT_CAPABILITY}
        if requested != declared:
            # Without the request in the runtime's own account of the Machine
            # there is nothing for it to be explicit about, and whether the CLI
            # republishes what was declared is criterion 15's claim, not this
            # one. Reported with both sets rather than passed over.
            check.not_implemented = (
                "this runtime does not project the Machine's declared capability request, so the snapshot "
                f"advertisement clause has no subject: declared {sorted(declared)}, reported "
                f"{sorted(requested)}. Whether the CLI republishes a declared request is criterion 15's claim.")
            return check.finish()
        check.ok(f"the Machine's request is projected exactly as declared ({sorted(requested)})")
        if not advertised:
            check.check(not granted,
                        f"{host}/linux/developer does not advertise snapshot ({status}), and the Machine did not "
                        f"negotiate it (granted {granted})")
            check.check(isinstance(reason, str) and bool(reason.strip()),
                        "the runtime accounts the request as an EXPLICIT unsupported capability: "
                        f"negotiated_capabilities.unsupported[{SNAPSHOT_CAPABILITY!r}] = {reason!r} "
                        "(expected a stable non-empty reason, not silence)")
            return check.finish()
        if not check.check(granted, f"{CAPABILITY_MATRIX} advertises snapshot as {status} for "
                           f"{host}/linux/developer, and the Machine negotiated it (granted {granted}, "
                           f"unsupported reason {reason!r})"):
            return check.finish()
        probe = ctx.release_dir / "bin" / PROBE
        if not check.check(probe.is_file() and not probe.is_symlink(),
                           f"the release ships bin/{PROBE}, the typed client snapshot/restore is reached through"):
            return check.finish()
        taken = probe_snapshot(ctx, check, "snap-a-snapshot", instance,
                               ["snapshot", "--socket", instance["env"]["VZ_RUNTIME_DAEMON_SOCKET"],
                                "--environment", "default", "--machine", "machine-0"])
        documents = probe_documents(taken) if taken.exit_code == 0 else []
        snapshot_id = documents[-1].get("snapshot_id") if documents else None
        if not check.check(taken.exit_code == 0 and isinstance(snapshot_id, str) and bool(snapshot_id),
                           f"snapshot returns an identity (exit {taken.exit_code}, snapshot_id {snapshot_id!r})"):
            return check.finish()
        token = "vzsnap-" + uuid.uuid4().hex[:16]
        written = machine_exec(ctx, check, "snap-a-write-after-snapshot", instance, "machine-0",
                               f"printf %s {token} > {SNAPSHOT_SENTINEL_PATH}; printf ':%s' $?")
        if not check.check(written.stdout.strip().endswith(b":0"),
                           f"a sentinel is written between snapshot and restore (observed {written.stdout[:60]!r})"):
            return check.finish()
        before = machine_exec(ctx, check, "snap-a-read-before-restore", instance, "machine-0",
                              f"/bin/busybox cat {SNAPSHOT_SENTINEL_PATH}")
        if not check.check(before.stdout.strip() == token.encode(),
                           f"the sentinel is readable before restore (observed {before.stdout[:60]!r}, expected "
                           f"{token!r})"):
            return check.finish()
        restored = probe_snapshot(ctx, check, "snap-a-restore", instance,
                                  ["restore", "--socket", instance["env"]["VZ_RUNTIME_DAEMON_SOCKET"],
                                   "--environment", "default", "--machine", "machine-0",
                                   "--snapshot-id", snapshot_id])
        if not check.check(restored.exit_code == 0, f"restore of {snapshot_id!r} succeeds (exit "
                           f"{restored.exit_code})"):
            return check.finish()
        after = machine_exec(ctx, check, "snap-a-read-after-restore", instance, "machine-0",
                             f"/bin/busybox cat {SNAPSHOT_SENTINEL_PATH}")
        check.check(after.stdout.strip() != token.encode(),
                    f"restore rewound the Machine past the sentinel written after the snapshot (observed "
                    f"{after.stdout[:60]!r}, which must not be {token!r})")
    finally:
        if instance is not None and instance.get("status") is not None:
            try:
                removed = ctx.run(check, "snap-a-delete",
                                  ["--json", "delete", "--environment", "default", "--timeout", "120"],
                                  cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
                check.check(removed.exit_code == 0, f"snap-a: deleted (exit {removed.exit_code})")
            except OSError as error:
                check.fail(f"snap-a: could not be deleted: {type(error).__name__}: {error}")
    return check.finish()


# --------------------------------------------------------------------- criterion 20
#
# `gate.network.exhaustive_denial_matrix`: one machine-readable
# source x destination x protocol x port matrix, enumerated BEFORE anything is
# probed, executed cell by cell, then compared cell by cell against the
# expectation each cell was declared with.
#
# This criterion is different in kind from its neighbours. Their deliverable is
# a set of claims; this one's deliverable is an artifact -- a table whose row
# count, destination classes and protocols are declared up front, so a probe
# loop that quietly produced nothing cannot pass as a matrix that found nothing
# wrong. Every row is emitted into the lane evidence as
# `connectivity-matrix.json` under the checked-in
# `schemas/vz-0.4-connectivity-matrix.schema.json` -- the schema the gate's
# "Versioned gate inputs" section already requires and that
# `vz04_schema.EVIDENCE_SCHEMAS` already loads -- so the aggregate validator
# re-reads and re-validates the same table this check graded.
#
# The grading is deliberately asymmetric, exactly as the criterion states it:
#
#   * a cell declared `deny` that was observed `allow` is an UNEXPECTED SUCCESS
#     and fails the gate, naming the cell;
#   * a cell declared `allow` that was not observed `allow` is a failure too,
#     but a different one, and is reported separately so the evidence says which
#     kind happened;
#   * a cell that answered without carrying its destination's own token is
#     INDETERMINATE -- neither reached nor refused -- and is reported as a third
#     kind rather than rounded into either.
#
# A cell whose Environment, address or guest tool does not exist is not dropped
# from the table. It is recorded `error` with the reason its resource was
# unavailable, and the check reports `not_implemented` in the runtime's own
# words. Shrinking the matrix to the cells that happen to work on this runtime
# would turn the artifact into a description of the runtime instead of a
# description of the criterion.
DENIAL_MATRIX_EVIDENCE = "connectivity-matrix.json"
DENIAL_MATRIX_KIND = "vz-0.4-connectivity-matrix"
DENIAL_MATRIX_SCHEMA = "schemas/vz-0.4-connectivity-matrix.schema.json"
# 1.1.1.1 and 8.8.8.8 rather than a documentation range: "offline" has to mean
# the real Internet, not an address that would have failed anyway.
MATRIX_INTERNET_ADDRESSES = ("1.1.1.1", "8.8.8.8")
MATRIX_INTERNET_PORTS = (443, 80)
MATRIX_INTERNET_NAME = "vz04-egress-probe.invalid"
# RFC 5737 documentation ranges: one inside the CIDR a CIDR policy would allow
# and one outside it, so the policy's own boundary is what decides the cell.
MATRIX_CIDR_ALLOW = "203.0.113.0/24"
MATRIX_CIDR_INSIDE = "203.0.113.10"
MATRIX_CIDR_OUTSIDE = "198.51.100.10"
MATRIX_DOMAIN_ALLOWED = "allowed.one.test"
MATRIX_DOMAIN_BLOCKED = "blocked.two.test"
# The control plane, addressed the way a Machine would have to address it. vz's
# daemon and every Machine's Docker endpoint are AF_UNIX sockets, so the only
# control-plane surface a guest could name over IP is a Docker Engine on TCP --
# which the contract forbids outright ("no Environment-wide/global socket or
# fallback daemon"). Both the plaintext and the TLS port are probed, on the host
# loopback the guest sees and on the shared NAT gateway alias.
MATRIX_CONTROL_PLANE_PORTS = (2375, 2376)
MATRIX_PROBE_TIMEOUT = 300
MATRIX_HOST_PROBE_TIMEOUT = 60
# The guest applet each probe kind needs. A Machine whose image lacks one
# reports those cells unexercised instead of reporting a missing applet's exit
# status as a routing fact -- the confusion criterion 7's `nc` clause already
# had to name.
MATRIX_PROBE_APPLETS = {"tcp": "wget", "udp": "nc", "icmp": "ping", "dns": "nslookup"}
# Every destination class the criterion enumerates, plus the two clauses it
# states separately (egress-attachment cross-talk, and ICMP where the runtime
# exposes it). Compared as a set against the classes the built matrix actually
# carries, so a class that silently stopped being enumerated fails the check.
DENIAL_MATRIX_CLASSES = (
    "control_plane",
    "egress_attachment_crosstalk",
    "host_export_declared",
    "host_export_undeclared",
    "host_import_declared",
    "host_import_undeclared",
    "icmp_unsolicited",
    "internet_allowed",
    "internet_cidr",
    "internet_domain",
    "internet_offline",
    "lan",
    "private_cross_environment",
    "private_in_environment",
    "public_like_cross_environment",
    "public_like_ingress",
    "public_like_undeclared",
)
# The row count of the enumeration on a host reporting no non-loopback address
# of its own. Each such address adds cells, so this is a floor rather than an
# equality -- but a floor with teeth: an enumeration that lost a whole class, or
# a source, drops below it.
DENIAL_MATRIX_MINIMUM_ROWS = 104
# The exact `detail` a cell still carries when nothing ran it. Compared rather
# than inferred from `observed == "error"`, because a cell that DID run and
# answered without its destination's token is also `error` and must not be
# counted as unexercised.
MATRIX_NOT_EXECUTED = "not executed"


class MatrixCell:
    """One declared source x destination x protocol x port cell.

    `expected` is fixed at enumeration time from the set of DECLARED
    authorizations and never from what was observed. `observed` starts at
    `error` and is only ever moved by a probe that ran, so a cell nothing
    executed cannot read as a denial that held.
    """

    def __init__(self, klass, source, destination, protocol, port, expected, *, phase, probe, target,
                 token=None, requires=(), ca_file=None):
        self.klass = klass
        self.source = source
        self.destination = destination
        self.protocol = protocol
        self.port = port
        self.expected = expected
        self.phase = phase
        self.probe = probe
        self.target = target
        self.token = token
        self.requires = tuple(requires)
        self.ca_file = ca_file
        self.index = None
        self.observed = "error"
        self.detail = MATRIX_NOT_EXECUTED

    @property
    def key(self) -> tuple:
        return (self.source, self.destination, self.protocol, self.port)

    def row(self) -> dict:
        """Exactly the seven fields the connectivity-matrix schema declares."""
        return {"source": self.source, "destination": self.destination, "protocol": self.protocol,
                "port": self.port, "expected": self.expected, "observed": self.observed,
                "match": self.observed == self.expected}

    def label(self) -> str:
        port = "-" if self.port is None else self.port
        return (f"cell ({self.source} -> {self.destination}, {self.protocol}/{port}) expected "
                f"{self.expected}, observed {self.observed} ({self.detail})")


def matrix_source_parts(source: str) -> tuple:
    isolate, _, machine = source.partition("/")
    return isolate, machine


def enumerate_denial_matrix(plan: dict) -> list:
    """The whole declared matrix, as data, before a single probe has run.

    Built as a cartesian product per destination class rather than as a list of
    interesting paths. The host-import block is the clearest case: it is every
    (source, host loopback port, protocol) triple this check can form, and
    `expected` is `allow` for exactly the tuples in `plan["grants"]` and `deny`
    for everything else -- so the denials are not chosen one by one, they are
    what is left once the declared grants are removed from the product.
    """
    cells = []
    grants = set(plan["grants"])

    def add(klass, source, destination, protocol, port, expected, **kwargs):
        cells.append(MatrixCell(klass, source, destination, protocol, port, expected, **kwargs))

    private_dst = f"{plan['private_endpoint']}@{plan['private_address'] or 'unresolved'}"
    # 1. Private in-Environment paths, and the very same address from outside.
    for source in plan["grant_machines"]:
        add("private_in_environment", source, private_dst, "tcp", PRIVATE_PORT, "allow",
            phase="served", probe="tcp", target=plan["private_address"], token=plan["private_token"],
            requires=("src:" + matrix_source_parts(source)[0], "dst:private"))
    for source in plan["foreign_machines"]:
        add("private_cross_environment", source, private_dst, "tcp", PRIVATE_PORT, "deny",
            phase="served", probe="tcp", target=plan["private_address"], token=plan["private_token"],
            requires=("src:" + matrix_source_parts(source)[0], "dst:private"))

    # 2. Public-like ingress: the declared name, an undeclared name on the same
    #    edge, and the declared name from three foreign Environments.
    for name, klass, sources, expected in (
            (plan["edge_name"], "public_like_ingress", plan["edge_machines"], "allow"),
            (plan["edge_undeclared_name"], "public_like_undeclared", plan["edge_machines"], "deny"),
            (plan["edge_name"], "public_like_cross_environment", plan["foreign_machines"], "deny")):
        for source in sources:
            for protocol, port, probe in (("dns", 53, "dns"), ("https", 443, "https")):
                add(klass, source, f"edge:{name}", protocol, port, expected,
                    phase="edge", probe=probe, target=name,
                    token=None if probe == "dns" else plan["edge_token"],
                    requires=("src:" + matrix_source_parts(source)[0], "dst:edge"),
                    ca_file=plan["edge_anchors"].get(source) if probe == "https" else None)

    # 3. Host imports. Every source x every host loopback port x every protocol;
    #    `allow` only where a declared grant names that exact tuple.
    for source in plan["import_machines"]:
        for destination, port, token in plan["host_ports"]:
            for protocol in ("tcp", "udp"):
                expected = "allow" if (source, destination, protocol, port) in grants else "deny"
                klass = "host_import_declared" if expected == "allow" else "host_import_undeclared"
                add(klass, source, destination, protocol, port, expected,
                    phase="open", probe=protocol, target="127.0.0.1", token=token,
                    requires=("src:" + matrix_source_parts(source)[0], "dst:host-import"))

    # 4. Host exports, probed from the host: the declared loopback port, an
    #    undeclared loopback port, and the declared port on every non-loopback
    #    address this host holds -- "exports never expose the LAN by accident".
    add("host_export_declared", "host", "host-export:127.0.0.1", "tcp", plan["export_host_port"], "allow",
        phase="served", probe="host_tcp", target="127.0.0.1", token=plan["private_token"],
        requires=("dst:host-export",))
    add("host_export_undeclared", "host", "host-export:127.0.0.1", "tcp", plan["undeclared_export_port"], "deny",
        phase="served", probe="host_tcp", target="127.0.0.1", token=plan["private_token"],
        requires=("dst:host-export",))
    for address in plan["lan_addresses"]:
        add("host_export_undeclared", "host", f"host-export:{address}", "tcp", plan["export_host_port"], "deny",
            phase="served", probe="host_tcp", target=address, token=plan["private_token"],
            requires=("dst:host-export",))

    # 5. Internet under offline policy. Every Machine here declares `offline`,
    #    and none of them may reach anything off this host.
    for source in plan["offline_machines"]:
        for address in MATRIX_INTERNET_ADDRESSES:
            for port in MATRIX_INTERNET_PORTS:
                add("internet_offline", source, f"internet:{address}", "tcp", port, "deny",
                    phase="open", probe="tcp", target=address,
                    requires=("src:" + matrix_source_parts(source)[0],))
        add("internet_offline", source, f"internet:{MATRIX_INTERNET_NAME}", "dns", 53, "deny",
            phase="open", probe="dns", target=MATRIX_INTERNET_NAME,
            requires=("src:" + matrix_source_parts(source)[0],))

    # 6. Internet under allowed / CIDR / domain policy, and the criterion's
    #    separate clause: two Machines of ONE Environment on DIFFERENT egress
    #    attachments, neither governing the other's policy or host import.
    permissive, restricted = plan["egress_machines"]
    add("internet_allowed", permissive, f"internet:{MATRIX_INTERNET_ADDRESSES[0]}", "tcp", 443, "allow",
        phase="open", probe="tcp", target=MATRIX_INTERNET_ADDRESSES[0], requires=("src:dm-egress",))
    add("egress_attachment_crosstalk", restricted, f"internet:{MATRIX_INTERNET_ADDRESSES[0]}", "tcp", 443, "deny",
        phase="open", probe="tcp", target=MATRIX_INTERNET_ADDRESSES[0], requires=("src:dm-egress",))
    add("egress_attachment_crosstalk", permissive, f"host-loopback:{GRANTED_GUEST_PORT}", "tcp",
        GRANTED_GUEST_PORT, "allow", phase="open", probe="tcp", target="127.0.0.1",
        token=plan["host_service_token"], requires=("src:dm-egress",))
    add("egress_attachment_crosstalk", restricted, f"host-loopback:{GRANTED_GUEST_PORT}", "tcp",
        GRANTED_GUEST_PORT, "deny", phase="open", probe="tcp", target="127.0.0.1",
        token=plan["host_service_token"], requires=("src:dm-egress",))
    for address, expected in ((MATRIX_CIDR_INSIDE, "allow"), (MATRIX_CIDR_OUTSIDE, "deny")):
        add("internet_cidr", plan["cidr_machine"], f"internet:{address}", "tcp", 443, expected,
            phase="open", probe="tcp", target=address, requires=("src:dm-cidr",))
    for name, expected in ((MATRIX_DOMAIN_ALLOWED, "allow"), (MATRIX_DOMAIN_BLOCKED, "deny")):
        for protocol, port, probe in (("dns", 53, "dns"), ("https", 443, "https")):
            add("internet_domain", plan["domain_machine"], f"internet:{name}", protocol, port, expected,
                phase="open", probe=probe, target=name, requires=("src:dm-domain",))

    # 7. LAN. The NAT alias the contract names explicitly, plus every
    #    non-loopback address this host actually holds, on the two host ports
    #    this check put in play. Probed while the export listener is LIVE, so a
    #    cell that succeeded is a real leak and not an absent listener.
    for source in plan["lan_machines"]:
        for address in (NAT_GATEWAY_ADDRESS, *plan["lan_addresses"]):
            for port, token in ((plan["host_service_port"], plan["host_service_token"]),
                                (plan["export_host_port"], plan["private_token"])):
                add("lan", source, f"lan:{address}", "tcp", port, "deny",
                    phase="served", probe="tcp", target=address, token=token,
                    requires=("src:" + matrix_source_parts(source)[0], "dst:host-export"))

    # 8. Control plane, and unsolicited ICMP.
    for source in plan["lan_machines"]:
        for address in ("127.0.0.1", NAT_GATEWAY_ADDRESS):
            for port in MATRIX_CONTROL_PLANE_PORTS:
                add("control_plane", source, f"control-plane:{address}", "tcp", port, "deny",
                    phase="open", probe="tcp", target=address,
                    requires=("src:" + matrix_source_parts(source)[0],))
        for address in (NAT_GATEWAY_ADDRESS, MATRIX_INTERNET_ADDRESSES[0]):
            add("icmp_unsolicited", source, f"icmp:{address}", "icmp", None, "deny",
                phase="open", probe="icmp", target=address,
                requires=("src:" + matrix_source_parts(source)[0],))
    for index, cell in enumerate(cells):
        cell.index = index
    return cells


def matrix_probe_command(cell: MatrixCell) -> str:
    """The one command this cell is, inside the Machine (or the URL, on the host)."""
    if cell.probe == "host_tcp":
        return f"http://{cell.target}:{cell.port}/"
    if cell.probe == "tcp":
        return f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{cell.target}:{cell.port}/"
    if cell.probe == "udp":
        return f"printf probe | /bin/busybox nc -u -w 2 {cell.target} {cell.port}"
    if cell.probe == "icmp":
        return f"/bin/busybox ping -c 1 -W 2 {cell.target}"
    if cell.probe == "dns":
        return f"/bin/busybox nslookup {cell.target}"
    anchor = f" --ca-file {cell.ca_file}" if cell.ca_file else ""
    return (f"{GUEST_FETCH} get --url https://{cell.target}/{anchor} "
            f"--timeout-millis {FETCH_TIMEOUT_MILLIS}")


# Read from each Machine before any of its cells is graded. A clause needing an
# applet this image lacks has to be able to see that it lacks it: the exit
# status of a missing applet says "denied" and means nothing.
MATRIX_PREFLIGHT = ('/bin/busybox --list | /bin/busybox awk \'{print "APPLET", $0}\'; '
                    f'if [ -x {GUEST_FETCH} ]; then printf "FETCH yes\\n"; else printf "FETCH no\\n"; fi')


def parse_matrix_preflight(receipt) -> tuple:
    """(the applets this BusyBox carries, whether the HTTPS client is present)."""
    applets, fetch = set(), False
    for line in receipt.stdout.decode("utf-8", "replace").splitlines():
        fields = line.split()
        if fields[:1] == ["APPLET"] and len(fields) == 2:
            applets.add(fields[1])
        elif fields[:2] == ["FETCH", "yes"]:
            fetch = True
    return applets, fetch


def matrix_probe_script(cells: list) -> str:
    """One script running every cell of one (phase, source), reporting each.

    Batched per source rather than one `vz exec` per cell: a hundred-cell matrix
    is otherwise a hundred Machine invocations, and the invocation is not what
    is under test. Each cell is still its own separate command inside the guest
    and still reports its own exit status and its own bytes, which is all the
    grading reads.
    """
    lines = []
    for cell in cells:
        lines.append(f'o=$({matrix_probe_command(cell)} 2>/dev/null); c=$?; '
                     f'printf "CELL {cell.index} %s " "$c"; '
                     'printf "%s" "$o" | /bin/busybox awk \'{printf "%s ", substr($0, 1, 120)}\'; '
                     'printf "\\n"')
    return "\n".join(lines)


def parse_matrix_probe(receipt) -> dict:
    """{cell index: (exit status, the bytes that cell was answered with)}."""
    results = {}
    for line in receipt.stdout.decode("utf-8", "replace").splitlines():
        fields = line.split(" ", 3)
        if fields[:1] != ["CELL"] or len(fields) < 3 or not fields[1].isdigit():
            continue
        status = fields[2].strip()
        if not status.lstrip("-").isdigit():
            continue
        results[int(fields[1])] = (int(status), fields[3] if len(fields) > 3 else "")
    return results


def observe_matrix_cell(cell: MatrixCell, status, output: str) -> tuple:
    """(observed, detail) for one cell, from that cell's own two streams.

    Three outcomes, not two. A nonzero status is a refusal. A zero status
    carrying the destination's own token is a reach. A zero status WITHOUT that
    token is neither: something answered and it was not the destination this
    cell names. That is recorded `error` rather than rounded into `allow` (it
    would manufacture an unexpected success) or into `deny` (it would hide one)
    -- the same distinction criterion 7's `denied` makes, kept as a third value
    instead of folded away.
    """
    if status is None:
        return "error", "the probe reported no exit status"
    if status != 0:
        return "deny", f"exit {status}"
    if cell.token is None:
        return "allow", f"exit 0, answered {output.strip()[:80]!r}"
    if cell.token in output:
        return "allow", f"exit 0 carrying {cell.token}"
    return "error", (f"exit 0 without the destination's token {cell.token} "
                     f"(answered {output.strip()[:80]!r})")


def denial_matrix_findings(cells: list) -> dict:
    """The three kinds of disagreement, kept apart.

    `unexpected_success` is the criterion's own hard failure and is listed
    first; `unmet_allow` is the opposite direction and is a different fact about
    the runtime; `indeterminate` is a cell whose probe answered without proving
    who answered it. Pure, so a test can hand it a wrong observation directly
    and read the finding back.
    """
    executed = [cell for cell in cells if cell.detail != MATRIX_NOT_EXECUTED]
    return {
        "executed": executed,
        "unexecuted": [cell for cell in cells if cell.detail == MATRIX_NOT_EXECUTED],
        "unexpected_success": [cell for cell in executed if cell.expected == "deny" and cell.observed == "allow"],
        "unmet_allow": [cell for cell in executed if cell.expected == "allow" and cell.observed == "deny"],
        "indeterminate": [cell for cell in executed if cell.observed == "error"],
    }


def egress_attachment_findings(cells: list) -> list:
    """The criterion's separate egress-attachment claim, as compared values.

    Two Machines of ONE Environment on DIFFERENT egress attachments: the
    permissive Machine's Internet policy must not govern its sibling, and the
    permissive Machine's host import must not be reachable from that sibling.
    Both are stated as a comparison between two observed values rather than as
    "both probes did something": two Machines that both reached the Internet and
    two that both failed are indistinguishable under a per-probe test, and it is
    exactly that difference the clause exists to establish.
    """
    relevant = {(cell.source, cell.destination): cell for cell in cells
                if cell.klass in ("internet_allowed", "egress_attachment_crosstalk")}
    sources = sorted({source for source, _destination in relevant})
    if len(sources) != 2:
        return [f"the egress-attachment clause needs exactly two Machines of one Environment "
                f"(the matrix carries {sources})"]
    permissive, restricted = sources
    findings = []
    for what, destination, want_permissive, want_restricted in (
            ("Internet policy", f"internet:{MATRIX_INTERNET_ADDRESSES[0]}", "allow", "deny"),
            ("host import", f"host-loopback:{GRANTED_GUEST_PORT}", "allow", "deny")):
        first = relevant.get((permissive, destination))
        second = relevant.get((restricted, destination))
        if first is None or second is None:
            findings.append(f"the {what} cell is missing for {permissive} or for {restricted}")
            continue
        if (first.observed, second.observed) != (want_permissive, want_restricted):
            findings.append(
                f"{what} at {destination}: {permissive} observed {first.observed} and {restricted} observed "
                f"{second.observed} (expected {want_permissive} and {want_restricted}); one Machine's egress "
                "attachment governs the other's")
    return findings


def denial_matrix_document(run_id: str, scenario_id: str, cells: list) -> dict:
    return {"schema_version": 1, "kind": DENIAL_MATRIX_KIND, "run_id": run_id, "scenario_id": scenario_id,
            "rows": [cell.row() for cell in cells]}


def matrix_definition(release_dir: Path, *, host_port: int, export_host_port: int) -> dict:
    """The Environment the private, host-import and host-export cells measure.

    Two Developer Linux Machines on one declared private network, with the
    import and the export granted to the FIRST Machine only. The second exists
    so "the wrong Machine is denied" is a claim about a real sibling in the same
    Environment rather than about a Machine that does not exist.
    """
    definition = two_machine_definition(release_dir)
    environment = definition["environment"]
    first = environment["machines"][0]
    environment["host_imports"] = [{
        "schema_version": 1, "name": "hostsvc", "machine": first["name"], "protocol": "tcp",
        "host_port": host_port, "guest_port": GRANTED_GUEST_PORT}]
    environment["host_exports"] = [{
        "schema_version": 1, "name": "api", "machine": first["name"], "protocol": "tcp",
        "machine_port": EXPORT_MACHINE_PORT, "host_port": export_host_port}]
    return definition


def egress_definition(release_dir: Path, *, host_port: int, policy: str) -> dict:
    """Two Machines of one Environment on DIFFERENT egress attachments.

    machine-0 takes the permissive attachment and the one host import;
    machine-1 stays offline and is granted nothing. `policy` is `allowed` for
    the enum the shipped project schema declares, and the CIDR/domain spellings
    for the two the criterion names and the schema does not -- those definitions
    are built anyway, so the reason a CIDR or domain Internet policy cannot be
    exercised is the SCHEMA's own words rather than this check's opinion.
    """
    definition = minimal_definition(release_dir)
    environment = definition["environment"]
    first = environment["machines"][0]
    second = copy.deepcopy(first)
    second["name"] = "machine-1"
    second["egress"] = "offline"
    if policy == "allowed":
        first["egress"] = "allowed"
    elif policy == "cidr":
        first["egress"] = {"policy": "cidr", "allow": [MATRIX_CIDR_ALLOW]}
    else:
        first["egress"] = {"policy": "domain", "allow": [MATRIX_DOMAIN_ALLOWED]}
    environment["machines"] = [first, second]
    environment["host_imports"] = [{
        "schema_version": 1, "name": "hostsvc", "machine": first["name"], "protocol": "tcp",
        "host_port": host_port, "guest_port": GRANTED_GUEST_PORT}]
    return definition


def matrix_tool_requirement(cell: MatrixCell):
    """The resource id for the guest tool this cell needs, or None on the host."""
    if cell.probe == "host_tcp":
        return None
    return f"tool:{cell.source}:{MATRIX_PROBE_APPLETS.get(cell.probe, GUEST_FETCH)}"


def check_exhaustive_denial_matrix(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """Criterion 20: the whole declared matrix, executed and graded cell by cell.

    The order is: declare, then build the subjects, then probe, then compare.
    Declaring first is the point -- the enumeration's length, its destination
    classes and its protocols are asserted BEFORE any probe runs, so a runtime
    that refused everything cannot pass by producing an empty table, and a probe
    loop that silently emitted no rows cannot pass by producing a table with
    nothing to disagree with.

    The subjects are: the three Environments pre-sleep left running (used as
    foreign sources, which is what makes every cross-Environment denial a claim
    about a live Environment rather than an absent one), one Environment with a
    declared private path plus a declared host import and export, one with a
    public-like edge, and three that declare the allowed/CIDR/domain egress
    policies the criterion names. Whatever this runtime refuses is recorded as
    unexercised in the refusing component's own words -- the runtime's for a
    refused Up, the shipped project schema's for a declaration it cannot express
    -- and the cells stay in the table as `error` rather than being dropped.
    """
    from vz04_common import GateError

    check = SubCheck(top, "exhaustive_denial_matrix")
    granted_service = foil_service = None
    try:
        granted_service = LoopbackHostService("vzmtxsvc-" + uuid.uuid4().hex[:16])
        # Never declared to anybody, so "the guest cannot choose a host
        # destination" is measured against a service that really is listening.
        foil_service = LoopbackHostService("vzmtxfoil-" + uuid.uuid4().hex[:16])
    except OSError as error:
        if granted_service is not None:
            granted_service.close()
        check.fail(f"cannot bind a loopback host service for the matrix to terminate against: {error}")
        return check.finish()

    export_host_port, undeclared_export_port = free_host_port(), free_host_port()
    private_token = "vzmtxpriv-" + uuid.uuid4().hex[:16]
    edge_token = "vzmtxedge-" + uuid.uuid4().hex[:16]
    unavailable, instances, provisioned = {}, {}, []
    schema_path = ctx.repo_root / PROJECT_DEFINITION_SCHEMA
    matrix_path = ctx.repo_root / DENIAL_MATRIX_SCHEMA

    def refuse(reason: str, *resources) -> None:
        for resource in resources:
            unavailable.setdefault(resource, reason)

    def invalid(definition: dict):
        """The shipped project schema's own first complaint, or None."""
        if not schema_path.is_file():
            return None
        problems = sorted(Draft202012Validator(load_json(schema_path)).iter_errors(definition),
                          key=lambda error: list(map(str, error.absolute_path)))
        return problems[0].message[:220] if problems else None

    try:
        # -- the subjects -------------------------------------------------------
        # 1. The Environments pre-sleep left running. Foreign sources for every
        #    cross-Environment cell, and offline sources for the Internet cells.
        for entry in established.get("environments") or []:
            name = entry["isolate"]
            try:
                instances[name] = ctx.reattach(name)
            except ReattachError as error:
                refuse(str(error)[:220], "src:" + name)
        recovered = sorted(instances)
        check.check(len(recovered) == 3,
                    f"the three Environments pre-sleep left running are addressable as matrix sources "
                    f"(observed {recovered})")

        # 2. The granted Environment: a declared private path, one declared host
        #    import on machine-0, one declared host export on machine-0.
        grant_definition = matrix_definition(ctx.release_dir, host_port=granted_service.port,
                                             export_host_port=export_host_port)
        problem = invalid(grant_definition)
        if problem:
            refuse("the shipped project schema rejects the granted-Environment definition: " + problem,
                   "src:dm-grant", "dst:private", "dst:host-import", "dst:host-export")
        else:
            grant = provision(ctx, check, "dm-grant", grant_definition)
            instances["dm-grant"] = grant
            if grant.get("unsupported") or grant["status"] is None:
                refuse("this runtime does not apply the declared private network, host import and host export "
                       "the granted Environment declares: " + (grant.get("unsupported") or "vz up left no status")[:220],
                       "src:dm-grant", "dst:private", "dst:host-import", "dst:host-export")
            else:
                provisioned.append("dm-grant")

        # 3. The public-like edge.
        edge_definition = public_like_definition(ctx.release_dir, PUBLIC_NAMES[0])
        problem = invalid(edge_definition)
        if problem:
            refuse("the shipped project schema rejects the public-like definition: " + problem,
                   "src:dm-edge", "dst:edge")
        else:
            edge = provision(ctx, check, "dm-edge", edge_definition)
            instances["dm-edge"] = edge
            if edge.get("unsupported") or edge["status"] is None:
                refuse("this runtime does not apply the declared public-like network: " +
                       (edge.get("unsupported") or "vz up left no status")[:220], "src:dm-edge", "dst:edge")
            else:
                provisioned.append("dm-edge")

        # 4. The three egress policies the criterion names. `allowed` is in the
        #    shipped enum, so its refusal (if any) is the runtime's; `cidr` and
        #    `domain` are not, so their refusal is the schema's. Both are
        #    recorded verbatim -- the criterion asks for these cells, and the
        #    honest answer to "why is this cell blank" is whose rule blanked it.
        for isolate, policy in (("dm-egress", "allowed"), ("dm-cidr", "cidr"), ("dm-domain", "domain")):
            definition = egress_definition(ctx.release_dir, host_port=granted_service.port, policy=policy)
            problem = invalid(definition)
            if problem:
                refuse(f"the shipped project schema cannot express a {policy} Internet policy: " + problem,
                       "src:" + isolate)
                continue
            attempt = attempt_up(ctx, check, isolate, definition, timeout=DELETE_TIMEOUT)
            instances[isolate] = attempt
            if attempt["exit_code"] != 0 or attempt["status"] is None:
                refuse(f"this runtime refuses a Machine with a {policy} egress attachment: " +
                       (attempt["message"] or "vz up failed without naming a reason")[:220], "src:" + isolate)
            else:
                provisioned.append(isolate)

        # -- the addresses the cells name --------------------------------------
        private_address = None
        if "dst:private" not in unavailable:
            probed = machine_exec(ctx, check, "dm-grant-fabric", instances["dm-grant"], "machine-0", FABRIC_PROBE)
            port = FabricState(probed).port()
            private_address = port["address"] if port else None
            if private_address is None:
                refuse("the granted Environment's machine-0 carries no fabric address the host derived "
                       f"({FabricState(probed).evidence()[:200]})", "dst:private")
        lan_addresses = [a for a in host_non_loopback_addresses(ctx, check) if a != NAT_GATEWAY_ADDRESS][:2]
        check.ok(f"non-loopback host addresses the LAN and export cells name: {lan_addresses}")

        edge_anchors = {}
        if "dst:edge" not in unavailable:
            published = edge_anchor(instances["dm-edge"])
            if len(published) != 1:
                refuse(f"the public-like Environment published {len(published)} authorities, not one", "dst:edge")
            else:
                pem = read_regular(published[0])
                for machine in ("machine-0", "machine-1"):
                    edge_anchors[f"dm-edge/{machine}"] = install_anchor(
                        ctx, check, f"dm-edge-anchor-{machine}", instances["dm-edge"], machine, "own.pem", pem)

        # -- the declaration ----------------------------------------------------
        grant_machines = ["dm-grant/machine-0", "dm-grant/machine-1"]
        foreign_machines = [f"{name}/machine-0" for name in ("rec-a", "rec-b", "rec-c")]
        plan = {
            "grants": {("dm-grant/machine-0", f"host-loopback:{GRANTED_GUEST_PORT}", "tcp", GRANTED_GUEST_PORT)},
            "grant_machines": grant_machines,
            "foreign_machines": foreign_machines,
            "edge_machines": ["dm-edge/machine-0", "dm-edge/machine-1"],
            "egress_machines": ["dm-egress/machine-0", "dm-egress/machine-1"],
            "cidr_machine": "dm-cidr/machine-0",
            "domain_machine": "dm-domain/machine-0",
            "import_machines": [*grant_machines, "rec-a/machine-0"],
            "offline_machines": [*foreign_machines, *grant_machines],
            "lan_machines": ["rec-a/machine-0", *grant_machines],
            "private_endpoint": "dm-grant/machine-0:probe",
            "private_address": private_address,
            "private_token": private_token,
            "host_ports": [(f"host-loopback:{GRANTED_GUEST_PORT}", GRANTED_GUEST_PORT, granted_service.token),
                           (f"host-loopback:{UNDECLARED_GUEST_PORT}", UNDECLARED_GUEST_PORT, None),
                           (f"host-service:{granted_service.port}", granted_service.port, granted_service.token),
                           (f"host-foil:{foil_service.port}", foil_service.port, foil_service.token)],
            "host_service_port": granted_service.port,
            "host_service_token": granted_service.token,
            "export_host_port": export_host_port,
            "undeclared_export_port": undeclared_export_port,
            "lan_addresses": lan_addresses,
            "edge_name": PUBLIC_NAMES[0],
            "edge_undeclared_name": UNDECLARED_NAME,
            "edge_token": edge_token,
            "edge_anchors": edge_anchors,
        }
        cells = enumerate_denial_matrix(plan)
        declared = len(cells)
        keys = [cell.key for cell in cells]
        classes = sorted({cell.klass for cell in cells})
        protocols = sorted({cell.protocol for cell in cells})
        expectations = sorted({cell.expected for cell in cells})
        check.check(declared >= DENIAL_MATRIX_MINIMUM_ROWS,
                    f"the declared matrix enumerates {declared} rows (floor {DENIAL_MATRIX_MINIMUM_ROWS})")
        check.check(len(set(keys)) == declared,
                    f"every declared cell is a distinct source x destination x protocol x port "
                    f"({len(set(keys))} distinct of {declared})")
        check.check(classes == sorted(DENIAL_MATRIX_CLASSES),
                    f"the matrix enumerates every destination class the criterion names (observed {classes}, "
                    f"declared {sorted(DENIAL_MATRIX_CLASSES)})")
        check.check({"tcp", "udp"} <= set(protocols),
                    f"the matrix enumerates at least TCP and UDP (observed {protocols})")
        check.check(expectations == ["allow", "deny"],
                    f"the matrix declares both expectations (observed {expectations})")

        # -- the probes ---------------------------------------------------------
        def blocked(cell):
            """The first resource this cell needs and does not have, or None."""
            for resource in (*cell.requires, matrix_tool_requirement(cell)):
                if resource is not None and resource in unavailable:
                    return resource
            return None

        def preflight(source: str) -> None:
            isolate, machine = matrix_source_parts(source)
            receipt = machine_exec(ctx, check, f"dm-preflight-{isolate}-{machine}", instances[isolate], machine,
                                   MATRIX_PREFLIGHT, timeout=HOST_BOUNDARY_TIMEOUT)
            applets, fetch = parse_matrix_preflight(receipt)
            if not applets:
                refuse(f"{source} could not report the applets its BusyBox carries "
                       f"(exit {receipt.exit_code})", *[f"tool:{source}:{a}" for a in MATRIX_PROBE_APPLETS.values()])
                return
            # The reason names the applet and not the Machine, so the
            # not_implemented text below groups every Machine that lacks it into
            # one sentence instead of repeating one fact about the image once
            # per source until it crowds the other reasons out.
            for applet in sorted(set(MATRIX_PROBE_APPLETS.values())):
                if applet not in applets:
                    refuse(f"this image's BusyBox carries no `{applet}` applet, so every cell needing it was "
                           "not exercised", f"tool:{source}:{applet}")
            if not fetch:
                refuse(f"this image does not carry {GUEST_FETCH}, so its HTTPS cells were not exercised",
                       f"tool:{source}:{GUEST_FETCH}")

        for source in sorted({cell.source for cell in cells if cell.source != "host"}):
            isolate = matrix_source_parts(source)[0]
            if "src:" + isolate in unavailable or isolate not in instances:
                refuse(unavailable.get("src:" + isolate, f"{isolate} was never provisioned"), "src:" + isolate)
                continue
            preflight(source)

        attempted = []

        def run_guest_phase(phase: str) -> None:
            for source in sorted({cell.source for cell in cells
                                  if cell.phase == phase and cell.source != "host"}):
                isolate, machine = matrix_source_parts(source)
                runnable = [cell for cell in cells
                            if cell.phase == phase and cell.source == source and blocked(cell) is None]
                if not runnable:
                    continue
                receipt = machine_exec(ctx, check, f"dm-{phase}-{isolate}-{machine}", instances[isolate], machine,
                                       matrix_probe_script(runnable), timeout=MATRIX_PROBE_TIMEOUT)
                results = parse_matrix_probe(receipt)
                for cell in runnable:
                    if cell.index not in results:
                        # Left exactly as it was declared. A cell the probe loop
                        # did not report is NOT an observation of anything, and
                        # writing a value here would be the matrix analogue of a
                        # sub-check that branches on a variable nobody set.
                        continue
                    status, output = results[cell.index]
                    cell.observed, cell.detail = observe_matrix_cell(cell, status, output)
                    attempted.append(cell)

        def run_host_cell(cell, attempts: int) -> None:
            status, output = None, ""
            for attempt in range(1, attempts + 1):
                receipt = ctx.run_tool(check, f"dm-host-{cell.port}-{attempt}",
                                       ["/usr/bin/curl", "--silent", "--show-error", "--max-time", str(WGET_TIMEOUT),
                                        matrix_probe_command(cell)],
                                       cwd=ctx.state.root, env=ctx.state.env(), timeout=MATRIX_HOST_PROBE_TIMEOUT)
                status, output = receipt.exit_code, receipt.stdout.decode("utf-8", "replace")
                if status == 0 and (cell.token is None or cell.token in output):
                    break
                if attempt < attempts:
                    time.sleep(LISTENER_INTERVAL)
            cell.observed, cell.detail = observe_matrix_cell(cell, status, output)
            attempted.append(cell)

        held = None
        try:
            if "dm-grant" in instances and ("dst:private" not in unavailable
                                            or "dst:host-export" not in unavailable):
                held = hold_machine_exec(ctx, check, "dm-served-origin", instances["dm-grant"], "machine-0",
                                         origin_script(private_token))
            # The declared export is the readiness signal for this whole phase:
            # it is the host's own view of the listener every `served` cell
            # depends on, and it is the phase's one expected-allow host cell.
            ready = next((cell for cell in cells
                          if cell.klass == "host_export_declared" and blocked(cell) is None), None)
            if ready is not None:
                run_host_cell(ready, LISTENER_ATTEMPTS)
            run_guest_phase("served")
            for cell in cells:
                if cell.source == "host" and cell is not ready and blocked(cell) is None:
                    run_host_cell(cell, 1)
        finally:
            if held is not None:
                released = ctx.release(check, held)
                check.check(released.exit_code is not None,
                            f"the granted Environment's held origin was released (exit {released.exit_code})")

        held = None
        try:
            if "dst:edge" not in unavailable:
                held = hold_machine_exec(ctx, check, "dm-edge-origin", instances["dm-edge"], "machine-0",
                                         origin_script(edge_token))
                first = next((cell for cell in cells
                              if cell.klass == "public_like_ingress" and cell.protocol == "https"
                              and blocked(cell) is None), None)
                for attempt in range(1, LISTENER_ATTEMPTS + 1):
                    if first is None:
                        break
                    isolate, machine = matrix_source_parts(first.source)
                    receipt = machine_exec(ctx, check, f"dm-edge-ready-{attempt}", instances[isolate], machine,
                                           matrix_probe_script([first]), timeout=MATRIX_PROBE_TIMEOUT)
                    status, output = parse_matrix_probe(receipt).get(first.index, (None, ""))
                    if observe_matrix_cell(first, status, output)[0] == "allow":
                        break
                    if attempt < LISTENER_ATTEMPTS:
                        time.sleep(LISTENER_INTERVAL)
            run_guest_phase("edge")
        finally:
            if held is not None:
                released = ctx.release(check, held)
                check.check(released.exit_code is not None,
                            f"the public-like Environment's held origin was released (exit {released.exit_code})")

        # Nothing is held for the `open` phase, deliberately: a Machine with a
        # live loopback listener of its own answers its OWN loopback on any
        # port, and every host-import, control-plane and Internet cell is
        # addressed at loopback or off-host. Probing them while a local listener
        # was up would read that listener as a host destination having answered.
        run_guest_phase("open")

        # -- the comparison -----------------------------------------------------
        findings = denial_matrix_findings(cells)
        runnable_total = len([cell for cell in cells if blocked(cell) is None])
        check.check(len(attempted) == runnable_total,
                    f"every cell whose subject exists was probed and reported a result of its own "
                    f"({len(attempted)} reported of {runnable_total} runnable)")
        orphans = [cell for cell in findings["unexecuted"] if blocked(cell) is None]
        check.check(not orphans,
                    "every unexercised cell names the resource this runtime does not have; a blank cell with no "
                    f"reason is a hole in the matrix (observed {[cell.label() for cell in orphans[:6]]})")
        check.check(not findings["unexpected_success"],
                    "no cell declared deny was observed allow; unexpected success is what fails this criterion "
                    f"(observed {[cell.label() for cell in findings['unexpected_success'][:6]]})")
        check.check(not findings["unmet_allow"],
                    "every cell declared allow was observed allow; a declared path that did not serve is a "
                    "different failure from an unexpected success and is reported as one "
                    f"(observed {[cell.label() for cell in findings['unmet_allow'][:6]]})")
        check.check(not findings["indeterminate"],
                    "no cell was answered by something that could not name itself; a reply without the "
                    "destination's own token is neither reached nor refused "
                    f"(observed {[cell.label() for cell in findings['indeterminate'][:6]]})")
        if "src:dm-egress" in unavailable:
            check.ok("the egress-attachment clause was not exercised: " + unavailable["src:dm-egress"][:200])
        else:
            crosstalk = egress_attachment_findings(cells)
            check.check(not crosstalk,
                        "two Machines of one Environment on different egress attachments neither share an "
                        f"Internet policy nor a host import ({crosstalk[:4]})")

        # -- the artifact -------------------------------------------------------
        payload = denial_matrix_document(ctx.recorder.run_id, top, cells)
        write_exclusive(ctx.evidence_dir / DENIAL_MATRIX_EVIDENCE,
                        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False,
                                   allow_nan=False).encode("utf-8") + b"\n")
        check.evidence.append(DENIAL_MATRIX_EVIDENCE)
        # Read back from the file the gate will validate, not from the object
        # that was just serialised: the artifact IS this criterion's deliverable,
        # and comparing the object against itself would prove nothing about the
        # table anyone else reads. Both the count and the identity of every cell
        # are compared, so a serialiser that dropped or merged rows is caught.
        written = load_json(ctx.evidence_dir / DENIAL_MATRIX_EVIDENCE)
        rows = written.get("rows") if isinstance(written, dict) else None
        check.check(isinstance(rows, list) and len(rows) == declared and declared > 0,
                    f"{DENIAL_MATRIX_EVIDENCE} carries one row per declared cell "
                    f"({len(rows) if isinstance(rows, list) else rows} rows of {declared} declared)")
        recorded = {(row.get("source"), row.get("destination"), row.get("protocol"), row.get("port"))
                    for row in rows} if isinstance(rows, list) else set()
        check.check(recorded == set(keys),
                    f"{DENIAL_MATRIX_EVIDENCE} names exactly the declared cells "
                    f"(missing {sorted(set(keys) - recorded)[:4]}, unexpected {sorted(recorded - set(keys))[:4]})")
        if matrix_path.is_file():
            problems = sorted(Draft202012Validator(load_json(matrix_path)).iter_errors(written),
                              key=lambda error: list(map(str, error.absolute_path)))
            check.check(not problems,
                        f"{DENIAL_MATRIX_EVIDENCE} validates against {DENIAL_MATRIX_SCHEMA}" if not problems
                        else f"{DENIAL_MATRIX_EVIDENCE} is not schema-valid: {problems[0].message[:200]}")
        else:
            check.fail(f"the connectivity-matrix schema is absent: {DENIAL_MATRIX_SCHEMA}")
        check.ok(f"matrix rows: {declared}; executed {len(findings['executed'])}; "
                 f"unexercised {len(findings['unexecuted'])}; classes {len(classes)}; protocols {protocols}")
        if unavailable:
            # Deduplicated by reason: a missing applet is one fact about the
            # image, and repeating it once per Machine buries the other reasons.
            grouped = {}
            for resource, reason in sorted(unavailable.items()):
                grouped.setdefault(reason, []).append(resource)
            reasons = "; ".join(f"{sorted(resources)}: {reason}" for reason, resources in sorted(grouped.items()))
            check.not_implemented = (
                f"{len(findings['unexecuted'])} of the {declared} declared cells were not exercised, so the "
                "matrix is complete as a declaration and partial as a measurement. Every cell that DID run is "
                "graded above. Unexercised, in the refusing component's own words: " + reasons[:2400])
    except (OSError, GateError) as error:
        check.fail(f"the denial-matrix check could not complete: {type(error).__name__}: {error}")
    finally:
        granted_service.close()
        foil_service.close()
        # Only what this check created. The `rec-*` Environments belong to
        # criterion 10 and to the phase, and deleting one here would remove the
        # subject of the check that ran before this one.
        for name in provisioned:
            instance = instances.get(name)
            if instance is None or instance.get("status") is None:
                continue
            try:
                removed = ctx.run(check, name + "-delete",
                                  ["--json", "delete", "--environment", "default", "--timeout", "120"],
                                  cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
                check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")
            except OSError as error:
                check.fail(f"{name}: could not be deleted: {type(error).__name__}: {error}")
    return check.finish()


# --------------------------------------------------------------- criterion 12
#
# `gate.agent.deterministic_workers`. The criterion's last sentence is the sharp
# one: "every output, cancellation, PTY, exit status, event, and receipt maps to
# the exact Environment, Machine, worker, and request". That is an attribution
# claim, and attribution is only falsifiable where two artifacts could be
# confused. So the schedule the checked-in driver runs deliberately contains a
# pair of workers that differ in NOTHING a runtime could key on except the
# request id -- one Environment, one Machine, identical command text, released
# together from one barrier -- and the round after it cancels exactly one of
# them.
#
# The driver and its schedule live in `tests/fixtures/vz-0.4/agent-driver` and
# are named by the contract's `fixtures.required_dirs`. The driver records; this
# check compares. Nothing below reads the driver's opinion of whether a step
# succeeded: every assertion compares a value the RUNTIME reported against a
# value this check derived from the topology (`vz status`, and the record the
# establishing check wrote) or from the plan it wrote itself.
#
# Imported here rather than at the top of the module: five criteria are edited
# in parallel in this file and a shared import block is where their merges
# collide.
import base64
import binascii

from vz04_common import document

AGENT_FIXTURE_DIR = "tests/fixtures/vz-0.4/agent-driver"
AGENT_DRIVER = AGENT_FIXTURE_DIR + "/driver.py"
AGENT_SCHEDULE = AGENT_FIXTURE_DIR + "/schedule.json"
AGENT_SCHEDULE_KIND = "vz-0.4-agent-schedule"
AGENT_TRANSCRIPT_KIND = "vz-0.4-agent-transcript"
AGENT_PYTHON = "/usr/bin/python3"
# The Environment holding the workspace-projection workers and the twin pair.
# Created by this check and, unlike the persisted-recovery isolates, removed by
# it again: nothing post-wake is meant to find it.
AGENT_WORKSPACE_ISOLATE = "agent-ws"
# The Environment holding the cooperating Linux/native-macOS pair.
AGENT_CROSSING_ISOLATE = "agent-x"
AGENT_RW_SOURCE, AGENT_RO_SOURCE = "rw", "ro"
AGENT_WRITE_NAME = "agent.txt"
# The read_only worker writes over the host-seeded file rather than beside it:
# a projection wrongly materialised as a writable share of the source is then
# visible as changed bytes on the host and not only as an extra file.
AGENT_SEED_NAME = "seed.txt"
AGENT_DRIVER_TIMEOUT = 900
AGENT_STEP_TIMEOUT = 120
AGENT_EMITTED = re.compile(r"^AGENT (\S+) (\S+)\s*$")
# The three isolated workers, the twin pair, the two workspace writers and the
# cooperating pair, in the schedule's own spelling.
AGENT_ISOLATED_BINDINGS = ("isolate-a", "isolate-b", "isolate-c")
AGENT_COOPERATING_BINDINGS = ("coop-linux", "coop-macos")
# Steps the schedule names, addressed by id where a clause is about that step in
# particular rather than about every step.
AGENT_CANCELLED_STEP = "s3_twin_a"
AGENT_CANCEL_PEER_STEP = "s3_twin_b"
AGENT_PTY_STEP = "s4_twin_a"
AGENT_WRITE_OK_STEP = "s5_writer_rw"
AGENT_WRITE_REFUSED_STEP = "s5_writer_ro"
AGENT_SERVE_STEP = "s6_coop_serve"
AGENT_FETCH_STEP = "s6_coop_fetch"
AGENT_EXIT_STATUS_ROUND = "isolated_exit_status"
AGENT_TWIN_ROUND = "twin_crosstalk"
AGENT_EXIT_STATUSES = [0, 3, 7]


def agent_workspace_definition(release_dir: Path) -> dict:
    """One Environment, two Developer Linux Machines, two projection modes.

    Criterion 12 asks that "workspace writer policy is enforced" for a worker,
    which needs a Machine whose projection forbids the write and a sibling whose
    projection permits it, so the refusal is measured against a write that was
    admitted in the same Environment at the same moment rather than against
    nothing at all.
    """
    definition = two_machine_definition(release_dir)
    environment = definition["environment"]
    for machine, mode, source, target in ((environment["machines"][0], "read_write", AGENT_RW_SOURCE, RW_TARGET),
                                          (environment["machines"][1], "read_only", AGENT_RO_SOURCE, RO_TARGET)):
        machine["workspace"] = {"binding": "source", "target_path": target, "mode": mode, "source_path": source}
    return definition


def agent_provision_seeded(ctx: CheckContext, check: SubCheck, name: str, definition: dict) -> dict:
    """`provision`, plus the host-side worktree the declared projections name.

    `provision` commits `vz.json` alone, and a definition declaring a workspace
    source the worktree does not carry is refused before any Machine exists. The
    sources are seeded first and committed with it.
    """
    data = json.dumps(definition, indent=2, sort_keys=True).encode() + b"\n"
    iso = ctx.isolated(name, project_files={"vz.json": data}, provision=True)
    env, project = iso["env"], iso["project"]
    seed_worktree(project)
    for argv in (["init", "--quiet", "--initial-branch", "main"], ["add", "-A"],
                 ["-c", "user.name=vz gate", "-c", "user.email=gate@vz.invalid",
                  "commit", "--quiet", "-m", "definition"]):
        receipt = ctx.run_tool(check, name + "-git", [GIT, *argv], cwd=project, env=env)
        check.check(receipt.exit_code == 0, f"{name}: git {argv[0]} exit {receipt.exit_code} (expected 0)")
    if check.status != "PASS":
        return {"env": env, "project": project, "status": None}
    up = ctx.run(check, name + "-up", ["--json", "up"], cwd=project, env=env, timeout=UP_TIMEOUT)
    if up.exit_code != 0:
        detail = ""
        try:
            detail = json.loads(up.stderr.decode("utf-8")).get("error", {}).get("message", "")
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            detail = ""
        if "adapters remain required" in detail:
            return {"env": env, "project": project, "status": None, "unsupported": detail}
        check.check(False, f"{name}: vz --json up exit {up.exit_code} (expected 0): {detail[:200]}")
        return {"env": env, "project": project, "status": None}
    check.check(True, f"{name}: vz --json up exit 0 (expected 0)")
    return {"env": env, "project": project,
            "status": read_status(ctx, check, name, project=project, env=env)}


def agent_identities(payload: dict) -> dict:
    """`{"environment_id": ..., "machines": {name: machine_id}}` from one status."""
    environments = (payload or {}).get("environments") or []
    if len(environments) != 1:
        return {}
    environment = environments[0]
    return {"environment_id": environment.get("environment_id"),
            "machines": {machine.get("name"): machine.get("machine_id")
                         for machine in environment.get("machines") or []}}


def agent_binding(instance: dict, identity: dict, machine: str, *, target_os: str = "linux",
                  params: dict = None) -> dict:
    """One plan binding, and the identity pair every artifact of it must carry."""
    return {"cwd": str(instance["project"]), "environment": "default", "machine": machine,
            "target_os": target_os, "env": dict(instance["env"]), "params": dict(params or {}),
            "expected": {"environment_id": identity.get("environment_id"),
                         "machine_id": (identity.get("machines") or {}).get(machine)}}


def agent_guest_text(row: dict) -> str:
    """The guest bytes the runtime attributed to this step, as text.

    A PTY transcript comes back through a terminal line discipline, so the
    carriage returns it inserts are removed; nothing else is normalised.
    """
    encoded = (row.get("guest") or {}).get("stdout") or ""
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError, binascii.Error):
        return ""
    return raw.decode("utf-8", "replace").replace("\r\n", "\n").replace("\r", "\n")


def agent_emitted(row: dict):
    """The `(request, token)` pair the guest program itself printed, or None.

    This is the end-to-end half of the attribution claim: the request id and the
    token were delivered INTO the Machine as this execution's guest environment,
    so the pair that comes back names whichever execution's environment the
    runtime actually installed.
    """
    for line in agent_guest_text(row).splitlines():
        match = AGENT_EMITTED.match(line)
        if match:
            return match.group(1), match.group(2)
    return None


def agent_records(row: dict, kind: str) -> list:
    return [record for record in row.get("records") or [] if record.get("record_type") == kind]


def agent_receipt(row: dict):
    receipts = agent_records(row or {}, "execution_receipt")
    return receipts[-1].get("receipt") if receipts else None


def agent_scope(row: dict) -> dict:
    return ((agent_receipt(row) or {}).get("scope") or {})


def agent_scopes(row: dict) -> list:
    """Every scope the runtime stamped on this step's records, in order."""
    scopes = []
    for record in row.get("records") or []:
        scope = record.get("scope")
        if isinstance(scope, dict):
            scopes.append((record.get("record_type"), scope))
        receipt = record.get("receipt")
        if isinstance(receipt, dict) and isinstance(receipt.get("scope"), dict):
            scopes.append((str(record.get("record_type")) + ".receipt", receipt["scope"]))
    return scopes


def agent_scope_mismatches(row: dict, expected: dict) -> list:
    """Every `(record, field, expected, observed)` this step's records got wrong.

    The four fields are the four the criterion names: the request, the
    Environment, the Machine, and -- through the idempotency key its worker
    chose -- the worker.
    """
    wrong = []
    for kind, scope in agent_scopes(row):
        for field in ("request_id", "idempotency_key", "environment_id", "machine_id"):
            want, observed = expected.get(field), scope.get(field)
            if observed != want:
                wrong.append(f"{kind}.scope.{field} expected {want!r} observed {observed!r}")
    return wrong


def agent_raw_text(row: dict) -> str:
    """Everything this step produced, records and streams alike, as one string."""
    parts = [json.dumps(row.get("records") or [], sort_keys=True), agent_guest_text(row)]
    for encoded in ((row.get("stderr_b64") or ""), (row.get("raw_b64") or "")):
        try:
            parts.append(base64.b64decode(encoded, validate=True).decode("utf-8", "replace"))
        except (ValueError, TypeError, binascii.Error):
            continue
    return "\n".join(parts)


def agent_foreign(row: dict, others: dict, allowed=()) -> list:
    """Identities belonging to OTHER steps that appear in this step's artifacts.

    `allowed` names the one legitimate crossing: the cooperating fetch is meant
    to read the serving worker's token, because that is the declared
    cross-target service path doing its job. Every other appearance of another
    step's request id, idempotency key or token is cross-attribution.
    """
    text = agent_raw_text(row)
    found = []
    for step_id, identity in sorted(others.items()):
        for field in ("request_id", "idempotency_key", "token"):
            value = identity.get(field)
            if value and value not in allowed and value in text:
                found.append(f"{step_id}.{field} {value!r}")
    return found


def agent_overlapped(rows: list) -> bool:
    """Whether every step in a round was in flight at one moment.

    The barrier's whole purpose: `max(start) < min(end)` holds only if the last
    step started before the first one finished, which is what makes a concurrent
    round concurrent rather than usually-concurrent.
    """
    starts = [row.get("started_unix_ns") for row in rows]
    ends = [row.get("ended_unix_ns") for row in rows]
    if not all(isinstance(value, int) for value in starts + ends):
        return False
    return max(starts) < min(ends)


def check_deterministic_agent_workers(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """Criterion 12: a checked-in deterministic driver, and exact attribution.

    Three isolated workers run against the three persisted-recovery
    Environments; two cooperating workers run against a Linux Machine and a
    native macOS Machine in one Environment over its declared cross-target
    service path; a twin pair runs against ONE Machine in one Environment with
    identical command text, so only the request id tells its two workers apart;
    and two workspace writers run concurrently, one holding a read_write
    projection and one a read_only projection.

    Every assertion compares a value. The Environment and Machine identities
    come from `vz status` and from the record the establishing check wrote
    before this one ran; the request identities come from the plan this check
    wrote; the driver contributes artifacts and no verdict at all.
    """
    check = SubCheck(top, "deterministic_agent_workers")
    driver = ctx.repo_root / AGENT_DRIVER
    schedule_path = ctx.repo_root / AGENT_SCHEDULE
    if not check.check(driver.is_file() and not driver.is_symlink(),
                       f"the checked-in agent driver is present at {AGENT_DRIVER}"):
        return check.finish()
    if not check.check(schedule_path.is_file() and not schedule_path.is_symlink(),
                       f"the checked-in agent schedule is present at {AGENT_SCHEDULE}"):
        return check.finish()
    try:
        schedule = load_json(schedule_path)
    except (OSError, ValueError) as error:
        check.fail(f"{AGENT_SCHEDULE} is not readable JSON: {error}")
        return check.finish()
    check.check(schedule.get("kind") == AGENT_SCHEDULE_KIND,
                f"the schedule declares kind {AGENT_SCHEDULE_KIND!r} (observed {schedule.get('kind')!r})")
    roles = {}
    for worker in schedule.get("workers") or []:
        roles.setdefault(worker.get("role"), []).append(worker.get("binding"))
    isolated = sorted(set(roles.get("isolated") or []))
    cooperating = sorted(set(roles.get("cooperating") or []))
    check.check(len(isolated) >= 3,
                f"the schedule runs at least three isolated workers on separate Environments "
                f"(observed {isolated})")
    check.check(cooperating == sorted(AGENT_COOPERATING_BINDINGS),
                f"the schedule runs two cooperating workers {sorted(AGENT_COOPERATING_BINDINGS)} "
                f"(observed {cooperating})")
    declared_steps = [row["id"] for round_row in schedule.get("rounds") or [] for row in round_row.get("steps") or []]
    check.check(len(declared_steps) == len(set(declared_steps)),
                f"every declared step id is unique ({len(set(declared_steps))} of {len(declared_steps)})")
    if check.status != "PASS":
        return check.finish()

    # -- the Environments the workers are bound to
    bindings, native = {}, macos_target(ctx.release_dir)
    recovery = list((established or {}).get("environments") or [])
    if not check.check(len(recovery) >= len(AGENT_ISOLATED_BINDINGS),
                       f"the establishing check left at least {len(AGENT_ISOLATED_BINDINGS)} Environments to "
                       f"work in (observed {len(recovery)})"):
        return check.finish()
    for name, entry in zip(AGENT_ISOLATED_BINDINGS, recovery):
        try:
            instance = ctx.reattach(entry["isolate"])
        except ReattachError as error:
            check.fail(str(error))
            return check.finish()
        machine = (entry.get("machines") or [{}])[0]
        bindings[name] = {"cwd": str(instance["project"]), "environment": "default",
                          "machine": machine.get("name"), "target_os": "linux",
                          "env": dict(instance["env"]), "params": {},
                          "expected": {"environment_id": entry.get("environment_id"),
                                       "machine_id": machine.get("machine_id")}}
    identifiers = [bindings[name]["expected"]["environment_id"] for name in AGENT_ISOLATED_BINDINGS]
    check.check(len(set(identifiers)) == len(AGENT_ISOLATED_BINDINGS),
                f"the three isolated workers address three distinct Environments ({identifiers})")
    if check.status != "PASS":
        return check.finish()

    try:
        workspace_definition = agent_workspace_definition(ctx.release_dir)
    except (StopIteration, KeyError, OSError) as error:
        check.fail(f"cannot derive a Developer target from the release machine-target-catalog: {error}")
        return check.finish()
    workspace = agent_provision_seeded(ctx, check, AGENT_WORKSPACE_ISOLATE, workspace_definition)
    if workspace.get("unsupported"):
        check.not_implemented = ("declared workspace projections are not applied by this runtime: " +
                                 workspace["unsupported"][:300])
        return check.finish()
    if check.status != "PASS" or not workspace["status"]:
        return check.finish()
    workspace_identity = agent_identities(workspace["status"])
    check.check(sorted(workspace_identity.get("machines") or {}) == ["machine-0", "machine-1"],
                f"the workspace Environment reports both declared Machines "
                f"(observed {sorted(workspace_identity.get('machines') or {})})")
    if check.status != "PASS":
        return check.finish()
    for name, machine, params in (("twin-a", "machine-0", {}), ("twin-b", "machine-0", {}),
                                  ("writer-rw", "machine-0", {"path": f"{RW_TARGET}/{AGENT_WRITE_NAME}"}),
                                  ("writer-ro", "machine-1", {"path": f"{RO_TARGET}/{AGENT_SEED_NAME}"})):
        bindings[name] = agent_binding(workspace, workspace_identity, machine, params=params)
    check.check(bindings["twin-a"]["expected"] == bindings["twin-b"]["expected"],
                f"the twin workers address one Environment and one Machine "
                f"({bindings['twin-a']['expected']})")

    # -- the cooperating pair, when this release can build a native macOS Machine
    crossing = None
    if native is not None:
        crossing = provision(ctx, check, AGENT_CROSSING_ISOLATE, crossing_definition(ctx.release_dir, native))
        if crossing.get("unsupported"):
            check.not_implemented = ("declared networks are not applied by this runtime: " +
                                     crossing["unsupported"][:300])
            return check.finish()
        if check.status != "PASS" or not crossing["status"]:
            return check.finish()
        crossing_identity = agent_identities(crossing["status"])
        probed = machine_exec(ctx, check, "agent-cross-port", crossing, "machine-0", FABRIC_PROBE)
        port = FabricState(probed).port()
        if not check.check(port is not None,
                           f"the serving Linux Machine holds the fabric address the host derived "
                           f"(exit {probed.exit_code})"):
            return check.finish()
        bindings["coop-linux"] = agent_binding(crossing, crossing_identity, "machine-0",
                                               params={"port": str(PRIVATE_PORT)})
        bindings["coop-macos"] = agent_binding(crossing, crossing_identity, "machine-mac", target_os="macos",
                                               params={"port": str(PRIVATE_PORT), "peer": port["address"]})

    # -- run the checked-in driver
    plan = {"schema_version": 1, "kind": "vz-0.4-agent-plan", "run_token": "vzag-" + uuid.uuid4().hex[:16],
            "cli": str(ctx.state.cli), "env": {}, "wall_timeout_seconds": AGENT_STEP_TIMEOUT,
            "bindings": {name: {key: value for key, value in binding.items() if key != "expected"}
                         for name, binding in bindings.items()}}
    plan_path = ctx.evidence_dir / "agent-plan.json"
    transcript_path = ctx.evidence_dir / "agent-transcript.json"
    document(plan_path, plan)
    check.evidence.append(plan_path.name)
    ran = ctx.run_tool(check, "agent-driver",
                       [AGENT_PYTHON, "-B", str(driver), "--schedule", str(schedule_path),
                        "--plan", str(plan_path), "--transcript", str(transcript_path)],
                       cwd=ctx.evidence_dir,
                       env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C",
                            "PYTHONDONTWRITEBYTECODE": "1"},
                       timeout=AGENT_DRIVER_TIMEOUT)
    if not check.check(ran.exit_code == 0,
                       f"the agent driver ran to completion (exit {ran.exit_code}, {ran.stderr[-200:]!r})"):
        return check.finish()
    if not check.check(transcript_path.is_file(), "the agent driver wrote its transcript"):
        return check.finish()
    check.evidence.append(transcript_path.name)
    try:
        transcript = load_json(transcript_path)
    except (OSError, ValueError) as error:
        check.fail(f"the agent transcript is not readable JSON: {error}")
        return check.finish()
    check.check(transcript.get("kind") == AGENT_TRANSCRIPT_KIND,
                f"the transcript declares kind {AGENT_TRANSCRIPT_KIND!r} (observed {transcript.get('kind')!r})")
    check.check(transcript.get("schedule_sha256") == digest_file(schedule_path),
                f"the driver ran the checked-in schedule (transcript {transcript.get('schedule_sha256')!r}, "
                f"file {digest_file(schedule_path)!r})")
    rows = transcript.get("steps") or []
    check.check([row.get("step") for row in rows] == declared_steps,
                f"the driver ran exactly the declared steps in the declared order "
                f"(observed {[row.get('step') for row in rows]})")
    if check.status != "PASS":
        return check.finish()
    by_step = {row["step"]: row for row in rows}
    ran_rows = [row for row in rows if not row.get("skipped")]

    # The one honest way out: a runtime whose `--json exec` files no terminal
    # receipt at all has no attribution surface for this criterion to be about.
    # Modelled on `check_private_topology_paths`' `unsupported` path -- reported
    # with what the runtime actually emitted, never inferred from a comparison
    # that failed.
    if not any(agent_records(row, "execution_receipt") for row in ran_rows):
        observed = ""
        for row in ran_rows:
            observed = (agent_raw_text(row) or "").strip()[:200]
            if observed:
                break
        check.not_implemented = ("this runtime's `vz --json exec` filed no execution_receipt record for any of "
                                 f"the {len(ran_rows)} executions the driver issued, so no output, "
                                 "cancellation, PTY, exit status, event or receipt carries an Environment, "
                                 f"Machine or request to compare. It answered: {observed!r}")
        return check.finish()

    # -- per-step attribution
    identities = {row["step"]: {"request_id": (row.get("intent") or {}).get("request_id"),
                                "idempotency_key": (row.get("intent") or {}).get("idempotency_key"),
                                "token": (row.get("intent") or {}).get("token")}
                  for row in ran_rows}
    check.check(len({tuple(sorted(value.items())) for value in identities.values()}) == len(identities),
                f"every step carries its own request identity ({len(identities)} steps)")
    executions = []
    for row in ran_rows:
        step = row["step"]
        intent = row.get("intent") or {}
        binding = bindings.get(intent.get("binding")) or {}
        expected = dict(binding.get("expected") or {})
        expected.update(request_id=intent.get("request_id"), idempotency_key=intent.get("idempotency_key"))
        argv = row.get("argv") or []
        for flag, want in (("--machine", binding.get("machine")), ("--environment", binding.get("environment")),
                           ("--request-id", intent.get("request_id")),
                           ("--idempotency-key", intent.get("idempotency_key"))):
            observed = argv[argv.index(flag) + 1] if flag in argv and argv.index(flag) + 1 < len(argv) else None
            check.check(observed == want,
                        f"{step}: the invocation named {flag} {want!r} (observed {observed!r})")
        check.check(intent.get("cwd") == binding.get("cwd"),
                    f"{step}: ran in its own worker's project ({binding.get('cwd')!r} observed "
                    f"{intent.get('cwd')!r})")
        # `vz exec --tty` refuses `--json`, so a terminal execution has no
        # record stream at all and its attribution rests entirely on the
        # transcript, the invocation and the process status. Asserting an empty
        # record set against the expected scope would be an assertion that
        # cannot fail, so the record clauses are made only where records exist.
        over_terminal = row.get("channel") == "pty"
        if not over_terminal:
            scopes = agent_scopes(row)
            wrong = agent_scope_mismatches(row, expected)
            # The count is part of the claim: a stream that carried no scope at
            # all would otherwise satisfy "nothing was misattributed".
            check.check(scopes and not wrong,
                        f"{step}: all {len(scopes)} scoped records map to Environment "
                        f"{expected.get('environment_id')!r}, Machine {expected.get('machine_id')!r}, request "
                        f"{expected.get('request_id')!r}"
                        if scopes and not wrong else
                        (f"{step}: no record carried a scope at all" if not scopes
                         else f"{step}: misattributed records: " + "; ".join(wrong[:4])))
            opened = [record.get("request_id") for record in agent_records(row, "request_started")]
            check.check(opened == [intent.get("request_id")],
                        f"{step}: the runtime opened exactly this request (expected "
                        f"{[intent.get('request_id')]}, observed {opened})")
        emitted = agent_emitted(row)
        check.check(emitted == (intent.get("request_id"), intent.get("token")),
                    f"{step}: the guest reported the request and token THIS execution carried (expected "
                    f"{(intent.get('request_id'), intent.get('token'))}, observed {emitted})")
        allowed = ({identities[AGENT_SERVE_STEP]["token"]}
                   if step == AGENT_FETCH_STEP and AGENT_SERVE_STEP in identities else set())
        foreign = agent_foreign(row, {other: value for other, value in identities.items() if other != step},
                                allowed)
        check.check(not foreign,
                    f"{step}: carries no other worker's identity"
                    if not foreign else f"{step}: carries another worker's identity: " + "; ".join(foreign[:4]))
        expectation = row.get("expect") or {}
        if over_terminal:
            check.check(row.get("exit_code") == expectation.get("code"),
                        f"{step}: the terminal execution returned its guest's exit status "
                        f"(expected {expectation.get('code')!r}, observed {row.get('exit_code')!r})")
            continue
        if row.get("held"):
            # A held service is ended deliberately, by signal, once the workers
            # that had to reach it have. There is no terminal receipt to demand
            # of it; its attribution was already asserted from the records it
            # streamed while it ran.
            continue
        receipt = agent_receipt(row)
        if not check.check(receipt is not None, f"{step}: the runtime filed a terminal receipt"):
            continue
        executions.append(receipt.get("execution_id") or (receipt.get("scope") or {}).get("execution_id"))
        kind = expectation.get("kind")
        if kind == "exit":
            check.check(receipt.get("exit_code") == expectation.get("code") and
                        row.get("exit_code") == expectation.get("code"),
                        f"{step}: exit status {expectation.get('code')} (receipt {receipt.get('exit_code')!r}, "
                        f"process {row.get('exit_code')!r})")
            check.check(receipt.get("state") == "completed",
                        f"{step}: the receipt is terminal-completed (observed {receipt.get('state')!r})")
        elif kind == "nonzero":
            check.check(isinstance(receipt.get("exit_code"), int) and receipt["exit_code"] != 0,
                        f"{step}: a nonzero exit status (receipt {receipt.get('exit_code')!r})")
        elif kind == "cancelled":
            check.check(receipt.get("state") == "quiesced",
                        f"{step}: its own deadline cancelled it and the runtime proved no live work remained "
                        f"(receipt state {receipt.get('state')!r}, expected 'quiesced')")
    check.check(len(set(executions)) == len(executions),
                f"every execution carries its own execution id ({len(set(executions))} of {len(executions)})")

    # -- the barriers really did make the declared rounds concurrent
    for round_row in transcript.get("rounds") or []:
        concurrent = [by_step[step] for step in round_row.get("steps") or []
                      if step in by_step and not by_step[step].get("skipped") and not by_step[step].get("held")]
        if len(concurrent) < 2:
            continue
        spans = [(row["step"], row.get("started_unix_ns"), row.get("ended_unix_ns")) for row in concurrent]
        check.check(agent_overlapped(concurrent),
                    f"round {round_row.get('name')!r} released {len(concurrent)} steps from one barrier and "
                    f"they overlapped (spans {spans})")

    # -- three separate Environments, three exit statuses, one Machine for the twins
    exit_round = next((row for row in transcript.get("rounds") or []
                       if row.get("name") == AGENT_EXIT_STATUS_ROUND), {})
    statuses = {step: (agent_receipt(by_step.get(step)) or {}).get("exit_code")
                for step in exit_round.get("steps") or [] if step in by_step}
    check.check(sorted(value for value in statuses.values() if value is not None) == AGENT_EXIT_STATUSES,
                f"three concurrent executions reported three different exit statuses (expected "
                f"{AGENT_EXIT_STATUSES}, observed {statuses})")
    environments = {step: agent_scope(by_step.get(step)).get("environment_id")
                    for step in exit_round.get("steps") or [] if step in by_step}
    check.check(len(set(environments.values())) == len(environments),
                f"those receipts name one distinct Environment each ({environments})")
    twin_round = next((row for row in transcript.get("rounds") or [] if row.get("name") == AGENT_TWIN_ROUND), {})
    twin_scopes = {step: agent_scope(by_step.get(step))
                   for step in twin_round.get("steps") or [] if step in by_step}
    check.check(len({(scope.get("environment_id"), scope.get("machine_id"))
                     for scope in twin_scopes.values()}) == 1,
                f"the twin workers' receipts name ONE Environment and ONE Machine "
                f"({[(s.get('environment_id'), s.get('machine_id')) for s in twin_scopes.values()]})")
    check.check(len({scope.get("request_id") for scope in twin_scopes.values()}) == len(twin_scopes),
                f"and are told apart only by their request ids "
                f"({[s.get('request_id') for s in twin_scopes.values()]})")

    # -- cancellation reached exactly one of two concurrent executions
    cancelled, peer = by_step.get(AGENT_CANCELLED_STEP, {}), by_step.get(AGENT_CANCEL_PEER_STEP, {})
    peer_receipt = agent_receipt(peer) or {}
    check.check(peer_receipt.get("state") == "completed" and peer_receipt.get("exit_code") == 0,
                f"the execution sharing that Machine ran to completion while its peer was cancelled "
                f"(state {peer_receipt.get('state')!r}, exit {peer_receipt.get('exit_code')!r})")
    check.check(agent_scope(cancelled).get("request_id") == (cancelled.get("intent") or {}).get("request_id"),
                f"the cancellation is attributed to the request that asked for it (receipt "
                f"{agent_scope(cancelled).get('request_id')!r}, requested "
                f"{(cancelled.get('intent') or {}).get('request_id')!r})")

    # -- the PTY step ran on a terminal and carried its own identity
    terminal = by_step.get(AGENT_PTY_STEP, {})
    check.check(terminal.get("terminal") is True,
                f"the PTY step ran on a terminal the driver allocated (observed {terminal.get('terminal')!r})")
    check.check(agent_emitted(terminal) == ((terminal.get("intent") or {}).get("request_id"),
                                            (terminal.get("intent") or {}).get("token")),
                f"the terminal transcript carries its own request and token (expected "
                f"{((terminal.get('intent') or {}).get('request_id'), (terminal.get('intent') or {}).get('token'))}, "
                f"observed {agent_emitted(terminal)})")

    # -- workspace writer policy, observed rather than assumed
    refused, admitted = by_step.get(AGENT_WRITE_REFUSED_STEP, {}), by_step.get(AGENT_WRITE_OK_STEP, {})
    refused_receipt, admitted_receipt = agent_receipt(refused) or {}, agent_receipt(admitted) or {}
    check.check(isinstance(refused_receipt.get("exit_code"), int) and refused_receipt["exit_code"] != 0,
                f"the worker holding a read_only projection is refused its write (receipt exit "
                f"{refused_receipt.get('exit_code')!r}, expected nonzero)")
    check.check(admitted_receipt.get("exit_code") == 0,
                f"the worker holding a read_write projection is admitted its write (receipt exit "
                f"{admitted_receipt.get('exit_code')!r}, expected 0)")
    written = workspace["project"] / AGENT_RW_SOURCE / AGENT_WRITE_NAME
    observed = written.read_bytes() if written.is_file() else b""
    want = (identities.get(AGENT_WRITE_OK_STEP) or {}).get("token") or ""
    check.check(observed == want.encode(),
                f"the admitted write reached the host worktree with that worker's own bytes (expected "
                f"{want!r}, observed {observed[:60]!r})")
    seed = workspace["project"] / AGENT_RO_SOURCE / AGENT_SEED_NAME
    survived = seed.read_bytes() if seed.is_file() else b""
    check.check(survived == SEED,
                f"the read_only source is byte-identical after the refused write (observed {survived[:40]!r})")

    # -- the cooperating pair over the declared cross-target service path
    if native is not None:
        serve, fetch = by_step.get(AGENT_SERVE_STEP, {}), by_step.get(AGENT_FETCH_STEP, {})
        check.check(serve.get("ready_observed") is True,
                    f"the macOS worker was released by the runtime's own execution_ready event for the Linux "
                    f"worker's service rather than by a sleep (observed {serve.get('ready_observed')!r})")
        served = (identities.get(AGENT_SERVE_STEP) or {}).get("token")
        check.check(bool(served) and served in agent_guest_text(fetch),
                    f"the native macOS worker read the Linux worker's own token across the declared "
                    f"cross-target path (expected {served!r} in {agent_guest_text(fetch)[:80]!r})")
        check.check(agent_scope(fetch).get("machine_id") == bindings["coop-macos"]["expected"]["machine_id"],
                    f"the fetch is attributed to the native macOS Machine (expected "
                    f"{bindings['coop-macos']['expected']['machine_id']!r}, observed "
                    f"{agent_scope(fetch).get('machine_id')!r})")

    # Removed whenever nothing failed -- including when the criterion is only
    # partly exercisable -- because these Environments are this check's own and
    # post-wake must find exactly the three the establishing check left running.
    if not check.failures:
        for name, instance in ((AGENT_WORKSPACE_ISOLATE, workspace), (AGENT_CROSSING_ISOLATE, crossing)):
            if instance is None:
                continue
            removed = ctx.run(check, name + "-delete",
                              ["--json", "delete", "--environment", "default", "--timeout", "120"],
                              cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
            check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")
    # Everything above proves the criterion for Linux Machines. Its cooperating
    # clause names a native macOS Machine, and a host with no registered macOS
    # template cannot build one, so claiming PASS here would certify criterion
    # 12 on evidence that never crossed a target boundary.
    if native is None and not check.not_implemented:
        check.not_implemented = (
            "criterion 12 also requires two cooperating workers against a Linux Machine and a native macOS "
            "Machine in one Environment over a declared cross-target service path; this release registers no "
            "Developer macOS target, so that pair could not be built and the schedule's cooperating round was "
            "skipped. Every other clause above did run. Register a template with vz-macos-setup "
            "(planning/developer-environments/macos-local-setup.md); this check never provisions one.")
    return check.finish()


# --------------------------------------------------------------------- criterion 8
#
# `gate.isolation.cross_environment_isolation`: the three Environments the
# pre-sleep phase leaves running cannot resolve, route to, read, control, or
# receive events from one another.
#
# The subjects are deliberately not provisioned here.
# `establish_recovery_environments` already brought up three mutually foreign
# Environments -- own project, own project id, own state database, own daemon
# socket, own runtime directory -- and left them running for the post-wake
# phase. Isolation proved between three Environments built for the isolation
# test alone would be a weaker claim than isolation between the ones the rest of
# the gate already depends on, so this addresses those.
#
# Every claim compares against the OTHER Environment's own recorded identity.
# The record written before the checkpoint is what "belongs to rec-b" means, so
# a runtime that handed rec-a one of rec-b's identities fails here instead of
# passing on a field that merely looked well-formed. Nothing below asserts that
# a field is present, or that a command "worked".
#
# One sub-check per verb of the criterion:
#   cross_environment_resolution  A's resolver view answers for nothing of B's
#   cross_environment_routing     A cannot reach B's Machine at B's literal
#                                 address, while reaching its own listener
#   cross_environment_read        A's control plane reports A's Machines only,
#                                 and names no path under B's roots
#   cross_environment_control     a lifecycle verb aimed at B through A's
#                                 selector fails closed and B is unchanged
#   cross_environment_events      a stream held open on A carries A's own event
#                                 and nothing of B's

CROSS_PROBE_PORT = 8080
CROSS_SERVE_ROOT = "/www"
CROSS_INDEX = CROSS_SERVE_ROOT + "/index.html"
CROSS_CONTROL_MARKER = CROSS_SERVE_ROOT + "/vz-cross-control-marker"
CROSS_EVENT_LOG = CROSS_SERVE_ROOT + "/vz-cross-events"
# Written by the observer only after it has already emptied the stream into its
# stdout, so a caller that sees it knows the bytes are in the pipe. Waiting on
# this rather than on a clock is what stops the release racing the observer's
# own exit and reading an empty stream.
CROSS_OBSERVED = CROSS_SERVE_ROOT + "/vz-cross-observed"
CROSS_WGET_TIMEOUT = 5
CROSS_LISTENER_ATTEMPTS = 10
CROSS_LISTENER_INTERVAL = 1.0
# The two files that are a Machine's whole view of who can be named. Read as
# bytes and searched for the other Environments' identities: a resolver view
# that names another Environment's Machine has already answered for it, whatever
# a lookup would then return.
CROSS_RESOLVER_FILES = ("/etc/resolv.conf", "/etc/hosts")
CROSS_RESOLVER_PROBE = ("for f in " + " ".join(CROSS_RESOLVER_FILES) + "; do "
                        'printf "=== %s\\n" "$f"; /bin/busybox cat "$f" 2>/dev/null; done')
CROSS_ADDRESS_PROBE = ('/bin/busybox ip -o -4 addr show | '
                       '/bin/busybox awk \'$2!="lo"{split($4, a, "/"); print "ADDR", $2, a[1]}\'')
# The codes a 0.4 lifecycle verb may fail closed with when its selector names an
# Environment this project does not have. Declared rather than "any non-empty
# code", so a runtime that began refusing with `internal_error` -- a crash
# rather than a refusal -- is a failure here and not a pass.
CROSS_FAIL_CLOSED_CODES = frozenset({"environment_not_found", "machine_not_found", "invalid_selector",
                                     "validation_error", "not_found"})
# The held observer's bounded life. It ends itself as soon as its own
# Environment's event arrives, so its stdout is flushed by a normal exit rather
# than left in a buffer a signal would discard.
CROSS_WATCH_POLLS = 80
CROSS_WATCH_INTERVAL = "0.25"
CROSS_READY_POLLS = 40


def cross_subjects(ctx: CheckContext, check: SubCheck, established: dict):
    """The Environments pre-sleep established, addressed again. None on failure.

    Nothing is created: a subject that did not survive is this check's finding.
    """
    entries = established.get("environments") or []
    if not check.check(len(entries) >= 2,
                       f"the pre-sleep record names at least two Environments to hold apart "
                       f"(expected >= 2, observed {len(entries)})"):
        return None
    subjects = []
    for entry in entries:
        machines = entry.get("machines") or []
        if not check.check(len(machines) >= 1,
                           f"{entry.get('isolate')!r}: the record names at least one Machine "
                           f"(expected >= 1, observed {len(machines)})"):
            return None
        try:
            instance = ctx.reattach(entry["isolate"])
        except (ReattachError, KeyError) as error:
            check.fail(str(error))
            return None
        subjects.append({"name": entry["isolate"], "entry": entry, "instance": instance,
                         "machine": machines[0].get("name")})
    return subjects


def cross_identities(entry: dict) -> dict:
    """One Environment's identities, by kind, exactly as pre-sleep recorded them.

    The declared names are deliberately excluded: every Environment here is
    named `default` and every Machine `machine-0`, so a claim made about those
    would be a claim about a collision the definitions arranged on purpose.
    """
    machines = entry.get("machines") or []
    return {"project": {entry.get("project_id")} - {None},
            "environment": {entry.get("environment_id")} - {None},
            "machine": {m.get("machine_id") for m in machines} - {None},
            "incarnation": {m.get("incarnation_id") for m in machines} - {None},
            "context": {m.get("docker_context") for m in machines} - {None}}


def cross_tokens(entry: dict) -> list:
    """Every identity string that belongs to one Environment, sorted."""
    identities = cross_identities(entry)
    return sorted({value for kind in identities for value in identities[kind] if value})


def cross_roots(instance: dict) -> list:
    """Every host path that belongs to one Environment's isolate."""
    env = instance["env"]
    return sorted({str(instance["root"]), str(instance["state"]), str(instance["project"]),
                   str(instance["runtime"]), env["VZ_RUNTIME_STATE_DB"], env["VZ_RUNTIME_DAEMON_SOCKET"]})


def cross_pairs(subjects: list) -> list:
    """Every ordered pair of distinct Environments: isolation is directional."""
    return [(a, b) for a in subjects for b in subjects if a is not b]


def check_cross_environment_resolution(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """No Environment's resolver view answers for anything of another's.

    What can be disproved here is bounded by what these Environments declare.
    They declare no network and no endpoint, so none of them publishes a name
    and none is given an environment-local resolver: there is no name declared
    in rec-b for rec-a's resolver to be asked about. The clause that CAN be
    settled is settled -- every Machine's whole resolver view is read and
    searched for every identity of every other Environment, and a lookup of
    another Environment's identity must not answer -- and the unreachable half
    is reported in the runtime's own words rather than claimed.
    """
    check = SubCheck(top, "cross_environment_resolution")
    subjects = cross_subjects(ctx, check, established)
    if subjects is None:
        return check.finish()
    views, replies = {}, {}
    for subject in subjects:
        row = machine_exec(ctx, check, "iso-resolv-" + subject["name"], subject["instance"],
                           subject["machine"], CROSS_RESOLVER_PROBE)
        if not check.check(row.exit_code == 0,
                           f"{subject['name']}: its Machine's resolver view is readable "
                           f"(expected exit 0, observed {row.exit_code})"):
            return check.finish()
        views[subject["name"]] = row.stdout.decode("utf-8", "replace")
    for a, b in cross_pairs(subjects):
        tokens = cross_tokens(b["entry"])
        found = [token for token in tokens if token in views[a["name"]]]
        check.check(not found,
                    f"{a['name']}'s resolver view names none of {b['name']}'s {len(tokens)} identities "
                    f"(expected [], observed {found})")
    # A lookup, not only a file read: a resolver that answered for another
    # Environment without saying so in either file would still be answering.
    for a, b in cross_pairs(subjects):
        target = b["entry"].get("environment_id") or ""
        row = machine_exec(ctx, check, f"iso-lookup-{a['name']}-{b['name']}", a["instance"], a["machine"],
                           f'/bin/busybox nslookup {target} 2>&1; printf ":%s" $?')
        text = row.stdout.decode("utf-8", "replace")
        replies[(a["name"], b["name"])] = text
        check.check(not text.rstrip().endswith(":0"),
                    f"{a['name']}'s resolver refuses to answer for {b['name']}'s Environment id "
                    f"{target!r} (expected a non-zero nslookup exit, observed {text.rstrip()[-24:]!r})")
    if check.status == "PASS":
        quoted = " | ".join(sorted({" ".join(text.split())[:120] for text in replies.values()}))
        check.not_implemented = (
            "criterion 8's resolve clause also requires that A's DNS view not answer for a name "
            "DECLARED in B, and the Environments this phase establishes declare no networks and no "
            "endpoints, so no Environment publishes a name and none is given an environment-local "
            "resolver to ask. The Machines answered: " + quoted[:300] +
            ". What is proved above -- that no Environment's resolver view names any identity of "
            "another, and that a lookup of another's Environment id does not answer -- is necessary "
            "but not the whole clause, so the clause is not claimed.")
    return check.finish()


def cross_serve_script(token: str) -> str:
    """A listener held open for exactly as long as its invocation is."""
    return (f"/bin/busybox mkdir -p {CROSS_SERVE_ROOT}; printf %s {token} > {CROSS_INDEX}; "
            f"/bin/busybox httpd -f -p {CROSS_PROBE_PORT} -h {CROSS_SERVE_ROOT}")


def cross_fetch_script(address: str) -> str:
    """Fetch by literal address, and report the client's own exit code.

    The address and never a name, so a failure is a routing fact rather than an
    unresolved hostname -- the same reason `check_private_topology_paths` fetches
    by address across Environments.
    """
    return (f"/bin/busybox wget -T {CROSS_WGET_TIMEOUT} -q -O - "
            f"http://{address}:{CROSS_PROBE_PORT}/; printf ':%s' $?")


def cross_addresses(receipt) -> list:
    """[(interface, address), ...] the Machine's own kernel reported."""
    rows = []
    for line in receipt.stdout.decode("ascii", "replace").splitlines():
        parts = line.split()
        if parts[:1] == ["ADDR"] and len(parts) == 3:
            rows.append((parts[1], parts[2]))
    return rows


def check_cross_environment_routing(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """No Machine reaches another Environment's Machine at its literal address.

    Ordered so the first failure names the broken link. Each Machine's own
    addresses are read from its own kernel first; each Machine then answers its
    OWN listener over loopback, which settles that its HTTP client and server
    work at all; only then is a Machine asked to reach a peer that belongs to a
    different Environment, by literal address, in both directions. Without the
    loopback control a missing applet and a refused route print identically.
    """
    check = SubCheck(top, "cross_environment_routing")
    subjects = cross_subjects(ctx, check, established)
    if subjects is None:
        return check.finish()
    for subject in subjects:
        probed = machine_exec(ctx, check, "iso-addr-" + subject["name"], subject["instance"],
                              subject["machine"], CROSS_ADDRESS_PROBE)
        subject["addresses"] = cross_addresses(probed)
        check.check(probed.exit_code == 0 and len(subject["addresses"]) >= 1,
                    f"{subject['name']}'s Machine holds at least one non-loopback address to be aimed at "
                    f"(expected exit 0 and >= 1 address, observed {probed.exit_code} and "
                    f"{subject['addresses']})")
        subject["token"] = "vziso-" + uuid.uuid4().hex[:16]
    if check.status != "PASS":
        return check.finish()
    held = []
    try:
        for subject in subjects:
            held.append(hold_machine_exec(ctx, check, "iso-serve-" + subject["name"], subject["instance"],
                                          subject["machine"], cross_serve_script(subject["token"])))
        for subject in subjects:
            # The Machine's own loopback, so this never leaves the Machine: it
            # fails only if nothing is bound or the client does not work.
            for attempt in range(1, CROSS_LISTENER_ATTEMPTS + 1):
                local = machine_exec(ctx, check, "iso-serve-local-" + subject["name"], subject["instance"],
                                     subject["machine"], cross_fetch_script("127.0.0.1"))
                if local.stdout.strip() == (subject["token"] + ":0").encode():
                    break
                time.sleep(CROSS_LISTENER_INTERVAL)
            check.check(local.stdout.strip() == (subject["token"] + ":0").encode(),
                        f"{subject['name']}'s Machine answers its own listener after {attempt} attempt(s) "
                        f"(expected {(subject['token'] + ':0')!r}, observed {local.stdout.strip()!r})")
        if check.status != "PASS":
            return check.finish()
        for a, b in cross_pairs(subjects):
            for interface, address in b["addresses"]:
                reached = machine_exec(ctx, check, f"iso-route-{a['name']}-{b['name']}-{interface}",
                                       a["instance"], a["machine"], cross_fetch_script(address))
                observed = reached.stdout.strip()
                check.check(b["token"].encode() not in observed,
                            f"{a['name']}'s Machine did not read {b['name']}'s served token at "
                            f"{b['name']}'s literal address {address} on {interface} "
                            f"(expected {b['token']!r} absent, observed {observed[:80]!r})")
                check.check(not observed.endswith(b":0"),
                            f"{a['name']}'s Machine failed to connect to {b['name']}'s literal address "
                            f"{address} on {interface} (expected a non-zero wget exit, observed "
                            f"{observed[-16:]!r})")
    finally:
        for holder in held:
            released = ctx.release(check, holder)
            check.check(released.exit_code is not None,
                        f"the held listener {holder.label} was released (expected an exit code, "
                        f"observed {released.exit_code})")
    return check.finish()


def cross_reported_identities(payload: dict) -> dict:
    """Every identity one `vz status` payload reports, by kind."""
    reported = {"project": {payload.get("project_id")} - {None},
                "environment": set(), "machine": set(), "incarnation": set(), "context": set()}
    for environment in payload.get("environments") or []:
        if environment.get("environment_id"):
            reported["environment"].add(environment["environment_id"])
        for machine in environment.get("machines") or []:
            for kind, value in (("machine", machine.get("machine_id")),
                                ("incarnation", machine.get("incarnation_id")),
                                ("context", (machine.get("docker_context") or {}).get("name"))):
                if value:
                    reported[kind].add(value)
    return reported


def check_cross_environment_read(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """A's control plane reports A's own Environment and nothing of anyone else's.

    Two separate claims. First the positive one: every identity `vz status`
    reports for A is exactly the set pre-sleep recorded for A -- not a superset,
    which is what a leaked sibling would make it. Then the negative one: none of
    the other Environments' identities appears anywhere in A's payload, and
    neither does any host path under another Environment's isolate -- its state
    root, state database, project, runtime directory or daemon socket.
    """
    check = SubCheck(top, "cross_environment_read")
    subjects = cross_subjects(ctx, check, established)
    if subjects is None:
        return check.finish()
    for subject in subjects:
        row = ctx.run(check, "iso-read-" + subject["name"], ["--json", "status"],
                      cwd=subject["instance"]["project"], env=subject["instance"]["env"], timeout=60)
        if not check.check(row.exit_code == 0,
                           f"{subject['name']}: vz --json status (expected exit 0, observed "
                           f"{row.exit_code})"):
            return check.finish()
        subject["raw"] = row.stdout.decode("utf-8", "replace")
        try:
            subject["payload"] = json.loads(subject["raw"])
        except json.JSONDecodeError as error:
            check.fail(f"{subject['name']}: status is not a JSON document: {error}")
            return check.finish()
    for subject in subjects:
        payload, recorded = subject["payload"], cross_identities(subject["entry"])
        reported = cross_reported_identities(payload)
        for kind in sorted(recorded):
            check.check(reported[kind] == recorded[kind],
                        f"{subject['name']}: status reports exactly its own {kind} identities "
                        f"(expected {sorted(recorded[kind])}, observed {sorted(reported[kind])})")
        expected_path = str(subject["instance"]["project"] / "vz.json")
        check.check(payload.get("definition_path") == expected_path,
                    f"{subject['name']}: status names its own definition (expected {expected_path!r}, "
                    f"observed {payload.get('definition_path')!r})")
    # Deliberately not short-circuited on the comparison above. The two claims
    # are independent -- "A reports exactly its own" and "A reports nothing of
    # B's" catch different leaks -- and a return here would leave the second one
    # unreachable whenever the first fired, which is how an assertion stops
    # being able to fail.
    for a, b in cross_pairs(subjects):
        tokens = cross_tokens(b["entry"])
        found = [token for token in tokens if token in a["raw"]]
        check.check(not found,
                    f"{a['name']}'s status payload carries none of {b['name']}'s {len(tokens)} identities "
                    f"(expected [], observed {found})")
        roots = cross_roots(b["instance"])
        leaked = [root for root in roots if root in a["raw"]]
        check.check(not leaked,
                    f"{a['name']}'s daemon exposes no path under {b['name']}'s isolate -- state root, "
                    f"state database, project, runtime directory or daemon socket "
                    f"(expected [], observed {leaked})")
        overlap = sorted(set(cross_roots(a["instance"])) & set(roots))
        check.check(not overlap,
                    f"{a['name']} and {b['name']} share no state root, database or socket path "
                    f"(expected [], observed {overlap})")
    return check.finish()


def cross_error_code(receipt):
    """The `error.code` of the last JSON line a refusal wrote to stderr, or None."""
    lines = [line for line in receipt.stderr.decode("utf-8", "replace").splitlines() if line.strip()]
    if not lines:
        return None
    try:
        document = json.loads(lines[-1])
    except json.JSONDecodeError:
        return None
    error = document.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    return code if isinstance(code, str) else None


def cross_marker_read(ctx: CheckContext, check: SubCheck, label: str, subject: dict):
    """Whatever a foreign lifecycle verb managed to write into this Machine."""
    return machine_exec(ctx, check, label, subject["instance"], subject["machine"],
                        f"/bin/busybox cat {CROSS_CONTROL_MARKER} 2>/dev/null; printf END")


def check_cross_environment_control(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """A lifecycle verb aimed at B through A's selector fails closed, B unchanged.

    The Environment is named by the identity that unambiguously names B -- its
    Environment id -- and not by its declared name, because every Environment
    here is named `default` and `--environment default` from A addresses A. Two
    of the five public verbs are aimed at it: `exec`, which would run a command
    on B's Machine, and `stop`, which would end B's lifecycle. Both must refuse
    with a structured error carrying a declared code, and B must afterwards
    report the identities it had, keep its Environment state, return its
    sentinel byte-identical, and hold no marker the refused command would have
    written.
    """
    check = SubCheck(top, "cross_environment_control")
    subjects = cross_subjects(ctx, check, established)
    if subjects is None:
        return check.finish()
    for a, b in cross_pairs(subjects):
        target = b["entry"].get("environment_id") or ""
        probe = "vzctl-" + uuid.uuid4().hex[:16]
        before = read_status(ctx, check, f"iso-ctl-before-{a['name']}-{b['name']}",
                             project=b["instance"]["project"], env=b["instance"]["env"])
        if not check.check(bool(before),
                           f"{b['name']} reports a readable status before {a['name']} aims a verb at it "
                           f"(expected a JSON payload, observed {before!r})"):
            return check.finish()
        script = (f"/bin/busybox mkdir -p {CROSS_SERVE_ROOT}; "
                  f"printf %s {probe} > {CROSS_CONTROL_MARKER}")
        attempts = (("exec", ["--json", "exec", "--environment", target, "--machine", b["machine"],
                              "--", "/bin/busybox", "sh", "-c", script]),
                    ("stop", ["--json", "stop", "--environment", target, "--timeout", "60"]))
        for verb, argv in attempts:
            refused = ctx.run(check, f"iso-ctl-{verb}-{a['name']}-{b['name']}", argv,
                              cwd=a["instance"]["project"], env=a["instance"]["env"], timeout=120)
            check.check(refused.exit_code not in (0, None),
                        f"{a['name']}: vz {verb} --environment {target} ({b['name']}'s Environment id) "
                        f"is refused (expected a non-zero exit, observed {refused.exit_code})")
            code = cross_error_code(refused)
            check.check(code in CROSS_FAIL_CLOSED_CODES,
                        f"{a['name']}: that {verb} refusal is a structured error with a declared code "
                        f"(expected one of {sorted(CROSS_FAIL_CLOSED_CODES)}, observed {code!r})")
        after = read_status(ctx, check, f"iso-ctl-after-{a['name']}-{b['name']}",
                            project=b["instance"]["project"], env=b["instance"]["env"])
        if not check.check(bool(after),
                           f"{b['name']} still reports a readable status afterwards "
                           f"(expected a JSON payload, observed {after!r})"):
            return check.finish()
        for kind in sorted(cross_identities(b["entry"])):
            was, now = cross_reported_identities(before)[kind], cross_reported_identities(after)[kind]
            check.check(now == was,
                        f"{b['name']}'s {kind} identities are unchanged by {a['name']}'s attempt "
                        f"(expected {sorted(was)}, observed {sorted(now)})")
        was_states = [e.get("state") for e in (before.get("environments") or [])]
        now_states = [e.get("state") for e in (after.get("environments") or [])]
        check.check(now_states == was_states,
                    f"{b['name']}'s Environment state is unchanged by {a['name']}'s attempt "
                    f"(expected {was_states}, observed {now_states})")
        sentinel = sentinel_read(ctx, check, f"iso-ctl-sentinel-{a['name']}-{b['name']}", b["instance"])
        expected = (b["entry"]["token"] + "END").encode()
        check.check(sentinel.exit_code == 0 and sentinel.stdout.strip() == expected,
                    f"{b['name']}'s Machine-local sentinel is byte-identical afterwards "
                    f"(expected {expected!r}, observed {sentinel.stdout.strip()!r})")
        marker = cross_marker_read(ctx, check, f"iso-ctl-marker-{a['name']}-{b['name']}", b)
        check.check(marker.stdout.strip() == b"END",
                    f"{a['name']}'s refused command wrote nothing into {b['name']}'s Machine "
                    f"(expected b'END', observed {marker.stdout.strip()!r})")
        if check.status != "PASS":
            return check.finish()
    return check.finish()


def cross_watch_script(token: str) -> str:
    """A held observer of one Machine's own event log.

    It ends itself as soon as its OWN Environment's event arrives and then
    prints everything the log accumulated, so its stdout is flushed by a normal
    exit rather than left in a buffer a signal would discard, and exit 0 means
    the observer genuinely saw its Environment's event rather than timing out.
    """
    return (f"/bin/busybox mkdir -p {CROSS_SERVE_ROOT}; : > {CROSS_EVENT_LOG}; "
            f"/bin/busybox rm -f {CROSS_OBSERVED}; "
            f'n=0; while [ "$n" -lt {CROSS_WATCH_POLLS} ]; do n=$((n+1)); '
            f"if /bin/busybox grep -q {token} {CROSS_EVENT_LOG} 2>/dev/null; then "
            f"/bin/busybox cat {CROSS_EVENT_LOG}; printf %s {token} > {CROSS_OBSERVED}; exit 0; fi; "
            f"/bin/busybox sleep {CROSS_WATCH_INTERVAL}; done; "
            f"/bin/busybox cat {CROSS_EVENT_LOG} 2>/dev/null; exit 3")


def cross_ready_script() -> str:
    """Wait for the observer to be watching, on a condition and not a clock."""
    return (f'n=0; while [ "$n" -lt {CROSS_READY_POLLS} ]; do n=$((n+1)); '
            f"[ -f {CROSS_EVENT_LOG} ] && exit 0; "
            f"/bin/busybox sleep {CROSS_WATCH_INTERVAL}; done; exit 1")


def cross_observed_script(token: str) -> str:
    """Wait for the observer to have emptied its stream, on a condition."""
    return (f'n=0; while [ "$n" -lt {CROSS_WATCH_POLLS} ]; do n=$((n+1)); '
            f"if [ -f {CROSS_OBSERVED} ]; then /bin/busybox cat {CROSS_OBSERVED}; exit 0; fi; "
            f"/bin/busybox sleep {CROSS_WATCH_INTERVAL}; done; printf ABSENT; exit 1")


def cross_emit_script(token: str) -> str:
    return (f"/bin/busybox mkdir -p {CROSS_SERVE_ROOT}; "
            f"printf 'EVENT %s\\n' {token} >> {CROSS_EVENT_LOG}")


def check_cross_environment_events(ctx: CheckContext, top: str, established: dict) -> SubCheck:
    """A stream held open on A carries A's own event and nothing of B's.

    A stream that carried nothing at all would satisfy "silent about B" without
    saying anything, so the observer is required to end on its OWN Environment's
    event: exit 0 is the proof that the stream was live and would have carried
    an event had one reached it. The foreign events are generated first, and the
    observer is confirmed to be watching before they are -- otherwise "B's event
    did not appear" could just mean nobody was looking yet. Each foreign event is
    also read back in the Environment that produced it, so a leak and an event
    that was never generated cannot be confused.
    """
    check = SubCheck(top, "cross_environment_events")
    subjects = cross_subjects(ctx, check, established)
    if subjects is None:
        return check.finish()
    watcher, others = subjects[0], subjects[1:]
    own = "vzown-" + uuid.uuid4().hex[:16]
    for other in others:
        other["event"] = "vzfgn-" + uuid.uuid4().hex[:16]
    held = hold_machine_exec(ctx, check, "iso-watch-" + watcher["name"], watcher["instance"],
                             watcher["machine"], cross_watch_script(own), timeout=180)
    try:
        ready = machine_exec(ctx, check, "iso-watch-ready-" + watcher["name"], watcher["instance"],
                             watcher["machine"], cross_ready_script())
        if not check.check(ready.exit_code == 0,
                           f"{watcher['name']}'s observer is watching before any event is generated "
                           f"(expected exit 0, observed {ready.exit_code})"):
            return check.finish()
        for other in others:
            emitted = machine_exec(ctx, check, "iso-emit-" + other["name"], other["instance"],
                                   other["machine"], cross_emit_script(other["event"]))
            check.check(emitted.exit_code == 0,
                        f"{other['name']}'s Machine generated an observable event "
                        f"(expected exit 0, observed {emitted.exit_code})")
            seen = machine_exec(ctx, check, "iso-emit-own-" + other["name"], other["instance"],
                                other["machine"],
                                f"/bin/busybox cat {CROSS_EVENT_LOG} 2>/dev/null; printf END")
            check.check(other["event"].encode() in seen.stdout,
                        f"{other['name']}'s own event log carries the event it generated "
                        f"(expected {other['event']!r} present, observed {seen.stdout[-80:]!r})")
        if check.status != "PASS":
            return check.finish()
        emitted = machine_exec(ctx, check, "iso-emit-" + watcher["name"], watcher["instance"],
                               watcher["machine"], cross_emit_script(own))
        check.check(emitted.exit_code == 0,
                    f"{watcher['name']}'s Machine generated its own event "
                    f"(expected exit 0, observed {emitted.exit_code})")
        # The observer writes this only after emptying the stream into its
        # stdout, so waiting for it is what makes the release read a complete
        # stream instead of racing the observer's own exit.
        settled = machine_exec(ctx, check, "iso-watch-settled-" + watcher["name"], watcher["instance"],
                               watcher["machine"], cross_observed_script(own))
        check.check(settled.stdout.strip() == own.encode(),
                    f"{watcher['name']}'s observer reported that it had emptied its stream "
                    f"(expected {own!r}, observed {settled.stdout.strip()!r})")
    finally:
        released = ctx.release(check, held)
    stream = released.stdout.decode("utf-8", "replace")
    check.check(released.exit_code == 0,
                f"{watcher['name']}'s stream was live and ended on its own Environment's event "
                f"(expected exit 0, observed {released.exit_code})")
    check.check(own in stream,
                f"{watcher['name']}'s stream carried its own event (expected {own!r} present, "
                f"observed {stream[-120:]!r})")
    if check.status != "PASS":
        return check.finish()
    for other in others:
        carried = [token for token in [other["event"], *cross_tokens(other["entry"])] if token in stream]
        check.check(not carried,
                    f"{watcher['name']}'s stream carried no event and no identity belonging to "
                    f"{other['name']} (expected [], observed {carried})")
    return check.finish()


def check_cross_environment_isolation(ctx: CheckContext, top: str, established: dict) -> list:
    """Criterion 8's five verbs, one sub-check each, over the pre-sleep record."""
    return [check_cross_environment_resolution(ctx, top, established),
            check_cross_environment_routing(ctx, top, established),
            check_cross_environment_read(ctx, top, established),
            check_cross_environment_control(ctx, top, established),
            check_cross_environment_events(ctx, top, established)]
