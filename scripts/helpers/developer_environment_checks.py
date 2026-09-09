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
        env = self.state.env(HOME=root / "absent-home", VZ_RUNTIME_STATE_DB=state_dir / "stack-state.db",
                             VZ_RUNTIME_DATA_DIR=runtime, VZ_RUNTIME_DAEMON_SOCKET=runtime / "d.sock",
                             VZ_DOCKER_CONFIG=state_dir / "docker", **overrides)
        return {"root": root, "state": state_dir, "project": project, "runtime": runtime, "env": env}


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
        definition = two_machine_definition(ctx.release_dir)
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
    check.check(sorted(names) == ["machine-0", "machine-1"], f"both declared Machines are present (observed {names})")
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
    if check.status == "PASS":
        entry = macos_target(ctx.release_dir)
        if entry is None:
            check.not_implemented = (
                "criterion 5 requires a required service path crossing between a Linux Machine and a "
                "native macOS Machine in both directions; this release registers no Developer macOS "
                "target, so no macOS Machine could be built and the crossing was never attempted. "
                "The Linux-to-Linux half above passed. Register a template with vz-macos-setup "
                "(planning/developer-environments/macos-local-setup.md); this check never provisions one.")
        else:
            check.not_implemented = (
                "a Developer macOS target is registered but the Linux-to-macOS crossing is not yet "
                "exercised by this check; crossing_definition() builds the declaration it needs.")
    return check.finish()


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
# every other field must appear. `test_developer_environment_e2e` reads all six
# sets out of `crates/vz-cli/src/commands/dev_status.rs` rather than restating
# them, so the next drift fails offline instead of on the gate host.
STATUS_FIELDS = {"schema_version", "request_id", "topology_state_source", "definition_path", "project_id",
                 "project_name", "host", "daemon", "desired_definition_digest", "persisted_definition_digest",
                 "definition_drift", "selection_source", "environments"}
STATUS_OPTIONAL_FIELDS = {"selection_source"}
ENVIRONMENT_FIELDS = {"environment_id", "name", "state", "definition_digest", "lifecycle_generation", "machines"}
ENVIRONMENT_OPTIONAL_FIELDS: set[str] = set()
MACHINE_FIELDS = {"machine_id", "name", "state", "profile", "target", "requested_capabilities",
                  "negotiated_capabilities", "backend", "incarnation_id", "incarnation_generation",
                  "docker_context", "docker_context_availability"}
MACHINE_OPTIONAL_FIELDS = {"backend", "incarnation_id", "incarnation_generation", "docker_context",
                           "docker_context_availability"}


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


def check_grpc_agreement(top: str) -> SubCheck:
    check = SubCheck(top, "grpc_api_live_agreement")
    # The lane provisions now, so Machines are no longer what blocks this. What
    # is missing is a typed gRPC client: agreement must be observed over the
    # daemon's own channel, not inferred from the CLI's JSON of the same state.
    check.not_implemented = ("CLI vs typed gRPC/API agreement (identities, transitions, events, receipts) needs a pinned "
                             "gRPC client for the daemon channel; the lane provisions Machines but speaks only the CLI.")
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
    '/bin/busybox ip -o -4 addr show | /bin/busybox awk \'$2!="lo"{print "ADDR", $2, $4}\''
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

    def evidence(self) -> str:
        return (f"cmdline gateways {self.gateways!r}, vz.dns {self.resolver_args!r}, "
                f"resolv.conf {self.resolv_conf!r}, addresses {self.addresses!r}, "
                f"hosts {self.hosts!r}")


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
    * and no listener attributable to this run appeared on the host's LAN or on
      any wildcard address while all of it was running, while the edge's own
      address is bound nowhere on the host at all -- both read off the host's
      real listener table before and after rather than inferred from the
      absence of a bind in the source.

    What it does NOT prove, and says so rather than passing on the rest: the
    TLS, routed-ingress and address-translation clauses. See the
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
    # about the whole machine rather than about vz. The attribution rule is the
    # lane's own leak-diff rule: a pid among the processes that reference this
    # run, or a row lsof could not attribute at all.
    scoped = {row["pid"] for row in after["processes"]}
    exposed = [row for row in appeared
               if row.get("scope") != "loopback"
               and (row.get("pid") in scoped or row.get("pid") is None)]
    check.check(not exposed,
                f"no listener on the host LAN or a wildcard address appeared while both edges ran "
                f"({len(appeared)} new listeners, {len(scoped)} processes attributable to this run, "
                f"exposed {exposed[:5]})")
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
    # also requires that the client reach the API through TLS, routed ingress
    # and NAT, and none of that was exercised here, because the client could not
    # be: the Developer Linux guest image ships no TLS client that can talk to
    # this edge.
    #
    # BusyBox 1.37.0's `ssl_client` (the only TLS in the image, reached by
    # `wget https://`) reads exactly one handshake message per TLS record, and
    # rustls coalesces its whole TLS 1.2 server flight into one record; the
    # handshake deadlocks. That is not a server setting: capping the record size
    # splits the flight on byte boundaries and BusyBox then rejects it outright.
    # And `networking/wget.c` force-sets no-check-certificate and prints "TLS
    # certificate validation not implemented", so even a completed handshake
    # would verify nothing -- it could not tell this Environment's authority
    # from any other, which is most of what the TLS clause is for.
    #
    # Reporting PASS on the DNS and host-listener clauses would certify the
    # criterion on evidence that never touched its TLS, ingress or translation
    # clauses. The edge implements all three and they are exercised end to end
    # over a real switch, by a real TLS client against a real origin that
    # reports the peer address it saw, in
    # `crates/vz-runtimed/src/environment_gateway_tests.rs`. That is
    # component-level evidence and it is not this criterion's evidence: nothing
    # in it boots a Machine.
    if check.status == "PASS":
        check.not_implemented = (
            "criterion 6's TLS, routed-ingress and NAT clauses were not exercised from inside a "
            "Machine. The Developer Linux guest image carries no TLS client that can reach this "
            "edge: BusyBox 1.37.0 `ssl_client` reads one handshake message per TLS record while "
            "rustls coalesces its TLS 1.2 server flight into one, so the handshake never completes, "
            "and `wget` force-sets no-check-certificate, so it could not verify the Environment's "
            "authority even if it did. The split-DNS, `.test`-hostname, edge-versus-origin, "
            "cross-Environment and host-listener clauses above all passed. Closing this criterion "
            "needs a certificate-verifying HTTPS client inside a Developer Linux Machine; the edge "
            "itself terminates TLS, routes by SNI and translates the source address, proved over a "
            "real switch in crates/vz-runtimed/src/environment_gateway_tests.rs.")
    return check.finish()
