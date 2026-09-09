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
