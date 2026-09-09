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
                       exactly its declared field set
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
# One probe, run on EVERY Machine on the network. Reporting only the server left
# the sibling's fabric NIC unobserved, so a missing NIC there was indistinguishable
# from a forwarding fault.
FABRIC_PROBE = ("/bin/busybox cat /proc/cmdline "
                "| /bin/busybox tr ' ' '\\n' "
                "| /bin/busybox grep '^vz.net.' ; printf '|' ; "
                "/bin/busybox ip -o -4 addr show "
                "| /bin/busybox awk '$2!=\"lo\"{print $2\" \"$4}'")


def fabric_state(receipt):
    """(declared cmdline addresses, [[iface, cidr], ...]) from one FABRIC_PROBE run."""
    declared_part, _, observed_part = receipt.stdout.decode("ascii", "replace").partition("|")
    declared = [item.split(",")[1].split("/")[0] for item in declared_part.split()
                if item.startswith("vz.net.") and "," in item]
    observed = [line.split() for line in observed_part.strip().splitlines() if line.split()]
    return declared, observed
PRIVATE_PORT = 8080
WGET_TIMEOUT = 5


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


def machine_exec(ctx, check, label, instance, machine, script, *, timeout=120):
    return ctx.run(check, label, ["exec", "--environment", "default", "--machine", machine, "--",
                                  "/bin/busybox", "sh", "-c", script],
                   cwd=instance["project"], env=instance["env"], timeout=timeout)


def check_private_topology_paths(ctx: CheckContext, top: str) -> SubCheck:
    """A declared private path serves inside its Environment and nowhere else.

    One Machine serves a token on a declared private network; its sibling in the
    same Environment must read exactly that token, and a Machine in a different
    Environment must fail to reach the very same address. The foreign probe uses
    the address rather than a name, so its failure is a routing fact and not an
    unresolved hostname.
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
    served = machine_exec(ctx, check, "net-serve", inside, "machine-0",
                          f"/bin/busybox mkdir -p /www; printf %s {token} > /www/index.html; "
                          f"/bin/busybox httpd -p {PRIVATE_PORT} -h /www; printf STARTED")
    check.check(served.exit_code == 0 and served.stdout.strip().endswith(b"STARTED"),
                f"the private endpoint is serving on machine-0 (exit {served.exit_code})")
    if check.status != "PASS":
        return check.finish()
    # The address comes from the serving Machine itself: the foreign probe must
    # target the same address, so its refusal is about routing and not a name.
    #
    # Every Machine now has TWO IPv4 interfaces: Apple's NAT `eth0`, and the
    # fabric NIC the declared network gives it. Taking the first non-lo entry
    # returned eth0's 192.168.64.x, so the sibling probe was routed over the
    # host-shared NAT segment instead of the private fabric -- which is not what
    # this check claims to test, and would be a false pass if NAT ever carried it.
    # Report every interface so a missing fabric NIC is legible in the evidence.
    # Name-based selection is not sound: a Machine carries Apple's NAT eth0, the
    # fabric NIC, and Docker's own bridge. Ask the guest which address the HOST
    # derived -- vz.net.N=<mac>,<ipv4>/<prefix> is on its kernel cmdline -- and
    # require an interface to actually carry it. That proves the derived address
    # reached the guest, which name matching never could.
    addressed = machine_exec(ctx, check, "net-address", inside, "machine-0",
                             FABRIC_PROBE)
    declared, observed = fabric_state(addressed)
    carried = {addr.split("/")[0] for _, addr in observed}
    check.check(addressed.exit_code == 0 and len(declared) == 1 and declared[0] in carried,
                "machine-0 carries the fabric address the host derived "
                f"(cmdline {declared!r}, interfaces {observed!r})")
    address = declared[0] if len(declared) == 1 and declared[0] in carried else ""
    # The sibling is the one that must REACH that address, so its own fabric port
    # is part of the claim. Observing only the server made a missing NIC here look
    # like a forwarding fault.
    sibling_state = machine_exec(ctx, check, "net-sibling-address", inside, "machine-1", FABRIC_PROBE)
    sib_declared, sib_observed = fabric_state(sibling_state)
    sib_carried = {addr.split("/")[0] for _, addr in sib_observed}
    check.check(sibling_state.exit_code == 0 and len(sib_declared) == 1
                and sib_declared[0] in sib_carried and sib_declared[0] != address,
                "machine-1 carries its own distinct fabric address "
                f"(cmdline {sib_declared!r}, interfaces {sib_observed!r})")
    if check.status != "PASS":
        return check.finish()
    sibling = machine_exec(ctx, check, "net-sibling", inside, "machine-1",
                           f"/bin/busybox wget -T {WGET_TIMEOUT} -q -O - http://{address}:{PRIVATE_PORT}/")
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
    if check.status == "PASS":
        for name, instance in (("net-a", inside), ("net-b", outside)):
            removed = ctx.run(check, name + "-delete",
                              ["--json", "delete", "--environment", "default", "--timeout", "120"],
                              cwd=instance["project"], env=instance["env"], timeout=DELETE_TIMEOUT)
            check.check(removed.exit_code == 0, f"{name}: deleted (exit {removed.exit_code})")
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
# topology, observed on the installed 0.4 binaries.
STATUS_FIELDS = {"schema_version", "request_id", "topology_state_source", "definition_path", "project_id",
                 "project_name", "host", "daemon", "desired_definition_digest", "persisted_definition_digest",
                 "definition_drift", "selection_source", "environments"}


def check_status_field_set(ctx: CheckContext, top: str) -> SubCheck:
    """`vz status --json` over a live topology emits exactly its declared fields.

    Extra fields are as much a contract break as missing ones, so the set is
    compared exactly rather than by presence. The digests must agree with each
    other and with an undrifted definition, which is what makes them evidence
    rather than two unrelated strings.
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
    check.check(set(payload) == STATUS_FIELDS,
                "status emits exactly its declared field set" if set(payload) == STATUS_FIELDS else
                f"field set differs: missing {sorted(STATUS_FIELDS - set(payload))}, "
                f"unexpected {sorted(set(payload) - STATUS_FIELDS)}")
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
