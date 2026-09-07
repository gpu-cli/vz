"""Physical sub-checks of the `topology` lane that the installed 0.4 binaries
can prove today without provisioning a Machine.

Criterion 21 (`gate.cli.legacy_removal_and_bootstrap`):
  bare_help            bare `vz` == pinned snapshot, exit 0, no discovery/mutation
  legacy_rejection     every removed root/flag/nested path in the CLI-removal
                       inventory returns exit 2 + one-line structured JSON
  clean_up_refuses     `vz up` in a clean directory fails `definition_not_found`
                       with zero state mutation and no daemon
  bootstrap_read_only  a schema-valid minimal vz.json is read by `vz status`
                       without spawning a daemon or creating state
  bootstrap_creates_default   NOT IMPLEMENTED (needs a real Up)
Criterion 15 (`gate.cli_api.agreement`):
  help_surface_exact   root/subcommand help is exactly the five verbs
  error_envelope_agreement   `up` and `status` agree on `definition_not_found`
  status_json_field_set      NOT IMPLEMENTED (needs persisted topology)
  grpc_api_live_agreement    NOT IMPLEMENTED

Sub-check ids are `<top-level id>__<slug>` (the lane-result schema allows
exactly three dot-separated `[a-z_]+` segments, so a fourth `.slug` segment
is not schema-valid). Every assertion is recorded from raw receipts.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import stat
import uuid

from jsonschema import Draft202012Validator

from developer_environment_recorder import (LaneState, Recorder, inventory, inventory_diff, processes_referencing,
                                            write_inventory)
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
                 cli_removal: dict):
        self.repo_root = Path(repo_root)
        self.release_dir = Path(release_dir)
        self.state = state
        self.recorder = recorder
        self.evidence_dir = Path(evidence_dir)
        self.cli_removal = cli_removal

    def run(self, check: SubCheck, label: str, argv: list, *, cwd: Path, env: dict, timeout: int = 5):
        receipt = self.recorder.run(label, [self.state.cli, *argv], cwd=cwd, env=env, scenario_id=check.id, timeout=timeout)
        check.evidence.extend(self.recorder.receipt_paths(receipt))
        return receipt

    def isolated(self, name: str, *, project_files: dict) -> dict:
        """`<lane state>/<name>/{state,project}` plus an absent HOME."""
        root = self.state.root / name
        root.mkdir(mode=0o700)
        state_dir = root / "state"
        state_dir.mkdir(mode=0o700)
        project = root / "project"
        project.mkdir(mode=0o700)
        for filename, data in project_files.items():
            write_exclusive(project / filename, data)
        return {"root": root, "state": state_dir, "project": project,
                "env": self.state.env(HOME=root / "absent-home", VZ_RUNTIME_STATE_DB=state_dir / "stack-state.db",
                                      VZ_RUNTIME_DATA_DIR=state_dir / "runtime",
                                      VZ_RUNTIME_DAEMON_SOCKET=state_dir / "runtime" / "runtimed.sock",
                                      VZ_DOCKER_CONFIG=state_dir / "docker", **{"CARGO_BIN_EXE_vz-runtimed": self.state.absent_daemon})}


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
    check.check(not os.path.lexists(iso["env"]["VZ_RUNTIME_DAEMON_SOCKET"]), "no daemon socket created")
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
    check.check(not os.path.lexists(env["VZ_RUNTIME_DAEMON_SOCKET"]) and not os.path.lexists(ctx.state.socket), "no daemon socket created")
    live = processes_referencing(ctx.state)
    check.check(not live, "no live process references the lane state root" if not live else f"live processes: {live[:3]}")
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
    check.check(not diff, "lane state root identical: no state DB, runtime dir or socket created" if not diff
                else "lane state root changed: " + "; ".join(diff[:6]))
    live = processes_referencing(ctx.state)
    check.check(not live, "no live process references the lane state root" if not live else f"live processes: {live[:3]}")
    return check.finish()


def check_bootstrap_creates_default(top: str) -> SubCheck:
    check = SubCheck(top, "bootstrap_creates_default")
    check.not_implemented = ("creating `default` from a schema/API-only bootstrap requires a real `vz up` (Machine provisioning); "
                             "this skeleton never provisions. Verified experimentally: a fresh daemon answers `status` with "
                             "project_not_found until Up persists topology.")
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


def check_status_field_set(top: str) -> SubCheck:
    check = SubCheck(top, "status_json_field_set")
    check.not_implemented = ("`vz status --json` success output (schema_version 1, request_id, topology_state_source, definition_path, "
                             "project_id, project_name, host, daemon, digests, definition_drift, environments[]) requires persisted "
                             "topology from a real Up; a fresh daemon answers project_not_found. Not provisioned by this skeleton.")
    return check.finish()


def check_grpc_agreement(top: str) -> SubCheck:
    check = SubCheck(top, "grpc_api_live_agreement")
    check.not_implemented = "CLI vs typed gRPC/API agreement over a live topology (identities, transitions, events, receipts) needs Machines."
    return check.finish()
