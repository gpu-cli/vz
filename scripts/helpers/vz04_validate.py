"""Read-only validator for an aggregate evidence root.

`validate_root(root)` reproduces the gate's verdict independently: schema
validation of every evidence file, `checksums.sha256` coverage, frozen-input /
release / tuple / index recomputation, required-scenario accounting, summary
versus raw recomputation, canary scan, and the retry / duplicate process-start
/ undeclared readiness poll / prohibited runtime / leak / sleep-wake checks.
PASS only with zero findings. The gate itself calls `evaluate` on the same
code path and then re-runs `validate_root` to prove it reproduces.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import docker_host_driver as driver
import vz04_candidate as candidate
import vz04_contract as contract_module
import vz04_decisions as decisions
import vz04_lanes as lanes
import vz04_schema as schema
from vz04_common import (CANARY_PREFIX, REPO_ROOT, GateError, canonical_digest, canonical_path, digest_file, load_json,
                         read_regular, tree_entries, verify_checksums)

KIND_TO_SCHEMA = {
    "vz-0.4-gate-manifest": "gate-manifest", "vz-0.4-lane-result": "lane-result", "vz-0.4-summary": "summary",
    "vz-0.4-state-handoff": "state-handoff", "vz-0.4-sleep-wake": "sleep-wake", "vz-0.4-connectivity-matrix": "connectivity-matrix",
    "vz-0.4-runtime-provenance": "runtime-provenance", "vz-0.4-resource-inventory": "resource-inventory",
    "vz-0.4-receipt": "receipt", "vz-0.4-run-index": "run-index",
}
UNKINDED_ALLOWED = frozenset(("invocation.json", "leak-diff.json", "facts.json", "lane-result.rejected.json"))
MAX_SCAN = 512 * 1024 * 1024


class Findings:
    def __init__(self):
        self.rows = []

    def add(self, code: str, subject: str, detail: str) -> None:
        self.rows.append({"code": code, "subject": str(subject)[:200], "detail": str(detail)[:500]})

    def extend(self, triples) -> None:
        for code, subject, detail in triples:
            self.add(code, subject, detail)

    def sorted(self) -> list:
        unique = {(r["code"], r["subject"], r["detail"]): r for r in self.rows}
        return [unique[key] for key in sorted(unique)]


def _load_evidence(root: Path, relative, kind_schema: str, findings: Findings, subject: str):
    if not relative:
        findings.add("evidence.missing", subject, f"no {kind_schema} evidence recorded")
        return None
    path = root / relative
    if not path.is_file() or path.is_symlink():
        findings.add("evidence.missing", subject, f"{relative} is absent")
        return None
    try:
        value = load_json(path)
    except GateError as error:
        findings.add("evidence.unreadable", subject, str(error))
        return None
    problems = schema.validate(kind_schema, value)
    for problem in problems:
        findings.add("evidence.schema", relative, problem)
    return None if problems else value


def scan_canaries(root: Path, prefix: str, findings: Findings) -> None:
    canaries = [prefix.encode("utf-8")]
    for relative, _mode, size, _digest in tree_entries(root, excluded_dirs=frozenset()):
        if size > MAX_SCAN:
            findings.add("canary.unscannable", relative, "evidence file exceeds scan bound")
            continue
        data = read_regular(root / relative)
        if driver.contains_canary((data,), canaries):
            findings.add("canary.present", relative, f"evidence contains the {prefix!r} canary prefix or undecodable nesting")


def schema_check_all(root: Path, findings: Findings) -> None:
    for relative, _mode, _size, _digest in tree_entries(root, excluded_dirs=frozenset()):
        if not relative.endswith(".json"):
            continue
        name = relative.rsplit("/", 1)[-1]
        try:
            value = load_json(root / relative)
        except GateError as error:
            findings.add("evidence.unreadable", relative, str(error))
            continue
        kind = value.get("kind") if isinstance(value, dict) else None
        if kind in KIND_TO_SCHEMA:
            for problem in schema.validate(KIND_TO_SCHEMA[kind], value):
                findings.add("evidence.schema", relative, problem)
        elif name not in UNKINDED_ALLOWED and not name.startswith("state-handoff."):
            findings.add("evidence.unknown_kind", relative, f"JSON evidence without a recognized kind: {kind!r}")


def evaluate(root: Path, manifest: dict, *, repo_root: Path = REPO_ROOT, codesign_verifier=candidate.run_codesign_verify) -> dict:
    """Recompute everything from the manifest and the files on disk.

    Returns {findings, scenarios, lanes}. Independent of manifest.verdict
    except for the index consistency check, which is skipped while running.
    """
    findings = Findings()
    if manifest["developer_overrides"]:
        findings.add("gate.developer_override", "manifest", f"developer overrides used: {', '.join(manifest['developer_overrides'])}; never release evidence")
    if manifest["source"]["dirty"]:
        findings.add("source.dirty", manifest["source"]["commit"], "gate ran from a dirty checkout")

    contract = contract_module.load_contract(repo_root)
    docker = contract_module.load_docker_contract(repo_root)
    frozen = contract_module.frozen_inputs(contract, repo_root)
    findings.extend(frozen["findings"])
    for key, entry in frozen["inputs"].items():
        recorded = manifest["inputs"].get(key)
        if recorded is None:
            findings.add("input.unrecorded", key, "manifest records no digest for this input")
        elif recorded["sha256"] != entry["sha256"] or recorded["path"] != entry["path"]:
            findings.add("input.digest_mismatch", key, f"manifest {recorded['sha256']} != current {entry['sha256']}")
    for key in set(manifest["inputs"]) - set(frozen["inputs"]):
        findings.add("input.unknown", key, "manifest records an input the contract does not define")

    release = None
    try:
        release = candidate.admit_release_dir(manifest["release"]["dir"], repo_root=repo_root, codesign_verifier=codesign_verifier)
        findings.extend(release["findings"])
        for key in ("release_dir_sha256", "release_manifest_sha256", "normalized_content_sha256", "signed_content_sha256",
                    "signing_class", "release_version", "source_commit"):
            if release[key] != manifest["release"][key]:
                findings.add("release.mismatch", key, f"manifest {manifest['release'][key]} != recomputed {release[key]}")
        if release["components"] != manifest["release"]["components"]:
            findings.add("release.mismatch", "components", "component digests differ from the release directory")
        missing = [c for c in contract["release"]["required_components"] if c not in release["components"]]
        if missing:
            findings.add("release.components_missing", manifest["release"]["dir"], "required components absent: " + ", ".join(missing))
    except GateError as error:
        findings.add("release.unavailable", manifest["release"]["dir"], str(error))

    if release is not None:
        tuple_value = candidate.candidate_tuple(source_commit=manifest["release"]["source_commit"],
                                               source_tree_sha256=manifest["candidate_tuple"].get("source", {}).get("tree_sha256"),
                                               release=release, frozen=frozen,
                                               notarization_ticket_sha256=manifest["candidate_tuple"].get("distribution", {}).get("notarization_ticket_sha256"))
        findings.extend(candidate.tuple_findings(release, frozen, tuple_value))
        if tuple_value["sha256"] != manifest["candidate_tuple_sha256"] or tuple_value["tuple"] != manifest["candidate_tuple"]:
            findings.add("tuple.mismatch", "candidate_tuple", f"recomputed {tuple_value['sha256']} != manifest {manifest['candidate_tuple_sha256']}")
    if manifest["source"]["commit"] != manifest["release"]["source_commit"]:
        findings.add("source.commit_mismatch", manifest["source"]["commit"], f"checkout differs from release source commit {manifest['release']['source_commit']}")

    index_path = Path(manifest["index_path"])
    try:
        index = load_json(index_path)
        problems = schema.validate("run-index", index)
        for problem in problems:
            findings.add("index.schema", str(index_path), problem)
        if not problems:
            entries = [e for e in index["entries"] if e["run_id"] == manifest["run_id"]]
            if len(entries) != 1:
                findings.add("index.entry", manifest["run_id"], f"{len(entries)} index entries for this run-id")
            else:
                entry = entries[0]
                if entry["candidate_tuple_sha256"] != manifest["candidate_tuple_sha256"]:
                    findings.add("index.tuple", manifest["run_id"], "index tuple differs from manifest")
                if manifest["verdict"] != "running" and entry["verdict"] != manifest["verdict"]:
                    findings.add("index.verdict", manifest["run_id"], f"index {entry['verdict']} != manifest {manifest['verdict']}")
                if Path(entry["evidence_dir"]) != root:
                    findings.add("index.evidence_dir", manifest["run_id"], "index evidence_dir differs from this root")
            ids = [e["run_id"] for e in index["entries"]]
            if len(ids) != len(set(ids)):
                findings.add("index.duplicate_run_id", str(index_path), "run-id recorded more than once")
    except GateError as error:
        findings.add("index.unreadable", str(index_path), str(error))

    verdicts = decisions.verify_decisions(manifest["release"]["source_commit"], repo_root=repo_root)
    findings.extend(verdicts["findings"])
    findings.extend(decisions.exclusion_findings(docker, verdicts["verified"]))

    required = contract_module.required_scenarios(contract, docker)
    if required != manifest["required_scenarios"]:
        findings.add("scenario.inventory_mismatch", "required_scenarios", "manifest inventory differs from contract-derived inventory")

    declared_polls = {poll["id"] for poll in contract["readiness_polls"]}
    lane_defs = contract_module.lane_by_name(contract)
    results = []
    lane_rows = []
    for phase in manifest["phases"]:
        for entry in phase["lanes"]:
            lane_rows.append(dict(entry))
            result = _load_evidence(root, entry["result_path"], "lane-result", findings, f"{entry['lane']}/{entry['phase']}")
            if result is None:
                continue
            subject = f"{entry['lane']}/{entry['phase']}"
            if (result["lane"], result["phase"], result["run_id"]) != (entry["lane"], entry["phase"], manifest["run_id"]):
                findings.add("lane.identity", subject, "lane result lane/phase/run-id differ from manifest")
            if result["candidate_tuple_sha256"] != manifest["candidate_tuple_sha256"]:
                findings.add("lane.tuple", subject, "lane result bound to a different candidate tuple")
            if result["release_dir_sha256"] != manifest["release"]["release_dir_sha256"]:
                findings.add("lane.release_digest", subject, "lane result bound to a different release directory digest")
            if result["fixture_sha256"] != frozen["digests"]["fixtures_tree_sha256"]:
                findings.add("lane.fixture_digest", subject, "lane result bound to a different fixture digest")
            if result["contract_sha256"] != frozen["inputs"]["e2e_contract"]["sha256"]:
                findings.add("lane.contract_digest", subject, "lane result bound to a different contract digest")
            if result["outcome"] != entry["outcome"] or (result["failure"] or {}).get("reason") != entry["failure_reason"]:
                findings.add("lane.manifest_mismatch", subject, "manifest lane row differs from lane result")
            recorded_lane = manifest["lanes"].get(entry["lane"])
            if recorded_lane is None or result["entry_point"]["path"] != recorded_lane["entry_point"] or \
                    result["entry_point"]["sha256"] != recorded_lane["sha256"]:
                findings.add("lane.entry_point", subject, "lane entry point path/digest differ from manifest")
            if entry["lane"] in lane_defs and lane_defs[entry["lane"]]["native_result_required"] and result["result_adapter"] is not None:
                findings.add("lane.adapter_forbidden", subject, "native lane result required; translated result rejected")
            if any(result["prohibited_observed"].values()):
                findings.add("lane.prohibited", subject, "prohibited runtime/daemon/path observed: " +
                             ", ".join(k for k, v in result["prohibited_observed"].items() if v))
            if result["leaks"]:
                findings.add("lane.leaks", subject, f"{len(result['leaks'])} leaked resources")
            if result["cleanup_errors"]:
                findings.add("lane.cleanup_errors", subject, "; ".join(result["cleanup_errors"])[:300])
            starts = [s["scenario_id"] for s in result["process_starts"]]
            for duplicate in sorted({s for s in starts if starts.count(s) > 1}):
                findings.add("lane.duplicate_process_start", duplicate, f"{subject}: scenario process started more than once")
            for scenario in result["scenarios"]:
                for poll in scenario["readiness_polls"]:
                    if poll["id"] not in declared_polls:
                        findings.add("lane.undeclared_poll", scenario["id"], f"{subject}: readiness poll {poll['id']} not declared in contract")
                    elif not poll["satisfied"]:
                        findings.add("lane.poll_unsatisfied", scenario["id"], f"{subject}: readiness poll {poll['id']} not satisfied")
                if scenario["ended_unix_ns"] < scenario["started_unix_ns"]:
                    findings.add("lane.timing", scenario["id"], f"{subject}: ended before started")
            for relative in result["evidence_files"]:
                if not (root / entry["lane"] / entry["phase"] / relative).is_file():
                    findings.add("lane.evidence_missing", subject, f"declared evidence file absent: {relative}")
            results.append(result)
        if phase["name"] == "clean-provision":
            handoff = _load_evidence(root, phase["handoff_path"], "state-handoff", findings, "state-handoff")
            if handoff is not None:
                expected = canonical_digest(handoff)
                if not str(phase["handoff_path"]).endswith(f"state-handoff.{expected}.json"):
                    findings.add("handoff.address", phase["handoff_path"], "handoff filename digest differs from content")
                if not handoff["complete"]:
                    findings.add("handoff.incomplete", phase["handoff_path"], "clean-provision produced no complete persistence handoff")
        if phase["name"] == "persisted-recovery":
            sleep = _load_evidence(root, phase["sleep_wake_path"], "sleep-wake", findings, "sleep-wake")
            if sleep is not None:
                if not sleep["observed"]:
                    findings.add("sleep_wake.not_observed", phase["sleep_wake_path"], f"hardware sleep/wake not observed: {sleep['reason']}")
                else:
                    wake = sleep["wake"]
                    if wake["discontinuity_seconds"] < contract["sleep_wake"]["minimum_sleep_seconds"]:
                        findings.add("sleep_wake.short", phase["sleep_wake_path"], "clock discontinuity below minimum_sleep_seconds")
                    if not (wake["nonce_echoed"] and wake["same_boot_session"] and wake["power_events"]):
                        findings.add("sleep_wake.unbound", phase["sleep_wake_path"], "nonce/boot session/power events do not bind the interval")
        if phase["name"] == "final-cleanup":
            if not phase["leak_diff_path"] or not (root / phase["leak_diff_path"]).is_file():
                findings.add("cleanup.leak_diff_missing", "final-cleanup", "no leak diff recorded")
            else:
                diff = load_json(root / phase["leak_diff_path"])
                if not diff.get("performed"):
                    findings.add("cleanup.leak_diff_not_performed", phase["leak_diff_path"], str(diff.get("reason")))
                elif diff.get("survivors"):
                    findings.add("cleanup.survivors", phase["leak_diff_path"], ", ".join(diff["survivors"])[:300])
    recorded_phases = [phase["name"] for phase in manifest["phases"]]
    if recorded_phases != ["clean-provision", "persisted-recovery", "final-cleanup"]:
        findings.add("phases.incomplete", "manifest", f"phases recorded: {recorded_phases}")

    for lane in contract["lanes"]:
        for lane_phase in lane["phases"]:
            if not any(r["lane"] == lane["name"] and r["phase"] == lane_phase for r in results):
                findings.add("lane.result_absent", f"{lane['name']}/{lane_phase}", "no lane result for a lane/phase the contract requires")

    accounting = lanes.account(manifest["required_scenarios"], results)
    if manifest["prerequisites"]["status"] == "failed":
        for row in accounting["rows"]:
            if row["reason"] == "lane_result_absent":
                row["reason"] = "prerequisite_failed"
        accounting["findings"] = [(c, s, d.replace("lane_result_absent", "prerequisite_failed")) for c, s, d in accounting["findings"]]
    findings.extend(accounting["findings"])

    if manifest["prerequisites"]["status"] != "passed":
        findings.add("prerequisites." + manifest["prerequisites"]["status"], "prerequisites",
                     "cargo fmt/clippy/nextest prerequisites did not pass")
    for relative in manifest["prerequisites"]["receipts"]:
        _load_evidence(root, relative, "receipt", findings, relative)

    for moment in ("before", "after"):
        inventory = _load_evidence(root, manifest["inventories"][moment], "resource-inventory", findings, f"inventory.{moment}")
        if inventory is not None and inventory["capture_state"] != "captured":
            findings.add("inventory.not_captured", moment, inventory["not_captured_reason"] or "not captured")

    if manifest["clients"]["docker"] is None or manifest["clients"]["compose_plugin"] is None or manifest["clients"]["buildx_plugin"] is None:
        findings.add("clients.unrecorded", "clients", "host Docker/Compose/buildx client paths not recorded")

    scan_canaries(root, contract["canaries"]["prefix"], findings)
    return {"findings": findings.sorted(), "scenarios": accounting["rows"], "lanes": lane_rows}


def verdict_for(findings: list, scenarios: list) -> str:
    return "PASS" if not findings and scenarios and all(row["status"] == "PASS" for row in scenarios) else "FAIL"


def validate_root(root, *, repo_root: Path = REPO_ROOT, codesign_verifier=candidate.run_codesign_verify) -> dict:
    """Independent validation of an archived evidence root."""
    root = canonical_path(root)
    findings = Findings()
    manifest_path = root / "manifest.json"
    manifest = load_json(manifest_path)
    schema.require_valid("gate-manifest", manifest, repo_root)
    if manifest["verdict"] == "running":
        findings.add("gate.incomplete", "manifest", "manifest verdict is still running; the run aborted before completion")
    for problem in verify_checksums(root):
        findings.add("checksums", "checksums.sha256", problem)
    schema_check_all(root, findings)
    evaluation = evaluate(root, manifest, repo_root=repo_root, codesign_verifier=codesign_verifier)
    findings.extend((r["code"], r["subject"], r["detail"]) for r in evaluation["findings"])
    summary_path = root / "summary.json"
    if summary_path.is_file():
        summary = load_json(summary_path)
        problems = schema.validate("summary", summary, repo_root)
        for problem in problems:
            findings.add("summary.schema", "summary.json", problem)
        if not problems:
            if summary["scenarios"] != evaluation["scenarios"]:
                findings.add("summary.mismatch", "scenarios", "summary scenario rows differ from raw recomputation")
            if summary["findings"] != evaluation["findings"]:
                findings.add("summary.mismatch", "findings", "summary findings differ from raw recomputation")
            counts = {"required": len(evaluation["scenarios"]), "PASS": sum(r["status"] == "PASS" for r in evaluation["scenarios"]),
                      "FAIL": sum(r["status"] == "FAIL" for r in evaluation["scenarios"]),
                      "MISSING": sum(r["status"] == "MISSING" for r in evaluation["scenarios"]), "findings": len(evaluation["findings"])}
            if summary["counts"] != counts:
                findings.add("summary.mismatch", "counts", f"summary counts {summary['counts']} != recomputed {counts}")
            if summary["verdict"] != manifest["verdict"]:
                findings.add("summary.mismatch", "verdict", "summary verdict differs from manifest")
        if not (root / "summary.txt").is_file():
            findings.add("summary.txt_missing", "summary.txt", "human summary absent")
    else:
        findings.add("summary.missing", "summary.json", "no summary written")
    rows = findings.sorted()
    verdict = verdict_for(rows, evaluation["scenarios"])
    if manifest["verdict"] != "running" and manifest["verdict"] != verdict:
        rows.append({"code": "verdict.mismatch", "subject": "manifest", "detail": f"manifest {manifest['verdict']} != recomputed {verdict}"})
        verdict = "FAIL"
    return {"verdict": verdict, "findings": rows, "scenarios": evaluation["scenarios"], "run_id": manifest["run_id"],
            "raw_findings": evaluation["findings"]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("manifest", help="path to <evidence root>/manifest.json")
    args = parser.parse_args(argv)
    try:
        manifest = canonical_path(args.manifest)
        if manifest.name != "manifest.json":
            raise GateError("argument must be the aggregate manifest.json")
        report = validate_root(manifest.parent)
    except (GateError, OSError) as error:
        print(f"INVALID_EVIDENCE: {error}", file=sys.stderr)
        return 2
    missing = sum(r["status"] == "MISSING" for r in report["scenarios"])
    failed = sum(r["status"] == "FAIL" for r in report["scenarios"])
    passed = sum(r["status"] == "PASS" for r in report["scenarios"])
    print(f"verdict={report['verdict']} run_id={report['run_id']} required={len(report['scenarios'])} PASS={passed} FAIL={failed} "
          f"MISSING={missing} findings={len(report['findings'])}")
    for row in report["findings"]:
        print(f"finding {row['code']} {row['subject']}: {row['detail']}")
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
