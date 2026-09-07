"""Three ordered phases: clean-provision, persisted-recovery (pre-sleep,
hardware sleep/wake checkpoint, post-wake), final-cleanup.

Step 1 records the sleep/wake checkpoint as `observed: false` (a FAIL finding)
and the final-cleanup leak diff as not performed (a FAIL finding). Both become
real in later build-order steps; nothing here pretends otherwise.
"""
from __future__ import annotations

from pathlib import Path

import vz04_lanes as lanes
from vz04_common import PHASES, canonical_digest, digest_file, document, utc_iso

PHASE_LANE_PHASES = {
    "clean-provision": ("clean-provision",),
    "persisted-recovery": ("persisted-recovery/pre-sleep", "persisted-recovery/post-wake"),
    "final-cleanup": ("final-cleanup",),
}


def lane_dir(root: Path, lane_name: str, lane_phase: str) -> Path:
    return root / lane_name / lane_phase


def assemble_handoff(root: Path, run_id: str, candidate_tuple_sha256: str, results: list) -> Path:
    """Content-addressed state-handoff.<sha256>.json from clean-provision results."""
    rows, sentinels = [], []
    for result in results:
        produced = result["handoff"]["produced"]
        produced_sha256 = None
        if produced:
            path = lane_dir(root, result["lane"], result["phase"]) / produced
            if path.is_file() and not path.is_symlink():
                produced_sha256 = digest_file(path)
                sentinels.append({"lane": result["lane"], "path": f"{result['lane']}/{result['phase']}/{produced}", "sha256": produced_sha256})
        rows.append({"lane": result["lane"], "outcome": result["outcome"], "produced": produced, "produced_sha256": produced_sha256})
    complete = bool(sentinels) and all(row["outcome"] == "passed" for row in rows if row["lane"] != "sandbox-vm")
    body = {"schema_version": 1, "kind": "vz-0.4-state-handoff", "run_id": run_id, "phase": "clean-provision",
            "produced_at_utc": utc_iso(), "candidate_tuple_sha256": candidate_tuple_sha256, "complete": complete,
            "lanes": rows, "sentinels": sentinels}
    directory = root / "phases" / "clean-provision"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"state-handoff.{canonical_digest(body)}.json"
    document(path, body)
    return path


def record_sleep_wake_not_observed(root: Path, run_id: str, minimum_sleep_seconds: int) -> Path:
    directory = root / "phases" / "persisted-recovery"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "sleep-wake.json"
    document(path, {"schema_version": 1, "kind": "vz-0.4-sleep-wake", "run_id": run_id,
                    "minimum_sleep_seconds": minimum_sleep_seconds, "observed": False, "reason": "not_observed_step1",
                    "checkpoint": None, "wake": None})
    return path


def record_leak_diff_not_performed(root: Path) -> Path:
    directory = root / "phases" / "final-cleanup"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "leak-diff.json"
    document(path, {"performed": False, "survivors": [], "reason": "host inventories and leak diff not implemented (build order step 2)"})
    return path


def run_phases(root: Path, contract: dict, ctx: lanes.LaneContext, *, dry: bool, observer=None) -> dict:
    """Execute every phase. Returns {phases: [manifest rows], results: [lane results]}."""
    ordered_lanes = contract["lanes"]
    results = []
    phase_rows = []
    handoff = None
    for phase in PHASES:
        row = {"name": phase, "started_at_utc": utc_iso(), "finished_at_utc": None, "lanes": [],
               "handoff_path": None, "sleep_wake_path": None, "leak_diff_path": None}
        for lane_phase in PHASE_LANE_PHASES[phase]:
            if lane_phase == "persisted-recovery/post-wake":
                sleep_path = record_sleep_wake_not_observed(root, ctx.run_id, contract["sleep_wake"]["minimum_sleep_seconds"])
                row["sleep_wake_path"] = str(sleep_path.relative_to(root))
            for lane in ordered_lanes:
                if lane_phase not in lane["phases"]:
                    continue
                directory = lane_dir(root, lane["name"], lane_phase)
                result = lanes.invoke_lane(lane, lane_phase, ctx, directory, handoff, dry=dry)
                results.append(result)
                row["lanes"].append({"lane": lane["name"], "phase": lane_phase,
                                     "result_path": str((directory / "lane-result.json").relative_to(root)),
                                     "outcome": result["outcome"],
                                     "failure_reason": None if result["failure"] is None else result["failure"]["reason"]})
                if observer is not None:
                    observer(phase, lane["name"], lane_phase, result)
        if phase == "clean-provision":
            handoff_path = assemble_handoff(root, ctx.run_id, ctx.candidate_tuple_sha256,
                                            [r for r in results if r["phase"] == "clean-provision"])
            row["handoff_path"] = str(handoff_path.relative_to(root))
            handoff = str(handoff_path)
        if phase == "final-cleanup":
            row["leak_diff_path"] = str(record_leak_diff_not_performed(root).relative_to(root))
        row["finished_at_utc"] = utc_iso()
        phase_rows.append(row)
    return {"phases": phase_rows, "results": results}
