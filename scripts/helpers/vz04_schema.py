"""JSON Schema loading and validation for vz 0.4 gate inputs and evidence.

Uses the hash-pinned `jsonschema` from `gate-requirements.txt`; a hand-written
subset validator would silently under-enforce `if/then`, `$ref`, `uniqueItems`
and `const`. Every schema lives at `schemas/vz-0.4-<name>.schema.json` or, for
shared inputs, at the path recorded in `SHARED_SCHEMAS`.
"""
from __future__ import annotations

from pathlib import Path

from jsonschema import Draft202012Validator

from vz04_common import REPO_ROOT, GateError, load_json

SHARED_SCHEMAS = {
    "docker-compatibility": "schemas/docker-compatibility-v0.4.schema.json",
    "host-target-capabilities": "schemas/host-target-capabilities-v0.4.schema.json",
    "release-manifest": "schemas/vz-0.4-release-manifest.schema.json",
}
INPUT_SCHEMAS = ("e2e-contract", "migration-barriers", "decisions", "decision-authorities")
EVIDENCE_SCHEMAS = ("gate-manifest", "lane-result", "summary", "state-handoff", "sleep-wake",
                    "connectivity-matrix", "runtime-provenance", "resource-inventory", "receipt", "run-index")

_cache: dict = {}


def schema_path(name: str, repo_root: Path = REPO_ROOT) -> Path:
    if name in SHARED_SCHEMAS:
        return repo_root / SHARED_SCHEMAS[name]
    return repo_root / "schemas" / f"vz-0.4-{name}.schema.json"


def load_schema(name: str, repo_root: Path = REPO_ROOT) -> dict:
    path = schema_path(name, repo_root)
    key = str(path)
    if key not in _cache:
        schema = load_json(path)
        Draft202012Validator.check_schema(schema)
        _cache[key] = schema
    return _cache[key]


def schema_available(name: str, repo_root: Path = REPO_ROOT) -> bool:
    path = schema_path(name, repo_root)
    return path.is_file() and not path.is_symlink()


def validate(name: str, value, repo_root: Path = REPO_ROOT, limit: int = 12) -> list:
    """Return human-readable schema findings (empty when valid)."""
    validator = Draft202012Validator(load_schema(name, repo_root))
    problems = sorted(validator.iter_errors(value), key=lambda error: list(map(str, error.absolute_path)))
    findings = []
    for error in problems[:limit]:
        location = "/".join(map(str, error.absolute_path)) or "<root>"
        findings.append(f"schema {name}: {location}: {error.message[:200]}")
    if len(problems) > limit:
        findings.append(f"schema {name}: {len(problems) - limit} further violations")
    return findings


def require_valid(name: str, value, repo_root: Path = REPO_ROOT) -> None:
    findings = validate(name, value, repo_root)
    if findings:
        raise GateError("; ".join(findings))
