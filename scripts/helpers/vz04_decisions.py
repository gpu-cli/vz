"""Scope/exclusion decision verification (SSHSIG ed25519 via `ssh-keygen -Y`).

Signed bytes: canonical JSON of the decision without its `signature` member,
plus "\\n". Detached signatures live under `config/vz-0.4-decision-signatures/`.
Verification also requires ancestry: `effective_commit`, the authority's
`valid_from_commit`, and the last commit touching the decision files must all
be strict ancestors of the candidate commit. Docker `intentional_exclusions`
must cite a verified decision.

The private signing key never lives in the repository (see docs).
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile

import vz04_schema as schema
from vz04_common import (CONFIG_FILES, DECISION_SIGNATURES_DIR, REPO_ROOT, GateError, canonical_json, git,
                         git_is_strict_ancestor, load_json, read_regular, require, sha256_bytes)

NAMESPACE = "vz-0.4-decision"


def signed_bytes(decision: dict) -> bytes:
    body = {key: value for key, value in decision.items() if key != "signature"}
    return (canonical_json(body) + "\n").encode("utf-8")


def allowed_signers(authorities: dict, *, include_revoked: bool = False) -> str:
    lines = []
    for authority in authorities["authorities"]:
        if authority["revoked"] and not include_revoked:
            continue
        lines.append(f'{authority["principal"]} namespaces="{NAMESPACE}" {authority["public_key"]}\n')
    return "".join(lines)


def ssh_keygen() -> str:
    for candidate in ("/usr/bin/ssh-keygen", "/usr/local/bin/ssh-keygen", "/opt/homebrew/bin/ssh-keygen"):
        if os.access(candidate, os.X_OK):
            return candidate
    raise GateError("ssh-keygen not found")


def sign(decision: dict, private_key: Path) -> bytes:
    """Produce a detached SSHSIG for a decision (used by owners and by tests)."""
    with tempfile.TemporaryDirectory(prefix="vz04-sign-") as directory:
        payload = Path(directory) / "decision.bin"
        payload.write_bytes(signed_bytes(decision))
        completed = subprocess.run([ssh_keygen(), "-Y", "sign", "-f", str(private_key), "-n", NAMESPACE, str(payload)],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, check=False)
        require(completed.returncode == 0, "ssh-keygen -Y sign failed: " + completed.stderr.decode("utf-8", "replace"))
        return payload.with_suffix(".bin.sig").read_bytes()


def verify_signature(decision: dict, signature: bytes, authorities: dict) -> tuple:
    """(ok, detail) using ssh-keygen -Y verify against generated allowed_signers."""
    principal = decision["approval"]["principal"]
    with tempfile.TemporaryDirectory(prefix="vz04-verify-") as directory:
        signers = Path(directory) / "allowed_signers"
        signers.write_text(allowed_signers(authorities))
        signature_path = Path(directory) / "decision.sig"
        signature_path.write_bytes(signature)
        completed = subprocess.run([ssh_keygen(), "-Y", "verify", "-f", str(signers), "-I", principal, "-n", NAMESPACE,
                                    "-s", str(signature_path)], input=signed_bytes(decision), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, timeout=60, check=False)
        detail = (completed.stdout + completed.stderr).decode("utf-8", "replace").strip()
        return completed.returncode == 0, detail


def verify_decisions(candidate_commit: str, *, repo_root: Path = REPO_ROOT, decisions: dict = None,
                     authorities: dict = None, ancestry: bool = True) -> dict:
    """Return {verified: [ids], findings: [(code, subject, detail)]}."""
    findings = []
    if decisions is None:
        decisions = load_json(repo_root / CONFIG_FILES["decisions"])
    if authorities is None:
        authorities = load_json(repo_root / CONFIG_FILES["decision_authorities"])
    for name, value in (("decisions", decisions), ("decision-authorities", authorities)):
        for problem in schema.validate(name, value):
            findings.append(("decision.schema", name, problem))
    if findings:
        return {"verified": [], "findings": findings}

    keys = {authority["key_id"]: authority for authority in authorities["authorities"]}
    require(len(keys) == len(authorities["authorities"]), "duplicate authority key_id")
    if ancestry:
        for authority in authorities["authorities"]:
            if not git_is_strict_ancestor(repo_root, authority["valid_from_commit"], candidate_commit):
                findings.append(("decision.authority_ancestry", authority["key_id"],
                                 f"valid_from_commit {authority['valid_from_commit'][:12]} is not a strict ancestor of the candidate"))
        touched = git(repo_root, "log", "-1", "--format=%H", "--", CONFIG_FILES["decisions"],
                      CONFIG_FILES["decision_authorities"], DECISION_SIGNATURES_DIR, check=False)
        if decisions["decisions"] and touched and not git_is_strict_ancestor(repo_root, touched, candidate_commit):
            findings.append(("decision.file_ancestry", CONFIG_FILES["decisions"],
                             f"last commit touching decision files {touched[:12]} is not a strict ancestor of the candidate"))
        if decisions["decisions"] and git(repo_root, "status", "--porcelain=v1", "--", CONFIG_FILES["decisions"],
                                          CONFIG_FILES["decision_authorities"], DECISION_SIGNATURES_DIR):
            findings.append(("decision.uncommitted", CONFIG_FILES["decisions"], "decision files have uncommitted changes"))

    verified = []
    seen = set()
    for decision in decisions["decisions"]:
        identifier = decision["id"]
        if identifier in seen:
            findings.append(("decision.duplicate", identifier, "duplicate decision id"))
            continue
        seen.add(identifier)
        authority = keys.get(decision["approval"]["key_id"])
        if authority is None or authority["revoked"] or authority["principal"] != decision["approval"]["principal"]:
            findings.append(("decision.authority", identifier, "approval key is unknown, revoked, or principal mismatch"))
            continue
        signature_file = decision["signature"]["file"]
        expected_file = f"{DECISION_SIGNATURES_DIR}/{identifier}.sig"
        if signature_file != expected_file:
            findings.append(("decision.signature_path", identifier, f"signature file must be {expected_file}"))
            continue
        try:
            signature = read_regular(repo_root / signature_file, 64 * 1024)
        except GateError as error:
            findings.append(("decision.signature_missing", identifier, str(error)))
            continue
        if sha256_bytes(signed_bytes(decision)) != decision["signature"]["signed_sha256"]:
            findings.append(("decision.signed_digest", identifier, "signed_sha256 does not match the canonical decision bytes"))
            continue
        ok, detail = verify_signature(decision, signature, authorities)
        if not ok:
            findings.append(("decision.signature_invalid", identifier, detail[:300]))
            continue
        if ancestry:
            if not git_is_strict_ancestor(repo_root, decision["effective_commit"], candidate_commit):
                findings.append(("decision.effective_ancestry", identifier, "effective_commit is not a strict ancestor of the candidate"))
                continue
            if not git_is_strict_ancestor(repo_root, authority["valid_from_commit"], decision["effective_commit"]) \
                    and authority["valid_from_commit"] != decision["effective_commit"]:
                findings.append(("decision.authority_window", identifier, "authority key was not yet valid at effective_commit"))
                continue
        verified.append(identifier)
    return {"verified": verified, "findings": findings}


def exclusion_findings(docker: dict, verified: list) -> list:
    findings = []
    for exclusion in docker.get("intentional_exclusions", []):
        decision_id = exclusion.get("decision_id") if isinstance(exclusion, dict) else None
        if decision_id not in verified:
            findings.append(("docker.exclusion_unverified", str(exclusion)[:80], "intentional exclusion does not cite a verified decision"))
    return findings
