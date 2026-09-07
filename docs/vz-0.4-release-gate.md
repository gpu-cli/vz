# vz 0.4 aggregate release gate

Status: **build-order step 1 (DEV)**. The gate runs today and fails honestly:
every one of the 85 required scenarios is `MISSING`, three acceptance lanes are
explicit `not_implemented` stubs, and the sandbox lane is translated from its
`summary.txt`. Nothing here is release evidence until the verdict is `PASS`
from a clean checkout against a `developer-id-notarized` release.

Normative source: [`GOAL-0.4.0.md`](../planning/developer-environments/GOAL-0.4.0.md)
("Strict E2E release gate" through "Evidence and mechanical verdict").
Implementation plan: [`RELEASE-GATE-PLAN.md`](../planning/developer-environments/RELEASE-GATE-PLAN.md).

## Entry points

```bash
scripts/build-vz-0.4-release-candidate.sh --output <new dir> --version <x.y.z[-pre]>   # builder (separate doc)
scripts/run-vz-0.4-release-gate.sh --suite all --release-dir <dir> --run-id <id> \
    --docker <path> --compose-plugin <path> --buildx-plugin <path> \
    [--evidence-root <dir>] [--state-root <dir>] [--linux-docker-context <name>] [--sleep-wake-ack-file <path>]
scripts/validate-vz-0.4-evidence.sh .artifacts/vz-0.4-e2e/<run-id>/manifest.json
```

Only `--suite all` is accepted; anything else exits 2 before any state is
touched. The Bash entry points are thin `uv run --no-project --python
/usr/bin/python3 --with-requirements scripts/helpers/gate-requirements.txt`
wrappers around `scripts/helpers/vz04_gate.py` and `vz04_validate.py`
(hash-pinned `jsonschema==4.23.0`; no global or user-site install). Exit codes:
`0` PASS, `1` FAIL, `2` input rejected before evidence was written, `3` lane
stub `not_implemented`.

Lane entry points invoked by the gate, in contract order:

| lane | entry point | role | phases |
|---|---|---|---|
| `sandbox-vm` | `scripts/run-sandbox-vm-e2e.sh --suite all --profile release --output-dir <state>/sandbox-vm-output` | prerequisite (zero acceptance IDs) | clean-provision |
| `topology` | `scripts/run-developer-environment-e2e.sh --suite all …` | acceptance | all four |
| `linux-docker` | `scripts/run-linux-docker-e2e.sh --suite all …` | acceptance (63 `docker.*` + Docker `gate.*`) | all four |
| `native-macos` | `scripts/run-macos-developer-environment-e2e.sh --suite all …` | acceptance | all four |

`run-developer-environment-e2e.sh` and `run-macos-developer-environment-e2e.sh`
are explicit failing stubs: they write a schema-valid lane result with
`outcome: failed`, `failure.reason: not_implemented` and exit 3.
`run-linux-docker-e2e.sh` rejects `--suite all` today, which the gate records as
`input_rejected`/`not_implemented`. There is no `skipped` state anywhere.

Lanes receive everything through argv, never ambient environment: `--run-id`,
`--phase`, `--release-dir`, `--evidence-dir`, `--state-root`, `--contract`,
`--candidate-tuple`, `--fixture-sha256`, `--handoff`, `--docker`,
`--compose-plugin`, `--buildx-plugin`. The minimal environment is `PATH`,
`HOME`, `TMPDIR` (under the state root), `LANG`, `VZ04_RUN_ID`, plus
`CARGO_HOME`/`~/.cargo/bin` on `PATH`, and `LINUX_DOCKER_CONTEXT` only when
`--linux-docker-context` is given (the sandbox lane needs it).

## Run flow

1. **Admission without mutation.** Run-id regex `^[a-z0-9][a-z0-9-]{7,63}$`;
   fresh `<evidence-root>/<run-id>`; the run-id is absent from
   `.artifacts/vz-0.4-e2e/index.json`; the release dir is a canonical read-only
   path whose `release-manifest.json` matches `release-manifest.sha256`, whose
   component digests, `normalized_content_sha256`, `signed_content_sha256` and
   `checksums.sha256` recompute, and whose Mach-O components pass
   `codesign --verify --strict --verbose=4`; frozen inputs load and validate;
   the candidate tuple is computed; a tuple already recorded by a real run is
   refused (a docs-only commit makes a new tuple).
2. **Evidence root and index.** `manifest.json` (`verdict: running`) and the
   index entry are written before anything runs, so an aborted run is retained.
3. **Prerequisites as receipts** under `prerequisites/`: `cargo fmt --check`,
   `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
   `cargo nextest run --workspace --all-features`. A failure stops before lanes;
   every scenario is `MISSING` for `prerequisite_failed`.
4. **Host facts and inventories.** Step 1 records `host/before-inventory.json`
   and `after-inventory.json` with `capture_state: not_captured` (a FAIL finding).
5. **Phases** (`vz04_phases.py`): `clean-provision` (lanes, then a
   content-addressed `phases/clean-provision/state-handoff.<sha256>.json`),
   `persisted-recovery` (`pre-sleep` lane invocations, the sleep/wake checkpoint,
   `post-wake` lane invocations with the handoff), `final-cleanup` (lanes, then
   the leak diff). Step 1 records the sleep/wake checkpoint as
   `observed: false, reason: not_observed_step1` and the leak diff as not
   performed; both are FAIL findings.
6. **Verdict.** `vz04_validate.evaluate` recomputes findings and scenario
   accounting; `summary.json`, `summary.txt`, the final `manifest.json`, the
   index verdict and `checksums.sha256` are written; then `validate_root` runs on
   the finished root and must reproduce the same verdict and findings. Exit 0
   only on PASS.

## Evidence layout

```text
.artifacts/vz-0.4-e2e/index.json                       append-only run index (vz-0.4-run-index)
.artifacts/vz-0.4-e2e/<run-id>/
├── manifest.json                                       vz-0.4-gate-manifest (rewritten exactly once: running → verdict)
├── summary.json / summary.txt                          vz-0.4-summary; text lists every unmet requirement
├── checksums.sha256                                    covers every other file
├── prerequisites/NNN-<label>.{intent,result}.json      vz-0.4-receipt (+ .stdout/.stderr)
├── host/{before,after}-inventory.json                  vz-0.4-resource-inventory
├── phases/clean-provision/state-handoff.<sha256>.json  vz-0.4-state-handoff
├── phases/persisted-recovery/sleep-wake.json           vz-0.4-sleep-wake
├── phases/final-cleanup/leak-diff.json
└── <lane>/<lane phase>/lane-result.json                vz-0.4-lane-result (+ invocation.json, lane.stdout, lane.stderr)
```

Lane phases are `clean-provision`, `persisted-recovery/pre-sleep`,
`persisted-recovery/post-wake`, `final-cleanup`. Every JSON evidence file
carries `kind`; the validator schema-checks each by kind and fails on unknown
kinds. Schemas live in `schemas/vz-0.4-*.schema.json` (`additionalProperties:
false`, `const` versions, `^[0-9a-f]{64}$` digests, `if/then` rules such as
"passed requires failure null and empty cleanup_errors/leaks").

## Candidate tuple

`candidate_tuple_sha256 = sha256(canonical_json(tuple) + "\n")` where
`canonical_json` is sorted-keys, compact separators (the same convention the
builder uses for `normalized_content_sha256`). The tuple covers `source
{commit, tree_sha256}` (from the release manifest), `release
{normalized_content_sha256, signed_content_sha256, signing_class,
release_manifest_sha256}`, `distribution {signed_content_sha256,
notarization_ticket_sha256}` (`null` locally; required for GA), `inputs`
(digests of the six configs, the IPSW pin, the BuildKit pin, the CLI-removal
config, the decision-signature tree), `schemas_tree_sha256`,
`fixtures_tree_sha256` (over `fixtures.required_dirs`, excluding
`__pycache__`), and `harness_tree_sha256` over the contract's explicit
`harness.files`.

## Frozen inputs

| file | schema | notes |
|---|---|---|
| `config/vz-0.4-e2e-contract.json` | `vz-0.4-e2e-contract` | 22 `gate.*` IDs with lane/phase, deadlines, tolerances, pressure, lanes, `harness.files`, `fixtures.required_dirs`, canary prefix `vz04-canary-`; `contract_state: draft_unverified` |
| `config/docker-compatibility-v0.4.json` | `docker-compatibility-v0.4` | 63 `docker.*` IDs come from `docker_compatibility_contract.REQUIRED_IDS` at runtime (never duplicated) |
| `config/vz-0.4-migration-barriers.json` | `vz-0.4-migration-barriers` | 17 barriers derived from the state-store migration stages, legacy sandbox classification, checkpoint archival, Docker context ownership and credential scope |
| `config/vz-0.4-decisions.json` | `vz-0.4-decisions` | empty; every decision needs a detached SSHSIG |
| `config/vz-0.4-decision-authorities.json` | `vz-0.4-decision-authorities` | one ed25519 key, `vz04-owner-1` |
| `config/host-target-capabilities-v0.4.json` | `host-target-capabilities-v0.4` | owned by the capability inventory work; absence is an unmet finding |

Draft state and `null` pins are unmet requirements (findings), not blockers to
execution. The 22 scenario IDs and the 63 Docker IDs make 85 required
scenarios; each must appear exactly once as `PASS` in its assigned lane/phase.

## Decisions and signatures

Decisions are signed with `ssh-keygen -Y sign -n vz-0.4-decision` over the
canonical JSON of the decision without its `signature` member plus `\n`, stored
as `config/vz-0.4-decision-signatures/<id>.sig`, and verified with `ssh-keygen
-Y verify` against an `allowed_signers` file generated from the non-revoked
authorities. Verification also requires the authority's `valid_from_commit`,
the decision's `effective_commit`, and the last commit touching the decision
files to be strict ancestors of the candidate commit. Docker
`intentional_exclusions` must cite a verified decision.

Key custody: the `vz04-owner-1` private key is `~/.ssh/vz04-decision-owner` on
the project owner's Mac. It is never checked in. Rotation is the project
owner's responsibility: add a new authority entry with a new `valid_from_commit`,
mark the old one `revoked: true` with `revoked_at_commit`, and re-sign live
decisions. Until the authorities file is committed, the validator reports
`decision.authority_ancestry` because `valid_from_commit` cannot be a strict
ancestor of the candidate commit it was created at (plan open decision 6).

## Validator

`vz04_validate.validate_root` is read-only and, run independently on an
archived root, produces the same verdict as the gate: schema validation of every
evidence file; `checksums.sha256` coverage and match; frozen-input digest
recomputation; release-dir re-admission (digests, codesign); tuple and index
consistency; required-scenario accounting; summary-versus-raw recomputation;
canary scan with the contract prefix over every file; retry, duplicate
process-start, undeclared readiness poll, prohibited runtime, leak, sleep/wake,
inventory and leak-diff checks. `PASS` only with zero findings.

## Developer-only dry mode

`--dry-lanes` substitutes each lane invocation with that lane's
`not_implemented` result and records the three cargo prerequisites as
`not_executed`, so the orchestrator/validator path is exercised without
provisioning. It is refused for `developer-id-notarized` releases, recorded in
`developer_overrides` (manifest, summary and index), produces a
`gate.developer_override` finding, and therefore can never PASS. Dry runs never
block a later real run with the same candidate tuple.

Unit tests build a fake release directory (`scripts/helpers/test_vz04_fixtures.py`)
and inject a fake codesign verifier through the `codesign_verifier` parameter /
`vz04_gate.CODESIGN_VERIFIER`; there is no CLI flag to skip codesign.

## Checks

```bash
uv run --no-project --python /usr/bin/python3 --with-requirements scripts/helpers/gate-requirements.txt \
  python -B -c 'import unittest; suite=unittest.TestSuite(); loader=unittest.TestLoader(); [suite.addTests(loader.discover("scripts/helpers",pattern=p)) for p in ("test_vz04_*.py",)]; r=unittest.TextTestRunner(verbosity=1).run(suite); raise SystemExit(not r.wasSuccessful())'
scripts/run-vz-0.4-release-gate.sh --suite all --release-dir <dir> --run-id gate-dry-smoke-1 --dry-lanes   # DEV: expect FAIL, 85 MISSING
scripts/validate-vz-0.4-evidence.sh .artifacts/vz-0.4-e2e/gate-dry-smoke-1/manifest.json                     # reproduces FAIL, identical findings
```

## What step 1 does not do yet

Real host inventories and the leak diff, the hardware sleep/wake checkpoint and
operator ack, native lane results from the Docker and sandbox lanes, the
topology and native lanes themselves, `release.yml` pre-signing digests, and
input freezing. Each lands against this gate; until then the honest verdict is
FAIL.
