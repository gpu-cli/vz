# vz 0.4 aggregate release gate

Status: **build-order steps 2–3, host side (DEV)**. The gate runs today and
fails honestly: every one of the 85 required scenarios is `MISSING`, three
acceptance lanes are explicit `not_implemented` stubs, and the sandbox lane is
translated from its `summary.txt`. Host inventories, the leak diff, client
provenance and the hardware sleep/wake checkpoint are real; the lanes are not.
Nothing here is release evidence until the verdict is `PASS` from a clean
checkout against a `developer-id-notarized` release.

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
`HOME`, `TMPDIR` (under the state root), `LANG`, `VZ04_RUN_ID`,
`VZ_DOCKER_CONFIG` (`<state root>/docker`, the only Docker config the run may
touch), plus `CARGO_HOME`/`~/.cargo/bin` on `PATH`, and `LINUX_DOCKER_CONTEXT`
only when `--linux-docker-context` is given (the sandbox lane needs it).

Client paths are admitted as given (absolute, executable) because OrbStack and
Docker Desktop ship multi-call binaries dispatched by `argv[0]`; the manifest
records the invocation path, its resolved target, the target's SHA-256 and the
reported version (`docker version --format '{{.Client.Version}}'`,
`docker-cli-plugin-metadata` for the plugins).

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
4. **Host facts and inventories** (`vz04_host.py`). The manifest records
   `sw_vers`, `hw.model`, `hw.memsize`, `hw.ncpu`, `kern.bootsessionuuid`,
   `kern.boottime`, free disk on the state root (`statvfs`), the client
   provenance above, `uv`/`cargo`/`cargo nextest`/`rustc`/`/usr/bin/python3`
   versions and the SHA-256 of `gate-requirements.txt`. `host/before.json`
   (`vz-0.4-host-inventory`) is captured after the prerequisites: listeners from
   `lsof -nP -iTCP -sTCP:LISTEN -iUDP -F pcPnT` cross-checked with `netstat -an
   -f inet -f inet6` (rows carry protocol, address, port, pid, command, a
   loopback/wildcard/non-loopback classification and which tool saw them),
   processes from `ps -axo pid=,ppid=,command=` scoped to the run (command line
   mentions the run-id, the state root or the release `bin/`; the gate's own
   ancestry is excluded and listed), Unix sockets under the state root, and
   Docker contexts in the isolated `VZ_DOCKER_CONFIG`. A tool failure makes the
   inventory `partial` with the error recorded (a FAIL finding).
5. **Phases** (`vz04_phases.py`): `clean-provision` (lanes, then a
   content-addressed `phases/clean-provision/state-handoff.<sha256>.json`),
   `persisted-recovery` (`pre-sleep` lane invocations, the sleep/wake checkpoint
   below, `post-wake` lane invocations with the handoff), `final-cleanup` (lanes,
   then `host/after.json` and `phases/final-cleanup/leak-diff.json`). Survivors
   are: every run-scoped process still alive, every socket under the state
   root, every non-`default` Docker context not present before, and every new
   listener that is owned by a run-scoped pid, has no attributable pid, or is a
   wildcard bind (`listener_checks.wildcard_listeners_forbidden`). New
   loopback/non-loopback listeners attributable to unrelated processes are
   counted but are not survivors. Any survivor is a FAIL finding.

### Hardware sleep/wake (`vz04_sleepwake.py`)

Between the `pre-sleep` and `post-wake` invocations the gate writes
`phases/persisted-recovery/sleep-wake-checkpoint.json`: run-id, a 32-byte hex
nonce, `kern.bootsessionuuid`, `kern.boottime`, `CLOCK_MONOTONIC`,
`CLOCK_UPTIME_RAW`, wall clock (UTC) and the SHA-256 of `pmset -g log`. It then
blocks for the operator: Enter on the controlling TTY (`/dev/tty`) or the
appearance of `--sleep-wake-ack-file` containing the nonce, within
`deadlines_seconds.operator_ack` (1800 s). No TTY and no ack file is
`operator_ack_missing`; an ack file without the nonce is `nonce_mismatch`.

After the ack the checkpoint is re-read from disk, the clocks are captured
again, the Sleep/Wake/DarkWake rows of `pmset -g log` and the powerd/kernel
sleep-wake messages of `log show --style json --start … --end … --predicate
'subsystem == "com.apple.powerd" OR process == "kernel"'` inside the window are
collected (bounded to 300 s and 64 MiB; a timeout, truncation or error is
recorded as `unified_log.state` and is a FAIL finding, never silent), and
`discontinuity_ns = ΔMONOTONIC − ΔUPTIME_RAW` is computed: on macOS
`CLOCK_MONOTONIC` keeps counting across sleep while `CLOCK_UPTIME_RAW` stops.
`observed: true` requires the same boot session, the nonce echoed from disk, a
pmset `Sleep` row followed by a `Wake`/`DarkWake` row inside the window, and a
discontinuity of at least `sleep_wake.minimum_sleep_seconds` (20 s); otherwise
`reason` names the first failed binding. Under `--dry-lanes` the checkpoint is
written but no ack is awaited (`reason: dry_lanes`); there is no other bypass.

Empirically on the development Mac (macOS 26.3.1, `/usr/bin/python3` 3.9.6):
both `time.CLOCK_MONOTONIC` and `time.CLOCK_UPTIME_RAW` are available and
already differ by the accumulated sleep since boot; `pmset -g log` is readable
unprivileged (~108k rows); `log show` over a one-minute window returns in
about 1–2.5 s.
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
├── host/{before,after}.json                            vz-0.4-host-inventory
├── phases/clean-provision/state-handoff.<sha256>.json  vz-0.4-state-handoff
├── phases/persisted-recovery/sleep-wake-checkpoint.json vz-0.4-sleep-wake-checkpoint (written before the ack)
├── phases/persisted-recovery/sleep-wake.json           vz-0.4-sleep-wake
├── phases/final-cleanup/leak-diff.json                 vz-0.4-leak-diff
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
process-start, undeclared readiness poll and prohibited runtime checks; both
host inventories present, schema-valid, bound to the run (run-id, moment, state
root) and `captured`; the leak diff present, equal to the manifest's
`leak_diff`, recomputed from the two inventories (`cleanup.leak_diff_mismatch`
otherwise) and empty (`cleanup.survivors` otherwise); the sleep/wake record
judged by `vz04_sleepwake.verify` with the persisted checkpoint matching;
client provenance recorded for all three clients with the resolved binary's
digest unchanged. `PASS` only with zero findings.

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
scripts/run-vz-0.4-release-gate.sh --suite all --release-dir "$PWD/<dir>" --run-id gate-dry-smoke-2 --dry-lanes \
  --docker /usr/local/bin/docker --compose-plugin <compose path> --buildx-plugin <buildx path>   # DEV: expect FAIL, 85 MISSING
scripts/validate-vz-0.4-evidence.sh .artifacts/vz-0.4-e2e/gate-dry-smoke-2/manifest.json           # reproduces FAIL, identical findings
```

The dry smoke on the development Mac reports `findings=109` (113 before the
host inventories, leak diff and client provenance landed) with both
inventories `captured`, an empty leak diff and `sleep_wake.not_observed:
dry_lanes`.

## GA parity in `release.yml`

`.github/workflows/release.yml` builds with `cargo build --locked` and, before
any `codesign` step, writes `dist/release-manifest-presign.json`: the unsigned
`shasum -a 256` of the five host binaries and `normalized_content_sha256 =
sha256(canonical_json(sorted [[path, unsigned_sha256], …]) + LF)` computed with
`vz04_source_tree.py canonical-sha256`, the same rule as the release-candidate
builder. Its scope is the host binaries only (`components_scope:
host-binaries`); the builder's digest additionally covers guest bundles,
BuildKit and the entitlements file.

## What is still missing

Native lane results from the Docker and sandbox lanes, the topology and native
lanes themselves, a real (non-dry) sleep/wake observation recorded against a
candidate, and input freezing. Each lands against this gate; until then the
honest verdict is FAIL.
