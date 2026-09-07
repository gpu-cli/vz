# vz 0.4 aggregate release-gate implementation plan

Status: implementation plan (2026-09-07). Normative source:
[`GOAL-0.4.0.md`](GOAL-0.4.0.md) sections "Strict E2E release gate", "Versioned
gate inputs", "Release candidate and entry point", "Staged run and safe state
handling", "Evidence and mechanical verdict". Tracked under `vz-mzs.15.1`.

The first deliverable is an entry point, validator and contract that run today
against the existing sandbox-vm and linux-docker lanes and produce a
mechanically FAILING verdict listing every unmet requirement. Every later piece
lands against that real gate instead of accumulating focused passes.

## Ground truth to reuse

| Need | Existing code |
|---|---|
| Guest bundle rebuild (both profiles) with logs and initramfs digests | `scripts/run-sandbox-vm-e2e.sh:699-723` |
| Cargo JSON artifact log and exact executable resolution | `scripts/run-sandbox-vm-e2e.sh:730-801` |
| `--locked --release` build, jq artifact path, ad-hoc sign, `codesign --verify --strict` | `scripts/run-installed-topology-cli-tests.sh:54-69` |
| Ad-hoc signing with entitlements | `scripts/run-sandbox-vm-e2e.sh:803-819`, `scripts/sign-dev.sh:33-53` |
| Sandbox lane `summary.txt` (`passed=`/`failed=`) and `run-info.txt` | `scripts/run-sandbox-vm-e2e.sh:3937-4079` |
| Bounded single-link SHA-256, fsync'd exclusive write, `document()` | `scripts/helpers/installed_developer_startup.py:40-71` |
| Canonical path admission, `--release-dir` preflight without PATH fallback | `installed_developer_startup.py:94-158` |
| Per-command intent/result receipts with canary withholding | `installed_developer_startup.py:192-240` |
| Isolated state root env (`VZ_RUNTIME_*`, `VZ_DOCKER_CONFIG`) | `installed_developer_startup.py:320-339` |
| `checksums.sha256` writer | `installed_developer_startup.py:670-676` |
| Docker lane `result.json` shape and honest `--suite all` rejection | `scripts/helpers/linux_docker_e2e.py:65, 941-994` |
| Strict JSON, no-follow open, bounded fixture tree digest, strict contract validation | `scripts/helpers/docker_compatibility_contract.py:114-286` |
| Canary scan, tree digest, checked text | `scripts/helpers/docker_host_driver.py:72-95, 329-331` |
| Hash-pinned isolated Python deps via `uv run --with-requirements` | `scripts/run-linux-docker-registry-e2e.sh:16-18`, `scripts/helpers/registry-requirements.in` |
| "DEV, never PASS" schema pattern (`const: false`) | `scripts/helpers/docker_host_results.schema.json` |
| Release artifact set and signing order | `.github/workflows/release.yml:93-137, 171-191, 336-367` |
| Frozen-input convention (`$schema`, `contract_state`, `normative_source`) | `config/docker-compatibility-v0.4.json:1-8`, `schemas/README.md` |

Host facts: `/usr/bin/python3` is 3.9.6 without `jsonschema`; `uv` is installed;
`ssh-keygen -Y` exists; `minisign` does not; `.artifacts/` is gitignored; the two
lane scripts `run-developer-environment-e2e.sh` and
`run-macos-developer-environment-e2e.sh` do not exist.

## Decisions

- Orchestration and validation are Python 3.9 modules under `scripts/helpers/`
  prefixed `vz04_`, with thin Bash entry points. The release-candidate builder is
  Bash because it is a Cargo/make/codesign pipeline.
- JSON Schema validation uses a hash-pinned `scripts/helpers/gate-requirements.txt`
  (`jsonschema==4.23.0` and transitive pins) run through `uv run --no-project
  --python /usr/bin/python3 --with-requirements`. A hand-written subset validator
  would silently under-enforce `if/then`, `$ref`, `uniqueItems`, `const`.
- Decision signatures use `ssh-keygen -Y sign/verify` (SSHSIG, ed25519), namespace
  `vz-0.4-decision`, detached files under `config/vz-0.4-decision-signatures/`.
  Signed bytes are the canonical JSON of the decision without its `signature`
  member plus `\n`.
- No `skipped` state exists anywhere. A lane that does not exist or does not
  implement `--suite all` yields `outcome: failed`, `failure.reason:
  not_implemented`, and every scenario assigned to it is `MISSING`.

## Files

Entry points: `scripts/build-vz-0.4-release-candidate.sh` (real logic),
`scripts/run-vz-0.4-release-gate.sh`, `scripts/validate-vz-0.4-evidence.sh`
(thin `uv run` wrappers), and explicit failing stubs
`scripts/run-developer-environment-e2e.sh`,
`scripts/run-macos-developer-environment-e2e.sh` that write a schema-valid
`not_implemented` lane result and exit 3.

Python (`scripts/helpers/`): `vz04_common.py`, `vz04_schema.py`,
`vz04_contract.py`, `vz04_candidate.py`, `vz04_source_tree.py`,
`vz04_decisions.py`, `vz04_lanes.py`, `vz04_phases.py`, `vz04_sleepwake.py`,
`vz04_host.py`, `vz04_gate.py`, `vz04_validate.py`, with `test_vz04_*.py`.

Config: `config/vz-0.4-e2e-contract.json`, `config/vz-0.4-migration-barriers.json`,
`config/vz-0.4-decisions.json`, `config/vz-0.4-decision-authorities.json`,
`config/host-target-capabilities-v0.4.json`, `config/vz-0.4-decision-signatures/`.

Schemas (`schemas/`): inputs `vz-0.4-e2e-contract`, `vz-0.4-migration-barriers`,
`vz-0.4-decisions`, `vz-0.4-decision-authorities`, `host-target-capabilities-v0.4`;
evidence `vz-0.4-release-manifest`, `vz-0.4-gate-manifest`, `vz-0.4-lane-result`,
`vz-0.4-summary`, `vz-0.4-state-handoff`, `vz-0.4-sleep-wake`,
`vz-0.4-connectivity-matrix`, `vz-0.4-runtime-provenance`,
`vz-0.4-resource-inventory`, `vz-0.4-receipt`, `vz-0.4-run-index`. All evidence
schemas use `additionalProperties: false`, `const` schema versions,
`^[0-9a-f]{64}$` digests, and `if/then` rules such as "passed requires failure
null and empty cleanup_errors".

## Release candidate builder

`scripts/build-vz-0.4-release-candidate.sh --output <new dir> --version <x.y.z[-pre]>`.

1. Preflight Darwin/arm64, `codesign`, `jq`, `shasum`, `cargo`, `uv`,
   `LINUX_DOCKER_CONTEXT`.
2. Fresh-checkout assertion: `git status --porcelain=v1 --untracked-files=all`
   empty, submodules clean, `crates/Cargo.lock` tracked and unchanged. Canonical
   source-tree digest is the SHA-256 of canonical JSON `[[path, gitmode,
   sha256(content)] ...]` over sorted `git ls-files -z`, plus `git rev-parse
   HEAD^{tree}`.
3. Guest bundles via the `run-sandbox-vm-e2e.sh` make loop, verified against
   `version.json` as `bundle_inputs` does.
4. Host binaries: one `cargo build --locked --release --message-format=json` for
   `vz`, `vz-macos-setup`, `vz-runtimed` (features must be empty),
   `vz-guest-agent`, `vz-agent-loader`; record `unsigned_sha256` before signing.
5. BuildKit archive via `scripts/build-runtime-free-buildkit.sh` and its validator.
6. Layout: `bin/`, `linux/developer/`, `linux/container/`, `buildkit/`,
   `machine-target-catalog.json` (written by `vz-runtimed
   --write-installed-machine-target-catalog`), `entitlements.plist`, `codesign/`,
   `release-manifest.json`, `release-manifest.sha256`, `checksums.sha256`.
7. Sign `vz`, `vz-runtimed`, `vz-macos-setup` with `--options runtime` and the
   entitlements (matching `release.yml`); ad-hoc sign the agent binaries; run
   `codesign --verify --strict --verbose=4` and capture `codesign -d
   --entitlements :- --verbose=4` metadata.
8. `chmod -R a-w` the output after the manifest is written.

`release-manifest.json`: `schema_version`, `kind`, `signing_class`
(`local-test-signed` | `developer-id-notarized`), `release_version`,
`built_at_utc`, `source {commit, git_tree, tree_sha256, clean, submodules}`,
`toolchain`, `components {path: {unsigned_sha256, signed_sha256, kind, cargo,
codesign}}`, `guest_bundles`, `buildkit`, `normalized_content_sha256` (over
sorted `[path, unsigned_sha256]`), `signed_content_sha256`.

GA parity prerequisite: `release.yml` must also record pre-signing digests.

## Gate entry point

`scripts/run-vz-0.4-release-gate.sh --suite all --release-dir <dir> --run-id <id>
[--evidence-root] [--state-root] --docker --compose-plugin --buildx-plugin
[--sleep-wake-ack-file]`. Only `--suite all` is accepted; anything else exits 2
before touching state.

1. Admit inputs without mutation: run-id regex, fresh evidence dir, canonical
   read-only release dir with a schema-valid manifest whose component digests and
   signatures re-verify; load and validate the frozen inputs; compute the
   candidate tuple; refuse a tuple already present in the append-only
   `.artifacts/vz-0.4-e2e/index.json`.
2. Create the evidence root with `manifest.json` (`verdict: running`), write the
   index entry first so an aborted run is retained as failed.
3. Prerequisites as receipts: `cargo fmt --check`, `cargo clippy --workspace
   --all-targets --all-features -- -D warnings`, `cargo nextest run --workspace
   --all-features`. On failure, stop before lanes; verdict FAIL with every
   scenario `MISSING` for reason `prerequisite_failed`.
4. Host facts and before-inventories (listeners, interfaces, processes) and the
   run-id pre-existence assertion.
5. Phase loop, lanes in contract order.
6. After-inventories, leak diff, `summary.json`, `summary.txt`,
   `checksums.sha256`, then the validator in-process; exit 0 only on PASS.

Lanes receive everything through argv with the minimal environment, never
ambient env: `--run-id`, `--phase`, `--release-dir`, `--evidence-dir`,
`--state-root`, `--contract`, `--candidate-tuple`, `--fixture-sha256`,
`--handoff`, client paths.

## Lanes and lane-result contract

| lane | entry point | role | phases |
|---|---|---|---|
| `sandbox-vm` | `scripts/run-sandbox-vm-e2e.sh --suite all --profile release` | prerequisite, zero acceptance IDs, must pass | clean-provision |
| `topology` | `scripts/run-developer-environment-e2e.sh --suite all` | acceptance | all |
| `linux-docker` | `scripts/run-linux-docker-e2e.sh --suite all` | acceptance (63 `docker.*` + Docker `gate.*`) | all |
| `native-macos` | `scripts/run-macos-developer-environment-e2e.sh --suite all` | acceptance | all |

Every lane writes `<lane dir>/<phase>/lane-result.json`: `schema_version`,
`kind`, `lane`, `phase`, `run_id`, `candidate_tuple_sha256`,
`release_dir_sha256`, `fixture_sha256`, `contract_sha256`, `entry_point {path,
sha256, argv}`, `outcome` (`passed` | `failed`), `failure` (`null` or `{reason:
not_implemented | assertion | prerequisite | cleanup | timeout | crash |
input_rejected | uncertain_effects, detail, exit_code}`), `scenarios [{id,
status PASS|FAIL, started_unix_ns, ended_unix_ns, assertions, evidence,
readiness_polls}]`, `test_case_retries: 0` (const), `process_starts`,
`prohibited_observed {docker_desktop, host_system_daemon, runc, crun, cargo_run,
path_fallback, ssh_hosts}`, `leaks: []`, `handoff {produced, consumed,
consumed_sha256}`, `retained_root`, `evidence_files`, `result_adapter` (`null`
or legacy summary translation for sandbox-vm).

Rules: a failed lane may have empty `scenarios`; every scenario assigned to it
becomes `MISSING` with the failure reason. A passed lane missing a required ID is
still FAIL. Duplicate IDs across lanes or phases fail. The sandbox lane is
translated from `summary.txt` until it emits a native result; the contract flag
`native_result_required` flips that to a hard requirement later.

## Phases

1. `clean-provision`: assert no run-id resources; invoke lanes; assemble a
   content-addressed `state-handoff.<sha256>.json`; no cleanup.
2. `persisted-recovery`: `pre-sleep` lane invocations, then the sleep/wake
   checkpoint (`run_id`, nonce, `kern.bootsessionuuid`, `kern.boottime`,
   `CLOCK_MONOTONIC`, `CLOCK_UPTIME_RAW`, wall clock, `pmset -g log` digest),
   operator ack with a contract deadline, wake capture (`pmset -g log` and
   unified log sleep/wake events inside the window, clock discontinuity
   `ΔMONOTONIC − ΔUPTIME_RAW ≥ minimum_sleep_seconds`, same boot session, nonce
   echoed), then `post-wake` lane invocations with the handoff.
3. `final-cleanup`: lanes delete only run-id-owned resources; the orchestrator
   diffs listeners, processes, sockets, Docker contexts against the before
   inventory and fails on any survivor.

## Candidate tuple

`candidate_tuple_sha256 = sha256(canonical_json(tuple) + "\n")` over `source
{commit, tree_sha256}`, `release {normalized_content_sha256,
signed_content_sha256, signing_class, release_manifest_sha256}`, `distribution
{signed_content_sha256, notarization_ticket_sha256}` (null locally, required for
GA), `inputs` (digests of the six configs, the signature tree, the IPSW pin, the
BuildKit pin, the CLI-removal config), `schemas_tree_sha256`,
`fixtures_tree_sha256`, `harness_tree_sha256` over the contract's explicit
`harness.files`. The run index is append-only; a run-id is admitted only for a
tuple absent from the index.

## Frozen inputs

`config/vz-0.4-e2e-contract.json`: `host_envelope`, `release`, `native_macos`,
`migration`, `resources`, `deadlines_seconds` (cold 600, warm 60, fault
activation/removal/revocation 5, stop 120, up 600, operator ack 1800),
`network_tolerances` (latency 100 ms ±30 over 100 requests; bandwidth 10 MiB/s
±20%; loss/partition 0 of 100 connections in 30 s with 0 probe failures),
`pressure` (20 containers 60 s, 4 pulls, 4 builds, 8 execs), `sleep_wake`,
`readiness_polls` (only declared polls may appear), `listener_checks`,
`cleanup_rules`, `canaries` (deterministic prefix `vz04-canary-`), `lanes`,
`harness.files`, `fixtures.required_dirs`, `docker_contract`, `scenarios`.

Top-level scenario IDs, one per acceptance criterion:

| # | id | lane | phase |
|---|---|---|---|
| 1 | `gate.instances.three_concurrent_no_collision` | topology | clean-provision |
| 2 | `gate.machines.mixed_profile_topology_status` | topology | clean-provision |
| 3 | `gate.docker.machine_scoped_contexts` | linux-docker | clean-provision |
| 4 | `gate.native.target_native_execution` | native-macos | clean-provision |
| 5 | `gate.network.private_topology_paths` | topology | clean-provision |
| 6 | `gate.network.public_like_ingress` | topology | clean-provision |
| 7 | `gate.host.import_export_boundaries` | topology | clean-provision |
| 8 | `gate.isolation.cross_environment_peering` | topology | persisted-recovery/pre-sleep |
| 9 | `gate.faults.measured_network_faults` | topology | persisted-recovery/pre-sleep |
| 10 | `gate.lifecycle.recovery_including_sleep_wake` | topology | persisted-recovery/post-wake |
| 11 | `gate.delete.single_environment_safety` | topology | final-cleanup |
| 12 | `gate.agent.deterministic_workers` | topology | persisted-recovery/pre-sleep |
| 13 | `gate.pressure.resource_pressure` | linux-docker | persisted-recovery/pre-sleep |
| 14 | `gate.runtime.youki_only_provenance` | linux-docker | final-cleanup |
| 15 | `gate.cli_api.agreement` | topology | clean-provision |
| 16 | `gate.reproducibility.recreate_from_definition` | topology | final-cleanup |
| 17 | `gate.storage.workspace_projection_policy` | topology | clean-provision |
| 18 | `gate.secrets.snapshots_scoped_redacted` | topology | persisted-recovery/post-wake |
| 19 | `gate.migration.install_upgrade_rollback_uninstall` | topology | clean-provision |
| 20 | `gate.network.exhaustive_denial_matrix` | topology | persisted-recovery/post-wake |
| 21 | `gate.cli.legacy_removal_and_bootstrap` | topology | clean-provision |
| 22 | `gate.definition.reconciliation_fencing` | topology | persisted-recovery/pre-sleep |

Plus the 63 `docker.*` IDs from `docker_compatibility_contract.REQUIRED_IDS`
assigned to `linux-docker` with each scenario's own `phase`. Total required
today: 85.

`config/vz-0.4-migration-barriers.json`: non-empty `barriers[]` with `id`,
`domain`, `pre_state`, `post_state`, `failure_injection`,
`expected_failure_state`, `rollback_assertion`, `receipt_field`.

`config/vz-0.4-decision-authorities.json`: ed25519 public keys with `key_id`,
`principal`, `valid_from_commit`, `revoked`. `config/vz-0.4-decisions.json`:
decisions with `id`, `kind`, `subject`, `rationale`, `source_reference`,
`approval`, `effective_commit`, `signature`. Verification also requires
`effective_commit` and the last commit touching the decision files to be strict
ancestors of the candidate commit. Docker `intentional_exclusions` must cite a
verified decision.

`config/host-target-capabilities-v0.4.json`: see the capability inventory; every
`MachineCapability` value appears exactly once per host×target×profile pair with
`ACTIVE`/`DEV`/`PLANNED`/`NA`, evidence rules enforced by schema and a Rust test.

## Validator

`vz04_validate.py` is read-only and produces the same verdict when run
independently on an archived root: schema validation of every evidence file;
`checksums.sha256` coverage and match; frozen-input, release, tuple and index
consistency; required-scenario accounting (each ID exactly once as PASS);
summary-versus-raw recomputation; canary scan with the fixed prefix; retry,
duplicate process-start, undeclared readiness poll, prohibited runtime, leak,
listener and sleep/wake checks. `PASS` only with zero findings.

## Build order

1. Gate core that runs today and fails honestly: requirements, `vz04_common`,
   `vz04_schema`, `vz04_contract`, `vz04_candidate`, `vz04_lanes` (sandbox
   translator, `not_implemented` handling), `vz04_phases` (sleep/wake recorded as
   not observed, a FAIL finding), `vz04_validate`, `vz04_gate`, entry points,
   lane stubs, draft contract, barriers, authorities with one real key, empty
   decisions, schemas, tests, `docs/vz-0.4-release-gate.md`. Expected first
   verdict: FAIL, 85 MISSING, three `not_implemented` lanes, sandbox translated.
2. Full builder, source-tree digest, decision verification, run index, host
   inventories, leak diff, `release.yml` pre-signing digests.
3. Real sleep/wake; native lane results from the Docker lane and the sandbox lane.
4. Real lanes landing scenario by scenario: topology lane skeleton (criteria 21,
   15, 2 first), Docker `--suite all` composition, native lane. Freeze inputs
   only when fixtures and harness digests are stable; certification last.

## Open decisions

1. Same-tuple re-runs are refused with no override (a docs-only commit makes a
   new tuple). Confirm this holds for infrastructure aborts.
2. Stop after prerequisite failure rather than burn VM hours. Confirm.
3. Sleep/wake as a blocking in-process operator ack. Confirm `CLOCK_UPTIME_RAW`
   stalls across sleep on this macOS and `pmset -g log` is readable unprivileged.
4. The builder runs from a clone or detached worktree; whether certification runs
   of the gate itself must also be from a clean checkout (recommended yes).
5. Canary plaintext: fixed deterministic prefix so archived evidence is scannable.
6. Decision key custody and bootstrap of `valid_from_commit`.
7. Host envelope numbers and native digests stay `null` under `draft_unverified`
   and count as unmet requirements, not blockers to execution.
8. Extra evidence dirs (`prerequisites/`, `host/`, `phases/`) beside the four lane
   dirs are allowed; the manifest binds every path.
9. The sandbox lane stays prerequisite-only.
10. `scripts/helpers/docker_host_results.schema.json` stays in place until its
    consumer emits lane results.
