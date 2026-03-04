# Daemon-Only Guardrails

This project enforces daemon ownership (`vz-runtimed`) for runtime mutations.

## Runtime Path Conventions

Default runtime path layout is deterministic:

- state DB: `~/.vz/stack-state.db`
- daemon runtime data dir: `<state-db-parent>/.vz-runtime`
- daemon gRPC socket: `<runtime-data-dir>/runtimed.sock`

Supported env overrides:

- `VZ_RUNTIME_STATE_DB`: override default state DB path used by CLI runtime commands.
- `VZ_RUNTIME_DATA_DIR`: override runtime data dir (socket is `<dir>/runtimed.sock`).
- `VZ_RUNTIME_DAEMON_SOCKET`: explicit socket path override (takes precedence over runtime data dir).
- `VZ_RUNTIME_DAEMON_AUTOSTART`: enable/disable daemon auto-spawn (`1/0`, `true/false`, etc).

## Static Guardrail Check

Run:

```bash
./scripts/check-daemon-only-guardrails.sh
```

The check fails when:

- Runtime CLI command files bypass daemon client wiring.
- Runtime CLI command files open SQLite directly.
- `vz-api` production surfaces open SQLite directly.
- Known fail-closed runtime parity gaps are removed without replacement.
- Any Runtime V2 RPC in `runtime_v2.proto` has no test invocation in daemon/client/API coverage suites.

Run the RPC coverage gate directly:

```bash
./scripts/check-runtime-v2-rpc-test-coverage.sh
```

## Happy-Path E2E Matrix (Daemon Endpoints)

Current daemon-backed integration coverage includes:

- Sandbox create stream progress/completion:
  - `cargo test -p vz-runtimed-client create_sandbox_stream_emits_progress_and_completion`
- Stack apply/status/events/teardown:
  - `cargo test -p vz-runtimed-client stack_apply_and_teardown_round_trip_via_daemon_client`
- API daemon-backed sandbox lifecycle smoke:
  - `cargo test -p vz-api --test server_smoke runtime_api_server_smoke_sandbox_crud_and_file_ops`

## Mission Evidence Workflow (Spaces)

Default evidence flow for checkpointed spaces combines runtime file/system evidence and git delta:

```bash
# Runtime-owned file/system evidence between checkpoints.
vz diff <from-checkpoint-id> <to-checkpoint-id> --mode patch

# Code-level working tree delta in the checked-out project.
git diff
```

API transport parity for checkpoint evidence is provided via `GET /v1/checkpoints/diff`
(`from_checkpoint_id` + `to_checkpoint_id` query parameters), and `vz diff` now uses that
endpoint when `VZ_CONTROL_PLANE_TRANSPORT=api-http`.

## Checkpoint Retention Lineage Semantics

Checkpoint retention now applies lineage-aware behavior for fork trees:

- Tagged checkpoints are protected from GC.
- Ancestors of tagged checkpoints are also protected.
- When a checkpoint is selected for deletion, descendants are deleted via
  lineage cascade.

Visibility surfaces:

- API checkpoint payload field `retention_gc_reason` may be:
  - `age_limit`
  - `count_limit`
  - `lineage_cascade`
- CLI `vz checkpoint list` shows the same reason in the `GC REASON` column.
- Maintenance audit records include:
  - event type `checkpoint_gc_compacted`
  - receipt operation `checkpoint_gc_compact`
  - reason-bucket arrays in receipt metadata:
    `deleted_by_age`, `deleted_by_count`, `deleted_by_lineage`

## Spaces Cache Lifecycle And GC

Daemon-owned spaces cache state currently uses:

- index file: `<state-store-parent>/space-cache-index.json`
- artifact root: `<state-store-parent>/space-cache-artifacts/<cache-name>/<digest-hex>/`

Lifecycle behavior:

- Cache identity is deterministic from canonical key material (`SpaceCacheKey` schema).
- First prepare is expected `local_miss_cold`; subsequent prepare with identical key is `local_hit`.
- Schema-version mismatch invalidates stale index entries during prepare.
- Remote verified artifacts are materialized only through daemon paths.

Portability and storage policy:

- Cache artifact materialization is fail-closed unless daemon state storage is Linux+btrfs.
- Linux non-btrfs daemon state parents are rejected for cache artifact materialization.
- Non-Linux platforms are rejected for cache artifact materialization.

Benchmark/evidence workflow:

```bash
./scripts/run-space-cache-benchmark.sh
```

Artifacts are written to `.artifacts/space-cache-bench/<timestamp>/` with:

- `run-info.txt` (host/profile/test metadata)
- `<test>.log` (raw command output)
- `summary.txt` (benchmark marker line)

## Release Scope: No Live Host Mounts

Spaces R1 excludes live host mount features in sandbox lifecycle surfaces.

- `vz sandbox create` does not expose `--mount`/`--volume` style host-mount flags.
- API `POST /v1/sandboxes` create contract is workspace/project-dir based and does not accept host
  mount configuration.
- Guardrail checks fail if host-mount flags are introduced on sandbox create paths.

## Restart/Reconcile Validation

Daemon startup reconcile and post-restart attach semantics are validated with:

- `cargo test -p vz-runtimed daemon_start_reconciles_non_terminal_executions_to_failed`
- `cargo test -p vz-runtimed daemon_start_reconciles_non_terminal_builds_to_failed`
- `cargo test -p vz-runtimed --lib stream_exec_output_after_restart_reconcile_returns_terminal_failure_event`

Run the complete client integration suite:

```bash
cd crates
cargo test -p vz-runtimed-client
```

## Expected Fail-Close Surfaces

Current transport note:

- `VZ_CONTROL_PLANE_TRANSPORT=api-http` is accepted by CLI and must route through API HTTP client helpers (no direct daemon-gRPC fallback shim).
- Legacy local-runtime OCI mutation paths (`vz oci run/create/exec/stop/rm`).
- Legacy local-runtime image build path (`vz image build`).
