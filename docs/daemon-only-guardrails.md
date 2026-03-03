# Daemon-Only Guardrails

This project enforces daemon ownership (`vz-runtimed`) for runtime mutations.

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
