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

Run the complete client integration suite:

```bash
cd crates
cargo test -p vz-runtimed-client
```

## Expected Fail-Close Surfaces

Until parity is implemented, these paths are intentionally blocked:

- `VZ_CONTROL_PLANE_TRANSPORT=api-http` (CLI transport selector).
- Legacy local-runtime OCI mutation paths (`vz oci run/create/exec/stop/rm`).
- Legacy local-runtime image build path (`vz image build`).
