# Retired CLI/API test coverage handoff

Status: DEV coverage inventory, not backend certification.

`api_http_mode_e2e.rs` formerly contained eight tests requiring removed CLI
commands to succeed. It now proves every inventoried old path and root flag
returns structured migration errors under both `api-http` and `daemon-grpc`.
Real loopback HTTP and Unix listeners must receive zero connections; malformed
state and project sentinels must remain unchanged. No backend or VM is started,
and there is no entitlement-based success/skip shortcut.

Retiring a command means removing its success test, not retaining an executable
alias. This does **not** make the replacement rejection tests equivalent to the
old workload/API checks. Typed API tests in their owning crates remain intact.
The table records the bounded source review and remaining coverage work; none of
the referenced backend suites was executed as part of this CLI retirement.

| Former CLI success scenario | Surviving typed coverage / remaining handoff |
| --- | --- |
| `cli_api_http_mode_end_to_end_sandbox_and_attach_flow` | Sandbox/list/inspect/lease/execution handlers have API and daemon tests. The old full API-created sandbox → CLI PTY detach/re-attach → close-shell → exit-7 chain was CLI-specific and is removed. Equivalent topology-scoped API/Exec physical terminal, detach where advertised, exit-status, and cleanup coverage must be established independently; this retirement does not certify it. |
| `cli_api_http_mode_image_commands_work_against_stub_api_without_daemon` | The old test called a stub, not a real image backend. `vz-runtimed-client/src/tests.rs::pull_and_prune_images_round_trip_via_daemon_client`, image stream tests in `vz-runtimed/src/grpc/tests.rs`, and HTTP image tests in `vz-api/src/tests.rs` remain. The CLI-to-stub serialization contract is intentionally gone. |
| `cli_api_http_mode_checkpoint_commands_work_against_stub_api_without_daemon` | The old test called stub checkpoint routes. API checkpoint response/error tests and daemon checkpoint create/restore/fork/export/import tests remain. This is not evidence that the complete topology-scoped snapshot contract passes. |
| `cli_api_http_mode_lease_commands_work_against_stub_api_without_daemon` | The old test called stub lease routes. `open_lease_then_get_and_close_round_trip` in daemon gRPC tests and HTTP authorization/lease tests remain. CLI lease formatting/routing is intentionally gone. |
| `cli_daemon_grpc_linux_save_restore_commands_cover_happy_path_and_errors` | The old CLI-specific class parser and output markers disappear. Typed daemon checkpoint restore/missing/fingerprint and btrfs restore/fork tests remain, but equivalence of this full successful Linux save→restore sequence was not established in the bounded review. Follow up with an explicit topology-owned typed API physical sequence and invalid-class/missing-checkpoint assertions. |
| `cli_daemon_grpc_linux_validate_reports_success_and_failure_modes` | Client test `validate_linux_vm_stream_reports_descriptor_checksum_failure` retains the mismatch/terminal failure assertion. The former valid-descriptor success plus daemon-ready CLI output check has no established equivalent in this review; retain a follow-up for both typed terminal outcomes, with no CLI marker dependency. |
| `cli_daemon_grpc_linux_base_lifecycle` | Client test `linux_vm_base_lifecycle_rpc_methods_are_covered` already checks typed upsert completion, list/get identity, deletion completion, and post-delete NotFound. No sole backend assertion was identified as lost; old CLI formatting is removed. |
| `cli_daemon_grpc_linux_patch_apply_incompatibility_and_rollback` | Client test `linux_vm_patch_apply_and_rollback_rpc_methods_are_covered` already checks apply/rollback completions, before/after descriptions, receipt lookup, and missing-base rejection. No sole backend assertion was identified as lost; old CLI formatting is removed. |

The terminal/detach chain, exact save/restore sequence, and successful validation
case are explicit coverage handoffs, not waived acceptance criteria. The parent
implementation session owns tracking them against the broader 0.4 API/backend
gate. Source presence of another test is not a claim that it passed.

## Script consumers requiring separate migration

These existing helper scripts still reference retired public commands and must
be migrated to typed test drivers/APIs or explicitly retired before being
advertised as executable workflows:

- `scripts/run-vz-linux-vm-e2e.sh`: `vz create` / sandbox CLI path.
- `scripts/run-vz-linux-vm-e2e-local.sh`: `vz vm mac ...` control/exec path.
- `scripts/run-vz-linux-hostboot-command.sh`: `vz vm linux ...` path.
- `scripts/install.sh`: old `vz run echo hello` completion guidance.

This change does not execute or silently claim to repair those scripts. The
current sandbox physical harness invokes signed test drivers directly and is a
separate lane; its results must not be inferred from these retirement tests.
