# Sandbox Isolation Threat Model and Hardening Closure

Status: In Review
Owner: James Lal
Last Updated: 2026-02-24

## Scope

This document covers host-guest sandbox command execution security, specifically:

- `vz-sandbox` host session execution (`crates/vz-sandbox/src/session.rs`)
- guest vsock accept path (`crates/vz-guest-agent/src/listener.rs`)
- protocol execution defaults/fields (`vz` gRPC `ExecRequest`)

## Attack Surface Inventory

| Surface | Vector | Notes |
|---|---|---|
| Host → guest vsock endpoint | AF_VSOCK listening socket (`VDADDR_CID_ANY` bind, service port) | Any peer that reaches vsock endpoint can open a connection and attempt protocol traffic unless source-validated. |
| Exec command dispatch | gRPC `Exec` request handling | Request carries command/args/working directory/user; user controls runtime privilege in guest. |
| Host working directory mapping | `acquire(project_dir)` path derivation to `guest_project_path` | Inputs derived from host path passed to pool API; malformed paths can become relative path expressions. |
| gRPC user default behavior | `user` field omitted (`None`) in execution requests | `None` currently means guest default user; implementation behavior depends on host caller. |
| Guest process lifecycle | Process spawn + stdin/signal/teardown paths | Long-lived process handles can be abused if request identity/privilege isn't constrained. |

## Findings and Closure

| ID | Finding | Severity | Decision | Evidence |
|---|---|---|---|---|
| F-01 | Non-host peers can connect to guest listener because accept path only checks no source identity | High | Remediated | `crates/vz-guest-agent/src/listener.rs`: `source_cid` extraction and host-only acceptance logic in `accept()`. Non-host CIDs are closed and logged. |
| F-02 | Sandbox session default exec path does not set an explicit non-root user, causing root execution when peer request omits `user` | High | Remediated | `crates/vz-sandbox/src/session.rs`: `DEFAULT_EXEC_USER = "dev"` and `resolve_exec_user(None) -> "dev"` used for default execution and `exec_streaming`. |
| F-03 | Default-user policy is not explicitly asserted by tests | Medium | Remediated | `crates/vz-sandbox/src/session.rs`: `resolve_exec_user_*` unit tests validate default and explicit override behavior. |
| F-04 | Guest listener allows non-host CID and does not exercise explicit host allowlist in unit tests | Medium | Remediated | `crates/vz-guest-agent/src/listener.rs`: `is_host_peer` unit test and host CID helper. |

## Explicit Sign-off

- Accepted Risk: None.
- Remediations applied: F-01 through F-04.
- Pending / open items: None for this closure.

## Verification Commands

- `cargo check -p vz-sandbox -p vz-guest-agent`
- `cargo clippy -p vz-sandbox -p vz-guest-agent -- -D warnings`
- `cargo nextest run -p vz-sandbox -p vz-guest-agent`
