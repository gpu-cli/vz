# 03 — Runtime Selection and Integration

## Purpose

Integrate backend abstraction into existing runtime/CLI/stack flows with minimal caller churn.

## Backend Selection

Selection inputs:

1. Host OS default:
   - macOS => `MacosRuntimeBackend`
   - Linux => `LinuxNativeRuntimeBackend`
2. Optional explicit override in config/env (for testing).

Example:

```rust
pub enum HostBackend {
    Auto,
    MacosVz,
    LinuxNative,
}
```

## `vz-oci` Integration Plan

1. Keep `Runtime` type as facade.
2. Replace direct VZ/Linux VM internals with delegated backend calls.
3. Preserve current method names and return types.
4. Keep image store and container store behavior stable.

## `vz-stack` Integration Plan

No trait/interface changes required if `vz-oci::Runtime` API remains stable.

Required work:

- ensure stack networking calls map correctly for Linux-native backend
- maintain current reconcile/executor semantics
- add backend-specific capability checks where needed

## CLI Integration Plan

### Build targets

- Linux build should include OCI/stack/validation commands.
- macOS-only VM commands remain supported on macOS.

### Command behavior on Linux

- macOS-specific commands should return clear unsupported messages.
- OCI/stack commands should be first-class on Linux host.

## Cargo / `cfg` Strategy

Current broad crate-level `#![cfg(target_os = "macos")]` gates block Linux builds.

Refactor to:

- module-level gating for platform-specific implementations
- shared facade compiled on both platforms
- backend modules compiled conditionally

## Backward Compatibility

- Existing macOS workflows remain unchanged.
- Existing CLI syntax for OCI commands remains valid.
- Existing `vz-stack` control-plane flow remains valid.

## Done When

- `cargo build --workspace` passes on Linux and macOS.
- Linux runtime path is selected by default on Linux host.
- `vz-stack up/down/ps/events` run on Linux backend.
