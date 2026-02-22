# 02 — Linux Native Backend Crate

## Purpose

Implement Linux host primitives in a dedicated crate so Linux support ships without coupling to macOS VZ internals.

## New Crate: `vz-linux-native`

## Responsibilities

1. Container lifecycle execution on Linux host.
2. Namespace/cgroup/runtime orchestration primitives.
3. Host networking and port forwarding primitives.
4. Backend implementation of `RuntimeBackend` trait.

## Reuse Targets

Reuse as much as possible from existing runtime model:

- image pull and store logic
- run/create/exec/stop/remove semantics
- container metadata persistence conventions
- stack network intent model (published ports, service addressing)

## Linux Execution Model

### Initial runtime target

- Use OCI runtime binary (`youki` first, `runc` fallback optional).
- Bundle-based execution (reuse existing bundle generation patterns).
- Keep lifecycle parity with existing VM-guest OCI path:
  - create
  - start
  - exec
  - kill/stop
  - delete

### Isolation modes

- `rootless` (preferred default where host supports delegated cgroup v2)
- `rootful` (explicit opt-in fallback)

## Internal Modules

- `runtime.rs` — backend trait implementation
- `bundle.rs` — OCI bundle creation/writing
- `ns.rs` — namespace setup helpers
- `cgroups.rs` — cpu/memory limit application
- `network.rs` — bridge/namespace/published-port wiring
- `process.rs` — runtime process management + cleanup

## Host Primitives

### Required

- mount + bind mount setup
- network namespace create/join
- veth and bridge setup (or host backend abstraction)
- cgroup v2 cpu quota + period
- signal forwarding and lifecycle state polling

### Optional (Phase 2+)

- eBPF/network policy enforcement
- seccomp profile support
- checkpoint/restore

## Configuration

Add Linux-native options to runtime config:

- runtime binary path override
- rootless/rootful mode
- cgroup driver settings
- network backend selection
- data directories for bundles/state/logs

## Error Model

Keep precise, structured errors:

- unsupported host kernel features
- missing runtime binary
- permission/capability issues
- cgroup delegation unavailable
- network setup failures

## Done When

- Linux host can pull, run, exec, stop, remove simple OCI containers.
- CPU limit settings are applied and test-verified.
- Port publishing works for TCP in supported mode.
- Runtime state cleanup is deterministic after crash/restart.
