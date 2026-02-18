# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is vz

Rust-native interface to Apple's Virtualization.framework for sandboxing coding agents in macOS VMs. Three crates:

1. **vz** — Safe, ergonomic async Rust API wrapping `objc2-virtualization` (auto-generated bindings). All unsafe is internal; public API is 100% safe Rust.
2. **vz-sandbox** — High-level sandbox abstraction (pool, sessions, channels, guest agent)
3. **vz-cli** — Standalone CLI for managing macOS VMs without writing Rust

There is no `vz-sys` crate. The `objc2-virtualization` crate (v0.3.2) provides auto-generated bindings to all Virtualization.framework classes, eliminating the need for hand-written FFI.

## Build & Development Commands

All Rust commands run from `crates/` (workspace root):

```bash
# Build
cd crates && cargo build --workspace

# Build single crate
cd crates && cargo build -p vz

# Test
cd crates && cargo nextest run --workspace

# Single crate test
cd crates && cargo nextest run -p vz

# Lint (clippy treats warnings as errors)
cd crates && cargo clippy --workspace -- -D warnings

# Format
cd crates && cargo fmt --workspace
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     vz-cli                          │
│  `vz run --image base --mount project:./workspace`  │
├─────────────────────────────────────────────────────┤
│                   vz-sandbox                        │
│  Pool, Session, Channel, Guest Agent binary         │
├─────────────────────────────────────────────────────┤
│                      vz                             │
│  Safe Rust: Vm, Config, VirtioFs, Vsock, SaveState  │
├─────────────────────────────────────────────────────┤
│              objc2-virtualization v0.3.2             │
│  Auto-generated bindings to ALL Vz.framework classes│
├─────────────────────────────────────────────────────┤
│           Apple Virtualization.framework            │
│              (macOS 14+ / Apple Silicon)            │
└─────────────────────────────────────────────────────┘
```

### Workspace Crates

| Crate | Purpose |
|-------|---------|
| `vz` | Safe async Rust API — Vm, VmConfig, VirtioFs, Vsock, SaveState. Wraps `objc2-virtualization` directly. |
| `vz-sandbox` | High-level sandbox — SandboxPool, SandboxSession, typed Channel, guest agent binary |
| `vz-cli` | CLI binary — `vz init`, `vz run`, `vz exec`, `vz save/restore` |

### Key Design Decisions

- **macOS 14 (Sonoma) minimum** — required for save/restore VM state
- **Apple Silicon only** — macOS guest VMs require Apple Silicon
- **objc2-virtualization for FFI** — auto-generated bindings, no hand-written sys crate, compile-time verification
- **Async-first (tokio)** — all VM ops bridge ObjC completion handlers to tokio futures
- **Long-lived VM model** — single VM stays running, project dirs swapped via VirtioFS mounts
- **vsock for communication** — host↔guest channel without network config

### Platform Constraints

- macOS host only (Virtualization.framework is macOS-only)
- Apple Silicon only for macOS guests
- 2 concurrent macOS VM limit (kernel-enforced)
- VirtioFS mounts are static (configured at VM creation, not runtime)
- No nested virtualization
- No Metal/GPU passthrough to guests
- Hardware-encrypted save files (tied to Mac + user, not portable)

## Coding Conventions

### Rust — Strict Rules

- **No `unwrap()` or `expect()` in production code** — use `?` operator, `anyhow::Context`, or proper `match`/`if let`. Acceptable only in tests.
- **No `println!`/`eprintln!`** — use `tracing` crate (`tracing::{info, warn, error, debug, trace}`).
- **No `json!` macro** — define proper Rust structs with `#[derive(Serialize, Deserialize)]`.
- Edition 2024, minimum Rust 1.85.0.
- Error handling: `thiserror` for library errors, `anyhow` for application/CLI errors.
- All public APIs must be documented with `///` doc comments.
- All `unsafe` is contained within the `vz` crate's internal `bridge.rs` module. The public API surface is 100% safe Rust. No other crate in the workspace should contain `unsafe`.

### Platform Gating

All code must compile on non-macOS (for CI, docs.rs, etc.) but functionality is gated:

```rust
#[cfg(target_os = "macos")]
mod implementation;

#[cfg(not(target_os = "macos"))]
compile_error!("vz requires macOS");
```

### Async Bridging Pattern

ObjC completion handlers → tokio futures:

```rust
// In vz bridge.rs: dispatch onto serial queue, create RcBlock with Cell<Option<Sender>>,
// ObjC completion handler calls tx.take().send(result),
// public async fn awaits the oneshot receiver
```

## Dependencies

Core dependencies:
- `objc2` + `objc2-foundation` + `objc2-virtualization` — Objective-C interop and Virtualization.framework bindings
- `block2` — Objective-C block support (for completion handlers)
- `dispatch2` — GCD serial queue management (mandatory for VZVirtualMachine)
- `tokio` — async runtime
- `tracing` — logging
- `thiserror` / `anyhow` — error handling
- `serde` / `serde_json` — serialization (for Channel protocol)
- `clap` — CLI argument parsing (vz-cli only)

## Planning & Design

Design documents live in `planning/`:
- `planning/README.md` — Master architecture overview, crate responsibilities, implementation phases
- 10 detailed planning docs (00-09) covering FFI, safe API, VirtioFS, vsock protocol, guest agent, sandbox, CLI, golden image, testing, and code signing

## Testing Strategy

- Unit tests in each crate's `src/` (standard Rust `#[test]`)
- Integration tests in `crates/*/tests/` — require macOS + Apple Silicon to run
- CI: build check on all platforms, tests only on macOS Apple Silicon runners
- Use `cargo nextest` (not `cargo test`) for better output and parallelism
