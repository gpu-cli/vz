# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is vz

Rust-native interface to Apple's Virtualization.framework for sandboxing coding agents in macOS VMs. Three crate layers:

1. **vz-sys** — Raw Objective-C FFI bindings via `objc2`
2. **vz** — Safe, ergonomic async Rust API
3. **vz-sandbox** — High-level sandbox abstraction (pool, sessions, channels)

Plus **vz-cli** for standalone use without writing Rust.

## Build & Development Commands

All Rust commands run from `crates/` (workspace root):

```bash
# Build
cd crates && cargo build --workspace

# Build single crate
cd crates && cargo build -p vz-sys
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
│  Pool, Session, Channel — high-level sandbox API    │
├─────────────────────────────────────────────────────┤
│                      vz                             │
│  Safe Rust: Vm, Config, VirtioFs, Vsock, SaveState  │
├─────────────────────────────────────────────────────┤
│                    vz-sys                           │
│  Raw FFI: objc2 bindings to Virtualization.framework│
├─────────────────────────────────────────────────────┤
│           Apple Virtualization.framework            │
│              (macOS 14+ / Apple Silicon)            │
└─────────────────────────────────────────────────────┘
```

### Workspace Crates

| Crate | Purpose |
|-------|---------|
| `vz-sys` | Raw unsafe FFI bindings to Virtualization.framework using `objc2` + `block2` |
| `vz` | Safe async Rust API — Vm, VmConfig, VirtioFs, Vsock, SaveState |
| `vz-sandbox` | High-level sandbox — SandboxPool, SandboxSession, typed Channel |
| `vz-cli` | CLI binary — `vz init`, `vz run`, `vz exec`, `vz save/restore` |

### Key Design Decisions

- **macOS 14 (Sonoma) minimum** — required for save/restore VM state
- **Apple Silicon only** — macOS guest VMs require Apple Silicon
- **objc2 for FFI** — compile-time verification, safe memory management, block support
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
- FFI layer (`vz-sys`): unsafe is expected but must be minimal and well-commented.
- Safe layer (`vz`): zero unsafe in public API — all unsafety contained in vz-sys.

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
// In vz-sys: raw ObjC call with block completion handler
// In vz: wrap with tokio::sync::oneshot to produce a Future
```

## Dependencies

Core dependencies:
- `objc2` + `objc2-foundation` — Objective-C interop
- `block2` — Objective-C block support (for completion handlers)
- `tokio` — async runtime
- `tracing` — logging
- `thiserror` / `anyhow` — error handling
- `serde` / `serde_json` — serialization (for Channel protocol)
- `clap` — CLI argument parsing (vz-cli only)

## Planning & Design

Design documents live in `planning/`:
- `planning/README.md` — Full design doc with API sketches and implementation plan

## Testing Strategy

- Unit tests in each crate's `src/` (standard Rust `#[test]`)
- Integration tests in `crates/*/tests/` — require macOS + Apple Silicon to run
- CI: build check on all platforms, tests only on macOS Apple Silicon runners
- Use `cargo nextest` (not `cargo test`) for better output and parallelism
