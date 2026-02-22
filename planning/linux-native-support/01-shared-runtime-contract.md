# 01 — Shared Runtime Contract

## Purpose

Create a backend-neutral contract so Linux primitives can live in a separate crate without forcing a large rewrite of callers.

## Design Principles

1. Keep caller API stable (`Runtime`, `RunConfig`, `ExecConfig`, container metadata).
2. Push host-specific details behind backend trait implementations.
3. Keep trait surface minimal and lifecycle-focused.
4. Prefer additive changes and adapters over mass renames.

## New Crate: `vz-runtime-contract`

### Responsibilities

- Define backend-neutral runtime traits.
- Own shared runtime-facing types used by multiple backends.
- Avoid direct dependency on Virtualization.framework or Linux-specific syscalls.

### Core Types

- `RuntimeConfig`
- `RunConfig`
- `ExecConfig`
- `PortMapping`
- `ContainerInfo` / `ContainerStatus`
- `ImageInfo`
- `ExecOutput`

Where possible, reuse existing `vz-oci` structs by moving them rather than copying.

## Backend Trait Shape

```rust
pub trait RuntimeBackend: Send + Sync {
    fn name(&self) -> &'static str;

    async fn pull(&self, image: &str) -> Result<ImageId, RuntimeError>;
    async fn run(&self, image: &str, run: RunConfig) -> Result<ExecOutput, RuntimeError>;

    async fn create_container(&self, image: &str, run: RunConfig) -> Result<String, RuntimeError>;
    async fn exec_container(&self, id: &str, exec: ExecConfig) -> Result<ExecOutput, RuntimeError>;
    async fn stop_container(&self, id: &str, force: bool) -> Result<ContainerInfo, RuntimeError>;
    async fn remove_container(&self, id: &str) -> Result<(), RuntimeError>;

    fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError>;
    fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError>;
    fn prune_images(&self) -> Result<PruneResult, RuntimeError>;
}
```

## Adapter Strategy

### macOS adapter (first)

Wrap existing `vz-oci` runtime behavior in `MacosRuntimeBackend` with minimal code movement.

### Linux adapter (next)

Implement the same trait in `vz-linux-native`.

## Migration Steps

1. Introduce trait + shared type crate.
2. Add adapter around current macOS path.
3. Make `vz-oci::Runtime` hold `Arc<dyn RuntimeBackend>`.
4. Keep existing `Runtime` public methods; delegate internally to backend.

## Compatibility Requirements

- Preserve current public behavior for macOS callers.
- No required API changes in `vz-stack` and `vz-validation` for phase 1.
- Error mapping remains actionable (backend-specific detail preserved in source chain).

## Done When

- `vz-oci` compiles with backend trait boundary.
- Existing macOS tests pass with adapter-backed runtime.
- No backend-specific imports leak into `vz-stack`.
