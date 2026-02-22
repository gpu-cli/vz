# 02 - Architecture

## Current State

Current path:

- Manifest/config fetch happens first.
- Layers are fetched in a serial `for` loop.
- Layer unpack and rootfs assembly are serial.

Relevant code:

- `crates/vz-oci/src/image.rs`
- `crates/vz-oci/src/store.rs`

## Target Pull Pipeline

```text
reference -> resolver -> auth/token cache
                    -> manifest/config fetch
                    -> layer planner
                    -> parallel blob scheduler (bounded)
                    -> blob verifier + atomic writer
                    -> parallel unpack scheduler (bounded)
                    -> ordered rootfs assembler
                    -> reference commit + cache index update
```

## Core Components

### 1) Pull Planner

Responsibilities:

- Resolve canonical reference and target digest.
- Compute missing blobs from local CAS.
- Produce pull plan with per-layer dependencies and priorities.

### 2) Blob Scheduler

Responsibilities:

- Execute parallel blob downloads with per-registry and global concurrency limits.
- Coalesce duplicate digest requests across concurrent pulls.
- Resume partial downloads using HTTP range when possible.
- Retry transient failures with bounded backoff and jitter.

Design:

- Global semaphore for total parallel blobs.
- Per-registry semaphore to prevent registry overload.
- Per-digest in-flight map to dedupe concurrent pulls.

### 3) Blob Writer + Verifier

Responsibilities:

- Stream bytes to temp file with running digest.
- Verify digest and expected size on completion.
- Atomically move temp blob into `layers/`.
- Record blob metadata in local index.

### 4) Unpack Scheduler

Responsibilities:

- Unpack verified blobs in parallel using blocking worker pool.
- Reuse existing unpacked layer directories by digest.
- Emit per-layer unpack timing and failures.

### 5) Rootfs Assembler

Responsibilities:

- Apply unpacked layers in manifest order.
- Preserve whiteout semantics deterministically.
- Produce rootfs snapshot path for runtime consumption.

Note:

- Assembly stays ordered even when download/unpack are parallel.

### 6) Progress/Event Bus

Responsibilities:

- Emit typed pull events for CLI/UI/tracing.
- Provide pull summary with throughput, retries, cache-hit ratio.
- Surface stable error kind chain (`auth`, `resolve`, `fetch`, `verify`, `unpack`, `assemble`).

Contract details:

- Per-pull monotonic sequence numbers.
- Event classes:
  `PullStarted`, `PhaseChanged`, `LayerQueued`, `LayerProgress`, `LayerRetried`,
  `LayerComplete`, `PullHeartbeat`, `PullCompleted`, `PullFailed`.
- Aggregation emits both per-layer and overall progress snapshots.
- Producer emits heartbeat at fixed interval when no byte movement occurs.
- Transport preserves ordering per pull session.

### 7) Pull Progress Aggregator

Responsibilities:

- Convert low-level layer events into UI-friendly overall progress.
- Compute smoothed throughput and ETA.
- Classify unknown ETA conditions (`waiting_auth`, `throttled`, `size_unknown`, `retrying`).
- Emit change-minimized snapshots to avoid UI flicker.

Design:

- Rolling windows for throughput smoothing.
- Weighted progress model:
  bytes dominates when total sizes are known, phase-weighted fallback when unknown.
- Debounce policy for high-frequency updates with max-latency bound.

## Data Model Additions

Planned persistent records:

- `blob_index`: digest -> blob path, size, media type, verified timestamp.
- `download_journal`: resumable partial state (temp path, bytes complete, etag/last-modified when available).
- `pull_sessions`: session id, image ref, status, timestamps, retry counters.

## Concurrency Strategy

Defaults (tunable):

- `global_layer_parallelism`: `min(8, cpu_count * 2)`.
- `per_registry_parallelism`: `4`.
- `unpack_parallelism`: `cpu_count`.

Adaptive controls:

- Decrease concurrency on repeated 429/503 responses.
- Increase cautiously after sustained success windows.

## Failure Handling

Rules:

- Never commit reference mapping until all required artifacts are verified.
- Preserve resumable temp files for retry/restart.
- Cancel dependent tasks quickly on fatal non-retryable errors.
- Keep successful blobs from partial failed pulls.

## Security and Safety

- Strict digest verification remains mandatory.
- Reject unknown digest algorithms.
- Harden unpack path traversal and special-file handling.
- Keep atomic writes to avoid torn state.

## Integration Boundaries

In scope:

- `vz-oci` pull/store/runtime host-side logic.
- `vz-validation` pull benchmarks and fault-injection harness.
- Progress-event stream consumed by CLI and UI.

Out of scope:

- Guest-side runtime execution behavior (`youki` lifecycle already separate concern).
