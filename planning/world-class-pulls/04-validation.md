# 04 - Validation Matrix

## Objective

Prove pull engine performance, correctness, and reliability under realistic workloads and failure modes.

## Benchmark Dimensions

- Network profile: LAN fast, consumer broadband, high-latency/packet-loss simulation.
- Cache state: cold, warm, partially warm.
- Workload shape: single image, multi-image cohort, concurrent pulls.
- Registry type: Docker Hub, GHCR, private OCI registry, mirror path.

## Image Cohorts

Tier 1 (PR smoke):

- `alpine:3.20`
- `python:3.12-slim`
- `nginx:1.27-alpine`

Tier 2 (nightly):

- add `node:22-alpine`, `redis:7-alpine`, `postgres:16-alpine`
- add at least one large multi-layer image (language build image)

Tier 3 (weekly stress):

- same as Tier 2 plus concurrent cohort pulls and repeated churn loops.

## Correctness Tests

- Digest mismatch rejection.
- Corrupt blob mid-stream detection.
- Whiteout semantics regression tests.
- Deterministic rootfs hash for same image digest across repeated pulls.

## Reliability Tests

Fault injection:

- TCP resets during layer fetch.
- Timeout during manifest fetch.
- 429 throttling from registry.
- Interruption and process restart during pull.

Expected outcomes:

- Pull either completes successfully via retry/resume or fails with stable error kind.
- No corrupted committed blobs.
- No broken reference mappings after failure.

## Performance Tests

Measure:

- Total pull wall-clock.
- Download-only time.
- Unpack-only time.
- Cache-hit ratio.
- Retry count and penalty.

Targets:

- Meet or exceed SLOs in `01-target-contract.md`.

## Observability Verification

Require:

- Structured events for each pull phase and layer.
- Summary counters: bytes, retries, cache hits, elapsed by phase.
- Trace parity between CLI and validation harness outputs.
- UI progress quality checks:
  monotonic sequence, heartbeat cadence, bounded ETA jitter, terminal event guarantee.

Progress-specific test set:

- Golden event replay tests for known pull traces.
- Snapshot-to-snapshot diff checks to catch flicker/regression behavior.
- Stalled network scenario ensures heartbeat + explicit throttled/waiting status.

## CI Gating

- PR gate: Tier 1 correctness + performance regression threshold.
- Nightly: Tier 2 full matrix with trend report artifact.
- Weekly: Tier 3 stress with flake-rate tracking.

## Exit Criteria

Promotion from rollout wave to next wave requires:

- No correctness regressions.
- Reliability and performance gates green for that wave.
- Clear rollback path validated.
