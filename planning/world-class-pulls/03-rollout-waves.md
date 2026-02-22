# 03 - Rollout Waves

## Delivery Strategy

Ship in production-safe waves with measurable gates and rollback options.

## Wave 0 - Baseline and Instrumentation

Scope:

- Add pull timing/progress events around existing serial implementation.
- Build baseline benchmark harness and golden reports.

Gate:

- Stable baseline metrics published for Tier 1 common images.

Rollback:

- Metrics-only changes are safe to keep; no behavior regression risk.

## Wave 1 - Parallel Blob Downloads

Scope:

- Introduce bounded parallel layer download scheduler.
- Add per-digest in-flight dedupe.
- Keep unpack/assembly unchanged (still serial).

Gate:

- No correctness regressions.
- `>= 1.8x` cold pull speedup on common image cohort.

Rollback:

- Feature flag to force serial downloader.

## Wave 2 - Retry/Resume Reliability

Scope:

- Add retry classifier + backoff.
- Add resumable download journal and range requests.
- Add 429/5xx adaptive throttling.

Gate:

- Fault-injection success rate `>= 99%`.
- No stuck partial state leaks.

Rollback:

- Disable resume + adaptive logic and retain Wave 1 parallel pull.

## Wave 3 - Parallel Unpack Pipeline

Scope:

- Parallel unpack worker pool for verified layers.
- Ordered rootfs assembly remains deterministic.

Gate:

- End-to-end cold pull+assemble `>= 2x` over baseline.
- Deterministic rootfs diff tests pass.

Rollback:

- Fallback to serial unpack.

## Wave 4 - Mirror/Fallback and Policy Controls

Scope:

- Registry mirror chain per host/registry.
- Health scoring and failover.
- Configurable pull policy (`always`, `if-missing`, `never`).

Gate:

- Mirror outage drills pass with transparent fallback.

Rollback:

- Disable mirrors and use primary registry only.

## Wave 5 - UX + Operational Hardening

Scope:

- Stable pull status model surfaced via CLI and trace events.
- Error taxonomy normalization.
- Capacity tuning defaults by hardware class.

Gate:

- Operator report can explain failures without raw debug logs.
- Pull SLOs green for 2 consecutive weekly runs.

Rollback:

- Keep engine improvements; disable advanced progress rendering only.

