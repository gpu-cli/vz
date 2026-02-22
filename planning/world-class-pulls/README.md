# World-Class OCI Pull System

Date: 2026-02-19

## Goal

Build a pull pipeline in `vz-oci` that is performance-competitive with Docker Engine for common workflows, while preserving strict OCI correctness and strong observability.

This track targets parity-or-better behavior for:

- Parallel layer downloads and unpack.
- Stable behavior under flaky networks.
- Deterministic digest-verified image materialization.
- High-quality pull progress/status surfaced to CLI, UI, and validation traces.

## Why This Track Exists

Current pull behavior is functionally correct but structurally conservative:

- Layer downloads are serial in `crates/vz-oci/src/image.rs`.
- Layer unpack + rootfs assembly are serial in `crates/vz-oci/src/store.rs`.
- No resumable partial blob downloads.
- No registry mirror/fallback selection.
- No explicit pull progress model exposed to callers.

Recent cache seeding support in `real_runner` removes repeated pull pain in validation, but it is a workflow optimization, not a production pull engine replacement.

## Product Bar (Parity Target)

We define "world class" as meeting all of the following simultaneously:

1. Throughput: bounded-parallel pull+unpack with adaptive concurrency.
2. Reliability: resilient retries/backoff and resume after interruption.
3. Correctness: strict digest and media verification, deterministic results.
4. UX: real-time progress events with usable ETA/throughput and stable status codes.
5. Operability: traceable pull lifecycle with measurable SLOs.

## SLO Summary

Exact measurement protocol is in `04-validation.md`.

- Warm pull (`all blobs cached`): `< 1s` p95 for manifest+config resolution.
- Cold pull (`6-12 layer image`): `>= 2x` speedup vs current serial baseline on the same host/network profile.
- Multi-service pull (6 common images): `>= 3x` aggregate wall-clock speedup vs current baseline.
- Transient fault resilience: `>= 99%` success rate with injected network resets/timeouts.
- Pull correctness: `100%` digest/media verification; no silent fallback on mismatch.

## Design Principles

- OCI correctness first, then throughput.
- Host-side pull/assembly remains host responsibility; guest stays runtime-spec execution.
- Explicit state machine; avoid hidden retries.
- Bounded parallelism with backpressure (never unbounded fanout).
- Idempotent storage writes and per-digest de-duplication under concurrency.

## Document Index

- `01-target-contract.md`: performance/reliability/correctness contract.
- `02-architecture.md`: target pull architecture and data model.
- `03-rollout-waves.md`: staged delivery plan and rollback gates.
- `04-validation.md`: benchmark/fault-injection/compatibility matrix.
- `05-beads.md`: dependency-ordered implementation beads.
