# Runtime V2 Benchmark Methodology

Status: Required for Public Claims
Owner: James Lal
Last Updated: 2026-02-24
Source Plan: ../private-vz/runtime-v2/09-rollout-validation-plan.md

## Purpose

This document defines the minimum artifact and reporting standard for any Runtime V2
performance or reliability claim. Claims must be reproducible and traceable to raw
benchmark evidence.

## Claims Policy

A public claim is any statement about Runtime V2 speed, latency, throughput, startup,
restore, convergence time, pull/build performance, or capacity/density.

A claim is publishable only if all required artifacts in this document are present.

## Required Artifacts

Every benchmark report must include all of the following sections.

### 1. Workload Classes

Document each workload class that was measured. Minimum examples:

- container lifecycle (`create/start/stop/remove`)
- one-off exec/control path latency
- stack apply/reconcile (`web+db+cache`) convergence
- checkpoint/restore/fork (where capability is enabled)
- Docker/Compose shim translation and execution overhead

### 2. Host Hardware and OS Versions

For each run, include:

- host CPU model and core count
- host memory size
- host storage class
- host OS version/build
- virtualization backend (`macos_vz`, `linux_firecracker`, or equivalent)

### 3. Runtime/Software Baseline

Record exact software versions and commit references:

- `vz` git commit SHA
- rust toolchain version
- crate versions or lockfile fingerprint
- relevant guest/kernel/artifact versions

### 4. Metrics and Distribution Summary

For every measured operation, report:

- unit (`ms`, `s`, `ops/s`, etc.)
- sample size (`n`)
- p50
- p95
- p99
- min/max (recommended)

### 5. Variance and Confidence

Document variability using at least one of:

- standard deviation
- interquartile range
- confidence interval

Also include warmup/discard policy and run count.

### 6. Constraints and Known Limits

List constraints that materially affect interpretation (for example: no nested
virtualization, backend capability gating, VM concurrency limits, host thermal
throttling risk).

### 7. Raw Artifacts and Reproduction

Provide:

- raw metric files (CSV/JSON)
- benchmark command lines
- environment variables/toggles used
- fixture definitions and seeds
- instructions to reproduce locally

## Reporting Template

Use this structure for each published benchmark section.

| Operation | Backend | n | Unit | p50 | p95 | p99 | Variance | Notes |
|---|---|---|---|---|---|---|---|---|
| example_operation | macos_vz | 100 | ms | 12.3 | 19.8 | 26.1 | stdev=3.4 | fixture=web-db-cache |

## Review Checklist

Before publishing benchmark claims, verify all checks:

- [ ] Workload classes are defined and mapped to reported metrics.
- [ ] Host hardware and OS versions are fully documented.
- [ ] Runtime/software baseline is pinned to commit/toolchain versions.
- [ ] Metrics include p50/p95/p99 and sample size.
- [ ] Variance/confidence information is present.
- [ ] Constraints and known limits are explicitly documented.
- [ ] Raw artifacts and reproduction commands are available.

## Non-Compliance Rule

If any required artifact is missing, the benchmark claim must be treated as internal
exploration only and must not be used in external messaging.
