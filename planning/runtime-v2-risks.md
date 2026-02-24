# Runtime V2 Rollout Risk Register

Status: Active
Owner: James Lal
Last Updated: 2026-02-24
Source Plan: ../private-vz/runtime-v2/09-rollout-validation-plan.md

## Purpose

Convert Runtime V2 rollout risks into trackable execution items with ownership,
acceptance criteria, and evidence.

## Risk Register

| Risk ID | Risk Statement | Owner | Likelihood | Impact | Mitigation Strategy | Acceptance Criteria | Status |
|---|---|---|---|---|---|---|---|
| R1 | Docker/Compose shim complexity distorts the core runtime model. | James Lal | Medium | High | Enforce adapter-only boundary and runtime-neutral contracts. | Shim changes cannot bypass runtime boundary checklist; unsupported features return stable `unsupported_operation`; shim mapping tests remain green. | Mitigating |
| R2 | Backend behavior drift breaks parity between macOS and Linux paths. | James Lal | Medium | High | Maintain canonical capability matrix and backend conformance suites. | Linux/macOS backend conformance jobs pass; cross-backend contract checks pass; parity regressions are caught in CI. | Mitigating |
| R3 | Snapshot/checkpoint portability assumptions cause invalid restores. | James Lal | Medium | High | Enforce explicit checkpoint classes and compatibility validation. | Restore/fork APIs reject incompatible metadata deterministically; capability-gated behavior is explicit and tested. | Mitigating |

## Mitigation Execution Tracker

### R1 - Shim Complexity Distorting Core Model

- [x] R1-A: Runtime/product boundary checklist published and linked.
  Evidence: `docs/runtime-api-review.md`, `planning/README.md`.
- [x] R1-B: Compose/Docker unsupported diagnostics standardized.
  Evidence: `vz-27k.8` closure + shim tests.
- [x] R1-C: Regression guards prevent product-domain primitive leakage in runtime/event labels.
  Evidence: `runtime_surface_forbids_product_domain_primitives`, `event_tags_forbid_product_domain_primitives` tests.
- [x] R1-D: CI quality gate for shim mapping and negative paths.
  Evidence: `.github/workflows/ci.yml` step `Runtime V2 Gate - Shim Mapping / Negative Paths`, `vz-stack/tests/quality_gates.rs`.

### R2 - Backend Behavior Drift

- [x] R2-A: Canonical backend capability matrix encoded and validated.
  Evidence: `vz-runtime-contract` capability/parity tests.
- [x] R2-B: Per-backend conformance suites run in CI.
  Evidence: `.github/workflows/ci.yml` jobs `conformance-linux-backend`, `conformance-macos-backend`.
- [x] R2-C: Cross-backend parity gate enforced.
  Evidence: `.github/workflows/ci.yml` job `conformance-cross-backend`.

### R3 - Snapshot Portability Confusion

- [x] R3-A: Checkpoint class contract and compatibility metadata enforced.
  Evidence: `validate_checkpoint_restore_compatibility`, `ensure_checkpoint_class_supported`.
- [x] R3-B: Degradation and mismatch paths return deterministic errors.
  Evidence: checkpoint compatibility tests in `vz-runtime-contract`.
- [x] R3-C: Capability-gated behavior documented in rollout and API review artifacts.
  Evidence: `planning/runtime-v2-rollout.md`, `docs/runtime-api-review.md`.

## Acceptance Criteria by Risk

### R1 Acceptance

- Runtime core entities contain no forbidden product-layer primitives.
- Docker/Compose shim behavior remains adapter-only and diagnostic-first.
- CI fails if shim regression tests fail.

### R2 Acceptance

- Capability matrix shape is stable across supported backends.
- Backend-specific regressions are caught by dedicated conformance lanes.
- Cross-backend parity checks are mandatory in CI.

### R3 Acceptance

- Incompatible checkpoint metadata fails fast with actionable diagnostics.
- Restore/fork behavior explicitly honors checkpoint class and capability gates.
- Portability constraints are documented with no ambiguous claims.

## Review Cadence

- Review frequency: every release candidate and after any transport/backend/shim refactor.
- Trigger immediate review when:
  - CI conformance gates fail on backend parity.
  - Any checkpoint restore regression is reported.
  - Shim coverage adds new command/key translations.
