# Runtime V2 Rollout Tracker (Phases 0-6)

Status: Active
Owner: James Lal
Last Updated: 2026-02-24
Source Plan: ../private-vz/runtime-v2/09-rollout-validation-plan.md

## Tracker Rules

- Each phase has explicit entry criteria, exit gates, and required evidence.
- A phase is considered complete only when all exit gates are satisfied.
- Evidence links must be reproducible (tests, docs, CI config, or Beads issue closures).
- Owner is responsible for keeping gate status current.

## Phase Gate Tracker

| Phase | Scope | Owner | Entry Criteria | Exit Gates | Evidence Required | Linked Beads | Gate Status |
|---|---|---|---|---|---|---|---|
| 0 - Contract Lock | Lock normative model + error/idempotency taxonomy | James Lal | Runtime V2 normative docs frozen (`00`-`08`) | G0.1 architecture sign-off; G0.2 no unresolved doc contradictions; G0.3 machine taxonomy frozen in code | Runtime contract machine-code tests; boundary checklist doc; Beads closure notes | `vz-27k.1`, `vz-27k.9` | Complete |
| 1 - Container Core + Workspace | Container lifecycle + workspace execution baseline | James Lal | Phase 0 complete | G1.1 workspace persists across one-off runs; G1.2 execution stream/control integration pass | Runtime/CLI tests for create/exec/control; feature closure evidence | `vz-27k.2`, `vz-27k.3` | Complete |
| 2 - Stack Upgrades | Deterministic stack reconciler + durable state | James Lal | Phase 1 complete | G2.1 deterministic web+db+cache apply/down/restart; G2.2 restart recovery behavior validated | `vz-stack` reconcile/state-store/health tests; fixture e2e runs | `vz-27k.4` | Complete |
| 3 - Build and Image Flow | Digest-first pull/build semantics + idempotency | James Lal | Phase 2 complete | G3.1 reproducible digest output for controlled fixtures; G3.2 build idempotency/conflict tests pass | Build/path tests, idempotency checks, runtime contract closure | `vz-27k.3` | Complete |
| 4 - Docker/Compose Shims | Docker CLI + Compose translation parity | James Lal | Phase 3 complete | G4.1 supported fixtures map to runtime semantics; G4.2 unsupported options emit structured `unsupported_operation` | `vz-cli` shim tests, compose fixtures, machine-error assertions | `vz-27k.8` | Complete |
| 5 - Snapshot/Replay | Checkpoint restore/fork with capability gates | James Lal | Phase 4 complete | G5.1 `fs_quick` restore conformance passes; G5.2 capability-gated `vm_full` behavior validated where enabled | Checkpoint compatibility tests + backend capability conformance evidence | `vz-27k.5`, `vz-27k.7` | Complete |
| 6 - Transport/API Stabilization | gRPC/OpenAPI parity + SDK readiness | James Lal | Phase 5 complete | G6.1 transport parity suite passes; G6.2 CLI/API core behavior parity established | Transport contract tests, parity assertions, stabilization closure evidence | `vz-27k.6`, `vz-27k.7` | Complete |

## Gate Checklist by Phase

### Phase 0

- [x] G0.1 Architecture review checkpoint captured in planning/docs.
- [x] G0.2 Runtime error taxonomy and idempotency surface stabilized in `vz-runtime-contract`.
- [x] G0.3 Boundary review checklist exists and is linked from planning index.

### Phase 1

- [x] G1.1 Container and workspace runtime flows implemented behind runtime contract.
- [x] G1.2 Execution control paths validated with scoped tests.

### Phase 2

- [x] G2.1 Reconciler/state store determinism and dependency gating implemented.
- [x] G2.2 Stack fixture/e2e coverage validates recovery and convergence behavior.

### Phase 3

- [x] G3.1 Build/image flow enforces digest-first behavior.
- [x] G3.2 Idempotency/conflict semantics are covered by tests and contract checks.

### Phase 4

- [x] G4.1 Docker shim command surface and compose translation matrix implemented.
- [x] G4.2 Unsupported option diagnostics standardized and tested.

### Phase 5

- [x] G5.1 Checkpoint lineage/compatibility constraints enforced.
- [x] G5.2 Capability gating for checkpoint classes validated across backends.

### Phase 6

- [x] G6.1 Shared transport contract and runtime machine-error envelope stabilized.
- [x] G6.2 Backend parity and transport-facing behavior validated by conformance tests.

## Open Rollout Items (Post Phase Encoding)

- `vz-27k.10.2`: CI quality gates enforcement.
- `vz-27k.10.3`: benchmark methodology artifacts.
- `vz-27k.10.4`: risk/mitigation tracker.
- `vz-27k.10.5`: final validation pass for rollout discipline artifacts.
