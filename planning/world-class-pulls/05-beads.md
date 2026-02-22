# 05 - Bead Breakdown

## Execution Strategy

Deliver in dependency order so each increment is benchmarkable and reversible.

Legend:

- `Blockers`: prerequisite bead IDs.
- `Done when`: objective acceptance signal.

## Milestone A - Pull Contract and Event Foundation

### P01 - Pull state machine + error taxonomy

- Scope: formalize pull phases, terminal states, and stable error kinds.
- Blockers: none
- Done when: pull lifecycle is represented by typed state machine and covered by unit tests.

### P02 - Progress event schema v1

- Scope: define typed pull event schema with `pull_id`, `seq`, timestamps, phase, counters.
- Blockers: P01
- Done when: schema supports per-layer and aggregate progress requirements in `01-target-contract.md`.

### P03 - Event sink abstraction and trace wiring

- Scope: add pull event sink trait in `vz-oci`; wire to existing trace/event output path.
- Blockers: P02
- Done when: all pull phases emit traceable structured events.

## Milestone B - Parallel Download Engine

### P04 - Parallel blob scheduler

- Scope: replace serial layer loop with bounded parallel downloader.
- Blockers: P01
- Done when: cold pull speed improves on cohort baseline with no correctness regressions.

### P05 - Per-registry/global concurrency guards

- Scope: add global and per-registry semaphores with tunable defaults.
- Blockers: P04
- Done when: no unbounded fanout and throttling behavior is observable.

### P06 - Per-digest in-flight de-duplication

- Scope: coalesce identical blob requests across concurrent pulls.
- Blockers: P04
- Done when: duplicate concurrent pulls fetch each digest at most once.

### P07 - Atomic blob write path under parallel load

- Scope: harden temp-file + atomic-rename behavior for concurrent writers.
- Blockers: P04, P06
- Done when: no partial/corrupt committed blobs under stress.

## Milestone C - Reliability and Resume

### P08 - Retry classifier + bounded backoff

- Scope: classify retryable errors and apply jittered exponential backoff.
- Blockers: P04
- Done when: transient fault suite meets success target.

### P09 - Resumable download journal

- Scope: persist partial blob download state and resume using HTTP range where possible.
- Blockers: P08
- Done when: interrupted pull can continue without full blob restart for supporting registries.

### P10 - Adaptive concurrency on 429/5xx

- Scope: dynamic pull concurrency reduction/recovery policy based on observed errors.
- Blockers: P05, P08
- Done when: pull remains stable under registry throttling and recovers automatically.

### P11 - Mirror/fallback chain

- Scope: registry mirror policy with deterministic fallback and health scoring.
- Blockers: P08
- Done when: mirror outage tests pass with transparent failover.

## Milestone D - Parallel Unpack and Assembly

### P12 - Parallel unpack worker pool

- Scope: unpack verified blobs in parallel via blocking worker pool.
- Blockers: P04, P07
- Done when: unpack throughput improves with no extraction correctness regressions.

### P13 - Deterministic ordered layer apply guardrails

- Scope: preserve ordered rootfs apply semantics while download/unpack are parallel.
- Blockers: P12
- Done when: deterministic rootfs tests pass across repeated runs.

## Milestone E - UI-Quality Progress

### P14 - Progress aggregator engine

- Scope: derive overall percent/throughput/ETA from low-level layer events.
- Blockers: P02, P03, P04
- Done when: aggregator emits stable snapshots with explicit unknown-ETA reasons.

### P15 - Heartbeat and no-silent-gap guarantees

- Scope: emit heartbeat/status events during stalls, auth waits, retries, and throttling.
- Blockers: P14
- Done when: no active pull has silent UI gap >2s.

### P16 - Snapshot smoothing and anti-flicker policy

- Scope: smoothing/debounce rules for progress updates and ETA jitter bounds.
- Blockers: P14
- Done when: UI replay tests show bounded jitter and no oscillation flicker.

### P17 - CLI/UI stream integration

- Scope: thread pull progress stream through CLI and UI-facing interfaces.
- Blockers: P14, P15
- Done when: UI displays live pull timeline with terminal summary event.

## Milestone F - Validation and Rollout

### P18 - Baseline benchmark harness

- Scope: capture serial baseline and publish comparable reports.
- Blockers: P03
- Done when: baseline reports are versioned and reproducible.

### P19 - Performance regression gates

- Scope: enforce throughput and latency budgets in CI tiers.
- Blockers: P18, P04, P12
- Done when: PR/nightly gates fail on pull performance regressions.

### P20 - Fault injection suite

- Scope: network reset/timeout/429/process interruption reliability tests.
- Blockers: P08, P09
- Done when: reliability SLOs are met in automated runs.

### P21 - Progress quality replay tests

- Scope: golden event traces and UI quality assertions (ordering, cadence, terminal event).
- Blockers: P14, P15, P16, P17
- Done when: progress stream contract is automatically enforced.

### P22 - Feature flags and rollback controls

- Scope: runtime toggles for parallel pull/resume/adaptive controls/mirror policy.
- Blockers: P04, P08, P11
- Done when: each major feature can be disabled safely in production.

## Suggested First Execution Slice

Start with:

`P01 -> P02 -> P03 -> P04 -> P05 -> P06 -> P07 -> P14 -> P15 -> P17`

This delivers immediate throughput gains plus high-quality progress events for UI while keeping deterministic rollback options.

