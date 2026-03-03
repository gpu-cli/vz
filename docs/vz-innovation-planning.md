# vz Innovation Planning

Date: 2026-02-26
Author: collaborative ideation draft

## Purpose

Generate a wider set of differentiated product ideas, then select the strongest plans to execute.

This document is intentionally split into:
1. Idea expansion (many options)
2. Selection logic (why some ideas win)
3. Best plans (execution-ready)

---

## Innovation North Star

Build the first developer sandbox platform that is:
- Local-first by default
- Cloud-expandable when needed
- Deterministic and auditable end-to-end
- AI-agent native at the control-plane level

Short version:
`vz` should feel like "Docker + e2b + deterministic replay + policy engine" in one system.

---

## Expanded Idea Set

## 1) Federated Sandbox Mesh

Run sandboxes across:
- local laptop
- trusted team machines
- managed cloud workers

with one API and scheduler.

```
                +-----------------------------+
                |      vz Control Plane       |
                |  scheduler + policy + auth  |
                +-------------+---------------+
                              |
          +-------------------+-------------------+
          |                   |                   |
+---------v--------+ +--------v---------+ +-------v---------+
| local node       | | team node         | | cloud node      |
| mac/linux runtime| | secure peering    | | autoscaled pool |
+------------------+ +-------------------+ +-----------------+
```

Why novel:
- Avoid vendor lock-in by design.
- Gives teams a smooth local->team->cloud escalation path.

---

## 2) Sandbox Capsules

Define a portable artifact containing:
- runtime/template descriptor
- policy manifest
- state snapshot baseline
- reproducibility checksum chain

```
+-------------------------------------------------------------+
|                     .vzcapsule artifact                     |
|-------------------------------------------------------------|
| manifest.json | policy.json | fs.delta | image.ref | sig   |
+-------------------------------------------------------------+
         | verify signature + policy |
         v
   deterministic launch anywhere (local/team/cloud)
```

Why novel:
- Strong supply-chain + reproducibility story.
- Moves templates from "build recipe" to "portable execution contract."

---

## 3) Time-Travel Debugging for Sandboxes

Record lifecycle events and execution deltas to replay failures.

Core features:
- timeline replay
- "jump to failing command"
- compare runs (why run B diverged from run A)

Why novel:
- Converts sandbox debugging from logs-only into deterministic analysis.

---

## 4) Policy-as-Code Runtime Guardrails

Policy engine controls:
- outbound network
- filesystem access domains
- secrets scope
- tool execution allowlists

at sandbox launch and runtime.

Why novel:
- Security and compliance become product primitives, not add-ons.

---

## 5) Agent-Native Sandbox Sessions

First-class AI agent sessions with:
- scoped tool permissions
- signed receipts for every action
- resumable task contexts tied to sandbox state

Why novel:
- Auditable AI execution in real environments.

---

## 6) Live Collaboration Mode

Multiple developers/agents can join one sandbox with:
- role-based permissions
- shared terminal sessions
- event attribution by actor

Why novel:
- Replaces ad-hoc pair-debugging with structured collaborative sandboxing.

---

## 7) Verified Build-to-Run Chain

Cryptographic chain from:
- source revision
- build steps
- image/template
- runtime launch receipts

Why novel:
- Strong trust story for enterprise and regulated teams.

---

## 8) Cost/Latency-Aware Placement

Scheduler chooses local/team/cloud based on:
- startup latency budget
- estimated cost
- required capabilities

Why novel:
- Users get performance/cost optimization without manual placement logic.

---

## 9) Sandbox Marketplace Protocol

Curated, signed templates/capsules with compatibility and trust metadata.

Why novel:
- Ecosystem growth flywheel.

---

## 10) Offline-First Dev Resilience

All core flows work without internet, then sync state/events when online.

Why novel:
- Best-in-class resilience for traveling/on-prem users.

---

## Selection Matrix (Impact vs Feasibility vs Differentiation)

Scoring scale: 1-5 (higher is better)

| Idea | Impact | Feasibility | Differentiation | Strategic fit | Total |
|---|---:|---:|---:|---:|---:|
| Federated Sandbox Mesh | 5 | 3 | 5 | 5 | 18 |
| Sandbox Capsules | 5 | 4 | 5 | 5 | 19 |
| Time-Travel Debugging | 4 | 3 | 5 | 4 | 16 |
| Policy-as-Code Guardrails | 5 | 4 | 4 | 5 | 18 |
| Agent-Native Sessions | 5 | 3 | 5 | 5 | 18 |
| Live Collaboration | 4 | 3 | 4 | 4 | 15 |
| Verified Build-to-Run Chain | 4 | 4 | 4 | 5 | 17 |
| Cost/Latency Placement | 4 | 3 | 4 | 4 | 15 |
| Marketplace Protocol | 3 | 2 | 4 | 3 | 12 |
| Offline-First Resilience | 4 | 4 | 4 | 5 | 17 |

Top bets by score:
1. Sandbox Capsules
2. Federated Sandbox Mesh
3. Policy-as-Code Guardrails
4. Agent-Native Sessions

---

## Best Plans to Execute

## Plan A: Capsule Platform (Primary)

Goal:
Make `vz` the most reproducible sandbox system.

Deliverables:
- `.vzcapsule` spec v1
- capsule create/verify/run commands
- signed manifest and policy bundle
- snapshot import/export with deterministic checks

Success metrics:
- 95%+ replay success across hosts for supported workloads
- <10s validation time for capsule verification
- adoption in CI and local parity workflows

Risks:
- spec complexity creep
- weak compatibility guarantees early

Mitigation:
- strict v1 scope
- compatibility matrix from day one

---

## Plan B: Federated Control Plane (Primary)

Goal:
One API to run workloads local/team/cloud with policy-aware scheduling.

Deliverables:
- node registration and trust bootstrap
- scheduler placement API
- capability + quota + policy aware dispatch
- run receipts and placement traceability

Success metrics:
- sub-2s placement decisions p95
- >80% successful fallback from unavailable nodes
- measurable cost reduction vs cloud-only baseline

Risks:
- operational complexity
- secure peer networking challenges

Mitigation:
- start with local + one remote class
- staged rollout with strict trust policy defaults

---

## Plan C: Agent Trust Layer (Primary)

Goal:
Make AI-agent execution auditable and safe by default.

Deliverables:
- sandbox-scoped agent identity
- tool permission envelope
- signed action receipts
- replayable agent timeline

Success metrics:
- 100% agent action attribution
- policy violation prevention without manual intervention
- reduced incident triage time via timeline replay

Risks:
- too much friction for developer velocity

Mitigation:
- policy presets: permissive, balanced, strict
- local override workflows with explicit receipts

---

## Integrated Roadmap (Recommended)

```
Phase 1 (0-8 weeks): Foundation
  - unified sandbox API semantics
  - event model + receipts
  - policy engine scaffolding

Phase 2 (8-16 weeks): Capsule + Agent trust
  - .vzcapsule v1
  - signed manifests
  - agent permission envelope + receipts

Phase 3 (16-24 weeks): Federated mesh
  - node registry
  - scheduler
  - local/team/cloud placement

Phase 4 (24-32 weeks): Differentiation hardening
  - time-travel debugging
  - advanced policy packs
  - enterprise controls
```

---

## What To Deprioritize (for now)

- Marketplace protocol (too early before core primitives stabilize)
- Full BYOC enterprise packaging in first wave
- Broad UI investments before API and artifact model harden

---

## 90-Day Execution Backlog (Draft)

1. Define `.vzcapsule` schema and signing model.
2. Add `create/verify/run` capsule APIs and CLI commands.
3. Implement run receipt chain with stable IDs.
4. Add policy checks for network/fs/secrets on launch.
5. Add agent permission envelope and action attribution events.
6. Ship minimal scheduler for local + one remote pool.
7. Publish compatibility and determinism test suite.

---

## Decision

Proceed with a combined strategy:
- Product wedge: **Capsules + Agent Trust**
- Platform wedge: **Federated execution**

This gives immediate differentiation while still converging toward e2b-class platform breadth.

---

## Strategic Update: Autonomous Systems (Deep Dive)

This section reframes the plan around current autonomous system needs and a strict runtime boundary.

## What the market needs now

Teams are not asking for "another chat agent." They need systems that can run real work loops safely:

1. Durable multi-step execution (not single-shot prompts)
2. Policy and approval control before irreversible actions
3. Strong audit evidence and replayability
4. Portable execution between local development and hosted scale
5. Operational controls (quotas, limits, lifecycle guardrails)
6. Standard integration points (APIs, hooks, receipts, events)

The clear signal from current platforms: execution primitives are table stakes; control-plane trust and operability are the differentiator.

---

## Product boundary: external orchestrator vs `vz`

`vz` is the open runtime substrate.  
Orchestration, mission logic, memory strategy, and human workflow UX belong outside `vz`.

```
+----------------------------------------------------------+
| External Orchestrator (private product)                 |
|----------------------------------------------------------|
| mission graph | personas | approvals UX | business logic|
+----------------------------+-----------------------------+
                             |
                             | runtime primitives API
                             v
+----------------------------------------------------------+
| vz (open source runtime substrate)                       |
|----------------------------------------------------------|
| sandboxes | executions | files | checkpoints | events    |
| receipts  | capabilities | policy hooks | placement hints|
+----------------------------+-----------------------------+
                             |
          +------------------+------------------+
          |                  |                  |
    local mac          local linux         hosted linux
```

## Non-negotiable boundary rules

1. No orchestrator-specific nouns in `vz` APIs, types, docs, events, or env vars.
2. `vz` exports generic primitives and extension hooks only.
3. Product semantics compile down to runtime metadata/policy context outside `vz`.
4. One Linux sandbox contract across local and hosted targets.

---

## Positioning update for `vz`

Primary message:

`Run production-like Linux sandboxes on Mac locally and on hosted Linux with the same deterministic runtime contract.`

Differentiators to emphasize:

1. Deterministic runtime and replay-friendly state model
2. Local-first to hosted continuity without workflow rewrite
3. Policy hook and receipt/event primitives for autonomous systems

---

## Missing Primitive Features in `vz` (Current Reality)

This is a runtime-only gap list (no product/orchestrator features).

## P0 (must complete first)

1. Execution data plane completion
- Add streaming exec output primitives and stdin write path parity.
- Current API includes creation/inspect/cancel/resize/signal, but interactive stream coverage is incomplete.

2. Filesystem API surface
- Add first-class file primitives: read, write, list, metadata, upload, download, watch.
- Current router has no dedicated `/v1/files...` surface.

3. Real backend execution wiring through API
- Ensure API routes drive actual runtime operations, not only state-store transitions.
- Prioritize `create_sandbox`, `create_execution`, `create_container`, and build lifecycle flows.

4. Canonical placement and substrate selection
- Add explicit backend/placement semantics in the sandbox spec.
- Must support Linux-on-Mac and hosted Linux under one contract.

## P1 (high leverage next)

1. Event and receipt hardening
- Strengthen receipts with event-range linking, request/policy context, and optional signer integration.
- Keep schema generic and transport-stable.

2. Hooks operationalization
- Wire `policy_hook` and `event_sink` from contract into runtime/API path execution.
- Preserve stable machine-error taxonomy for allow/deny/transport failures.

3. Build and container parity gaps
- Complete stream/cancel/report parity for build operations.
- Close remaining contract gaps where runtime operations are listed but not surfaced end-to-end.

## P2 (scale and operability)

1. Quota/rate control primitives
- Add generic resource quota and rate-limit primitives suitable for hosted execution.

2. Capability profile maturity
- Publish substrate capability profiles (`local-mac`, `local-linux`, `hosted-linux`) with deterministic compatibility behavior.

3. Extended network primitives
- Fill volume/network primitives that remain unclaimed in conformance where strategically relevant.

---

## Implementation sequence update

```
Phase A: Runtime Completion (P0)
  - execution stream + stdin primitives
  - filesystem API
  - real runtime wiring behind API
  - explicit placement semantics

Phase B: Trustable Runtime Interfaces (P1)
  - receipt/event hardening
  - live policy_hook + event_sink integration
  - build/container parity completion

Phase C: Hosted-Ready Runtime (P2)
  - quotas and limits
  - capability profiles by substrate
  - remaining conformance gaps (network/volume as needed)
```

Success criterion:
`An external orchestrator can run autonomous workflows entirely through generic vz primitives, with zero product-specific code inside vz.`
