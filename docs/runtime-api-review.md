# Runtime API Boundary Review Checklist

## Purpose

This checklist is mandatory for new Runtime V2 API surface changes.
It prevents product-domain semantics from leaking into runtime contracts.

Runtime scope is limited to compute/runtime primitives:

- command/spec execution
- environment and mounts
- resource and network controls
- lifecycle and observability events
- opaque policy references and metadata

## Hard Boundary Rules

The runtime contract MUST NOT introduce first-class product entities such as:

- identity provider models
- memory/knowledge provider models
- tool marketplace or tool gateway models
- mission/workflow orchestration types

If a product concern is needed, it must compile down to runtime-native fields
(`env`, mounts, limits, policy refs, opaque metadata).

## Required Review Questions

Every new API, enum, struct field, or event type must pass all checks:

1. Is this runtime semantics (not product semantics)?
2. Can this be expressed with existing generic metadata/policy refs?
3. Does this preserve backend/transport parity?
4. Is naming product-neutral and Docker-neutral in core entities?
5. Does this keep state-machine invariants unchanged?
6. Are extension failures mapped to stable machine error codes?

Any "no" answer blocks merge until redesign.

## Redesign Triggers

Redesign is required if any proposed change:

- introduces product nouns in core contract types
- requires backend-specific semantics in a shared type
- adds extension hooks that can bypass lifecycle invariants
- relies on ad-hoc error text instead of stable machine taxonomy
- duplicates existing generic metadata mechanisms

## Approved Extension Pattern

Use only generic extension points:

- policy hooks at preflight/enforcement boundaries
- event sink adapters
- opaque metadata passthrough for trace/correlation

Extension hooks may deny or observe. They must not mutate core state-machine
transitions.

## PR Template Snippet

Include this in Runtime V2 contract PRs:

- Boundary checklist complete: yes/no
- Product-domain nouns introduced: yes/no
- New extension points added: yes/no
- Failure taxonomy mapping updated: yes/no
- Backend parity impact assessed: yes/no

If any item is "yes/no" in a risky direction, link the redesign decision.
