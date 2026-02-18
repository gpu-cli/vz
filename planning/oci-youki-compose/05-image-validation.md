# 05 - Image Validation Matrix (Common Images)

## Objective

Define extensive, repeatable validation for widely used OCI images so `youki`-backed runtime behavior is proven before Compose replacement claims.

## Validation Principles

- Validate behavior, not just boot success.
- Test both `guest-exec` (legacy) and `oci-runtime` (youki) during migration.
- Keep a fast PR smoke suite and a broader scheduled conformance suite.
- Fail on semantic regressions (signals, user, mounts, entrypoint/cmd behavior), not only crashes.

## Common Image Cohorts

### Base distro images

- `ubuntu:24.04`
- `debian:bookworm-slim`
- `alpine:3.20`
- `fedora:41`

### Language/runtime images

- `python:3.12-slim`
- `node:22-alpine`
- `rust:1.85-slim`
- `golang:1.24`
- `eclipse-temurin:21-jre`

### Service images (Compose-critical)

- `nginx:1.27-alpine`
- `redis:7-alpine`
- `postgres:16-alpine`

## Required Validation Dimensions

For each image in scope:

1. Pull and digest validation.
2. OCI config translation correctness:
   - entrypoint/cmd default behavior
   - env merging
   - working directory
   - user
3. Lifecycle correctness:
   - create/start/state
   - exec during running state
   - stop (SIGTERM grace) then force kill fallback
   - delete cleanup
4. Filesystem/mount correctness:
   - read-only base rootfs
   - bind mount read/write behavior
   - named volume persistence (where applicable)
5. Networking:
   - outbound egress toggle
   - published TCP ports
   - UDP publish path (when implemented)
6. Compose readiness semantics:
   - dependency ordering
   - healthcheck gating
   - service-to-service connectivity by name

## Test Matrix

## Tier 1: PR smoke (required on every PR)

Images:

- `alpine:3.20`
- `python:3.12-slim`
- `nginx:1.27-alpine`

Checks:

- pull + run + exit code
- env/cwd/user propagation
- port publish TCP
- graceful stop

## Tier 2: Full conformance (nightly + release)

Images:

- all cohorts listed above

Checks:

- full lifecycle + mount + networking + compose stack scenarios
- restart recovery against persisted runtime state
- cleanup idempotency

## Tier 3: Weekly stress

Scenarios:

- repeated create/start/stop/delete loops (100+ iterations/image cohort subset)
- concurrent multi-service stacks
- orphan recovery after host process crash/restart

## Canonical Validation Scenarios

### S1 - Entrypoint/Cmd resolution

- image defaults only
- CLI override command only
- CLI override command + args

Pass condition:

- final argv observed in guest matches OCI precedence rules.

### S2 - User and permissions

- run as image default user
- run as explicit numeric UID/GID
- run as explicit username when present

Pass condition:

- process identity and file ownership behavior match expected UID/GID semantics.

### S3 - Mount semantics

- bind mount read-write
- bind mount read-only
- named volume persisted across restart

Pass condition:

- write/read expectations hold and mount topology changes trigger recreate behavior.

### S4 - Signal handling

- stop sends SIGTERM with timeout, then SIGKILL fallback.

Pass condition:

- containers with signal handlers exit gracefully when possible; forced kill works deterministically when not.

### S5 - Service image behavior

- `nginx` reachable on published port.
- `redis` responds to ping.
- `postgres` readiness and data persistence via named volume.

Pass condition:

- baseline service workloads run without manual image-specific hacks.

### S6 - Compose fixture validation

Fixtures:

- `web + redis`
- `web + postgres + redis`

Pass condition:

- deterministic startup order, name-based connectivity, stable restart behavior.

## Artifacts and Reporting

Persist per test run:

- pulled image digest
- generated OCI `config.json`
- runtime lifecycle event log
- stdout/stderr and exit codes
- timing metrics (create->ready, stop latency)

Report output:

- pass/fail by image and scenario
- regression delta from prior baseline
- flaky test quarantine list (must be zero for release cut)

## CI Gating Policy

- PR merge gate: Tier 1 must pass.
- nightly health gate: Tier 2 failures create blocking issues.
- release gate: last 3 nightly Tier 2 runs green; latest weekly Tier 3 green.

## Initial Implementation Beads (Validation Track)

- `B25`: image validation harness crate/module for shared fixtures.
- `B26`: image cohort manifest with pinned tags/digests.
- `B27`: CI infrastructure for Tier 1/Tier 2/Tier 3 schedules.
- `B28`: PR smoke gate.
- `B29`: nightly conformance gate.
- `B30`: weekly stress gate.
