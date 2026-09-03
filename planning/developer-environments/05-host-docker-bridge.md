# Per-Linux-Machine Engine Endpoint Adapter and Docker contexts

Depends on: Minimal five-verb Developer Environment CLI; implicit Docker per
Linux Machine; multi-environment isolation and topology networking

## Purpose

Connect unmodified Docker tooling to exactly one Docker-capable Linux Machine
without a global mutable selector. macOS is implemented first; Linux and Windows
hosts follow through host-specific endpoint adapters.

## Step 1: Per-Machine endpoint routing

- Allocate a bounded private host endpoint and guest route keyed by
  `(environment_id, machine_id)`.
- Keep the persisted endpoint type opaque and backend-owned so Windows can use a named pipe or another authenticated Docker-context transport without changing the public environment contract.
- Implement an Engine Endpoint Adapter rather than promising a permanently
  byte-transparent proxy. Preserve Docker streaming/hijacking, half-close,
  cancellation, and concurrency; permit narrowly scoped, authorized host bind
  translation where host and Linux Machine path syntax differs.
- Bind with least-privilege permissions and authenticate both Environment and
  Machine identity.
- Support concurrent clients, streaming/hijacked HTTP connections, half-close, cancellation, and backpressure required by Docker exec/attach/events/build.
- Fail closed on stale, stopped, missing, ambiguous, or unauthorized targets.

## Step 2: Managed Docker contexts

- Automatically create/repair one stable context per Linux Machine during
  Environment reconciliation.
- Record ownership metadata so Machine rebuild preserves logical selection and
  Environment delete removes only its managed contexts.
- Never select or rewrite Docker's global default context.
- Preserve context and endpoint identity over stop/restart; remove the live socket while stopped.

## Step 3: Docker CLI compatibility

Support normal host `docker`, `docker compose`, and `docker buildx` clients for
Linux Machines, including API negotiation, streaming progress, attach/exec TTY,
events, stats, and concurrent requests. No acceptance test may substitute an
internal Rust API. Non-Docker Machines expose no Engine endpoint.

## Step 4: Observability and recovery

Expose adapter state, target Environment/Machine, engine readiness, versions,
active clients, and last error through status/events. Atomically replace stale
sockets and recover from adapter/daemon/Machine restarts without cross-routing.

## Validation

- Protocol tests for streaming, upgrades/hijack, cancellation, permissions, invalid routing, and bind-path authorization/translation.
- Direct host Docker CLI smoke against two Linux Machines in one Environment and
  simultaneous Machines in other Environments.
- Stop/delete failure proves no Docker Desktop, sibling-Machine, system-daemon,
  or other-Environment fallback.
