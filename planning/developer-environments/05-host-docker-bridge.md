# Per-environment Engine Endpoint Adapter and Docker contexts

Depends on: Converged Developer Environment CLI; Implicit private Docker service; Environment isolation, storage, and networking

## Purpose

Connect unmodified Docker tooling to exactly one Docker-capable Linux-target Developer Environment without a global mutable selector. macOS is implemented first; Linux and Windows hosts follow through host-specific endpoint adapters.

## Step 1: Per-environment endpoint routing

- Allocate a bounded private host Unix socket and environment-specific guest route.
- Keep the persisted endpoint type opaque and backend-owned so Windows can use a named pipe or another authenticated Docker-context transport without changing the public environment contract.
- Implement an Engine Endpoint Adapter rather than promising a permanently byte-transparent proxy. Preserve Docker HTTP streaming, connection hijacking, half-close, cancellation, and concurrency; permit narrowly scoped, authorization-checked host bind-path translation through the Share Backend where the host and Linux environment use different path syntax.
- Bind with least-privilege permissions and authenticate the environment identity.
- Support concurrent clients, streaming/hijacked HTTP connections, half-close, cancellation, and backpressure required by Docker exec/attach/events/build.
- Fail closed on stale, stopped, missing, ambiguous, or unauthorized targets.

## Step 2: Managed Docker contexts

- Automatically create/repair a stable context during environment reconciliation.
- Record ownership metadata so delete removes only contexts managed for that environment.
- Never select or rewrite Docker's global default context.
- Preserve context and endpoint identity over stop/restart; remove the live socket while stopped.

## Step 3: Docker CLI compatibility

Support the normal host `docker`, `docker compose`, and `docker buildx` clients for Linux targets, including API negotiation, streaming progress, attach/exec TTY, events, stats, and concurrent requests. No acceptance test may call an internal Rust API instead of the host client. Non-Docker targets expose no Engine endpoint.

## Step 4: Observability and recovery

Expose proxy state, target environment, engine readiness, versions, active clients, and last error through status/logs. Atomically replace stale sockets and recover from proxy/daemon/VM restarts without cross-routing.

## Validation

- Protocol tests for streaming, upgrades/hijack, cancellation, permissions, invalid routing, and bind-path authorization/translation.
- Direct host Docker CLI smoke against two simultaneous contexts.
- Stop/delete failure behavior proves no Docker Desktop or other-environment fallback.
