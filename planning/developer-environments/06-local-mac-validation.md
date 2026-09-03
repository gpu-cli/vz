# Linux-on-macOS Docker validation

Depends on: Minimal five-verb Developer Environment CLI; implicit Docker per
Developer-profile Linux Machine; multi-environment topology isolation;
per-Developer-Linux-Machine Engine
Endpoint Adapter and contexts

## Purpose

Release-gate the universal Linux Machine target's Docker capability from the
current Apple Silicon Mac. This target gate feeds the aggregate topology gate;
it does not replace it.

## Step 1: Build the dedicated harness

Create `scripts/run-linux-docker-e2e.sh` with focused suites for bootstrap/API,
lifecycle, images/registry, builds/buildx, Compose, networking, storage,
recovery/concurrency, and security, plus `--suite all`. It must use local
vz-managed Linux Machines and the Mac's installed Docker CLI/plugins through
Machine-specific managed contexts—never Docker Desktop, internal shortcuts,
guest-side substitutes, ad-hoc SSH, or external hosts.

## Step 2: Exercise the compatibility matrix

- Engine: version/info/ping, API negotiation, events/stats, concurrent streams.
- Lifecycle: create/run/start/stop/restart/kill/wait/inspect/logs/attach/exec/pause/rm, stdin/TTY/resize/signals/exit codes, health and restart policies.
- Images/registry: pull/tag/history/save/load/rm, disposable TLS registry auth/pull/push, interruption and redaction.
- Builds: Dockerfile and multi-stage behavior, args/secrets/SSH, cache import/export/prune/persistence, cancellation, parallel buildx, load/push.
- Compose: config/pull/build/up/ps/logs/exec/restart/down, dependencies, health gates, volumes, binds, networks, ports.
- Networking/storage: DNS, TCP/UDP/HTTPS egress, host reachability, ports, named volumes, approved Mac bind mounts, Unicode/spaces/symlinks/RO, large/many files, disk-full recovery.
- Reliability: cold/warm boots, at least 20 containers, parallel Machines and
  Environments, daemon/adapter crash, abrupt VM stop, interrupted operations,
  sleep/wake where automatable, and leak checks.

## Step 3: Prove isolation and runtime invariants

Run at least two Linux Machines in one Environment plus simultaneous Linux
Machines in another Environment. Prove each has an independent Engine, context,
images, volumes, networks, BuildKit cache, sockets, events, and lifecycle.
Recursively inventory guest-visible artifacts and capture execution proof that
BuildKit, Docker, and containerd invoke pinned youki only.

## Step 4: Release-built repeatability

Run all focused suites and `--suite all` twice against release-built artifacts: first from clean environment state, then reusing images/cache/volumes after stop/restart. Run workspace fmt/build/strict clippy/nextest and existing local VM regression lanes.

## Evidence contract

Write `.artifacts/linux-docker-e2e/<timestamp>/` with per-command logs, exit
status/timing, Environment/Machine identity, host client/plugin versions, Engine
API version, profile/kernel versions, hashes, configs, daemon/adapter logs,
image digests, runtime inventory/invocation proof, cross-context denial, leak
checks, `summary.json`, and `summary.txt`. Tests fail if evidence is absent,
malformed, skipped, or inconsistent.

## Validation

No Linux-target Docker claim becomes ACTIVE until the complete release-built gate passes twice on the current Mac and every intentional incompatibility is documented.
