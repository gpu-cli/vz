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
  one measured real hardware sleep/wake cycle, and leak checks. CI that cannot
  perform host sleep cannot certify this lane.

## Step 3: Prove isolation and runtime invariants

Run at least two Linux Machines in one Environment plus simultaneous Linux
Machines in another Environment. Prove each has an independent Engine, context,
images, volumes, networks, BuildKit cache, sockets, events, and lifecycle.
Recursively inventory guest-visible artifacts and capture execution proof that
BuildKit, Docker, and containerd invoke pinned youki only.

## Step 4: Release-built repeatability

Run all focused suites and `--suite all` within the canonical single staged
`clean-provision` → `persisted-recovery` → `final-cleanup` invocation in
[GOAL-0.4.0.md](GOAL-0.4.0.md). Keep the same candidate digest tuple and exact
content-addressed handoff between phases; do not clean up between the clean and
persisted phases. Each test process starts once, with zero test-case retries.
Retain failed candidates. Run workspace fmt/build/strict clippy/nextest and
existing local VM regression lanes.

## Evidence contract

Write `.artifacts/linux-docker-e2e/<timestamp>/` with per-command logs, exit
status/timing, Environment/Machine identity, host client/plugin versions, Engine
API version, profile/kernel versions, hashes, configs, daemon/adapter logs,
image digests, runtime inventory/invocation proof, cross-context denial, leak
checks, `summary.json`, and `summary.txt`. Tests fail if evidence is absent,
malformed, skipped, or inconsistent.

## Validation

No Linux-target Docker claim becomes ACTIVE until the complete release-built
staged gate passes on the current Mac and every intentional incompatibility is
documented. Focused DEV slices cannot substitute for the full lane or aggregate.

## Implementation checkpoint

`scripts/run-linux-docker-e2e.sh --suite compose` now provisions isolated installed
Developer Machines through public Up, then connects the host Docker clients to
their exact authenticated contexts. Its bounded DEV scope is eight Compose
recipes on three Machines across two Environments, with four continuous sibling
container sentinels and independent raw-receipt replay. Physical results must be
read from the retained candidate evidence; implementation alone is not a pass.

The `--suite build` integration uses that same installed topology and monitoring
boundary for five Buildx recipes on three Machines. Each Machine receives an
exact owned runtime-free BuildKit builder and private cache, using pinned youki;
independent replay binds command receipts and local output bytes. Physical
candidate verification remains required. Cache export/import and denial,
parallel builds, SSH, and full image/cache secret scans are not covered by
these five recipes.

Installed Buildx candidate 1 reached four ready Machines but failed starting
the exact owned `docker-container` builder through youki, before recipe dispatch.
Its 1,497-file failed evidence is retained at
`.artifacts/linux-docker-build-candidate-1`; a separate disposition positively
stopped all four Machines and closed the daemon while preserving disk/object
state. That candidate made no passing physical Buildx-slice claim.

Candidate 4 subsequently reached the running OCI worker after the Developer
IPv4 raw-table fix, but its first nested RUN failed at cgroup PID admission
(`EOPNOTSUPP`), before Python executed. Its evidence and separate positive Stop
disposition remain under `.artifacts/linux-docker-build-candidate-4` and
`.artifacts/linux-docker-build-candidate-4-disposition`. The current integration
restores the pinned upstream BuildKit cgroup-root setup setting and explicitly
requires a private cgroup namespace. The accompanying youki tenant-cgroup fix,
external cgroup observations, and fresh installed verification are tracked by
`vz-amq`; source and offline checks alone are not a passing physical slice.

Candidate 9 subsequently passed the unchanged five recipes on all three tested
Machines, independent raw replay, direct youki lifecycle probes, normal owned
builder/cache-volume removal, and positive public Stops. Its immutable evidence
is `.artifacts/linux-docker-build-candidate-9` (manifest SHA256
`81acf811304fa2e2a4c13de52a4eae37f9ace4f28e1bc8fbd85e938ae0a13163`).
This is DEV installed-Mac evidence, not all Docker scenarios or Delete acceptance.

The separate `--suite artifacts` implementation adds OCI descriptor/layer and
exported-cache validation, plus distinct fresh source/control/importer builder
roles with identical inputs. It must earn its own fresh installed-Mac result;
candidate 9 does not certify this new path. See `vz-mzs.7.1.3` for the exact
acceptance criteria and retained verification status.

`--suite all` fails before provisioning: the 63-scenario dispatcher, full cache
runtime inventory, and remaining target/aggregate acceptance are unfinished.
See [runner usage and evidence limits](../../scripts/helpers/linux_docker_e2e.md).
