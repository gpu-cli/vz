# Architecture: multi-instance Developer Environment topologies

## Aggregate model

```text
ProjectDefinition (versioned desired topology)
└── EnvironmentInstance (isolation, ownership, lifecycle, and evidence root)
    ├── WorkspaceBinding[]
    ├── MachineInstance[]
    │   ├── TargetSpec { os, arch, image/version, requirements }
    │   ├── HostBackend + TargetAdapter + CapabilitySet
    │   ├── MachineIncarnation and target-native persistent state
    │   └── Linux only: private Docker/containerd/BuildKit + youki
    ├── Network[] + NIC attachments + declared service paths
    ├── Endpoint[] + DNS/ingress/TLS/NAT/firewall/egress
    └── Volume[], SecretBinding[], Fault[], Execution[], Receipt[]
```

The Environment is a topology aggregate, not a VM or one target OS. A
single-Machine topology is the default simple case. Target OS and target-native
capabilities live on each Machine, permitting Linux and native macOS Machines in
one Environment on macOS today and Windows Machines on Windows later.

`Sandbox` remains a low-level runtime-contract name for an isolation primitive
during migration. A Linux Docker container is a workload inside one Linux
Machine. Neither is a peer product journey.

## Definitions and identity

- `ProjectDefinition`: versioned machines, networks, services, endpoints,
  workspace projections, volumes, policies, and reproducible inputs.
- `WorkspaceBinding`: associates a checkout/worktree with a project and an
  Environment without making the native path a persistent identity.
- `EnvironmentInstance`: immutable ID, human name, definition digest,
  parameters, ownership graph, aggregate state, and zero or more bindings.
- `MachineInstance`: immutable ID and stable name within an Environment, target
  specification, resources, capabilities, attachments, logical state, and a
  replaceable incarnation.

Canonical resource identity is `(project_id, environment_id, machine_id)` where
applicable. Names and paths are selectors. Every persisted child resource
includes `environment_id`; Machine-owned resources also include `machine_id`.
Identifiers used in paths, sockets, contexts, networks, and routes are bounded
and collision checked.

Selection order is explicit ID/name, process-scoped selector, unambiguous
workspace binding, then a declared default or sole Machine. Ambiguity fails with
candidates. No mutable global current-Environment symlink or Docker selector is
permitted.

## Reconciliation and lifecycle

The Environment state machine owns aggregate reconciliation:

```text
Creating -> Reconciling -> Ready <-> Stopped -> Deleting -> Deleted
    |            |          |          |           |
    +------------+----------+----------+---------> Failed
```

Machines have child state and ordered dependencies. Environment `up` plans the
topology, allocates only owned resources, starts dependency closure, waits for
declared readiness, and streams progress. A Machine may fail independently; the
aggregate reports degraded/failed state without identity substitution.

- `stop` removes live compute and endpoints while preserving the Environment,
  Machine identities, declared topology, and persistent state.
- Machine rebuild changes its incarnation but not logical identity or endpoints.
- `delete` traverses the Environment ownership graph and cannot select or remove
  a sibling Environment's resources.
- Daemon restart reconstructs routes, DNS, sockets, contexts, and state from the
  authoritative aggregate, removing only provably stale owned resources.
- Long-running mutations use idempotency keys, stream progress, and terminate
  with a receipt. Unary APIs remain for short bounded reads.

## Machine backends and capabilities

The daemon selects a Machine backend from `(host OS, Machine target OS,
architecture, requested capabilities)`. Unsupported tuples fail explicitly.

- `HostBackend`: compute isolation, disks, native shares, host networking, and
  target transport for one Machine.
- `TargetAdapter`: native boot, process/console, service supervision, shutdown,
  and target inspection.
- `CapabilitySet`: Docker/OCI, POSIX TTY/signals, Windows console, filesystems,
  ports, suspend, snapshot, checkpoint, GUI, and target-specific behavior.
- `EnvironmentSupervisor`: topology ordering, aggregate lifecycle, resource
  ownership, endpoint readiness, and recovery.
- `WorkspaceBackend`: authorized projection as read-write, read-only, or
  snapshot with explicit writer semantics.
- `StorageBackend`: Machine disks plus Environment-owned volumes; multi-Machine
  sharing requires a protocol and consistency contract, never silent block
  multi-attach.

Virtualization.framework, vsock, VirtioFS, Linux namespaces, WSL/Hyper-V,
gateway addresses, and endpoint paths remain diagnostics rather than portable
configuration.

## Linux Machine Docker boundary

Each Linux Machine reconciles an independent private Docker Engine, containerd,
BuildKit state, image/volume stores, networks, credentials, endpoint, and Docker
context. Even Machines in the same Environment cannot share a daemon implicitly.
Shared registries/caches must be declared topology resources.

The Engine Endpoint Adapter authorizes and routes each request by
`(environment_id, machine_id)`, preserves Docker streaming/hijacking semantics,
and performs only narrowly authorized host-path translation. `vz status`
returns a context for every Linux Machine. Normal use is:

```bash
docker --context <context-from-vz-status> ps
```

vz never changes Docker's default context, exposes an Environment-wide socket,
or falls back to Docker Desktop, system Docker, another Machine, or another
Environment. Stopped/missing/ambiguous/unauthorized targets fail closed. Native
macOS and Windows Machines have no implicit Docker capability. Pinned youki is
the only OCI runtime in Linux Machines.

## Environment network fabric

Each Environment gets a separate routing domain, split DNS view, gateways, NAT
state, firewall, port registry, ingress, impairment state, and credentials.
Overlapping CIDRs and repeated DNS/service names across Environments are valid.
No trusted flat network is created implicitly.

Machines attach to declared networks and service paths:

- private segments provide explicit Environment-local connectivity;
- simulated-public segments put clients and services behind separate routing
  and force DNS, ingress, TLS, firewall, and NAT behavior through an
  Environment-local edge;
- external egress is independently offline, enabled, or allowlisted;
- host imports/exports are explicit, loopback-only by default, and
  collision-safe; physical LAN publication is a separate high-friction policy.

Per-Environment authoritative DNS allows aliases such as `api.shop.test` to
resolve differently in simultaneous instances. Managed TLS uses
Environment-scoped trust; host trust installation is explicit. Container port
publication first enters the Machine/Environment boundary and never silently
claims a global host port.

Baseline and runtime fault policies cover seeded latency, jitter, loss,
duplication, reordering, bandwidth, queueing, MTU, reset, DNS failure, and
partition. Runtime faults have scope, TTL, and receipts so they cannot outlive
the test that created them.

Cross-Environment traffic is default-deny. A directional `EnvironmentPeerGrant`
may expose one declared endpoint/protocol/port with owner and expiry. It does not
merge routes, permit transitivity, or grant control-plane, Docker, storage,
credential, or private-DNS access. Full L3 peering is not part of the public 0.4
product.

## Public CLI and API

The public Developer Environment CLI contains exactly:

```text
vz up
vz exec
vz status
vz stop
vz delete
```

`--environment` selects an instance; `--machine` selects exec/status scope.
Topology is declared in the project definition and operated as a unit. No
public or hidden `vz dev`, `run`, `shell`, `list`, `logs`, `restart`, `docker`,
`stack`, `network`, `machine`, or `vm` family survives the 0.4 migration. Bare
`vz` is read-only. Docker operations remain Docker operations.

The typed API is intentionally richer:

- Environment create/reconcile/get/list/watch/start/stop/delete;
- Machine get/list/watch/start/stop/restart/rebuild/exec;
- files, workspace projections, volumes, snapshots, and secrets;
- networks, endpoints, DNS, ingress, egress, faults, and peer grants;
- streaming events/logs/execution/progress and terminal receipts; and
- authorized Admin APIs for backend images, provisioning, diagnostics, and raw
  primitives that are not product-level CLI concepts.

## Compatibility and release rule

Legacy records migrate deterministically into a ProjectDefinition containing one
Environment with one Machine. Legacy command spellings receive actionable
migration errors; they are not hidden aliases. Hardened keeps its low-level
security contract and does not inherit Developer/Docker capabilities.

Public status remains **DEV** until the strict local-Mac, release-built gates in
[`GOAL-0.4.0.md`](GOAL-0.4.0.md) pass with complete evidence. A one-Machine demo,
unit tests, or guest-local Docker commands do not establish product completion.
