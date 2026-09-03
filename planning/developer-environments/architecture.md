# Architecture: multi-instance Developer Environment topologies

## Aggregate model

```text
ProjectDefinition (versioned desired topology)
└── EnvironmentInstance (isolation, ownership, lifecycle, and evidence root)
    ├── WorkspaceBinding[]
    ├── MachineInstance[]
    │   ├── MachineProfile { Developer | Hardened }
    │   ├── TargetSpec { os, arch, image/version, requirements }
    │   ├── HostBackend + TargetAdapter + CapabilitySet
    │   ├── MachineIncarnation and target-native persistent state
    │   └── Developer Linux: private Docker/containerd/BuildKit; all Linux: youki
    ├── Network[] + NIC attachments + declared service paths
    ├── Endpoint[] + HostImport[] + HostExport[]
    │   └── DNS/ingress/TLS/NAT/firewall/EgressPolicy[]
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
- `WorkspaceBinding`: associates an opaque workspace token with a project and
  an Environment. The token is created once in the resolved per-worktree Git
  metadata directory; the native checkout path is not persistent identity.
- `EnvironmentInstance`: immutable ID, human name, definition digest,
  parameters, ownership graph, aggregate state, and zero or more bindings.
- `MachineInstance`: immutable ID and stable name within an Environment,
  explicit Developer/Hardened profile, target specification, resources,
  capabilities, attachments, logical state, and a replaceable incarnation.

Canonical resource identity is `(project_id, environment_id, machine_id)` where
applicable. Names and workspace bindings are selectors; paths are discovery or
diagnostic data only. Every persisted child resource includes `environment_id`;
Machine-owned resources also include `machine_id`. Identifiers used in paths,
sockets, contexts, networks, and routes are bounded and collision checked.

Environment selection has strict precedence: an explicit ID/name, then the
process-scoped `VZ_ENVIRONMENT_ID`, then the unambiguous binding for the current
workspace token. `VZ_ENVIRONMENT_ID` accepts an immutable Environment ID only.
A selector that is present but invalid or stale fails at that level; resolution
never falls through to a lower-precedence selector. Within the selected
Environment, Machine selection uses an explicit ID/name, then the process-scoped
`VZ_MACHINE_ID`, then the declared default or sole Machine. `VZ_MACHINE_ID`
accepts an immutable Machine ID only and is ownership-checked against that
Environment; a present invalid, stale, or foreign Machine ID fails without
falling through. Ambiguity fails with candidates. No mutable global
current-Environment symlink or Docker selector is permitted.

The workspace token is random and opaque, is stored at
`<resolved-per-worktree-git-dir>/vz/workspace-id` rather than under the checkout
root, and is the only authorizing key for workspace binding lookup. It survives
a worktree move; a newly created worktree or clone receives a new token. A raw
checkout path or persisted `path_hint` may be recorded and refreshed for
diagnostics, but is never identity, proof of a binding, or authority to select or adopt an
Environment.

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
architecture, profile, requested capabilities)`. Unsupported tuples fail
explicitly. Developer is the primary capability-rich profile. Hardened is a
restricted Linux profile with no implicit Docker; native/Hardened combinations
are unsupported in 0.4.

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

Each Developer-profile Linux Machine reconciles an independent private Docker Engine, containerd,
BuildKit state, image/volume stores, networks, credentials, endpoint, and Docker
context. Even Machines in the same Environment cannot share a daemon implicitly.
Shared registries/caches must be declared topology resources.

The Engine Endpoint Adapter authorizes and routes each request by
`(environment_id, machine_id)`, preserves Docker streaming/hijacking semantics,
and performs only narrowly authorized host-path translation. `vz status`
returns a context for every Developer-profile Linux Machine. Normal use is:

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
- host imports and exports are separate explicit resources; imports authorize
  one guest-to-host protocol/port and exports authorize one host-to-guest
  endpoint;
- exports bind collision-safe host loopback listeners by default; physical LAN
  publication is a separate high-friction policy.

A `HostImport` never piggybacks on general Internet egress and never asks a host
service to bind a wildcard or LAN address. The host side stores the exact
loopback destination and validates the owning Environment, Machine, Network,
import ID, protocol, and port before dialing it. The guest receives only an
exact Machine/network-scoped relay address and guest-side port for the declared
import; a DNS alias is materialized only when that import declares one. No other
Machine or attachment receives the address or alias. A reverse-vsock or
equivalent authenticated Machine transport may implement this
contract, but the guest cannot choose an arbitrary host destination. There is
no unconditional `host.vz.internal`, shared NAT-gateway address, or host route
to a nested guest CIDR in the portable design.

External egress is a distinct Environment-owned firewall/NAT policy. It is
installed deny-first for exact owned source networks, blocks host, LAN,
link-local, multicast, control-plane, and sibling-Environment ranges unless a
separate capability allows them, and applies source NAT only after policy
acceptance. Domain allowlists use mediated DNS and expiring resolved-address
sets rather than an unenforced resolver configuration. Setup commits policy
before forwarding is enabled; rollback and deletion remove only exact
inventoried owner resources and restore forwarding only after the final
vz-owned policy is gone.

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
