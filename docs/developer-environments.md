# Developer Environments: Product Contract

Date: 2026-09-02
Status: committed direction; implementation status is tagged below
Release definition of done: [`../planning/developer-environments/GOAL-0.4.0.md`](../planning/developer-environments/GOAL-0.4.0.md)

This is the canonical vz product contract. When older documents use `sandbox`,
`container`, or `VM` as the top-level product object, interpret those as
implementation mechanisms unless the document explicitly describes a low-level
API or the currently shipped legacy CLI.

## Product definition

**vz creates reproducible, parallel Developer Environments on local hardware.**

A project defines a reproducible topology. That definition can be instantiated
as any number of independently named Developer Environments for worktrees,
agents, comparisons, tests, and releases. A Developer Environment is the stable
user-facing isolation, ownership, and lifecycle boundary. It contains one or
more target-native Machines plus the storage, networks, DNS, endpoints,
credentials, policies, faults, executions, and evidence that make those Machines
one reproducible system.

```text
ProjectDefinition
└── EnvironmentInstance[]
    ├── MachineInstance[]
    ├── Network[] and declared service paths
    ├── Endpoint[], HostImport[], HostExport[], and EgressPolicy[]
    │   └── environment-local public-like ingress
    └── Volume[], SecretBinding[], Fault[], Execution[], and Receipt[]
```

A worktree is a workspace binding and convenient default selector, not an
Environment identity and not a one-instance limit. One worktree may bind
several Environments; one Environment may contain several Machines. A sandbox,
VM, container, process boundary, or native OS facility implements a Machine or
a capability behind this contract and is not a competing product concept.

Target OS belongs to a Machine and is independent of the host OS. An Environment
may be heterogeneous. On macOS it may contain Linux Machines and native macOS
Machines that communicate through declared topology. Linux is the universal
Machine target across macOS, Linux, and Windows hosts. Native targets complement
Linux where the host permits them.

## Host and Machine-target matrix

Status labels describe backend availability, not complete feature parity:

- **ACTIVE**: working backend capabilities exist in the repository today.
- **DEV**: implementation exists or is being unified, but the 0.4 contract is
  not complete.
- **PLANNED**: committed direction with no complete implementation claim.
- **N/A**: not a supported host/Machine-target pairing.

| Host | Linux Machine target | macOS Machine target | Windows Machine target |
|---|---|---|---|
| macOS on Apple silicon | **ACTIVE** primitives; unified lifecycle, per-Machine Docker, and topology are **DEV** | **ACTIVE** VM flows; unified Machine lifecycle and mixed topology are **DEV** | N/A |
| Linux | **DEV** native backend; contract parity remains in progress | N/A | N/A |
| Windows | **PLANNED** using the selected Windows virtualization backend | N/A | **PLANNED**, after Linux-on-Windows |

Delivery order is Linux-on-macOS and macOS-on-macOS, Linux-on-Linux,
Linux-on-Windows, then Windows-on-Windows. Linux being universal does not require
the same backend: Virtualization.framework, Linux-native isolation, and future
Windows virtualization may implement the same observable Machine contract.

## Identity, selection, and ownership

Canonical identity has three independent levels:

```text
project_id / environment_id / machine_id
```

Each level has an immutable internal ID. Human names and worktree bindings are
selectors, not storage or ownership keys; configuration paths are discovery or
diagnostic data only.

Environment selection has strict precedence: an explicit Environment ID/name,
then the process-scoped `VZ_ENVIRONMENT_ID`, then the unambiguous binding for the
current workspace token. `VZ_ENVIRONMENT_ID` accepts an immutable Environment
ID only. A present explicit or process selector that is invalid or stale fails
at that level and never falls through to workspace selection. Within the
selected Environment, Machine selection uses an explicit Machine ID/name, then
the process-scoped `VZ_MACHINE_ID`, then the declared default or sole Machine.
`VZ_MACHINE_ID` accepts an immutable Machine ID only and is ownership-checked
against that Environment; a present invalid, stale, or foreign value fails
without falling through. Ambiguity fails closed and lists candidates. vz has no
mutable global current Environment.

The workspace binding key is a random opaque token persisted at
`<resolved-per-worktree-git-dir>/vz/workspace-id`. It survives moving the
worktree, while every new worktree or clone gets a new token. The raw checkout
path and an optional refreshable `path_hint` are non-authorizing diagnostics:
neither is identity, proof of a binding, nor a basis for selecting or adopting
an Environment.

For 0.4, the nearest checked-in `vz.json` is the versioned ProjectDefinition.
`vz up --environment <name>` creates or reconciles that project-unique named
instance and, on success, always creates or refreshes the current worktree
binding after ownership validation. Without a selector, the
sole instance already bound to this worktree is selected and multiple bound
instances are ambiguous. `default` is created only when the Project has no
Environment; a new unbound worktree never silently adopts or creates beside an
existing instance. Missing, invalid, or ambiguous definitions fail before
mutation. Project identity is stored in the definition and never derived from
the checkout path; authoring uses
`schemas/vz-project-definition-v1.schema.json`,
`examples/developer-environment/vz.json`, or the typed authoring API rather than
a second `vz init` lifecycle.

An Environment exclusively owns its Machines, disks, shares, credentials,
networks, DNS view, ingress, NAT state, ports, faults, events, and endpoints.
Every resource key includes `environment_id`; Machine-owned resources also
include `machine_id`. Repeated Machine names, service names, DNS aliases, guest
CIDRs, and internal ports are valid in other Environments. Stop preserves
identity and declared state. Delete traverses only the selected Environment's
ownership graph.

Workspace projection is explicit per Machine: read-write, read-only, or
snapshot. Shared-writer and shared-volume semantics require a declared
consistency contract. vz never silently multi-attaches a writable block disk.

## Machine contract

Every Machine has an immutable target specification containing OS,
architecture, image/version, and requested capabilities. It also has resources,
filesystem state, network attachments, lifecycle, negotiated capabilities, and
a replaceable incarnation. Rebuild may change the incarnation without changing
the logical Machine identity or declared endpoints.

Every new 0.4 MachineSpec explicitly selects `Developer` or `Hardened`.
Developer is the normal capability-rich profile. Hardened is a restricted Linux
Machine profile, does not inherit Docker, and rejects unsupported native-target
combinations. Legacy migration assigns the profile from provenance and never
silently converts a Hardened/generic record into Developer.

Every supported Machine provides target-native execution, streaming stdin and
stdout/stderr, PTY where supported, cancellation, exit status, inspectable
state, and lifecycle behavior. Unsupported host/Machine-target pairs or capabilities
fail explicitly and never substitute another Machine or target.

### Linux Machines and Docker

Docker compatibility is implicit for every Linux Developer Machine. Each Linux
Machine owns its own Docker Engine, containerd, BuildKit state, image and volume
stores, Docker networks, endpoint, credentials scope, and managed Docker
context. A multi-Machine Environment therefore has multiple independent Docker
engines and contexts; it never collapses them onto one shared daemon.

Host `docker`, `docker compose`, and `docker buildx` select an exact
`(environment_id, machine_id)` through both the configuration directory and
context returned by `vz status`: `docker --config <config_dir> --context <name>`.
Each Machine owns its client credentials as well as its Engine. A context name
alone does not select a credential scope. Ambient host credentials and native
keychain/helper defaults must not be inherited by another Machine.
vz never changes Docker's global default context and never falls back to Docker
Desktop, a system daemon, another Machine, or another Environment. There is no
global `~/.vz/docker.sock` and no Environment-wide `DOCKER_HOST` selector.
Transport paths are private backend details.

Linux Machines also own OCI execution and Linux checkpoint capabilities. On
macOS they execute inside vz-managed Linux VMs. The pinned, verified youki
binary is the only OCI runtime allowed in the guest; runc/crun installation,
override, or fallback fails the release gate. Full host-Docker compatibility is
**DEV** until the dedicated release-built local-Mac lane passes.

### Native macOS and Windows Machines

A native macOS Machine runs macOS workloads in a macOS VM and supports Xcode,
Swift, Darwin processes, launchd, APFS, and other target-native behavior. It
does not advertise Docker or silently create a Linux sidecar; a Linux Machine is
declared in the same or another Environment when Linux containers are required.
Existing macOS VM primitives are **ACTIVE**; their integration into the shared
topology contract is **DEV**.

The 0.4 native macOS release gate requires at least one exact macOS 26+ guest
version/build with an obtainable exact published base image, an authenticated
published matching bootstrap patch, and installed first-use evidence. The product
downloads and prepares this pair automatically with streamed progress, caching an
immutable prepared template per pinned release for subsequent Machine creation.
A latest-supported pointer resolves to a pinned compatible pair; existing
Environments retain that resolution until an explicit update. IPSW installation
is a maintainer preparation step. First-use preparation must apply the patch
and Machine creation must start the guest agent without manual
host sudo, disk mounting, ownership repair, or agent injection. Patcher code or
a manually prepared VM alone does not establish this supported path.

Linux-on-Windows precedes Windows-on-Windows. Native Windows Machines will expose
Windows process, service, console, NTFS, and isolation capabilities without
inheriting Linux OCI/youki assumptions. Both Windows pairings are **PLANNED**.

## Network topology contract

Every Environment owns a distinct route domain, DNS view, gateway/NAT state,
firewall, port registry, ingress, impairment state, and network credentials.
Project membership never grants connectivity and overlapping guest CIDRs are
allowed because route domains do not merge.

There is no implicit trusted flat LAN. Machines attach to named networks and
communicate only through declared service paths:

- `private` paths provide Environment-local connectivity and DNS;
- a simulated-public edge forces traffic through routed ingress, split DNS,
  firewall/NAT, and optional TLS using synthetic `.test` names while remaining
  local and isolated;
- real Internet egress is separately controlled as offline, allowed, or
  domain/CIDR allowlisted;
- host imports and exports are separate explicit capabilities; exports default
  to collision-safe loopback listeners and never expose the LAN by accident.

A host import authorizes one Environment/Machine to reach one stored host
loopback protocol/port through an authenticated private relay. It does not use
general Internet egress, require a wildcard/LAN host listener, expose arbitrary
host destinations, or grant another Machine access. Compatibility DNS names
such as `host.docker.internal` or `host.vz.internal` exist only where an import
is declared and resolve to an Environment-local relay—not an unconditional
shared Apple NAT gateway address.

External egress is independently deny-first and audited. Enabling it does not
authorize host imports, LAN access, control-plane access, or cross-Environment
traffic. Offline Machines may still use their exact declared imports. Domain
allowlists require mediated DNS and expiring resolved-address policy; a static
resolver or `/etc/hosts` entry is not enforcement.

Deterministic latency, jitter, loss, bandwidth, reset, DNS failure, and
partition controls are scoped to a declared path, seeded, bounded by TTL, and
produce receipts. Runtime faults expire rather than stranding connectivity.

Separate Environments cannot resolve, route to, inspect, or control one another.
When independently managed Environments must interact, a directional,
service-scoped, least-privilege, expiring peer grant may expose one declared
endpoint. It never merges L2/L3 networks, becomes transitive, or grants access
to storage, credentials, Docker, private DNS, or the control plane. Systems that
share lifecycle should normally be Machines in one Environment instead.

## Public UX and API contract

The 0.4 Developer Environment CLI has five top-level lifecycle verbs:

```text
vz up [--environment <name-or-id>]
vz exec [--environment <name-or-id>] [--machine <name-or-id>] -- <command>
vz status [--environment <name-or-id> | --all] [--machine <name-or-id>] [--json]
vz stop [--environment <name-or-id>]
vz delete [--environment <name-or-id>]
```

`up`, `stop`, and `delete` operate on the complete topology. `exec` targets the
declared default/only Machine or requires `--machine`; it can reconcile that
Machine and its dependencies. `status` reports topology, identities, targets,
capabilities, health, endpoints, and a Docker context for each Developer-profile
Linux Machine; Hardened Machines omit Docker contexts. Bare `vz` prints static
top-level help, exits zero, and does not inspect or create resources.

There is no canonical `vz dev` namespace and no public or hidden `run`, `shell`,
`list`, `logs`, `restart`, `docker`, `stack`, `network`, `machine`, or `vm`
compatibility family in 0.4. Advanced lifecycle, topology, files, logs,
snapshots, faults, and peering are typed API resources; native Docker clients use
the Docker API. Migration guidance may explain replacements without preserving
old execution paths.

The root Environment API owns topology create/reconcile/get/list/watch/start/
stop/delete. Child Machine APIs expose get/list/watch/lifecycle/exec and
capability discovery. Network, Endpoint, Volume, SecretBinding, Fault,
Execution, and Receipt resources are explicitly scoped. Interactive and
long-running operations stream progress and terminal results; unary APIs are
limited to short, bounded operations.

## Product boundary and profiles

vz is local-first. Hosted placement may reuse the contract later but is not
required for 0.4. The primary value is reproducibility and high-concurrency
agentic development, not lockdown alone.

The locked-down Hardened profile, temporarily represented by the legacy
`Container` name on disk during migration, remains a specialized Linux policy.
It must not constrain Developer Machines from using cgroups, networking, Docker,
or other required development capabilities.

## Completion and documentation rule

The product is not complete based on unit tests or a one-Machine demonstration.
The normative pass/fail scenarios, required local-Mac lanes, prohibited
shortcuts, evidence schema, and terminal definition of done are in
[`GOAL-0.4.0.md`](../planning/developer-environments/GOAL-0.4.0.md). Missing,
skipped, flaky, or malformed required evidence leaves the release open.

Product, planning, CLI, API, site, skill, and architecture documents use
**Developer Environment** for the topology instance and **Machine** for a
target-native compute member. Use `sandbox`, `container`, `VM`, and `process`
only for a current compatibility command, protocol entity, security boundary,
or backend. Claims carry **ACTIVE**, **DEV**, or **PLANNED** whenever direction
could be mistaken for shipped behavior.
