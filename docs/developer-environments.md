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

The machine-readable matrix
[`config/host-target-capabilities-v0.4.json`](../config/host-target-capabilities-v0.4.json)
is the source of truth for every host×Machine-target×profile capability label.
Its `status_definitions` are the only status vocabulary; every surface that
defines the vocabulary repeats them verbatim, and
`python3 -B scripts/check-capability-claims.py` verifies that generated
surfaces match the matrix (see [capability claim markers](#capability-claim-markers)).

<!-- capability-matrix: definitions -->
- **ACTIVE**: Shipped in the published target release AND retained installed
  evidence exists for the exact host×target×profile pair. Requires non-empty
  evidence.
- **DEV**: Implemented and demonstrated by an installed local-Mac slice that is
  not release certified. Requires non-empty evidence.
- **PLANNED**: Committed direction with no negotiation path. Requires empty
  `negotiated_by`, `rejected_by` and evidence.
- **NA**: Explicitly rejected by validation as an unsupported pairing or
  declaration. Requires non-empty `rejected_by`.

No entry is labelled shipped until a 0.4 release is published; capabilities the
matrix does not list are <!-- capability-matrix: vocabulary -->PLANNED by definition. Labels describe the matrix
entry, not complete feature parity.

| Host | Linux Machine target | macOS Machine target | Windows Machine target |
|---|---|---|---|
| macOS on Apple silicon | <!-- capability-matrix: macos-arm64/linux/* pair -->**DEV** lifecycle, exec and private Developer-profile Docker; topology networking and workspace projections are <!-- capability-matrix: macos-arm64/linux/* network_private,workspace_read_write -->**PLANNED** | <!-- capability-matrix: macos-arm64/macos/developer pair -->**DEV** Developer Machines with exec and PTY; the Hardened profile is <!-- capability-matrix: macos-arm64/macos/hardened pair -->**NA** | <!-- capability-matrix: macos-arm64/windows/* pair -->**NA** |
| Linux | <!-- capability-matrix: linux-*/linux/* pair -->**PLANNED**; the partial `linux-native` backend in the tree has no Machine target resolver on a Linux host | <!-- capability-matrix: linux-*/macos/* pair -->**NA** | <!-- capability-matrix: linux-*/windows/* pair -->**NA** |
| Windows | <!-- capability-matrix: windows-*/linux/* pair -->**PLANNED** using the selected Windows virtualization backend | <!-- capability-matrix: windows-*/macos/* pair -->**NA** | <!-- capability-matrix: windows-*/windows/developer pair -->**PLANNED** Developer Machines, after Linux-on-Windows; Hardened is <!-- capability-matrix: windows-*/windows/hardened pair -->**NA** |

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
a second `init` lifecycle.

An Environment exclusively owns its Machines, disks, shares, credentials,
networks, DNS view, ingress, NAT state, ports, faults, events, and endpoints.
Every resource key includes `environment_id`; Machine-owned resources also
include `machine_id`. Repeated Machine names, service names, DNS aliases, guest
CIDRs, and internal ports are valid in other Environments. Stop preserves
identity and declared state. Delete traverses only the selected Environment's
ownership graph.

Workspace projection is explicit per Machine: read-write, read-only, or
snapshot. A Machine declares `source_path` relative to the worktree root, and
the runtime joins it to the authoritative root, canonicalises the result, and
refuses anything that leaves the root.

Shared-writer and shared-volume semantics require a declared consistency
contract. vz never silently multi-attaches a writable disk: within one
Environment, a host source that any Machine projects writable is projected into
no other Machine, and Up refuses such a declaration at admission, before it
allocates identities or reserves a workspace binding. Two read-only projections
of one source are allowed, because there is no writer to serialise.

Environment-owned storage is declared as a `Volume`, which is separate from a
workspace projection: a projection shows a Machine part of the user's worktree,
while a volume is storage the Environment itself owns and no host path outside
it backs. A volume declares one of two kinds, and the kind decides the
multi-attach rule.

A `block` volume is one sparse disk image carrying one ext4 filesystem,
attached as a virtio-block device and mounted at its declared path. It declares
`size_bytes`. Because ext4 is not a cluster filesystem, a writable block volume
attached to more than one Machine is refused at admission, before any identity
is reserved and before any image is allocated; read-only multi-attach is
allowed, because no writer exists to serialise.

A `shared_cache` volume is one host directory exported to every attached
Machine over its own VirtioFS device, and multi-attach is its purpose rather
than its hazard: there is no shared block layer to corrupt and the host
filesystem serialises the writes. What the carrier does not provide for free is
*when* one Machine observes another's write, so a shared cache must declare its
`consistency` explicitly. The only model is `bounded_staleness` with a
`staleness_bound_millis`: a write closed on one attached Machine becomes visible
to every other attached Machine within that bound, and nothing is promised
before it, because each Machine runs its own virtio-fs attribute and dentry
cache over the one host directory.

A volume is Environment-scoped rather than Machine-scoped, so its ownership
record carries no `machine_id`: a shared cache spans several Machines and a
block volume outlives the incarnation that mounted it. Stop preserves a volume,
and Delete reclaims its storage as part of the selected Environment's ownership
graph.

Every workspace projection is
<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer workspace_read_write,workspace_read_only,workspace_snapshot -->**PLANNED**;
promotion waits on gate evidence, not on the adapter. Up applies all three
modes on Developer Linux Machines. A `snapshot` is a private copy-on-write clone
of the declared source, made with `clonefile` and shared writable: the Machine
may write into its own copy and nothing it writes reaches the worktree, and the
copy is remade on every Up so the tree is the source as it was at that boot. It
lives inside that Machine's own runtime store, which is already an accounted
owned resource, so Delete reclaims it without a resource kind of its own. A
projection declared on a Hardened or non-Linux Machine is still rejected, because
neither carries a VirtioFS share to serve it. Declared volumes follow the same
rule and for the same reason.

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
stdout/stderr, PTY where negotiated, cancellation, exit status, inspectable
state, and lifecycle behavior. Today `posix_pty` is negotiated only by native
macOS Machines
(<!-- capability-matrix: macos-arm64/macos/developer posix_pty -->**DEV**);
Linux Machines negotiate `posix_exec` without a PTY, so `vz exec -t` against a
Linux Machine is
<!-- capability-matrix: macos-arm64/linux/* posix_pty -->**PLANNED**.
Unsupported host/Machine-target pairs or capabilities
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
<!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx,docker_context -->**DEV**
until the dedicated release-built local-Mac lane passes.

### Native macOS and Windows Machines

A native macOS Machine runs macOS workloads in a macOS VM and supports Xcode,
Swift, Darwin processes, launchd, APFS, and other target-native behavior. It
does not advertise Docker or silently create a Linux sidecar; a Linux Machine is
declared in the same or another Environment when Linux containers are required.
Native macOS Developer Machines are
<!-- capability-matrix: macos-arm64/macos/developer pair -->**DEV**: the
installed local bundle passes Up/exec/PTY/Stop/Delete, but no published
authenticated base/patch/catalog exists. The Hardened profile on a native macOS
Machine is <!-- capability-matrix: macos-arm64/macos/hardened pair -->**NA**.

The 0.4 native macOS release gate requires at least one exact macOS 26+ guest
version/build prepared locally from pinned Apple IPSW bytes. The explicit host
setup operation installs macOS, requests administrator authorization to provision
the new guest disk, and validates an immutable local template. By default it
prepares macOS without Xcode or Command Line Tools. Opting in with `--xcode`
installs the selected local application and validates native Swift build/test/run.
The native release gate covers both clean and Xcode-equipped Machines; Xcode is
not required for users who want a clean OS for testing. The `clean` and `xcode`
catalog channels select separate templates when both are installed.
Setup is once per host and recipe/toolchain pin. Ordinary Machine creation uses
private APFS clones with fresh platform identities; Up/exec/Stop/Delete require
no host sudo or manual guest repair. Repeating a completed setup reuses its
validated template. Existing Environments retain their original pins.

`vz-macos-setup` is an installation utility, separate from the five Environment
lifecycle verbs. It reports preparation progress and accepts the selected Xcode
license only with explicit operator authorization. This local setup path is
<!-- capability-matrix: macos-arm64/macos/developer pair -->**DEV** until its
fresh installed-user gate passes. Public redistribution of
macOS/Xcode disk images is not a 0.4 dependency. The `vz-macos-provision::image_delta`
API and version-1 exact-base/patch format remain available for optional artifact
workflows; local setup uses a version-2 complete local-image manifest and never
creates or applies a block patch.

Linux-on-Windows precedes Windows-on-Windows. Native Windows Machines will expose
Windows process, service, console, NTFS, and isolation capabilities without
inheriting Linux OCI/youki assumptions. Both Windows pairings are
<!-- capability-matrix: windows-*/linux/*,windows-*/windows/developer pair -->**PLANNED**.

## Network topology contract

Every topology capability in this section—private networks, the simulated-public
edge, endpoints, split DNS, TLS ingress, NAT/firewall, host imports and exports,
egress policy—is
<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer network_private,network_simulated_public,endpoint,split_dns,tls_ingress,nat_firewall,host_import,host_export,egress_policy -->**PLANNED**
for every pair in the matrix. Up currently rejects declared networks and
endpoints without admission; the typed contract below is the committed
direction, not shipped behavior.

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

Separate Environments cannot resolve, route to, inspect, or control one another,
and 0.4 offers no way to change that: there is no peer grant, no exception, and
no escape hatch. Systems that must interact are Machines in one Environment,
which is what its declared networks and endpoints are for. Deferring the
cross-Environment case keeps the isolation claim absolute rather than
conditional, which is the property parallel Environments are worth having for.

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

Every field of `status` but one is a projection of persisted state. The
exception is each Machine's `health`, which the answering daemon recomputes for
every reply from its own supervision registry: `supervised` when it still holds
the live session it registered when it booted that Machine and that session
names the persisted runtime identity, `unsupervised` when the record is Ready
but this daemon holds no session for it, `diverged` when a session exists but
disagrees with the record, `inactive` when no live supervision is expected and
none is claimed — no session under a record that does not claim Ready, or the
spent session a positive Stop leaves behind — and `unobservable` when the
daemon could not read its own registry. Health reaches
into no guest: it is not a ping, a guest-agent round trip, a Docker Engine
probe, or a service check, so a `supervised` Machine is one the daemon is still
running rather than one whose workload is known to be serving. It is never
persisted, and `docker_context_availability` is likewise a persisted
lifecycle/capability projection and never a live Engine probe.

Routine `status` reports the Environment's shape — its networks, which Machines
hold a port on them, and the declared endpoints — because those are what the
definition asked for and what makes the Environment legible. It does not report
workspace bindings, the internal owned-resource graph, host imports and exports,
or egress policy.

There is no canonical `vz dev` namespace and no public or hidden `run`, `shell`,
`list`, `logs`, `restart`, `docker`, `stack`, `network`, `machine`, or `vm`
compatibility family in 0.4. Advanced lifecycle, topology, files, logs,
snapshots and receipts are typed API resources; native Docker clients use
the Docker API. Migration guidance may explain replacements without preserving
old execution paths.

The release ships `vz-runtime-probe`, a typed client for that channel. It is not
a second CLI and adds no lifecycle verb: it connects to an existing daemon,
never spawns one, and prints the daemon's own aggregate and Up event stream as
JSON. Its purpose is that CLI/API agreement can be observed rather than
asserted -- comparing `vz status --json` against the CLI's own state store
would show only that the CLI is self-consistent.

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
or backend. Claims carry **ACTIVE**, **DEV**, **PLANNED**, or **NA** whenever
direction could be mistaken for shipped behavior, and every such claim on a
generated surface is bound to the matrix as described below.

### Capability claim markers

`generated_surfaces` in
[`config/host-target-capabilities-v0.4.json`](../config/host-target-capabilities-v0.4.json)
lists the public surfaces (README, docs, site, skill, CLI help source) that
`python3 -B scripts/check-capability-claims.py` lints. The linter treats every
occurrence of a status token in those files as a claim and requires an explicit
marker to bind it to matrix entries; nothing is inferred from table headers or
prose. A token with no marker is a violation unless it is a vocabulary mention
(two or more distinct tokens joined only by commas, slashes, `or`, or `and`) or
sits inside a definitions block.

Marker forms share one grammar:

```text
<!-- capability-matrix: <selectors> <capabilities> -->   markdown and HTML
vz-capability="<selectors> <capabilities>"               HTML attribute
// capability-matrix: <selectors> <capabilities>         Rust help source
```

`<selectors>` is a comma-separated list of `host/target/profile` pair ids
(segments may be globs, such as `macos-arm64/linux/*`; profile aliases such as
`container` and backend aliases such as `macos-vz` from the matrix
`vocabularies` are accepted), `host:<host-glob>` items (host status, no
capability), or `backend:<wire-name-or-alias>` items (every pair on that
backend). `<capabilities>` is a comma-separated list of `pair` (the pair
status), a machine capability, or a topology capability (optionally prefixed
`topology:`). The claim must equal the matrix status of every selected entry.

A marker binds the next status token after it on its own line, or on the next
line with non-marker content when the marker line has none; several marker-only
lines stack and bind that content line's tokens in order. Put markers inline
inside table cells and list items (directly before the bold label) and on their
own line before headings and paragraphs. A marker followed by a blank line or by
a line without a claim is a violation. In markdown, inline code spans are
literal text (neither markers nor claims) and fenced blocks cannot carry
markers, so keep status claims outside fenced blocks. A definitions block ends
at a blank line or a closing list tag. Two special markers exist:
`<!-- capability-matrix: vocabulary -->` exempts the tokens in its window (a
legend or vocabulary list), and `<!-- capability-matrix: definitions -->`
starts a definitions block whose items must repeat `status_definitions`
verbatim, one status per item, all four present. The linter also rejects any
removed CLI root from `config/cli-removal-v0.4.json` presented as a command
(`vz <root>` in a code span, fenced block, `<code>`/`<pre>` element, or Rust
help text). The examples above are bound like any other claim:
<!-- capability-matrix: macos-arm64/linux/developer posix_exec -->**DEV** for
Linux Developer exec on Apple silicon and
<!-- capability-matrix: linux-*/linux/* pair -->**PLANNED** for Linux hosts.
