# vz

Reproducible Developer Environment topologies, with Linux as the universal
Machine target.

A **Developer Environment** is the primary `vz` product object: a named,
isolated realization of a project topology containing one or more target-native
Machines plus their storage, networks, DNS, endpoints, credentials, lifecycle,
and workload state. It is not a synonym for one VM or container. A project and
even one worktree may have several Environment instances; a single Environment
may contain Linux and native macOS Machines.

The target model is deliberately asymmetric:

- **Linux is universal.** Linux Machines run on macOS today and are planned for
  Linux and Windows hosts.
- **Native targets follow the host.** Native macOS Machines run on macOS;
  native Windows Machines are planned for Windows.
- **Docker belongs to each Developer-profile Linux Machine.** The complete Docker-compatible
  workflow is in progress. Every Developer-profile Linux Machine will implicitly own its private
  Docker Engine, containerd, BuildKit cache, image and volume state, networks,
  endpoint, and Docker context. There is no Environment-global or global `vz`
  Docker daemon and no Docker capability implied for native macOS/Windows
  Machines.
- **Networking can be realistic without being public.** Machines use declared
  private paths or an Environment-local simulated-public DNS/TLS/ingress/NAT
  edge. Separate Environments are default-deny.
- **Hardened Machines are secondary.** The constrained `container` kernel
  profile remains available for locked-down workloads, but it is not a peer
  product or the default Developer Environment.

Today, `vz` ships Linux VM/OCI/BuildKit primitives and native macOS VM automation
on Apple Silicon. Their convergence into the complete Developer Environment
contract—including topology and private per-Developer-Linux-Machine Docker—is in
development. Additional
host/Machine-target combinations below are roadmap work, not shipped functionality.

## Why vz

- **Environment-first.** A project definition creates isolated reproducible
  topology instances, each with one or more Machines.
- **Linux everywhere.** Keep the Linux target consistent while choosing the
  best isolation backend for macOS, Linux, or Windows.
- **Native where it matters.** Use native macOS environments on Apple hardware;
  native Windows is a later target for Windows hosts.
- **Private by construction.** Linux Docker state is scoped to one Machine;
  Environment routing, DNS, storage, credentials, and endpoints cannot leak into
  another instance.
- **Script-friendly.** Consistent command flows and `--json` output support
  automation.

## Install

```bash
curl -sSf https://raw.githubusercontent.com/gpu-cli/vz/main/scripts/install.sh | sh
```

The installer selects published binaries and Linux artifacts for Apple-silicon
macOS. A published release can have a different CLI from this development
checkout; always inspect the installed `vz --help`. This README describes the
current <!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV** surface, not a
completed or certified 0.4 distribution.

Options:

- `VZ_VERSION=<published-version>` — select a specific release.
- `VZ_NO_LINUX=1` — skip Linux kernel download.

### Build from source

From the checkout, use the workspace's Rust toolchain and lockfile:

```bash
cargo build --manifest-path crates/Cargo.toml --locked --release \
  -p vz-cli -p vz-runtimed
```

Source builds are development artifacts. Virtualization.framework tests require
signed binaries with the appropriate entitlements; the supported sandbox harness
below builds and signs its own test drivers. The old `self-sign` CLI command is
retired, not an installation prerequisite.

Build the primary Developer and secondary Hardened Linux bundles using the
explicit local Docker context and case-sensitive source pipeline documented in
[linux/README.md](linux/README.md). Docker is build infrastructure here, not a
substitute runtime or Docker compatibility evidence. Both profiles use the
pinned youki runtime, with verified source/artifact provenance.

## Host and Machine-target roadmap

Status labels use the vocabulary of the machine-readable capability matrix,
[`config/host-target-capabilities-v0.4.json`](config/host-target-capabilities-v0.4.json).
`python3 -B scripts/check-capability-claims.py` checks every label in this file
against that matrix; see the
[marker convention](docs/developer-environments.md#capability-claim-markers).

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

No pair carries the shipped-release label until a 0.4 release is published.
Labels describe the matrix entry, not complete product parity.

| Host | Linux target | Native macOS target | Native Windows target |
| --- | --- | --- | --- |
| macOS (Apple Silicon) | <!-- capability-matrix: macos-arm64/linux/* pair -->**DEV** lifecycle, exec and private Developer-profile Docker; topology networking is <!-- capability-matrix: macos-arm64/linux/* network_private -->**PLANNED** | <!-- capability-matrix: macos-arm64/macos/developer pair -->**DEV** Developer Machines; the Hardened profile is <!-- capability-matrix: macos-arm64/macos/hardened pair -->**NA** | <!-- capability-matrix: macos-arm64/windows/* pair -->**NA** |
| Linux | <!-- capability-matrix: linux-*/linux/* pair -->**PLANNED**; a partial `linux-native` backend exists in the tree, but no Machine target resolves on a Linux host | <!-- capability-matrix: linux-*/macos/* pair -->**NA** | <!-- capability-matrix: linux-*/windows/* pair -->**NA** |
| Windows | <!-- capability-matrix: windows-*/linux/* pair -->**PLANNED** Linux Machines using the selected Windows virtualization backend | <!-- capability-matrix: windows-*/macos/* pair -->**NA** | <!-- capability-matrix: windows-*/windows/developer pair -->**PLANNED** native Windows Developer Machines, after Linux-on-Windows |

Linux is the universal target; native targets complement it. Mixed Linux/macOS
Environment topology is part of the 0.4 goal, not a completed CLI capability.

<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->
## Current CLI: the five-verb lifecycle is DEV

The implemented topology CLI currently exposes:

- `vz up`: streamed whole-Environment admission and retained startup for Linux
  and native macOS Machines on Apple silicon, discovering a verified installed
  artifact catalog or using an exact operator override. Linux Developer
  readiness requires measured host Engine, Compose and buildx operations through
  each Machine's managed context and digest-bound offline probe. This
  <!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx -->**DEV**
  path does not certify full Docker compatibility; see
  [startup readiness](docs/developer-startup-readiness.md).
- `vz status`: read-only persisted Project/Environment/Machine state, definition
  drift, recorded capabilities and Machine-specific Docker contexts. It does not
  turn persisted data into a live health probe or infer Docker readiness from a
  Developer profile.
- `vz exec`: streamed execution in an already Ready, exactly owned Linux or
  native macOS Machine on Apple silicon
  (<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer posix_exec -->**DEV**).
  An interactive terminal (`-t`) is negotiated only by native macOS Machines
  (<!-- capability-matrix: macos-arm64/macos/developer posix_pty -->**DEV**);
  Linux Machine PTY is
  <!-- capability-matrix: macos-arm64/linux/* posix_pty -->**PLANNED**.
  Automatic startup/dependency reconciliation is still unfinished.
- `vz stop`: selected-Environment Stop with exact ownership and positive
  execution reaping. Unsupported resources and unknown live ownership fail
  closed; it is not the former single-VM Stop path.
- `vz delete`: streamed ownership-safe removal of the selected Environment's
  Linux and native macOS Machines on Apple silicon, managed Docker contexts and
  private stores
  (<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV**).
  Positive quiescence and exact ownership are required; other topology cleanup
  adapters remain unsupported.

All five lifecycle verbs are present as
<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV** adapters
with installed local-Mac evidence that is not release certified. Complete Up
reconciliation, mixed-target topology and the full physical lifecycle release
gate remain open.

Bare `vz` prints static help, exits zero, and performs no project/state discovery
or mutation. All 16 retired infrastructure roots—including `run`, `vm`,
`create`, `init`, `image`, `stack`, and hidden `debug`—and old bare-mode
mutation flags return structured `legacy_command_removed` errors before state
or transport access. No hidden old parser or fallback binary remains. See the
[removal inventory](planning/developer-environments/legacy-cli-removal.md) and
[machine-readable removal inventory](config/cli-removal-v0.4.json).

### Work with an existing Developer Environment

These commands require a valid nearest `vz.json`, an already-running runtime
daemon, and an existing, exactly owned topology. Execution additionally requires
a Ready Machine; this is **not** a clean-install bootstrap sequence.

```bash
vz status --environment dev --json
vz exec --environment dev --machine app -- uname -s
vz stop --environment dev
```

Author definitions using the repository's
[ProjectDefinition schema](schemas/vz-project-definition-v1.schema.json) and
[example](examples/developer-environment/vz.json), or the typed authoring API.
The example does not invent a downloadable appliance digest. Definition
authoring does not create a Machine, and there is no mutating `init` fallback.
The <!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV** Up path
requires a daemon configured with a verified target catalog;
automatic installed-catalog discovery and the complete bootstrap gate remain work.

Host Docker, Compose, and buildx must select the exact Developer Linux Machine's
context when its capability is actually available. Never assume a global
`~/.vz/docker.sock`, Environment-wide daemon, or Docker Desktop fallback.
The [Docker compatibility contract](docs/docker-compatibility-contract.md)
remains unverified until its full installed-host-client lane passes.

### Host services and isolation

An explicit Environment/Machine-owned import authorizes one host-loopback
protocol/port through a private authenticated relay. An undeclared Machine must
not receive `host.vz.internal` or another implicit host gateway. External
egress does not authorize host access, and wildcard/LAN binding is not a
substitute. The network contract (private networks, the simulated-public edge,
host imports/exports, egress policy, faults and peering) is
<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer network_private,network_simulated_public,endpoint,split_dns,tls_ingress,nat_firewall,host_import,host_export,egress_policy,faults,peering -->**PLANNED**
for every pair; Up currently rejects declared networks and endpoints. See
[Developer Environments](docs/developer-environments.md).

## Runtime daemon connectivity

The current lifecycle adapters use the typed runtime daemon over gRPC/UDS.
They connect only to an already-running daemon and do not autostart one.

- Default socket: `<state-db-parent>/.vz-runtime/runtimed.sock`.
- Explicit paths: `VZ_RUNTIME_STATE_DB`, `VZ_RUNTIME_DATA_DIR`, and
  `VZ_RUNTIME_DAEMON_SOCKET`.
- `VZ_CONTROL_PLANE_TRANSPORT=daemon-grpc` is the supported CLI transport.
  `api-http` fails closed for these adapters; it does not trigger a compatibility
  connector or daemon fallback.
- Richer topology, artifact, snapshot, file, and diagnostic operations belong to
  typed APIs, not retired CLI command families.

## Architecture

```text
vz lifecycle CLI → typed daemon API → Environment/Machine ownership
                                      ├─ Linux-on-macOS backend
                                      ├─ native macOS backend
                                      └─ later host/target backends
```

Both Apple-silicon backends are
<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV**.
Identity, authorization, execution, lifecycle, and evidence stay scoped to the
selected Environment and Machine. A backend primitive is not a second public
product lifecycle.

## Development and verification

```bash
cargo build --manifest-path crates/Cargo.toml --workspace
cargo clippy --manifest-path crates/Cargo.toml --workspace -- -D warnings
cargo nextest run --manifest-path crates/Cargo.toml --workspace
```

The supported local Apple-silicon sandbox backend gate uses signed test drivers,
not retired public VM commands:

```bash
./scripts/run-sandbox-vm-e2e.sh --profile release --suite all
```

See the [sandbox harness guide](docs/sandbox-vm-e2e.md) for prerequisites,
case-sensitive guest builds, selected scenarios, and retained artifacts.
The old Linux-VM/hostboot/daemon-release helper workflows are
[retired](docs/retired-cli-workflows.md); they cannot be revived by pointing
`VZ_BIN` at an older executable.

A passing sandbox backend gate does **not** certify the complete five-verb
lifecycle, host-Docker compatibility, native macOS Machines, mixed topologies,
migration, or the aggregate 0.4 release. Those remain separate requirements in
[GOAL-0.4.0](planning/developer-environments/GOAL-0.4.0.md) and
[backend verification](docs/agent-verification.md).

Additional references:

- [Runtime primitive conformance matrix](docs/runtime-primitive-conformance.md)
- [Daemon-only guardrails and fail-closed policy](docs/daemon-only-guardrails.md)

## License

[MIT](LICENSE.md)
