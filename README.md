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
current **DEV** surface, not a completed or certified 0.4 distribution.

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

Status describes backend maturity, not complete product parity.

| Host | Linux target | Native macOS target | Native Windows target |
| --- | --- | --- | --- |
| macOS (Apple Silicon) | **ACTIVE** Virtualization.framework Linux VM/OCI/BuildKit primitives; unified lifecycle and private Docker are **DEV** | **ACTIVE** provisioning/automation primitives; unified Machine lifecycle is **DEV** | Not applicable |
| Linux | **DEV:** partial `linux-native` backend; complete Developer Environment parity remains in progress | Not applicable | Not applicable |
| Windows | **PLANNED:** Linux Machines using the selected Windows virtualization backend | Not applicable | **PLANNED later:** native Windows Machines |

Linux is the universal target; native targets complement it. Mixed Linux/macOS
Environment topology is part of the 0.4 goal, not a completed CLI capability.

## Current CLI: DEV, not the complete five-verb lifecycle

The implemented topology CLI currently exposes:

- `vz up`: streamed whole-Environment admission and retained Linux-on-macOS
  startup, using an explicitly configured verified artifact catalog. Developer
  boots retain private Engine endpoints but fail readiness until full host
  Docker/Compose/buildx and managed-context evidence exists; they are not Ready.
- `vz status`: read-only persisted Project/Environment/Machine state, definition
  drift, and recorded capabilities. It does not turn persisted data into a live
  health probe or infer Docker readiness from a Developer profile.
- `vz exec`: streamed execution in an already Ready, exactly owned Linux Machine
  through the Linux-on-macOS DEV adapter. Automatic startup/dependency
  reconciliation and native-target execution are still unfinished.
- `vz stop`: selected-Environment Stop with exact ownership and positive
  execution reaping. Unsupported resources and unknown live ownership fail
  closed; it is not the former single-VM Stop path.

`delete` is absent; complete Up reconciliation, installed-catalog bootstrap,
and physical lifecycle acceptance remain unfinished. There is no compatibility
alias to make the command count look complete. The release goal remains exactly
`up`, `exec`, `status`, `stop`, and `delete`.

Bare `vz` prints static help, exits zero, and performs no project/state discovery
or mutation. All 16 retired infrastructure roots—including `run`, `vm`,
`create`, `init`, `image`, `stack`, and hidden `debug`—and old bare-mode
mutation flags return structured `legacy_command_removed` errors before state
or transport access. No hidden old parser or fallback binary remains. See the
[removal inventory](planning/developer-environments/legacy-cli-removal.md) and
[machine-readable DEV inventory](config/cli-removal-v0.4.json).

### Work with an existing Developer Environment

These commands require a valid nearest `vz.json`, an already-running runtime
daemon, and an existing, exactly owned topology. Execution additionally requires
a Ready Linux Machine; this is **not** a clean-install bootstrap sequence.

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
The DEV Up path requires a daemon configured with a verified target catalog;
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
substitute. The complete network contract remains **DEV**; see
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
                                      ├─ native macOS integration (DEV)
                                      └─ later host/target backends
```

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
