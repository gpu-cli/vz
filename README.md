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
- **Hardened environments are secondary.** The constrained `container` kernel
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

This installs pre-built binaries (signed + notarized) and the Linux kernel to `~/.vz/bin/`.
Requires macOS on Apple Silicon.

Options:
- `VZ_VERSION=0.3.0` — pin a specific version
- `VZ_NO_LINUX=1` — skip Linux kernel download

### Install from source

```bash
# Requires Rust 1.85+
cargo install --git https://github.com/gpu-cli/vz.git vz-cli
vz self-sign  # apply Virtualization.framework entitlements
```

### Build the Linux kernel (for source installs)

```bash
cd linux && make docker-build  # requires Docker
mkdir -p ~/.vz/linux && cp linux/out/{vmlinux,initramfs.img,youki,version.json} ~/.vz/linux/
```

The default kernel profile is `developer` and keeps nested virtualization for
Virgil-style Firecracker host VMs. To build the constrained container sandbox
bundle, use:

```bash
cd linux && make docker-build KERNEL_PROFILE=container
```

Release CI caches the developer/container kernel images by kernel inputs, then
rebuilds the initramfs and metadata for each `vz` release.

## Host and Machine-target roadmap

Status describes backend maturity, not complete product parity.

| Host | Linux target | Native macOS target | Native Windows target |
| --- | --- | --- | --- |
| macOS (Apple Silicon) | **ACTIVE** primitives: Virtualization.framework Linux VM, OCI, and BuildKit; unified lifecycle and private Docker are **DEV** | **ACTIVE** `vz vm ...` provisioning and automation; unified lifecycle is **DEV** | Not applicable |
| Linux | **DEV:** partial `linux-native` backend; complete Developer Environment parity remains in progress | Not applicable | Not applicable |
| Windows | **PLANNED:** Linux Developer Environments using the appropriate Windows virtualization backend | Not applicable | **PLANNED later:** native Windows Developer Environments |

Linux is the universal Machine target across all three hosts. Native macOS and
native Windows Machines complement it; they do not replace it. On macOS, Linux
and native macOS Machines may participate in the same declared Environment
topology.

## Quick start (current legacy macOS/Linux-target workflow)

The commands below document the shipped 0.3 single-Linux-VM surface. They are
being replaced for 0.4 by `vz up`, `vz exec`, `vz status`, `vz stop`, and
`vz delete`; they do not define the future product object model.

### 1. Run commands in a Linux VM

```bash
cd your-project

# Generate a vz.json config (auto-detects Rust, Node, Python, Go)
vz init

# Run any command inside the Linux VM
vz run echo "hello from Linux"

# Compile and run a Rust project
vz run cargo build
vz run cargo test

# Open an interactive shell
vz run -i bash

# Check VM status
vz status

# Stop the VM when done
vz stop
```

The first `vz run` boots the environment's Linux VM (~3s), pulls the base
image, and runs setup commands from `vz.json`. Subsequent runs reuse that
environment and skip setup when the setup hash is unchanged.

The intended Developer Environment UX will also start each declared Linux
Machine's private Docker service and report its managed context. Until the Docker roadmap is
complete, do not assume that host `docker`, Compose, or buildx commands have
full compatibility merely because the OCI workflows below are available.

#### vz.json

```json
{
  "image": "ubuntu:24.04",
  "workspace": "/workspace",
  "mounts": [{ "source": ".", "target": "/workspace" }],
  "setup": [
    "apt-get update",
    "apt-get install -y build-essential curl"
  ],
  "env": { "PATH": "/root/.cargo/bin:/usr/local/bin:/usr/bin:/bin" },
  "resources": { "cpus": 4, "memory": "8G" }
}
```

### 2. Run a current vz-managed Compose stack

```bash
# Start services
vz stack up -f compose.yaml -n demo

# Inspect and stream logs
vz stack ps demo
vz stack logs demo --service web --follow

# Tear down
vz stack down demo --volumes
```

Stack networking defaults to service identity inside the stack network.
Host-facing port publishing is explicit opt-in via Compose host bindings
(`HOST:CONTAINER`); container-only ports remain internal.

#### Reaching macOS host services from inside a container

Developer Environments do not expose the macOS host through an unconditional
gateway alias. In particular, an undeclared Machine must not receive
`host.vz.internal` in `/etc/hosts`, and external egress does not authorize host
access.

The 0.4 design requires an explicit Environment/Machine-owned host import for
one host-loopback protocol and port, carried over a private authenticated relay.
That relay is still under development; do not bind a host service to a wildcard
or LAN address as a substitute. See
[`docs/developer-environments.md`](docs/developer-environments.md) for the
normative network contract.

### 3. Manage macOS VMs (macOS only)

```bash
# Create a pinned base image from the stable channel
vz vm init --base stable

# Provision account + guest agent after fingerprint verification (system mode is default)
sudo vz vm provision --image ~/.vz/images/base.img --base-id stable

# No-local-sudo local path (opt-in runtime policy)
vz vm provision --image ~/.vz/images/base.img --base-id stable --agent-mode user

# Verify a local image against the stable channel pin
vz vm base verify --image ~/.vz/images/base.img --base-id stable

# Start headless VM
vz vm run --image ~/.vz/images/base.img --name dev --headless &

# Execute in guest over vsock
vz vm exec dev -- sw_vers

# Save state and stop
vz vm save dev --stop

# Restore fast from saved state
vz vm run --image ~/.vz/images/base.img --name dev --restore ~/.vz/state/dev.vzsave --headless &
```

### 4. Pinned-base automation policy (macOS VM flows)

- `vz vm init --base <selector>`, `vz vm provision --base-id <selector>`, and `vz vm base verify --base-id <selector>` accept immutable base IDs plus channel aliases (`stable`, `previous`).
- Base descriptors include support lifecycle metadata (`active` or `retired`); selecting a retired or unknown base fails with explicit fallback guidance.
- Retirement guidance always includes `vz vm init --base stable` and, when available, a concrete replacement selector/base.
- `vz vm patch verify` and `vz vm patch apply` reject bundles targeting retired or unsupported base descriptors.
- Unpinned flows require explicit `--allow-unpinned`.
- In CI (`CI=true`), unpinned flows are blocked unless `VZ_ALLOW_UNPINNED_IN_CI=1` is set.
- Runtime policy: `--agent-mode system` is the default for reliability; `--agent-mode user` is opt-in for no-local-sudo workflows.

```bash
# Explicit unpinned local flow
vz vm init --allow-unpinned --ipsw ~/Downloads/restore.ipsw
sudo vz vm provision --image ~/.vz/images/base.img --allow-unpinned
```

### 5. Create signed patch bundles

```bash
# Generate an Ed25519 signing key (PKCS#8 PEM)
openssl genpkey -algorithm Ed25519 -out /tmp/vz-patch-signing-key.pem

# One-command inline patch creation (no operations.json or payload directory required)
vz vm patch create \
  --bundle /tmp/patch-1.vzpatch \
  --base-id stable \
  --mkdir /usr/local/libexec:755 \
  --write-file /path/to/vz-agent:/usr/local/libexec/vz-agent:755 \
  --symlink /usr/local/bin/vz-agent:/usr/local/libexec/vz-agent \
  --set-owner /usr/local/libexec/vz-agent:0:0 \
  --set-mode /usr/local/libexec/vz-agent:755 \
  --signing-key /tmp/vz-patch-signing-key.pem

vz vm patch verify --bundle /tmp/patch-1.vzpatch
sudo vz vm patch apply --bundle /tmp/patch-1.vzpatch --image ~/.vz/images/base.img
```

For advanced CI workflows, `vz vm patch create` also supports `--operations <json>` + `--payload-dir <dir>`.

### 6. Primary image-delta patch flow (sudo once, then sudoless apply)

```bash
# 1) Create a binary image delta from a signed bundle (runs bundle apply on a temp image copy)
sudo vz vm patch create-delta \
  --bundle /tmp/patch-1.vzpatch \
  --base-image ~/.vz/images/base.img \
  --delta /tmp/patch-1.vzdelta

# 2) Apply the binary delta without sudo to produce a new bootable image
vz vm patch apply-delta \
  --base-image ~/.vz/images/base.img \
  --delta /tmp/patch-1.vzdelta \
  --output-image ~/.vz/images/base-patched.img

# 3) Boot-test the patched image
vz vm run --image ~/.vz/images/base-patched.img --name delta-test --headless
```

## Current 0.3 command groups

### Dev environments

`init`, `run`, `run -i`, `stop`, `status`, `logs`

These are the shipped legacy lifecycle commands. They and the infrastructure
groups below are removed from the 0.4 public surface in favor of `up`, `exec`,
`status`, `stop`, and `delete`; advanced operations move to typed APIs.

### OCI workloads

`pull`, `run`, `create`, `exec`, `images`, `prune`, `ps`, `stop`, `rm`, `logs`

### vz-managed stacks

`stack up`, `stack down`, `stack ps`, `stack ls`, `stack config`, `stack events`, `stack logs`, `stack exec`, `stack run`, `stack stop`, `stack start`, `stack restart`, `stack dashboard`

### VMs (macOS)

`vm init`, `vm run`, `vm exec`, `vm save`, `vm restore`, `vm list`, `vm stop`, `vm cache`, `vm provision`, `vm cleanup`, `vm self-sign`, `vm validate`, `vm base`, `vm patch`

## Runtime Daemon Connectivity

Runtime-mutating CLI surfaces (`sandbox`, `stack`, `image`, `file`, `lease`, `execution`, `checkpoint`, `build`) use `vz-runtimed` over gRPC/UDS.

- Default socket path is derived from the state DB directory:
  - `<state-db-parent>/.vz-runtime/runtimed.sock`
- Endpoint override:
  - `VZ_RUNTIME_DAEMON_SOCKET=/absolute/path/to/runtimed.sock`
- Autostart policy:
  - `VZ_RUNTIME_DAEMON_AUTOSTART=1` (default) enables daemon cold-start
  - `VZ_RUNTIME_DAEMON_AUTOSTART=0` disables autostart and fails fast when unreachable
- Transport selector:
  - `VZ_CONTROL_PLANE_TRANSPORT=daemon-grpc` (default)
  - `VZ_CONTROL_PLANE_TRANSPORT=api-http` is accepted; current CLI execution path uses a compatibility connector while full HTTP control-plane routing is tracked in bead `vz-pip6`
- Sandbox startup defaults (daemon policy):
  - `VZ_SANDBOX_DEFAULT_BASE_IMAGE=<image-ref>`
  - `VZ_SANDBOX_DEFAULT_MAIN_CONTAINER=<command-or-container-hint>`
  - `VZ_SANDBOX_DISABLE_LEGACY_DEFAULT_BASE_IMAGE=1` disables compatibility fallback (`debian:bookworm`)
- Retention policy defaults (daemon-owned GC):
  - Untagged checkpoints: max `128` retained, max age `30` days
  - Tagged checkpoints (`--tag`): retained until explicit deletion
  - Receipts: max `20_000` retained, max age `14` days

## Architecture

```
vz-cli
  |
  +-- container commands --> vz-oci --> vz-runtime-contract
  |                              |-> macOS backend (vz-oci-macos, VM-backed)
  |                              '-- Linux backend (vz-linux-native)
  |
  +-- stack commands -----> vz-stack (Compose orchestration)
  |
  '-- vm commands (macOS) -> vz (Virtualization.framework wrapper) + vz-guest-agent
```

## Development

```bash
cd crates
cargo build --workspace
cargo clippy --workspace -- -D warnings
cargo nextest run --workspace
```

Runtime API adapter local smoke test:

```bash
cd crates
cargo run -p vz-api -- \
  --bind 127.0.0.1:8181 \
  --state-store-path /tmp/vz-api-state.db \
  --daemon-auto-spawn true \
  --stack-baseline \
  --capability fs_quick_checkpoint

# in another shell
curl -s http://127.0.0.1:8181/v1/capabilities
curl -s http://127.0.0.1:8181/openapi.json
```

`vz-api` daemon lifecycle behavior can be tuned for local/dev/operator scenarios:

- `VZ_RUNTIME_DAEMON_AUTOSTART=1` (default) enables cold-start of `vz-runtimed`
- `VZ_RUNTIME_DAEMON_AUTOSTART=0` disables auto-start and returns `daemon_unavailable` if daemon is not already running
- `VZ_RUNTIME_DAEMON_SOCKET=/absolute/path/to/runtimed.sock` overrides daemon socket target
- `VZ_RUNTIME_DAEMON_RUNTIME_DIR=/absolute/path/to/.vz-runtime` overrides runtime data directory used during daemon spawn

Sandbox-specific real VM integration validation (macOS ARM64):

```bash
./scripts/run-sandbox-vm-e2e.sh --suite sandbox
```

Full VM lanes (runtime + stack + buildkit):

```bash
./scripts/run-sandbox-vm-e2e.sh --suite all
```

See `docs/sandbox-vm-e2e.md` for reproducible debug workflow and artifact paths.

Conformance and parity coverage:

- [Runtime primitive conformance matrix](docs/runtime-primitive-conformance.md)
- [Daemon-only guardrails and fail-close policy](docs/daemon-only-guardrails.md)

## License

[MIT](LICENSE.md)
