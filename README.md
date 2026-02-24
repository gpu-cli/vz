# vz

Cross-platform runtime for containerized workloads and macOS VM automation.

`vz` provides one CLI for:
- OCI image and container lifecycle
- Multi-service stacks from Compose files
- macOS VM provisioning and control (Apple Virtualization.framework)

Typical use cases:
- Run isolated build/test workloads from OCI images
- Launch local multi-service environments from Compose
- Automate deterministic macOS VM test sandboxes

## Why vz

- **One interface, multiple runtimes.** Use the same CLI flow on macOS and Linux.
- **Container-native.** Pull, run, create, exec, log, stop, and remove OCI workloads.
- **Stack-aware.** Bring up complete Compose apps with events, logs, and service exec.
- **VM automation on macOS.** Provision, run, exec, save, and restore macOS VMs over vsock.
- **Script-friendly.** Consistent command model with `--json` support across commands.

## Install

### Build from source

```bash
# Requires Rust 1.85+
git clone https://github.com/gpu-cli/vz.git
cd vz/crates
cargo build --workspace --release
```

## Platform support

- **Linux:** container + stack commands
- **macOS (Apple Silicon):** container + stack commands, plus `vz vm ...`
- **macOS VM requirement:** virtualization entitlement (`vz vm self-sign`)

### macOS VM entitlement (macOS only)

VM commands require the `com.apple.security.virtualization` entitlement.

```bash
./target/release/vz vm self-sign
```

## Quick start

### 1. Run OCI containers

```bash
# Run a one-shot command
vz run alpine:3.20 -- echo "hello from vz"

# Publish a port
vz run --publish 8080:80 nginx:alpine
```

### 2. Create and manage long-lived containers

```bash
# Start a container in the background
vz create --name devbox ubuntu:24.04 -- sleep infinity

# Inspect and execute commands
vz ps
vz exec devbox -- uname -a
vz logs devbox --tail 50

# Stop and remove
vz stop devbox
vz rm devbox
```

### 3. Run a Compose stack

```bash
# Start services
vz stack up -f compose.yaml -n demo

# Inspect and stream logs
vz stack ps demo
vz stack logs demo --service web --follow

# Tear down
vz stack down demo --volumes
```

### 4. Manage macOS VMs (macOS only)

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

### 5. Pinned-base automation policy (macOS VM flows)

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

### 6. Create signed patch bundles

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

### 7. Primary image-delta patch flow (sudo once, then sudoless apply)

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

## Command groups

### Containers

`pull`, `run`, `create`, `exec`, `images`, `prune`, `ps`, `stop`, `rm`, `logs`

### Stacks

`stack up`, `stack down`, `stack ps`, `stack ls`, `stack config`, `stack events`, `stack logs`, `stack exec`, `stack run`, `stack stop`, `stack start`, `stack restart`, `stack dashboard`

### VMs (macOS)

`vm init`, `vm run`, `vm exec`, `vm save`, `vm restore`, `vm list`, `vm stop`, `vm cache`, `vm provision`, `vm cleanup`, `vm self-sign`, `vm validate`, `vm base`, `vm patch`

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

## License

[MIT](LICENSE.md)
