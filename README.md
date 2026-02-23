# vz

Cross-platform runtime for containerized workloads and macOS VM automation.

`vz` gives you one CLI for:
- OCI image and container lifecycle
- Multi-service stacks from Compose files
- macOS VM provisioning and control (Apple Virtualization.framework)

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
# Create a base image from IPSW
vz vm init --disk-size 64G

# Provision account + guest agent (one-time per image)
sudo vz vm provision --image ~/.vz/images/base.img

# Start headless VM
vz vm run --image ~/.vz/images/base.img --name dev --headless &

# Execute in guest over vsock
vz vm exec dev -- sw_vers

# Save state and stop
vz vm save dev --stop

# Restore fast from saved state
vz vm run --image ~/.vz/images/base.img --name dev --restore ~/.vz/state/dev.vzsave --headless &
```

## Command groups

### Containers

`pull`, `run`, `create`, `exec`, `images`, `prune`, `ps`, `stop`, `rm`, `logs`

### Stacks

`stack up`, `stack down`, `stack ps`, `stack ls`, `stack config`, `stack events`, `stack logs`, `stack exec`, `stack run`, `stack stop`, `stack start`, `stack restart`, `stack dashboard`

### VMs (macOS)

`vm init`, `vm run`, `vm exec`, `vm save`, `vm restore`, `vm list`, `vm stop`, `vm cache`, `vm provision`, `vm cleanup`, `vm self-sign`, `vm validate`

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
