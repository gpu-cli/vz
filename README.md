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

### 1. Boot an instant sandbox

```bash
# Create a required checked-in space definition
cat > vz.json <<'JSON'
{
  "name": "my-workspace"
}
JSON

# Create + attach a new sandbox for the current directory
vz --name my-workspace --cpus 4 --memory 4096 \
  --base-image debian:bookworm \
  --main-container workspace-main

# Inspect persisted startup selection
vz inspect my-workspace
```

`--base-image` and `--main-container` apply when creating a new sandbox (`vz` with no `-c/-r`).
Spaces mode requires `vz.json` and Linux btrfs-backed workspace storage.
`vz.json` must not embed raw secrets; use external env references under `secrets` instead:

```json
{
  "secrets": {
    "db_password": { "env": "DB_PASSWORD" }
  }
}
```

### 2. Manage sandboxes

```bash
# Continue or resume
vz -c
vz -r my-workspace

# List and inspect
vz ls
vz inspect my-workspace

# Remove a sandbox
vz rm my-workspace
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
