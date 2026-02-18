# 06 — vz-cli Commands & UX

## Purpose

Standalone CLI for managing macOS VMs without writing Rust. Built with clap 4.

## Commands

### `vz init`

Create a golden macOS VM image from scratch.

```
vz init [--ipsw <path>] [--disk-size 64G] [--output ./images/base.img]
```

Steps:

1. Download latest compatible IPSW from Apple (or use provided path)
2. Create empty disk image at specified size
3. Configure VM (macOS boot, platform identity, hardware model)
4. Run VZMacOSInstaller with progress bar
5. Boot VM for first-time setup (user account, Xcode CLI tools)
6. Shut down — golden image ready

Interactive first-boot wizard:

- Automatically skip Apple ID (use Setup Assistant skip)
- Create admin user (default: "dev" / auto-generated password)
- Install Xcode Command Line Tools
- Install Homebrew (optional)
- Install Rust toolchain (optional)
- Shut down cleanly

### `vz run`

Start a VM with optional mounts and display.

```
vz run [--image <path>] [--mount <tag>:<host-path>] [--cpus 4] [--memory 8G]
       [--headless] [--restore <state-path>] [--name <vm-name>]
```

- `--mount` can be specified multiple times
- `--headless` skips display configuration (default for server use)
- `--restore` starts from saved state instead of cold boot
- VM runs in foreground, Ctrl+C sends stop request

### `vz exec <name> -- <cmd...>`

Execute a command inside a running VM via the guest agent.

```
vz exec my-vm -- cargo build --release
vz exec my-vm -- bash -c "cd /mnt/workspace/project && cargo test"
```

- Connects to guest agent over vsock
- Streams stdout/stderr in real time
- Exits with the same exit code as the remote command
- Supports stdin forwarding (pipe-friendly)

### `vz save <name>`

Save VM state for fast restore.

```
vz save my-vm [--output ./states/my-vm.state]
```

- VM must be running — pauses, saves state, resumes (or stops if `--stop`)
- State file is hardware-encrypted, not portable

### `vz restore <name>`

Restore VM from saved state.

```
vz restore --state ./states/my-vm.state --image ./images/base.img [--mount ...]
```

- ~5-10s to restore vs 30-60s cold boot
- Must use same VM configuration (CPU, memory, devices)

### `vz list`

Show running VMs.

```
vz list
```

Output: name, state, cpus, memory, uptime, mounts

### `vz stop <name>`

Stop a running VM.

```
vz stop my-vm [--force]
```

- Default: sends graceful stop request
- `--force`: immediate termination

### `vz cache`

Manage cached files (IPSWs, partial downloads).

```
vz cache list             # Show cached files and sizes
vz cache clean            # Delete cached IPSWs (images and states kept)
vz cache clean --all      # Delete everything in ~/.vz/cache/
```

### `vz cleanup`

Detect and clean up orphaned VMs (stale PIDs, leaked resources).

```
vz cleanup
```

### `vz self-sign`

Ad-hoc sign the vz binary with required entitlements. Needed after `cargo install vz-cli`.

```
vz self-sign
```

## Global Options

```
--verbose / -v     Increase log verbosity
--quiet / -q       Suppress non-error output
--json             Output as JSON (for scripting)
```

## VM Registry

VMs are tracked in `~/.vz/vms.json`:

```json
{
  "my-vm": {
    "image": "/path/to/image.img",
    "state": "running",
    "pid": 12345,
    "vsock_port": 7424,
    "mounts": [{"tag": "workspace", "source": "/Users/dev/workspace"}]
  }
}
```

## Orphaned VM Detection

### The Problem

macOS VMs created via Virtualization.framework persist even if the parent process crashes. If `vz run` is killed (SIGKILL, power loss, system crash), the VM keeps running but the CLI process is gone. The VM registry (`~/.vz/vms.json`) shows the VM as "running" with a stale PID.

On next invocation, `vz run` may fail due to the 2-VM limit being exhausted by orphaned VMs, or resources may be silently consumed.

### Detection

On every `vz` command that interacts with VMs (`run`, `list`, `exec`, `stop`):

1. Read `~/.vz/vms.json`.
2. For each entry with `"state": "running"`:
   a. Check if the PID is still alive: `kill(pid, 0)`.
   b. If the process is dead, mark the entry as orphaned.
3. Report orphaned VMs to the user.

### Cleanup

```
vz cleanup
```

- Lists orphaned VMs (stale PID, no running process).
- Attempts to stop any still-running VMs (the VM process may have been reparented to launchd).
- Removes stale entries from the registry.
- Reports freed resources.

### Automatic Cleanup on `vz run`

If `vz run` detects orphaned VMs and the 2-VM limit would be exceeded:

1. Print warning: "Found orphaned VM 'my-vm' (PID 12345 no longer running). Cleaning up..."
2. Clean up the orphaned entry.
3. Proceed with the new VM.

This ensures `vz run` always works if there is capacity, without requiring the user to manually run `vz cleanup`.

### PID File

Each running VM also writes a PID file at `~/.vz/run/<name>.pid` for faster detection:

```
~/.vz/
├── run/
│   ├── my-vm.pid      # Contains PID of the vz process managing this VM
│   └── my-vm.lock     # flock() held while VM is running
```

The `.lock` file uses `flock()` — if the lock is not held, the VM is orphaned regardless of what the PID file says. This handles PID reuse correctly.

## Configuration File

Optional `~/.vz/config.toml`:

```toml
[defaults]
cpus = 4
memory_gb = 8
headless = true

[images]
dir = "~/.vz/images"

[states]
dir = "~/.vz/states"
```

## Distribution

### cargo install (Build from Source)

```bash
cargo install vz-cli
```

After installation, users must ad-hoc sign the binary (Virtualization.framework requires the `com.apple.security.virtualization` entitlement):

```bash
codesign --sign - \
  --entitlements <(curl -sL https://raw.githubusercontent.com/conduit-ventures/vz/main/entitlements/vz-cli.entitlements.plist) \
  --force \
  "$(which vz)"
```

Consider adding a `vz self-sign` command that performs this automatically if the binary is not signed.

### Homebrew Tap (Recommended for End Users)

```bash
brew tap conduit-ventures/tap
brew install vz
```

The Homebrew formula installs a pre-built, signed, and notarized binary. No manual signing step required. The formula is updated automatically by the release workflow.

### GitHub Releases

Each tagged release publishes:
- `vz-darwin-arm64.tar.gz` — Signed + notarized macOS binary
- `vz-darwin-arm64.tar.gz.sha256` — SHA256 checksum

### Install Script

```bash
curl -sSf https://vz.dev/install | sh
```

The script:
1. Detects architecture (arm64 required — Intel Macs cannot run macOS guests)
2. Downloads the latest signed binary from GitHub Releases
3. Verifies SHA256 checksum
4. Installs to `~/.vz/bin/vz`
5. Prints PATH instructions

See `09-signing.md` for full signing and distribution details.
