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
