---
name: vz
description: >
  Work in reproducible vz Developer Environments. The current skill drives the
  shipped Linux-on-macOS command surface. Use when the user needs to: (1)
  compile or test code for Linux (cross-compile, Linux-only deps), (2) run
  Linux-specific tools (apt-get, systemd, OCI workloads), (3) execute commands
  in a reproducible Linux environment,
  (4) work on a project that has a vz.json file.
  TRIGGER when: project contains vz.json, user says "run in Linux",
  "test on Linux", "compile for Linux", "vz run", or needs Linux-only behavior.
  DO NOT TRIGGER when: commands should intentionally run in the current host
  shell and no vz environment was requested.
---

# vz — Linux Developer Environment execution

Run commands inside the current Linux-on-macOS Developer Environment backend,
implemented by a lightweight Linux VM through Apple's Virtualization.framework.
The repository documents ~3s boot, a VirtioFS project mount, and persistent
reuse. Multi-Machine/mixed-target Environments and the five-verb
`vz up/exec/status/stop/delete` lifecycle are 0.4 product direction but are not
substituted for the currently shipped commands below.

## Detection

Check if the project has a `vz.json` in the working directory or parents. If it does,
Linux commands should go through `vz run` instead of running locally.

## Commands

### Run a command

```bash
vz run <command...>
```

Output streams in real-time. Exit code propagates. Environment variables from
`vz.json` are injected automatically (PATH, HOME, CARGO_TARGET_DIR, etc.).

Examples:
```bash
vz run cargo build
vz run cargo test
vz run make -j4
vz run apt-get install -y libssl-dev  # only works during setup, not ad-hoc
vz run python3 script.py
```

### Interactive shell

```bash
vz run -i bash
```

Opens a PTY-backed interactive session. Use for debugging, exploring the
VM filesystem, or running interactive tools.

### Project setup

```bash
vz init                    # generate vz.json (auto-detects Rust/Node/Python/Go)
vz init --template rust    # force a specific template
vz init --image debian:12  # override base image
```

### Lifecycle

```bash
vz status       # show daemon/VM state, project, mounts
vz stop         # stop the VM (persists disk, next run reboots)
vz run --fresh  # destroy VM + re-run setup from scratch
vz logs         # show daemon logs
vz logs -f      # follow daemon logs
```

## When to use vz run vs local execution

Use `vz run` when:
- The command needs Linux (cargo build for linux targets, apt-get, Linux syscalls)
- The project has a `vz.json`
- The user explicitly asks to run in Linux or in a VM

Use local execution when:
- The command works on macOS natively (git, formatting, linting)
- No `vz.json` exists and user hasn't asked for Linux

## Setup commands vs runtime commands

`vz.json` has a `setup` array — these run once on first boot and are cached by hash.
Do NOT put one-off commands in setup. Instead:
- **Setup**: package installs, toolchain setup (apt-get, rustup, npm install -g)
- **Runtime**: build, test, run commands (cargo build, npm test)

If the user needs a new package installed, suggest adding it to the `setup` array
in `vz.json` and running `vz run --fresh` to re-provision.

## Error patterns

- **"no vz.json found"** — suggest `vz init`
- **"getcwd() failed"** — harmless kernel warning, ignore it
- **Setup reruns every time** — setup command may be failing silently; check with `vz run --fresh` and watch output
- **"daemon startup timed out"** — stale lock file; `vz stop` then retry
- **Command not found in VM** — tool not in setup commands; add to `vz.json` setup array
