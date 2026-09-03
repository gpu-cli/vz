# Native macOS-on-macOS validation

Depends on: Minimal five-verb Developer Environment CLI; native macOS Machines;
multi-environment isolation and topology networking

## Purpose

Release-gate native macOS Machines on an Apple Silicon Mac and prove they
participate in aggregate and mixed-target Environment topologies without
inheriting Linux/Docker assumptions.

## Step 1: Add the target-native harness

Create `scripts/run-macos-developer-environment-e2e.sh --suite all` using
release-built artifacts, the five public lifecycle verbs, and typed APIs where
the CLI intentionally has no operation. Cover pinned image verification, native
exec, PTY/signals, launchd, workspace projection, APFS, networking, persistence,
stop/up/delete, update/retirement, and recovery.

## Step 2: Run shared and target suites

- `core.*`: identity, lifecycle, persistence, isolation, sharing authorization, networking, recovery, and concurrency.
- `posix.*`: process, signal, PTY, permissions, and filesystem behavior shared with Linux where meaningful.
- `target.macos.*`: IPSW/base, launchd, APFS, macOS build compatibility, VM limitations, save/restore, and hardware-bound state behavior.
- `host.macos.*`: Virtualization.framework, VirtioFS, vsock, sleep/wake, signing, install, upgrade, and firewall behavior.

## Step 3: Cross-target isolation

Run Linux and macOS Machines in the same Environment through declared private
and public-like paths, then run equivalent Machines in separate Environments.
Prove lifecycle, disks, mounts, ports, credentials, events, endpoints, and
processes do not cross ownership boundaries. A macOS Machine cannot inherit or
select a sibling Linux Machine's Docker endpoint.

## Validation

Run clean and persisted release-built gates twice on the current Mac and retain
machine-readable evidence under
`.artifacts/macos-developer-environment-e2e/<timestamp>/`. Missing or skipped
required evidence is failure.
