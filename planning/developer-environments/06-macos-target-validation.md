# Native macOS-on-macOS validation

Depends on: Converged Developer Environment CLI; Native macOS-on-macOS Developer Environments; Environment isolation, storage, and networking

## Purpose

Release-gate the native macOS target on an Apple Silicon Mac and prove it participates in the same core Environment contract without inheriting Linux/Docker assumptions.

## Step 1: Add the target-native harness

Create a release-built local harness using only the public `vz dev` lifecycle. Cover pinned image provisioning/verification, native shell and exec, PTY/signals, launchd services, project sharing, APFS/native path behavior, networking, ports, persistence, stop/restart/delete, update/retirement, and recovery.

## Step 2: Run shared and target suites

- `core.*`: identity, lifecycle, persistence, isolation, sharing authorization, networking, recovery, and concurrency.
- `posix.*`: process, signal, PTY, permissions, and filesystem behavior shared with Linux where meaningful.
- `target.macos.*`: IPSW/base, launchd, APFS, macOS build compatibility, VM limitations, save/restore, and hardware-bound state behavior.
- `host.macos.*`: Virtualization.framework, VirtioFS, vsock, sleep/wake, signing, install, upgrade, and firewall behavior.

## Step 3: Cross-target isolation

Run Linux and macOS Developer Environments simultaneously on the same Mac. Prove lifecycle, disks, mounts, ports, credentials, logs, endpoints, and processes cannot cross. Docker access remains present only on the Linux target.

## Validation

Run clean and persisted release-built gates twice on the current Mac and retain machine-readable evidence under `.artifacts/developer-environment-e2e/<timestamp>/`.
