# Linux-on-Windows Developer Environment parity

Depends on: Linux host Developer Environment parity

## Purpose

Bring the universal Linux-target Developer Environment to Windows hosts after Linux-on-Linux parity, with an explicit WSL2/Hyper-V backend decision and no leakage of Unix/macOS assumptions into the public contract.

## Step 1: Run a bounded backend spike

Evaluate supported Windows versions and Linux isolation choices including WSL2 and Hyper-V. Select one Linux-target production backend using measured startup, filesystem, networking, suspend/restart, privilege, packaging, x64/ARM64, and observability evidence.

## Step 2: Implement Windows host integration

- Map Windows project/worktree paths to stable environment identity without keying persistence on raw path spelling or case.
- Translate authorized native `C:\...` bind sources through the Engine Endpoint Adapter/Share Backend; a byte-only proxy to a Linux daemon is insufficient.
- Implement filesystem sharing, networking, published ports, host reachability, persistent disks, lifecycle reconciliation, installation/upgrades, and a Docker-context endpoint such as a private named pipe or other authenticated local transport supported by the selected backend.
- Preserve per-environment ownership, fail-closed routing, implicit Docker, and youki-only Linux execution. Windows-native environments are a separate subsequent phase.

## Step 3: Port the host driver and UX

Support PowerShell and command-shell friendly environment/context selection without mutating Docker's global default context. Keep commands, configuration, JSON schemas, lifecycle, and status semantically identical to macOS/Linux.

## Step 4: Reuse the scenario suite

Run the shared lifecycle, Docker, buildx, Compose, storage, networking, recovery, concurrency, and isolation scenarios. Add Windows-specific path, named-pipe, ACL, sleep/resume, firewall, and installer/upgrade cases.

## Validation

- Release-built clean and persisted runs pass twice on each supported Windows/backend combination.
- Required shared scenario IDs match macOS and Linux summaries.
- Test spaces, Unicode, long paths, case behavior, symlinks/junctions, ACLs, named-pipe ownership, firewall exposure, and restart recovery.
- Publish the Windows support matrix and intentional differences without weakening the common product contract.
