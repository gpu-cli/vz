# Native Windows Machine topology parity on Windows hosts

Depends on: Linux-Machine topology parity on Windows hosts

## Purpose

Implement native Windows Machines within the shared Environment topology on
Windows hosts after Linux-on-Windows is stable. These Machines use Windows-native
execution and isolation rather than Linux OCI or youki.

## Step 1: Select the Windows-native isolation backend

Evaluate Windows process isolation, Windows containers, Hyper-V isolation, base-image servicing, licensing, host-version coupling, startup, checkpointing, networking, storage, and security. Record a supported backend/version matrix before implementation.

## Step 2: Implement native target semantics

- Add Windows TargetAdapter and Machine supervision for processes, services,
  PowerShell, ConPTY, exit/cancellation, NTFS, ACLs, junctions/reparse points,
  named pipes, network attachments, firewall, ports, persistent state,
  lifecycle, and observability.
- Add any Windows-native container capability as an explicit negotiated capability; do not present Docker/OCI/youki as universal.

## Step 3: Preserve the common product contract

Use the same Project/Environment/Machine identity, configuration, aggregate and
child lifecycle, topology, ownership, status, workspace resolution, and
fail-closed isolation. Target-specific differences appear as capabilities and
documented errors.

## Validation

- Run `core.*`, `target.windows.*`, and `host.windows.*` release-built suites twice from clean and persisted state.
- Test PowerShell, ConPTY, Windows services, NTFS/ACLs, case behavior, long paths, Unicode, symlinks/junctions, named-pipe ACLs, firewall exposure, crash/restart, installer, upgrade, and two-environment isolation.
- Do not declare Windows-on-Windows ACTIVE until every required native scenario passes.
