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

Include a required clean bootstrap scenario for at least one exact macOS 26+
guest version/build. Start without a preconfigured guest or manually installed
loader. Through supported installed product interfaces, download the exact
published base image and authenticated matching block patch, verify their
digests and compatibility, apply the patch, boot the selected Machine, and
observe its agent becoming ready. No manual host sudo, disk mounting, ownership
repair, or agent injection is permitted. Record artifact sources, authentication,
digests, base production/selection, patch application, host compatibility, and
guest `sw_vers` plus VM/process identity. Placeholder catalog entries, local-only
unpublished patches, and a patcher unit test do not count as this scenario.

Build/test the pinned native Swift fixture in that Machine, execute its expected
protocol/version output, then prove Stop/Up persistence and ownership-safe Delete
in the aggregate staged run. Wrong-base or tampered patches fail before Machine
activation. The clean-path result must demonstrate that the distributed patch
matches the exact base the user downloads. Exercise latest-supported resolution
to a persisted immutable pair, then prove a pointer advance leaves an existing
Environment pinned. Measure cold preparation, cached clone creation and warm
Stop/Up separately; cached creation must reuse the prepared template without
repeating downloads, IPSW installation or patch application.

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

Run the native lane in the canonical aggregate gate's clean-provision,
persisted-recovery, and final-cleanup phases on the local Mac, using its one
candidate tuple and run ID. Retain machine-readable evidence under
`.artifacts/vz-0.4-e2e/<run-id>/native-macos/`. Required scenarios run once without
test-case retries; missing or skipped required evidence is failure. Follow
`GOAL-0.4.0.md` for candidate retention and the terminal release verdict.
