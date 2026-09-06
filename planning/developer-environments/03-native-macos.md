# Native macOS Machines on macOS

> **Delivery decision updated 2026-09-06:** the supported 0.4 path is now
> pinned IPSW → local install → scoped privileged provisioning → native validation
> → cached template → private Machine clones. See [local macOS setup](macos-local-setup.md).
> Earlier published-base/patch requirements below describe the previous design;
> the delta implementation remains optional. The product contract and 0.4 gate
> carry the current acceptance criteria.

Depends on: First-class Environment topology, Machine identity, and durable lifecycle

## Purpose

Lift the existing macOS VM machinery into the shared Machine lifecycle so native
macOS and Linux Machines can participate in one Environment topology while
preserving native macOS process and service semantics.

## Step 1: Adapt existing macOS VM assets

- Reuse pinned IPSW/base descriptors, provisioning, guest agent, vsock execution, account policy, image verification, patching, save/restore, and Virtualization.framework integration.
- Move these behind the shared HostBackend/TargetAdapter contracts rather than
  exposing a separate `vz vm` product lifecycle.
- Give each macOS Machine an immutable verified base plus private writable disk,
  Machine identity/incarnation, auxiliary storage, unique MAC address, locks,
  network attachments, and Environment ownership record.

## Step 2: Define native workspace behavior

- Mount or synchronize the project/worktree using authorized macOS target paths.
- Provide native shell, streaming exec, PTY/signals, launchd services, networking, ports, persistent state, logs, GUI/headless modes, stop/restart/delete, and capability reporting.
- Do not create an implicit Linux sidecar or advertise Docker. When Docker is
  required, the ProjectDefinition declares a Linux Machine in the topology and
  communication uses declared networks/endpoints.

## Step 3: Reconcile reproducibility

Pin macOS image/build/channel and provisioning inputs. Treat saved state as an optional same-host, exact-configuration acceleration—not the portable source of reproducibility. Make update, retirement, repair, rollback, and destructive replacement explicit.

## Required macOS 26+ happy path

Ship at least one complete path for a specific macOS version and build with major
version 26 or later. Pin the Apple restore IPSW source and digest, compatible
base identity, authenticated published bootstrap patch and digest, guest agent,
and Xcode/Swift toolchain in the release inputs. Select the exact version through
implementation and physical verification; a moving channel or placeholder
fingerprint is not a supported release entry.

From a clean supported installation, the product must obtain the artifacts,
verify and apply the matching patch, create the native Machine, and start its
agent without manual host sudo, disk mounting, ownership repair, or agent
injection. Users download the exact published base and matching block patch;
maintainers perform IPSW installation before publication. Resolve a latest-supported
pointer to an authenticated immutable artifact set and persist its pins, keeping
existing Environments stable when that pointer advances. Privileged artifact
preparation must be handled before publication rather than left as a user setup step.

Reuse the image patcher and guest loader behind typed provisioning APIs. The
existing patch code in retired CLI modules and mac-agent's LoaderManifest
selection are foundations; neither proves a complete clean bootstrap. Preserve
the five public lifecycle verbs. Completion requires the available matching
patch and the installed physical happy-path evidence, as specified in
`GOAL-0.4.0.md` and `06-macos-target-validation.md`.

See [automated bootstrap delivery](macos-bootstrap-delivery.md) for the maintainer
artifact workflow, user progress/cache behavior, current candidate and remaining
base compatibility decision.

## Validation

- Contract tests for macOS Machine TargetSpec, capabilities, lifecycle, and
  unsupported Docker errors.
- Real macOS-on-macOS Machine exec/service/network/storage/lifecycle tests plus
  one mixed Linux/macOS Environment topology.
- Legacy `vz vm` records migrate safely; the public command is removed with
  actionable guidance rather than retained as an alias.
