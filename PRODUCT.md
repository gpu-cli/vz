# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

The website presents a local-first developer-infrastructure product using
static HTML, CSS, and JavaScript.

## Canonical contract

The authoritative product definition, platform matrix, capability boundaries,
and status language live in
[`docs/developer-environments.md`](docs/developer-environments.md). Product copy
must not contradict that contract.

## Users

Primary today: developers on Apple-silicon Macs who need reproducible, parallel
Linux and native macOS environments for human and agent-driven work.

Next: Linux-host developers using the same Linux target contract. Planned after
that: Windows-host developers, first with the universal Linux target and finally
with native Windows environments.

## Product purpose

**vz creates reproducible, parallel Developer Environments on local hardware.**

The Developer Environment is the product object. It binds a project or worktree
to stable identity, target OS, compute, files, tools, network, persistent state,
and agent sessions. VMs, containers, sandboxes, and processes are backend
mechanisms selected for the host/target pair.

Linux is the universal target across macOS, Linux, and Windows hosts. Native
macOS environments are also part of the current macOS product; native Windows
environments follow Linux-on-Windows. This is a multi-target product, not a
Linux-only sandbox with future host ports.

## Core positioning

vz makes local parallel environments explicit, inspectable, reproducible, and
addressable. Workloads run inside the selected environment boundary, while host
effects occur only through declared mounts, ports, credentials, and control
channels. The concrete isolation mechanism may be a VM or a native OS facility;
the user-facing contract remains stable.

The main value is productive, repeatable development at high concurrency.
Locked-down execution remains an available profile, but is not the organizing
principle of the Developer Environment product.

## Docker contract

Docker is implicit for Linux Developer Environments. Every Linux environment has
its own Docker Engine, state, BuildKit cache, networks, proxy endpoint, and Docker
context. There is no global vz Docker socket and no optional shared Docker
facade. The unmodified host Docker CLI must address a specific environment and
must never fall back to Docker Desktop or another environment.

Docker is not an implicit capability of native macOS or native Windows targets.
Those workflows select a Linux Developer Environment when they require the Linux
Docker contract.

## Current status

- **ACTIVE:** Apple-silicon macOS host support; local Linux VM and OCI/BuildKit
  primitives; native macOS VM flows; deterministic stacks; checkpoint
  primitives; gRPC/HTTP runtime services; current sandbox/run/vm command
  surfaces.
- **DEV:** one unified Developer Environment lifecycle; implicit per-environment
  Docker driven by the local Mac's Docker/Compose/buildx clients; full isolation
  and conformance evidence; Linux-host contract parity.
- **PLANNED:** Linux Developer Environments on Windows, followed by native
  Windows Developer Environments; hosted placement may reuse the contract later.

These labels describe implementation maturity. They do not turn a partial
backend into a full-product parity claim.

## Operating context

Humans and agents select a Developer Environment explicitly or through an
unambiguous project/worktree association. A Linux environment starts with Docker
as part of readiness; its managed Docker context is scoped to that environment.
Multiple environments must run simultaneously without sharing identity, daemon
state, sockets, ports, files, images, volumes, caches, networks, or credentials.

Existing commands such as `vz run`, `vz stack`, `vz build`, `vz docker`, and
`vz vm` describe ACTIVE mechanisms. Their migration toward the unified `vz dev`
experience is DEV; documentation must not present proposed command spelling as
already shipped.

## Product principles

1. Developer Environment first: users choose a target and environment, not a bag
   of backend mechanisms.
2. Linux everywhere: one Linux target contract across macOS, Linux, and Windows.
3. Native where it matters: macOS-on-macOS now; Windows-on-Windows after
   Linux-on-Windows.
4. Parallel by construction: identity and all mutable resources are scoped per
   environment.
5. Docker belongs to Linux environment readiness, not to a global daemon or
   optional compatibility tier.
6. Explicit host effects: mounts, ports, credentials, and policy are declared and
   inspectable.
7. Measured and status-tagged claims: commands and end-to-end evidence precede
   parity claims.

## Evidence and constraints

Repository evidence includes `README.md`, the runtime and VM implementation,
BuildKit and Linux-VM end-to-end lanes, `docs/linux-support.md`, and current macOS
VM flows. The repo states an approximately three-second Linux VM boot; do not
invent benchmarks, users, testimonials, pricing, integrations, or complete
Docker parity.

The currently supported macOS host baseline is Apple silicon on macOS 14+.
Capabilities under DEV or PLANNED must read as direction, not shipped behavior.

## Brand and voice

Name: **vz**. Organization: gpu-cli. License: MIT. Voice: technical, precise,
mechanism-forward, and honest about status.

## Accessibility

Use semantic markup, sufficient contrast, keyboard-operable controls, reduced
motion support, and other WCAG-grade defaults in product surfaces.
