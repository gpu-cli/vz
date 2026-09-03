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

The Developer Environment is the product object: an isolated realization of a
project topology. A project or worktree can have multiple named Environment
instances, and each Environment can contain multiple target-native Machines plus
their storage, network fabric, endpoints, policies, and agent sessions. VMs,
containers, sandboxes, and processes are backend mechanisms for Machines.

Target OS belongs to each Machine. Linux is the universal Machine target across
macOS, Linux, and Windows hosts. Native macOS Machines are also part of the
current macOS product; native Windows Machines follow Linux-on-Windows. A single
Environment may contain heterogeneous Machines.

## Core positioning

vz makes local parallel topology instances explicit, inspectable, reproducible,
and addressable. Machines communicate only through declared private or
public-like paths; host effects occur only through declared workspace
projections, endpoints, credentials, and control channels. Separate Environments
are mutually isolated by default.

The main value is productive, repeatable development at high concurrency.
Locked-down execution remains an available profile, but is not the organizing
principle of the Developer Environment product.

## Docker contract

Docker is implicit for Linux Developer Machines. Every Developer-profile Linux Machine has its own
Docker Engine, state, BuildKit cache, networks, endpoint, and Docker context.
There is no Environment-global or global vz Docker socket. The unmodified host
Docker CLI addresses one exact Environment/Machine and never falls back to
Docker Desktop, a sibling Machine, or another Environment.

Docker is not an implicit capability of native macOS or native Windows targets.
Those workflows declare a Linux Machine when they require the Linux Docker
contract.

## Current status

- **ACTIVE:** Apple-silicon macOS host support; local Linux VM and OCI/BuildKit
  primitives; native macOS VM flows; deterministic stacks; checkpoint
  primitives; gRPC/HTTP runtime services; current sandbox/run/vm command
  surfaces.
- **DEV:** Project/Environment/Machine topology lifecycle; implicit per-Linux-
  Machine Docker driven by the local Mac's Docker/Compose/buildx clients;
  realistic network topology, full isolation, and conformance evidence.
- **PLANNED:** Linux Developer Environments on Windows, followed by native
  Windows Developer Environments; hosted placement may reuse the contract later.

These labels describe implementation maturity. They do not turn a partial
backend into a full-product parity claim.

## Operating context

Humans and agents select an Environment explicitly or through an unambiguous
project/worktree association, then select a default or named Machine. A worktree
may have multiple Environments. Every Developer-profile Linux Machine starts with Docker readiness
and a Machine-specific context. Environment instances may repeat Machine names,
ports, DNS aliases, and CIDRs without sharing identity or mutable state.

Existing commands such as `vz run`, `vz stack`, `vz build`, `vz docker`, and
`vz vm` describe ACTIVE legacy mechanisms. Their replacement by the five-verb
`vz up/exec/status/stop/delete` surface is DEV; proposed spelling must not be
presented as already shipped.

## Product principles

1. Developer Environment first: users instantiate an isolated project topology,
   not a bag of backend mechanisms.
2. Multiple on both axes: many Environment instances per project/worktree and
   many target-native Machines per Environment.
3. Linux everywhere: one Linux Machine contract across macOS, Linux, and Windows.
4. Native where it matters: macOS-on-macOS now; Windows-on-Windows after
   Linux-on-Windows.
5. Parallel by construction: identity and all mutable resources are scoped per
   environment.
6. Docker belongs to each Developer-profile Linux Machine, not to an Environment-global daemon or
   optional compatibility tier.
7. Network realism without exposure: declared private paths and a local
   simulated-public DNS/TLS/ingress/NAT edge; cross-Environment access is
   explicit and least-privilege.
8. Explicit host effects: mounts, ports, credentials, and policy are declared
   and inspectable. Host imports are authenticated, exact loopback-service
   grants and remain independent from Internet egress; no shared gateway alias
   is an authority boundary.
9. Measured and status-tagged claims: commands and end-to-end evidence precede
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
