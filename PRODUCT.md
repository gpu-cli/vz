# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

User-decided: single-file static HTML/CSS/JS ("Write me a html page"). No framework, no build step. [Inferred: hosted as a GitHub Pages / static asset for the gpu-cli/vz repo.]

## Users

Primary: developers on Apple-Silicon Macs who run agentic AI coding tools (Claude Code, Codex, OpenCode, Aider) and need those agents to execute arbitrary commands — builds, tests, package installs, network calls — without endangering the host. They live in the terminal, distrust heavyweight VMs, and want Docker-class workflows without Docker Desktop.

Secondary: teams evaluating local sandboxing vs. hosted sandboxes (e2b, Daytona, Fly Sprites); ops-minded developers who want a fast, inspectable Linux VM on their Mac. Future: Linux hosts running the same contract.

## Product Purpose

vz is a local-first sandbox runtime: one CLI for OCI containers, Compose stacks, and Linux VMs on macOS (Apple Virtualization.framework), later Linux. An agent gets a real, disposable Linux VM that boots in ~3s, mounts the project via VirtioFS, runs commands over vsock, and can checkpoint/restore state. Success: an agent's riskiest command becomes a non-event — the host stays clean, the sandbox is disposable, everything is inspectable.

This page is the **future-state vision**: the product after Wave 1–3 land (streaming API, filesystem API, full Docker inside the guest VM, MCP-first agent surfaces, port forwarding, checkpoint templates). It must sell the destination honestly — mechanisms and commands, not vaporware adjectives.

## Positioning

The only sandbox that is all of: (1) real VM isolation on your own hardware — no cloud round-trips, no hidden Linux VM doing unaccountable things (unlike Docker Desktop), (2) agent-native by design — MCP server, streaming exec, files API, receipts, egress policy (unlike Docker Desktop/Tart), (3) local-first but with the same runtime contract for hosted Linux later (unlike e2b's cloud-only model), (4) speed — ~3s boot, checkpoint restore, persistent warm state (unlike full-boot VM tools).

The architectural claim a competitor cannot copy without changing their product: **the host only ever runs a CLI, a control daemon, and thin vsock/virtiofs plumbing — every workload, including dockerd itself, executes inside the guest VM**.

## Operating Context

Terminal-first usage: `vz init` generates a per-project `vz.json`; `vz run cargo test` boots or reuses the VM; `vz stack up` runs Compose; `vz build` uses BuildKit inside a guest VM. Agents integrate via MCP tools (`vz_exec`, `vz_read_file`, ...) or gRPC/HTTP runtime API; humans use the real `docker` CLI through `DOCKER_HOST=~/.vz/docker.sock` proxied over vsock to dockerd inside the guest. Projects keep working directories on the host, mounted read-write via VirtioFS.

## Capabilities and Constraints

Real today (repo evidence): Linux VM runtime w/ custom kernel 6.12.85 profiles, youki OCI runtime in-guest, deterministic Compose stacks (`vz stack`), BuildKit builds (`vz build`), `fs_quick` checkpoints, macOS VM flows + signed patch bundles, gRPC daemon (`vz-runtimed`) + HTTP API (`vz-api`), `vz run` + `vz.json`, ~3s VM boot, host stays clean (all workloads in guest).

Future (this page may show, labeled as the road we're on): sandbox lifecycle + streaming exec/stdin/PTY API, filesystem API, **real Docker inside the guest VM** (kernel `MEMCG`+`BRIDGE_NETFILTER`, dockerd/containerd in guest, vsock socket proxy — host never runs containers), MCP server + agent receipts + egress policy, port forwarding/preview URLs, checkpoint templates.

Constraints: Apple Silicon + macOS 14+ (host); claims must stay within these facts — no invented benchmarks, customers, or pricing. Performance claims only with repo-measured evidence (planning/README: measured-claims-only rule).

## Brand Commitments

Name: **vz**. Org: gpu-cli. License: MIT. Voice: technical, precise, mechanism-forward; no hype adjectives. [Inferred from README/planning voice — confirm if a different tone is wanted.]

## Evidence on Hand

Repo docs: `README.md` (command tree, architecture), `docs/vz-innovation-planning.md` (product boundary, P0–P2), `docs/e2b-vs-vz-gap-analysis.md`, `planning/agent-sandbox-api-landscape.md` (§7.5 MCP tool list — direct content source), `private-vz/vz-run-devx-improvements.md` (real usage learnings). Real command examples: `vz run cargo test`, `vz stack up`, `vz build .`, `vz docker run --rm`. Boot claim "~3s" is repo-stated. **Absences that must not be fabricated:** benchmarks, user counts, testimonials, pricing, logos of integrations.

## Product Principles

1. Local-first: the sandbox is on your hardware; cloud is an escape hatch, not a requirement.
2. The host stays clean: nothing container-related executes on macOS, ever — and the design says so out loud.
3. Agent-native, not agent-compatible: MCP/gRPC/HTTP are first-class surfaces, not afterthoughts.
4. Prove with commands, not adjectives: every section demonstrates a real command or protocol.
5. Inspectable and honest: future capabilities are framed as the road we're on, never as shipped facts.

## Accessibility & Inclusion

[No project-specific requirement established; apply WCAG-grade defaults: semantic markup, contrast, keyboard operability.]