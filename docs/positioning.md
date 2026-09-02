# vz Positioning — the "ship dockerd in the guest" decision

Date: 2026-09-02
Status: committed direction (this is a decision brief, not a shipped-features list)
Audience: contributors, collaborators, and anyone deciding whether vz is their sandbox runtime

---

## The one-liner

**vz is a local-first sandbox runtime for agentic workloads: every workload — including the Docker daemon itself — executes inside a disposable Linux VM on your Mac. The host stays clean.**

## The decision

We are shipping **real Docker inside the guest VM**: actual `dockerd` + `containerd` running on the guest kernel, with the host's `docker`/`docker compose`/`buildx` CLIs pointed at it through a unix-socket proxy over vsock (`DOCKER_HOST=unix://~/.vz/docker.sock`).

This is not the existing `vz docker` shim (a translation layer with strict guardrails, which stays for vz-native containers). It is the full-compat tier: zero workflow rewrite for anyone who already knows Docker.

## The invariant nobody can copy without changing their product

The host runs three things, ever:

1. the vz CLI
2. the vz-runtimed control daemon
3. thin vsock / VirtioFS plumbing

Everything else — the guest kernel, youki, buildkitd, **dockerd** — executes in the guest.

Docker Desktop also hides a Linux VM with dockerd inside it. The difference: Docker Desktop's VM is unaccountable and opaque. In vz the VM **is the product** — inspectable down to its kernel config, checkpointable, disposable in one command. We are not building a hidden VM; we are building the visible, fast, agent-native one.

## Why this is tractable (the pattern is already proven)

- The guest kernel already runs containers in-guest — youki proves namespaces, cgroups, overlayfs, seccomp. The gaps are two guest-kernel config flags: `CONFIG_MEMCG`, `CONFIG_BRIDGE_NETFILTER`.
- `buildkitd` already ships in-guest and is proxied over vsock. dockerd is the same problem class: provision static arm64 binaries, supervise via the guest agent, persistent `/var/lib/docker` volume, proxy the API socket.
- The nft/iptables userspace work (bead `vz-0ml`) is dual-purpose: it fixes `host.vz.internal` reachability today and unblocks Docker bridge networking tomorrow.

## Positioning against the field

| Neighbor | Their model | Why vz wins |
|---|---|---|
| Docker Desktop | Opaque hidden Linux VM, slow, agent-blind | Same architecture, visible; ~3s boot; agent-native surfaces; the VM is inspectable, not magic |
| e2b / Daytona / Sprites | Cloud-only sandbox platforms, API-first | vz is the **local** equivalent — no cloud round-trip, your hardware, same runtime contract planned for hosted Linux later |
| Tart | Ephemeral macOS VM manager, CI-focused | Long-lived sandboxes, session model, agent surfaces, vsock — not just clone-and-boot |
| Seatbelt / sandbox-exec | Deprecated process-level rules | Real VM isolation, network egress policy (planned), no known escape class of process sandboxing |
| Vibe / VibeBox / Firecracker | Linux-host microVMs | They can't serve Mac developers; vz covers the Mac (and joins Linux later via `vz-linux-native`) |

## The three-wave roadmap

**Wave 1 — Agent data plane** (unblocks everything): streaming exec + stdin/PTY/cancel, filesystem API, sandbox lifecycle through daemon and HTTP API, stack egress MASQUERADE (`vz-0ml`).

**Wave 2 — Full Docker in the guest**: `MEMCG` + `BRIDGE_NETFILTER` guest kernel flags, dockerd/containerd provisioning, vsock socket proxy, `DOCKER_HOST` compat, a dedicated Linux-VM e2e lane.

**Wave 3 — Agent-native surface** (the moat): MCP server (10-tool register: create/destroy sandbox, exec, read/write/list files, checkpoint, restore, set network, preview URL), port forwarding / preview URLs, egress policy with receipts.

## How we talk about it

- Status-tagged honesty: **ACTIVE** (shipped), **DEV** (in progress), **PLANNED** (committed roadmap, subject to change). Future features never read as shipped.
- Measured claims only: ~3s boot cites repo docs; no benchmarks, customer counts, or testimonials that don't exist.
- The pitch is the truth table: whatever the agent does, **host effect: NONE** — except through the one declared, visible channel (the project mount).

## The future-state page

`site/index.html` renders this positioning as a developer-facing specification ("The Datasheet" direction): part-number masthead, boundary schematic with dockerd drawn inside the guest, MCP register as an IC pin map, an illustrative boundary test bench, and the truth table where every agent action resolves to `HOST EFFECT: NONE`. Use it to sell the destination honestly — the ACTIVE/DEV/PLANNED tags keep it truthful while the waves land.