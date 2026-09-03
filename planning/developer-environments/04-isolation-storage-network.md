# Multi-environment isolation, storage, and realistic topology networking

Depends on: First-class Environment topology, Machine identity, and durable lifecycle

## Purpose

Run many copies of a project and realistic multi-Machine systems concurrently
without accidental coupling. Make private and public-like communication
reproducible while the entire topology remains local and isolated.

## Step 1: Storage and workspace ownership

- Give each Machine independent target image, writable disk, service state, home,
  caches, and identity. Linux Machines additionally own independent Docker,
  BuildKit, image, and volume state.
- Make Environment volumes explicit. A volume normally has one Machine owner;
  multi-Machine use declares a sharing protocol and consistency contract. Never
  silently multi-attach writable block storage.
- Project workspaces are Environment resources projected per Machine as
  read-write, read-only, or snapshot with explicit writer rules.
- Use typed native `HostPath` and target-qualified paths. Preserve declared
  spaces, Unicode, case, executable/ACL, symlink/junction, locking, and
  file-watching semantics through authorized translation.
- Validate growth, disk-full, interruption, recovery, restart, and deletion.

## Step 2: Isolated Environment network fabrics

- Give every Environment its own route domain, DNS view, gateways, NAT state,
  firewall, port registry, ingress, impairment state, and credentials.
- Allow overlapping guest CIDRs and repeated internal Machine/service/DNS names
  across Environments without global host routes or DNS leakage.
- Attach Machines only to declared network segments. No implicit flat trusted
  LAN or transitive route exists.
- Define target-neutral TCP/UDP/IPv4/IPv6, internal DNS, host import/export,
  egress, and published-endpoint behavior. Keep gateway implementation details
  out of portable configuration.

## Step 3: Public-like local edge

- Provide an Environment-local edge that forces clients through split DNS,
  routed ingress, firewall/NAT, and optional TLS using synthetic `.test`
  hostnames. Tests must be able to prove no private or host-loopback shortcut was
  used.
- Separate service bind ports, Environment ingress names/ports, and host
  publication. Host exports are explicit, collision-safe, and loopback-only by
  default. Physical LAN or real public exposure requires separate explicit
  policy.
- Make guest-to-host imports explicit and protocol/port scoped. Linux Docker
  supports conventional `host.docker.internal` behavior plus documented
  `host.vz.internal` compatibility only when the import is authorized.
- Control external egress as offline, allowed/audited, or domain/CIDR
  allowlisted. External DNS obeys that policy and can be recorded/pinned.

## Step 4: Deterministic faults and peering

- Support declarative baseline and API-injected latency, jitter, loss,
  duplication, reorder, bandwidth, queue, MTU, reset, DNS failure, blackhole,
  and partition policies with seed, scope, start, TTL, and receipt.
- Ensure TTL cleanup restores baseline even after caller/control-plane failure.
- Deny every cross-Environment route by default. A directional service peer
  grant may expose one Endpoint/protocol/port with owner, expiry, and audit.
- Peering never merges route domains, becomes transitive, or exposes private
  DNS, storage, credentials, Docker, or control-plane endpoints.

## Step 5: Resource and blast-radius behavior

Enforce/inspect CPU, memory, PID, file-descriptor, disk, and network limits.
Controlled OOM, disk-full, service failure, Machine stop, Environment stop, and
delete must not corrupt or interrupt siblings outside the selected ownership
scope.

## Validation

- Run two worktrees plus multiple named Environments from one worktree with
  repeated names, ports, DNS aliases, and overlapping CIDRs.
- Run client -> simulated-public edge -> API -> private database; prove the
  client cannot reach the database and traffic crossed DNS/TLS/ingress/NAT.
- Prove split DNS, host import/export, egress/offline/allowlist behavior,
  deterministic faults and recovery, explicit peering and revocation.
- Prove same-target and mixed Linux/macOS Machine topology, per-Machine Docker
  isolation, stop/up persistence, crash reconstruction, delete safety, and empty
  post-run leak inventory on the real Mac.
- Linux stress covers at least 20 containers plus parallel pulls/builds/execs;
  native Machines use equivalent target-qualified pressure.
