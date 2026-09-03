# Multi-environment isolation, storage, and realistic topology networking

Depends on: First-class Environment topology, Machine identity, and durable lifecycle

## Purpose

Run many copies of a project and realistic multi-Machine systems concurrently
without accidental coupling. Make private and public-like communication
reproducible while the entire topology remains local and isolated.

## Step 1: Storage and workspace ownership

- Give each Machine independent target image, writable disk, service state, home,
  caches, and identity. Developer-profile Linux Machines additionally own
  independent Docker, BuildKit, image, and volume state.
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
- Persist stable Environment, Machine, Network, and endpoint ownership in the
  resolved network plan. Resource names, truncated display names, bridge
  prefixes, and guest CIDRs are never sufficient cleanup or authorization keys.

## Step 3: Public-like local edge

- Provide an Environment-local edge that forces clients through split DNS,
  routed ingress, firewall/NAT, and optional TLS using synthetic `.test`
  hostnames. Tests must be able to prove no private or host-loopback shortcut was
  used.
- Separate service bind ports, Environment ingress names/ports, and host
  publication. Host exports are explicit, collision-safe, and loopback-only by
  default. Physical LAN or real public exposure requires separate explicit
  policy.
- Make guest-to-host imports explicit and protocol/port scoped. A host import
  stores an exact host-loopback destination, authenticates the owning
  Environment/Machine/import over a private relay transport, and exposes the
  exact relay address, guest-side port, network attachment, and optional alias
  only to that owning Machine. The guest never supplies an arbitrary host destination. `host.docker.internal` or
  `host.vz.internal` may resolve only for a declared import; neither name maps
  unconditionally to a shared Apple NAT gateway.
- Keep host imports independent from external egress. The Environment owns and
  atomically reconciles the ruleset, while policy attachment and source matching
  are explicit per Machine/network. An offline Machine can
  use an authorized host import without receiving Internet, LAN, or arbitrary
  host access, and enabling egress never authorizes a host import.
- Control external egress as offline, allowed/audited, or domain/CIDR
  allowlisted using an Environment-owned deny-first firewall/NAT ruleset.
  Domain policy uses mediated DNS plus TTL-bound address sets. Host, LAN,
  link-local, multicast, control-plane, and sibling-Environment ranges stay
  denied unless a distinct explicit capability permits them.

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

Network setup is transactional. It installs owner-scoped deny rules before
enabling forwarding and rolls back in reverse order after any partial failure.
Stop, delete, and recovery first revoke import/peer authorization, close relay
sessions, remove exact owned policy and NAT state, then remove exact inventoried
links/namespaces. Cleanup failures remain visible and retryable; prefix scans or
cross-Environment rule deletion are forbidden.

## Validation

- Run two worktrees plus multiple named Environments from one worktree with
  repeated names, ports, DNS aliases, and overlapping CIDRs.
- Run client -> simulated-public edge -> API -> private database; prove the
  client cannot reach the database and traffic crossed DNS/TLS/ingress/NAT.
- Prove a service bound only to host `127.0.0.1` works through one declared
  import while the undeclared port, wrong protocol, wrong Machine, sibling
  Environment, host LAN, and arbitrary host destinations fail. Prove no host
  wildcard/LAN listener exists during the test.
- Prove split DNS, loopback-only exports, independent
  offline/allowed/CIDR/domain egress behavior, deterministic faults and
  recovery, explicit peering and revocation.
- In one Environment, run two Machines with different egress attachments and
  prove allowed traffic from one does not change the other's offline/allowlist
  behavior or either Machine's host-import authority.
- Prove same-target and mixed Linux/macOS Machine topology, per-Machine Docker
  isolation, stop/up persistence, crash reconstruction, delete safety, and empty
  post-run leak inventory on the real Mac.
- Linux stress covers at least 20 containers plus parallel pulls/builds/execs;
  native Machines use equivalent target-qualified pressure.
