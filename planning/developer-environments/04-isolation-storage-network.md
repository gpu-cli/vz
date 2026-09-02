# Environment isolation, storage, and networking

Depends on: First-class Developer Environment identity and lifecycle

## Purpose

Make parallel Developer Environments reproducible and independent under realistic native and Docker-capable workloads.

## Step 1: Persistent storage ownership

- Give each environment independent target image, writable disk, service, and workspace state. Linux targets additionally own independent BuildKit, image, and volume state.
- Support target-native persistent storage and explicitly approved host shares. Linux Docker named volumes and host binds document consistency and UID/GID semantics; native targets document their own permission model.
- Define typed native `HostPath` and `TargetPath` plus a logical workspace root. Host-relative sources use native host semantics; target paths use the selected target's native semantics; Share Backends perform authorized translation. Preserve spaces, Unicode, case, executable/ACL, symlink/junction, and file-watching behavior as declared capabilities.
- Validate disk growth, disk-full recovery, interrupted writes, cleanup, and restart persistence.

## Step 2: Environment-scoped networking

- Define target-neutral DNS, TCP/UDP egress, host reachability, and published-port behavior. Linux backends additionally complete bridge forwarding, MASQUERADE, and container-to-host access.
- Linux Docker targets support conventional `host.docker.internal` behavior while retaining documented `host.vz.internal` compatibility.
- Keep gateway addresses and NAT implementation out of public configuration so Linux and Windows can provide the same logical behavior differently.
- Maintain an environment-owned port registry with deterministic collision errors and no default LAN exposure.

## Step 3: Parallel isolation

- Separate routes, DNS, port forwards, credentials, mounts, caches, services, and event streams. Linux targets additionally separate Docker networks, images, volumes, and contexts.
- Prevent cross-environment socket access and unauthorized bind paths.
- Ensure failure, stop, restart, or deletion of one environment cannot disturb another.

## Step 4: Resource behavior

Enforce and inspect CPU, memory, PID, and file-descriptor limits. Confirm controlled OOM and daemon failure do not kill unrelated environments or corrupt persistent state.

## Validation

- Cross-target and same-target two-environment suites cover native processes/services, storage, networks, endpoints, ports, events, and credentials; Linux pairs also cover containers, images, volumes, and contexts.
- Linux stress covers at least 20 concurrent containers plus parallel pulls/builds/execs; native-target stress uses equivalent target-qualified workloads.
- Real Mac port publishing, host reachability, egress, DNS, storage, and restart tests.
