# Linux-on-Linux Developer Environment parity

Depends on: macOS migration, documentation, and launch

## Purpose

Implement the universal Linux-target Developer Environment on Linux hosts after Mac GA, using Linux-native isolation without a host-root Docker dependency.

## Step 1: Implement the Linux backend

- Map the durable environment aggregate onto the rootless outer-sandbox design.
- Provide the same workspace container, implicit private Docker Engine, persistent state, managed Docker context, networking, ports, mounts, status, stop/restart/delete semantics, and youki-only runtime invariant.
- Use host-neutral contract types; do not introduce a second Linux-only product model.

## Step 2: Resolve Linux-specific boundaries

- Validate cgroup v2 delegation, user namespaces, rootless overlay/fuse-overlay fallback, UID/GID mapping, SELinux/AppArmor interaction, distribution/kernel capability detection, Unix-socket permissions, and rootless networking.
- Never fall back to the host's system Docker daemon when an environment is selected.

## Step 3: Reuse the scenario suite

Run the same lifecycle, Docker, buildx, Compose, storage, networking, recovery, concurrency, and isolation scenarios through a Linux host driver. Host-specific assertions are capability-labelled additions, not replacements for shared scenarios.

## Validation

- Release-built clean and persisted runs pass twice on supported Linux distributions/kernels.
- macOS and Linux machine-readable summaries cover the same required scenario IDs.
- No host-root daemon, cross-environment access, alternate OCI runtime, or system-Docker fallback occurs.
- Publish the supported distribution/kernel/cgroup/filesystem matrix and every intentional limitation.
