# Linux-Machine topology parity on Linux hosts

Depends on: macOS migration, documentation, and launch

## Purpose

Implement Linux Machines and the same multi-instance/multi-Machine Environment
topology contract on Linux hosts after Mac GA, using native isolation without a
host-root Docker dependency.

## Step 1: Implement the Linux backend

- Map each Machine plus the Environment aggregate onto the rootless
  outer-sandbox design.
- Provide per-Developer-Linux-Machine Docker, persistent state, contexts, workspace
  projections, topology networks, public-like edge, faults, peering, status,
  lifecycle, and youki-only behavior.
- Use host-neutral contract types; do not introduce a second Linux-only product model.

## Step 2: Resolve Linux-specific boundaries

- Validate cgroup v2 delegation, user namespaces, rootless overlay/fuse-overlay fallback, UID/GID mapping, SELinux/AppArmor interaction, distribution/kernel capability detection, Unix-socket permissions, and rootless networking.
- Never fall back to the host's system Docker daemon when an environment is selected.

## Step 3: Reuse the scenario suite

Run the same stable 0.4 topology, lifecycle, Docker, buildx, Compose, storage,
networking, recovery, concurrency, and isolation scenario IDs through a Linux
host driver. Host-specific assertions are additions, not replacements.

## Validation

- Release-built clean and persisted runs pass twice on supported Linux distributions/kernels.
- macOS and Linux machine-readable summaries cover the same required scenario IDs.
- No host-root daemon, cross-environment access, alternate OCI runtime, or system-Docker fallback occurs.
- Publish the supported distribution/kernel/cgroup/filesystem matrix and every intentional limitation.
