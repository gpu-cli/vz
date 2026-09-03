# Linux Machine Docker artifacts and lazy supervision

Status: **DEV** — shared artifact provisioning and supervision seam implemented;
per-Linux-Machine state migration, endpoint integration, and Linux VM E2E pending.

Docker compatibility is an implicit capability of each Linux Developer Machine.
Reconciliation provisions or verifies its requirements; the Machine's private
Engine may still start lazily on first API traffic. The
host endpoint adapter (`vz-7ez`) calls the guest agent's server-streamed
`EnsureDocker` RPC for the selected Environment/Machine. Native macOS and native
Windows Machines do not use this contract.

## Host artifact and Machine-state layout

`ensure_docker_artifacts()` owns the checksum-pinned, immutable binary cache.
Mutable state must be owned by stable Environment and Machine identities rather than the
shared artifact directory:

```text
~/.vz/docker/
├── bin/
│   ├── containerd
│   ├── containerd-shim-runc-v2
│   ├── docker-init
│   ├── docker-proxy
│   └── dockerd
└── version.json

<daemon-owned-environment-root>/<environment-id>/machines/<machine-id>/docker/
└── data.img
```

Physical state and endpoint paths are backend-owned details. Users address the
Environment/Machine by name/ID or managed context and do not construct paths.

Docker Engine 29.7.2 for `linux/arm64` is pinned from Docker's official stable
static-download index (the current stable ARM64 release when checked on
2026-09-02). The archive SHA-256 and an independent SHA-256 for every installed binary are pinned. Cached files
are re-hashed on every reuse. The installer rejects symlinks, non-executable or
non-regular files, and any inventory other than the five files above. In
particular, it never extracts `runc`, `docker`, or `ctr` from the upstream
archive. Installation uses a private OS advisory lock (released by the kernel
if the installer exits), stages a complete generation, and
retains the prior generation until the metadata commit succeeds.

Each Linux Machine's `data.img` is a private sparse persistent disk. Docker state
is never placed in the initramfs or shared with another Machine. Its backend
must attach the owned image as `/dev/vda`, expose
`~/.vz/docker/bin` through the read-only `vz-docker-bin` VirtioFS tag, and
expose the kernel artifact directory (containing youki) through the read-only
`linux-bin` tag.

## Guest supervision contract

`EnsureDocker` streams validation, startup, wait, and ready events for one
Environment/Machine. On first call, the guest agent mounts the two read-only
binary shares and requires that Machine's existing ext4 `/dev/vda` mount at
`/var/lib/docker`. It then supervises external containerd and dockerd, keeping
all state and logs on that persistent mount and all sockets under
`/run/vz-docker`. Readiness requires an HTTP `GET /_ping`
against the Engine Unix socket to return status 200 with body `OK`.

The guest refuses startup unless:

- the Docker binary share contains exactly the five allowlisted files;
- `/mnt/linux-bin/youki` exists;
- the containerd daemon plumbing config and Docker daemon JSON config are
  regular, non-symlink files;
- the Docker daemon JSON config does not add or override runtimes; and
- `/var/lib/docker` is an ext4 virtio-block mount, not initramfs storage.

The shim name `containerd-shim-runc-v2` describes containerd's protocol and is
not an installed OCI runtime. Runtime selection is not inferred from
containerd's CRI-only TOML runtime tables. Instead the supervisor starts
dockerd with immutable `--add-runtime youki=/mnt/linux-bin/youki` and
`--default-runtime youki` arguments. Moby translates that registered runc
drop-in into the runc-v2 shim's `BinaryName` task option. Dockerd is also
pointed explicitly at the persistent `/var/lib/docker/config/daemon.json`, so
an initramfs configuration cannot silently supply another runtime.

## Pending integration and evidence

`vz-k3v` must materialize the containerd daemon plumbing configuration and a
Docker daemon JSON config without runtime keys. `vz-7ez` must attach/mount the
selected Environment/Machine's artifact paths and invoke `EnsureDocker` from
its private endpoint activation. Existing single-image state must migrate to
Machine-owned Docker storage. A newly created sparse `data.img` is not
automatically formatted by the guest agent: it currently fails closed unless
preformatted as ext4. Safe one-time initialization of a proven-new disk remains
unresolved.

Do not close the Docker wave beads until `scripts/run-linux-docker-e2e.sh`
passes in a local `vz`-managed Linux VM and its evidence proves that youki is
the only OCI runtime binary present in the guest.
