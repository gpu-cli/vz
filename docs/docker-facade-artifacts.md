# Docker facade artifacts and lazy supervision

Status: provisioning and supervision seam implemented; VM integration and Linux VM E2E pending.

The Docker Engine compatibility tier is downstream-only. `vz run`, `vz stack`,
and `vz build` do not provision or start Docker. The future host Docker socket
proxy (`vz-7ez`) explicitly calls the guest agent's server-streamed
`EnsureDocker` RPC on first facade use.

## Persistent host layout

`ensure_docker_artifacts()` owns this private, on-disk layout:

```text
~/.vz/docker/
├── bin/
│   ├── containerd
│   ├── containerd-shim-runc-v2
│   ├── docker-init
│   ├── docker-proxy
│   └── dockerd
├── data.img
└── version.json
```

Docker Engine 29.7.2 for `linux/arm64` is pinned from Docker's official stable
static-download index (the current stable ARM64 release when checked on
2026-09-02). The archive SHA-256 and an independent SHA-256 for every installed binary are pinned. Cached files
are re-hashed on every reuse. The installer rejects symlinks, non-executable or
non-regular files, and any inventory other than the five files above. In
particular, it never extracts `runc`, `docker`, or `ctr` from the upstream
archive. Installation uses a private OS advisory lock (released by the kernel
if the installer exits), stages a complete generation, and
retains the prior generation until the metadata commit succeeds.

`data.img` is a private sparse persistent disk. Docker state is never placed in
the initramfs. The facade VM must attach this image as `/dev/vda`, expose
`~/.vz/docker/bin` through the read-only `vz-docker-bin` VirtioFS tag, and
expose the kernel artifact directory (containing youki) through the read-only
`linux-bin` tag.

## Guest supervision contract

`EnsureDocker` streams validation, startup, wait, and ready events. On its first
call, the guest agent mounts the two read-only binary shares and requires an
existing ext4 `/dev/vda` mount at `/var/lib/docker`. It then supervises external
containerd and dockerd, keeping all state and logs on that persistent mount and
all sockets under `/run/vz-docker`. Readiness requires an HTTP `GET /_ping`
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
artifact paths and invoke `EnsureDocker` from actual host socket activation.
The newly created sparse `data.img` is not automatically formatted by the
guest agent: it currently fails closed unless preformatted as ext4. Safe
one-time initialization of a proven-new disk remains unresolved.

Do not close the Docker wave beads until `scripts/run-linux-docker-e2e.sh`
passes in a local `vz`-managed Linux VM and its evidence proves that youki is
the only OCI runtime binary present in the guest.
