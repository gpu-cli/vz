# Linux Developer Environment Kernel Artifacts

This directory builds the Linux target used by `vz` Developer Environments.
Linux is the universal target: it is shipped through Virtualization.framework
on Apple Silicon macOS and is planned on Linux and Windows hosts. The artifacts
here are guest artifacts; host-specific backends decide how to run them.

The `developer` profile is the primary product profile:

- `out/vmlinux` for the default `developer` profile
- matching `initramfs.img`, `youki`, and `version.json`
- `developer-probe-rootfs.tar`, whose exact checksum/provenance is embedded in
  `version.json` for new Developer builds

The secondary hardened profile is built under `out/container/`. It exists for
constrained workloads and compatibility while the product converges on
Developer Environments; it is not a separate peer product.

Full Docker compatibility is roadmap work. The Developer guest now includes a
pinned, statically linked iptables legacy frontend for Docker bridge/NAT setup;
its kernel must build in the IPv4 raw table used by Docker's bridge direct-access
filtering. Kernel builds and cache checks validate the effective configuration,
and the local-Mac backend gate exercises an exact raw PREROUTING rule and its
removal. This does not establish IPv6 or full Docker parity.
The hardened Container guest intentionally does not acquire this Developer
requirement or the frontend. When complete, Docker will
be an implicit, private capability of each Developer-profile Linux Machine, never a
global daemon or a capability of native macOS/Windows targets. Both Linux
profiles use the pinned youki runtime; runc fallback is not supported.

## Quick start

On macOS, build sources and compiler workspaces must reside on a case-sensitive
Linux filesystem. Select your local Docker build context explicitly:

```bash
export YOUKI_DOCKER_CONTEXT=orbstack
export IPTABLES_DOCKER_CONTEXT=orbstack
export LINUX_DOCKER_CONTEXT=orbstack
make -C linux docker-build-all
```

Use your local context name (`default` on many Linux build hosts); this does not
change Docker's global current context. Docker is used as build infrastructure,
not as a substitute for vz-managed Machine verification. A validated cached
youki artifact can be installed without a Docker daemon. The pinned source
recipe, static dependencies and verification contract are documented in
[`youki/README.md`](youki/README.md).

Fresh iptables builds on macOS also use the explicitly selected local Linux
builder, with source extracted into its private case-sensitive filesystem.
Upstream case-distinct headers cannot be safely extracted into a default Mac
checkout. Prepare `vz-linux-builder` from `linux/Dockerfile` if it is absent;
the helper resolves and records the exact local image ID and builds offline.

Build only the primary Developer profile:

```bash
make -C linux docker-build
```

Direct `make all`/`make kernel` require case-sensitive source and build storage
and a suitable Linux cross/native toolchain. They intentionally reject the
usual case-insensitive Mac checkout. The Docker wrapper uses verified,
profile-specific Linux storage without deleting or reusing the old host source
tree. See [`SOURCE-BUILDS.md`](SOURCE-BUILDS.md) for pinned source inventories,
case-preservation checks, staged builds, and cache provenance.

On macOS, standalone `make iptables` and Developer initramfs builds also route
iptables compilation through a local Linux builder. Upstream contains
case-distinct source/header names (for example `xt_TCPMSS.h` and `xt_tcpmss.h`),
which cannot safely be extracted on the usual case-insensitive Mac filesystem.
Prepare and explicitly select the builder before those targets:

```bash
docker --context orbstack build -t vz-linux-builder linux
IPTABLES_DOCKER_CONTEXT=orbstack make -C linux iptables
```

Choose your actual local Unix-socket Docker context; there is no implicit default
daemon fallback. `IPTABLES_DOCKER_BUILDER` may select another prepared Linux/arm64
builder image. The helper resolves its immutable image ID before execution,
verifies the pinned source archive, and builds with networking disabled in the
container's private, case-sensitive `/tmp`. The checkout and archive are mounted
read-only; only the selected output directory is writable. This is build
infrastructure, not host-Docker runtime conformance evidence. Hardened builds do
not acquire iptables.

Build both distributable profiles:

```bash
make -C linux docker-build-all
```

## Kernel Profiles

Developer initramfs builds also include real, static Linux/arm64 e2fsprogs
`mke2fs` and `dumpe2fs` under `/sbin`. `make e2fsprogs` uses the same explicit
`LINUX_DOCKER_CONTEXT` on macOS. The helper pins e2fsprogs 1.47.3 to the upstream
archive SHA-256, builds offline in the local Linux builder, rejects dynamic or
foreign ELF output, and revalidates source/recipe/binary hashes on cache reuse.
The provenance record ships as `/etc/vz-e2fsprogs.json`; the initramfs digest
binds both tools and this record. Hardened initramfs builds omit these tools.

Private Docker disks are formatted only with an exact outstanding format
intent. New disks use journaled ext4 with eager inode/journal initialization;
both new and existing disks must pass a read-only feature, UUID, clean-state,
and recorded-error check before Docker admission. Existing ext2/ext3, dirty,
or corrupt disks are preserved and refused, never automatically repaired or
reformatted. A formatter build test is not persistence conformance: that still
requires the installed local-vz Machine Stop/restart workload gate.

Offline packaging tests: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover
-s linux -p test_e2fsprogs_build.py`.

| Profile | Output | Baseline | Intended use |
| --- | --- | --- | --- |
| `developer` | `linux/out/` | arm64 `defconfig` + `vz-linux.config` | **Primary.** Broad Linux Developer Environment kernel, including nested KVM, TUN/TAP, user namespaces, and pinned static iptables userspace for private Docker bridge/NAT. |
| `container` | `linux/out/container/` | `allnoconfig` + `vz-linux-container.config` | **Secondary hardened option.** Constrained workload kernel with virtio/vsock/virtiofs, overlayfs, netns, seccomp, io_uring, btrfs snapshots, and kernel NFS server support. Docker compatibility is not promised for this profile. |

The secondary hardened profile intentionally does not expose `/proc/config.gz`
(`IKCONFIG`) and does not include nested virtualization, TUN/TAP, USB gadget,
SCSI/ATA, NFS client support, 9p, SquashFS, or FAT/VFAT.

Release CI caches each profile kernel image separately from the initramfs and
metadata. Normal `vz` releases rebuild the guest agent/initramfs and regenerate
`version.json`, but only recompile a profile kernel image when that profile's
kernel config, `kernel-version.mk`, source/build recipes, or Docker build
environment changes. Cached images require matching, validated `.build.json`
provenance; an image's presence or timestamp alone is not sufficient.

## Profile selection API

The installer lays out release artifacts as:

- `~/.vz/linux/developer/` for the broad developer profile
- `~/.vz/linux/container/` for the constrained container profile
- `~/.vz/linux/` as a legacy developer-profile default

Rust callers should select intent with `KernelProfile` and use capabilities as
additional validation:

- `vz_linux::ensure_kernel_profile(KernelProfile::Developer)`
- `vz_linux::ensure_kernel_profile(KernelProfile::Container)`
- `vz_linux::ensure_kernel_bundle(KernelBundleOptions { profile: Some(...), required_capabilities: ..., ..Default::default() })`

OCI runtime callers can set `RuntimeConfig::linux_profile`. The current 0.4
development CLI exposes only `up`, `exec`, `status`, `stop`, and `delete`;
`vz vm` and the legacy backend command families are rejected, not hidden aliases.
The ProjectDefinition carries each Machine's explicit Developer/Hardened profile,
and those five lifecycle commands operate on the topology. This CLI shape does
not imply that the full 0.4 release gate has passed.

## Benchmark boot latency

```bash
cd crates
cargo run -p vz-linux --bin vz-linux-bench -- \
  --bundle-dir ../linux/out \
  --iterations 10 \
  --timeout-secs 8 \
  --guest-logs \
  --http-smoke-url http://example.com/
```

Useful benchmark flags:

- `--guest-logs` captures `dmesg | tail -n 120` after each run.
- `--http-smoke-url URL` runs a curl-like HTTP smoke check in guest via BusyBox `wget`.
- `--guest-log-command "..."` captures a custom guest command via `sh -lc`.
- `--retry-log-every N` prints a readiness heartbeat every N retries.
- `--rootfs-dir PATH` mounts a host rootfs directory via VirtioFS tag `rootfs` and benchmarks overlay+chroot rootfs boot mode.

## Inputs

- `vz-linux.config` developer kernel config fragment
- `vz-linux-container.config` container kernel config fragment
- `kernel-version.mk` shared kernel version/cache schema
- `youki/` checksum-pinned source, Rust/native build inputs and static binary
  verification, with explicit cgroup-v2, device-filter and seccomp features
- pinned netfilter.org iptables source (Developer profile only; archive SHA-256
  is verified before its static legacy frontend is built)
- `initramfs/` template files (`init`, `resolv.conf`, `udhcpc.script`)
- `crates/vz-guest-agent` binary (cross-compiled for Linux)

## Output compatibility

`version.json` includes guest-agent, pinned `youki`, and profile-qualified
iptables version metadata,
artifact SHA256 checksums, the kernel `profile`, a `security_profile`, and
declared kernel capabilities (`vsock`, `virtiofs`, `hvc0_serial`, `ext4_root`,
`overlayfs`, `netns`, `seccomp`, `io_uring`, `btrfs_snapshots`,
`device_mapper`, `dm_crypt`, `nfsd`, etc.).
`vz-linux::ensure_kernel()` uses the version metadata to reject mismatched
artifact sets and verifies SHA256 checksums when present.
`vz-linux::ensure_kernel_bundle()` additionally lets external callers choose
the install directory and require specific kernel capabilities before booting
their own rootfs.

## Offline Developer startup input

Normal Developer builds also run `developer-probe.py` against the exact pinned,
provenance-verified static Linux/arm64 BusyBox binary. It creates a deterministic
USTAR rootfs with UID/GID/mtime zero, only BusyBox and fixed relative applet links,
a writable temporary directory, and the public marker
`/etc/vz-developer-probe` (`vz-developer-probe-v1\n`). It contains no OCI runtime,
Docker daemon, package manager downloads, credentials, or mutable image reference.

The build's `developer-probe.json` sidecar is embedded as `developer_probe` in
`version.json`, recording the rootfs checksum, BusyBox checksum/version, pinned
source archive/inventory digests, exact build-provenance checksum and marker
checksum. The existing appliance digest already hashes the exact version bytes;
the read-only bundle verifier additionally verifies the declared archive.
Normal installation and exactly-owned Machine artifact pinning copy it and
verify it again. Recovery uses only the retained pin, not the original catalog
or another Machine's files. A typed `VerifiedDeveloperProbe` exposes the pinned
archive path and expected hash to the startup adapter.

The primary use is an offline, Machine-scoped startup usability check driven by
the host's unmodified Docker/Compose/buildx clients. The rootfs can be imported
through the exact Machine endpoint, after which workloads use that import's
returned immutable image ID. BuildKit can use `FROM scratch` and a checked public
payload without pulling a base. This small probe does **not** certify full
Docker compatibility, cross-Machine isolation, or the 63-scenario release lane.
Legacy bundles may lack this declaration; they do not thereby gain successful
Developer readiness. Hardened bundles omit the startup-probe declaration and
must not acquire Developer/Docker behavior through it.

Offline packaging tests:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s linux -p test_developer_probe.py
```
