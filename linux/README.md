# Linux Kernel Artifacts

This directory builds the Linux boot artifacts used by `vz-linux`:

- `out/vmlinux` for the default `developer` profile
- `out/container/vmlinux` for the constrained `container` profile
- matching `initramfs.img`, `youki`, and `version.json` files in each bundle

## Quick start

```bash
make -C linux all
```

Build the constrained container-sandbox profile:

```bash
make -C linux KERNEL_PROFILE=container all
```

If your host toolchain does not have ARM64 + musl cross support:

```bash
make -C linux docker-build
```

Build both distributable profiles:

```bash
make -C linux docker-build-all
```

## Kernel Profiles

| Profile | Output | Baseline | Intended use |
| --- | --- | --- | --- |
| `developer` | `linux/out/` | arm64 `defconfig` + `vz-linux.config` | Broad dev/host VM kernel, including nested KVM and TUN/TAP for Virgil's Firecracker host path. |
| `container` | `linux/out/container/` | `allnoconfig` + `vz-linux-container.config` | Deployed container/sandbox VM kernel with virtio/vsock/virtiofs, overlayfs, netns, seccomp, io_uring, btrfs snapshots, and kernel NFS server support. |

The container profile intentionally does not expose `/proc/config.gz`
(`IKCONFIG`) and does not include nested virtualization, TUN/TAP, USB gadget,
SCSI/ATA, NFS client support, 9p, SquashFS, or FAT/VFAT.

Release CI caches each profile kernel image separately from the initramfs and
metadata. Normal `vz` releases rebuild the guest agent/initramfs and regenerate
`version.json`, but only recompile a profile kernel image when that profile's
kernel config, `kernel-version.mk`, or Docker build environment changes.

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

OCI runtime callers can set `RuntimeConfig::linux_profile`. CLI users can pass
`--kernel-profile developer|container` on OCI commands and `vz vm linux init`.

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
- `initramfs/` template files (`init`, `resolv.conf`, `udhcpc.script`)
- `crates/vz-guest-agent` binary (cross-compiled for Linux)

## Output compatibility

`version.json` includes guest-agent and pinned `youki` version metadata,
artifact SHA256 checksums, the kernel `profile`, a `security_profile`, and
declared kernel capabilities (`vsock`, `virtiofs`, `hvc0_serial`, `ext4_root`,
`overlayfs`, `netns`, `seccomp`, `io_uring`, `btrfs_snapshots`, `nfsd`, etc.).
`vz-linux::ensure_kernel()` uses the version metadata to reject mismatched
artifact sets and verifies SHA256 checksums when present.
`vz-linux::ensure_kernel_bundle()` additionally lets external callers choose
the install directory and require specific kernel capabilities before booting
their own rootfs.
