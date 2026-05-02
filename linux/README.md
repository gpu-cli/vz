# Linux Kernel Artifacts

This directory builds the Linux boot artifacts used by `vz-linux`:

- `out/vmlinux`
- `out/initramfs.img`
- `out/youki`
- `out/version.json`

## Quick start

```bash
make -C linux all
```

If your host toolchain does not have ARM64 + musl cross support:

```bash
make -C linux docker-build
```

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

- `vz-linux.config` kernel config fragment
- `initramfs/` template files (`init`, `resolv.conf`, `udhcpc.script`)
- `crates/vz-guest-agent` binary (cross-compiled for Linux)

## Output compatibility

`version.json` includes guest-agent and pinned `youki` version metadata,
artifact SHA256 checksums, and declared kernel capabilities (`vsock`,
`virtiofs`, `hvc0_serial`, `ext4_root`). `vz-linux::ensure_kernel()` uses the
version metadata to reject mismatched artifact sets and verifies SHA256
checksums when present. `vz-linux::ensure_kernel_bundle()` additionally lets
external callers choose the install directory and require specific kernel
capabilities before booting their own rootfs.
