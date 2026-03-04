# Linux VM Init Contract

This document defines the command contract for the Linux guest image pipeline.

## Commands

- `vz vm linux init`
- `vz vm linux run`
- `vz vm linux save` (planned)
- `vz vm linux restore` (planned)

## `vz vm linux init`

Purpose:
- Initialize Linux guest image metadata and persistent disk assets.

Current CLI contract:
- `--name <name>` required logical image name
- `--output-dir <path>` target directory for image assets (default `~/.vz/images`)
- `--disk-size-gb <n>` persistent disk size in GiB (default `64`)
- `--kernel <path>` optional kernel path override
- `--initramfs <path>` optional initramfs path override
- `--force` replace existing artifacts

Behavior:
- Validates kernel/initramfs + `version.json` checksums against `~/.vz/linux`.
- Provisions sparse disk image `<output-dir>/<name>.img`.
- Writes descriptor `<output-dir>/<name>.linux.json`.
- Re-runs are idempotent when descriptor/disk match.
- `--force` replaces disk/descriptor metadata.

## `vz vm linux run`

Current CLI contract:
- `--name <name>` logical image name (descriptor file `<name>.linux.json`)
- `--descriptor <path>` optional explicit descriptor path
- `--output-dir <path>` descriptor directory when `--descriptor` is omitted
- `--cpus <n>` VM CPUs (default `2`)
- `--memory <mb>` VM memory in MB (default `2048`)
- `--cmdline <string>` optional kernel cmdline override
- `--rootfs-dir <path>` optional VirtioFS `rootfs` source
- `--mount <TAG:HOST_PATH[:ro|rw]>` additional VirtioFS mounts (repeatable)
- `--agent-timeout-secs <n>` guest-agent readiness timeout (default `30`)
- `--stop-after-ready` boot smoke mode; stop VM once guest agent is ready
- `--guest-command <shell>` execute `/bin/sh -lc <shell>` after guest-agent readiness
- `--guest-command-timeout-secs <n>` command timeout (default `900`)
- `--guest-command-user <user>` optional user for command execution

Behavior:
- Loads descriptor and validates kernel/initramfs compatibility.
- Validates persistent disk exists and matches descriptor size.
- Boots Linux guest via host Linux boot path.
- Waits for guest-agent readiness, then keeps VM running until Ctrl+C (or exits immediately in `--stop-after-ready` mode).
- When `--guest-command` is set, command output is streamed and exit code is propagated; VM is stopped after command completion.

## Canonical Workflow

```bash
# 1) Prepare descriptor + persistent disk.
vz vm linux init --name dev-linux --disk-size-gb 80

# 2) Boot from descriptor.
vz vm linux run --name dev-linux --cpus 4 --memory 8192
```

## Migration Notes

- Legacy assumption: `vz vm linux run` creates daemon-managed spaces.
- Current behavior: `vz vm linux run` is host Linux guest boot from descriptor.
- For daemon-managed space lifecycle, use:
  - `vz create` for sandbox creation
  - `vz vm linux list|inspect|attach|exec|stop|rm` for Linux-daemon-scoped operations
- Save/restore parity mapping is documented in `docs/linux-vm-save-restore-contract.md`.
- Init daemon roadmap is documented in `docs/linux-vm-init-daemon-roadmap.md`.
- Base/validate/patch parity matrix is documented in `docs/linux-vm-base-validate-patch-parity.md`.

## Backward Compatibility

- `vz vm linux e2e` remains available as a hidden legacy alias.
- New canonical test workflow is `vz vm linux test e2e`.

## Troubleshooting

- `linux image descriptor file not found`:
  - run `vz vm linux init --name <name>` first, or pass `--descriptor <path>`.
- `checksum mismatch` during init/run:
  - refresh `~/.vz/linux` artifacts or pass explicit `--kernel`/`--initramfs` matching `version.json`.
- `disk image ... exists with size ... expected ...`:
  - use matching `--disk-size-gb`, or re-run init with `--force`.
- `host boot is only supported on macOS hosts`:
  - run `vz vm linux run` from a macOS host with Virtualization.framework support.
