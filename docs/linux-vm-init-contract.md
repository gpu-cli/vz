# Linux VM Init Contract

This document defines the command contract for the Linux guest image pipeline.

## Commands

- `vz vm linux init`
- `vz vm linux run`

## `vz vm linux init`

Purpose:
- Reserve and initialize Linux guest image metadata and persistent disk assets.

Current CLI contract:
- `--name <name>` required logical image name
- `--output-dir <path>` target directory for image assets (default `~/.vz/images`)
- `--disk-size-gb <n>` persistent disk size in GiB (default `64`)
- `--kernel <path>` optional kernel path override
- `--initramfs <path>` optional initramfs path override
- `--force` replace existing artifacts

Behavior notes:
- Existing implementation currently returns an explicit not-yet-implemented error.
- Follow-up implementation is tracked in `vz-t8zg.2`, `vz-t8zg.3`, and `vz-t8zg.4`.

## `vz vm linux run`

Current behavior:
- Creates/runs Linux spaces through daemon-owned sandbox APIs.

Planned behavior extension:
- Support host Linux boot mode using `vz-linux` artifacts and persistent disk provisioned by `init`.
- Validate descriptor compatibility before boot.

## Backward Compatibility

- `vz vm linux e2e` remains available as a hidden legacy alias.
- New canonical test workflow is `vz vm linux test e2e`.
