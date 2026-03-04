# Linux VM Init: Daemon/Proto Roadmap

This document defines how `vz vm linux init` evolves from host-local descriptor provisioning to daemon-owned lifecycle management.

## Current State

- `vz vm linux init` is a host-local command.
- It validates `~/.vz/linux` artifacts, provisions a sparse disk image, and writes a descriptor file.
- It does not call runtime daemon APIs.

## Target State

`vz vm linux init` should have a daemon-owned mode for reproducible, policy-governed image preparation:

- daemon validates artifact compatibility and policy gates.
- daemon manages descriptor lifecycle metadata in runtime state.
- CLI remains a transport facade that streams progress events.

## Command Contract (Target)

`vz vm linux init --name <name> [--disk-size-gb <n>] [--kernel <path>] [--initramfs <path>] [--force] [--state-db <path>]`

Behavior:

1. validate artifact inputs and checksums.
2. validate policy/capability compatibility.
3. provision/verify persistent disk path.
4. write/update Linux image descriptor record.
5. emit receipt/events and return descriptor metadata.

## Required Runtime Contract Changes

Add a dedicated service surface (stream-first UX):

1. `LinuxVmImageService.InitializeLinuxImage` (server stream)
- request:
  - `image_name`
  - `disk_size_gb`
  - `kernel_path` (optional)
  - `initramfs_path` (optional)
  - `force`
  - `metadata`
- stream events:
  - progress (`phase`, `detail`, `sequence`)
  - completion (`descriptor_path`, `disk_path`, `image_name`, `receipt_id`)

2. `LinuxVmImageService.GetLinuxImage`
3. `LinuxVmImageService.ListLinuxImages`
4. `LinuxVmImageService.DeleteLinuxImage` (for `--force`/cleanup workflows)

## Required Daemon Changes

1. Descriptor state model:
- persist image descriptor records in daemon-owned state store.
- include artifact hashes and compatibility metadata.

2. Filesystem ownership and lock discipline:
- single writer semantics through daemon transactions.
- lock/lease on image name during initialize/delete operations.

3. Receipt/event linkage:
- emit ordered progress and terminal completion.
- persist receipt and event references in one mutation flow.

## Follow-up Implementation Bead

- `vz-g4ea.4.1.1`: implement LinuxVmImageService proto + daemon handler + CLI wiring for `vz vm linux init`.
