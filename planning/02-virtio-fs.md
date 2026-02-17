# VirtioFS Mount Strategy

## What is VirtioFS

VirtioFS is a paravirtualized filesystem protocol, part of the virtio specification, designed to share directories between a host and a guest virtual machine. Unlike network-based file sharing (NFS, SMB), VirtioFS operates over the virtio transport layer, giving it near-native I/O performance without the overhead of a network stack.

The key properties:

- **Paravirtualized**: the guest knows it is in a VM and cooperates with the hypervisor for optimal performance, rather than emulating a physical disk or network share.
- **POSIX semantics**: supports the full range of filesystem operations — symlinks, hardlinks, xattrs, mmap, locking — unlike 9p or plan9 which have well-known semantic gaps.
- **Shared page cache**: the host and guest can share page cache entries, avoiding double-caching that plagues NFS/SMB mounts.

## Apple's Implementation

Apple's Virtualization.framework provides first-class VirtioFS support through these types:

| Type | Role |
|------|------|
| `VZVirtioFileSystemDeviceConfiguration` | Top-level device config, holds a tag and a share |
| `VZSharedDirectory` | Points to a host directory, with read-only flag |
| `VZSingleDirectoryShare` | Wraps one VZSharedDirectory |
| `VZMultipleDirectoryShare` | Maps multiple VZSharedDirectory instances by name |
| `VZDirectorySharingDeviceConfiguration` | Protocol that VZVirtioFileSystemDeviceConfiguration conforms to |

A share is attached to a `VZVirtualMachineConfiguration` before the VM starts.

## Performance

VirtioFS achieves **75-95% of native I/O** depending on workload:

- Sequential reads/writes: ~95% of native (large block sizes amortize the virtio transport overhead)
- Random small I/O: ~75-80% of native (each operation crosses the host-guest boundary)
- Metadata operations (stat, readdir): ~80-85% of native

This is dramatically better than NFS (~40-60%) or 9p (~30-50%) for development workloads that are metadata-heavy (compilation, git operations, node_modules traversal).

## Critical Constraint: Static Mounts

**Mounts are configured at VM creation time and cannot be added or removed while the VM is running.** This is a hard limitation of Apple's Virtualization.framework — there is no API to hot-add or hot-remove a `VZVirtioFileSystemDeviceConfiguration` after `VZVirtualMachine.start()`.

This means:

- You cannot mount a new project directory into a running VM
- You cannot unmount a directory without stopping the VM
- The set of shared directories is fixed for the VM's lifetime

## Solution: Workspace Root Pattern

Instead of mounting individual project directories, mount a single **workspace root** directory and scope access per-session at the application layer.

```
Host:   ~/workspace/
          ├── project-a/
          ├── project-b/
          └── project-c/

Guest:  /mnt/workspace/        ← single VirtioFS mount
          ├── project-a/       ← session 1 scoped here
          ├── project-b/       ← session 2 scoped here
          └── project-c/
```

The sandbox layer (vz-sandbox) validates that a session's `project_dir` falls under the mounted workspace root. The guest agent sets the working directory to the appropriate subdirectory. The VM itself has access to the full workspace root, but each session is constrained by the application layer.

This pattern avoids the static mount limitation entirely — one mount serves all projects.

## Guest-Side Mounting

### Manual mount

```bash
mount -t virtiofs workspace /mnt/workspace
```

The tag (`workspace`) must match the tag configured on the host side.

### Automount via /etc/fstab

Add to the guest's `/etc/fstab`:

```
workspace /mnt/workspace virtiofs rw,nofail 0 0
```

The `nofail` flag prevents boot failure if the mount is not available (useful for golden images that may run without a share attached).

### macOS guest note

On macOS guests, VirtioFS mounts appear automatically under `/Volumes/<tag>` without explicit mount commands. The guest kernel handles this via the VirtioFS kext. For consistency, the guest agent should resolve the actual mount point at startup.

## Read-Only vs Read-Write Mounts

```rust
SharedDirConfig {
    tag: "workspace",
    source: PathBuf::from("/Users/dev/workspace"),
    read_only: false,  // read-write: agent can modify files
}

SharedDirConfig {
    tag: "tools",
    source: PathBuf::from("/usr/local/share/vz-tools"),
    read_only: true,   // read-only: immutable toolchain/SDK
}
```

Use read-only mounts for:
- Tool binaries, SDKs, shared caches
- Reference data that should not be modified

Use read-write mounts for:
- Project workspace directories (the agent needs to write build artifacts, modify files)

## Configuration in VmConfigBuilder

The Rust-side configuration:

```rust
.shared_dir(SharedDirConfig {
    tag: "workspace",
    source: PathBuf::from("/Users/dev/workspace"),
    read_only: false,
})
```

This translates to the following ObjC bridging code that sets up the Virtualization.framework objects:

```objc
// Create the shared directory pointing to the host path
VZSharedDirectory *sharedDir = [[VZSharedDirectory alloc]
    initWithURL:[NSURL fileURLWithPath:@"/Users/dev/workspace"]
    readOnly:NO];

// Wrap it in a single directory share
VZSingleDirectoryShare *share = [[VZSingleDirectoryShare alloc]
    initWithDirectory:sharedDir];

// Create the VirtioFS device configuration with the tag
VZVirtioFileSystemDeviceConfiguration *fsConfig =
    [[VZVirtioFileSystemDeviceConfiguration alloc] initWithTag:@"workspace"];
fsConfig.share = share;

// Attach to the VM configuration
vmConfig.directorySharingDevices = @[fsConfig];
```

For multiple directories, use `VZMultipleDirectoryShare`:

```objc
VZMultipleDirectoryShare *multiShare = [[VZMultipleDirectoryShare alloc]
    initWithDirectories:@{
        @"workspace": workspaceDir,
        @"tools": toolsDir,
    }];
```

## Automount in Guest via /etc/fstab

The golden VM image should have `/etc/fstab` pre-configured:

```
# VirtioFS mounts from host
workspace    /mnt/workspace    virtiofs    rw,nofail    0 0
```

And the mount point created:

```bash
sudo mkdir -p /mnt/workspace
```

This ensures the workspace is available immediately after boot without any manual intervention or guest agent involvement.

## Security

VirtioFS provides filesystem-level isolation:

- **The guest cannot escape the mounted directory.** The VirtioFS daemon on the host (handled by Virtualization.framework internally) enforces that all paths resolve within the shared directory. Path traversal attacks (e.g., symlinks pointing outside the share) are blocked.
- **Permission mapping**: the guest sees files with the UID/GID of the user running the VM process on the host. There is no UID remapping.
- **No network exposure**: unlike NFS/SMB, VirtioFS operates over the virtio transport — there is no network port to attack.
- **Mount scope**: each VM only sees the directories explicitly shared with it. Other host directories are completely invisible to the guest.

The combination of the Workspace Root Pattern and read-only/read-write flags gives fine-grained control: the workspace is read-write for development, while tool directories can be mounted read-only to prevent tampering.
