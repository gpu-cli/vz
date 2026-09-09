//! Apply declared Environment-owned storage: block volumes and shared caches.
//!
//! This is the storage half of the workspace-and-storage policy;
//! [`super::workspace_projection`] is the workspace half, and the two share the
//! multi-attach rule through
//! [`workspace_projection::first_writable_multi_attach`].
//!
//! **The refusal that matters.** A [`VolumeKind::Block`] volume is one sparse
//! disk image carrying one ext4 filesystem. ext4 is not a cluster filesystem: a
//! second guest mounting the same image read-write corrupts it, and neither
//! kernel notices, because each has its own journal and its own idea of the
//! free-space bitmaps. So a writable block volume attached to more than one
//! Machine is refused. The refusal is made in [`refuse_unsupported_volumes`],
//! which `environment_up::validate_supported` calls before
//! `reserve_environment_up_admission`, so nothing has been reserved, no image
//! has been allocated and no directory has been created when it fires. That
//! ordering is the claim: "rejected *before* mutation".
//!
//! **Why a shared cache is not the same case.** A [`VolumeKind::SharedCache`]
//! is one host directory exported to every attached Machine over its own
//! VirtioFS device. There is no shared block layer to corrupt; the host
//! filesystem serialises the writes and each guest runs an ordinary FUSE client
//! against it. Multi-attach is the point rather than the hazard. What the
//! carrier does not give for free is *when* one Machine observes another's
//! write, which is why the declaration has to state a bound
//! (`SharedCacheConsistency`) rather than leave it implied.
//!
//! **Ordering.** Both carriers are fixed when `LinuxVm::create` runs — a
//! VirtioFS share and a virtio-block device are configured before boot and
//! cannot be added afterwards — so every volume is materialised on the host
//! before the boot loop, exactly as `install_environment_fabric` mints switch
//! ports and `workspace_projection` resolves shares before it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use vz_runtime_contract::{
    EnvironmentId, EnvironmentSpec, MachineId, MachineInstance, MachineProfile, OperatingSystem,
    StackVolumeMount, VolumeAccessMode, VolumeAttachment, VolumeId, VolumeInstance, VolumeKind,
    VolumeSpec,
};

use super::workspace_projection;

/// Why a declared volume could not be applied.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VolumeError {
    #[error(
        "volume `{volume}` is a writable block device attached to Machines `{first}` and `{second}`; a block volume carries one ext4 filesystem, which has exactly one writer, so at most one Machine may attach it unless every attachment is read-only"
    )]
    WritableBlockMultiAttach {
        volume: String,
        first: String,
        second: String,
    },
    #[error(
        "volume `{volume}` is attached to Machine `{machine}`, which is not a Developer Linux Machine; only a Developer Linux Machine carries a declared volume"
    )]
    UnsupportedMachine { volume: String, machine: String },
    #[error("volume `{volume}` names Machine `{machine}`, which this Environment does not have")]
    UnknownMachine { volume: String, machine: String },
    #[error("volume `{volume}` storage at `{path}` could not be prepared: {reason}")]
    StorageUnavailable {
        volume: String,
        path: String,
        reason: String,
    },
    #[error("volume `{volume}` was declared but no persisted instance carries that name")]
    UnresolvedInstance { volume: String },
}

/// Refuse every declared volume this Up cannot serve, before any mutation.
///
/// Declaration-only: it reads the definition and touches neither the filesystem
/// nor the state store, which is what lets `validate_supported` call it ahead of
/// admission.
pub fn refuse_unsupported_volumes(spec: &EnvironmentSpec) -> Result<(), VolumeError> {
    let machines: BTreeMap<&str, (MachineProfile, OperatingSystem)> = spec
        .machines
        .iter()
        .map(|machine| {
            (
                machine.name.as_str(),
                (machine.profile, machine.target.os),
            )
        })
        .collect();
    for volume in &spec.volumes {
        for attachment in &volume.attachments {
            // `EnvironmentSpec::validate` already refuses an attachment naming
            // an undeclared Machine. Repeating it is not redundant: this
            // function is also the guard for a request that reached the daemon
            // over gRPC, where the definition is decoded rather than validated
            // by the CLI, and an unmatched name here would otherwise become an
            // index panic below.
            let Some((profile, os)) = machines.get(attachment.machine.as_str()).copied() else {
                return Err(VolumeError::UnknownMachine {
                    volume: volume.name.clone(),
                    machine: attachment.machine.clone(),
                });
            };
            // A volume is carried by a VirtioFS share or a virtio-block device
            // that `boot_or_inspect_machine` hands to the Linux backend as a
            // `StackResourceHint`. The native macOS backend receives no hint at
            // all, so admitting one there would boot a Machine whose declared
            // storage silently never appears. Hardened is the restricted
            // profile and declares none of this topology.
            if os != OperatingSystem::Linux || profile != MachineProfile::Developer {
                return Err(VolumeError::UnsupportedMachine {
                    volume: volume.name.clone(),
                    machine: attachment.machine.clone(),
                });
            }
        }
        if volume.kind != VolumeKind::Block {
            continue;
        }
        // Every pair of attachments to one block volume names the one image, so
        // "overlaps" is unconditionally true here. The shared rule then reduces
        // to exactly the criterion's clause: a writer admits no second
        // attachment.
        if let Some((first, second)) = workspace_projection::first_writable_multi_attach(
            &volume.attachments,
            |attachment: &VolumeAttachment| attachment.mode.writes(),
            |_, _| true,
        ) {
            return Err(VolumeError::WritableBlockMultiAttach {
                volume: volume.name.clone(),
                first: first.machine.clone(),
                second: second.machine.clone(),
            });
        }
    }
    Ok(())
}

/// Root of every volume this Environment owns.
///
/// Keyed by the Environment id and never by a Machine id: a shared cache spans
/// several Machines and a block volume outlives the incarnation that mounted
/// it, so putting the storage under a Machine's directory would destroy it when
/// that Machine went away. Delete reclaims this whole subtree, which is why
/// nothing outside the Environment may ever be written into it.
pub fn environment_volume_root(data_dir: &Path, environment_id: &EnvironmentId) -> PathBuf {
    data_dir.join("volumes").join(environment_id.to_string())
}

/// One volume's own directory, under [`environment_volume_root`].
pub fn volume_directory(
    data_dir: &Path,
    environment_id: &EnvironmentId,
    volume_id: &VolumeId,
) -> PathBuf {
    environment_volume_root(data_dir, environment_id).join(volume_id.to_string())
}

/// The sparse image backing a `block` volume.
pub fn block_image_path(
    data_dir: &Path,
    environment_id: &EnvironmentId,
    volume_id: &VolumeId,
) -> PathBuf {
    volume_directory(data_dir, environment_id, volume_id).join("image.img")
}

/// The host directory backing a `shared_cache` volume.
pub fn shared_cache_path(
    data_dir: &Path,
    environment_id: &EnvironmentId,
    volume_id: &VolumeId,
) -> PathBuf {
    volume_directory(data_dir, environment_id, volume_id).join("cache")
}

/// VirtioFS tag prefix for a shared cache share.
///
/// `stack_vm` strips `vz-mount-` to build the `vz.mount.{N}={guest_path}` kernel
/// argument `linux/initramfs/init` parses, so the tag has to keep that shape or
/// the guest never bind-mounts the share. Workspace projections and shared
/// caches therefore draw indices from one sequence per Machine, which
/// [`resolve_environment_volumes`] takes as its `next_index` argument.
fn mount_tag(index: usize) -> String {
    format!("vz-mount-{index}")
}

/// A block volume that must be attached to one Machine as a virtio-block device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBlockVolume {
    /// Stable device id, used for host-side diagnostics only; the guest sees
    /// `/dev/vdX` by attachment order.
    pub id: String,
    pub host_path: PathBuf,
    /// Absolute path inside the Machine where the filesystem is mounted.
    pub guest_path: String,
    pub read_only: bool,
    /// Guest-visible size, used once when the image is first created.
    pub size_bytes: u64,
}

/// Everything one Machine's boot needs from the declared volumes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MachineVolumeAttachments {
    /// Shared caches, carried by the existing VirtioFS mount carrier.
    pub shares: Vec<StackVolumeMount>,
    /// Block volumes, carried as ordered virtio-block devices.
    pub blocks: Vec<ResolvedBlockVolume>,
}

/// Create every declared volume's host storage and map it onto each Machine.
///
/// `next_index` is the first free VirtioFS mount index for each Machine, so a
/// Machine that also has a workspace projection does not have its share
/// overwritten by a cache sharing tag `vz-mount-0`.
///
/// Creation is idempotent: a second Up over the same Environment finds the image
/// and the directory already there and leaves their contents alone, which is
/// what makes a volume survive `vz stop` and `vz up`.
pub fn resolve_environment_volumes(
    spec: &EnvironmentSpec,
    instances: &[VolumeInstance],
    machines: &[MachineInstance],
    data_dir: &Path,
    environment_id: &EnvironmentId,
    next_index: &BTreeMap<MachineId, usize>,
) -> Result<BTreeMap<MachineId, MachineVolumeAttachments>, VolumeError> {
    let mut resolved: BTreeMap<MachineId, MachineVolumeAttachments> = BTreeMap::new();
    let mut index: BTreeMap<MachineId, usize> = next_index.clone();
    for volume in &spec.volumes {
        let Some(instance) = instances
            .iter()
            .find(|candidate| candidate.name == volume.name)
        else {
            // A declared volume with no persisted identity has no directory to
            // live in, because the directory is named by the identity. Refusing
            // is the only honest outcome: silently minting one here would create
            // storage the ownership graph does not account for and Delete would
            // then leak.
            return Err(VolumeError::UnresolvedInstance {
                volume: volume.name.clone(),
            });
        };
        let directory = volume_directory(data_dir, environment_id, &instance.volume_id);
        create_directory(&volume.name, &directory)?;
        for attachment in &volume.attachments {
            let Some(machine) = machines
                .iter()
                .find(|machine| machine.name == attachment.machine)
            else {
                return Err(VolumeError::UnknownMachine {
                    volume: volume.name.clone(),
                    machine: attachment.machine.clone(),
                });
            };
            let entry = resolved.entry(machine.machine_id.clone()).or_default();
            match volume.kind {
                VolumeKind::SharedCache => {
                    let host_path = shared_cache_path(data_dir, environment_id, &instance.volume_id);
                    create_directory(&volume.name, &host_path)?;
                    let slot = index.entry(machine.machine_id.clone()).or_insert(0);
                    entry.shares.push(StackVolumeMount {
                        tag: mount_tag(*slot),
                        host_path,
                        guest_path: Some(attachment.target_path.clone()),
                        read_only: attachment.mode == VolumeAccessMode::ReadOnly,
                    });
                    *slot += 1;
                }
                VolumeKind::Block => {
                    let host_path = block_image_path(data_dir, environment_id, &instance.volume_id);
                    let size_bytes = volume.size_bytes.ok_or_else(|| {
                        // `validate_volume` requires it; reaching here means the
                        // definition bypassed validation, and guessing a size
                        // would silently create the wrong device.
                        VolumeError::StorageUnavailable {
                            volume: volume.name.clone(),
                            path: host_path.display().to_string(),
                            reason: "`block` volume carries no `size_bytes`".to_string(),
                        }
                    })?;
                    ensure_sparse_image(&volume.name, &host_path, size_bytes)?;
                    entry.blocks.push(ResolvedBlockVolume {
                        id: format!("volume-{}", instance.volume_id),
                        host_path,
                        guest_path: attachment.target_path.clone(),
                        read_only: attachment.mode == VolumeAccessMode::ReadOnly,
                        size_bytes,
                    });
                }
            }
        }
    }
    Ok(resolved)
}

fn create_directory(volume: &str, path: &Path) -> Result<(), VolumeError> {
    std::fs::create_dir_all(path).map_err(|error| VolumeError::StorageUnavailable {
        volume: volume.to_string(),
        path: path.display().to_string(),
        reason: error.to_string(),
    })
}

/// Create the sparse image if it is absent, and leave an existing one alone.
///
/// An existing image is never resized or truncated. Growing it would be a
/// silent reformat from the guest's point of view (ext4 does not follow the
/// device), and shrinking it would destroy data, so a declaration whose size
/// changed is applied to new Environments only. The file is created with
/// owner-only permissions before any content exists.
fn ensure_sparse_image(volume: &str, path: &Path, size_bytes: u64) -> Result<(), VolumeError> {
    let unavailable = |error: std::io::Error| VolumeError::StorageUnavailable {
        volume: volume.to_string(),
        path: path.display().to_string(),
        reason: error.to_string(),
    };
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => return Ok(()),
        Ok(_) => {
            return Err(VolumeError::StorageUnavailable {
                volume: volume.to_string(),
                path: path.display().to_string(),
                reason: "exists and is not a regular file".to_string(),
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(unavailable(error)),
    }
    if let Some(parent) = path.parent() {
        create_directory(volume, parent)?;
    }
    let file = std::fs::File::create(path).map_err(unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(unavailable)?;
    }
    file.set_len(size_bytes).map_err(unavailable)?;
    Ok(())
}

/// Remove one volume's whole directory.
///
/// Delete calls this per accounted `OwnedResourceKind::Volume` record. An absent
/// directory is success, not an error: a volume whose Up never reached the
/// provisioning step has an ownership record and no storage, and refusing there
/// would make an Environment undeletable for having been interrupted.
pub fn reclaim_volume(
    data_dir: &Path,
    environment_id: &EnvironmentId,
    volume_id: &VolumeId,
) -> Result<(), std::io::Error> {
    let directory = volume_directory(data_dir, environment_id, volume_id);
    match std::fs::remove_dir_all(&directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Remove the Environment's volume root once every volume under it is gone.
///
/// `remove_dir` and not `remove_dir_all`: the root must be empty by then, and an
/// unexpected survivor is a leak this must surface rather than erase. A
/// non-empty root is therefore left in place, exactly like an absent one.
pub fn reclaim_environment_volume_root(data_dir: &Path, environment_id: &EnvironmentId) {
    let _ = std::fs::remove_dir(environment_volume_root(data_dir, environment_id));
}

/// Whether this Environment declares any volume at all.
pub fn declares_volumes(spec: &EnvironmentSpec) -> bool {
    !spec.volumes.is_empty()
}

/// Every declared volume's name, for joining a definition to persisted identity.
pub fn declared_volume_names(spec: &EnvironmentSpec) -> Vec<&str> {
    spec.volumes
        .iter()
        .map(|volume: &VolumeSpec| volume.name.as_str())
        .collect()
}

#[cfg(test)]
#[path = "volumes/tests.rs"]
mod tests;
