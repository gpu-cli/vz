//! Resolve declared workspace projections into VirtioFS shares.
//!
//! Three rules from the owner decisions of 2026-09-08 shape this module.
//!
//! * **Decision 7 (host source).** A definition declares `source_path`
//!   RELATIVE to the worktree root. The authoritative root arrives as
//!   `EnvironmentUpRequest::workspace_root`, never as `path_hint`, which is
//!   diagnostic and excluded from the mutation identity. This module joins the
//!   two, canonicalises the result, and refuses anything that leaves the root.
//! * **Decision 8 (binding naming).** The definition names a *symbolic slot*.
//!   Up keeps minting the opaque `worktree-{sha256(workspace_key)}` binding
//!   name and records the slots that binding answers for in its durable
//!   resolution table, so a slot the definition invented can still resolve.
//! * **Ordering.** VirtioFS shares are immutable after `LinuxVm::create`, so
//!   every share must be resolved before the boot loop, exactly as
//!   `install_environment_fabric` mints switch ports before it.
//!
//! `WorkspaceProjectionMode::Snapshot` is deliberately refused: there is no
//! directory-tree copy primitive (only the single-file `clone_file` in
//! `vz-macos-provision`) and no `OwnedResourceKind` variant for a snapshot, so
//! Delete could neither reclaim nor account for one. Refusing is the honest
//! behavior until a snapshot resource kind exists.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    EnvironmentSpec, MachineId, MachineInstance, StackVolumeMount, WorkspaceProjection,
    WorkspaceProjectionMode, validate_workspace_source_path,
};

/// Why a declared workspace projection could not be applied.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkspaceProjectionError {
    #[error("Machine `{machine}` declares a workspace projection but Up carries no workspace root")]
    MissingWorkspaceRoot { machine: String },
    #[error("workspace root `{root}` is not a usable absolute directory: {reason}")]
    InvalidWorkspaceRoot { root: String, reason: String },
    #[error("Machine `{machine}` declares an invalid workspace source path: {reason}")]
    InvalidSourcePath { machine: String, reason: String },
    #[error(
        "Machine `{machine}` workspace source `{declared}` resolves to `{resolved}`, outside worktree root `{root}`"
    )]
    EscapesWorktreeRoot {
        machine: String,
        declared: String,
        resolved: String,
        root: String,
    },
    #[error(
        "Machine `{machine}` requests `snapshot` workspace projection, which has no directory-copy primitive and no owned-resource kind; only `read_write` and `read_only` are implemented"
    )]
    SnapshotUnsupported { machine: String },
    #[error(
        "Machine `{machine}` declares workspace slot `{slot}`, which no minted binding resolves"
    )]
    UnresolvedSlot { machine: String, slot: String },
}

/// Mint the opaque per-worktree binding name.
///
/// Unchanged by decision 8: the symbolic slot never becomes the binding name.
/// The 41-byte prefix keeps the name inside the store's 1..=128 name bound
/// while staying collision-free in practice.
pub fn minted_binding_name(workspace_key: &str) -> String {
    format!("worktree-{:x}", Sha256::digest(workspace_key.as_bytes()))[..41].to_string()
}

/// Every symbolic slot the definition declares, in sorted order.
pub fn declared_workspace_slots(spec: &EnvironmentSpec) -> BTreeSet<String> {
    spec.machines
        .iter()
        .filter_map(|machine| machine.workspace.as_ref())
        .map(|workspace| workspace.binding.clone())
        .collect()
}

/// Resolve one declared source path against the authoritative worktree root.
///
/// The syntactic half (absolute paths, `..`, empty components) is refused
/// before the filesystem is touched. The filesystem half canonicalises both
/// sides and compares prefixes, which is what actually catches a symlink
/// pointing outside the worktree: a well-formed relative path with no `..` can
/// still escape through a link. A symlink whose target canonicalises back
/// inside the root is allowed, because after canonicalisation it *is* inside.
pub fn resolve_contained_source(
    machine: &str,
    root: &Path,
    source_path: &str,
) -> Result<PathBuf, WorkspaceProjectionError> {
    validate_workspace_source_path(machine, source_path).map_err(|error| {
        WorkspaceProjectionError::InvalidSourcePath {
            machine: machine.to_string(),
            reason: error.to_string(),
        }
    })?;
    if !root.is_absolute() {
        return Err(WorkspaceProjectionError::InvalidWorkspaceRoot {
            root: root.display().to_string(),
            reason: "worktree root must be absolute".to_string(),
        });
    }
    let canonical_root =
        root.canonicalize()
            .map_err(|error| WorkspaceProjectionError::InvalidWorkspaceRoot {
                root: root.display().to_string(),
                reason: error.to_string(),
            })?;
    if !canonical_root.is_dir() {
        return Err(WorkspaceProjectionError::InvalidWorkspaceRoot {
            root: root.display().to_string(),
            reason: "worktree root is not a directory".to_string(),
        });
    }
    let joined = if source_path == "." {
        canonical_root.clone()
    } else {
        canonical_root.join(source_path)
    };
    // Canonicalising resolves every symlink in the path, including a final
    // component that points elsewhere. A missing path is refused rather than
    // silently shared as an empty directory.
    let resolved =
        joined
            .canonicalize()
            .map_err(|error| WorkspaceProjectionError::InvalidSourcePath {
                machine: machine.to_string(),
                reason: format!("`{source_path}` is not resolvable: {error}"),
            })?;
    if !resolved.starts_with(&canonical_root) {
        return Err(WorkspaceProjectionError::EscapesWorktreeRoot {
            machine: machine.to_string(),
            declared: source_path.to_string(),
            resolved: resolved.display().to_string(),
            root: canonical_root.display().to_string(),
        });
    }
    Ok(resolved)
}

/// Map one resolved projection onto the existing VirtioFS carrier.
///
/// `tag` must keep the `vz-mount-{N}` shape: `stack_vm` strips that prefix to
/// build the `vz.mount.{N}={guest_path}` kernel argument that
/// `linux/initramfs/init` parses, so a differently shaped tag would create a
/// share the guest never bind-mounts.
pub fn projection_to_volume_mount(
    machine: &str,
    index: usize,
    projection: &WorkspaceProjection,
    host_path: PathBuf,
) -> Result<StackVolumeMount, WorkspaceProjectionError> {
    let read_only = match projection.mode {
        WorkspaceProjectionMode::ReadWrite => false,
        WorkspaceProjectionMode::ReadOnly => true,
        WorkspaceProjectionMode::Snapshot => {
            return Err(WorkspaceProjectionError::SnapshotUnsupported {
                machine: machine.to_string(),
            });
        }
    };
    Ok(StackVolumeMount {
        tag: format!("vz-mount-{index}"),
        host_path,
        guest_path: Some(projection.target_path.clone()),
        read_only,
    })
}

/// Resolve every Machine's declared workspace projection into its shares.
///
/// Returns a per-Machine map so the caller can hand each boot exactly the
/// shares its own definition asked for. Machines that declare no projection are
/// absent from the map rather than present with an empty vector.
pub fn resolve_environment_workspace_mounts(
    spec: &EnvironmentSpec,
    machines: &[MachineInstance],
    resolved_slots: &BTreeSet<String>,
    workspace_root: Option<&str>,
) -> Result<BTreeMap<MachineId, Vec<StackVolumeMount>>, WorkspaceProjectionError> {
    let mut mounts = BTreeMap::new();
    for desired in &spec.machines {
        let Some(projection) = &desired.workspace else {
            continue;
        };
        if !resolved_slots.contains(projection.binding.as_str()) {
            return Err(WorkspaceProjectionError::UnresolvedSlot {
                machine: desired.name.clone(),
                slot: projection.binding.clone(),
            });
        }
        let root =
            workspace_root.ok_or_else(|| WorkspaceProjectionError::MissingWorkspaceRoot {
                machine: desired.name.clone(),
            })?;
        let host_path =
            resolve_contained_source(&desired.name, Path::new(root), &projection.source_path)?;
        let mount = projection_to_volume_mount(&desired.name, 0, projection, host_path)?;
        let Some(instance) = machines.iter().find(|machine| machine.name == desired.name) else {
            continue;
        };
        mounts.insert(instance.machine_id.clone(), vec![mount]);
    }
    Ok(mounts)
}

#[cfg(test)]
mod tests;
