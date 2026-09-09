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
//! `WorkspaceProjectionMode::Snapshot` is a private per-Machine copy of the
//! declared source, made with `clone_path` and shared read-write. Two earlier
//! objections stood against it and both are answered here.
//!
//! * *"There is no directory-tree copy primitive, only the single-file
//!   `clone_file` in `vz-macos-provision`."* `clonefile(2)` clones a directory
//!   hierarchy recursively on APFS — the recursion is the syscall's, not the
//!   caller's — so the primitive was always there; only its single-file use
//!   was. `vz_macos_provision::clone::clone_path` is now that one wrapper, used
//!   for both shapes, and its own tests prove a cloned tree is recursive,
//!   preserves symlinks, and is a separate inode from its source.
//! * *"There is no `OwnedResourceKind` variant, so Delete could neither reclaim
//!   nor account for one."* A snapshot needs no variant of its own because it is
//!   not Environment-scoped: it is a private copy for exactly one Machine,
//!   remade on every Up, so it lives INSIDE that Machine's runtime store
//!   directory. That store is already an accounted owned resource
//!   (`OwnedResourceKind::Other("machine_runtime_store")`), and Delete removes
//!   it positively. Putting the copy anywhere else is what would have needed a
//!   new resource kind.
//!
//! Remade on every Up is the semantics, not an implementation shortcut: a
//! snapshot is the source as it was when the Machine booted, so a stale copy
//! from a previous boot would be the wrong answer. The Machine writes into its
//! copy freely and nothing propagates back to the worktree, which is the whole
//! difference from `read_write`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    EnvironmentSpec, MachineId, MachineInstance, MachineSpec, StackVolumeMount,
    WorkspaceProjection, WorkspaceProjectionMode, validate_workspace_source_path,
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
    #[error("Machine `{machine}` snapshot copy at `{destination}` could not be made: {reason}")]
    SnapshotUnavailable {
        machine: String,
        destination: String,
        reason: String,
    },
    #[error(
        "Machine `{machine}` declares workspace slot `{slot}`, which no minted binding resolves"
    )]
    UnresolvedSlot { machine: String, slot: String },
    #[error(
        "Machines `{first}` (`{first_source}`) and `{second}` (`{second_source}`) project overlapping host sources and at least one is writable; a writable host source is attached to at most one Machine"
    )]
    WritableSourceMultiAttach {
        first: String,
        first_source: String,
        second: String,
        second_source: String,
    },
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

/// Whether this mode makes the Machine holding the share a writer *of the
/// shared host source*.
///
/// `Snapshot` is deliberately not a writer. The multi-attach rule exists to
/// stop two Machines mutating one host subtree with no coherence protocol
/// between them, and a snapshot mutates a private clone instead — nothing it
/// writes is visible to any other Machine or to the worktree. Two snapshots of
/// one source, or a snapshot beside a reader, are therefore allowed, and
/// `snapshot_projections_of_one_source_do_not_collide` pins that.
fn is_writer(mode: WorkspaceProjectionMode) -> bool {
    matches!(mode, WorkspaceProjectionMode::ReadWrite)
}

/// Declared source path as path components, `"."` being the worktree root.
///
/// `validate_workspace_source_path` has already refused `..`, empty components
/// and absolute paths, so a plain component split is the whole decomposition.
fn source_components(source_path: &str) -> Vec<&str> {
    if source_path == "." {
        Vec::new()
    } else {
        source_path.split('/').collect()
    }
}

/// The shared "no silent writable multi-attach" rule, over any subject.
///
/// Three declarations need this rule and they differ only in what "the same
/// subject" means: two workspace projections overlap when their declared source
/// components nest, two resolved projections overlap when their canonicalised
/// host paths nest, and two attachments of one block volume always overlap
/// because they name the one image. The *rule* is identical in all three — a
/// subject with a writer admits no second attachment, reader or writer — so it
/// lives here once. Duplicating it is how the reader-beside-a-writer case would
/// come to be refused for a projection and quietly allowed for a volume.
///
/// Returns the first offending pair in declaration order, so the refusal names
/// a specific pair rather than reporting that some pair exists.
pub fn first_writable_multi_attach<T>(
    attachments: &[T],
    writes: impl Fn(&T) -> bool,
    overlaps: impl Fn(&T, &T) -> bool,
) -> Option<(&T, &T)> {
    for (index, first) in attachments.iter().enumerate() {
        for second in &attachments[index + 1..] {
            // Two readers of one subject are allowed: there is no writer to
            // serialise against. One writer and one reader is not, because the
            // reader observes a subject another Machine mutates underneath it
            // with no coherence protocol between the two carriers.
            if (writes(first) || writes(second)) && overlaps(first, second) {
                return Some((first, second));
            }
        }
    }
    None
}

/// Whether two component lists name the same host subtree or nested subtrees.
///
/// Overlap, not equality, is the property: a Machine projecting `.` and another
/// projecting `src` share every byte under `src`. The comparison is
/// per-component so `inside` and `inside-two` are correctly disjoint, which a
/// string-prefix comparison would get wrong.
fn components_overlap(first: &[&str], second: &[&str]) -> bool {
    let shared = first.len().min(second.len());
    first[..shared] == second[..shared]
}

/// Refuse two Machines sharing one host source when either of them can write.
///
/// This is the declaration-level half, and it runs at admission so the refusal
/// lands before any identity or workspace binding is reserved. It compares the
/// declared `source_path` strings and therefore cannot see two paths that
/// become one directory through a symlink; `resolve_environment_workspace_mounts`
/// repeats the rule over the canonicalised paths for that case, exactly as
/// source containment is checked syntactically and then again after
/// canonicalisation.
///
/// The reader/writer rule itself lives in [`first_writable_multi_attach`],
/// which the block-volume half of the storage policy shares.
pub fn refuse_declared_writable_multi_attach(
    spec: &EnvironmentSpec,
) -> Result<(), WorkspaceProjectionError> {
    let declared: Vec<(&str, &WorkspaceProjection)> = spec
        .machines
        .iter()
        .filter_map(|machine| {
            machine
                .workspace
                .as_ref()
                .map(|projection| (machine.name.as_str(), projection))
        })
        .collect();
    if let Some(((first, first_projection), (second, second_projection))) =
        first_writable_multi_attach(
            &declared,
            |(_, projection)| is_writer(projection.mode),
            |(_, first), (_, second)| {
                components_overlap(
                    &source_components(&first.source_path),
                    &source_components(&second.source_path),
                )
            },
        )
    {
        return Err(WorkspaceProjectionError::WritableSourceMultiAttach {
            first: (*first).to_string(),
            first_source: first_projection.source_path.clone(),
            second: (*second).to_string(),
            second_source: second_projection.source_path.clone(),
        });
    }
    Ok(())
}

/// Repeat the multi-attach rule over canonicalised host paths.
///
/// `Path::starts_with` compares whole components, so a sibling whose name
/// merely shares a prefix is not treated as nested.
fn refuse_resolved_writable_multi_attach(
    resolved: &[(String, String, PathBuf, bool)],
) -> Result<(), WorkspaceProjectionError> {
    if let Some(((first, first_source, _, _), (second, second_source, _, _))) =
        first_writable_multi_attach(
            resolved,
            |(_, _, _, writes)| *writes,
            |(_, _, first_path, _), (_, _, second_path, _)| {
                first_path.starts_with(second_path) || second_path.starts_with(first_path)
            },
        )
    {
        return Err(WorkspaceProjectionError::WritableSourceMultiAttach {
            first: first.clone(),
            first_source: first_source.clone(),
            second: second.clone(),
            second_source: second_source.clone(),
        });
    }
    Ok(())
}

/// Where one Machine's snapshot copy lives inside its own runtime store.
///
/// Inside the Machine's store and not beside the Environment's volumes: a
/// snapshot belongs to one Machine, is remade on every Up, and must not outlive
/// the Machine that asked for it. Delete removes the store, so the copy is
/// reclaimed with no ownership record of its own.
pub fn machine_snapshot_path(machine_data_path: &Path) -> PathBuf {
    machine_data_path.join("workspace-snapshot")
}

/// Remake one Machine's snapshot copy of its declared source.
///
/// Removes a previous boot's copy first, so the tree the Machine sees is the
/// source as it was at this boot and never a merge of two.
pub fn materialise_snapshot(
    machine: &str,
    source: &Path,
    machine_data_path: &Path,
) -> Result<PathBuf, WorkspaceProjectionError> {
    let destination = machine_snapshot_path(machine_data_path);
    let failure = |reason: String| WorkspaceProjectionError::SnapshotUnavailable {
        machine: machine.to_string(),
        destination: destination.display().to_string(),
        reason,
    };
    match std::fs::remove_dir_all(&destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(failure(error.to_string())),
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent).map_err(|error| failure(error.to_string()))?;
    }
    vz_macos_provision::clone::clone_path(source, &destination)
        .map_err(|error| failure(error.to_string()))?;
    Ok(destination)
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
    let _ = machine;
    let read_only = match projection.mode {
        WorkspaceProjectionMode::ReadWrite => false,
        // A snapshot is shared read-write on purpose. The Machine owns its copy
        // and the point of the mode is that it may write into it; what it must
        // not do is write into the worktree, and the private clone is what
        // stops that. Mounting a snapshot read-only would make it an awkward
        // synonym for `read_only` with an extra copy.
        WorkspaceProjectionMode::Snapshot => false,
        WorkspaceProjectionMode::ReadOnly => true,
    };
    Ok(StackVolumeMount {
        tag: format!("vz-mount-{index}"),
        host_path,
        guest_path: Some(projection.target_path.clone()),
        read_only,
    })
}

/// Every Machine's resolved workspace shares, plus the sources still to clone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedWorkspaceMounts {
    /// Per-Machine VirtioFS shares. A `snapshot` Machine's share is present
    /// here with its host path still pointing at the SOURCE, because the copy
    /// cannot be made until the Machine's own runtime store exists.
    pub mounts: BTreeMap<MachineId, Vec<StackVolumeMount>>,
    /// Machines whose share must be redirected onto a private clone before
    /// boot, and the source each clone is taken from.
    pub snapshot_sources: BTreeMap<MachineId, PathBuf>,
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
) -> Result<ResolvedWorkspaceMounts, WorkspaceProjectionError> {
    // Every source is resolved first so the multi-attach rule can compare
    // canonicalised host paths. Two declarations that differ as strings can be
    // one directory once a symlink is followed, and that pair must be refused
    // before any share is handed to a boot.
    let mut resolved: Vec<(&MachineSpec, &WorkspaceProjection, PathBuf)> = Vec::new();
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
        resolved.push((desired, projection, host_path));
    }
    let attachments: Vec<(String, String, PathBuf, bool)> = resolved
        .iter()
        .map(|(desired, projection, host_path)| {
            (
                desired.name.clone(),
                projection.source_path.clone(),
                host_path.clone(),
                is_writer(projection.mode),
            )
        })
        .collect();
    refuse_resolved_writable_multi_attach(&attachments)?;

    let mut resolved_mounts = ResolvedWorkspaceMounts::default();
    for (desired, projection, host_path) in resolved {
        let Some(instance) = machines.iter().find(|machine| machine.name == desired.name) else {
            continue;
        };
        if projection.mode == WorkspaceProjectionMode::Snapshot {
            // Recorded, not cloned here. The destination is inside the
            // Machine's runtime store, which `attach_machine` has not created
            // yet at this point in Up, so the clone is made in the boot loop and
            // this share's host path is redirected onto it there.
            resolved_mounts
                .snapshot_sources
                .insert(instance.machine_id.clone(), host_path.clone());
        }
        let mount = projection_to_volume_mount(&desired.name, 0, projection, host_path)?;
        resolved_mounts
            .mounts
            .insert(instance.machine_id.clone(), vec![mount]);
    }
    Ok(resolved_mounts)
}

#[cfg(test)]
mod tests;
