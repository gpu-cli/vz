//! Declared Environment-owned storage: block volumes and shared caches.
//!
//! A [`WorkspaceProjection`](super::WorkspaceProjection) projects the *user's
//! worktree* into one Machine. A [`VolumeSpec`] is the other half of the
//! storage policy: storage the Environment itself owns, which no host path
//! outside the Environment backs and which outlives every individual Machine
//! boot inside it.
//!
//! Two kinds exist, and the split is the whole reason the type has a `kind` at
//! all rather than one universal "volume":
//!
//! * [`VolumeKind::Block`] is a virtio-block device backed by one sparse disk
//!   image carrying one ext4 filesystem. A filesystem image has exactly one
//!   writer by construction — ext4 is not a cluster filesystem, and two guests
//!   mounting one image read-write corrupt it without either kernel noticing —
//!   so a writable block volume is refused on more than one Machine. That
//!   refusal is the "a writable block device is never silently attached to two
//!   Machines" rule of the product contract, and it is made at admission,
//!   before any image is allocated.
//! * [`VolumeKind::SharedCache`] is one host directory exported to every
//!   attached Machine over its own VirtioFS device. Multi-attach is the point
//!   rather than the hazard: there is no shared block layer to corrupt, each
//!   guest runs an ordinary FUSE client against one host filesystem, and the
//!   host filesystem serialises the writes. What a shared cache still cannot
//!   offer for free is *when* one Machine sees another's write, so the
//!   declaration must state it: see [`SharedCacheConsistency`].
//!
//! Nothing here decides where the storage lives on the host or when it is
//! reclaimed; that is the runtime's business. This module is the portable
//! declaration and its syntactic validation only.

use serde::{Deserialize, Serialize};

use super::topology::{
    EnvironmentId, TopologyValidationError, VolumeId, validate_machine_target_path,
};

/// Largest declarable block volume, and the smallest.
///
/// The floor is one mebibyte because `mke2fs` cannot make a filesystem in less
/// and the guest-side format would fail after the Machine had already booted.
/// The ceiling is one tebibyte: the image is sparse, so the number is a promise
/// about the guest's block device rather than about host bytes consumed, and a
/// promise the host cannot keep is worse than a refusal.
pub const MIN_BLOCK_VOLUME_BYTES: u64 = 1024 * 1024;
/// See [`MIN_BLOCK_VOLUME_BYTES`].
pub const MAX_BLOCK_VOLUME_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// Largest number of Machines one volume may be attached to.
///
/// An Environment is capped at 128 Machines, so this is the point at which the
/// declaration stops being expressible rather than an independent budget.
pub const MAX_VOLUME_ATTACHMENTS: usize = 128;

/// What a declared volume physically is.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeKind {
    /// One sparse disk image carrying one ext4 filesystem, attached as a
    /// virtio-block device. At most one attachment may write it.
    Block,
    /// One host directory exported to every attached Machine over VirtioFS.
    /// Every attachment may write it, under a declared consistency model.
    SharedCache,
}

/// Whether one attachment may write the volume it names.
///
/// Deliberately a separate enum from `WorkspaceProjectionMode`, which also
/// carries `snapshot`: a snapshot is a private copy of a *worktree* subtree and
/// has no meaning for Environment-owned storage, whose whole purpose is that it
/// is not a copy of anything.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum VolumeAccessMode {
    ReadWrite,
    ReadOnly,
}

impl VolumeAccessMode {
    /// Whether this mode makes the attachment holding it a writer.
    pub fn writes(self) -> bool {
        matches!(self, VolumeAccessMode::ReadWrite)
    }
}

/// The consistency model a shared cache advertises, and the only one it does.
///
/// Only one model exists because only one is true of the carrier. Each attached
/// Machine runs its own virtio-fs FUSE client with its own attribute and dentry
/// caches over one host directory; the host filesystem serialises the writes,
/// but a reader that has a valid cached attribute for a path does not consult
/// the server, so a read issued immediately after another Machine's write may
/// legitimately observe the pre-write state. Nothing in the stack turns that
/// off, so declaring close-to-open — which would promise that any open after a
/// remote close sees the write — would be a claim the carrier does not honour.
///
/// The declaration therefore states the bound instead of a name that implies
/// zero: a closed write on one attached Machine becomes visible to every other
/// attached Machine within `staleness_bound_millis`, and nothing is promised
/// before it. A fixture proving the model has to poll to that bound; one that
/// reads once and passes proved only that it got lucky.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SharedCacheConsistencyModel {
    /// A closed write is visible to every other attachment within the declared
    /// bound. Reads before the bound may observe either state.
    BoundedStaleness,
}

/// A shared cache's declared consistency, model and bound together.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct SharedCacheConsistency {
    pub model: SharedCacheConsistencyModel,
    /// Upper bound, in milliseconds, on how long a closed write may remain
    /// invisible to another attached Machine.
    pub staleness_bound_millis: u32,
}

/// Smallest and largest declarable staleness bound.
///
/// Zero is refused because it is the one value the carrier certainly cannot
/// honour, and a declaration nothing can satisfy is a bug rather than a strict
/// requirement. The ceiling keeps a fixture that must wait out the bound
/// bounded itself.
pub const MIN_STALENESS_BOUND_MILLIS: u32 = 1;
/// See [`MIN_STALENESS_BOUND_MILLIS`].
pub const MAX_STALENESS_BOUND_MILLIS: u32 = 60_000;

/// One Machine's attachment to one declared volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct VolumeAttachment {
    /// Topology-local name of the Machine this volume appears inside.
    pub machine: String,
    /// Absolute path inside that Machine where the volume appears.
    pub target_path: String,
    pub mode: VolumeAccessMode,
}

/// One declared Environment-owned storage resource.
///
/// `size_bytes` and `consistency` are each meaningful for exactly one kind and
/// refused for the other, rather than being ignored: a `size_bytes` on a shared
/// cache would silently not cap anything, and a `consistency` on a block volume
/// would advertise a model the single-writer rule already makes vacuous.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VolumeSpec {
    pub schema_version: u32,
    /// Topology-local volume name, unique within the Environment.
    pub name: String,
    pub kind: VolumeKind,
    /// Guest-visible size of a [`VolumeKind::Block`] volume. Required for
    /// `block`, refused for `shared_cache`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Consistency a [`VolumeKind::SharedCache`] advertises. Required for
    /// `shared_cache`, refused for `block`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<SharedCacheConsistency>,
    /// Machines this volume is attached to. At least one; a volume no Machine
    /// can see is storage nothing could ever reclaim on purpose.
    pub attachments: Vec<VolumeAttachment>,
}

impl VolumeSpec {
    /// Every attachment that may write this volume.
    pub fn writers(&self) -> impl Iterator<Item = &VolumeAttachment> {
        self.attachments
            .iter()
            .filter(|attachment| attachment.mode.writes())
    }
}

/// Validate one declared volume in isolation.
///
/// Cross-references (does the named Machine exist, is the name unique in the
/// Environment) belong to `EnvironmentSpec::validate`, and the multi-attach
/// policy belongs to admission, where the workspace half of the same rule is
/// already enforced before any state is reserved.
pub fn validate_volume(spec: &VolumeSpec) -> Result<(), TopologyValidationError> {
    let invalid = |reason: &str| TopologyValidationError::InvalidIdentifier {
        kind: "volume".to_string(),
        value: spec.name.clone(),
        reason: reason.to_string(),
    };
    if spec.schema_version != super::topology::TOPOLOGY_SCHEMA_VERSION {
        return Err(TopologyValidationError::UnsupportedSchemaVersion {
            found: spec.schema_version,
            supported: super::topology::TOPOLOGY_SCHEMA_VERSION,
        });
    }
    if spec.attachments.is_empty() || spec.attachments.len() > MAX_VOLUME_ATTACHMENTS {
        return Err(invalid("volume requires 1..=128 attachments"));
    }
    match spec.kind {
        VolumeKind::Block => {
            let Some(size) = spec.size_bytes else {
                return Err(invalid("`block` volume requires `size_bytes`"));
            };
            if !(MIN_BLOCK_VOLUME_BYTES..=MAX_BLOCK_VOLUME_BYTES).contains(&size) {
                return Err(invalid("`block` volume `size_bytes` must be 1 MiB..=1 TiB"));
            }
            if spec.consistency.is_some() {
                return Err(invalid(
                    "`block` volume must not declare `consistency`; a single-writer block device has no shared-cache consistency to state",
                ));
            }
        }
        VolumeKind::SharedCache => {
            if spec.size_bytes.is_some() {
                return Err(invalid(
                    "`shared_cache` volume must not declare `size_bytes`; a VirtioFS export imposes no size of its own",
                ));
            }
            let Some(consistency) = spec.consistency else {
                return Err(invalid(
                    "`shared_cache` volume requires an explicit `consistency` declaration",
                ));
            };
            if !(MIN_STALENESS_BOUND_MILLIS..=MAX_STALENESS_BOUND_MILLIS)
                .contains(&consistency.staleness_bound_millis)
            {
                return Err(invalid(
                    "`shared_cache` `staleness_bound_millis` must be 1..=60000",
                ));
            }
        }
    }
    let mut machines = std::collections::BTreeSet::new();
    for attachment in &spec.attachments {
        // One Machine naming one volume twice would mean two mount points for
        // one device, which the runtime would have to serialise against itself.
        if !machines.insert(attachment.machine.as_str()) {
            return Err(TopologyValidationError::Duplicate {
                kind: format!("volume.{}.attachment.machine", spec.name),
                value: attachment.machine.clone(),
            });
        }
        validate_machine_target_path(
            &format!("volume.{}.attachment.target_path", spec.name),
            &attachment.machine,
            &attachment.target_path,
        )?;
    }
    Ok(())
}

/// Persisted identity of one declared volume.
///
/// Like [`HostExportInstance`](super::HostExportInstance) this carries identity
/// and ownership only; the size, kind and attachment set stay in the
/// definition, which Up re-joins by name. The instance is deliberately NOT
/// Machine-scoped: a shared cache is attached to several Machines at once, and
/// a block volume outlives the particular Machine incarnation that mounted it,
/// so an owning Machine id here would be a lie about lifetime in both cases.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeInstance {
    pub schema_version: u32,
    pub volume_id: VolumeId,
    pub environment_id: EnvironmentId,
    pub name: String,
    pub kind: VolumeKind,
}

#[cfg(test)]
#[path = "volume_tests.rs"]
mod tests;
