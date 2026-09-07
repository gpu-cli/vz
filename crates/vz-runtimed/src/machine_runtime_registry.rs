//! Private, exactly-owned runtime stores. This is admission infrastructure,
//! not a ProjectDefinition target resolver or permission to start a Machine.
//!
//! Callers reserve the returned ownership record in the topology store before
//! admitting a new directory. During lifecycle recovery they must use
//! `ExistingOnly`, and separately fence the operation before runtime effects.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fchmod, fstat, mkdirat, openat, renameat_with,
    statat, unlinkat,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use vz_runtime_contract::{
    EnvironmentId, EnvironmentLifecycleKind, EnvironmentLifecycleOperation, LifecycleOperationId,
    MachineId, MachineIncarnation, MachineState, OwnedResourceKind, OwnershipRecord, ProjectId,
    ResourceOwner, TOPOLOGY_SCHEMA_VERSION,
};

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const PRIVATE_DIRECTORY: Mode = Mode::from_raw_mode(0o700);
const PRIVATE_FILE: Mode = Mode::from_raw_mode(0o600);
const MANIFEST_NAME: &str = "owner.json";
const DATA_NAME: &str = "data";
const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
const DELETE_NAMESPACE: &str = "topology-machine-deletions";
const DELETE_INTENT: &str = "intent.json";
const DELETE_RECEIPT: &str = "receipt.json";
const DELETE_TREE: &str = "store";
const MAX_DELETE_ENTRIES: usize = 1_000_000;
const MAX_DELETE_DEPTH: usize = 128;
const DELETE_WALK_LIMIT: Duration = Duration::from_secs(60);
const MAX_DELETE_RECORD_BYTES: u64 = 256 * 1024;

#[derive(Debug, Error)]
pub enum MachineRuntimeRegistryError {
    #[error("invalid Machine runtime admission: {0}")]
    Invalid(String),
    #[error("Machine runtime store ownership conflict: {0}")]
    Conflict(String),
    #[error("Machine runtime store is already leased: {0}")]
    Leased(String),
    #[error("Machine runtime store does not exist: {0}")]
    NotFound(String),
    #[error("Machine runtime registry lock was poisoned")]
    Poisoned,
    #[error("Machine runtime store I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

impl From<rustix::io::Errno> for MachineRuntimeRegistryError {
    fn from(error: rustix::io::Errno) -> Self {
        Self::Io(error.into())
    }
}

/// Admission never creates anything in recovery mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineRuntimeAdmission {
    CreateOrOpen,
    ExistingOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerManifest {
    schema_version: u32,
    owner: ResourceOwner,
    reservation: OwnershipRecord,
    configuration_digest: String,
}

/// Filesystem identities, not path aliases, bound by a deletion receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineStoreFileIdentity {
    pub device: u64,
    pub inode: u64,
}

impl MachineStoreFileIdentity {
    fn of(file: &File) -> Result<Self, MachineRuntimeRegistryError> {
        Ok(Self::from_stat(&fstat(file)?))
    }

    #[allow(
        clippy::unnecessary_cast,
        reason = "libc dev_t and ino_t widths differ between supported hosts"
    )]
    fn from_stat(stat: &rustix::fs::Stat) -> Self {
        Self {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
        }
    }

    fn require(&self, file: &File) -> Result<(), MachineRuntimeRegistryError> {
        if *self != Self::of(file)? {
            return Err(delete_conflict("deletion directory identity changed"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteOperationIdentity {
    operation_id: LifecycleOperationId,
    generation: u64,
    request_id: String,
    idempotency_key: String,
    request_hash: String,
    definition_digest: String,
    initial_machine_state: MachineState,
    expected_incarnation: Option<MachineIncarnation>,
}

impl DeleteOperationIdentity {
    fn from_operation(
        manifest: &OwnerManifest,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<Self, MachineRuntimeRegistryError> {
        operation.validate_structure().map_err(invalid)?;
        let machine = manifest
            .owner
            .machine_id
            .as_ref()
            .ok_or_else(|| delete_conflict("missing Machine owner"))?;
        if operation.kind != EnvironmentLifecycleKind::Delete
            || operation.project_id != manifest.owner.project_id
            || operation.environment_id != manifest.owner.environment_id
            || !operation
                .cleanup_steps
                .iter()
                .any(|step| step.ownership == manifest.reservation)
        {
            return Err(delete_conflict(
                "Delete operation does not own this exact store",
            ));
        }
        let step = operation
            .machine_steps
            .iter()
            .find(|step| step.machine_id == *machine)
            .ok_or_else(|| delete_conflict("Delete operation omits the exact Machine"))?;
        Ok(Self {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation,
            request_id: operation.request_id.clone(),
            idempotency_key: operation.idempotency_key.clone(),
            request_hash: operation.request_hash.clone(),
            definition_digest: operation.definition_digest.clone(),
            initial_machine_state: step.initial_state,
            expected_incarnation: step.expected_incarnation.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MachineStoreDeleteIntent {
    schema_version: u32,
    manifest: OwnerManifest,
    operation: DeleteOperationIdentity,
    root: MachineStoreFileIdentity,
    namespace: MachineStoreFileIdentity,
    store: MachineStoreFileIdentity,
    data: MachineStoreFileIdentity,
    quiescence: serde_json::Value,
}

/// Immutable evidence retained outside the deleted Machine store. It proves
/// only the exact private runtime tree was removed, not external user data or
/// the rest of the Environment ownership graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineStoreDeleteReceipt {
    pub schema_version: u32,
    pub owner: ResourceOwner,
    pub operation_id: LifecycleOperationId,
    pub generation: u64,
    pub configuration_digest: String,
    pub store: MachineStoreFileIdentity,
    pub data: MachineStoreFileIdentity,
    pub intent_sha256: String,
    pub store_removed: bool,
}

/// A read-only, non-constructible claim about an exact current store or an
/// already-admitted deletion. Absence alone never constructs this claim.
pub(crate) struct MachineStoreDeletePreflight {
    manifest: OwnerManifest,
    registry_root: PathBuf,
    lease: Option<Arc<MachineRuntimeStoreLease>>,
    pending: Option<MachineStoreDeleteIntent>,
}

impl MachineStoreDeletePreflight {
    pub(crate) fn owner(&self) -> &ResourceOwner {
        &self.manifest.owner
    }
    pub(crate) fn configuration_digest(&self) -> &str {
        &self.manifest.configuration_digest
    }
    pub(crate) fn lease(&self) -> Option<&Arc<MachineRuntimeStoreLease>> {
        self.lease.as_ref()
    }
    pub(crate) fn quiescence_evidence(&self) -> Option<&serde_json::Value> {
        self.pending.as_ref().map(|intent| &intent.quiescence)
    }
    pub(crate) fn delete_operation_id(&self) -> Option<&LifecycleOperationId> {
        self.pending
            .as_ref()
            .map(|intent| &intent.operation.operation_id)
    }
    pub(crate) fn matches_operation(
        &self,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<(), MachineRuntimeRegistryError> {
        let expected = DeleteOperationIdentity::from_operation(&self.manifest, operation)?;
        if self
            .pending
            .as_ref()
            .is_some_and(|intent| intent.operation != expected)
        {
            return Err(delete_conflict(
                "persisted deletion belongs to a different immutable operation",
            ));
        }
        Ok(())
    }
}

/// An admitted deletion retains its sealed quiescence/controller authority
/// until removal either succeeds or returns a recoverable durable failure.
#[cfg(target_os = "macos")]
pub(crate) struct MachineStoreDeletion<'a, R> {
    registry: &'a MachineRuntimeRegistry<R>,
    claim: MachineStoreDeletePreflight,
    intent: MachineStoreDeleteIntent,
    intent_directory: File,
    store: Option<File>,
    _quiescence: crate::machine_live_sessions::MachineDeleteQuiescence,
}

/// A runtime-free lease on one exactly-owned private Machine store.
///
/// The retained store and data directory descriptors pin the identities that
/// were admitted, while the exclusive lock on `directory` fences other daemon
/// registries for the lifetime of the lease. The registry strongly retains
/// acquired leases, so dropping a caller's `Arc` cannot release that fence.
pub struct MachineRuntimeStoreLease {
    manifest: OwnerManifest,
    registry_root: PathBuf,
    data_path: PathBuf,
    directory: File,
    data_directory: File,
}

impl MachineRuntimeStoreLease {
    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    pub fn owner(&self) -> &ResourceOwner {
        &self.manifest.owner
    }

    pub fn configuration_digest(&self) -> &str {
        &self.manifest.configuration_digest
    }

    pub(crate) fn data_directory(&self) -> &File {
        &self.data_directory
    }

    pub(crate) fn validate_current(&self) -> Result<(), MachineRuntimeRegistryError> {
        validate_current_lease(self)
    }
}

/// Holds a backend together with the store lease that fences all of its
/// persistent state. Registry entries are strongly retained; dropping a
/// caller's `Arc` cannot permit a duplicate backend constructor while this
/// registry remains alive.
pub struct MachineRuntimeEntry<R> {
    runtime: R,
    lease: Arc<MachineRuntimeStoreLease>,
}

impl<R> MachineRuntimeEntry<R> {
    /// Durably bind a positive physical Stop result to this exact leased store.
    /// Publication precedes the lifecycle acknowledgement. Existing evidence is
    /// never replaced, including after an interrupted publication.
    pub(crate) fn persist_stop_receipt(
        &self,
        receipt: &crate::machine_live_sessions::MachineSessionStopReceipt,
    ) -> Result<(), MachineRuntimeRegistryError> {
        self.validate_current()?;
        if receipt.owner != *self.owner()
            || receipt.generation == 0
            || receipt.outcome != vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped
        {
            return Err(invalid("Stop receipt lacks exact positive Machine closure"));
        }
        receipt.runtime_identity.validate().map_err(invalid)?;
        let operation = LifecycleOperationId::new(receipt.operation_id.clone()).map_err(invalid)?;
        // Lifecycle evidence is mutable Machine-owned state, not part of the
        // immutable linux-target artifact pin and its exact inventory.
        let target = child_directory(self.lease.data_directory(), "linux-lifecycle", true)?;
        let stops = child_directory(&target, "stops", true)?;
        validate_stop_receipt_directories(self.lease.data_directory(), &target, &stops)?;
        publish_delete_record(&stops, &format!("{operation}.json"), receipt)?;
        validate_stop_receipt_directories(self.lease.data_directory(), &target, &stops)?;
        self.validate_current()
    }

    pub(crate) fn validate_current(&self) -> Result<(), MachineRuntimeRegistryError> {
        self.lease.validate_current()
    }

    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    pub fn data_path(&self) -> &Path {
        self.lease.data_path()
    }

    pub fn owner(&self) -> &ResourceOwner {
        self.lease.owner()
    }

    pub fn configuration_digest(&self) -> &str {
        self.lease.configuration_digest()
    }
}

fn validate_stop_receipt_directories(
    data: &File,
    target: &File,
    stops: &File,
) -> Result<(), MachineRuntimeRegistryError> {
    let current_target = child_directory(data, "linux-lifecycle", false)?;
    let current_stops = child_directory(&current_target, "stops", false)?;
    if !same_file(target, &current_target)? || !same_file(stops, &current_stops)? {
        return Err(invalid(
            "Stop evidence directory was replaced; closure publication is uncertain",
        ));
    }
    Ok(())
}

/// A daemon-owned registry, separate from the legacy global runtime manager.
/// Construction is read-only. Admission opens the configured root without
/// following symlinks and derives all child names from typed ownership, never
/// path hints.
///
/// The runtime backend still accepts a path rather than a directory descriptor.
/// The registry verifies that path immediately before construction and on every
/// cached admission, and retains both directory descriptors. Admission also
/// requires every configured-root path component to be owned by root or the
/// effective user and rejects writable non-sticky ancestors. A trusted sticky
/// ancestor is accepted only when its next component is also root- or
/// effective-user-owned.
///
/// These checks inspect POSIX owner and mode bits. Callers must additionally
/// ensure that the configured ancestry has no ACL or equivalent grant allowing
/// an untrusted process to rename a component; mode-bit validation cannot prove
/// the absence of such grants. Processes running as the same effective user
/// remain inside the filesystem trust boundary and can rename writable
/// ancestors despite descriptor-relative validation.
pub struct MachineRuntimeRegistry<R> {
    root: PathBuf,
    state: Mutex<MachineRuntimeRegistryState<R>>,
}

struct MachineRuntimeRegistryState<R> {
    leases: HashMap<String, Arc<MachineRuntimeStoreLease>>,
    entries: HashMap<String, Arc<MachineRuntimeEntry<R>>>,
}

impl<R> MachineRuntimeRegistry<R> {
    pub(crate) fn native_bootstrap_cache_path(&self) -> PathBuf {
        self.root.join("native-bootstrap")
    }
    pub fn new(root: PathBuf) -> Result<Self, MachineRuntimeRegistryError> {
        let root = absolute_path_without_parent_traversal(&root)?;
        Ok(Self {
            root,
            state: Mutex::new(MachineRuntimeRegistryState {
                leases: HashMap::new(),
                entries: HashMap::new(),
            }),
        })
    }

    pub fn reservation(
        owner: &ResourceOwner,
    ) -> Result<OwnershipRecord, MachineRuntimeRegistryError> {
        ProjectId::new(owner.project_id.as_str()).map_err(invalid)?;
        EnvironmentId::new(owner.environment_id.as_str()).map_err(invalid)?;
        let machine_id = owner.machine_id.as_ref().ok_or_else(|| {
            MachineRuntimeRegistryError::Invalid("Machine ownership is required".into())
        })?;
        MachineId::new(machine_id.as_str()).map_err(invalid)?;
        let resource_kind = OwnedResourceKind::Other("machine_runtime_store".into());
        Ok(OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_id: owner
                .bounded_resource_name(&resource_kind, "runtime", 64)
                .map_err(invalid)?,
            resource_kind,
            environment_id: owner.environment_id.clone(),
            machine_id: owner.machine_id.clone(),
        })
    }

    /// Acquires the exact private store without constructing a runtime.
    ///
    /// A creating admission requires a digest that binds the already-resolved
    /// backend, target, profile, resources and artifact identities. Recovery
    /// may omit the expected digest in `ExistingOnly` mode to discover the
    /// validated persisted selection; supplying one still requires an exact
    /// match. This API does not resolve `TargetSpec`.
    pub fn acquire_store(
        &self,
        owner: &ResourceOwner,
        reservation: &OwnershipRecord,
        expected_configuration_digest: Option<&str>,
        mode: MachineRuntimeAdmission,
    ) -> Result<Arc<MachineRuntimeStoreLease>, MachineRuntimeRegistryError> {
        let expected = Self::reservation(owner)?;
        if expected != *reservation {
            return Err(MachineRuntimeRegistryError::Conflict(
                "reservation does not match exact Project/Environment/Machine store identity"
                    .into(),
            ));
        }
        if mode == MachineRuntimeAdmission::CreateOrOpen && expected_configuration_digest.is_none()
        {
            return Err(MachineRuntimeRegistryError::Invalid(
                "creating a Machine runtime store requires a resolved configuration digest".into(),
            ));
        }
        if let Some(digest) = expected_configuration_digest {
            validate_configuration_digest(digest)?;
        }
        let creating_manifest =
            expected_configuration_digest.map(|configuration_digest| OwnerManifest {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                owner: owner.clone(),
                reservation: expected.clone(),
                configuration_digest: configuration_digest.into(),
            });
        let mut state = self
            .state
            .lock()
            .map_err(|_| MachineRuntimeRegistryError::Poisoned)?;
        let root = open_trusted_registry_root(&self.root)?;
        validate_registry_root(&root)?;
        let _admission_gate = lock_registry_gate(&root)?;
        let key = &expected.resource_id;
        reject_delete_fence(&root, key)?;
        let namespace = match child_directory(
            &root,
            "topology-machines",
            mode == MachineRuntimeAdmission::CreateOrOpen,
        ) {
            Ok(namespace) => namespace,
            Err(MachineRuntimeRegistryError::Io(error))
                if mode == MachineRuntimeAdmission::ExistingOnly
                    && error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Err(MachineRuntimeRegistryError::NotFound(key.clone()));
            }
            Err(error) => return Err(error),
        };
        let directory = match openat(&namespace, key.as_str(), DIRECTORY_FLAGS, Mode::empty()) {
            Ok(fd) => File::from(fd),
            Err(rustix::io::Errno::NOENT) if mode == MachineRuntimeAdmission::CreateOrOpen => {
                publish_directory(
                    &namespace,
                    key,
                    creating_manifest.as_ref().ok_or_else(|| {
                        MachineRuntimeRegistryError::Invalid(
                            "creating a Machine runtime store requires a resolved configuration digest"
                                .into(),
                        )
                    })?,
                )?
            }
            Err(rustix::io::Errno::NOENT) => {
                return Err(MachineRuntimeRegistryError::NotFound(key.clone()));
            }
            Err(error) => return Err(error.into()),
        };
        validate_private_directory(&directory)?;
        let actual = read_manifest(&directory)?;
        validate_persisted_manifest(&actual, owner, &expected)?;
        if expected_configuration_digest.is_some_and(|digest| digest != actual.configuration_digest)
        {
            return Err(MachineRuntimeRegistryError::Conflict(
                "persisted owner or resolved configuration differs".into(),
            ));
        }
        let data = child_directory(&directory, DATA_NAME, false)?;
        let data_path = self
            .root
            .join("topology-machines")
            .join(key)
            .join(DATA_NAME);
        // Reopen through the configured path immediately before using the
        // backend. Path-based backend I/O must still name the exact retained
        // data directory, even if a writable ancestor was replaced.
        let current = open_absolute_directory(&data_path)?;
        if !same_file(&current, &data)? {
            return Err(MachineRuntimeRegistryError::Conflict(
                "runtime data ancestry changed during admission".into(),
            ));
        }
        if let Some(lease) = state.leases.get(key) {
            if lease.manifest != actual
                || !same_file(&lease.directory, &directory)?
                || !same_file(&lease.data_directory, &data)?
            {
                return Err(MachineRuntimeRegistryError::Conflict(
                    "leased runtime directory or data directory was replaced".into(),
                ));
            }
            return Ok(Arc::clone(lease));
        }
        fs2::FileExt::try_lock_exclusive(&directory).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                MachineRuntimeRegistryError::Leased(key.clone())
            } else {
                error.into()
            }
        })?;
        let lease = Arc::new(MachineRuntimeStoreLease {
            manifest: actual,
            registry_root: self.root.clone(),
            data_path,
            directory,
            data_directory: data,
        });
        state.leases.insert(
            lease.manifest.reservation.resource_id.clone(),
            Arc::clone(&lease),
        );
        Ok(lease)
    }

    /// Attaches one runtime to a lease acquired from this exact registry.
    ///
    /// The factory is serialized with all other operations on this registry
    /// and runs only after the cached lease and current filesystem identities
    /// have been revalidated. A failed factory leaves the runtime-free lease
    /// retained and can be retried without reopening or republishing the store.
    pub fn attach_runtime(
        &self,
        lease: Arc<MachineRuntimeStoreLease>,
        factory: impl FnOnce(&Path) -> Result<R, MachineRuntimeRegistryError>,
    ) -> Result<Arc<MachineRuntimeEntry<R>>, MachineRuntimeRegistryError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MachineRuntimeRegistryError::Poisoned)?;
        let key = lease.manifest.reservation.resource_id.clone();
        let Some(cached_lease) = state.leases.get(&key) else {
            return Err(MachineRuntimeRegistryError::Conflict(
                "runtime store lease was not acquired by this registry".into(),
            ));
        };
        if !Arc::ptr_eq(cached_lease, &lease) {
            return Err(MachineRuntimeRegistryError::Conflict(
                "runtime store lease does not match this registry's exact lease".into(),
            ));
        }
        let root = open_trusted_registry_root(&self.root)?;
        let _admission_gate = lock_registry_gate(&root)?;
        reject_delete_fence(&root, &key)?;
        lease.validate_current()?;
        if let Some(entry) = state.entries.get(&key) {
            if !Arc::ptr_eq(&entry.lease, &lease) {
                return Err(MachineRuntimeRegistryError::Conflict(
                    "attached runtime does not retain the exact store lease".into(),
                ));
            }
            return Ok(Arc::clone(entry));
        }
        let runtime = factory(lease.data_path())?;
        let entry = Arc::new(MachineRuntimeEntry { runtime, lease });
        state.entries.insert(key, Arc::clone(&entry));
        Ok(entry)
    }

    /// Compatibility wrapper that acquires the exact store and attaches its
    /// runtime in one serialized admission.
    pub fn admit(
        &self,
        owner: &ResourceOwner,
        reservation: &OwnershipRecord,
        configuration_digest: &str,
        mode: MachineRuntimeAdmission,
        factory: impl FnOnce(&Path) -> Result<R, MachineRuntimeRegistryError>,
    ) -> Result<Arc<MachineRuntimeEntry<R>>, MachineRuntimeRegistryError> {
        let lease = self.acquire_store(owner, reservation, Some(configuration_digest), mode)?;
        self.attach_runtime(lease, factory)
    }

    /// Checks an entire current private tree without writing files or creating
    /// a backend. Call for every sibling before admitting any Delete effects.
    /// A prior deletion is discovered from its outside-tree immutable intent.
    pub(crate) fn preflight_delete(
        &self,
        owner: &ResourceOwner,
        reservation: &OwnershipRecord,
    ) -> Result<MachineStoreDeletePreflight, MachineRuntimeRegistryError> {
        if Self::reservation(owner)? != *reservation {
            return Err(delete_conflict(
                "Delete reservation is not the exact Machine store",
            ));
        }
        let root = open_trusted_registry_root(&self.root)?;
        validate_registry_root(&root)?;
        if let Some(directory) = delete_intent_directory(&root, &reservation.resource_id)? {
            let intent: MachineStoreDeleteIntent = read_delete_record(&directory, DELETE_INTENT)?;
            validate_delete_intent(&intent, owner, reservation, &root)?;
            let namespace = child_directory(&root, "topology-machines", false)?;
            let remaining =
                open_delete_store(&namespace, &reservation.resource_id, &directory, &intent)?;
            if let Some(receipt) =
                optional_delete_record::<MachineStoreDeleteReceipt>(&directory, DELETE_RECEIPT)?
            {
                if receipt != delete_receipt(&intent)? || remaining.is_some() {
                    return Err(delete_conflict(
                        "completed Delete receipt conflicts with the exact tree",
                    ));
                }
            }
            if let Some(remaining) = remaining {
                walk_owned_tree(&remaining, false, &mut DeleteWalk::new(), 0)?;
            }
            return Ok(MachineStoreDeletePreflight {
                manifest: intent.manifest.clone(),
                registry_root: self.root.clone(),
                lease: None,
                pending: Some(intent),
            });
        }
        let lease = self.acquire_store(
            owner,
            reservation,
            None,
            MachineRuntimeAdmission::ExistingOnly,
        )?;
        walk_owned_tree(&lease.directory, false, &mut DeleteWalk::new(), 0)?;
        lease.validate_current()?;
        Ok(MachineStoreDeletePreflight {
            manifest: lease.manifest.clone(),
            registry_root: self.root.clone(),
            lease: Some(lease),
            pending: None,
        })
    }

    /// Admits exact irreversible removal only after the durable Delete journal,
    /// positive original-runtime quiescence and owned Docker context cleanup.
    /// This permanently fences the old Machine store identity against acquire.
    #[cfg(target_os = "macos")]
    pub(crate) fn begin_delete(
        &self,
        claim: MachineStoreDeletePreflight,
        operation: &EnvironmentLifecycleOperation,
        quiescence: crate::machine_live_sessions::MachineDeleteQuiescence,
    ) -> Result<MachineStoreDeletion<'_, R>, MachineRuntimeRegistryError> {
        if claim.registry_root != self.root {
            return Err(delete_conflict(
                "Delete preflight belongs to another registry root",
            ));
        }
        claim.matches_operation(operation)?;
        quiescence
            .require_store(&claim, operation)
            .map_err(|error| delete_conflict(&error.to_string()))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| MachineRuntimeRegistryError::Poisoned)?;
        let key = &claim.manifest.reservation.resource_id;
        let root = open_trusted_registry_root(&self.root)?;
        validate_registry_root(&root)?;
        let _gate = lock_registry_gate(&root)?;
        let namespace = child_directory(&root, "topology-machines", false)?;
        let entry = state.entries.get(key);
        if let Some(entry) = entry {
            if Arc::strong_count(entry) != 1
                || quiescence.runtime_entry_address() != Some(Arc::as_ptr(entry) as usize)
            {
                return Err(delete_conflict(
                    "original runtime still has readers or differs from quiescence authority",
                ));
            }
        } else if claim.pending.is_none() && quiescence.runtime_entry_address().is_some() {
            return Err(delete_conflict(
                "quiesced runtime is not owned by this registry",
            ));
        }
        if let Some(lease) = state.leases.get(key) {
            let expected_refs =
                1 + usize::from(entry.is_some()) + usize::from(claim.lease.is_some());
            if Arc::strong_count(lease) != expected_refs
                || claim
                    .lease
                    .as_ref()
                    .is_some_and(|claimed| !Arc::ptr_eq(claimed, lease))
            {
                return Err(delete_conflict(
                    "Machine store still has external lease readers",
                ));
            }
        } else if claim.lease.is_some() {
            return Err(delete_conflict(
                "preflight lease was not retained by this registry",
            ));
        }
        let (intent, intent_directory, store) = if let Some(expected) = &claim.pending {
            let directory = delete_intent_directory(&root, key)?
                .ok_or_else(|| delete_conflict("persisted Delete intent disappeared"))?;
            let actual: MachineStoreDeleteIntent = read_delete_record(&directory, DELETE_INTENT)?;
            if actual != *expected {
                return Err(delete_conflict("persisted Delete intent changed"));
            }
            validate_delete_intent(&actual, claim.owner(), &claim.manifest.reservation, &root)?;
            actual.namespace.require(&namespace)?;
            let store = open_delete_store(&namespace, key, &directory, &actual)?;
            if let Some(store) = &store {
                if !state.leases.contains_key(key) {
                    fs2::FileExt::try_lock_exclusive(store).map_err(|error| {
                        if error.kind() == std::io::ErrorKind::WouldBlock {
                            MachineRuntimeRegistryError::Leased(key.clone())
                        } else {
                            error.into()
                        }
                    })?;
                }
            }
            (actual, directory, store)
        } else {
            reject_delete_fence(&root, key)?;
            let lease = claim
                .lease
                .as_ref()
                .ok_or_else(|| delete_conflict("missing exact live store lease"))?;
            lease.validate_current()?;
            // Host-side context removal must be positively verified before
            // deleting its intentionally retained claim and cleanup journal.
            crate::machine_docker_context::require_deleted_for_store(lease, operation).map_err(
                |error| {
                    delete_conflict(&format!(
                        "owned Docker context cleanup is not complete: {error:#}"
                    ))
                },
            )?;
            walk_owned_tree(&lease.directory, false, &mut DeleteWalk::new(), 0)?;
            let intent = MachineStoreDeleteIntent {
                schema_version: 1,
                manifest: claim.manifest.clone(),
                operation: DeleteOperationIdentity::from_operation(&claim.manifest, operation)?,
                root: MachineStoreFileIdentity::of(&root)?,
                namespace: MachineStoreFileIdentity::of(&namespace)?,
                store: MachineStoreFileIdentity::of(&lease.directory)?,
                data: MachineStoreFileIdentity::of(&lease.data_directory)?,
                quiescence: quiescence.evidence(),
            };
            let directory = publish_delete_intent(&root, key, &intent)?;
            (intent, directory, Some(lease.directory.try_clone()?))
        };
        // Only the registry's final strong runtime reference may be retired.
        // The sealed token retains its Weak identity and controller fence.
        state.entries.remove(key);
        Ok(MachineStoreDeletion {
            registry: self,
            claim,
            intent,
            intent_directory,
            store,
            _quiescence: quiescence,
        })
    }
}

#[cfg(target_os = "macos")]
impl<R> MachineStoreDeletion<'_, R> {
    /// Removes only the inode-bound private tree. A failure leaves its immutable
    /// intent and any unremoved contents in place for exact-operation recovery.
    pub(crate) fn remove(self) -> Result<MachineStoreDeleteReceipt, MachineRuntimeRegistryError> {
        let root = open_trusted_registry_root(&self.registry.root)?;
        self.intent.root.require(&root)?;
        let _gate = lock_registry_gate(&root)?;
        let namespace = child_directory(&root, "topology-machines", false)?;
        self.intent.namespace.require(&namespace)?;
        let key = &self.intent.manifest.reservation.resource_id;
        let attached = delete_intent_directory(&root, key)?
            .ok_or_else(|| delete_conflict("Delete intent directory disappeared"))?;
        if !same_file(&attached, &self.intent_directory)?
            || read_delete_record::<MachineStoreDeleteIntent>(&attached, DELETE_INTENT)?
                != self.intent
        {
            return Err(delete_conflict("Delete intent was replaced"));
        }
        let receipt = delete_receipt(&self.intent)?;
        if let Some(existing) =
            optional_delete_record::<MachineStoreDeleteReceipt>(&attached, DELETE_RECEIPT)?
        {
            if existing != receipt
                || open_delete_store(&namespace, key, &attached, &self.intent)?.is_some()
            {
                return Err(delete_conflict(
                    "completed deletion receipt conflicts with current tree or intent",
                ));
            }
            return Ok(existing);
        }
        if let Some(current) = open_delete_store(&namespace, key, &attached, &self.intent)? {
            let retained = self
                .store
                .as_ref()
                .ok_or_else(|| delete_conflict("tree appeared after absent-store admission"))?;
            if !same_file(retained, &current)? {
                return Err(delete_conflict("retained deletion tree was replaced"));
            }
            if optional_directory(&namespace, key)?.is_some() {
                renameat_with(
                    &namespace,
                    key.as_str(),
                    &attached,
                    DELETE_TREE,
                    RenameFlags::NOREPLACE,
                )?;
                namespace.sync_all()?;
                attached.sync_all()?;
            }
            // Reopen after rename, and never follow a replacement, mount, or
            // symlink. No file chmod is performed (including hardlinked files).
            let moved = File::from(openat(
                &attached,
                DELETE_TREE,
                DIRECTORY_FLAGS,
                Mode::empty(),
            )?);
            self.intent.store.require(&moved)?;
            walk_owned_tree(&moved, true, &mut DeleteWalk::new(), 0)?;
            require_child_identity(&attached, DELETE_TREE, &self.intent.store)?;
            unlinkat(&attached, DELETE_TREE, AtFlags::REMOVEDIR)?;
            attached.sync_all()?;
        }
        if open_delete_store(&namespace, key, &attached, &self.intent)?.is_some() {
            return Err(delete_conflict("Machine tree remains after removal"));
        }
        publish_delete_record(&attached, DELETE_RECEIPT, &receipt)?;
        let mut state = self
            .registry
            .state
            .lock()
            .map_err(|_| MachineRuntimeRegistryError::Poisoned)?;
        if state.entries.contains_key(key) {
            return Err(delete_conflict("runtime reattached during deletion"));
        }
        state.leases.remove(key);
        // Keep the claim and its descriptors alive through receipt fsync.
        drop(self.claim);
        Ok(receipt)
    }
}

fn delete_conflict(message: &str) -> MachineRuntimeRegistryError {
    MachineRuntimeRegistryError::Conflict(message.into())
}

// A root descriptor lock serializes the short publication/admission decision
// across registry instances. The permanent per-owner intent is the lasting
// fence; no global daemon lock file or path alias is adopted.
fn lock_registry_gate(root: &File) -> Result<File, MachineRuntimeRegistryError> {
    let gate = root.try_clone()?;
    fs2::FileExt::try_lock_exclusive(&gate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            MachineRuntimeRegistryError::Leased("runtime registry admission/deletion gate".into())
        } else {
            error.into()
        }
    })?;
    Ok(gate)
}

fn optional_directory(
    parent: &File,
    name: &str,
) -> Result<Option<File>, MachineRuntimeRegistryError> {
    match openat(parent, name, DIRECTORY_FLAGS, Mode::empty()) {
        Ok(fd) => Ok(Some(File::from(fd))),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn delete_intent_directory(
    root: &File,
    key: &str,
) -> Result<Option<File>, MachineRuntimeRegistryError> {
    let Some(namespace) = optional_directory(root, DELETE_NAMESPACE)? else {
        return Ok(None);
    };
    validate_private_directory(&namespace)?;
    let directory = optional_directory(&namespace, key)?;
    if let Some(directory) = &directory {
        validate_private_directory(directory)?;
    }
    Ok(directory)
}

fn reject_delete_fence(root: &File, key: &str) -> Result<(), MachineRuntimeRegistryError> {
    if delete_intent_directory(root, key)?.is_some() {
        return Err(delete_conflict(
            "Machine store has a permanent admitted Delete fence",
        ));
    }
    Ok(())
}

fn read_delete_record<T: serde::de::DeserializeOwned>(
    directory: &File,
    name: &str,
) -> Result<T, MachineRuntimeRegistryError> {
    let mut bytes = Vec::new();
    private_file(directory, name)?
        .take(MAX_DELETE_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_DELETE_RECORD_BYTES {
        return Err(delete_conflict("Delete record exceeds bounded size"));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| delete_conflict(&format!("invalid durable Delete record: {error}")))
}

fn optional_delete_record<T: serde::de::DeserializeOwned>(
    directory: &File,
    name: &str,
) -> Result<Option<T>, MachineRuntimeRegistryError> {
    match statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => read_delete_record(directory, name).map(Some),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn publish_delete_record<T: Serialize>(
    directory: &File,
    name: &str,
    record: &T,
) -> Result<(), MachineRuntimeRegistryError> {
    let bytes = serde_json::to_vec(record).map_err(invalid)?;
    if bytes.len() as u64 > MAX_DELETE_RECORD_BYTES {
        return Err(delete_conflict("Delete record exceeds bounded size"));
    }
    let pending = format!(".pending-{}", LifecycleOperationId::generate());
    let mut file = File::from(openat(
        directory,
        pending.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        PRIVATE_FILE,
    )?);
    file.write_all(&bytes)?;
    file.sync_all()?;
    renameat_with(
        directory,
        pending.as_str(),
        directory,
        name,
        RenameFlags::NOREPLACE,
    )?;
    directory.sync_all()?;
    Ok(())
}

fn publish_delete_intent(
    root: &File,
    key: &str,
    intent: &MachineStoreDeleteIntent,
) -> Result<File, MachineRuntimeRegistryError> {
    let namespace = child_directory(root, DELETE_NAMESPACE, true)?;
    let pending = format!(".pending-{}", LifecycleOperationId::generate());
    mkdirat(&namespace, pending.as_str(), PRIVATE_DIRECTORY)?;
    let directory = child_directory(&namespace, &pending, false)?;
    publish_delete_record(&directory, DELETE_INTENT, intent)?;
    renameat_with(
        &namespace,
        pending.as_str(),
        &namespace,
        key,
        RenameFlags::NOREPLACE,
    )?;
    namespace.sync_all()?;
    Ok(directory)
}

fn validate_delete_intent(
    intent: &MachineStoreDeleteIntent,
    owner: &ResourceOwner,
    reservation: &OwnershipRecord,
    root: &File,
) -> Result<(), MachineRuntimeRegistryError> {
    if intent.schema_version != 1 || intent.quiescence.is_null() {
        return Err(delete_conflict(
            "unsupported Delete intent or missing positive quiescence evidence",
        ));
    }
    validate_persisted_manifest(&intent.manifest, owner, reservation)?;
    intent.root.require(root)?;
    let namespace = child_directory(root, "topology-machines", false)?;
    intent.namespace.require(&namespace)?;
    Ok(())
}

fn require_child_identity(
    parent: &File,
    name: &str,
    expected: &MachineStoreFileIdentity,
) -> Result<(), MachineRuntimeRegistryError> {
    let stat = statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
    if MachineStoreFileIdentity::from_stat(&stat) != *expected {
        return Err(delete_conflict("child changed during exact-owned removal"));
    }
    Ok(())
}

fn open_delete_store(
    namespace: &File,
    key: &str,
    intent_directory: &File,
    intent: &MachineStoreDeleteIntent,
) -> Result<Option<File>, MachineRuntimeRegistryError> {
    let original = optional_directory(namespace, key)?;
    let moved = optional_directory(intent_directory, DELETE_TREE)?;
    if original.is_some() && moved.is_some() {
        return Err(delete_conflict(
            "both original and quarantined Machine stores exist",
        ));
    }
    let not_yet_moved = original.is_some();
    let current = original.or(moved);
    if let Some(current) = &current {
        intent.store.require(current)?;
        if let Some(data) = optional_directory(current, DATA_NAME)? {
            intent.data.require(&data)?;
        } else if not_yet_moved {
            return Err(delete_conflict(
                "original store data disappeared before quarantine",
            ));
        }
        if not_yet_moved && read_manifest(current)? != intent.manifest {
            return Err(delete_conflict(
                "original store owner changed before quarantine",
            ));
        }
    }
    Ok(current)
}

fn delete_receipt(
    intent: &MachineStoreDeleteIntent,
) -> Result<MachineStoreDeleteReceipt, MachineRuntimeRegistryError> {
    let encoded = serde_json::to_vec(intent).map_err(invalid)?;
    Ok(MachineStoreDeleteReceipt {
        schema_version: 1,
        owner: intent.manifest.owner.clone(),
        operation_id: intent.operation.operation_id.clone(),
        generation: intent.operation.generation,
        configuration_digest: intent.manifest.configuration_digest.clone(),
        store: intent.store.clone(),
        data: intent.data.clone(),
        intent_sha256: format!("sha256:{:x}", Sha256::digest(encoded)),
        store_removed: true,
    })
}

struct DeleteWalk {
    remaining: usize,
    deadline: Instant,
    device: Option<u64>,
}
impl DeleteWalk {
    fn new() -> Self {
        Self {
            remaining: MAX_DELETE_ENTRIES,
            deadline: Instant::now() + DELETE_WALK_LIMIT,
            device: None,
        }
    }
    fn check(&mut self, depth: usize) -> Result<(), MachineRuntimeRegistryError> {
        if depth > MAX_DELETE_DEPTH || self.remaining == 0 || Instant::now() >= self.deadline {
            return Err(delete_conflict(
                "bounded private-tree deletion requires continuation",
            ));
        }
        self.remaining -= 1;
        Ok(())
    }
}

fn walk_owned_tree(
    directory: &File,
    remove: bool,
    walk: &mut DeleteWalk,
    depth: usize,
) -> Result<(), MachineRuntimeRegistryError> {
    walk.check(depth)?;
    let stat = fstat(directory)?;
    let directory_identity = MachineStoreFileIdentity::from_stat(&stat);
    let device = *walk.device.get_or_insert(directory_identity.device);
    let mode = Mode::from_raw_mode(stat.st_mode);
    if stat.st_uid != rustix::process::geteuid().as_raw()
        || directory_identity.device != device
        || !FileType::from_raw_mode(stat.st_mode).is_dir()
        || mode.intersects(Mode::WGRP | Mode::WOTH | Mode::SUID | Mode::SGID | Mode::SVTX)
    {
        return Err(delete_conflict(
            "private tree contains an unowned, writable, special or cross-device directory",
        ));
    }
    if remove {
        fchmod(directory, PRIVATE_DIRECTORY)?;
    }
    for entry in rustix::fs::Dir::read_from(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        walk.check(depth)?;
        let child = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)?;
        let identity = MachineStoreFileIdentity::from_stat(&child);
        if child.st_uid != rustix::process::geteuid().as_raw() || identity.device != device {
            return Err(delete_conflict(
                "private tree contains an unowned or cross-device entry",
            ));
        }
        let kind = FileType::from_raw_mode(child.st_mode);
        if kind.is_dir() {
            let child_directory =
                File::from(openat(directory, name, DIRECTORY_FLAGS, Mode::empty())?);
            identity.require(&child_directory)?;
            walk_owned_tree(&child_directory, remove, walk, depth + 1)?;
        } else if matches!(kind, FileType::BlockDevice | FileType::CharacterDevice) {
            return Err(delete_conflict(
                "private host store unexpectedly contains a device node",
            ));
        }
        if remove {
            let current = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)?;
            if current.st_dev != child.st_dev || current.st_ino != child.st_ino {
                return Err(delete_conflict(
                    "private tree entry replaced during removal",
                ));
            }
            unlinkat(
                directory,
                name,
                if kind.is_dir() {
                    AtFlags::REMOVEDIR
                } else {
                    AtFlags::empty()
                },
            )?;
        }
    }
    if remove {
        directory.sync_all()?;
    }
    Ok(())
}

fn validate_current_lease(
    lease: &MachineRuntimeStoreLease,
) -> Result<(), MachineRuntimeRegistryError> {
    let root = open_trusted_registry_root(&lease.registry_root)?;
    validate_registry_root(&root)?;
    reject_delete_fence(&root, &lease.manifest.reservation.resource_id)?;
    let namespace = child_directory(&root, "topology-machines", false)?;
    let key = &lease.manifest.reservation.resource_id;
    let directory = File::from(openat(
        &namespace,
        key.as_str(),
        DIRECTORY_FLAGS,
        Mode::empty(),
    )?);
    validate_private_directory(&directory)?;
    if read_manifest(&directory)? != lease.manifest || !same_file(&directory, &lease.directory)? {
        return Err(MachineRuntimeRegistryError::Conflict(
            "leased runtime directory or owner manifest was replaced".into(),
        ));
    }
    let data = child_directory(&directory, DATA_NAME, false)?;
    let current = open_absolute_directory(lease.data_path())?;
    if !same_file(&data, lease.data_directory())? || !same_file(&current, lease.data_directory())? {
        return Err(MachineRuntimeRegistryError::Conflict(
            "leased runtime data directory was replaced".into(),
        ));
    }
    Ok(())
}

fn validate_configuration_digest(digest: &str) -> Result<(), MachineRuntimeRegistryError> {
    let value = digest.strip_prefix("sha256:").unwrap_or("");
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MachineRuntimeRegistryError::Invalid(
            "resolved configuration requires a canonical SHA-256 digest".into(),
        ));
    }
    Ok(())
}

fn validate_persisted_manifest(
    manifest: &OwnerManifest,
    owner: &ResourceOwner,
    reservation: &OwnershipRecord,
) -> Result<(), MachineRuntimeRegistryError> {
    if manifest.schema_version != TOPOLOGY_SCHEMA_VERSION
        || manifest.owner != *owner
        || manifest.reservation != *reservation
    {
        return Err(MachineRuntimeRegistryError::Conflict(
            "persisted runtime store owner or reservation differs".into(),
        ));
    }
    validate_configuration_digest(&manifest.configuration_digest).map_err(|_| {
        MachineRuntimeRegistryError::Conflict(
            "persisted runtime store configuration digest is not canonical".into(),
        )
    })
}

fn invalid(error: impl std::fmt::Display) -> MachineRuntimeRegistryError {
    MachineRuntimeRegistryError::Invalid(error.to_string())
}

fn absolute_path_without_parent_traversal(
    path: &Path,
) -> Result<PathBuf, MachineRuntimeRegistryError> {
    let absolute = std::path::absolute(path)?;
    if !absolute
        .components()
        .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(MachineRuntimeRegistryError::Invalid(
            "runtime root must be absolute and must not contain parent traversal".into(),
        ));
    }
    Ok(absolute)
}

fn open_absolute_directory(path: &Path) -> Result<File, MachineRuntimeRegistryError> {
    if !path.is_absolute() {
        return Err(MachineRuntimeRegistryError::Invalid(
            "runtime path must be absolute".into(),
        ));
    }
    let mut directory = File::from(rustix::fs::open("/", DIRECTORY_FLAGS, Mode::empty())?);
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = File::from(openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())?)
            }
            _ => {
                return Err(MachineRuntimeRegistryError::Invalid(
                    "runtime path must not contain parent traversal".into(),
                ));
            }
        }
    }
    Ok(directory)
}

pub(crate) fn open_trusted_registry_root(path: &Path) -> Result<File, MachineRuntimeRegistryError> {
    if !path.is_absolute() {
        return Err(MachineRuntimeRegistryError::Invalid(
            "runtime path must be absolute".into(),
        ));
    }
    let expected_uid = rustix::process::geteuid().as_raw();
    let mut directory = File::from(rustix::fs::open("/", DIRECTORY_FLAGS, Mode::empty())?);
    validate_trusted_ancestry_component(&directory, expected_uid)?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                let child = File::from(openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())?);
                validate_trusted_ancestry_edge(&directory, &child, expected_uid)?;
                directory = child;
            }
            _ => {
                return Err(MachineRuntimeRegistryError::Invalid(
                    "runtime path must not contain parent traversal".into(),
                ));
            }
        }
    }
    Ok(directory)
}

fn validate_trusted_ancestry_component(
    directory: &File,
    expected_uid: u32,
) -> Result<(), MachineRuntimeRegistryError> {
    let metadata = fstat(directory)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir()
        || (metadata.st_uid != 0 && metadata.st_uid != expected_uid)
    {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime root ancestry must contain only root- or effective-user-owned directories"
                .into(),
        ));
    }
    Ok(())
}

fn validate_trusted_ancestry_edge(
    parent: &File,
    child: &File,
    expected_uid: u32,
) -> Result<(), MachineRuntimeRegistryError> {
    validate_trusted_ancestry_component(parent, expected_uid)?;
    validate_trusted_ancestry_component(child, expected_uid)?;
    let parent_metadata = fstat(parent)?;
    let parent_mode = Mode::from_raw_mode(parent_metadata.st_mode);
    if parent_mode.intersects(Mode::WGRP | Mode::WOTH) && !parent_mode.contains(Mode::SVTX) {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime root ancestry must not contain a non-sticky group/world-writable directory"
                .into(),
        ));
    }
    Ok(())
}

fn child_directory(
    parent: &File,
    name: &str,
    create: bool,
) -> Result<File, MachineRuntimeRegistryError> {
    if create {
        match mkdirat(parent, name, PRIVATE_DIRECTORY) {
            Ok(()) => parent.sync_all()?,
            Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let directory = File::from(openat(parent, name, DIRECTORY_FLAGS, Mode::empty())?);
    validate_private_directory(&directory)?;
    Ok(directory)
}

fn validate_private_directory(directory: &File) -> Result<(), MachineRuntimeRegistryError> {
    validate_private_directory_for_uid(directory, rustix::process::geteuid().as_raw())
}

fn validate_registry_root(directory: &File) -> Result<(), MachineRuntimeRegistryError> {
    let metadata = fstat(directory)?;
    let mode = Mode::from_raw_mode(metadata.st_mode).as_raw_mode();
    if !FileType::from_raw_mode(metadata.st_mode).is_dir()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || mode & 0o7022 != 0
    {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime root must be owned by the effective user, have no special mode bits, and not be group/world-writable"
                .into(),
        ));
    }
    Ok(())
}

fn validate_private_directory_for_uid(
    directory: &File,
    expected_uid: u32,
) -> Result<(), MachineRuntimeRegistryError> {
    let metadata = fstat(directory)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir()
        || Mode::from_raw_mode(metadata.st_mode) != PRIVATE_DIRECTORY
        || metadata.st_uid != expected_uid
    {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime directory must be owned by the effective user with exact mode 0700".into(),
        ));
    }
    Ok(())
}

fn private_file(directory: &File, name: &str) -> Result<File, MachineRuntimeRegistryError> {
    let file = File::from(openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )?);
    let metadata = fstat(&file)?;
    let expected_uid = rustix::process::geteuid().as_raw();
    if !FileType::from_raw_mode(metadata.st_mode).is_file()
        || metadata.st_nlink != 1
        || Mode::from_raw_mode(metadata.st_mode) != PRIVATE_FILE
        || metadata.st_uid != expected_uid
    {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime manifest must be an effective-user-owned private single-link regular file"
                .into(),
        ));
    }
    Ok(file)
}

fn read_manifest(directory: &File) -> Result<OwnerManifest, MachineRuntimeRegistryError> {
    let file = private_file(directory, MANIFEST_NAME)?;
    if u64::try_from(fstat(&file)?.st_size).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime owner manifest exceeds size limit".into(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(MachineRuntimeRegistryError::Conflict(
            "runtime owner manifest grew beyond size limit".into(),
        ));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        MachineRuntimeRegistryError::Conflict(format!(
            "persisted runtime owner manifest is malformed: {error}"
        ))
    })
}

fn same_file(left: &File, right: &File) -> Result<bool, rustix::io::Errno> {
    let left = fstat(left)?;
    let right = fstat(right)?;
    Ok(left.st_dev == right.st_dev && left.st_ino == right.st_ino)
}

fn publish_directory(
    parent: &File,
    key: &str,
    manifest: &OwnerManifest,
) -> Result<File, MachineRuntimeRegistryError> {
    let pending = format!(".pending-{}", LifecycleOperationId::generate());
    mkdirat(parent, pending.as_str(), PRIVATE_DIRECTORY)?;
    let directory = match openat(parent, pending.as_str(), DIRECTORY_FLAGS, Mode::empty()) {
        Ok(fd) => File::from(fd),
        Err(error) => {
            let _ = unlinkat(parent, pending.as_str(), AtFlags::REMOVEDIR);
            let _ = parent.sync_all();
            return Err(error.into());
        }
    };
    let result = (|| {
        validate_private_directory(&directory)?;
        let mut file = File::from(openat(
            &directory,
            MANIFEST_NAME,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            PRIVATE_FILE,
        )?);
        let encoded = serde_json::to_vec(manifest).map_err(|error| {
            MachineRuntimeRegistryError::Invalid(format!(
                "failed to encode runtime owner manifest: {error}"
            ))
        })?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        mkdirat(&directory, DATA_NAME, PRIVATE_DIRECTORY)?;
        let data = File::from(openat(
            &directory,
            DATA_NAME,
            DIRECTORY_FLAGS,
            Mode::empty(),
        )?);
        validate_private_directory(&data)?;
        directory.sync_all()?;
        renameat_with(
            parent,
            pending.as_str(),
            parent,
            key,
            RenameFlags::NOREPLACE,
        )?;
        parent.sync_all()?;
        Ok::<_, MachineRuntimeRegistryError>(())
    })();
    if let Err(error) = result {
        // Only remove our still-attached unpublished directory, and only known
        // files/empty data. Never recursively remove or overwrite a contender.
        let metadata = fstat(&directory)?;
        if statat(parent, pending.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .is_ok_and(|stat| stat.st_dev == metadata.st_dev && stat.st_ino == metadata.st_ino)
        {
            let _ = unlinkat(&directory, MANIFEST_NAME, AtFlags::empty());
            let _ = unlinkat(&directory, DATA_NAME, AtFlags::REMOVEDIR);
            let _ = unlinkat(parent, pending.as_str(), AtFlags::REMOVEDIR);
            let _ = parent.sync_all();
        }
        if matches!(&error, MachineRuntimeRegistryError::Io(io) if io.kind() == std::io::ErrorKind::AlreadyExists)
        {
            return Ok(File::from(openat(
                parent,
                key,
                DIRECTORY_FLAGS,
                Mode::empty(),
            )?));
        }
        return Err(error);
    }
    Ok(directory)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::TempDir;

    use super::*;

    const DIGEST_A: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn owner() -> ResourceOwner {
        ResourceOwner {
            project_id: ProjectId::new("prj_registry_test").expect("valid Project ID"),
            environment_id: EnvironmentId::new("env_registry_test").expect("valid Environment ID"),
            machine_id: Some(MachineId::new("mch_registry_test").expect("valid Machine ID")),
        }
    }

    fn reservation() -> OwnershipRecord {
        MachineRuntimeRegistry::<usize>::reservation(&owner()).expect("valid reservation")
    }

    fn store_path(root: &Path) -> PathBuf {
        root.join("topology-machines")
            .join(reservation().resource_id)
    }

    fn registry(root: &Path) -> MachineRuntimeRegistry<usize> {
        MachineRuntimeRegistry::new(fs::canonicalize(root).expect("canonical registry root"))
            .expect("valid registry root")
    }

    fn admit(
        registry: &MachineRuntimeRegistry<usize>,
        digest: &str,
        mode: MachineRuntimeAdmission,
        value: usize,
    ) -> Result<Arc<MachineRuntimeEntry<usize>>, MachineRuntimeRegistryError> {
        registry.admit(&owner(), &reservation(), digest, mode, |_| Ok(value))
    }

    fn acquire(
        registry: &MachineRuntimeRegistry<usize>,
        digest: Option<&str>,
        mode: MachineRuntimeAdmission,
    ) -> Result<Arc<MachineRuntimeStoreLease>, MachineRuntimeRegistryError> {
        registry.acquire_store(&owner(), &reservation(), digest, mode)
    }

    fn assert_conflict<T>(result: Result<T, MachineRuntimeRegistryError>) {
        assert!(
            matches!(result, Err(MachineRuntimeRegistryError::Conflict(_))),
            "expected ownership conflict"
        );
    }

    fn stop_receipt() -> crate::machine_live_sessions::MachineSessionStopReceipt {
        crate::machine_live_sessions::MachineSessionStopReceipt {
            owner: owner(),
            operation_id: LifecycleOperationId::generate().to_string(),
            generation: 1,
            runtime_identity: vz_runtime_contract::StackRuntimeIdentity::new("vm-stop-proof")
                .unwrap(),
            endpoint: None,
            docker_shutdown: None,
            outcome: vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped,
        }
    }

    #[test]
    fn stop_receipt_is_exact_private_and_never_overwritten() {
        let temp = TempDir::new().unwrap();
        let registry = registry(temp.path());
        let entry = admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            1,
        )
        .unwrap();
        let receipt = stop_receipt();
        entry.persist_stop_receipt(&receipt).unwrap();
        let path = entry
            .data_path()
            .join("linux-lifecycle/stops")
            .join(format!("{}.json", receipt.operation_id));
        let original = fs::read(&path).unwrap();
        assert_eq!(original, serde_json::to_vec(&receipt).unwrap());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(entry.persist_stop_receipt(&receipt).is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
    }

    #[test]
    fn stop_receipt_rejects_foreign_absent_and_symlinked_evidence() {
        let temp = TempDir::new().unwrap();
        let registry = registry(temp.path());
        let entry = admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            1,
        )
        .unwrap();
        let mut receipt = stop_receipt();
        receipt.owner.machine_id = Some(MachineId::generate());
        assert!(entry.persist_stop_receipt(&receipt).is_err());
        receipt.owner = owner();
        receipt.outcome = vz_runtime_contract::StackRuntimeShutdownOutcome::AlreadyAbsent;
        assert!(entry.persist_stop_receipt(&receipt).is_err());
        assert!(!entry.data_path().join("linux-lifecycle").exists());
        receipt.outcome = vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped;
        let decoy = TempDir::new().unwrap();
        symlink(decoy.path(), entry.data_path().join("linux-lifecycle")).unwrap();
        assert!(entry.persist_stop_receipt(&receipt).is_err());
        assert_eq!(fs::read_dir(decoy.path()).unwrap().count(), 0);
    }

    #[test]
    fn stop_receipt_directory_replacement_is_not_publication_proof() {
        let temp = TempDir::new().unwrap();
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .unwrap();
        let target = child_directory(lease.data_directory(), "linux-lifecycle", true).unwrap();
        let stops = child_directory(&target, "stops", true).unwrap();
        validate_stop_receipt_directories(lease.data_directory(), &target, &stops).unwrap();
        let path = lease.data_path().join("linux-lifecycle");
        fs::rename(path.join("stops"), path.join("retained-stops")).unwrap();
        child_directory(&target, "stops", true).unwrap();
        assert!(
            validate_stop_receipt_directories(lease.data_directory(), &target, &stops).is_err()
        );
    }

    fn deletion_intent(lease: &MachineRuntimeStoreLease) -> MachineStoreDeleteIntent {
        let root =
            open_trusted_registry_root(&lease.registry_root).expect("verified registry root");
        let namespace =
            child_directory(&root, "topology-machines", false).expect("owned namespace");
        MachineStoreDeleteIntent {
            schema_version: 1,
            manifest: lease.manifest.clone(),
            operation: DeleteOperationIdentity {
                operation_id: LifecycleOperationId::generate(),
                generation: 2,
                request_id: "delete-request".into(),
                idempotency_key: "delete-key".into(),
                request_hash: DIGEST_A.into(),
                definition_digest: DIGEST_B.into(),
                initial_machine_state: MachineState::Stopped,
                expected_incarnation: None,
            },
            root: MachineStoreFileIdentity::of(&root).expect("root inode"),
            namespace: MachineStoreFileIdentity::of(&namespace).expect("namespace inode"),
            store: MachineStoreFileIdentity::of(&lease.directory).expect("store inode"),
            data: MachineStoreFileIdentity::of(&lease.data_directory).expect("data inode"),
            // Filesystem-helper tests never mint production quiescence tokens.
            quiescence: serde_json::json!({"test_only_positive_quiescence": true}),
        }
    }

    fn delete_operation() -> EnvironmentLifecycleOperation {
        use vz_runtime_contract::{
            EnvironmentLifecycleStatus, EnvironmentState, LifecycleStepStatus,
            MachineLifecycleStep, OwnershipCleanupStep,
        };
        EnvironmentLifecycleOperation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            operation_id: LifecycleOperationId::generate(),
            project_id: owner().project_id,
            environment_id: owner().environment_id,
            kind: EnvironmentLifecycleKind::Delete,
            generation: 2,
            request_id: "delete-request".into(),
            idempotency_key: "delete-key".into(),
            request_hash: DIGEST_A.into(),
            definition_digest: DIGEST_B.into(),
            initial_state: EnvironmentState::Stopped,
            requested_target: EnvironmentState::Deleted,
            status: EnvironmentLifecycleStatus::Running,
            machine_steps: vec![MachineLifecycleStep {
                machine_id: owner().machine_id.expect("Machine owner"),
                initial_state: MachineState::Stopped,
                target_state: None,
                expected_incarnation: None,
                resulting_incarnation: None,
                resulting_activation: None,
                status: LifecycleStepStatus::Succeeded,
                failure_reason: None,
            }],
            cleanup_steps: vec![OwnershipCleanupStep {
                ownership: reservation(),
                status: LifecycleStepStatus::Pending,
                failure_reason: None,
            }],
            created_at: 1,
            updated_at: 2,
            completed_at: None,
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn exact_runtime_free_delete_is_durable_and_cannot_resurrect() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let store = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        fs::write(store.data_path().join("owned"), b"private payload").expect("payload");
        drop(store);
        let operation = delete_operation();
        operation
            .validate_structure()
            .expect("valid Delete fixture");
        let controller =
            crate::environment_runtime_controller::EnvironmentRuntimeController::default();
        let lease = controller
            .acquire(&owner().project_id, &owner().environment_id)
            .await
            .expect("controller fence");
        let claim = registry
            .preflight_delete(&owner(), &reservation())
            .expect("preflight");
        let token = crate::machine_live_sessions::MachineDeleteQuiescence::for_runtime_free_test(
            &claim, &operation, &lease,
        )
        .expect("test-only positive authority");
        let receipt = registry
            .begin_delete(claim, &operation, token)
            .expect("admit exact Delete")
            .remove()
            .expect("remove exact store");
        assert!(receipt.store_removed);
        assert_eq!(receipt.owner, owner());
        assert_eq!(receipt.operation_id, operation.operation_id);
        assert_eq!(receipt.configuration_digest, DIGEST_A);
        assert!(!store_path(temp.path()).exists());
        assert_conflict(acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        ));
        let fresh =
            MachineRuntimeRegistry::<usize>::new(registry.root.clone()).expect("fresh registry");
        let claim = fresh
            .preflight_delete(&owner(), &reservation())
            .expect("outside-tree replay");
        let token = crate::machine_live_sessions::MachineDeleteQuiescence::for_runtime_free_test(
            &claim, &operation, &lease,
        )
        .expect("test-only exact replay authority");
        let replay = fresh
            .begin_delete(claim, &operation, token)
            .expect("admit replay")
            .remove()
            .expect("replay receipt");
        assert_eq!(receipt, replay);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn delete_rejects_extra_lease_before_publishing_intent() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let reader = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("retained reader");
        let operation = delete_operation();
        let controller =
            crate::environment_runtime_controller::EnvironmentRuntimeController::default();
        let lease = controller
            .acquire(&owner().project_id, &owner().environment_id)
            .await
            .expect("controller fence");
        let claim = registry
            .preflight_delete(&owner(), &reservation())
            .expect("preflight");
        let token = crate::machine_live_sessions::MachineDeleteQuiescence::for_runtime_free_test(
            &claim, &operation, &lease,
        )
        .expect("test-only positive authority");
        assert_conflict(registry.begin_delete(claim, &operation, token));
        assert!(reader.validate_current().is_ok());
        assert!(!temp.path().join(DELETE_NAMESPACE).exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn delete_resumes_admitted_intent_and_rejects_changed_operation() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        drop(
            acquire(
                &registry,
                Some(DIGEST_A),
                MachineRuntimeAdmission::CreateOrOpen,
            )
            .expect("store"),
        );
        let operation = delete_operation();
        let controller =
            crate::environment_runtime_controller::EnvironmentRuntimeController::default();
        let lease = controller
            .acquire(&owner().project_id, &owner().environment_id)
            .await
            .expect("controller fence");
        let claim = registry
            .preflight_delete(&owner(), &reservation())
            .expect("preflight");
        let token = crate::machine_live_sessions::MachineDeleteQuiescence::for_runtime_free_test(
            &claim, &operation, &lease,
        )
        .expect("test-only positive authority");
        drop(
            registry
                .begin_delete(claim, &operation, token)
                .expect("durable intent before first removal"),
        );
        assert!(store_path(temp.path()).exists());
        let claim = registry
            .preflight_delete(&owner(), &reservation())
            .expect("intent replay");
        let mut changed = operation.clone();
        changed.request_hash = DIGEST_B.into();
        assert_conflict(claim.matches_operation(&changed));
        let token = crate::machine_live_sessions::MachineDeleteQuiescence::for_runtime_free_test(
            &claim, &operation, &lease,
        )
        .expect("test-only exact replay authority");
        let receipt = registry
            .begin_delete(claim, &operation, token)
            .expect("resume exact intent")
            .remove()
            .expect("finish admitted removal");
        assert!(receipt.store_removed);
    }

    #[test]
    fn delete_preflight_is_read_only_and_does_not_construct_runtime() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        fs::write(lease.data_path().join("keep"), b"retained contents").expect("private payload");
        let manifest = fs::read(store_path(temp.path()).join(MANIFEST_NAME)).expect("owner bytes");
        let claim = registry
            .preflight_delete(&owner(), &reservation())
            .expect("read-only preflight");
        assert_eq!(claim.owner(), &owner());
        assert_eq!(claim.configuration_digest(), DIGEST_A);
        assert!(claim.quiescence_evidence().is_none());
        assert!(Arc::ptr_eq(claim.lease().expect("current lease"), &lease));
        assert!(registry.state.lock().expect("state").entries.is_empty());
        assert!(!temp.path().join(DELETE_NAMESPACE).exists());
        assert_eq!(
            fs::read(store_path(temp.path()).join(MANIFEST_NAME)).expect("owner remains"),
            manifest
        );
        assert_eq!(
            fs::read(lease.data_path().join("keep")).expect("payload remains"),
            b"retained contents"
        );
    }

    #[test]
    fn deletion_walker_preserves_external_symlink_and_hardlink_targets() {
        let temp = TempDir::new().expect("temporary root");
        let external = TempDir::new().expect("external user data");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        let external_file = external.path().join("user-file");
        fs::write(&external_file, b"user-owned bytes").expect("external file");
        fs::set_permissions(&external_file, fs::Permissions::from_mode(0o400))
            .expect("external immutable mode");
        symlink(external.path(), lease.data_path().join("outside-directory"))
            .expect("owned symlink");
        fs::hard_link(&external_file, lease.data_path().join("shared-file"))
            .expect("owned hardlink");
        let nested = lease.data_path().join("pinned");
        fs::create_dir(&nested).expect("pin directory");
        fs::write(nested.join("artifact"), b"owned bytes").expect("pin artifact");
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o500))
            .expect("read-only pinned directory");
        walk_owned_tree(&lease.data_directory, true, &mut DeleteWalk::new(), 0)
            .expect("remove only owned links and private data");
        assert_eq!(
            fs::read(&external_file).expect("external file survives"),
            b"user-owned bytes"
        );
        assert_eq!(
            fs::metadata(&external_file)
                .expect("external metadata")
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
        assert_eq!(
            fs::read_dir(lease.data_path())
                .expect("empty owned data")
                .count(),
            0
        );
    }

    #[test]
    fn deletion_preflight_rejects_untrusted_directory_without_mutation() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        let child = lease.data_path().join("untrusted");
        fs::create_dir(&child).expect("child directory");
        fs::set_permissions(&child, fs::Permissions::from_mode(0o777)).expect("untrusted mode");
        assert_conflict(registry.preflight_delete(&owner(), &reservation()));
        assert_eq!(
            fs::metadata(&child)
                .expect("child preserved")
                .permissions()
                .mode()
                & 0o777,
            0o777
        );
        assert!(!temp.path().join(DELETE_NAMESPACE).exists());
    }

    #[test]
    fn deletion_walk_rejects_cross_device_and_budget_before_effects() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        fs::write(lease.data_path().join("keep"), b"preserved").expect("payload");
        let mut cross_device = DeleteWalk::new();
        cross_device.device = Some(
            MachineStoreFileIdentity::of(&lease.data_directory)
                .expect("device")
                .device
                .wrapping_add(1),
        );
        assert_conflict(walk_owned_tree(
            &lease.data_directory,
            true,
            &mut cross_device,
            0,
        ));
        let mut exhausted = DeleteWalk::new();
        exhausted.remaining = 0;
        assert_conflict(walk_owned_tree(
            &lease.data_directory,
            true,
            &mut exhausted,
            0,
        ));
        assert_eq!(
            fs::read(lease.data_path().join("keep")).expect("preserved payload"),
            b"preserved"
        );
    }

    #[test]
    fn durable_delete_intent_fences_cached_fresh_and_attach_admission() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        let intent = deletion_intent(&lease);
        let root = open_trusted_registry_root(&registry.root).expect("root");
        publish_delete_intent(&root, &reservation().resource_id, &intent).expect("durable intent");
        assert_conflict(acquire(
            &registry,
            None,
            MachineRuntimeAdmission::ExistingOnly,
        ));
        assert_conflict(registry.attach_runtime(Arc::clone(&lease), |_| {
            panic!("deleted Machine must not construct runtime")
        }));
        let fresh =
            MachineRuntimeRegistry::<usize>::new(registry.root.clone()).expect("fresh registry");
        assert_conflict(acquire(
            &fresh,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        ));
        assert!(
            store_path(temp.path()).exists(),
            "admitted intent alone does not remove files"
        );
    }

    #[test]
    fn deletion_preflight_recovers_after_removed_tree_without_original_manifest() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        let intent = deletion_intent(&lease);
        let root = open_trusted_registry_root(&registry.root).expect("root");
        let namespace = child_directory(&root, "topology-machines", false).expect("namespace");
        let directory = publish_delete_intent(&root, &reservation().resource_id, &intent)
            .expect("durable intent");
        renameat_with(
            &namespace,
            reservation().resource_id.as_str(),
            &directory,
            DELETE_TREE,
            RenameFlags::NOREPLACE,
        )
        .expect("exact quarantine");
        walk_owned_tree(&lease.directory, true, &mut DeleteWalk::new(), 0)
            .expect("remove admitted tree contents");
        unlinkat(&directory, DELETE_TREE, AtFlags::REMOVEDIR).expect("remove empty store");
        let claim = registry
            .preflight_delete(&owner(), &reservation())
            .expect("recover without original tree");
        assert!(claim.lease().is_none());
        assert_eq!(claim.configuration_digest(), DIGEST_A);
        assert_eq!(
            claim.delete_operation_id(),
            Some(&intent.operation.operation_id)
        );
        assert_eq!(claim.quiescence_evidence(), Some(&intent.quiescence));
        let receipt = delete_receipt(&intent).expect("exact receipt");
        publish_delete_record(&directory, DELETE_RECEIPT, &receipt)
            .expect("durable terminal receipt");
        assert!(registry.preflight_delete(&owner(), &reservation()).is_ok());
        assert!(!store_path(temp.path()).exists());
    }

    #[test]
    fn deletion_replay_rejects_replacement_tree_and_tampered_receipt() {
        let temp = TempDir::new().expect("temporary root");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("admitted store");
        let intent = deletion_intent(&lease);
        let root = open_trusted_registry_root(&registry.root).expect("root");
        let namespace = child_directory(&root, "topology-machines", false).expect("namespace");
        let directory = publish_delete_intent(&root, &reservation().resource_id, &intent)
            .expect("durable intent");
        renameat_with(
            &namespace,
            reservation().resource_id.as_str(),
            &directory,
            DELETE_TREE,
            RenameFlags::NOREPLACE,
        )
        .expect("quarantine original");
        fs::create_dir(store_path(temp.path())).expect("foreign replacement");
        fs::set_permissions(store_path(temp.path()), fs::Permissions::from_mode(0o700))
            .expect("replacement mode");
        fs::write(store_path(temp.path()).join("foreign"), b"do not delete")
            .expect("replacement sentinel");
        assert_conflict(registry.preflight_delete(&owner(), &reservation()));
        assert_eq!(
            fs::read(store_path(temp.path()).join("foreign")).expect("foreign survives"),
            b"do not delete"
        );
        let mut receipt = delete_receipt(&intent).expect("receipt");
        receipt.configuration_digest = DIGEST_B.into();
        publish_delete_record(&directory, DELETE_RECEIPT, &receipt)
            .expect("tampered receipt fixture");
        assert_conflict(registry.preflight_delete(&owner(), &reservation()));
    }

    #[test]
    fn constructor_normalizes_relative_root_and_rejects_parent_traversal() {
        let registry = MachineRuntimeRegistry::<usize>::new(PathBuf::from("relative-runtime"))
            .expect("relative root is resolved once without filesystem access");
        assert!(registry.root.is_absolute());
        assert!(matches!(
            MachineRuntimeRegistry::<usize>::new(PathBuf::from("../runtime")),
            Err(MachineRuntimeRegistryError::Invalid(_))
        ));
    }

    #[test]
    fn create_without_configuration_digest_has_zero_filesystem_effects() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = registry(temp.path());

        let result = acquire(&registry, None, MachineRuntimeAdmission::CreateOrOpen);

        assert!(matches!(
            result,
            Err(MachineRuntimeRegistryError::Invalid(_))
        ));
        assert!(!temp.path().join("topology-machines").exists());
        let state = registry.state.lock().expect("registry state");
        assert!(state.leases.is_empty());
        assert!(state.entries.is_empty());
    }

    #[test]
    fn concurrent_store_acquisition_is_runtime_free_and_returns_one_lease() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = Arc::new(registry(temp.path()));
        let starts = Arc::new(Barrier::new(8));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let registry = Arc::clone(&registry);
            let starts = Arc::clone(&starts);
            threads.push(thread::spawn(move || {
                starts.wait();
                acquire(
                    &registry,
                    Some(DIGEST_A),
                    MachineRuntimeAdmission::CreateOrOpen,
                )
                .expect("concurrent store acquisition")
            }));
        }

        let leases = threads
            .into_iter()
            .map(|thread| thread.join().expect("acquisition thread"))
            .collect::<Vec<_>>();
        assert!(leases.iter().all(|lease| Arc::ptr_eq(lease, &leases[0])));
        assert_eq!(leases[0].configuration_digest(), DIGEST_A);
        let state = registry.state.lock().expect("registry state");
        assert_eq!(state.leases.len(), 1);
        assert!(state.entries.is_empty());
    }

    #[test]
    fn concurrent_runtime_attachment_runs_factory_once() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = Arc::new(registry(temp.path()));
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("acquire store");
        let starts = Arc::new(Barrier::new(8));
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let registry = Arc::clone(&registry);
            let lease = Arc::clone(&lease);
            let starts = Arc::clone(&starts);
            let factory_calls = Arc::clone(&factory_calls);
            threads.push(thread::spawn(move || {
                starts.wait();
                registry
                    .attach_runtime(lease, |_| {
                        factory_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(42)
                    })
                    .expect("concurrent runtime attachment")
            }));
        }

        let entries = threads
            .into_iter()
            .map(|thread| thread.join().expect("attachment thread"))
            .collect::<Vec<_>>();
        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
        assert!(entries.iter().all(|entry| Arc::ptr_eq(entry, &entries[0])));
        assert_eq!(*entries[0].runtime(), 42);
    }

    #[test]
    fn foreign_registry_refuses_store_lease_without_running_factory() {
        let temp = TempDir::new().expect("temporary directory");
        let first_registry = registry(temp.path());
        let lease = acquire(
            &first_registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("acquire first registry lease");
        let second_registry = registry(temp.path());
        let factory_calls = AtomicUsize::new(0);

        let result = second_registry.attach_runtime(lease, |_| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        });

        assert_conflict(result);
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn existing_only_discovers_persisted_digest_without_runtime_or_writes() {
        let temp = TempDir::new().expect("temporary directory");
        let first_registry = registry(temp.path());
        let first = acquire(
            &first_registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("create store lease");
        let manifest_path = store_path(temp.path()).join(MANIFEST_NAME);
        let manifest_before = fs::read(&manifest_path).expect("read owner manifest");
        let manifest_metadata_before = fs::metadata(&manifest_path).expect("stat owner manifest");
        let namespace_before = fs::read_dir(temp.path().join("topology-machines"))
            .expect("read namespace")
            .map(|entry| entry.expect("namespace entry").file_name())
            .collect::<Vec<_>>();
        drop(first);
        drop(first_registry);

        let recovered_registry = registry(temp.path());
        let recovered = acquire(
            &recovered_registry,
            None,
            MachineRuntimeAdmission::ExistingOnly,
        )
        .expect("discover exact persisted configuration");

        assert_eq!(recovered.owner(), &owner());
        assert_eq!(recovered.configuration_digest(), DIGEST_A);
        recovered.validate_current().expect("lease remains current");
        assert_eq!(
            fs::read(&manifest_path).expect("reread owner manifest"),
            manifest_before
        );
        let manifest_metadata_after = fs::metadata(&manifest_path).expect("restat owner manifest");
        assert_eq!(
            manifest_metadata_after
                .modified()
                .expect("manifest modified time"),
            manifest_metadata_before
                .modified()
                .expect("original manifest modified time")
        );
        assert_eq!(
            manifest_metadata_after.len(),
            manifest_metadata_before.len()
        );
        assert_eq!(
            fs::read_dir(temp.path().join("topology-machines"))
                .expect("reread namespace")
                .map(|entry| entry.expect("namespace entry").file_name())
                .collect::<Vec<_>>(),
            namespace_before
        );
        let state = recovered_registry.state.lock().expect("registry state");
        assert_eq!(state.leases.len(), 1);
        assert!(state.entries.is_empty());
    }

    #[test]
    fn replaced_data_directory_refuses_validation_and_runtime_attachment() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = registry(temp.path());
        let lease = acquire(
            &registry,
            Some(DIGEST_A),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .expect("acquire store");
        let store = store_path(temp.path());
        fs::rename(store.join(DATA_NAME), store.join("displaced-data"))
            .expect("displace data directory");
        fs::create_dir(store.join(DATA_NAME)).expect("replace data directory");
        fs::set_permissions(store.join(DATA_NAME), fs::Permissions::from_mode(0o700))
            .expect("make replacement private");
        let factory_calls = AtomicUsize::new(0);

        assert_conflict(lease.validate_current());
        assert_conflict(registry.attach_runtime(lease, |_| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn exact_reopen_reuses_store_and_configuration_drift_is_rejected() {
        let temp = TempDir::new().expect("temporary directory");
        let first_registry = registry(temp.path());
        let first = admit(
            &first_registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            7,
        )
        .expect("create store");
        let cached = admit(
            &first_registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            99,
        )
        .expect("reuse cached store");
        assert!(Arc::ptr_eq(&first, &cached));
        assert_eq!(*cached.runtime(), 7);

        drop(cached);
        drop(first);
        drop(first_registry);

        let reopened_registry = registry(temp.path());
        let reopened = admit(
            &reopened_registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            8,
        )
        .expect("reopen exact store");
        assert_eq!(*reopened.runtime(), 8);
        assert_conflict(admit(
            &reopened_registry,
            DIGEST_B,
            MachineRuntimeAdmission::ExistingOnly,
            9,
        ));
    }

    #[test]
    fn second_registry_cannot_construct_while_directory_is_leased() {
        let temp = TempDir::new().expect("temporary directory");
        let first_registry = registry(temp.path());
        let _entry = admit(
            &first_registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            1,
        )
        .expect("create store");
        let second_registry = registry(temp.path());
        let result = admit(
            &second_registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            2,
        );
        assert!(matches!(
            result,
            Err(MachineRuntimeRegistryError::Leased(_))
        ));
    }

    #[test]
    fn concurrent_admission_runs_constructor_once() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = Arc::new(registry(temp.path()));
        let starts = Arc::new(Barrier::new(8));
        let constructor_calls = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let registry = Arc::clone(&registry);
            let starts = Arc::clone(&starts);
            let constructor_calls = Arc::clone(&constructor_calls);
            threads.push(thread::spawn(move || {
                starts.wait();
                registry
                    .admit(
                        &owner(),
                        &reservation(),
                        DIGEST_A,
                        MachineRuntimeAdmission::CreateOrOpen,
                        |_| {
                            constructor_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(42)
                        },
                    )
                    .expect("concurrent admission")
            }));
        }
        let entries: Vec<_> = threads
            .into_iter()
            .map(|handle| handle.join().expect("admission thread"))
            .collect();
        assert_eq!(constructor_calls.load(Ordering::SeqCst), 1);
        assert!(entries.iter().all(|entry| Arc::ptr_eq(entry, &entries[0])));
    }

    #[test]
    fn concurrent_registries_publish_once_and_leave_no_staging_directories() {
        let temp = TempDir::new().expect("temporary directory");
        let canonical_root = fs::canonicalize(temp.path()).expect("canonical registry root");
        let starts = Arc::new(Barrier::new(8));
        let constructor_calls = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let root = canonical_root.clone();
            let starts = Arc::clone(&starts);
            let constructor_calls = Arc::clone(&constructor_calls);
            threads.push(thread::spawn(move || {
                let registry = MachineRuntimeRegistry::new(root).expect("valid registry root");
                starts.wait();
                registry.admit(
                    &owner(),
                    &reservation(),
                    DIGEST_A,
                    MachineRuntimeAdmission::CreateOrOpen,
                    |_| {
                        constructor_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(42)
                    },
                )
            }));
        }

        let mut successful_entries = Vec::new();
        let mut leased = 0;
        for thread in threads {
            match thread.join().expect("admission thread") {
                Ok(entry) => successful_entries.push(entry),
                Err(MachineRuntimeRegistryError::Leased(_)) => leased += 1,
                Err(error) => panic!("unexpected racing admission error: {error}"),
            }
        }
        assert_eq!(constructor_calls.load(Ordering::SeqCst), 1);
        assert_eq!(successful_entries.len(), 1);
        assert_eq!(leased, 7);

        let namespace = canonical_root.join("topology-machines");
        let mut names = fs::read_dir(&namespace)
            .expect("read runtime namespace")
            .map(|entry| {
                entry
                    .expect("read namespace entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec![reservation().resource_id]);
        assert!(store_path(&canonical_root).is_dir());
    }

    #[test]
    fn cached_admission_rejects_replaced_data_directory() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = registry(temp.path());
        let _entry = admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            1,
        )
        .expect("create store");
        let store = store_path(temp.path());
        fs::rename(store.join(DATA_NAME), store.join("displaced-data"))
            .expect("displace data directory");
        fs::create_dir(store.join(DATA_NAME)).expect("replace data directory");
        fs::set_permissions(
            store.join(DATA_NAME),
            fs::Permissions::from_mode(PRIVATE_DIRECTORY.as_raw_mode().into()),
        )
        .expect("make replacement private");

        assert_conflict(admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            2,
        ));
    }

    #[test]
    fn cached_admission_rejects_replaced_store_directory() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = registry(temp.path());
        let _entry = admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            1,
        )
        .expect("create store");
        let store = store_path(temp.path());
        let displaced = temp.path().join("displaced-store");
        fs::rename(&store, &displaced).expect("displace store directory");
        fs::create_dir(&store).expect("replace store directory");
        fs::set_permissions(&store, fs::Permissions::from_mode(0o700))
            .expect("make replacement private");
        fs::copy(displaced.join(MANIFEST_NAME), store.join(MANIFEST_NAME))
            .expect("copy matching manifest");
        fs::create_dir(store.join(DATA_NAME)).expect("create replacement data directory");
        fs::set_permissions(store.join(DATA_NAME), fs::Permissions::from_mode(0o700))
            .expect("make replacement data private");

        assert_conflict(admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            2,
        ));
    }

    #[test]
    fn factory_failure_leaves_exact_published_store_recoverable() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = registry(temp.path());
        let result = registry.admit(
            &owner(),
            &reservation(),
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            |_| {
                Err(MachineRuntimeRegistryError::Invalid(
                    "injected constructor failure".into(),
                ))
            },
        );
        assert!(matches!(
            result,
            Err(MachineRuntimeRegistryError::Invalid(message))
                if message == "injected constructor failure"
        ));
        assert!(store_path(temp.path()).is_dir());
        assert!(
            fs::read_dir(temp.path().join("topology-machines"))
                .expect("read namespace")
                .all(|entry| !entry
                    .expect("read namespace entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".pending-"))
        );

        let recovered = admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            17,
        )
        .expect("retry exact published store");
        assert_eq!(*recovered.runtime(), 17);
    }

    #[test]
    fn foreign_manifest_and_configuration_are_never_adopted() {
        let temp = TempDir::new().expect("temporary directory");
        {
            let registry = registry(temp.path());
            let _entry = admit(
                &registry,
                DIGEST_A,
                MachineRuntimeAdmission::CreateOrOpen,
                1,
            )
            .expect("create store");
        }
        let manifest_path = store_path(temp.path()).join(MANIFEST_NAME);
        let mut manifest: OwnerManifest =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
                .expect("decode manifest");
        manifest.owner.project_id = ProjectId::new("prj_foreign").expect("valid foreign ID");
        fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest).expect("encode foreign manifest"),
        )
        .expect("replace manifest contents");
        let registry = registry(temp.path());
        assert_conflict(admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            2,
        ));
    }

    #[test]
    fn non_private_store_mode_is_rejected() {
        let temp = TempDir::new().expect("temporary directory");
        {
            let registry = registry(temp.path());
            let _entry = admit(
                &registry,
                DIGEST_A,
                MachineRuntimeAdmission::CreateOrOpen,
                1,
            )
            .expect("create store");
        }
        fs::set_permissions(store_path(temp.path()), fs::Permissions::from_mode(0o750))
            .expect("make store non-private");
        let registry = registry(temp.path());
        assert_conflict(admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            2,
        ));
    }

    #[test]
    fn directory_symlink_is_not_followed() {
        let temp = TempDir::new().expect("temporary directory");
        let namespace = temp.path().join("topology-machines");
        fs::create_dir(&namespace).expect("create namespace");
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700))
            .expect("make namespace private");
        let foreign = temp.path().join("foreign");
        fs::create_dir(&foreign).expect("create foreign directory");
        fs::write(foreign.join("sentinel"), b"unchanged").expect("write foreign sentinel");
        symlink(&foreign, namespace.join(reservation().resource_id)).expect("create symlink");

        let registry = registry(temp.path());
        assert!(
            admit(
                &registry,
                DIGEST_A,
                MachineRuntimeAdmission::ExistingOnly,
                2,
            )
            .is_err()
        );
        assert_eq!(
            fs::read(foreign.join("sentinel")).expect("read sentinel"),
            b"unchanged"
        );
    }

    #[test]
    fn hard_linked_manifest_is_rejected() {
        let temp = TempDir::new().expect("temporary directory");
        {
            let registry = registry(temp.path());
            let _entry = admit(
                &registry,
                DIGEST_A,
                MachineRuntimeAdmission::CreateOrOpen,
                1,
            )
            .expect("create store");
        }
        let manifest = store_path(temp.path()).join(MANIFEST_NAME);
        fs::hard_link(&manifest, temp.path().join("manifest-alias")).expect("hard link manifest");
        let registry = registry(temp.path());
        assert_conflict(admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            2,
        ));
    }

    #[test]
    fn effective_user_ownership_is_required() {
        let temp = TempDir::new().expect("temporary directory");
        let directory = File::open(temp.path()).expect("open temporary directory");
        let different_uid = rustix::process::geteuid().as_raw().wrapping_add(1);
        assert_conflict(validate_private_directory_for_uid(
            &directory,
            different_uid,
        ));
    }

    #[test]
    fn writable_runtime_root_is_rejected_without_creating_or_constructing() {
        let temp = TempDir::new().expect("temporary directory");
        let root = temp.path().join("writable-root");
        fs::create_dir(&root).expect("create runtime root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o777))
            .expect("make root group/world writable");
        let registry = registry(&root);
        let constructor_calls = AtomicUsize::new(0);
        let result = registry.admit(
            &owner(),
            &reservation(),
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            |_| {
                constructor_calls.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            },
        );
        assert_conflict(result);
        assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
        assert!(!root.join("topology-machines").exists());
    }

    #[test]
    fn writable_non_sticky_ancestor_is_rejected_without_effects() {
        let temp = TempDir::new().expect("temporary directory");
        let canonical_temp = fs::canonicalize(temp.path()).expect("canonical temporary directory");
        let writable_ancestor = canonical_temp.join("untrusted-writable-ancestor");
        fs::create_dir(&writable_ancestor).expect("create writable ancestor");
        fs::set_permissions(&writable_ancestor, fs::Permissions::from_mode(0o777))
            .expect("make ancestor group/world writable without sticky bit");
        let root = writable_ancestor.join("runtime-root");
        fs::create_dir(&root).expect("create strict runtime root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("make runtime root private");

        let registry = MachineRuntimeRegistry::<usize>::new(root.clone())
            .expect("constructor remains filesystem-read-free");
        let constructor_calls = AtomicUsize::new(0);
        let result = registry.admit(
            &owner(),
            &reservation(),
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            |_| {
                constructor_calls.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            },
        );

        assert_conflict(result);
        assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
        assert!(!root.join("topology-machines").exists());
    }

    #[test]
    fn canonical_temporary_root_below_trusted_sticky_ancestor_is_accepted() {
        let temp = TempDir::new().expect("temporary directory");
        let canonical_temp = fs::canonicalize(temp.path()).expect("canonical temporary directory");
        let sticky_ancestor = canonical_temp.join("trusted-sticky-ancestor");
        fs::create_dir(&sticky_ancestor).expect("create sticky ancestor");
        fs::set_permissions(&sticky_ancestor, fs::Permissions::from_mode(0o1777))
            .expect("make trusted ancestor sticky and writable");
        let root = sticky_ancestor.join("runtime-root");
        fs::create_dir(&root).expect("create runtime root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("make runtime root private");

        let registry = MachineRuntimeRegistry::<usize>::new(root.clone())
            .expect("constructor remains filesystem-read-free");
        let constructor_calls = AtomicUsize::new(0);
        let entry = registry
            .admit(
                &owner(),
                &reservation(),
                DIGEST_A,
                MachineRuntimeAdmission::CreateOrOpen,
                |_| {
                    constructor_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                },
            )
            .expect("trusted sticky ancestry must admit the exact runtime root");

        assert_eq!(constructor_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*entry.runtime(), 1);
        assert!(store_path(&root).is_dir());
    }

    #[test]
    fn configured_root_symlink_is_not_followed() {
        let temp = TempDir::new().expect("temporary directory");
        let canonical_temp = fs::canonicalize(temp.path()).expect("canonical temporary directory");
        let actual_root = canonical_temp.join("actual-root");
        fs::create_dir(&actual_root).expect("create actual runtime root");
        let configured_root = canonical_temp.join("configured-root");
        symlink(&actual_root, &configured_root).expect("symlink configured root");
        let registry = MachineRuntimeRegistry::<usize>::new(configured_root)
            .expect("constructor remains filesystem-read-free");
        let constructor_calls = AtomicUsize::new(0);
        let result = registry.admit(
            &owner(),
            &reservation(),
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            |_| {
                constructor_calls.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            },
        );
        assert!(matches!(result, Err(MachineRuntimeRegistryError::Io(_))));
        assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
        assert!(!actual_root.join("topology-machines").exists());
    }

    #[test]
    fn unknown_staging_directory_is_not_adopted_or_removed() {
        let temp = TempDir::new().expect("temporary directory");
        let namespace = temp.path().join("topology-machines");
        fs::create_dir(&namespace).expect("create namespace");
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700))
            .expect("make namespace private");
        let unknown = namespace.join(".pending-foreign-operation");
        fs::create_dir(&unknown).expect("create unknown staging directory");
        fs::write(unknown.join("sentinel"), b"foreign").expect("write staging sentinel");

        let registry = registry(temp.path());
        let _entry = admit(
            &registry,
            DIGEST_A,
            MachineRuntimeAdmission::CreateOrOpen,
            1,
        )
        .expect("create exact store beside unknown staging");
        assert_eq!(
            fs::read(unknown.join("sentinel")).expect("read staging sentinel"),
            b"foreign"
        );
        assert!(store_path(temp.path()).is_dir());
    }

    #[test]
    fn existing_only_missing_store_has_no_filesystem_or_factory_effects() {
        let temp = TempDir::new().expect("temporary directory");
        let registry = registry(temp.path());
        let constructor_calls = AtomicUsize::new(0);
        let result = registry.admit(
            &owner(),
            &reservation(),
            DIGEST_A,
            MachineRuntimeAdmission::ExistingOnly,
            |_| {
                constructor_calls.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            },
        );
        // Recovery admission is read-only and reports the same typed missing
        // exact store whether or not its private namespace exists yet.
        assert!(matches!(
            result,
            Err(MachineRuntimeRegistryError::NotFound(_))
        ));
        assert_eq!(constructor_calls.load(Ordering::SeqCst), 0);
        assert!(!temp.path().join("topology-machines").exists());

        let namespace = temp.path().join("topology-machines");
        fs::create_dir(&namespace).expect("create namespace");
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700))
            .expect("make namespace private");
        assert!(matches!(
            admit(
                &registry,
                DIGEST_A,
                MachineRuntimeAdmission::ExistingOnly,
                2
            ),
            Err(MachineRuntimeRegistryError::NotFound(_))
        ));
    }
}
