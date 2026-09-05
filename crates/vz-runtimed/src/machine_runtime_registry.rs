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

use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fstat, mkdirat, openat, renameat_with, statat,
    unlinkat,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use vz_runtime_contract::{
    EnvironmentId, LifecycleOperationId, MachineId, OwnedResourceKind, OwnershipRecord, ProjectId,
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

/// Holds both the backend and its lifetime filesystem lock. Registry entries
/// are strongly retained; dropping a caller's Arc cannot permit a duplicate
/// backend constructor while this registry remains alive.
pub struct MachineRuntimeEntry<R> {
    runtime: R,
    manifest: OwnerManifest,
    data_path: PathBuf,
    directory: File,
    data_directory: File,
}

impl<R> MachineRuntimeEntry<R> {
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    pub fn owner(&self) -> &ResourceOwner {
        &self.manifest.owner
    }
}

/// A daemon-owned registry, separate from the legacy global runtime manager.
/// Construction is read-only. Admission opens the configured root without
/// following symlinks and derives all child names from typed ownership, never
/// path hints.
///
/// The runtime backend still accepts a path rather than a directory descriptor.
/// The registry verifies that path immediately before construction and on every
/// cached admission, and retains both directory descriptors. Processes running
/// as the same effective user remain inside the filesystem trust boundary: they
/// can rename writable ancestors despite descriptor-relative validation.
pub struct MachineRuntimeRegistry<R> {
    root: PathBuf,
    entries: Mutex<HashMap<String, Arc<MachineRuntimeEntry<R>>>>,
}

impl<R> MachineRuntimeRegistry<R> {
    pub fn new(root: PathBuf) -> Result<Self, MachineRuntimeRegistryError> {
        let root = absolute_path_without_parent_traversal(&root)?;
        Ok(Self {
            root,
            entries: Mutex::new(HashMap::new()),
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

    /// The digest must bind the already-resolved backend, target, profile,
    /// resources and artifact identities. A changed selection never reuses a
    /// live or persisted store silently. This API does not resolve TargetSpec.
    ///
    /// `factory` runs only after exact manifest verification and an exclusive
    /// lifetime lock, since backend constructors may reconcile persistent data.
    /// Callers must retain the entry while using its backend, including clones.
    pub fn admit(
        &self,
        owner: &ResourceOwner,
        reservation: &OwnershipRecord,
        configuration_digest: &str,
        mode: MachineRuntimeAdmission,
        factory: impl FnOnce(&Path) -> Result<R, MachineRuntimeRegistryError>,
    ) -> Result<Arc<MachineRuntimeEntry<R>>, MachineRuntimeRegistryError> {
        let expected = Self::reservation(owner)?;
        if expected != *reservation {
            return Err(MachineRuntimeRegistryError::Conflict(
                "reservation does not match exact Project/Environment/Machine store identity"
                    .into(),
            ));
        }
        let digest = configuration_digest.strip_prefix("sha256:").unwrap_or("");
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(MachineRuntimeRegistryError::Invalid(
                "resolved configuration requires a canonical SHA-256 digest".into(),
            ));
        }
        let manifest = OwnerManifest {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            owner: owner.clone(),
            reservation: expected,
            configuration_digest: configuration_digest.into(),
        };
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| MachineRuntimeRegistryError::Poisoned)?;
        let root = open_absolute_directory(&self.root)?;
        validate_registry_root(&root)?;
        let key = &manifest.reservation.resource_id;
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
                publish_directory(&namespace, key, &manifest)?
            }
            Err(rustix::io::Errno::NOENT) => {
                return Err(MachineRuntimeRegistryError::NotFound(key.clone()));
            }
            Err(error) => return Err(error.into()),
        };
        validate_private_directory(&directory)?;
        let actual = read_manifest(&directory)?;
        if actual != manifest {
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
        if let Some(entry) = entries.get(key) {
            if entry.manifest != manifest
                || !same_file(&entry.directory, &directory)?
                || !same_file(&entry.data_directory, &data)?
            {
                return Err(MachineRuntimeRegistryError::Conflict(
                    "leased runtime directory or data directory was replaced".into(),
                ));
            }
            return Ok(Arc::clone(entry));
        }
        fs2::FileExt::try_lock_exclusive(&directory).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                MachineRuntimeRegistryError::Leased(key.clone())
            } else {
                error.into()
            }
        })?;
        let runtime = factory(&data_path)?;
        let entry = Arc::new(MachineRuntimeEntry {
            runtime,
            manifest,
            data_path,
            directory,
            data_directory: data,
        });
        entries.insert(
            entry.manifest.reservation.resource_id.clone(),
            Arc::clone(&entry),
        );
        Ok(entry)
    }
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

    fn assert_conflict<T>(result: Result<T, MachineRuntimeRegistryError>) {
        assert!(
            matches!(result, Err(MachineRuntimeRegistryError::Conflict(_))),
            "expected ownership conflict"
        );
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
