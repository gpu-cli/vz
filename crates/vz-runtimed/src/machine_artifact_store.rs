//! Exactly-owned, read-only artifact pins for topology Machine admission.
//!
//! A pin is published before Runtime construction. Recovery only opens an
//! existing pin; neither a missing catalog nor a changed source can select new
//! bytes after lifecycle effects begin. The owner store lease is retained for
//! the entire operation. Host processes with the daemon UID remain trusted.

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use rustix::fs::{
    AtFlags, Mode, OFlags, RenameFlags, fchmod, fstat, mkdirat, openat, renameat_with, statat,
    unlinkat,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use vz_linux::verify_kernel_bundle_read_only;
use vz_runtime_contract::{HostSpec, LifecycleOperationId, MachineSpec};

use crate::machine_runtime_registry::{MachineRuntimeRegistryError, MachineRuntimeStoreLease};
use crate::machine_target_resolver::{
    ResolvedLinuxMachineTarget, ResolvedMachineConfiguration, TargetResolutionError,
};

const PIN: &str = "linux-target";
const CONFIG: &str = "configuration.json";
const BUNDLE: &str = "bundle";
const FILES: [&str; 4] = ["vmlinux", "initramfs.img", "youki", "version.json"];
const MAX_CONFIG: u64 = 1024 * 1024;
const MAX_ARTIFACT: u64 = 8 * 1024 * 1024 * 1024;
const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const FILE_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);

#[derive(Debug, Error)]
pub enum MachineArtifactStoreError {
    #[error(transparent)]
    Registry(#[from] MachineRuntimeRegistryError),
    #[error(transparent)]
    Resolution(#[from] TargetResolutionError),
    #[error("Machine artifact pin conflict: {0}")]
    Conflict(String),
    #[error("Machine artifact pin is missing; recovery cannot create it")]
    Missing,
    #[error("Machine artifact pin I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Machine artifact pin worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
}

impl From<rustix::io::Errno> for MachineArtifactStoreError {
    fn from(error: rustix::io::Errno) -> Self {
        Self::Io(error.into())
    }
}

/// Retains ownership and the verified private directories. Paths are usable
/// only while the owning runtime store remains leased and trusted.
pub struct PinnedMachineArtifacts {
    store: Arc<MachineRuntimeStoreLease>,
    configuration: ResolvedMachineConfiguration,
    directory: File,
    bundle: File,
}

impl PinnedMachineArtifacts {
    pub fn configuration(&self) -> &ResolvedMachineConfiguration {
        &self.configuration
    }
    pub fn store(&self) -> &Arc<MachineRuntimeStoreLease> {
        &self.store
    }
    pub fn bundle_dir(&self) -> PathBuf {
        self.store.data_path().join(PIN).join(BUNDLE)
    }
    pub fn runtime_bundle(&self) -> vz_oci_macos::PinnedLinuxBundle {
        vz_oci_macos::PinnedLinuxBundle {
            directory: self.bundle_dir(),
            artifact_identity: self.configuration.artifact.clone(),
        }
    }
    pub fn validate_current(&self) -> Result<(), MachineArtifactStoreError> {
        self.store.validate_current()?;
        let directory = open_dir(self.store.data_directory(), PIN, 0o700)?;
        same_inode(&directory, &self.directory)?;
        let bundle = open_dir(&directory, BUNDLE, 0o500)?;
        same_inode(&bundle, &self.bundle)
    }
}

/// Fresh, pre-lifecycle admission only. The caller must reserve exact ownership
/// first and finish pinning every sibling before attaching any Runtime. Existing
/// pins are verified read-only, never overwritten or repaired.
pub async fn pin_machine_artifacts(
    store: Arc<MachineRuntimeStoreLease>,
    target: &ResolvedLinuxMachineTarget,
) -> Result<PinnedMachineArtifacts, MachineArtifactStoreError> {
    let configuration = target.configuration().clone();
    let source = target.bundle_dir().to_path_buf();
    pin_inner(store, configuration, source).await
}

/// Keep a controller fence alive through detached copying and staging cleanup
/// if its async waiter is cancelled. The trusted caller retains its own fence
/// through subsequent lifecycle effects.
pub(crate) async fn pin_machine_artifacts_retaining_fence(
    store: Arc<MachineRuntimeStoreLease>,
    target: &ResolvedLinuxMachineTarget,
    fence: Arc<dyn Send + Sync>,
) -> Result<PinnedMachineArtifacts, MachineArtifactStoreError> {
    pin_inner_with_fence(
        store,
        target.configuration().clone(),
        target.bundle_dir().to_path_buf(),
        Some(fence),
    )
    .await
}

/// Post-admission recovery. Missing, malformed or mismatched pins fail closed
/// without consulting the original catalog, source bundle or ambient installer.
pub async fn load_machine_artifacts(
    store: Arc<MachineRuntimeStoreLease>,
    host: HostSpec,
    machine: &MachineSpec,
) -> Result<PinnedMachineArtifacts, MachineArtifactStoreError> {
    load_inner(store, host, machine).await
}

async fn pin_inner(
    store: Arc<MachineRuntimeStoreLease>,
    configuration: ResolvedMachineConfiguration,
    source: PathBuf,
) -> Result<PinnedMachineArtifacts, MachineArtifactStoreError> {
    pin_inner_with_fence(store, configuration, source, None).await
}

async fn pin_inner_with_fence(
    store: Arc<MachineRuntimeStoreLease>,
    configuration: ResolvedMachineConfiguration,
    source: PathBuf,
    fence: Option<Arc<dyn Send + Sync>>,
) -> Result<PinnedMachineArtifacts, MachineArtifactStoreError> {
    configuration.validate_for_machine(configuration.host, &configuration.machine)?;
    if configuration.configuration_digest()? != store.configuration_digest() {
        return Err(conflict(
            "resolved configuration differs from the exact owner store",
        ));
    }
    store.validate_current()?;
    match openat(store.data_directory(), PIN, DIR_FLAGS, Mode::empty()) {
        Ok(_) => return load_inner(store, configuration.host, &configuration.machine).await,
        Err(rustix::io::Errno::NOENT) => {}
        Err(error) => return Err(error.into()),
    }

    let copy_store = Arc::clone(&store);
    let copy_configuration = configuration.clone();
    let mut stage = tokio::task::spawn_blocking(move || {
        copy_pin(copy_store, &copy_configuration, &source, fence)
    })
    .await??;
    let pending_path = store.data_path().join(&stage.name).join(BUNDLE);
    let verified = verify_kernel_bundle_read_only(&pending_path, configuration.kernel_profile)
        .await
        .map_err(|error| conflict(error.to_string()))?;
    if verified.artifact_identity != configuration.artifact {
        return Err(conflict("staged artifact identity mismatch"));
    }
    store.validate_current()?;
    match renameat_with(
        store.data_directory(),
        stage.name.as_str(),
        store.data_directory(),
        PIN,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {
            stage.published = true;
            #[cfg(test)]
            pin_checkpoint("published");
            store.data_directory().sync_all()?;
            #[cfg(test)]
            pin_checkpoint("parent_synced");
        }
        Err(rustix::io::Errno::EXIST) => {}
        Err(error) => return Err(error.into()),
    }
    load_inner(store, configuration.host, &configuration.machine).await
}

fn copy_pin(
    store: Arc<MachineRuntimeStoreLease>,
    configuration: &ResolvedMachineConfiguration,
    source: &Path,
    fence: Option<Arc<dyn Send + Sync>>,
) -> Result<PendingPin, MachineArtifactStoreError> {
    store.validate_current()?;
    // Inspect every source before creating a pending destination. Copies are
    // hashed again and the completed destination is independently verified.
    let source_dir = open_absolute_dir(source)?;
    let mut sources = Vec::new();
    for name in FILES {
        let file = File::from(openat(&source_dir, name, FILE_FLAGS, Mode::empty())?);
        let metadata = file.metadata()?;
        use std::os::unix::fs::MetadataExt;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.len() == 0
            || metadata.len() > MAX_ARTIFACT
            || (name == "version.json" && metadata.len() > 64 * 1024)
        {
            return Err(conflict(
                "source must be a bounded nonempty single-link regular artifact",
            ));
        }
        sources.push((name, file, metadata.len()));
    }
    let pending = format!(".pending-linux-target-{}", LifecycleOperationId::generate());
    mkdirat(
        store.data_directory(),
        pending.as_str(),
        Mode::from_raw_mode(0o700),
    )?;
    let directory = open_dir_raw(store.data_directory(), &pending)?;
    let mut stage = PendingPin {
        store: Arc::clone(&store),
        name: pending,
        directory,
        bundle: None,
        published: false,
        _fence: fence,
    };
    #[cfg(test)]
    pin_checkpoint("pending_created");
    mkdirat(&stage.directory, BUNDLE, Mode::from_raw_mode(0o700))?;
    stage.bundle = Some(open_dir_raw(&stage.directory, BUNDLE)?);
    #[cfg(test)]
    cancellation_tests::pause_copy_worker(store.data_path());
    let bundle = stage
        .bundle
        .as_ref()
        .ok_or_else(|| conflict("missing staging bundle"))?;
    for (name, mut input, size) in sources {
        let mut output = create_file(bundle, name)?;
        let mut hasher = Sha256::new();
        let mut bytes = [0_u8; 64 * 1024];
        let mut copied = 0_u64;
        loop {
            let count = input.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            copied = copied
                .checked_add(count as u64)
                .ok_or_else(|| conflict("artifact length overflow"))?;
            if copied > size {
                return Err(conflict("source artifact grew during copy"));
            }
            output.write_all(&bytes[..count])?;
            hasher.update(&bytes[..count]);
        }
        if copied != size
            || format!("{:x}", hasher.finalize()) != expected_hash(configuration, name)
        {
            return Err(conflict(
                "copied source does not match the resolved artifact identity",
            ));
        }
        fchmod(&output, Mode::from_raw_mode(pinned_file_mode(name)))?;
        output.sync_all()?;
        #[cfg(test)]
        pin_checkpoint(match name {
            "vmlinux" => "vmlinux_synced",
            "initramfs.img" => "initramfs_synced",
            "youki" => "youki_synced",
            _ => "version_synced",
        });
    }
    let canonical = serde_json::to_value(configuration)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|error| conflict(error.to_string()))?;
    if canonical.len() as u64 > MAX_CONFIG {
        return Err(conflict("resolved configuration exceeds size bound"));
    }
    let mut file = create_file(&stage.directory, CONFIG)?;
    file.write_all(&canonical)?;
    fchmod(&file, Mode::from_raw_mode(0o400))?;
    file.sync_all()?;
    #[cfg(test)]
    pin_checkpoint("configuration_synced");
    fchmod(bundle, Mode::from_raw_mode(0o500))?;
    bundle.sync_all()?;
    #[cfg(test)]
    pin_checkpoint("bundle_synced");
    // Darwin requires write permission on the directory being renamed. Keep
    // the private owner envelope 0700; its contents are never overwritten by
    // this API. The nested bundle and executable youki are 0500; other files
    // are 0400. Docker executes youki directly from its read-only guest share.
    // Same-UID host processes are already inside the filesystem trust boundary.
    stage.directory.sync_all()?;
    #[cfg(test)]
    pin_checkpoint("directory_synced");
    Ok(stage)
}

async fn load_inner(
    store: Arc<MachineRuntimeStoreLease>,
    host: HostSpec,
    machine: &MachineSpec,
) -> Result<PinnedMachineArtifacts, MachineArtifactStoreError> {
    store.validate_current()?;
    let directory = match openat(store.data_directory(), PIN, DIR_FLAGS, Mode::empty()) {
        Ok(fd) => File::from(fd),
        Err(rustix::io::Errno::NOENT) => return Err(MachineArtifactStoreError::Missing),
        Err(error) => return Err(error.into()),
    };
    validate_directory(&directory, 0o700)?;
    let bundle = open_dir(&directory, BUNDLE, 0o500)?;
    exact_inventory(&directory, &[CONFIG, BUNDLE])?;
    exact_inventory(&bundle, &FILES)?;
    let file = readonly_file(&directory, CONFIG)?;
    if file.metadata()?.len() > MAX_CONFIG {
        return Err(conflict("pin configuration exceeds size bound"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG {
        return Err(conflict("pin configuration grew beyond size bound"));
    }
    let configuration: ResolvedMachineConfiguration =
        serde_json::from_slice(&bytes).map_err(|error| conflict(error.to_string()))?;
    let canonical = serde_json::to_value(&configuration)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|error| conflict(error.to_string()))?;
    if canonical != bytes {
        return Err(conflict(
            "persisted configuration is not its exact canonical encoding",
        ));
    }
    configuration.validate_for_machine(host, machine)?;
    if configuration.configuration_digest()? != store.configuration_digest() {
        return Err(conflict(
            "persisted pin configuration differs from owner manifest",
        ));
    }
    for name in FILES {
        readonly_file(&bundle, name)?;
    }
    let verified = verify_kernel_bundle_read_only(
        &store.data_path().join(PIN).join(BUNDLE),
        configuration.kernel_profile,
    )
    .await
    .map_err(|error| conflict(error.to_string()))?;
    if verified.artifact_identity != configuration.artifact {
        return Err(conflict("persisted pin artifact identity mismatch"));
    }
    let pin = PinnedMachineArtifacts {
        store,
        configuration,
        directory,
        bundle,
    };
    pin.validate_current()?;
    Ok(pin)
}

fn conflict(message: impl Into<String>) -> MachineArtifactStoreError {
    MachineArtifactStoreError::Conflict(message.into())
}
fn expected_hash<'a>(configuration: &'a ResolvedMachineConfiguration, name: &str) -> &'a str {
    match name {
        "vmlinux" => &configuration.artifact.kernel_sha256,
        "initramfs.img" => &configuration.artifact.initramfs_sha256,
        "youki" => &configuration.artifact.youki_sha256,
        _ => &configuration.artifact.version_sha256,
    }
}
fn open_dir_raw(parent: &File, name: &str) -> Result<File, MachineArtifactStoreError> {
    Ok(File::from(openat(parent, name, DIR_FLAGS, Mode::empty())?))
}
fn open_dir(parent: &File, name: &str, mode: u16) -> Result<File, MachineArtifactStoreError> {
    let file = open_dir_raw(parent, name)?;
    validate_directory(&file, mode)?;
    Ok(file)
}
fn validate_directory(file: &File, mode: u16) -> Result<(), MachineArtifactStoreError> {
    let stat = fstat(file)?;
    if Mode::from_raw_mode(stat.st_mode) != Mode::from_raw_mode(mode)
        || stat.st_uid != rustix::process::geteuid().as_raw()
    {
        return Err(conflict(format!(
            "pin directory must be effective-user-owned mode {mode:04o}"
        )));
    }
    Ok(())
}
fn readonly_file(parent: &File, name: &str) -> Result<File, MachineArtifactStoreError> {
    let file = File::from(openat(parent, name, FILE_FLAGS, Mode::empty())?);
    let stat = fstat(&file)?;
    if !rustix::fs::FileType::from_raw_mode(stat.st_mode).is_file()
        || stat.st_nlink != 1
        || stat.st_size <= 0
        || u64::try_from(stat.st_size).unwrap_or(u64::MAX) > MAX_ARTIFACT
        || stat.st_uid != rustix::process::geteuid().as_raw()
        || Mode::from_raw_mode(stat.st_mode) != Mode::from_raw_mode(pinned_file_mode(name))
    {
        return Err(conflict(
            "pin file must be effective-user-owned single-link regular file with exact read-only mode (0500 for youki, 0400 otherwise)",
        ));
    }
    Ok(file)
}
fn pinned_file_mode(name: &str) -> u16 {
    // VirtioFS preserves these permissions. Unlike generic OCI bootstrap,
    // Docker/BuildKit execute this exact shared file rather than a guest copy.
    if name == "youki" { 0o500 } else { 0o400 }
}
fn create_file(parent: &File, name: &str) -> Result<File, MachineArtifactStoreError> {
    Ok(File::from(openat(
        parent,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )?))
}
fn same_inode(left: &File, right: &File) -> Result<(), MachineArtifactStoreError> {
    let left = fstat(left)?;
    let right = fstat(right)?;
    if left.st_dev != right.st_dev || left.st_ino != right.st_ino {
        return Err(conflict("pin directory was replaced"));
    }
    Ok(())
}
fn exact_inventory(directory: &File, expected: &[&str]) -> Result<(), MachineArtifactStoreError> {
    let mut names = Vec::new();
    for entry in rustix::fs::Dir::read_from(directory)? {
        let entry = entry?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec());
        }
    }
    names.sort();
    let mut expected = expected
        .iter()
        .map(|name| name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    expected.sort();
    if names != expected {
        return Err(conflict("pin contains missing or unexpected entries"));
    }
    Ok(())
}
fn open_absolute_dir(path: &Path) -> Result<File, MachineArtifactStoreError> {
    if !path.is_absolute() {
        return Err(conflict("source bundle path is not absolute"));
    }
    let mut current = File::from(rustix::fs::open("/", DIR_FLAGS, Mode::empty())?);
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                current = File::from(openat(&current, name, DIR_FLAGS, Mode::empty())?)
            }
            _ => return Err(conflict("source bundle path contains traversal")),
        }
    }
    Ok(current)
}

/// Cleanup is confined to this call's still-attached unpublished directory.
/// Unknown crash leftovers are never candidates and are never recursively
/// deleted. A successful rename remains durable state even if parent fsync fails.
struct PendingPin {
    store: Arc<MachineRuntimeStoreLease>,
    name: String,
    directory: File,
    bundle: Option<File>,
    published: bool,
    // Fields are dropped only after Drop::drop completes. Keep this last so
    // even detached-worker cleanup and release of its store remain fenced.
    _fence: Option<Arc<dyn Send + Sync>>,
}
impl Drop for PendingPin {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let Ok(metadata) = fstat(&self.directory) else {
            return;
        };
        if !statat(
            self.store.data_directory(),
            self.name.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .is_ok_and(|stat| stat.st_dev == metadata.st_dev && stat.st_ino == metadata.st_ino)
        {
            return;
        }
        let _ = fchmod(&self.directory, Mode::from_raw_mode(0o700));
        if let Some(bundle) = &self.bundle {
            let _ = fchmod(bundle, Mode::from_raw_mode(0o700));
            for name in FILES {
                let _ = unlinkat(bundle, name, AtFlags::empty());
            }
            let _ = unlinkat(&self.directory, BUNDLE, AtFlags::REMOVEDIR);
        }
        let _ = unlinkat(&self.directory, CONFIG, AtFlags::empty());
        let _ = unlinkat(
            self.store.data_directory(),
            self.name.as_str(),
            AtFlags::REMOVEDIR,
        );
        let _ = self.store.data_directory().sync_all();
    }
}

// Compiled only into library test drivers, never the installed daemon.
#[cfg(test)]
fn pin_checkpoint(phase: &str) {
    eprintln!("VZ_ARTIFACT_PIN_CHECKPOINT={phase}");
    if std::env::var("VZ_ARTIFACT_PIN_CRASH_PHASE").as_deref() == Ok(phase) {
        eprintln!("VZ_ARTIFACT_PIN_CRASH_PHASE={phase}");
        if let Err(error) =
            rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::KILL)
        {
            panic!("failed to deliver SIGKILL checkpoint: {error}");
        }
        // Darwin may return from kill before another thread processes SIGKILL.
        // Never unwind here: PendingPin::drop could clean up the very crash
        // boundary this test must leave on disk. The parent bounds child life.
        loop {
            std::thread::park();
        }
    }
}

#[cfg(test)]
#[path = "machine_artifact_crash_tests.rs"]
mod crash_tests;

#[cfg(test)]
#[path = "machine_artifact_store_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "machine_artifact_cancellation_tests.rs"]
mod cancellation_tests;
