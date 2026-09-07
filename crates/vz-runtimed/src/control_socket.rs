//! Exact daemon control-socket ownership, independent of Machine quiescence.
//!
//! Both persistent locks survive until this authority is dropped. Recovery
//! never signals a process, and an absent daemon never proves an absent VM.
//! Incomplete preparation without a published ownership record is deliberately
//! retained and rejected: this does not certify every startup crash boundary.
//! Same-euid actors are trusted; final pathname checks are not an atomic CAS
//! against a malicious same-euid renamer.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixListener;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, chmodat, mkdirat, open, openat, renameat,
    renameat_with, statat, unlinkat,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vz::process_identity::{self, ProcessIdentity, ProcessObservation};
use vz_runtime_contract::LifecycleOperationId;

use crate::{RuntimedConfig, startup_lock::StartupLock};

const LIMIT: u64 = 64 * 1024;
const READ: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);
const NEW: OFlags = OFlags::RDWR
    .union(OFlags::CREATE)
    .union(OFlags::EXCL)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);
const DIRECTORY: OFlags = READ.union(OFlags::DIRECTORY);
const PRIVATE: Mode = Mode::RUSR.union(Mode::WUSR);
const SCOPE: &str = "control_socket_only_not_VM_quiescence";

fn conflict(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
fn mapped(_error: impl std::fmt::Display) -> io::Error {
    // Do not copy arbitrary persisted JSON or diagnostic contents into errors.
    conflict("control ownership validation failed")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    device: u64,
    inode: u64,
}
impl Identity {
    fn of(file: &File) -> io::Result<Self> {
        let value = file.metadata()?;
        Ok(Self {
            device: value.dev(),
            inode: value.ino(),
        })
    }
    fn of_stat(value: &rustix::fs::Stat) -> io::Result<Self> {
        Ok(Self {
            device: u64::try_from(value.st_dev)
                .map_err(|_| conflict("negative device identity"))?,
            inode: value.st_ino,
        })
    }
    fn lock(lock: &StartupLock) -> io::Result<Self> {
        let (device, inode) = lock.identity().map_err(mapped)?;
        Ok(Self { device, inode })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    socket_path: PathBuf,
    state_store_path: PathBuf,
    runtime_data_dir: PathBuf,
    log_path: PathBuf,
    pid_path: PathBuf,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PathIdentity {
    path: PathBuf,
    identity: Identity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SocketIdentity {
    path: PathBuf,
    staging_path: PathBuf,
    identity: Identity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordIdentity {
    identity: Identity,
    sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerRecord {
    schema_version: u32,
    daemon_id: String,
    process: ProcessIdentity,
    configuration: Configuration,
    socket_parent: Identity,
    state_parent: Identity,
    runtime_root: Identity,
    history_root: Identity,
    staging_parent: Identity,
    database: Identity,
    database_lock: PathIdentity,
    socket_lock: PathIdentity,
    socket: SocketIdentity,
    log: PathIdentity,
    pid: PathIdentity,
    preparation: RecordIdentity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    schema_version: u32,
    daemon_id: String,
    process: ProcessIdentity,
    configuration: Configuration,
    staging_path: PathBuf,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosedRecord {
    schema_version: u32,
    daemon_id: String,
    owner_sha256: String,
    socket_removed: bool,
    pid_removed: bool,
    scope: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryRecord {
    schema_version: u32,
    daemon_id: String,
    previous_daemon_id: String,
    previous_owner_sha256: String,
    previous_process_observation: Option<ProcessObservation>,
    graceful_closed: Option<RecordIdentity>,
    scope: String,
}

#[derive(Debug)]
struct Directory {
    path: PathBuf,
    original: PathBuf,
    file: File,
    identity: Identity,
    private: bool,
}
impl Directory {
    fn open(path: &Path, private: bool) -> io::Result<Self> {
        reject_traversal(path)?;
        if private && path.symlink_metadata()?.file_type().is_symlink() {
            return Err(conflict("private control directory cannot be a symlink"));
        }
        let canonical = path.canonicalize()?;
        let file = File::from(open(&canonical, DIRECTORY, Mode::empty())?);
        require_directory(&file, private)?;
        let result = Self {
            identity: Identity::of(&file)?,
            path: canonical,
            original: path.into(),
            file,
            private,
        };
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> io::Result<()> {
        if self.original.canonicalize()? != self.path {
            return Err(conflict("control parent resolution changed"));
        }
        require_directory(&self.file, self.private)?;
        let current = File::from(open(&self.path, DIRECTORY, Mode::empty())?);
        require_directory(&current, self.private)?;
        if Identity::of(&self.file)? != self.identity || Identity::of(&current)? != self.identity {
            return Err(conflict("control parent inode changed"));
        }
        Ok(())
    }
    fn stat(&self, name: &str) -> io::Result<Option<rustix::fs::Stat>> {
        self.validate()?;
        match statat(&self.file, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(value) => Ok(Some(value)),
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    fn absent(&self, name: &str) -> io::Result<()> {
        if self.stat(name)?.is_some() {
            return Err(conflict("unowned or unresolved control path exists"));
        }
        Ok(())
    }
    fn regular(&self, name: &str) -> io::Result<File> {
        self.validate()?;
        let file = File::from(openat(&self.file, name, READ, Mode::empty())?);
        require_regular(&file)?;
        self.require_file(name, Identity::of(&file)?)?;
        Ok(file)
    }
    fn database(&self, name: &str) -> io::Result<File> {
        self.validate()?;
        let file = File::from(openat(&self.file, name, READ, Mode::empty())?);
        require_database(&file)?;
        self.require_database(name, Identity::of(&file)?)?;
        Ok(file)
    }
    fn require_database(&self, name: &str, expected: Identity) -> io::Result<()> {
        let value = self
            .stat(name)?
            .ok_or_else(|| conflict("owned database disappeared"))?;
        if !FileType::from_raw_mode(value.st_mode).is_file()
            || value.st_uid != rustix::process::geteuid().as_raw()
            || value.st_nlink != 1
            || !matches!(
                Mode::from_raw_mode(value.st_mode).as_raw_mode(),
                0o600 | 0o640 | 0o644
            )
            || Identity::of_stat(&value)? != expected
        {
            return Err(conflict("owned database identity or permissions changed"));
        }
        Ok(())
    }
    fn require_file(&self, name: &str, expected: Identity) -> io::Result<()> {
        let value = self
            .stat(name)?
            .ok_or_else(|| conflict("owned control file disappeared"))?;
        if !FileType::from_raw_mode(value.st_mode).is_file()
            || value.st_uid != rustix::process::geteuid().as_raw()
            || value.st_nlink != 1
            || Mode::from_raw_mode(value.st_mode) != PRIVATE
            || Identity::of_stat(&value)? != expected
        {
            return Err(conflict("owned control file identity changed"));
        }
        Ok(())
    }
    fn create(&self, name: &str) -> io::Result<File> {
        self.validate()?;
        let file = File::from(openat(&self.file, name, NEW, PRIVATE)?);
        require_regular(&file)?;
        self.require_file(name, Identity::of(&file)?)?;
        file.sync_all()?;
        self.file.sync_all()?;
        Ok(file)
    }
    fn socket(&self, name: &str, expected: Identity) -> io::Result<bool> {
        let Some(value) = self.stat(name)? else {
            return Ok(false);
        };
        if !FileType::from_raw_mode(value.st_mode).is_socket()
            || value.st_uid != rustix::process::geteuid().as_raw()
            || value.st_nlink != 1
            || Mode::from_raw_mode(value.st_mode) != PRIVATE
            || Identity::of_stat(&value)? != expected
        {
            return Err(conflict("control socket is not the recorded inode"));
        }
        Ok(true)
    }
    fn remove_socket(&self, name: &str, expected: Identity) -> io::Result<()> {
        if self.socket(name, expected)? {
            unlinkat(&self.file, name, AtFlags::empty())?;
            self.file.sync_all()?;
        }
        Ok(())
    }
    fn remove_file(&self, name: &str, expected: Identity) -> io::Result<()> {
        if self.stat(name)?.is_some() {
            self.require_file(name, expected)?;
            unlinkat(&self.file, name, AtFlags::empty())?;
            self.file.sync_all()?;
        }
        Ok(())
    }
}

fn reject_traversal(path: &Path) -> io::Result<()> {
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
        || path.as_os_str().len() > 4096
    {
        return Err(conflict("control path traversal or excessive length"));
    }
    Ok(())
}
fn normalized_file(path: &Path) -> io::Result<PathBuf> {
    reject_traversal(path)?;
    let name = path
        .file_name()
        .ok_or_else(|| conflict("control filename missing"))?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(parent.canonicalize()?.join(name))
}
fn name(path: &Path) -> io::Result<&str> {
    path.file_name()
        .and_then(|part| part.to_str())
        .filter(|part| !part.is_empty())
        .ok_or_else(|| conflict("control filename must be bounded UTF-8"))
}
fn require_directory(file: &File, private: bool) -> io::Result<()> {
    let value = file.metadata()?;
    if !value.is_dir()
        || value.uid() != rustix::process::geteuid().as_raw()
        || value.mode() & 0o022 != 0
        || (private && value.mode() & 0o7777 != 0o700)
    {
        return Err(conflict("control directory is not safely owned"));
    }
    Ok(())
}
fn require_regular(file: &File) -> io::Result<()> {
    let value = file.metadata()?;
    if !value.is_file()
        || value.uid() != rustix::process::geteuid().as_raw()
        || value.nlink() != 1
        || value.mode() & 0o7777 != 0o600
    {
        return Err(conflict(
            "control file must be owned regular single-link mode0600",
        ));
    }
    Ok(())
}
fn require_database(file: &File) -> io::Result<()> {
    let value = file.metadata()?;
    // An existing SQLite database is data, not a new ownership credential.
    // Preserve legacy read permissions; never permit non-owner write/execute.
    if !value.is_file()
        || value.uid() != rustix::process::geteuid().as_raw()
        || value.nlink() != 1
        || !matches!(value.mode() & 0o7777, 0o600 | 0o640 | 0o644)
    {
        return Err(conflict(
            "database must be owned single-link regular with safe legacy permissions",
        ));
    }
    Ok(())
}
fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug)]
struct JsonPin {
    name: String,
    identity: Identity,
    bytes: Vec<u8>,
}
impl JsonPin {
    fn read(directory: &Directory, name: &str) -> io::Result<Self> {
        let mut file = directory.regular(name)?;
        let identity = Identity::of(&file)?;
        if file.metadata()?.len() > LIMIT {
            return Err(conflict("control ownership record too large"));
        }
        let mut bytes = Vec::new();
        (&mut file).take(LIMIT + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > LIMIT {
            return Err(conflict("control ownership record too large"));
        }
        directory.require_file(name, identity)?;
        Ok(Self {
            name: name.into(),
            identity,
            bytes,
        })
    }
    fn new(directory: &Directory, name: &str, value: &impl Serialize) -> io::Result<Self> {
        let bytes = serde_json::to_vec(value).map_err(mapped)?;
        if bytes.len() as u64 > LIMIT {
            return Err(conflict("control ownership record too large"));
        }
        let mut file = directory.create(name)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        directory.file.sync_all()?;
        let pin = Self {
            name: name.into(),
            identity: Identity::of(&file)?,
            bytes,
        };
        pin.validate(directory)?;
        Ok(pin)
    }
    fn decode<T: serde::de::DeserializeOwned>(&self) -> io::Result<T> {
        serde_json::from_slice(&self.bytes).map_err(mapped)
    }
    fn proof(&self) -> RecordIdentity {
        RecordIdentity {
            identity: self.identity,
            sha256: sha(&self.bytes),
        }
    }
    fn validate(&self, directory: &Directory) -> io::Result<()> {
        let current = Self::read(directory, &self.name)?;
        if current.identity != self.identity || current.bytes != self.bytes {
            return Err(conflict("control ownership record replaced or changed"));
        }
        Ok(())
    }
}

/// Startup-owned control authority. Holding this object does not authorize any
/// Machine state transition; those remain fenced by their typed journals.
#[derive(Debug)]
pub(crate) struct ControlSocket {
    parent: Directory,
    state_parent: Directory,
    runtime: Directory,
    history: Directory,
    stage: Directory,
    database: File,
    log: File,
    pid: File,
    record: OwnerRecord,
    index: JsonPin,
    preparation: JsonPin,
    listener: Mutex<Option<UnixListener>>,
    socket_lock: StartupLock,
    database_lock: Arc<StartupLock>,
    #[cfg(test)]
    simulate_crash: bool,
}

impl ControlSocket {
    pub(crate) fn acquire(
        config: &RuntimedConfig,
        database_lock: Arc<StartupLock>,
    ) -> io::Result<Self> {
        Self::acquire_with(
            config,
            database_lock,
            process_identity::current()?,
            process_identity::capture,
        )
    }

    fn acquire_with(
        config: &RuntimedConfig,
        database_lock: Arc<StartupLock>,
        process: ProcessIdentity,
        lookup: impl Fn(u32) -> io::Result<Option<ProcessObservation>>,
    ) -> io::Result<Self> {
        database_lock.validate_current().map_err(mapped)?;
        validate_process_identity(&process)?;
        let socket_path = normalized_file(&config.socket_path)?;
        let state_store_path = normalized_file(&config.state_store_path)?;
        let parent = Directory::open(
            socket_path
                .parent()
                .ok_or_else(|| conflict("socket parent missing"))?,
            false,
        )?;
        let state_parent = Directory::open(
            state_store_path
                .parent()
                .ok_or_else(|| conflict("database parent missing"))?,
            false,
        )?;
        let runtime = Directory::open(&config.runtime_data_dir, false)?;
        let configuration = Configuration {
            log_path: socket_path.with_extension("log"),
            pid_path: socket_path.with_extension("pid"),
            socket_path,
            state_store_path,
            runtime_data_dir: runtime.path.clone(),
        };
        let socket_name = name(&configuration.socket_path)?.to_owned();
        let index_name = format!("{socket_name}.owner.json");
        let preparation_name = format!("{socket_name}.preparing.json");
        let next_name = format!("{socket_name}.owner.next");
        let history_name = format!("{socket_name}.owners");
        let history_path = parent.path.join(&history_name);
        let stage_directory_name = format!(".c{}", &sha(socket_name.as_bytes())[..8]);
        let stage_directory_path = parent.path.join(&stage_directory_name);
        let stage_path = stage_directory_path.join("s");
        // Darwin sockaddr_un includes its terminating NUL in 104 bytes.
        if configuration.socket_path.as_os_str().len() >= 104 || stage_path.as_os_str().len() >= 104
        {
            return Err(conflict(
                "control socket or private stage exceeds native socket path bound",
            ));
        }
        let socket_lock_path = parent.path.join(format!("{socket_name}.lock"));
        let database_lock_path = normalized_file(database_lock.path())?;
        if database_lock_path
            != normalized_file(&crate::startup_lock_path(&configuration.state_store_path))?
        {
            return Err(conflict(
                "database fence belongs to a different state store",
            ));
        }
        let reserved_index = parent.path.join(&index_name);
        let reserved_preparation = parent.path.join(&preparation_name);
        let reserved_next = parent.path.join(&next_name);
        let paths = [
            &configuration.socket_path,
            &configuration.state_store_path,
            &configuration.log_path,
            &configuration.pid_path,
            &socket_lock_path,
            &database_lock_path,
            &reserved_index,
            &reserved_preparation,
            &reserved_next,
            &history_path,
            &stage_directory_path,
        ];
        if paths
            .iter()
            .enumerate()
            .any(|(i, path)| paths[..i].contains(path))
        {
            return Err(conflict("control resources alias each other"));
        }
        let socket_lock = StartupLock::acquire(socket_lock_path.clone()).map_err(mapped)?;
        let database_lock_id = PathIdentity {
            path: database_lock_path,
            identity: Identity::lock(&database_lock)?,
        };
        let socket_lock_id = PathIdentity {
            path: socket_lock_path,
            identity: Identity::lock(&socket_lock)?,
        };
        parent.absent(&next_name)?;
        let old_index = if parent.stat(&index_name)?.is_some() {
            Some(JsonPin::read(&parent, &index_name)?)
        } else {
            None
        };
        let old_record: Option<OwnerRecord> =
            old_index.as_ref().map(JsonPin::decode).transpose()?;
        let old_preparation = if parent.stat(&preparation_name)?.is_some() {
            Some(JsonPin::read(&parent, &preparation_name)?)
        } else {
            None
        };
        let mut history = if parent.stat(&history_name)?.is_some() {
            Some(Directory::open(&history_path, true)?)
        } else {
            None
        };
        let mut stage = if parent.stat(&stage_directory_name)?.is_some() {
            Some(Directory::open(&stage_directory_path, true)?)
        } else {
            None
        };
        let database = if state_parent
            .stat(name(&configuration.state_store_path)?)?
            .is_some()
        {
            Some(state_parent.database(name(&configuration.state_store_path)?)?)
        } else {
            None
        };
        let daemon_id = format!("runtimed-{}", LifecycleOperationId::generate());
        let mut recovery = None;
        if let Some(old) = &old_record {
            validate_daemon_id(&old.daemon_id)?;
            validate_process_identity(&old.process)?;
            let previous = old_index
                .as_ref()
                .ok_or_else(|| conflict("owner index missing"))?;
            let history = history
                .as_ref()
                .ok_or_else(|| conflict("owner history missing"))?;
            let stage = stage
                .as_ref()
                .ok_or_else(|| conflict("private staging directory missing"))?;
            let archived = JsonPin::read(history, &format!("{}.owner.json", old.daemon_id))?;
            if archived.bytes != previous.bytes
                || old.schema_version != 1
                || old.configuration != configuration
                || old.socket_parent != parent.identity
                || old.state_parent != state_parent.identity
                || old.runtime_root != runtime.identity
                || old.history_root != history.identity
                || old.staging_parent != stage.identity
                || old.database_lock != database_lock_id
                || old.socket_lock != socket_lock_id
                || database.as_ref().map(Identity::of).transpose()? != Some(old.database)
                || old.socket.path != configuration.socket_path
                || old.socket.staging_path != stage_path
                || old.log.path != configuration.log_path
                || old.pid.path != configuration.pid_path
                || old.process.uid != rustix::process::geteuid().as_raw()
            {
                return Err(conflict(
                    "prior control ownership does not match exact startup configuration",
                ));
            }
            parent.require_file(name(&configuration.log_path)?, old.log.identity)?;
            if parent.stat(name(&configuration.pid_path)?)?.is_some() {
                parent.require_file(name(&configuration.pid_path)?, old.pid.identity)?;
            }
            let stable = parent.socket(&socket_name, old.socket.identity)?;
            let staged = stage.socket("s", old.socket.identity)?;
            if stable && staged {
                return Err(conflict("both staged and published control sockets exist"));
            }
            if let Some(pin) = &old_preparation {
                let preparation: Preparation = pin.decode()?;
                if pin.proof() != old.preparation || preparation != preparation_for(old) {
                    return Err(conflict(
                        "unresolved preparation is not the exact recorded owner",
                    ));
                }
            }
            let closed_name = format!("{}.closed.json", old.daemon_id);
            let closed = if history.stat(&closed_name)?.is_some() {
                let pin = JsonPin::read(history, &closed_name)?;
                let receipt: ClosedRecord = pin.decode()?;
                if receipt != closed_for(old, &previous.bytes)
                    || stable
                    || staged
                    || parent.stat(name(&configuration.pid_path)?)?.is_some()
                {
                    return Err(conflict("graceful control closure receipt is inconsistent"));
                }
                Some(pin.proof())
            } else {
                None
            };
            let observation = if closed.is_none() {
                let observation = lookup(old.process.pid)?;
                if observation
                    .as_ref()
                    .is_some_and(|actual| actual.identity == old.process && !actual.zombie)
                {
                    return Err(conflict("recorded daemon process is still live"));
                }
                observation
            } else {
                None
            };
            recovery = Some(RecoveryRecord {
                schema_version: 1,
                daemon_id: daemon_id.clone(),
                previous_daemon_id: old.daemon_id.clone(),
                previous_owner_sha256: sha(&previous.bytes),
                previous_process_observation: observation,
                graceful_closed: closed,
                scope: SCOPE.into(),
            });
        } else {
            // No durable ownership means no adoption, including diagnostics and
            // preparation left by a crash before the full inode record existed.
            if old_preparation.is_some() || history.is_some() || stage.is_some() {
                return Err(conflict(
                    "incomplete control preparation retained; recovery is not certified",
                ));
            }
            for path in [
                &configuration.socket_path,
                &configuration.log_path,
                &configuration.pid_path,
            ] {
                parent.absent(name(path)?)?;
            }
        }
        // Every existing control/stage/diagnostic path was checked before DB
        // creation, pathname removals, and publication of new ownership.
        database_lock.validate_current().map_err(mapped)?;
        socket_lock.validate_current().map_err(mapped)?;
        parent.validate()?;
        state_parent.validate()?;
        runtime.validate()?;
        if history.is_none() {
            mkdirat(
                &parent.file,
                history_name.as_str(),
                Mode::from_raw_mode(0o700),
            )?;
            parent.file.sync_all()?;
            history = Some(Directory::open(&history_path, true)?);
        }
        let history = history.ok_or_else(|| conflict("control history unavailable"))?;
        if stage.is_none() {
            mkdirat(
                &parent.file,
                stage_directory_name.as_str(),
                Mode::from_raw_mode(0o700),
            )?;
            parent.file.sync_all()?;
            stage = Some(Directory::open(&stage_directory_path, true)?);
        }
        let stage = stage.ok_or_else(|| conflict("control staging directory unavailable"))?;
        history.absent(&format!("{daemon_id}.owner.json"))?;
        history.absent(&format!("{daemon_id}.recovery.json"))?;
        history.absent(&format!("{daemon_id}.closed.json"))?;
        if let (Some(old), Some(previous), Some(recovery)) = (&old_record, &old_index, &recovery) {
            previous.validate(&parent)?;
            JsonPin::new(&history, &format!("{daemon_id}.recovery.json"), recovery)?;
            parent.remove_socket(&socket_name, old.socket.identity)?;
            stage.remove_socket("s", old.socket.identity)?;
            parent.remove_file(name(&configuration.pid_path)?, old.pid.identity)?;
            if let Some(preparation) = &old_preparation {
                preparation.validate(&parent)?;
                parent.remove_file(&preparation_name, preparation.identity)?;
            }
        }
        let database = match database {
            Some(file) => file,
            None => state_parent.create(name(&configuration.state_store_path)?)?,
        };
        let preparation = Preparation {
            schema_version: 1,
            daemon_id: daemon_id.clone(),
            process: process.clone(),
            configuration: configuration.clone(),
            staging_path: stage_path.clone(),
        };
        let preparation = JsonPin::new(&parent, &preparation_name, &preparation)?;
        let log = if let Some(old) = &old_record {
            parent.require_file(name(&configuration.log_path)?, old.log.identity)?;
            File::from(openat(
                &parent.file,
                name(&configuration.log_path)?,
                OFlags::WRONLY
                    .union(OFlags::APPEND)
                    .union(READ.difference(OFlags::RDONLY)),
                Mode::empty(),
            )?)
        } else {
            parent.create(name(&configuration.log_path)?)?
        };
        require_regular(&log)?;
        let pid = parent.create(name(&configuration.pid_path)?)?;
        stage.absent("s")?;
        parent.absent(&socket_name)?;
        // From here until the complete record is durable, failure intentionally
        // leaves the preparation claim and exact stage name for inspection.
        let listener = UnixListener::bind(&stage_path)?;
        let staged_inode = stage
            .stat("s")?
            .ok_or_else(|| conflict("bound staging inode unavailable; preparation retained"))?;
        if !FileType::from_raw_mode(staged_inode.st_mode).is_socket()
            || staged_inode.st_nlink != 1
            || staged_inode.st_uid != rustix::process::geteuid().as_raw()
        {
            return Err(conflict(
                "bound staging identity untrusted; preparation retained",
            ));
        }
        let socket_identity = Identity::of_stat(&staged_inode)?;
        chmodat(&stage.file, "s", PRIVATE, AtFlags::empty())?;
        if !stage.socket("s", socket_identity)? {
            return Err(conflict("staging socket disappeared"));
        }
        listener.set_nonblocking(true)?;
        stage.file.sync_all()?;
        let log_path = configuration.log_path.clone();
        let pid_path = configuration.pid_path.clone();
        let record = OwnerRecord {
            schema_version: 1,
            daemon_id,
            process,
            configuration,
            socket_parent: parent.identity,
            state_parent: state_parent.identity,
            runtime_root: runtime.identity,
            history_root: history.identity,
            staging_parent: stage.identity,
            database: Identity::of(&database)?,
            database_lock: database_lock_id,
            socket_lock: socket_lock_id,
            socket: SocketIdentity {
                path: parent.path.join(&socket_name),
                staging_path: stage_path,
                identity: socket_identity,
            },
            log: PathIdentity {
                path: log_path,
                identity: Identity::of(&log)?,
            },
            pid: PathIdentity {
                path: pid_path,
                identity: Identity::of(&pid)?,
            },
            preparation: preparation.proof(),
        };
        JsonPin::new(
            &history,
            &format!("{}.owner.json", record.daemon_id),
            &record,
        )?;
        let mut index = JsonPin::new(&parent, &next_name, &record)?;
        if let Some(previous) = &old_index {
            previous.validate(&parent)?;
            renameat(
                &parent.file,
                next_name.as_str(),
                &parent.file,
                index_name.as_str(),
            )?;
        } else {
            parent.absent(&index_name)?;
            renameat_with(
                &parent.file,
                next_name.as_str(),
                &parent.file,
                index_name.as_str(),
                RenameFlags::NOREPLACE,
            )?;
        }
        index.name = index_name;
        parent.file.sync_all()?;
        index.validate(&parent)?;
        let result = Self {
            parent,
            state_parent,
            runtime,
            history,
            stage,
            database,
            log,
            pid,
            record,
            index,
            preparation,
            listener: Mutex::new(Some(listener)),
            socket_lock,
            database_lock,
            #[cfg(test)]
            simulate_crash: false,
        };
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> io::Result<()> {
        self.database_lock.validate_current().map_err(mapped)?;
        self.socket_lock.validate_current().map_err(mapped)?;
        self.parent.validate()?;
        self.state_parent.validate()?;
        self.runtime.validate()?;
        self.history.validate()?;
        self.stage.validate()?;
        self.index.validate(&self.parent)?;
        self.preparation.validate(&self.parent)?;
        let archived = JsonPin::read(
            &self.history,
            &format!("{}.owner.json", self.record.daemon_id),
        )?;
        if archived.bytes != self.index.bytes {
            return Err(conflict("immutable control owner history changed"));
        }
        self.parent
            .require_file(name(&self.record.log.path)?, self.record.log.identity)?;
        require_regular(&self.log)?;
        if Identity::of(&self.log)? != self.record.log.identity {
            return Err(conflict("pinned log descriptor changed"));
        }
        self.verify_state_store()?;
        Ok(())
    }

    pub(crate) fn publish_and_take_listener(
        &self,
        requested_path: &Path,
    ) -> io::Result<UnixListener> {
        if normalized_file(requested_path)? != self.record.configuration.socket_path {
            return Err(conflict("server requested an unowned control socket"));
        }
        self.validate()?;
        let mut listener = self
            .listener
            .lock()
            .map_err(|_| conflict("control listener state poisoned"))?;
        if listener.is_none() {
            return Err(conflict("control listener has already been taken"));
        }
        if !self.stage.socket("s", self.record.socket.identity)? {
            return Err(conflict("owned staging socket missing"));
        }
        self.parent.absent(name(&self.record.socket.path)?)?;
        renameat_with(
            &self.stage.file,
            "s",
            &self.parent.file,
            name(&self.record.socket.path)?,
            RenameFlags::NOREPLACE,
        )?;
        self.parent.file.sync_all()?;
        self.stage.file.sync_all()?;
        if !self
            .parent
            .socket(name(&self.record.socket.path)?, self.record.socket.identity)?
        {
            return Err(conflict("published control socket missing"));
        }
        listener
            .take()
            .ok_or_else(|| conflict("control listener disappeared"))
    }

    pub(crate) fn verify_state_store(&self) -> io::Result<()> {
        self.database_lock.validate_current().map_err(mapped)?;
        require_database(&self.database)?;
        if Identity::of(&self.database)? != self.record.database {
            return Err(conflict("pinned database descriptor changed"));
        }
        self.state_parent.require_database(
            name(&self.record.configuration.state_store_path)?,
            self.record.database,
        )
    }
    pub(crate) fn daemon_id(&self) -> &str {
        &self.record.daemon_id
    }
    pub(crate) fn open_log(&self) -> io::Result<File> {
        self.validate()?;
        self.parent
            .require_file(name(&self.record.log.path)?, self.record.log.identity)?;
        require_regular(&self.log)?;
        let file = File::from(openat(
            &self.parent.file,
            name(&self.record.log.path)?,
            OFlags::WRONLY
                .union(OFlags::APPEND)
                .union(READ.difference(OFlags::RDONLY)),
            Mode::empty(),
        )?);
        if Identity::of(&file)? != self.record.log.identity {
            return Err(conflict("log inode changed"));
        }
        Ok(file)
    }
    pub(crate) fn write_pid(&self) -> io::Result<()> {
        self.validate()?;
        self.parent
            .require_file(name(&self.record.pid.path)?, self.record.pid.identity)?;
        require_regular(&self.pid)?;
        let mut file = &self.pid;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        write!(&mut file, "{}", self.record.process.pid)?;
        file.sync_all()?;
        self.parent
            .require_file(name(&self.record.pid.path)?, self.record.pid.identity)
    }

    fn close_owned(&mut self) -> io::Result<()> {
        self.validate()?;
        let listener = self
            .listener
            .get_mut()
            .map_err(|_| conflict("control listener state poisoned"))?;
        listener.take();
        // Validate every removal target before removing any of them.
        self.parent
            .socket(name(&self.record.socket.path)?, self.record.socket.identity)?;
        self.stage.socket("s", self.record.socket.identity)?;
        if self.parent.stat(name(&self.record.pid.path)?)?.is_some() {
            self.parent
                .require_file(name(&self.record.pid.path)?, self.record.pid.identity)?;
        }
        self.parent
            .remove_socket(name(&self.record.socket.path)?, self.record.socket.identity)?;
        self.stage.remove_socket("s", self.record.socket.identity)?;
        self.parent
            .remove_file(name(&self.record.pid.path)?, self.record.pid.identity)?;
        JsonPin::new(
            &self.history,
            &format!("{}.closed.json", self.record.daemon_id),
            &closed_for(&self.record, &self.index.bytes),
        )?;
        // Keep preparation/index/history/log and both lock names persistent.
        // The exact closed record permits a later owner even in this process.
        Ok(())
    }
}
impl Drop for ControlSocket {
    fn drop(&mut self) {
        #[cfg(test)]
        if self.simulate_crash {
            return;
        }
        if let Err(error) = self.close_owned() {
            tracing::warn!(daemon_id = %self.record.daemon_id, error_kind = ?error.kind(), "control cleanup incomplete; ownership evidence retained");
        }
    }
}
fn validate_daemon_id(value: &str) -> io::Result<()> {
    if value.is_empty()
        || value.len() > 96
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(conflict("invalid daemon history identifier"));
    }
    Ok(())
}
fn validate_process_identity(value: &ProcessIdentity) -> io::Result<()> {
    if value.pid == 0
        || value.uid != rustix::process::geteuid().as_raw()
        || value.start_seconds == 0
        || value.start_microseconds >= 1_000_000
        || value.boot_session_uuid.len() != 36
        || !value
            .boot_session_uuid
            .bytes()
            .enumerate()
            .all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                }
            })
    {
        return Err(conflict("invalid recorded daemon process birth identity"));
    }
    Ok(())
}
fn preparation_for(owner: &OwnerRecord) -> Preparation {
    Preparation {
        schema_version: 1,
        daemon_id: owner.daemon_id.clone(),
        process: owner.process.clone(),
        configuration: owner.configuration.clone(),
        staging_path: owner.socket.staging_path.clone(),
    }
}
fn closed_for(owner: &OwnerRecord, bytes: &[u8]) -> ClosedRecord {
    ClosedRecord {
        schema_version: 1,
        daemon_id: owner.daemon_id.clone(),
        owner_sha256: sha(bytes),
        socket_removed: true,
        pid_removed: true,
        scope: SCOPE.into(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    use super::*;

    struct Fixture {
        _root: tempfile::TempDir,
        root: PathBuf,
        config: RuntimedConfig,
    }
    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::Builder::new()
                .prefix("vz-c-")
                .tempdir_in("/private/tmp")
                .unwrap();
            let root = temporary.path().canonicalize().unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let runtime = root.join("r");
            fs::create_dir(&runtime).unwrap();
            fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
            let config = RuntimedConfig {
                state_store_path: root.join("state.db"),
                runtime_data_dir: runtime.clone(),
                socket_path: runtime.join("d.sock"),
            };
            Self {
                _root: temporary,
                root,
                config,
            }
        }
        fn lock(&self) -> Arc<StartupLock> {
            Arc::new(
                StartupLock::acquire(crate::startup_lock_path(&self.config.state_store_path))
                    .unwrap(),
            )
        }
        fn start(&self) -> ControlSocket {
            ControlSocket::acquire(&self.config, self.lock()).unwrap()
        }
        fn recover(
            &self,
            lookup: impl Fn(u32) -> io::Result<Option<ProcessObservation>>,
        ) -> io::Result<ControlSocket> {
            ControlSocket::acquire_with(
                &self.config,
                self.lock(),
                process_identity::current().unwrap(),
                lookup,
            )
        }
    }
    fn crash(mut control: ControlSocket) {
        // Filesystem-only crash seam: close held FDs without graceful cleanup.
        // This never represents a real process/VM kill or physical recovery.
        control.simulate_crash = true;
        drop(control);
    }
    fn private_file(path: &Path, bytes: &[u8]) {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(bytes).unwrap();
    }
    type Snapshot = BTreeMap<PathBuf, (u64, u64, u32, Option<Vec<u8>>)>;
    fn snapshot(root: &Path) -> Snapshot {
        let mut result = BTreeMap::new();
        let mut pending = vec![root.to_owned()];
        while let Some(path) = pending.pop() {
            assert!(result.len() < 200, "bounded fixture inventory");
            let metadata = path.symlink_metadata().unwrap();
            let bytes = if metadata.is_file() {
                Some(fs::read(&path).unwrap())
            } else {
                None
            };
            if metadata.is_dir() {
                pending.extend(
                    fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
            }
            result.insert(
                path,
                (metadata.dev(), metadata.ino(), metadata.mode(), bytes),
            );
        }
        result
    }

    #[test]
    fn fresh_publication_connects_after_rename_and_graceful_restart_appends_log() {
        let fixture = Fixture::new();
        let first = fixture.start();
        assert!(!fixture.config.socket_path.exists());
        let stage_metadata = first.record.socket.staging_path.symlink_metadata().unwrap();
        assert_eq!(stage_metadata.mode() & 0o7777, 0o600);
        first.verify_state_store().unwrap();
        first.open_log().unwrap().write_all(b"first\n").unwrap();
        first.write_pid().unwrap();
        first.write_pid().unwrap();
        assert_eq!(
            fs::read(&first.record.pid.path).unwrap(),
            first.record.process.pid.to_string().as_bytes()
        );
        let listener = first
            .publish_and_take_listener(&fixture.config.socket_path)
            .unwrap();
        assert!(
            first
                .publish_and_take_listener(&fixture.config.socket_path)
                .is_err()
        );
        let mut client = UnixStream::connect(&fixture.config.socket_path).unwrap();
        let (mut accepted, _) = listener.accept().unwrap();
        accepted
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        client.write_all(b"control").unwrap();
        let mut payload = [0; 7];
        accepted.read_exact(&mut payload).unwrap();
        assert_eq!(&payload, b"control");
        let prior = first.record.clone();
        let history = first.history.path.clone();
        drop(client);
        drop(accepted);
        drop(listener);
        drop(first);
        assert!(!fixture.config.socket_path.exists());
        assert!(!prior.pid.path.exists());
        assert!(
            history
                .join(format!("{}.closed.json", prior.daemon_id))
                .exists()
        );
        let second = fixture
            .recover(|_| panic!("valid closed record does not require process absence"))
            .unwrap();
        let mut log = second.open_log().unwrap();
        assert!(
            rustix::fs::fcntl_getfl(&log)
                .unwrap()
                .contains(OFlags::APPEND)
        );
        log.write_all(b"second\n").unwrap();
        assert_eq!(fs::read(&prior.log.path).unwrap(), b"first\nsecond\n");
        assert_eq!(second.record.log.identity, prior.log.identity);
        assert_eq!(second.record.database, prior.database);
        assert_ne!(second.daemon_id(), prior.daemon_id);
        assert!(
            history
                .join(format!("{}.owner.json", prior.daemon_id))
                .exists()
        );
    }

    #[test]
    fn stale_published_and_staged_records_recover_only_with_exact_absence_proof() {
        for published in [false, true] {
            let fixture = Fixture::new();
            let first = fixture.start();
            let old = first.record.clone();
            let bytes = first.index.bytes.clone();
            if published {
                drop(
                    first
                        .publish_and_take_listener(&fixture.config.socket_path)
                        .unwrap(),
                );
            }
            crash(first);
            let second = fixture
                .recover(|pid| {
                    assert_eq!(pid, old.process.pid);
                    Ok(None)
                })
                .unwrap();
            assert_eq!(second.record.database, old.database);
            assert_eq!(second.record.socket_lock, old.socket_lock);
            assert_eq!(second.record.database_lock, old.database_lock);
            assert_ne!(second.record.socket.identity, old.socket.identity);
            assert!(!fixture.config.socket_path.exists());
            let receipt: RecoveryRecord = JsonPin::read(
                &second.history,
                &format!("{}.recovery.json", second.daemon_id()),
            )
            .unwrap()
            .decode()
            .unwrap();
            assert_eq!(receipt.previous_owner_sha256, sha(&bytes));
            assert_eq!(receipt.previous_daemon_id, old.daemon_id);
            assert_eq!(receipt.previous_process_observation, None);
            assert_eq!(receipt.graceful_closed, None);
            assert_eq!(receipt.scope, SCOPE);
            assert_eq!(
                JsonPin::read(&second.history, &format!("{}.owner.json", old.daemon_id))
                    .unwrap()
                    .bytes,
                bytes
            );
        }
    }

    #[test]
    fn live_birth_and_lookup_error_preserve_all_control_resources() {
        let fixture = Fixture::new();
        let first = fixture.start();
        let old = first.record.clone();
        drop(
            first
                .publish_and_take_listener(&fixture.config.socket_path)
                .unwrap(),
        );
        crash(first);
        let before = snapshot(&fixture.root);
        assert!(
            fixture
                .recover(|_| Ok(Some(ProcessObservation {
                    identity: old.process.clone(),
                    zombie: false
                })))
                .is_err()
        );
        assert_eq!(snapshot(&fixture.root), before);
        assert!(
            fixture
                .recover(|_| Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "test lookup denial"
                )))
                .is_err()
        );
        assert_eq!(snapshot(&fixture.root), before);
    }

    #[test]
    fn reused_birth_and_zombie_observations_are_retained_exactly() {
        for zombie in [false, true] {
            let fixture = Fixture::new();
            let first = fixture.start();
            let mut observation = ProcessObservation {
                identity: first.record.process.clone(),
                zombie,
            };
            if !zombie {
                observation.identity.start_seconds += 1;
            }
            crash(first);
            let second = fixture.recover(|_| Ok(Some(observation.clone()))).unwrap();
            let receipt: RecoveryRecord = JsonPin::read(
                &second.history,
                &format!("{}.recovery.json", second.daemon_id()),
            )
            .unwrap()
            .decode()
            .unwrap();
            assert_eq!(receipt.previous_process_observation, Some(observation));
            assert_eq!(receipt.scope, SCOPE);
        }
    }

    #[test]
    fn unknown_socket_and_diagnostics_are_rejected_before_database_creation() {
        for kind in ["socket", "regular", "symlink", "fifo", "log", "pid"] {
            let fixture = Fixture::new();
            let path = match kind {
                "log" => fixture.config.socket_path.with_extension("log"),
                "pid" => fixture.config.socket_path.with_extension("pid"),
                _ => fixture.config.socket_path.clone(),
            };
            let listener = if kind == "socket" {
                Some(UnixListener::bind(&path).unwrap())
            } else {
                None
            };
            match kind {
                "socket" => (),
                "symlink" => symlink(fixture.root.join("missing"), &path).unwrap(),
                "fifo" => assert!(
                    std::process::Command::new("/usr/bin/mkfifo")
                        .arg("-m")
                        .arg("600")
                        .arg(&path)
                        .status()
                        .unwrap()
                        .success()
                ),
                _ => private_file(&path, b"foreign"),
            }
            let metadata = path.symlink_metadata().unwrap();
            let started = std::time::Instant::now();
            assert!(
                ControlSocket::acquire(&fixture.config, fixture.lock()).is_err(),
                "{kind}"
            );
            assert!(started.elapsed() < Duration::from_secs(2));
            assert!(!fixture.config.state_store_path.exists());
            let after = path.symlink_metadata().unwrap();
            assert_eq!(
                (metadata.dev(), metadata.ino(), metadata.mode()),
                (after.dev(), after.ino(), after.mode())
            );
            if after.is_file() {
                assert_eq!(fs::read(&path).unwrap(), b"foreign");
            }
            drop(listener);
        }
    }

    #[test]
    fn replacement_of_each_owned_resource_prevents_recovery_before_removal() {
        for target in ["socket", "stage", "log", "pid", "database"] {
            let fixture = Fixture::new();
            let first = fixture.start();
            if target != "stage" {
                drop(
                    first
                        .publish_and_take_listener(&fixture.config.socket_path)
                        .unwrap(),
                );
            }
            let path = match target {
                "socket" => first.record.socket.path.clone(),
                "stage" => first.record.socket.staging_path.clone(),
                "log" => first.record.log.path.clone(),
                "pid" => first.record.pid.path.clone(),
                _ => first.record.configuration.state_store_path.clone(),
            };
            crash(first);
            fs::rename(&path, fixture.root.join("retained-original")).unwrap();
            private_file(&path, b"replacement");
            let before = snapshot(&fixture.root);
            assert!(
                fixture
                    .recover(|_| panic!("path rejection must precede process proof"))
                    .is_err(),
                "{target}"
            );
            assert_eq!(snapshot(&fixture.root), before, "{target}");
        }
    }

    #[test]
    fn current_parent_and_database_replacement_are_not_adopted() {
        let fixture = Fixture::new();
        let control = fixture.start();
        fs::write(
            &fixture.config.state_store_path,
            b"SQLite may change database contents",
        )
        .unwrap();
        control.verify_state_store().unwrap();
        fs::rename(
            &fixture.config.state_store_path,
            fixture.root.join("retained.db"),
        )
        .unwrap();
        private_file(&fixture.config.state_store_path, b"foreign database");
        assert!(control.verify_state_store().is_err());
        assert!(
            control
                .publish_and_take_listener(&fixture.config.socket_path)
                .is_err()
        );
        let before = snapshot(&fixture.root);
        drop(control);
        assert_eq!(snapshot(&fixture.root), before);
    }

    #[test]
    fn owner_index_and_history_tampering_never_authorize_recovery() {
        for history in [false, true] {
            let fixture = Fixture::new();
            let control = fixture.start();
            let path = if history {
                control
                    .history
                    .path
                    .join(format!("{}.owner.json", control.daemon_id()))
            } else {
                control.parent.path.join(&control.index.name)
            };
            crash(control);
            fs::write(path, b"{\"schema_version\":999}").unwrap();
            let before = snapshot(&fixture.root);
            assert!(fixture.recover(|_| Ok(None)).is_err());
            assert_eq!(snapshot(&fixture.root), before);
        }
    }

    #[test]
    fn incomplete_preparation_is_explicitly_retained_not_certified() {
        let fixture = Fixture::new();
        let control = fixture.start();
        let index_path = control.parent.path.join(&control.index.name);
        let stage_path = control.record.socket.staging_path.clone();
        crash(control);
        fs::remove_file(index_path).unwrap();
        let before = snapshot(&fixture.root);
        let error = fixture.recover(|_| Ok(None)).unwrap_err();
        assert!(error.to_string().contains("preparation retained"));
        assert_eq!(snapshot(&fixture.root), before);
        assert!(stage_path.exists());
    }

    #[test]
    fn publication_and_drop_preserve_replacement_socket() {
        let fixture = Fixture::new();
        let control = fixture.start();
        assert!(
            control
                .publish_and_take_listener(&fixture.root.join("foreign.sock"))
                .is_err()
        );
        let replacement = UnixListener::bind(&fixture.config.socket_path).unwrap();
        let before = snapshot(&fixture.root);
        assert!(
            control
                .publish_and_take_listener(&fixture.config.socket_path)
                .is_err()
        );
        drop(control);
        assert_eq!(snapshot(&fixture.root), before);
        drop(replacement);
    }

    #[test]
    fn invalid_graceful_receipt_cannot_bypass_live_birth_check() {
        let fixture = Fixture::new();
        let control = fixture.start();
        let receipt = control
            .history
            .path
            .join(format!("{}.closed.json", control.daemon_id()));
        drop(control);
        fs::write(&receipt, b"{}").unwrap();
        let before = snapshot(&fixture.root);
        assert!(fixture.recover(|_| Ok(None)).is_err());
        assert_eq!(snapshot(&fixture.root), before);
    }

    #[test]
    fn different_database_or_unrelated_lock_never_claims_original_socket() {
        let fixture = Fixture::new();
        let control = fixture.start();
        crash(control);
        let mut changed = fixture.config.clone();
        changed.state_store_path = fixture.root.join("other.db");
        let other_lock = Arc::new(
            StartupLock::acquire(crate::startup_lock_path(&changed.state_store_path)).unwrap(),
        );
        let before = snapshot(&fixture.root);
        assert!(
            ControlSocket::acquire_with(
                &changed,
                Arc::clone(&other_lock),
                process_identity::current().unwrap(),
                |_| Ok(None)
            )
            .is_err()
        );
        assert!(!changed.state_store_path.exists());
        assert!(
            ControlSocket::acquire_with(
                &fixture.config,
                other_lock,
                process_identity::current().unwrap(),
                |_| Ok(None)
            )
            .is_err()
        );
        assert_eq!(snapshot(&fixture.root), before);
    }

    #[test]
    fn legacy_database_read_permissions_are_preserved_without_weakening_authority_files() {
        for mode in [0o600, 0o640, 0o644, 0o660, 0o666, 0o700, 0o4600] {
            let fixture = Fixture::new();
            private_file(
                &fixture.config.state_store_path,
                b"retained legacy database bytes",
            );
            fs::set_permissions(
                &fixture.config.state_store_path,
                fs::Permissions::from_mode(mode),
            )
            .unwrap();
            let before = fixture.config.state_store_path.metadata().unwrap();
            let result = ControlSocket::acquire(&fixture.config, fixture.lock());
            assert_eq!(result.is_ok(), matches!(mode, 0o600 | 0o640 | 0o644));
            assert_eq!(
                fs::read(&fixture.config.state_store_path).unwrap(),
                b"retained legacy database bytes"
            );
            let after = fixture.config.state_store_path.metadata().unwrap();
            assert_eq!(
                (before.dev(), before.ino(), before.mode()),
                (after.dev(), after.ino(), after.mode())
            );
            if let Ok(control) = result {
                control.verify_state_store().unwrap();
                for path in [
                    &control.record.log.path,
                    &control.record.pid.path,
                    &control.parent.path.join(&control.index.name),
                ] {
                    assert_eq!(path.metadata().unwrap().mode() & 0o7777, 0o600);
                }
            }
        }
    }
}
