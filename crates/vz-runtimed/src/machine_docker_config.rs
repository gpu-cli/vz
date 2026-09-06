//! Persistent, owner-bound host client storage. Credentials are mutable private
//! contents, never ownership evidence or diagnostic output. No helper policy is
//! inferred here; callers supply the initial, separately admitted configuration.

use anyhow::{Context, Result, ensure};
use rustix::fs::{Mode, OFlags, RenameFlags, mkdirat, openat, renameat_with};
use serde::{Deserialize, Serialize};
use std::fs::{File, Metadata};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vz_runtime_contract::{LifecycleOperationId, ResourceOwner};

use crate::machine_runtime_registry::MachineRuntimeStoreLease;

const DIRECTORY: &str = "docker-client";
const CLAIM: &str = "vz-owner.json";
const CONFIG: &str = "config.json";
const CONFIG_LIMIT: u64 = 1024 * 1024;
const CLAIM_LIMIT: u64 = 16 * 1024;
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    device: u64,
    inode: u64,
}

impl Identity {
    fn of(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    schema_version: u32,
    owner: ResourceOwner,
    nonce: String,
    directory: Identity,
}

/// Stable logical-Machine path, independent of incarnation and ambient Docker
/// settings. This function does not create or admit filesystem state.
pub(crate) fn path(store: &MachineRuntimeStoreLease) -> PathBuf {
    store.data_path().join(DIRECTORY)
}

pub(crate) struct ManagedMachineDockerConfig {
    store: Arc<MachineRuntimeStoreLease>,
    path: PathBuf,
    directory: File,
    claim: Claim,
    claim_identity: Identity,
    claim_bytes: Vec<u8>,
}

impl ManagedMachineDockerConfig {
    /// Publish a complete fresh directory atomically, or reopen an already
    /// claimed directory. Existing mutable credentials are never overwritten.
    /// Failed staging remains private and is not subsequently adopted.
    pub(crate) fn ensure(
        store: Arc<MachineRuntimeStoreLease>,
        initial_config: &[u8],
    ) -> Result<Self> {
        validate_json(initial_config)?;
        if let Some(existing) = Self::open_existing(Arc::clone(&store))? {
            return Ok(existing);
        }
        store.validate_current()?;
        let nonce = LifecycleOperationId::generate().to_string();
        let pending = format!(".docker-client.pending-{nonce}");
        mkdirat(
            store.data_directory(),
            pending.as_str(),
            Mode::from_raw_mode(0o700),
        )?;
        let directory = open_directory(store.data_directory(), &pending)?
            .context("private Docker config staging directory missing")?;
        let identity = validate_directory(&directory)?;
        let claim = Claim {
            schema_version: 1,
            owner: store.owner().clone(),
            nonce,
            directory: identity.clone(),
        };
        let claim_bytes = serde_json::to_vec(&claim)?;
        ensure!(
            claim_bytes.len() as u64 <= CLAIM_LIMIT,
            "Docker config ownership claim too large"
        );
        write_new(&directory, CLAIM, &claim_bytes)?;
        write_new(&directory, CONFIG, initial_config)?;
        directory.sync_all()?;
        store.validate_current()?;
        let attached = open_directory(store.data_directory(), &pending)?
            .context("private Docker config staging directory detached")?;
        ensure!(
            validate_directory(&attached)? == identity,
            "private Docker config staging directory replaced"
        );
        let (observed, _) = read_regular(&directory, CLAIM, CLAIM_LIMIT)?;
        ensure!(
            observed == claim_bytes,
            "Docker config ownership claim changed before publication"
        );
        let (observed, _) = read_regular(&directory, CONFIG, CONFIG_LIMIT)?;
        ensure!(
            observed == initial_config,
            "Docker config contents changed before publication"
        );
        renameat_with(
            store.data_directory(),
            pending.as_str(),
            store.data_directory(),
            DIRECTORY,
            RenameFlags::NOREPLACE,
        )?;
        store.data_directory().sync_all()?;
        let result = Self::open_existing(store)?.context("published Docker config missing")?;
        ensure!(
            result.claim == claim && result.claim_bytes == claim_bytes,
            "published Docker config ownership changed"
        );
        Ok(result)
    }

    /// Absence is accepted only for the entire fixed directory; missing or
    /// malformed ownership/config files never authorize adoption or repair.
    pub(crate) fn open_existing(store: Arc<MachineRuntimeStoreLease>) -> Result<Option<Self>> {
        store.validate_current()?;
        let Some(directory) = open_directory(store.data_directory(), DIRECTORY)? else {
            return Ok(None);
        };
        let identity = validate_directory(&directory)?;
        let (claim_bytes, claim_identity) = read_regular(&directory, CLAIM, CLAIM_LIMIT)?;
        let claim: Claim = serde_json::from_slice(&claim_bytes)
            .map_err(|_| anyhow::anyhow!("invalid Docker config ownership claim"))?;
        ensure!(
            claim.schema_version == 1
                && claim.owner == *store.owner()
                && claim.owner.machine_id.is_some()
                && claim.directory == identity
                && claim.nonce.len() == 36
                && claim.nonce.starts_with("lop_")
                && claim.nonce[4..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "Docker config claim has foreign ownership or directory identity"
        );
        let result = Self {
            path: path(&store),
            store,
            directory,
            claim,
            claim_identity,
            claim_bytes,
        };
        result.validate_current()?;
        Ok(Some(result))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Private internal policy input. Callers must never log these bytes.
    pub(crate) fn read_config(&self) -> Result<Vec<u8>> {
        self.read_config_with_checkpoint(|| Ok(()))
    }

    fn read_config_with_checkpoint(
        &self,
        checkpoint: impl FnOnce() -> Result<()>,
    ) -> Result<Vec<u8>> {
        self.validate_current()?;
        let (bytes, identity) = read_regular(&self.directory, CONFIG, CONFIG_LIMIT)?;
        validate_json(&bytes)?;
        checkpoint()?;
        self.validate_current()?;
        let (after, after_identity) = read_regular(&self.directory, CONFIG, CONFIG_LIMIT)?;
        ensure!(
            identity == after_identity && bytes == after,
            "Docker config changed during policy read"
        );
        Ok(bytes)
    }

    /// File identity may change when Docker atomically saves credentials. Each
    /// read must still be bounded, private, regular, and coherent; the enclosing
    /// directory and immutable claim may not change identity or bytes.
    pub(crate) fn validate_current(&self) -> Result<()> {
        self.store.validate_current()?;
        let attached = open_directory(self.store.data_directory(), DIRECTORY)?
            .context("private Docker config directory disappeared")?;
        ensure!(
            validate_directory(&attached)? == self.claim.directory
                && validate_directory(&self.directory)? == self.claim.directory,
            "private Docker config directory replaced"
        );
        let (bytes, identity) = read_regular(&self.directory, CLAIM, CLAIM_LIMIT)?;
        ensure!(
            identity == self.claim_identity && bytes == self.claim_bytes,
            "Docker config ownership claim replaced"
        );
        let (bytes, _) = read_regular(&self.directory, CONFIG, CONFIG_LIMIT)?;
        validate_json(&bytes)?;
        self.store.validate_current()?;
        let attached = open_directory(self.store.data_directory(), DIRECTORY)?
            .context("private Docker config directory disappeared")?;
        ensure!(
            validate_directory(&attached)? == self.claim.directory,
            "private Docker config directory changed during validation"
        );
        Ok(())
    }
}

fn open_directory(parent: &File, name: &str) -> Result<Option<File>> {
    match openat(parent, name, DIRECTORY_FLAGS, Mode::empty()) {
        Ok(fd) => Ok(Some(File::from(fd))),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn validate_directory(file: &File) -> Result<Identity> {
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o7777 == 0o700,
        "Docker config directory is not private and user-owned"
    );
    Ok(Identity::of(&metadata))
}

fn write_new(directory: &File, name: &str, bytes: &[u8]) -> Result<()> {
    let mut file = File::from(openat(
        directory,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )?);
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_regular(directory: &File, name: &str, limit: u64) -> Result<(Vec<u8>, Identity)> {
    read_regular_with_checkpoint(directory, name, limit, || Ok(()))
}

fn read_regular_with_checkpoint(
    directory: &File,
    name: &str,
    limit: u64,
    checkpoint: impl FnOnce() -> Result<()>,
) -> Result<(Vec<u8>, Identity)> {
    let mut file = File::from(openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let before = file.metadata()?;
    ensure!(
        before.is_file()
            && before.nlink() == 1
            && before.uid() == rustix::process::geteuid().as_raw()
            && before.mode() & 0o7777 == 0o600
            && before.len() <= limit,
        "Docker config file has invalid type, ownership, permissions, links or size"
    );
    let fingerprint = |metadata: &Metadata| {
        (
            Identity::of(metadata),
            metadata.mode(),
            metadata.uid(),
            metadata.gid(),
            metadata.nlink(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    let mut bytes = Vec::new();
    (&mut file).take(limit + 1).read_to_end(&mut bytes)?;
    checkpoint()?;
    ensure!(
        bytes.len() as u64 == before.len()
            && fingerprint(&file.metadata()?) == fingerprint(&before),
        "Docker config file changed during bounded read"
    );
    let current = File::from(openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    ensure!(
        fingerprint(&current.metadata()?) == fingerprint(&before),
        "Docker config file replaced during bounded read"
    );
    Ok((bytes, Identity::of(&before)))
}

fn validate_json(bytes: &[u8]) -> Result<()> {
    ensure!(
        bytes.len() as u64 <= CONFIG_LIMIT,
        "Docker config exceeds private size limit"
    );
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| anyhow::anyhow!("Docker config must contain a valid JSON object"))?;
    ensure!(
        value.is_object(),
        "Docker config must contain a valid JSON object"
    );
    Ok(())
}

#[cfg(test)]
#[path = "machine_docker_config_tests.rs"]
mod tests;
