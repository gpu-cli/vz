//! Stable native Docker contexts, backed by exact Machine-store claims.
//! The claim is durable before host publication. Stop retains logical context
//! identity; neither publication nor inspection changes Docker's default.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use vz_runtime_contract::{
    EnvironmentLifecycleKind, EnvironmentLifecycleOperation, EnvironmentLifecycleStatus,
    LifecycleOperationId, MachineDockerContextDescriptor, MachineIncarnation, OwnedResourceKind,
    ResourceOwner,
};

use crate::machine_docker_host::HostDockerClient;
use crate::machine_runtime_registry::MachineRuntimeStoreLease;

const CLAIM: &str = "docker-context.json";
const CLAIM_LIMIT: u64 = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextClaim {
    schema_version: u32,
    owner: ResourceOwner,
    name: String,
    endpoint: String,
    config_dir: String,
    nonce: String,
}

pub struct ManagedMachineDockerContext {
    claim: ContextClaim,
    store: Arc<MachineRuntimeStoreLease>,
}

impl ManagedMachineDockerContext {
    pub fn name(&self) -> &str {
        &self.claim.name
    }

    /// Read-only Delete admission, including a claim published before failed Up.
    /// No descriptor/claim means no authority to adopt a context at the derived
    /// name: its exact metadata and TLS paths must both be absent.
    pub fn prepare_existing_delete(
        store: Arc<MachineRuntimeStoreLease>,
        expected_descriptor: Option<&MachineDockerContextDescriptor>,
        expected_config_dir: &Path,
        expected_socket: &Path,
    ) -> Result<Option<PreparedMachineDockerContextDelete>> {
        store.validate_current()?;
        ensure!(
            expected_config_dir.is_absolute() && expected_socket.is_absolute(),
            "Delete context paths must be absolute"
        );
        let name =
            store
                .owner()
                .bounded_resource_name(&OwnedResourceKind::DockerContext, "docker", 64)?;
        let key = format!("{:x}", Sha256::digest(name.as_bytes()));
        let location = ContextDeleteLocation::open(expected_config_dir, &key)?;
        let Some(claim) = read_claim(&store)? else {
            ensure!(
                expected_descriptor.is_none(),
                "published context has no durable claim"
            );
            ensure!(
                location.directories[3].is_none(),
                "unclaimed context exists at selected name"
            );
            ensure!(
                delete_read_file(store.data_directory(), DELETE_INTENT, Some(0o600))?.is_none(),
                "context Delete intent has no claim"
            );
            return Ok(None);
        };
        ensure!(
            claim.owner == *store.owner()
                && claim.name == name
                && Path::new(&claim.config_dir) == expected_config_dir
                && claim.endpoint
                    == format!(
                        "unix://{}",
                        expected_socket.to_str().context("non-UTF8 Docker socket")?
                    )
                && claim.nonce.len() == 36
                && claim.nonce.starts_with("lop_")
                && claim.nonce[4..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "Delete context claim has foreign ownership or connection identity"
        );
        if let Some(descriptor) = expected_descriptor {
            descriptor.validate()?;
            ensure!(
                descriptor.owner == claim.owner
                    && descriptor.name == claim.name
                    && descriptor.config_dir == claim.config_dir
                    && descriptor.endpoint == claim.endpoint,
                "Delete descriptor differs from durable context claim"
            );
        }
        let claim_file = delete_read_file(store.data_directory(), CLAIM, Some(0o600))?
            .context("context claim disappeared")?;
        ensure!(
            serde_json::from_slice::<ContextClaim>(&claim_file.bytes)? == claim,
            "context claim changed during Delete admission"
        );
        let previous_file = delete_read_file(store.data_directory(), DELETE_INTENT, Some(0o600))?;
        let previous = previous_file
            .as_ref()
            .map(|file| serde_json::from_slice::<ContextDeleteIntent>(&file.bytes))
            .transpose()?;
        let snapshot = location.snapshot(&claim, previous.as_ref())?;
        if let Some(intent) = &previous {
            ensure!(
                intent.schema_version == 1
                    && intent.claim == claim
                    && intent.claim_identity == claim_file.identity,
                "Delete intent belongs to another context claim"
            );
        }
        Ok(Some(PreparedMachineDockerContextDelete {
            store,
            claim,
            claim_identity: claim_file.identity,
            key,
            location,
            snapshot,
            previous,
            intent_identity: previous_file.map(|file| file.identity),
        }))
    }

    /// Ensure exactly this context. A foreign/malformed existing context is
    /// never updated, replaced, adopted or deleted, even at the derived name.
    pub async fn ensure(
        client: &HostDockerClient,
        store: Arc<MachineRuntimeStoreLease>,
        socket: &Path,
    ) -> Result<Self> {
        Self::ensure_before(
            client,
            store,
            socket,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await
    }

    pub async fn ensure_before(
        client: &HostDockerClient,
        store: Arc<MachineRuntimeStoreLease>,
        socket: &Path,
        deadline: tokio::time::Instant,
    ) -> Result<Self> {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "startup deadline exhausted before context admission"
        );
        store.validate_current()?;
        ensure!(
            socket.is_absolute(),
            "Machine Docker socket must be absolute"
        );
        let mut expected = ContextClaim {
            schema_version: 1,
            owner: store.owner().clone(),
            name: store.owner().bounded_resource_name(
                &OwnedResourceKind::DockerContext,
                "docker",
                64,
            )?,
            endpoint: format!(
                "unix://{}",
                socket.to_str().context("non-UTF8 Machine Docker socket")?
            ),
            config_dir: client
                .config_dir()
                .to_str()
                .context("non-UTF8 Docker config directory")?
                .into(),
            nonce: LifecycleOperationId::generate().to_string(),
        };
        let claim = match read_claim(&store)? {
            Some(claim) => {
                expected.nonce = claim.nonce.clone();
                ensure!(
                    claim == expected,
                    "Machine Docker context claim changed ownership or connection identity"
                );
                claim
            }
            None => {
                publish_claim(&store, &expected)?;
                expected
            }
        };
        let context = Self { claim, store };
        let inspection = client
            .run(
                None,
                &[
                    "context".into(),
                    "inspect".into(),
                    context.claim.name.clone(),
                ],
                None,
                Duration::from_secs(10),
            )
            .await?;
        if inspection.status.success() {
            context.verify_inspection(&inspection.stdout)?;
            return Ok(context);
        }
        // An unknown CLI failure is not absence. This exact named not-found
        // response is checked before the create-only Docker operation; a race
        // or foreign existing context remains a conflict, never an update.
        let error = std::str::from_utf8(&inspection.stderr)?;
        ensure!(
            error.starts_with(&format!(
                "context {:?}: context not found:",
                context.claim.name
            )),
            "cannot establish absence of exact managed Docker context"
        );
        context.store.validate_current()?;
        ensure!(
            tokio::time::Instant::now() < deadline,
            "startup deadline exhausted before context creation"
        );
        client
            .run(
                None,
                &[
                    "context".into(),
                    "create".into(),
                    "--description".into(),
                    context.description()?,
                    "--docker".into(),
                    format!("host={}", context.claim.endpoint),
                    context.claim.name.clone(),
                ],
                None,
                Duration::from_secs(10),
            )
            .await?
            .success()?;
        context.verify(client).await?;
        Ok(context)
    }

    pub async fn verify(&self, client: &HostDockerClient) -> Result<()> {
        self.store.validate_current()?;
        ensure!(
            read_claim(&self.store)?.as_ref() == Some(&self.claim),
            "managed Docker claim was replaced"
        );
        ensure!(
            client.config_dir().to_str() == Some(&self.claim.config_dir),
            "Docker configuration changed"
        );
        let output = client
            .run(
                None,
                &["context".into(), "inspect".into(), self.claim.name.clone()],
                None,
                Duration::from_secs(10),
            )
            .await?
            .success()?;
        self.verify_inspection(&output.stdout)
    }

    pub fn descriptor(
        &self,
        incarnation: &MachineIncarnation,
        engine_id: String,
    ) -> Result<MachineDockerContextDescriptor> {
        ensure!(
            self.claim.owner.machine_id.as_ref() == Some(&incarnation.machine_id),
            "context incarnation has foreign Machine"
        );
        let descriptor = MachineDockerContextDescriptor {
            schema_version: 1,
            owner: self.claim.owner.clone(),
            name: self.claim.name.clone(),
            endpoint: self.claim.endpoint.clone(),
            config_dir: self.claim.config_dir.clone(),
            engine_id,
            incarnation_id: incarnation.incarnation_id.clone(),
            incarnation_generation: incarnation.generation,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn description(&self) -> Result<String> {
        Ok(format!(
            "vz managed Machine context v1 {}",
            serde_json::to_string(&self.claim)?
        ))
    }

    fn verify_inspection(&self, bytes: &[u8]) -> Result<()> {
        let rows: Vec<Value> = serde_json::from_slice(bytes)?;
        ensure!(
            rows.len() == 1,
            "Docker returned ambiguous context inspection"
        );
        let row = &rows[0];
        ensure!(
            row["Name"] == self.claim.name
                && row["Metadata"]["Description"] == self.description()?,
            "Docker context is not this Machine's claimed context"
        );
        let endpoints = row["Endpoints"]
            .as_object()
            .context("context endpoints missing")?;
        ensure!(
            endpoints.len() == 1
                && row["Endpoints"]["docker"]["Host"] == self.claim.endpoint
                && row["Endpoints"]["docker"]["SkipTLSVerify"] == false,
            "Docker context endpoint changed or contains another transport"
        );
        ensure!(
            row["TLSMaterial"]
                .as_object()
                .is_some_and(|value| value.is_empty()),
            "unexpected Docker context credentials"
        );
        Ok(())
    }
}

fn read_claim(store: &MachineRuntimeStoreLease) -> Result<Option<ContextClaim>> {
    use rustix::fs::{Mode, OFlags, openat};
    store.validate_current()?;
    let fd = match openat(
        store.data_directory(),
        CLAIM,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let file = File::from(fd);
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.nlink() == 1
            && metadata.mode() & 0o777 == 0o600
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.len() <= CLAIM_LIMIT,
        "invalid Machine Docker claim file"
    );
    let mut bytes = Vec::new();
    file.take(CLAIM_LIMIT + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= CLAIM_LIMIT,
        "Machine Docker claim too large"
    );
    let claim: ContextClaim = serde_json::from_slice(&bytes)?;
    ensure!(
        claim.schema_version == 1 && claim.nonce.starts_with("lop_") && claim.nonce.len() <= 256,
        "invalid Machine Docker claim version or nonce"
    );
    Ok(Some(claim))
}

fn publish_claim(store: &MachineRuntimeStoreLease, claim: &ContextClaim) -> Result<()> {
    use rustix::fs::{Mode, OFlags, openat};
    let bytes = serde_json::to_vec(claim)?;
    ensure!(
        bytes.len() as u64 <= CLAIM_LIMIT,
        "Machine Docker claim too large"
    );
    store.validate_current()?;
    let mut file = File::from(openat(
        store.data_directory(),
        CLAIM,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    file.write_all(&bytes)?;
    file.sync_all()?;
    store.data_directory().sync_all()?;
    ensure!(
        read_claim(store)?.as_ref() == Some(claim),
        "Machine Docker claim publication mismatch"
    );
    Ok(())
}

const DELETE_INTENT: &str = "docker-context-delete.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextFileIdentity {
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
}

impl ContextFileIdentity {
    fn of(file: &File) -> Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            mode: metadata.mode(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextDeleteSnapshot {
    directories: [Option<ContextFileIdentity>; 4],
    metadata: Option<ContextFileIdentity>,
    metadata_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextDeleteOperation {
    operation_id: LifecycleOperationId,
    generation: u64,
    request_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextDeleteIntent {
    schema_version: u32,
    claim: ContextClaim,
    claim_identity: ContextFileIdentity,
    operation: ContextDeleteOperation,
    snapshot: ContextDeleteSnapshot,
}

struct ContextDeleteLocation {
    // config / contexts / meta / SHA256(context name), opened without following
    // symlinks. Shared ancestors are retained, never removed.
    directories: [Option<File>; 4],
}

impl ContextDeleteLocation {
    fn open(config: &Path, key: &str) -> Result<Self> {
        use crate::machine_runtime_registry::{
            MachineRuntimeRegistryError, open_trusted_registry_root,
        };
        let mut directories: [Option<File>; 4] = std::array::from_fn(|_| None);
        let root = match open_trusted_registry_root(config) {
            Ok(root) => root,
            Err(MachineRuntimeRegistryError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(Self { directories });
            }
            Err(error) => return Err(error.into()),
        };
        delete_validate_directory(&root)?;
        directories[0] = Some(root);
        for (index, name) in [(1, "contexts"), (2, "meta"), (3, key)] {
            if let Some(parent) = &directories[index - 1] {
                directories[index] = delete_open_directory(parent, name)?;
            }
        }
        if let Some(contexts) = &directories[1] {
            if let Some(tls) = delete_open_directory(contexts, "tls")? {
                use rustix::fs::{AtFlags, statat};
                match statat(&tls, key, AtFlags::SYMLINK_NOFOLLOW) {
                    Err(rustix::io::Errno::NOENT) => {}
                    Ok(_) => {
                        anyhow::bail!("selected managed Unix context has unowned TLS material")
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(Self { directories })
    }

    fn snapshot(
        &self,
        claim: &ContextClaim,
        previous: Option<&ContextDeleteIntent>,
    ) -> Result<ContextDeleteSnapshot> {
        let mut identities = std::array::from_fn(|_| None);
        for (index, directory) in self.directories.iter().enumerate() {
            identities[index] = directory
                .as_ref()
                .map(ContextFileIdentity::of)
                .transpose()?;
        }
        let metadata = if let Some(directory) = &self.directories[3] {
            let names = delete_inventory(directory)?;
            ensure!(
                names.is_empty() || names == [b"meta.json".to_vec()],
                "context directory contains unexpected resources"
            );
            let metadata = delete_read_file(directory, "meta.json", None)?;
            if let Some(file) = &metadata {
                let expected = serde_json::json!({"Name": claim.name,
                    "Metadata": {"Description": format!("vz managed Machine context v1 {}", serde_json::to_string(claim)?)},
                    "Endpoints": {"docker": {"Host": claim.endpoint, "SkipTLSVerify": false}}});
                ensure!(
                    serde_json::from_slice::<Value>(&file.bytes)? == expected,
                    "native context metadata is not the exact nonce-bound claim"
                );
            } else {
                ensure!(
                    previous.is_some(),
                    "empty context directory has no deletion authority"
                );
            }
            metadata
        } else {
            None
        };
        let observed = ContextDeleteSnapshot {
            directories: identities,
            metadata: metadata.as_ref().map(|file| file.identity.clone()),
            metadata_sha256: metadata
                .as_ref()
                .map(|file| format!("{:x}", Sha256::digest(&file.bytes))),
        };
        if let Some(previous) = previous {
            let expected = &previous.snapshot;
            for index in 0..4 {
                ensure!(
                    observed.directories[index] == expected.directories[index]
                        || (index == 3 && observed.directories[index].is_none()),
                    "context path was replaced since Delete admission"
                );
            }
            ensure!(
                observed.metadata.is_none()
                    || (observed.metadata == expected.metadata
                        && observed.metadata_sha256 == expected.metadata_sha256),
                "context metadata was replaced since Delete admission"
            );
            return Ok(expected.clone());
        }
        Ok(observed)
    }
}

struct DeleteFile {
    identity: ContextFileIdentity,
    bytes: Vec<u8>,
}

fn delete_read_file(
    parent: &File,
    name: &str,
    exact_mode: Option<u32>,
) -> Result<Option<DeleteFile>> {
    use rustix::fs::{Mode, OFlags, openat};
    let fd = match openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut file = File::from(fd);
    let before = file.metadata()?;
    ensure!(
        before.is_file()
            && before.nlink() == 1
            && before.uid() == rustix::process::geteuid().as_raw()
            && before.mode() & 0o022 == 0
            && exact_mode.is_none_or(|mode| before.mode() & 0o777 == mode)
            && before.len() <= CLAIM_LIMIT,
        "invalid context deletion file"
    );
    let identity = ContextFileIdentity::of(&file)?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(CLAIM_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    ensure!(
        bytes.len() as u64 == before.len()
            && ContextFileIdentity::of(&file)? == identity
            && after.nlink() == 1
            && after.len() == before.len()
            && after.mtime() == before.mtime()
            && after.mtime_nsec() == before.mtime_nsec()
            && after.ctime() == before.ctime()
            && after.ctime_nsec() == before.ctime_nsec(),
        "context deletion file changed while read"
    );
    Ok(Some(DeleteFile { identity, bytes }))
}

fn delete_validate_directory(directory: &File) -> Result<()> {
    let metadata = directory.metadata()?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o022 == 0,
        "untrusted Docker context directory"
    );
    Ok(())
}

fn delete_open_directory(parent: &File, name: &str) -> Result<Option<File>> {
    use rustix::fs::{Mode, OFlags, openat};
    match openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => {
            let directory = File::from(fd);
            delete_validate_directory(&directory)?;
            Ok(Some(directory))
        }
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn delete_inventory(directory: &File) -> Result<Vec<Vec<u8>>> {
    let mut names = Vec::new();
    for entry in rustix::fs::Dir::read_from(directory)? {
        let entry = entry?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec());
        }
        ensure!(
            names.len() <= 1,
            "context directory contains unexpected resources"
        );
    }
    Ok(names)
}

fn delete_operation_binding(
    owner: &ResourceOwner,
    operation: &EnvironmentLifecycleOperation,
) -> Result<ContextDeleteOperation> {
    ensure!(
        operation.kind == EnvironmentLifecycleKind::Delete
            && operation.schema_version == 1
            && operation.status == EnvironmentLifecycleStatus::Running
            && operation.generation > 0
            && operation.project_id == owner.project_id
            && operation.environment_id == owner.environment_id
            && operation
                .machine_steps
                .iter()
                .any(|step| Some(&step.machine_id) == owner.machine_id.as_ref()),
        "context deletion requires this owner's exact Delete operation"
    );
    let request_hash = operation
        .request_hash
        .strip_prefix("sha256:")
        .context("Delete request hash missing")?;
    ensure!(
        request_hash.len() == 64
            && request_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "Delete request hash invalid"
    );
    Ok(ContextDeleteOperation {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        request_hash: operation.request_hash.clone(),
    })
}

/// Read-only prerequisite for removing the Machine store's context authorities.
/// No claim requires no orphaned Delete intent; the caller must already have
/// prepared the derived context's absence with trusted config/socket inputs.
/// A claim requires an exact operation-bound intent AND current host absence.
pub(crate) fn require_deleted_for_store(
    store: &MachineRuntimeStoreLease,
    operation: &EnvironmentLifecycleOperation,
) -> Result<()> {
    store.validate_current()?;
    let binding = delete_operation_binding(store.owner(), operation)?;
    let Some(claim) = read_claim(store)? else {
        ensure!(
            delete_read_file(store.data_directory(), DELETE_INTENT, Some(0o600))?.is_none(),
            "orphaned context Delete intent cannot authorize store removal"
        );
        return Ok(());
    };
    ensure!(
        claim.owner == *store.owner()
            && claim.name
                == store.owner().bounded_resource_name(
                    &OwnedResourceKind::DockerContext,
                    "docker",
                    64
                )?,
        "store context claim has foreign ownership"
    );
    let claim_file = delete_read_file(store.data_directory(), CLAIM, Some(0o600))?
        .context("store context claim disappeared")?;
    ensure!(
        serde_json::from_slice::<ContextClaim>(&claim_file.bytes)? == claim,
        "store context claim changed"
    );
    let intent_file = delete_read_file(store.data_directory(), DELETE_INTENT, Some(0o600))?
        .context("context deletion is not durably authorized")?;
    let intent: ContextDeleteIntent = serde_json::from_slice(&intent_file.bytes)?;
    ensure!(
        intent.schema_version == 1
            && intent.claim == claim
            && intent.claim_identity == claim_file.identity
            && intent.operation == binding,
        "store context deletion intent has different owner or operation"
    );
    let key = format!("{:x}", Sha256::digest(claim.name.as_bytes()));
    let location = ContextDeleteLocation::open(Path::new(&claim.config_dir), &key)?;
    location.snapshot(&claim, Some(&intent))?;
    ensure!(
        location.directories[3].is_none(),
        "host context deletion is incomplete; retain Machine store authorities"
    );
    store.validate_current()?;
    Ok(())
}

/// Prepared filesystem-only deletion of one exactly claimed native context.
/// The caller must hold the Environment operation fence and prove quiescence.
/// Retained no-follow parent FDs and revalidation reject observed replacements;
/// POSIX unlinkat is not an atomic inode-CAS. Root/euid remain trusted, as with
/// the registry/socket primitives. Claim and intent survive until store deletion.
pub struct PreparedMachineDockerContextDelete {
    store: Arc<MachineRuntimeStoreLease>,
    claim: ContextClaim,
    claim_identity: ContextFileIdentity,
    key: String,
    location: ContextDeleteLocation,
    snapshot: ContextDeleteSnapshot,
    previous: Option<ContextDeleteIntent>,
    intent_identity: Option<ContextFileIdentity>,
}

impl PreparedMachineDockerContextDelete {
    pub fn remove_exact(&mut self, operation: &EnvironmentLifecycleOperation) -> Result<()> {
        self.remove_with_checkpoint(operation, || Ok(()))
    }

    fn remove_with_checkpoint(
        &mut self,
        operation: &EnvironmentLifecycleOperation,
        after_metadata: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.remove_with_checkpoints(operation, |_| Ok(()), after_metadata)
    }

    fn remove_with_checkpoints(
        &mut self,
        operation: &EnvironmentLifecycleOperation,
        before_publication: impl FnOnce(&File) -> Result<()>,
        after_metadata: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let binding = delete_operation_binding(&self.claim.owner, operation)?;
        let intent = ContextDeleteIntent {
            schema_version: 1,
            claim: self.claim.clone(),
            claim_identity: self.claim_identity.clone(),
            operation: binding,
            snapshot: self.snapshot.clone(),
        };
        if let Some(previous) = &self.previous {
            ensure!(
                *previous == intent,
                "context deletion belongs to another Delete operation"
            );
        }
        self.validate(&intent, self.previous.is_some())?;
        if self.previous.is_none() {
            use rustix::fs::{Mode, OFlags, RenameFlags, openat, renameat_with};
            let bytes = serde_json::to_vec(&intent)?;
            ensure!(
                bytes.len() as u64 <= CLAIM_LIMIT,
                "context Delete intent too large"
            );
            // Only complete, fsynced records become authoritative. Interrupted
            // temporary records remain private, non-authorizing store contents;
            // replay never adopts or removes a pending name by pattern.
            let pending = format!(
                ".{DELETE_INTENT}.pending-{}",
                LifecycleOperationId::generate()
            );
            let mut file = File::from(openat(
                self.store.data_directory(),
                pending.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            file.write_all(&bytes)?;
            file.sync_all()?;
            before_publication(&file)?;
            self.validate(&intent, false)?;
            let identity = ContextFileIdentity::of(&file)?;
            let current = delete_read_file(self.store.data_directory(), &pending, Some(0o600))?
                .context("pending context Delete intent disappeared")?;
            ensure!(
                current.identity == identity && current.bytes == bytes,
                "pending context Delete intent changed before publication"
            );
            renameat_with(
                self.store.data_directory(),
                pending.as_str(),
                self.store.data_directory(),
                DELETE_INTENT,
                RenameFlags::NOREPLACE,
            )?;
            self.store.data_directory().sync_all()?;
            self.intent_identity = Some(identity);
            self.previous = Some(intent.clone());
        }
        self.validate(&intent, true)?;
        if let Some(directory) = &self.location.directories[3] {
            use rustix::fs::{AtFlags, unlinkat};
            if delete_read_file(directory, "meta.json", None)?.is_some() {
                unlinkat(directory, "meta.json", AtFlags::empty())?;
                directory.sync_all()?;
            }
            after_metadata()?;
            self.validate(&intent, true)?;
            ensure!(
                delete_inventory(directory)?.is_empty(),
                "context gained unexpected files during Delete"
            );
            let current =
                ContextDeleteLocation::open(Path::new(&self.claim.config_dir), &self.key)?;
            if current.directories[3].is_some() {
                let parent = self.location.directories[2]
                    .as_ref()
                    .context("context metadata parent missing")?;
                unlinkat(parent, self.key.as_str(), AtFlags::REMOVEDIR)?;
                parent.sync_all()?;
            }
        }
        self.validate(&intent, true)?;
        let current = ContextDeleteLocation::open(Path::new(&self.claim.config_dir), &self.key)?;
        ensure!(
            current.directories[3].is_none(),
            "context metadata remains after Delete"
        );
        Ok(())
    }

    fn validate(&self, intent: &ContextDeleteIntent, persisted: bool) -> Result<()> {
        self.store.validate_current()?;
        let claim = delete_read_file(self.store.data_directory(), CLAIM, Some(0o600))?
            .context("context claim disappeared")?;
        ensure!(
            claim.identity == self.claim_identity
                && serde_json::from_slice::<ContextClaim>(&claim.bytes)? == self.claim,
            "context claim replaced after Delete preparation"
        );
        if persisted {
            let journal =
                delete_read_file(self.store.data_directory(), DELETE_INTENT, Some(0o600))?
                    .context("context Delete intent disappeared")?;
            ensure!(
                Some(&journal.identity) == self.intent_identity.as_ref()
                    && serde_json::from_slice::<ContextDeleteIntent>(&journal.bytes)? == *intent,
                "context Delete intent was replaced"
            );
        }
        let current = ContextDeleteLocation::open(Path::new(&self.claim.config_dir), &self.key)?;
        let snapshot = current.snapshot(&self.claim, persisted.then_some(intent))?;
        ensure!(
            snapshot == self.snapshot,
            "context filesystem changed after Delete preparation"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "machine_docker_context_tests.rs"]
mod tests;
