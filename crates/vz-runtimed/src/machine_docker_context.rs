//! Stable native Docker contexts, backed by exact Machine-store claims.
//! The claim is durable before host publication. Stop retains logical context
//! identity; neither publication nor inspection changes Docker's default.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use vz_runtime_contract::{
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

#[cfg(test)]
#[path = "machine_docker_context_tests.rs"]
mod tests;
