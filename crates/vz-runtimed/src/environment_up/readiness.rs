//! Readiness evidence is separate from artifact selection and Engine startup.
use super::*;
use crate::machine_artifact_store::PinnedMachineArtifacts;
use crate::machine_docker_context::ManagedMachineDockerContext;
use crate::machine_docker_host::HostDockerClient;
use crate::machine_docker_runtime_inventory::VerifiedMachineRuntimeInventory;
use std::path::Path;

/// Await the owned probe even after expiry, then refuse late readiness. A
/// timeout must never abandon the future that still owns its guest effects.
pub(super) async fn await_readiness<T>(
    work: impl std::future::Future<Output = Result<T, MachineError>>,
    deadline: tokio::time::Instant,
    metadata: &RequestMetadata,
) -> Result<T, MachineError> {
    let value = work.await?;
    if tokio::time::Instant::now() >= deadline {
        return Err(failure(
            metadata,
            MachineErrorCode::Timeout,
            "readiness probe finished after Up deadline; original activation retained, no late Ready publication",
        ));
    }
    Ok(value)
}

#[tonic::async_trait]
pub(super) trait ReadinessEvidenceProvider: Send + Sync {
    async fn verify(
        &self,
        activation: &Arc<MachineRuntimeActivation>,
        machine: &MachineInstance,
        incarnation: MachineIncarnation,
        metadata: &RequestMetadata,
    ) -> Result<MachineActivationEvidence, MachineError>;
}

pub(super) struct MeasuredLinuxReadiness<'a> {
    pub pin: &'a PinnedMachineArtifacts,
    pub docker_endpoint: Option<&'a Path>,
    pub deadline: tokio::time::Instant,
}

#[tonic::async_trait]
impl ReadinessEvidenceProvider for MeasuredLinuxReadiness<'_> {
    async fn verify(
        &self,
        activation: &Arc<MachineRuntimeActivation>,
        machine: &MachineInstance,
        incarnation: MachineIncarnation,
        metadata: &RequestMetadata,
    ) -> Result<MachineActivationEvidence, MachineError> {
        // Execute only through the original exact-boot lease; no host command,
        // OCI container alias, or capability inferred from installed artifacts.
        let probe = activation
            .exec(
                "/bin/sh".into(),
                vec!["-c".into(), "printf vz-up-posix-ready".into()],
                Duration::from_secs(10),
            )
            .await
            .map_err(|error| failure(metadata, MachineErrorCode::BackendUnavailable, error))?;
        if probe.exit_code != 0 || probe.stdout != "vz-up-posix-ready" || !probe.stderr.is_empty() {
            return Err(failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                "exact Machine POSIX readiness probe failed",
            ));
        }
        let mut measured = vec![MachineCapability::PosixExec];
        let docker_context = if machine.profile == MachineProfile::Developer {
            let backend_error = |error: anyhow::Error| {
                failure(
                    metadata,
                    MachineErrorCode::BackendUnavailable,
                    format!(
                        "Developer operational readiness failed; original Machine retained for Stop: {error:#}"
                    ),
                )
            };
            self.pin
                .validate_current()
                .map_err(|error| backend_error(error.into()))?;
            let probe = self.pin.developer_probe().ok_or_else(|| failure(
                metadata, MachineErrorCode::UnsupportedOperation,
                "Developer bundle lacks a digest-bound offline startup probe; install a current bundle, no image or Engine fallback is allowed",
            ))?;
            let endpoint = self.docker_endpoint.ok_or_else(|| {
                failure(
                    metadata,
                    MachineErrorCode::BackendUnavailable,
                    "exact Developer Machine has no retained Docker endpoint",
                )
            })?;
            let client = HostDockerClient::discover_for_machine(Arc::clone(self.pin.store()))
                .map_err(backend_error)?;
            let inventory = VerifiedMachineRuntimeInventory::measure(
                activation,
                &incarnation,
                &self.pin.configuration().artifact.youki_sha256,
            )
            .await
            .map_err(backend_error)?;
            let context = ManagedMachineDockerContext::ensure_before(
                &client,
                Arc::clone(self.pin.store()),
                endpoint,
                self.deadline,
            )
            .await
            .map_err(backend_error)?;
            let evidence = crate::machine_docker_operational_probe::verify(
                &client,
                &context,
                Arc::clone(self.pin.store()),
                probe,
                &inventory,
                &incarnation,
                self.deadline,
            )
            .await
            .map_err(backend_error)?;
            let required = CapabilitySet::new([
                MachineCapability::DockerEngine,
                MachineCapability::Compose,
                MachineCapability::Buildx,
            ]);
            if evidence.schema_version != 1
                || evidence.owner != *activation.owner()
                || evidence.incarnation != incarnation
                || evidence.configuration_digest != self.pin.store().configuration_digest()
                || evidence.archive_sha256 != probe.metadata.sha256
                || evidence.client_sha256 != client.executable_sha256()
                || evidence.runtime_inventory
                    != serde_json::to_value(&inventory)
                        .map_err(|error| backend_error(error.into()))?
                || !evidence.cleanup_confirmed
                || evidence.cleanup_scope
                    != "disposable_probe_containers_compose_objects_and_images"
                || !evidence.retained_buildkit_cache
                || evidence.capabilities != required
            {
                return Err(failure(
                    metadata,
                    MachineErrorCode::BackendUnavailable,
                    "Docker operational evidence does not match the exact retained Machine incarnation, artifacts, client and required capabilities",
                ));
            }
            context.verify(&client).await.map_err(backend_error)?;
            let after_inventory = VerifiedMachineRuntimeInventory::measure(
                activation,
                &incarnation,
                &self.pin.configuration().artifact.youki_sha256,
            )
            .await
            .map_err(backend_error)?;
            if after_inventory.stdout() != inventory.stdout() {
                return Err(failure(
                    metadata,
                    MachineErrorCode::BackendUnavailable,
                    "Machine runtime inventory changed during Docker operational probes",
                ));
            }
            // Preserve the post-operation measurement beside the immutable
            // probe receipt, binding both before publishing activation evidence.
            self.pin
                .store()
                .validate_current()
                .map_err(|error| backend_error(error.into()))?;
            let after_path = evidence
                .receipt_path
                .with_file_name("runtime-inventory-after.json");
            use std::os::unix::fs::OpenOptionsExt;
            let mut after_file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&after_path)
                .map_err(|error| backend_error(error.into()))?;
            serde_json::to_writer(
                &mut after_file,
                &serde_json::json!({
                    "schema_version": 1, "probe_receipt_sha256": evidence.receipt_sha256,
                    "runtime_inventory": after_inventory,
                }),
            )
            .map_err(|error| backend_error(error.into()))?;
            after_file
                .sync_all()
                .map_err(|error| backend_error(error.into()))?;
            if let Some(directory) = after_path.parent() {
                std::fs::File::open(directory)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| backend_error(error.into()))?;
            }
            measured.extend([
                MachineCapability::DockerEngine,
                MachineCapability::Compose,
                MachineCapability::Buildx,
            ]);
            Some(
                context
                    .descriptor(&incarnation, evidence.engine_id)
                    .map_err(backend_error)?,
            )
        } else {
            None
        };
        let capabilities = CapabilitySet::new(measured);
        if !machine
            .requested_capabilities
            .unaccounted_by(&capabilities)
            .is_empty()
        {
            return Err(failure(
                metadata,
                MachineErrorCode::UnsupportedOperation,
                "requested Machine capabilities lack measured readiness evidence",
            ));
        }
        Ok(MachineActivationEvidence {
            schema_version: 1,
            docker_context,
            incarnation,
            backend: MachineBackend::MacosVirtualizationLinux,
            negotiated_capabilities: capabilities,
            runtime_identity: MachineRuntimeIdentity {
                schema_version: 1,
                opaque_id: serde_json::to_string(activation.runtime_identity())
                    .map_err(|error| failure(metadata, MachineErrorCode::InternalError, error))?,
            },
        })
    }
}
