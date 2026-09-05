//! Readiness evidence is separate from artifact selection and Engine startup.
use super::*;

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

pub(super) struct MeasuredLinuxReadiness;

#[tonic::async_trait]
impl ReadinessEvidenceProvider for MeasuredLinuxReadiness {
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
        let capabilities = CapabilitySet::new([MachineCapability::PosixExec]);
        if machine.profile == MachineProfile::Developer {
            return Err(failure(
                metadata,
                MachineErrorCode::UnsupportedOperation,
                "Developer Machine boot and private Engine endpoint exist, but required host Docker/Compose/buildx conformance and managed context evidence are absent; Machine is not Ready, original ownership is retained for Stop",
            ));
        }
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
