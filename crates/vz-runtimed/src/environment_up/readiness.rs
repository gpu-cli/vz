//! Readiness evidence is separate from artifact selection and Engine startup.
use super::*;
use crate::machine_artifact_store::PinnedMachineArtifacts;
use crate::machine_docker_context::ManagedMachineDockerContext;
use crate::machine_docker_host::HostDockerClient;
use crate::machine_docker_runtime_inventory::VerifiedMachineRuntimeInventory;
use std::path::Path;

/// The PTY readiness probe's own budget and window.
///
/// Short, because this runs inside the Up deadline and a Machine that cannot
/// answer on a terminal in this long will not answer at all. The window is the
/// classic 24x80: nothing reads it, but a zero dimension is refused upstream.
const POSIX_PTY_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Where the guest initramfs records declared fabric ports it could not apply.
const FABRIC_UNCONFIGURED_PATH: &str = "/run/vz-fabric-unconfigured";
const FABRIC_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const POSIX_PTY_PROBE_ROWS: u32 = 24;
const POSIX_PTY_PROBE_COLUMNS: u32 = 80;

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

/// Whether this Machine's guest can run a command on a real PTY.
///
/// `Ok(false)` means the guest answered and the answer was no; the Machine
/// boots without the capability. `Err` is reserved for a transport failure,
/// where we learned nothing and must not publish readiness on a guess.
/// Every declared fabric port this boot could not configure, or `None`.
///
/// The guest records them one per line in `/run/vz-fabric-unconfigured`; an
/// absent file is the healthy case and the overwhelmingly common one, so the
/// probe is a single `cat` that is allowed to fail. `Err` is reserved for a
/// transport failure, where we learned nothing and must not publish readiness
/// on a guess -- the same rule `measure_posix_pty` follows.
async fn measure_unconfigured_fabric_ports(
    activation: &Arc<MachineRuntimeActivation>,
    metadata: &RequestMetadata,
) -> Result<Option<String>, MachineError> {
    let lease = activation.execution_lease();
    let ticket = lease
        .prepare_machine_exec_request()
        .await
        .map_err(|error| {
            failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                format!("fabric-port readiness could not obtain an execution ticket: {error}"),
            )
        })?;
    let deadline = tokio::time::Instant::now() + FABRIC_PROBE_TIMEOUT;
    let started = lease
        .start_machine_exec(
            vz_linux::ContainerExecDispatchGate::new(deadline),
            ticket,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!("cat {FABRIC_UNCONFIGURED_PATH} 2>/dev/null; printf ''"),
            ],
            Default::default(),
            None,
        )
        .await
        .map_err(|error| {
            failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                format!("fabric-port readiness probe could not start: {error}"),
            )
        })?;
    let (stream, _) = started;
    let observed = match tokio::time::timeout_at(deadline, stream.collect()).await {
        Ok(observed) => observed,
        Err(_) => {
            return Err(failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                "fabric-port readiness probe exceeded its own budget; original activation retained",
            ));
        }
    };
    let listed = observed
        .stdout
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(", ");
    Ok((!listed.is_empty()).then_some(listed))
}

async fn measure_posix_pty(
    activation: &Arc<MachineRuntimeActivation>,
    metadata: &RequestMetadata,
) -> Result<bool, MachineError> {
    let lease = activation.execution_lease();
    let ticket = match lease.prepare_machine_exec_request().await {
        Ok(ticket) => ticket,
        Err(error) => {
            return Err(failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                format!("PTY readiness could not obtain an execution ticket: {error}"),
            ));
        }
    };
    let deadline = tokio::time::Instant::now() + POSIX_PTY_PROBE_TIMEOUT;
    let started = lease
        .start_machine_exec(
            vz_linux::ContainerExecDispatchGate::new(deadline),
            ticket,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                "test -t 0 && test -t 1 && printf vz-up-posix-pty".into(),
            ],
            Default::default(),
            Some((POSIX_PTY_PROBE_ROWS, POSIX_PTY_PROBE_COLUMNS)),
        )
        .await;
    let (stream, _) = match started {
        Ok(started) => started,
        // The backend refusing to start a PTY exec at all is the guest saying
        // it has none, which is exactly what this measures.
        Err(_) => return Ok(false),
    };
    // The probe owns guest effects until it finishes, so the timeout bounds
    // the WAIT and never abandons the future: a dropped observation is not
    // evidence that the guest stopped.
    let observed = match tokio::time::timeout_at(deadline, stream.collect()).await {
        Ok(observed) => observed,
        Err(_) => {
            return Err(failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                "PTY readiness probe exceeded its own budget; original activation retained",
            ));
        }
    };
    Ok(observed.exit_code == 0 && observed.stdout == "vz-up-posix-pty")
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
        // Measure PTY execution, so a Machine that has it can negotiate it.
        //
        // The guest agent has implemented `exec_pty` for the supervised
        // Machine path all along (vz-guest-agent grpc_server.rs), and the whole
        // chain down to it is wired -- `MachineExecutionSpec.terminal`, the
        // supervisor, `term_rows`/`term_cols`. What was missing was any
        // measurement, so `PosixPty` never entered `negotiated_capabilities`
        // and `machine_exec.rs` refused every `vz exec --tty` against a Linux
        // Machine. `host-target-capabilities-v0.4.json` recorded that
        // faithfully: "Linux readiness measures only posix_exec plus Docker
        // capabilities".
        //
        // The probe is the one native macOS readiness already uses, because
        // the claim is the same one: `test -t 0 && test -t 1` is true only on
        // a real terminal, so a pipe dressed up as one cannot answer it, and
        // the sentinel proves the bytes came back through that terminal
        // rather than from a shell that exited early.
        //
        // A Machine whose PTY does not answer is NOT a readiness failure. It
        // simply does not negotiate the capability, and an exec that wanted
        // one is refused later with a message naming exactly that -- which is
        // a better outcome than refusing the boot of a Machine whose declared
        // work never needed a terminal.
        match measure_posix_pty(activation, metadata).await {
            Ok(true) => measured.push(MachineCapability::PosixPty),
            Ok(false) => {}
            Err(error) => return Err(error),
        }
        // A declared fabric port that the guest could not configure is a
        // READINESS FAILURE, not a console warning.
        //
        // The initramfs waits a bounded time for the NIC whose MAC the host
        // minted and, when it never appears, used to log
        // "fabric port left unconfigured" and carry on booting. The Machine
        // then reported `ready` holding nothing but `docker0`, so its declared
        // private path did not exist while status said it did -- and the first
        // symptom was an unrelated "Network is unreachable" from whatever tried
        // to use it. Refusing here names the cause at the point it happened.
        if let Some(ports) = measure_unconfigured_fabric_ports(activation, metadata).await? {
            return Err(failure(
                metadata,
                MachineErrorCode::BackendUnavailable,
                format!(
                    "Machine declares fabric port(s) the guest could not configure, so it is not on the \
                     network its definition declares: {ports}. The host minted the address and put it on \
                     the kernel cmdline; no interface carrying that MAC appeared. Original Machine \
                     retained for Stop."
                ),
            ));
        }
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
