//! Composite ownership of a private runtime store and one exact Linux boot.
//!
//! A backend VM lease alone does not keep its registry's filesystem lock alive.
//! Topology controller tasks retain this composite across readiness and evidence
//! publication, even if their owning daemon registry is dropped concurrently.

use crate::machine_backend::{MachineBackendRuntime as MacosRuntimeBackend, MachineExecutionLease};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use vz_oci_macos::{
    KernelProfile, MacosOciError as OciError, PortMapping, SharedVmDockerReadiness,
};
use vz_runtime_contract::{
    ExecOutput, OwnedResourceKind, OwnershipRecord, ResourceOwner, StackResourceHint,
    StackRuntimeIdentity, TOPOLOGY_SCHEMA_VERSION,
};

use crate::machine_runtime_registry::{
    MachineRuntimeEntry, MachineRuntimeRegistry, MachineRuntimeRegistryError,
};

#[derive(Debug, Error)]
pub enum MachineRuntimeActivationError {
    #[error(transparent)]
    Admission(#[from] MachineRuntimeRegistryError),
    #[error(transparent)]
    Runtime(#[from] OciError),
}

/// Retains the store lock until this exact VM lease is released.
///
/// The fields are private and the handle is not Clone. Dropping the registry
/// or other entry handles cannot let another backend constructor reconcile this
/// Machine's store while its activation is still held. This is not permission
/// to publish Ready or a substitute for the Environment generation fence.
#[must_use = "retain the activation through readiness and evidence publication"]
pub struct MachineRuntimeActivation {
    // Release the VM lifecycle fence before dropping the owning runtime store.
    lease: MachineExecutionLease,
    entry: Arc<MachineRuntimeEntry<MacosRuntimeBackend>>,
}

impl MachineRuntimeActivation {
    pub(crate) fn native_lease(&self) -> Option<&crate::native_macos::runtime::NativeMacosLease> {
        match &self.lease {
            MachineExecutionLease::Native(lease) => Some(lease),
            _ => None,
        }
    }
    pub(crate) fn execution_lease(&self) -> &MachineExecutionLease {
        &self.lease
    }
    pub(crate) fn entry(&self) -> &Arc<MachineRuntimeEntry<MacosRuntimeBackend>> {
        &self.entry
    }

    pub fn owner(&self) -> &ResourceOwner {
        self.entry.owner()
    }

    pub fn runtime_identity(&self) -> &StackRuntimeIdentity {
        self.lease.runtime_identity()
    }

    pub fn verified_profile(&self) -> Option<KernelProfile> {
        match &self.lease {
            MachineExecutionLease::Linux(l) => Some(l.verified_profile()),
            MachineExecutionLease::Native(_) => None,
        }
    }

    /// Guest-local readiness only; no host socket/context or capabilities are
    /// created or inferred by this operation.
    pub async fn ensure_docker_ready(&self) -> Result<SharedVmDockerReadiness, OciError> {
        match &self.lease {
            MachineExecutionLease::Linux(l) => l.ensure_docker_ready().await,
            MachineExecutionLease::Native(_) => {
                Err(OciError::InvalidConfig("native macOS has no Docker".into()))
            }
        }
    }

    /// Exact-boot Docker transport. An endpoint supervisor must retain this
    /// activation for every live stream, then drain clients before stopping VM.
    pub async fn open_docker_stream(&self) -> Result<vz_linux::GrpcDockerStream, OciError> {
        match &self.lease {
            MachineExecutionLease::Linux(l) => l.open_docker_stream().await,
            MachineExecutionLease::Native(_) => {
                Err(OciError::InvalidConfig("native macOS has no Docker".into()))
            }
        }
    }

    /// Execute in the exact leased guest without recursively acquiring a
    /// lifecycle read lock behind a queued shutdown writer.
    pub async fn exec(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, OciError> {
        let output = match &self.lease {
            MachineExecutionLease::Linux(l) => l.exec(command, args, timeout).await?,
            MachineExecutionLease::Native(l) => tokio::time::timeout(timeout, async {
                Ok::<_, OciError>(
                    l.client()
                        .await?
                        .exec_stream(command, args, Default::default())
                        .await?
                        .collect()
                        .await,
                )
            })
            .await
            .map_err(|_| OciError::InvalidConfig("native probe timed out".into()))??,
        };
        Ok(ExecOutput {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

impl MachineRuntimeEntry<MacosRuntimeBackend> {
    /// Derive the VM reservation independently of the persistent store record.
    /// Both records must be reserved by the topology controller before effects.
    pub fn vm_reservation(
        owner: &ResourceOwner,
    ) -> Result<OwnershipRecord, MachineRuntimeRegistryError> {
        // Validate all typed IDs and require a Machine owner before naming it.
        MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(owner)?;
        let resource_kind = OwnedResourceKind::Other("runtime_vm".to_string());
        let resource_id = owner
            .bounded_resource_name(&resource_kind, "vm", 64)
            .map_err(|error| MachineRuntimeRegistryError::Invalid(error.to_string()))?;
        Ok(OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind,
            resource_id,
            environment_id: owner.environment_id.clone(),
            machine_id: owner.machine_id.clone(),
        })
    }

    /// Acquire a boot whose physical name comes only from this entry's owner.
    ///
    /// The controller must have already resolved and verified the requested
    /// Machine target, validated both persisted reservations, and fenced its
    /// lifecycle operation/generation. This method does not resolve images or
    /// provide authorization. This method returns no detached raw VM lease;
    /// low-level backend access remains a trusted-library interface.
    pub async fn boot_or_inspect_machine(
        self: &Arc<Self>,
        reserved_vm: &OwnershipRecord,
        ports: Vec<PortMapping>,
        resources: StackResourceHint,
    ) -> Result<MachineRuntimeActivation, MachineRuntimeActivationError> {
        let reservation = Self::vm_reservation(self.owner())?;
        if &reservation != reserved_vm {
            return Err(MachineRuntimeRegistryError::Conflict(
                "VM reservation does not match the exact private runtime owner".to_string(),
            )
            .into());
        }
        let entry = Arc::clone(self);
        let lease = match entry.runtime() {
            MacosRuntimeBackend::Linux(runtime) => MachineExecutionLease::Linux(
                runtime
                    .inner()
                    .boot_or_inspect_shared_vm(&reservation.resource_id, ports, resources)
                    .await?,
            ),
            MacosRuntimeBackend::Native(runtime) => {
                MachineExecutionLease::Native(runtime.boot(&reservation.resource_id).await?)
            }
        };
        Ok(MachineRuntimeActivation { lease, entry })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine_runtime_registry::MachineRuntimeAdmission;
    use vz_runtime_contract::{EnvironmentId, MachineId, ProjectId};

    #[test]
    fn vm_name_is_owner_scoped_and_distinct_from_runtime_store() {
        let first = ResourceOwner {
            project_id: ProjectId::generate(),
            environment_id: EnvironmentId::generate(),
            machine_id: Some(MachineId::generate()),
        };
        let sibling = ResourceOwner {
            machine_id: Some(MachineId::generate()),
            ..first.clone()
        };
        let other_environment = ResourceOwner {
            environment_id: EnvironmentId::generate(),
            ..first.clone()
        };
        let vm = MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&first)
            .expect("valid exact Machine owner");
        for owner in [&sibling, &other_environment] {
            assert_ne!(
                vm.resource_id,
                MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(owner)
                    .expect("valid sibling owner")
                    .resource_id
            );
        }
        assert_ne!(
            vm.resource_id,
            MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&first)
                .expect("store owner")
                .resource_id
        );
        assert_eq!(vm.machine_id, first.machine_id);
        assert!(vm.resource_id.len() <= 64);
        let environment_owner = ResourceOwner {
            machine_id: None,
            ..first
        };
        assert!(
            MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&environment_owner).is_err()
        );
    }

    #[tokio::test]
    async fn foreign_vm_reservation_is_rejected_before_backend_acquisition() {
        let root = tempfile::tempdir().expect("private registry fixture");
        let registry = MachineRuntimeRegistry::new(
            root.path().canonicalize().expect("explicit physical root"),
        )
        .expect("registry");
        let owner = ResourceOwner {
            project_id: ProjectId::generate(),
            environment_id: EnvironmentId::generate(),
            machine_id: Some(MachineId::generate()),
        };
        let store = MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&owner)
            .expect("store reservation");
        let entry = registry
            .admit(
                &owner,
                &store,
                &format!("sha256:{}", "a".repeat(64)),
                MachineRuntimeAdmission::CreateOrOpen,
                |data| {
                    Ok(MacosRuntimeBackend::new(vz_oci_macos::Runtime::new(
                        vz_oci_macos::RuntimeConfig {
                            data_dir: data.to_path_buf(),
                            // No profile is intentional: reaching the managed
                            // backend would yield a different, profile error.
                            ..Default::default()
                        },
                    )))
                },
            )
            .expect("test-only store admission");
        let expected = MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&owner)
            .expect("VM reservation");
        let mut foreign = expected.clone();
        foreign.machine_id = Some(MachineId::generate());
        assert!(matches!(
            entry
                .boot_or_inspect_machine(&foreign, vec![], StackResourceHint::default())
                .await,
            Err(MachineRuntimeActivationError::Admission(
                MachineRuntimeRegistryError::Conflict(_)
            ))
        ));
        assert!(
            !entry
                .runtime()
                .linux()
                .expect("Linux runtime")
                .has_shared_vm(&expected.resource_id)
                .await
        );
    }
}
