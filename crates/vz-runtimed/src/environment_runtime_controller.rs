//! Serialized, all-sibling runtime admission for one persisted Environment.
//!
//! This trusted-library boundary prepares Machines, not a complete Developer Up.
//! Callers must authorize the full topology and resolve durable request replay
//! before calling it. No runtime is constructed until all sibling pins exist.
//! The retained Environment lease must also surround lifecycle begin, effects,
//! and acknowledgements. Stop/Delete must use the same controller lock registry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use vz_oci_macos::{MacosRuntimeBackend, Runtime, RuntimeConfig};
use vz_runtime_contract::{
    EnvironmentId, EnvironmentInstance, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentState, MachineErrorCode, MachineId, OwnershipRecord, ProjectId, ProjectState,
    ResourceOwner,
};
use vz_stack::{StackError, StateStore};

use crate::machine_artifact_store::{
    MachineArtifactStoreError, PinnedMachineArtifacts, load_machine_artifacts,
    pin_machine_artifacts_retaining_fence,
};
use crate::machine_runtime_registry::{
    MachineRuntimeAdmission, MachineRuntimeEntry, MachineRuntimeRegistry,
    MachineRuntimeRegistryError,
};
use crate::machine_target_resolver::{MachineTargetResolver, TargetResolutionError};

#[derive(Debug, Error)]
pub enum EnvironmentRuntimeControllerError {
    #[error(transparent)]
    State(#[from] StackError),
    #[error(transparent)]
    Registry(#[from] MachineRuntimeRegistryError),
    #[error(transparent)]
    Artifacts(#[from] MachineArtifactStoreError),
    #[error(transparent)]
    Resolution(#[from] TargetResolutionError),
}

fn conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

/// Bounded database access; no database mutex is held across an artifact await.
pub trait EnvironmentStateStore {
    fn access<T>(
        &self,
        operation: impl FnOnce(&StateStore) -> Result<T, StackError>,
    ) -> Result<T, StackError>;
}

impl EnvironmentStateStore for StateStore {
    fn access<T>(
        &self,
        operation: impl FnOnce(&StateStore) -> Result<T, StackError>,
    ) -> Result<T, StackError> {
        operation(self)
    }
}

impl EnvironmentStateStore for Mutex<StateStore> {
    fn access<T>(
        &self,
        operation: impl FnOnce(&StateStore) -> Result<T, StackError>,
    ) -> Result<T, StackError> {
        let store = self
            .lock()
            .map_err(|_| conflict("Environment state-store mutex poisoned"))?;
        operation(&store)
    }
}

/// One instance is owned by the daemon and shared by every topology operation.
#[derive(Default)]
pub struct EnvironmentRuntimeController {
    locks: Mutex<HashMap<EnvironmentId, Weak<AsyncMutex<()>>>>,
    identity: Arc<()>,
}

impl EnvironmentRuntimeController {
    pub(crate) fn require_own_lease(
        &self,
        lease: &EnvironmentControllerLease,
    ) -> Result<(), EnvironmentRuntimeControllerError> {
        if !Arc::ptr_eq(&self.identity, &lease.controller_identity) {
            return Err(conflict("lease belongs to a different Environment controller").into());
        }
        Ok(())
    }

    pub async fn acquire(
        &self,
        project_id: &ProjectId,
        environment_id: &EnvironmentId,
    ) -> Result<EnvironmentControllerLease, EnvironmentRuntimeControllerError> {
        // Environment IDs are globally unique. Do not key by project: a forged
        // project selector must not obtain a second lock for the same instance.
        ProjectId::new(project_id.to_string()).map_err(|e| conflict(e.to_string()))?;
        EnvironmentId::new(environment_id.to_string()).map_err(|e| conflict(e.to_string()))?;
        let lock = {
            let mut locks = self
                .locks
                .lock()
                .map_err(|_| conflict("Environment controller mutex poisoned"))?;
            locks.retain(|_, lock| lock.strong_count() > 0);
            let lock = locks
                .get(environment_id)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Arc::new(AsyncMutex::new(())));
            locks.insert(environment_id.clone(), Arc::downgrade(&lock));
            lock
        };
        Ok(EnvironmentControllerLease {
            project_id: project_id.clone(),
            environment_id: environment_id.clone(),
            controller_identity: Arc::clone(&self.identity),
            guard: Arc::new(lock.lock_owned().await),
        })
    }
}

/// A non-cloneable fence retained across admission, lifecycle and effects.
#[must_use = "retain this lease across the complete Environment operation"]
pub struct EnvironmentControllerLease {
    project_id: ProjectId,
    environment_id: EnvironmentId,
    controller_identity: Arc<()>,
    guard: Arc<OwnedMutexGuard<()>>,
}

#[cfg(test)]
#[path = "environment_runtime_controller_tests.rs"]
mod tests;

fn load_exact(
    store: &StateStore,
    expected: &EnvironmentInstance,
) -> Result<ProjectState, StackError> {
    let project = store
        .load_project_state_snapshot(expected.project_id.as_str())?
        .ok_or_else(|| conflict("admission project disappeared"))?;
    if project
        .environments
        .iter()
        .find(|environment| environment.environment_id == expected.environment_id)
        != Some(expected)
    {
        return Err(conflict("Environment changed during runtime admission"));
    }
    Ok(project)
}

fn reservations(
    owner: &ResourceOwner,
) -> Result<[OwnershipRecord; 2], MachineRuntimeRegistryError> {
    Ok([
        MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(owner)?,
        MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(owner)?,
    ])
}

impl EnvironmentControllerLease {
    /// Prepare all Machines under this lease. This never boots or publishes Ready.
    /// Fresh admission may complete partially prepared stores only while the
    /// durable never-started fence holds. Every later phase is read-only recovery.
    pub async fn prepare<S: EnvironmentStateStore>(
        self,
        state: &S,
        registry: &MachineRuntimeRegistry<MacosRuntimeBackend>,
        resolver: &MachineTargetResolver,
        expected: &EnvironmentInstance,
        now: u64,
    ) -> Result<PreparedEnvironmentMachines, EnvironmentRuntimeControllerError> {
        if self.project_id != expected.project_id || self.environment_id != expected.environment_id
        {
            return Err(conflict("controller lease belongs to another Environment").into());
        }
        let project = state.access(|store| load_exact(store, expected))?;
        let fresh = expected.state == EnvironmentState::Creating
            && expected.lifecycle_generation == 0
            && expected.active_operation_id.is_none();
        if fresh {
            state.access(|store| store.require_environment_admission_fence(expected))?;
        } else {
            state.access(|store| {
                if let Some(operation) =
                    store.load_current_environment_lifecycle(expected.environment_id.as_str())?
                    && operation.kind != EnvironmentLifecycleKind::Up
                {
                    return Err(conflict(
                        "another lifecycle operation owns this Environment",
                    ));
                }
                Ok(())
            })?;
        }
        // Resolve every sibling before creating even the first ownership row.
        // A recovered Environment never asks the resolver for artifact selection.
        let resolved = if fresh {
            Some(resolver.resolve_project(&project.definition).await?)
        } else {
            None
        };
        state.access(|store| {
            load_exact(store, expected)?;
            if fresh {
                store.require_environment_admission_fence(expected)?;
            }
            Ok(())
        })?;
        let owners = expected
            .machines
            .iter()
            .map(|machine| ResourceOwner {
                project_id: expected.project_id.clone(),
                environment_id: expected.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            })
            .collect::<Vec<_>>();
        let records = owners
            .iter()
            .map(reservations)
            .collect::<Result<Vec<_>, _>>()?;
        // Reserve every sibling before any filesystem store acquisition.
        let mut admitted = expected.clone();
        for pair in &records {
            for record in pair {
                admitted = state.access(|store| {
                    load_exact(store, &admitted)?;
                    if fresh {
                        store.require_environment_admission_fence(&admitted)?;
                        store.reserve_owned_resource(record, now)?;
                    } else {
                        store.require_owned_resource(record)?;
                    }
                    let current = store
                        .load_project_state_snapshot(admitted.project_id.as_str())?
                        .and_then(|project| {
                            project.environments.into_iter().find(|environment| {
                                environment.environment_id == admitted.environment_id
                            })
                        })
                        .ok_or_else(|| conflict("admission Environment disappeared"))?;
                    // Only our exact reservation and monotonic timestamp may change.
                    let mut wanted = admitted.clone();
                    if fresh && !wanted.ownership.contains(record) {
                        wanted.ownership.push(record.clone());
                        wanted.updated_at = wanted.updated_at.max(now);
                    }
                    wanted.ownership.sort_by_key(|record| {
                        (
                            format!("{:?}", record.resource_kind),
                            record.resource_id.clone(),
                        )
                    });
                    let mut ordered = current.clone();
                    ordered.ownership.sort_by_key(|record| {
                        (
                            format!("{:?}", record.resource_kind),
                            record.resource_id.clone(),
                        )
                    });
                    if wanted != ordered {
                        return Err(conflict(
                            "unexpected state change during Machine reservation",
                        ));
                    }
                    Ok(current)
                })?;
            }
        }
        let mut stores = Vec::new();
        for ((owner, pair), machine) in owners.iter().zip(&records).zip(&admitted.machines) {
            let target = resolved
                .as_ref()
                .and_then(|targets| targets.machines.get(&machine.name));
            if fresh && target.is_none() {
                return Err(conflict("resolved sibling is missing").into());
            }
            stores.push(registry.acquire_store(
                owner,
                &pair[0],
                target.map(|target| target.configuration_digest()),
                if fresh {
                    MachineRuntimeAdmission::CreateOrOpen
                } else {
                    MachineRuntimeAdmission::ExistingOnly
                },
            )?);
        }
        let mut pins = Vec::new();
        for (store, machine) in stores.iter().zip(&admitted.machines) {
            state.access(|store| {
                load_exact(store, &admitted)?;
                if fresh {
                    store.require_environment_admission_fence(&admitted)?;
                }
                Ok(())
            })?;
            let pin = if let Some(target) = resolved
                .as_ref()
                .and_then(|targets| targets.machines.get(&machine.name))
            {
                pin_machine_artifacts_retaining_fence(
                    Arc::clone(store),
                    target,
                    Arc::clone(&self.guard) as Arc<dyn Send + Sync>,
                )
                .await?
            } else {
                let spec = project
                    .definition
                    .environment
                    .machines
                    .iter()
                    .find(|spec| spec.name == machine.name)
                    .ok_or_else(|| conflict("persisted Machine specification is missing"))?;
                load_machine_artifacts(Arc::clone(store), resolver.host(), spec).await?
            };
            pins.push(pin);
        }
        state.access(|store| {
            load_exact(store, &admitted)?;
            if fresh {
                store.require_environment_admission_fence(&admitted)?;
            }
            for record in records.iter().flatten() {
                store.require_owned_resource(record)?;
            }
            Ok(())
        })?;
        Ok(PreparedEnvironmentMachines {
            environment: admitted,
            pins,
            _lease: self,
        })
    }
}

/// All sibling pins and the Environment fence, retained until lifecycle effects
/// and their durable acknowledgements finish. This is not a Ready certificate.
#[must_use = "retain prepared admission through effects and durable acknowledgements"]
pub struct PreparedEnvironmentMachines {
    environment: EnvironmentInstance,
    pins: Vec<PinnedMachineArtifacts>,
    _lease: EnvironmentControllerLease,
}

impl PreparedEnvironmentMachines {
    pub fn environment(&self) -> &EnvironmentInstance {
        &self.environment
    }
    pub fn pins(&self) -> &[PinnedMachineArtifacts] {
        &self.pins
    }

    /// Construct/attach only after all siblings are pinned and the exact Up
    /// generation plus BOTH store/VM reservations are current. No boot occurs.
    pub fn attach_machine<S: EnvironmentStateStore>(
        &self,
        state: &S,
        registry: &MachineRuntimeRegistry<MacosRuntimeBackend>,
        operation: &EnvironmentLifecycleOperation,
        machine_id: &MachineId,
    ) -> Result<Arc<MachineRuntimeEntry<MacosRuntimeBackend>>, EnvironmentRuntimeControllerError>
    {
        if operation.environment_id != self.environment.environment_id
            || operation.project_id != self.environment.project_id
            || operation.definition_digest != self.environment.definition_digest
            || operation.kind != EnvironmentLifecycleKind::Up
        {
            return Err(
                conflict("runtime attachment requires this Environment's Up operation").into(),
            );
        }
        let expected_generation = if let Some(active) = &self.environment.active_operation_id {
            if active != &operation.operation_id {
                return Err(conflict("prepared recovery belongs to a different operation").into());
            }
            Some(self.environment.lifecycle_generation)
        } else {
            self.environment.lifecycle_generation.checked_add(1)
        };
        if expected_generation != Some(operation.generation) {
            return Err(conflict(
                "prepared admission cannot authorize another lifecycle generation",
            )
            .into());
        }
        let pin = self
            .pins
            .iter()
            .find(|pin| pin.store().owner().machine_id.as_ref() == Some(machine_id))
            .ok_or_else(|| conflict("Machine is not part of prepared admission"))?;
        let step = operation
            .machine_steps
            .iter()
            .find(|step| &step.machine_id == machine_id)
            .ok_or_else(|| conflict("Machine is not part of the Up operation"))?;
        let records = reservations(pin.store().owner())?;
        state.access(|store| {
            store.require_current_machine_lifecycle_fence(operation, step, &records)
        })?;
        // No partial sibling pin can escape through this API. Also reject path
        // substitution of any sibling before constructing the first runtime.
        for sibling in &self.pins {
            sibling.validate_current()?;
        }
        let bundle = pin.runtime_bundle();
        let profile = pin.configuration().kernel_profile;
        let memory_mb = pin.configuration().resources.memory_mb;
        Ok(
            registry.attach_runtime(Arc::clone(pin.store()), move |data| {
                Ok(MacosRuntimeBackend::new(Runtime::new(RuntimeConfig {
                    data_dir: data.into(),
                    linux_install_dir: None,
                    linux_bundle_dir: None,
                    linux_profile: Some(profile),
                    pinned_linux_bundle: Some(bundle),
                    require_exact_agent_version: true,
                    agent_ready_timeout: Duration::from_secs(35),
                    exec_timeout: Duration::from_secs(30),
                    default_memory_mb: memory_mb,
                    ..RuntimeConfig::default()
                })))
            })?,
        )
    }
}
