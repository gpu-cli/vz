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

use crate::machine_backend::MachineBackendRuntime as MacosRuntimeBackend;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use vz_oci_macos::{Runtime, RuntimeConfig};
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
    MachineRuntimeRegistryError, MachineRuntimeStoreLease,
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
    #[error("native macOS preparation: {0}")]
    Native(#[from] anyhow::Error),
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
    pub(crate) fn require_owner(&self, owner: &ResourceOwner) -> Result<(), StackError> {
        if self.project_id != owner.project_id || self.environment_id != owner.environment_id {
            return Err(conflict(
                "Environment controller lease belongs to another owner",
            ));
        }
        Ok(())
    }

    pub(crate) fn retained_guard(&self) -> Arc<OwnedMutexGuard<()>> {
        Arc::clone(&self.guard)
    }

    pub(crate) fn controller_identity(&self) -> &Arc<()> {
        &self.controller_identity
    }

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
        self.prepare_with_progress(state, registry, resolver, expected, now, |_| Ok(()))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_with_progress<S: EnvironmentStateStore>(
        self,
        state: &S,
        registry: &MachineRuntimeRegistry<MacosRuntimeBackend>,
        resolver: &MachineTargetResolver,
        expected: &EnvironmentInstance,
        now: u64,
        mut progress: impl FnMut(vz_macos_provision::bootstrap::Progress) -> anyhow::Result<()>,
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
                    // Reserving is per Machine, not per Environment. A fork is a
                    // never-started Machine inside an Environment that has
                    // already run, so it needs its two runtime reservations
                    // minted here; `fresh` alone would send it to
                    // `require_owned_resource` and refuse its first Up with
                    // `not_found` on a row nothing had ever created.
                    //
                    // Order matters: a record already in the aggregate is
                    // REQUIRED, never re-minted, so a replayed fork Up verifies
                    // the reservations it took the first time rather than
                    // minting a second set.
                    let mint_for_fork = !fresh
                        && !admitted.ownership.contains(record)
                        && record.machine_id.as_ref().is_some_and(|machine_id| {
                            admitted.machines.iter().any(|machine| {
                                machine.machine_id == *machine_id && machine.fork.is_some()
                            })
                        });
                    if fresh {
                        store.require_environment_admission_fence(&admitted)?;
                        store.reserve_owned_resource(record, now)?;
                    } else if mint_for_fork {
                        let machine_id = record
                            .machine_id
                            .as_ref()
                            .ok_or_else(|| conflict("fork reservation carries no Machine"))?;
                        store.require_machine_admission_fence(&admitted, machine_id)?;
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
                    // Only our exact reservation and monotonic timestamp may
                    // change. The expected snapshot has to account for whichever
                    // branch above actually minted, not just the fresh one, or a
                    // fork's own reservation reads as somebody else's change.
                    let mut wanted = admitted.clone();
                    if (fresh || mint_for_fork) && !wanted.ownership.contains(record) {
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
        // A never-started fork is the one Machine here whose runtime store does
        // not exist yet inside an Environment that is not fresh, so it is the
        // one that must be CREATED rather than opened. Its configuration digest
        // is its PARENT's, and not by convenience: the fork is seeded from that
        // Machine's disk and carries the same target, profile, resources and
        // artifact identities, which is exactly what the digest binds. Target
        // resolution cannot supply it -- it reads the project definition, which
        // never contains a fork, and it is skipped entirely when not fresh.
        //
        // Two passes rather than one, indexed so `stores` still lines up with
        // `admitted.machines`: a fork's parent must be open before the fork can
        // read its digest, and relying on the aggregate's Machine order to
        // arrange that would be a silent assumption.
        let new_fork = |machine: &vz_runtime_contract::MachineInstance| {
            machine.fork.is_some()
                && machine.incarnation.is_none()
                && machine.runtime_identity.is_none()
        };
        let mut opened: Vec<Option<Arc<MachineRuntimeStoreLease>>> =
            (0..admitted.machines.len()).map(|_| None).collect();
        for pass_forks in [false, true] {
            for (index, ((owner, pair), machine)) in owners
                .iter()
                .zip(&records)
                .zip(&admitted.machines)
                .enumerate()
            {
                if new_fork(machine) != pass_forks {
                    continue;
                }
                let target = resolved
                    .as_ref()
                    .and_then(|targets| targets.machines.get(&machine.name));
                let native = resolved
                    .as_ref()
                    .and_then(|targets| targets.native.get(&machine.name));
                if fresh && target.is_none() && native.is_none() {
                    return Err(conflict("resolved sibling is missing").into());
                }
                let inherited = if pass_forks {
                    let origin = machine
                        .fork
                        .as_ref()
                        .ok_or_else(|| conflict("fork lost its lineage during admission"))?;
                    let parent = admitted
                        .machines
                        .iter()
                        .position(|candidate| candidate.machine_id == origin.parent_machine_id)
                        .and_then(|at| opened[at].as_ref())
                        .ok_or_else(|| {
                            conflict("a fork's parent has no open runtime store to inherit from")
                        })?;
                    Some(parent.configuration_digest().to_string())
                } else {
                    None
                };
                let digest = inherited.as_deref().or_else(|| {
                    target
                        .map(|target| target.configuration_digest())
                        .or_else(|| native.map(|target| target.configuration_digest.as_str()))
                });
                opened[index] = Some(registry.acquire_store(
                    owner,
                    &pair[0],
                    digest,
                    if fresh || pass_forks {
                        MachineRuntimeAdmission::CreateOrOpen
                    } else {
                        MachineRuntimeAdmission::ExistingOnly
                    },
                )?);
            }
        }
        let stores = opened
            .into_iter()
            .map(|store| store.ok_or_else(|| conflict("a Machine was never admitted a store")))
            .collect::<Result<Vec<_>, _>>()?;
        // A fork has no declaration to find: it is absent from `vz.json` by
        // definition, and its name is `<parent>@<label>`, which no spec carries.
        // Its artifacts are its PARENT's -- it was seeded from that Machine's
        // disk and shares its target, profile and resources -- so the spec to
        // load is looked up under the parent's name, reached through the
        // lineage rather than by trimming the label off a string.
        let declared_name = |machine: &vz_runtime_contract::MachineInstance| {
            let Some(origin) = machine.fork.as_ref() else {
                return Ok(machine.name.clone());
            };
            admitted
                .machines
                .iter()
                .find(|candidate| candidate.machine_id == origin.parent_machine_id)
                .map(|parent| parent.name.clone())
                .ok_or_else(|| conflict("a fork's parent is absent from its own Environment"))
        };
        // Same two passes, and for the same reason, as the store acquisition
        // above: a fork inherits its parent's pin, so the parent must be pinned
        // first, and the aggregate's Machine order is not a guarantee of that.
        // `pins` cannot be indexed by Machine position either -- native macOS
        // Machines land in `native_pins` instead -- so a fork finds its parent
        // through an explicit index rather than by counting.
        let mut pins: Vec<PinnedMachineArtifacts> = Vec::new();
        let mut native_pins = Vec::new();
        let mut pinned_at: HashMap<MachineId, usize> = HashMap::new();
        for pass_forks in [false, true] {
            for (store, machine) in stores.iter().zip(&admitted.machines) {
                if new_fork(machine) != pass_forks {
                    continue;
                }
                state.access(|store| {
                    load_exact(store, &admitted)?;
                    if fresh {
                        store.require_environment_admission_fence(&admitted)?;
                    }
                    Ok(())
                })?;
                if machine.target.os == vz_runtime_contract::OperatingSystem::Macos {
                    let native = if let Some(target) = resolved
                        .as_ref()
                        .and_then(|targets| targets.native.get(&machine.name))
                    {
                        crate::native_macos::artifacts::prepare(
                            Arc::clone(store),
                            target.configuration.clone(),
                            target.installed_bundle.as_deref(),
                            registry.native_bootstrap_cache_path(),
                            &mut progress,
                        )
                        .await?
                    } else {
                        let wanted = declared_name(machine)?;
                        let spec = project
                            .definition
                            .environment
                            .machines
                            .iter()
                            .find(|s| s.name == wanted)
                            .ok_or_else(|| conflict("missing native Machine specification"))?;
                        crate::native_macos::artifacts::load(
                            Arc::clone(store),
                            resolver.host(),
                            spec,
                        )?
                    };
                    native_pins.push(native);
                    continue;
                }
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
                } else if new_fork(machine) {
                    // A fork's store was created moments ago and holds no pin.
                    // Recovery refuses to create one and re-resolving could hand the
                    // fork a different kernel than the disk it inherited was built
                    // against, so it takes its parent's pinned configuration and
                    // verified bundle. Forks are appended after their parents in
                    // this loop for the same reason their stores are opened in two
                    // passes, so the parent's pin is already in `pins`.
                    let origin = machine
                        .fork
                        .as_ref()
                        .ok_or_else(|| conflict("fork lost its lineage during admission"))?;
                    let parent = pinned_at
                        .get(&origin.parent_machine_id)
                        .and_then(|at| pins.get(*at))
                        .ok_or_else(|| {
                            conflict("a fork's parent has no pinned artifacts to inherit")
                        })?;
                    crate::machine_artifact_store::inherit_machine_artifacts(
                        Arc::clone(store),
                        parent,
                        Arc::clone(&self.guard) as Arc<dyn Send + Sync>,
                    )
                    .await?
                } else {
                    let wanted = declared_name(machine)?;
                    let spec = project
                        .definition
                        .environment
                        .machines
                        .iter()
                        .find(|spec| spec.name == wanted)
                        .ok_or_else(|| conflict("persisted Machine specification is missing"))?;
                    load_machine_artifacts(Arc::clone(store), resolver.host(), spec).await?
                };
                pinned_at.insert(machine.machine_id.clone(), pins.len());
                pins.push(pin);
            }
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
            native_pins,
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
    native_pins: Vec<crate::native_macos::artifacts::NativePin>,
    _lease: EnvironmentControllerLease,
}

impl PreparedEnvironmentMachines {
    pub fn native_pins(&self) -> &[crate::native_macos::artifacts::NativePin] {
        &self.native_pins
    }
    pub(crate) fn lease(&self) -> &EnvironmentControllerLease {
        &self._lease
    }
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
        let linux_pin = self
            .pins
            .iter()
            .find(|pin| pin.store().owner().machine_id.as_ref() == Some(machine_id));
        let native_pin = self
            .native_pins
            .iter()
            .find(|pin| pin.store().owner().machine_id.as_ref() == Some(machine_id));
        let owner_store = linux_pin
            .map(|p| p.store())
            .or_else(|| native_pin.map(|p| p.store()))
            .ok_or_else(|| conflict("Machine is not part of prepared admission"))?;
        let step = operation
            .machine_steps
            .iter()
            .find(|step| &step.machine_id == machine_id)
            .ok_or_else(|| conflict("Machine is not part of the Up operation"))?;
        let records = reservations(owner_store.owner())?;
        state.access(|store| {
            store.require_current_machine_lifecycle_fence(operation, step, &records)
        })?;
        // No partial sibling pin can escape through this API. Also reject path
        // substitution of any sibling before constructing the first runtime.
        for sibling in &self.pins {
            sibling.validate_current()?;
        }
        for sibling in &self.native_pins {
            sibling.validate_current()?;
        }
        // The runtime SHAPE comes from the durable Machine record, not from the
        // pin. A pin is artifact storage: it says which kernel, initramfs and
        // youki this Machine boots, and it is deliberately immutable once
        // published. How many CPUs and how much memory the VM gets is declared
        // state that a definition reconcile updates in place (criterion 22),
        // so reading it from the pin would have pinned it too -- a Machine
        // would keep booting at the size it was first created with no matter
        // what `vz.json` said afterwards. Normalized through the resolver's
        // single defaults-and-bounds function so both readers agree.
        let declared = self
            .environment
            .machines
            .iter()
            .find(|machine| &machine.machine_id == machine_id)
            .ok_or_else(|| conflict("Machine is not part of this Environment"))?;
        if let Some(pin) = native_pin {
            let shape = crate::machine_target_resolver::resolve_native_machine_resources(
                &declared.resources,
            )
            .map_err(conflict)?;
            let runtime = crate::native_macos::runtime::NativeMacosRuntime::new(
                pin.directory(),
                shape.cpus,
                shape.memory_mb,
            );
            return Ok(registry.attach_runtime(Arc::clone(pin.store()), |_| {
                Ok(MacosRuntimeBackend::Native(Arc::new(runtime)))
            })?);
        }
        let pin = linux_pin.ok_or_else(|| conflict("missing Linux pin"))?;
        let bundle = pin.runtime_bundle();
        let profile = pin.configuration().kernel_profile;
        let memory_mb = crate::machine_target_resolver::resolve_machine_resources(
            &declared.resources,
            declared.profile,
        )
        .map_err(conflict)?
        .memory_mb;
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
