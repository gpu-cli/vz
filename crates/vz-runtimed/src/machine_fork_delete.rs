//! Reclaiming exactly one forked Machine, with its Environment still serving.
//!
//! `vz delete --machine <machine>@<label>` is not a narrower Environment
//! Delete. An Environment Delete quiesces every Machine, reclaims the
//! Environment-scoped fabric and storage, tombstones the Environment identity
//! and removes the aggregate. None of that may happen here: the Environment
//! keeps running, its declared Machines keep serving, and its sibling forks
//! keep their warm state, from admission to the terminal journal.
//!
//! What it shares with the Environment path is the part that matters — every
//! effect is fenced on a *persisted* lifecycle operation, and the ownership it
//! releases is compared as an exact set rather than counted. The operation is
//! Machine-scoped ([`vz_runtime_contract::EnvironmentLifecycleOperation::plan_machine_delete`]):
//! one Machine step, that Machine's ownership records and no others, and a
//! begin that takes the next lifecycle generation without taking the
//! Environment's `active_operation_id`.
//!
//! The order of effects is the Environment path's order, restricted to one
//! Machine: quiesce it, retire it, acknowledge its step, remove its host Docker
//! context, remove its runtime store — which is where its Docker data disk
//! lives — and only then finish the journal and remove its rows, in one
//! transaction. Anything that cannot be reclaimed fails the operation with its
//! rows intact, because a fork whose store or context survived while its
//! ownership rows said otherwise is exactly the unaccounted state this design
//! exists to prevent.
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use vz_runtime_contract::{
    Architecture, EnvironmentInstance, EnvironmentLifecycleOperation, EnvironmentLifecycleStatus,
    LifecycleStepResult, LifecycleStepStatus, MachineError, MachineErrorCode, MachineForkAddress,
    MachineId, MachineInstance, MachineLifecycleStepAcknowledgement, OperatingSystem,
    OwnedResourceKind, OwnershipCleanupStepAcknowledgement, OwnershipRecord, ResourceOwner,
};

use crate::RuntimeDaemon;
use crate::environment_delete::{
    DeleteEnvironmentInput, PreparedDeleteMachine, Progress, conflict, docker_config_dir, failure,
    progress, request_hash, validate_input,
};
use crate::environment_runtime_controller::EnvironmentControllerLease;
use crate::machine_docker_config::ManagedMachineDockerConfig;
use crate::machine_docker_context::ManagedMachineDockerContext;
use crate::machine_docker_endpoint::MachineDockerEndpoint;
use crate::machine_runtime_registry::{MachineRuntimeEntry, MachineRuntimeRegistry};

impl RuntimeDaemon {
    /// Reclaim one forked Machine named `<machine>@<label>`.
    ///
    /// The three answers this can give are deliberately distinct, because
    /// `--machine` is *resolved* before it is acted on: a Machine the
    /// definition declares is refused as unsupported, a label no Machine
    /// answers to is a `NotFound`, and a fork is reclaimed.
    pub(crate) async fn delete_machine_fork(
        self: &Arc<Self>,
        input: DeleteEnvironmentInput,
        selector: String,
    ) -> Result<watch::Receiver<Progress>, MachineError> {
        validate_input(&input)?;
        let address = MachineForkAddress::parse(&selector).map_err(|error| {
            failure(&input, MachineErrorCode::ValidationError, error.to_string())
        })?;
        if address.label.is_none() {
            return Err(failure(
                &input,
                MachineErrorCode::UnsupportedOperation,
                format!(
                    "`{selector}` names a Machine the project definition declares; only a fork `<machine>@<label>` can be deleted on its own"
                ),
            ));
        }
        // Immutable replay resolves before the selector does: a completed
        // reclamation must answer from its journal even though the Machine it
        // names is gone and could no longer be resolved at all.
        if let Some(terminal) = self.completed_machine_delete(&input)? {
            return Ok(terminal);
        }
        let environment = self.selected_delete(&input)?;
        let machine = self.resolve_fork(&input, &environment, &address, &selector)?;
        self.authorize_machine_delete(&input, &environment, &machine.machine_id)?;
        let lease = tokio::time::timeout(
            Duration::from_secs(30),
            self.acquire_environment_controller(&input.project_id, &environment.environment_id),
        )
        .await
        .map_err(|_| {
            failure(
                &input,
                MachineErrorCode::Timeout,
                "Delete admission exceeded 30 seconds; no new operation admitted",
            )
        })?
        .map_err(|error| conflict(&input, error))?;
        if let Some(terminal) = self.completed_machine_delete(&input)? {
            return Ok(terminal);
        }
        // Re-resolved under the retained controller: the selector, the
        // Environment and the fork's own identity must all still be what
        // admission decided, or nothing is admitted.
        let settled = self.selected_delete(&input)?;
        if settled != environment {
            return Err(conflict(
                &input,
                "Delete selection changed during admission",
            ));
        }
        let machine = self.resolve_fork(&input, &settled, &address, &selector)?;
        self.authorize_machine_delete(&input, &settled, &machine.machine_id)?;
        let expected = validate_fork_supported(&input, &settled, &machine)?;
        let prepared = self.prepare_fork_delete(&input, &lease, &settled, &machine)?;
        let hash = request_hash(&input, &settled.environment_id)?;
        let operation = self
            .with_state_store(|store| {
                store.begin_machine_lifecycle_delete(
                    settled.environment_id.as_str(),
                    &machine.machine_id,
                    input.metadata.request_id.as_deref().unwrap_or_default(),
                    input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default(),
                    &hash,
                    crate::current_unix_secs(),
                )
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?;
        // The plan the store persisted must be the exact set this admission
        // proved it could release. A plan that acquired or lost a record
        // between admission and begin is refused before any effect.
        let planned = operation
            .cleanup_steps
            .iter()
            .map(|step| &step.ownership)
            .collect::<BTreeSet<_>>();
        if planned.len() != operation.cleanup_steps.len()
            || planned != expected.iter().collect::<BTreeSet<_>>()
        {
            return Err(conflict(
                &input,
                "Delete ownership plan changed between admission and begin; no effects admitted",
            ));
        }
        let (sender, receiver) = watch::channel(progress(&input, &operation, 0, None));
        let daemon = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = daemon
                .drive_machine_delete(&input, &lease, operation, prepared, &sender)
                .await
            {
                // The durable operation remains authoritative. No reclamation is
                // fabricated after an uncertain effect.
                let _ = sender.send_replace(Err(error));
            }
        });
        Ok(receiver)
    }

    /// Resolve `<machine>@<label>` to a fork, or say precisely why it is not one.
    fn resolve_fork(
        &self,
        input: &DeleteEnvironmentInput,
        environment: &EnvironmentInstance,
        address: &MachineForkAddress,
        selector: &str,
    ) -> Result<MachineInstance, MachineError> {
        let Some(machine) = environment.machine_by_address(address) else {
            return Err(failure(
                input,
                MachineErrorCode::NotFound,
                format!(
                    "no Machine `{selector}` in Environment `{}`; `vz status` lists forks with their labels",
                    environment.environment_id
                ),
            ));
        };
        if machine.fork.is_none() {
            return Err(failure(
                input,
                MachineErrorCode::UnsupportedOperation,
                format!(
                    "Machine `{}` is declared by the project definition; only a fork can be deleted on its own",
                    machine.name
                ),
            ));
        }
        Ok(machine.clone())
    }

    fn authorize_machine_delete(
        &self,
        input: &DeleteEnvironmentInput,
        environment: &EnvironmentInstance,
        machine_id: &MachineId,
    ) -> Result<(), MachineError> {
        self.authorize_delete_scope(
            input,
            environment.project_id.clone(),
            environment.environment_id.clone(),
            vec![machine_id.clone()],
            environment.definition_digest.clone(),
        )
    }

    /// A completed Machine-scoped Delete replays from its terminal journal.
    ///
    /// There is no tombstone to load: a tombstone retires an Environment
    /// identity, and this Environment was never retired. The journal is the
    /// receipt, and it is terminal only because `finish_machine_delete`
    /// committed it in the same transaction that removed the fork's rows.
    fn completed_machine_delete(
        &self,
        input: &DeleteEnvironmentInput,
    ) -> Result<Option<watch::Receiver<Progress>>, MachineError> {
        let Some(operation) = self
            .with_state_store(|store| {
                store.load_environment_lifecycle_by_idempotency_key(
                    input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default(),
                )
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?
        else {
            return Ok(None);
        };
        let Some(machine_id) = operation.machine_scope.clone() else {
            return Err(conflict(
                input,
                "idempotency key belongs to an Environment-wide Delete request",
            ));
        };
        if operation.project_id != input.project_id
            || operation.request_id != input.metadata.request_id.as_deref().unwrap_or_default()
            || operation.request_hash != request_hash(input, &operation.environment_id)?
        {
            return Err(conflict(
                input,
                "idempotency key belongs to a different immutable Delete request",
            ));
        }
        if !matches!(
            operation.status,
            EnvironmentLifecycleStatus::Succeeded | EnvironmentLifecycleStatus::Blocked
        ) {
            return Ok(None);
        }
        self.authorize_machine_delete(input, &self.selected_delete(input)?, &machine_id)?;
        let (_, receiver) = watch::channel(progress(input, &operation, 0, None));
        Ok(Some(receiver))
    }

    /// Prepare every physical effect before a single one is admitted.
    fn prepare_fork_delete(
        &self,
        input: &DeleteEnvironmentInput,
        lease: &EnvironmentControllerLease,
        environment: &EnvironmentInstance,
        machine: &MachineInstance,
    ) -> Result<PreparedDeleteMachine, MachineError> {
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        };
        let reservation =
            MachineRuntimeRegistry::<crate::machine_backend::MachineBackendRuntime>::reservation(
                &owner,
            )
            .map_err(|e| conflict(input, e))?;
        self.with_state_store(|store| store.require_owned_resource(&reservation))
            .map_err(|e| e.to_machine_error(&input.metadata))?;
        let store = self
            .machine_runtime_registry()
            .preflight_delete(&owner, &reservation)
            .map_err(|e| conflict(input, e))?;
        if let Some(id) = store.delete_operation_id() {
            // A store already bound to a Delete intent belongs to that
            // operation. Only its own replay may continue it.
            let current = self
                .with_state_store(|state| state.load_environment_lifecycle(id.as_str()))
                .map_err(|e| e.to_machine_error(&input.metadata))?;
            if current.as_ref().is_none_or(|operation| {
                operation.idempotency_key
                    != input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default()
            }) {
                return Err(conflict(
                    input,
                    "Machine store belongs to a different Delete intent",
                ));
            }
        }
        let context = if let Some(store_lease) = store
            .lease()
            .filter(|_| machine.target.os == OperatingSystem::Linux)
        {
            let socket =
                MachineDockerEndpoint::socket_path_for(&self.config.runtime_data_dir, &owner)
                    .map_err(|e| conflict(input, e))?;
            let config_dir =
                match ManagedMachineDockerConfig::open_existing(Arc::clone(store_lease))
                    .map_err(|e| conflict(input, e))?
                {
                    Some(config) => config.path().to_path_buf(),
                    None => docker_config_dir(input)?,
                };
            ManagedMachineDockerContext::prepare_existing_delete(
                Arc::clone(store_lease),
                machine.docker_context.as_ref(),
                &config_dir,
                &socket,
            )
            .map_err(|e| conflict(input, e))?
        } else {
            None
        };
        let absent = if store.quiescence_evidence().is_some() {
            None
        } else {
            self.machine_live_sessions()
                .prepare_delete_absence(lease, &self.state_store, environment, &machine.machine_id)
                .map_err(|e| conflict(input, e))?
        };
        Ok(PreparedDeleteMachine {
            id: machine.machine_id.clone(),
            store,
            context,
            absent,
            quiescence: None,
        })
    }

    /// Quiesce, retire, reclaim and finish — for one Machine, in that order.
    async fn drive_machine_delete(
        self: &Arc<Self>,
        input: &DeleteEnvironmentInput,
        lease: &EnvironmentControllerLease,
        mut operation: EnvironmentLifecycleOperation,
        mut machine: PreparedDeleteMachine,
        sender: &watch::Sender<Progress>,
    ) -> Result<(), MachineError> {
        let mut sequence = 0;
        let environment_id = operation.environment_id.clone();
        self.authorize_machine_delete(input, &self.selected_delete(input)?, &machine.id)?;
        let step = operation
            .machine_steps
            .iter()
            .find(|step| step.machine_id == machine.id)
            .ok_or_else(|| conflict(input, "Delete Machine step missing"))?
            .clone();
        if step.status != LifecycleStepStatus::Succeeded {
            if step.status != LifecycleStepStatus::Pending
                && step.status != LifecycleStepStatus::Running
            {
                return Err(conflict(
                    input,
                    "failed Delete Machine step needs explicit reconciliation",
                ));
            }
            if machine.absent.is_none() {
                self.machine_live_sessions()
                    .stop_for_delete(
                        lease,
                        &self.state_store,
                        &operation,
                        &machine.id,
                        input.machine_timeout,
                    )
                    .await
                    .map_err(|e| conflict(input, e))?;
            }
        }
        machine.quiescence = Some(
            self.machine_live_sessions()
                .retire_for_delete(
                    lease,
                    &self.state_store,
                    &operation,
                    &machine.id,
                    &machine.store,
                    machine.absent.take(),
                )
                .map_err(|e| conflict(input, e))?,
        );
        if step.status != LifecycleStepStatus::Succeeded {
            operation = self
                .with_state_store(|store| {
                    store.acknowledge_environment_machine_step(
                        &MachineLifecycleStepAcknowledgement {
                            operation_id: operation.operation_id.clone(),
                            generation: operation.generation,
                            machine_id: step.machine_id.clone(),
                            initial_state: step.initial_state,
                            target_state: step.target_state,
                            expected_incarnation: step.expected_incarnation.clone(),
                            resulting_incarnation: None,
                            resulting_activation: None,
                            result: LifecycleStepResult::Succeeded,
                        },
                        crate::current_unix_secs(),
                    )
                })
                .map_err(|e| e.to_machine_error(&input.metadata))?;
        }
        sequence += 1;
        let _ = sender.send_replace(progress(input, &operation, sequence, None));
        // The host Docker context names a socket this Machine no longer serves,
        // so it is removed only after the Machine is positively quiet.
        if let Some(mut context) = machine.context.take() {
            context
                .remove_exact(&operation)
                .map_err(|e| conflict(input, e))?;
        }
        let quiescence = machine
            .quiescence
            .ok_or_else(|| conflict(input, "Delete omitted positive quiescence"))?;
        let daemon = Arc::clone(self);
        let exact_operation = operation.clone();
        let machine_id = machine.id.clone();
        // The blocking walker owns its token/fence until completion. Never
        // timeout/drop it on client observation loss or strand its effects.
        let receipt = tokio::task::spawn_blocking(move || {
            daemon
                .machine_runtime_registry()
                .begin_delete(machine.store, &exact_operation, quiescence)?
                .remove()
        })
        .await
        .map_err(|e| conflict(input, e))?
        .map_err(|e| conflict(input, e))?;
        if !receipt.store_removed
            || receipt.owner.machine_id.as_ref() != Some(&machine_id)
            || receipt.operation_id != operation.operation_id
            || receipt.generation != operation.generation
        {
            return Err(conflict(
                input,
                "Machine store deletion receipt changed authority",
            ));
        }
        for cleanup in operation
            .cleanup_steps
            .clone()
            .into_iter()
            .filter(|step| step.status != LifecycleStepStatus::Succeeded)
        {
            operation = self
                .with_state_store(|store| {
                    store.acknowledge_environment_cleanup_step(
                        &OwnershipCleanupStepAcknowledgement {
                            operation_id: operation.operation_id.clone(),
                            generation: operation.generation,
                            ownership: cleanup.ownership,
                            result: LifecycleStepResult::Succeeded,
                        },
                        crate::current_unix_secs(),
                    )
                })
                .map_err(|e| e.to_machine_error(&input.metadata))?;
        }
        sequence += 1;
        let _ = sender.send_replace(progress(input, &operation, sequence, None));
        let (finished, remaining) = self
            .with_state_store(|store| {
                store.finish_machine_delete(
                    operation.operation_id.as_str(),
                    operation.generation,
                    crate::current_unix_secs(),
                )
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?;
        if remaining
            .machines
            .iter()
            .any(|row| row.machine_id == machine_id)
            || remaining.environment_id != environment_id
        {
            return Err(conflict(
                input,
                "Machine-scoped Delete finished with its Machine still in the Environment",
            ));
        }
        let _ = sender.send_replace(progress(input, &finished, sequence + 1, None));
        Ok(())
    }
}

/// The exact ownership one fork may hold, minted from the persisted instances.
///
/// This is the Machine-scoped half of `validate_supported`, and it is exact in
/// the same way: the expected set is derived from the aggregate's own instances
/// and compared as a `BTreeSet` against what the ownership graph attributes to
/// this Machine, with a repeated identity refused rather than deduplicated.
///
/// It is also narrower, and deliberately so. A fork mints no endpoint, no host
/// export and no host import — those names and ports are Environment-unique —
/// and a volume is Environment-scoped and belongs to no Machine at all.
/// Reclaiming any of them means reclaiming something the Environment is still
/// using, so a fork that somehow held one is refused rather than half-reclaimed.
fn validate_fork_supported(
    input: &DeleteEnvironmentInput,
    environment: &EnvironmentInstance,
    machine: &MachineInstance,
) -> Result<Vec<OwnershipRecord>, MachineError> {
    if environment.legacy_migration.is_some()
        || !matches!(
            machine.target.os,
            OperatingSystem::Linux | OperatingSystem::Macos
        )
        || machine.target.arch != Architecture::Aarch64
    {
        return Err(failure(
            input,
            MachineErrorCode::UnsupportedOperation,
            "Delete supports registered Linux/native macOS ARM64 Machines; additional topology resource adapters remain unsupported",
        ));
    }
    let machine_id = &machine.machine_id;
    for (kind, held) in [
        (
            "endpoint",
            environment
                .endpoints
                .iter()
                .any(|row| &row.machine_id == machine_id),
        ),
        (
            "host export",
            environment
                .host_exports
                .iter()
                .any(|row| &row.machine_id == machine_id),
        ),
        (
            "host import",
            environment
                .host_imports
                .iter()
                .any(|row| &row.machine_id == machine_id),
        ),
    ] {
        if held {
            return Err(failure(
                input,
                MachineErrorCode::UnsupportedOperation,
                format!(
                    "fork `{}` holds an Environment-unique {kind}; reclaiming it on its own would take a resource the Environment still publishes",
                    machine.name
                ),
            ));
        }
    }
    let owner = ResourceOwner {
        project_id: environment.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        machine_id: Some(machine_id.clone()),
    };
    let record = |kind: OwnedResourceKind, resource_id: String| OwnershipRecord {
        schema_version: 1,
        resource_kind: kind,
        resource_id,
        environment_id: environment.environment_id.clone(),
        machine_id: Some(machine_id.clone()),
    };
    let mut expected = vec![
        record(OwnedResourceKind::Machine, machine_id.to_string()),
        // The record that says this Machine's disk was seeded from a sibling
        // rather than provisioned. The cloned tree it names is inside this
        // Machine's runtime store, which this reclamation removes.
        record(OwnedResourceKind::MachineFork, machine_id.to_string()),
        MachineRuntimeRegistry::<crate::machine_backend::MachineBackendRuntime>::reservation(
            &owner,
        )
        .map_err(|e| conflict(input, e))?,
        MachineRuntimeEntry::<crate::machine_backend::MachineBackendRuntime>::vm_reservation(
            &owner,
        )
        .map_err(|e| conflict(input, e))?,
    ];
    if let Some(incarnation) = &machine.incarnation {
        expected.push(record(
            OwnedResourceKind::Incarnation,
            incarnation.incarnation_id.to_string(),
        ));
    }
    if let Some(context) = &machine.docker_context {
        context.validate().map_err(|e| conflict(input, e))?;
        if context.owner != owner {
            return Err(conflict(input, "Delete context is foreign"));
        }
        expected.push(record(
            OwnedResourceKind::DockerContext,
            context.name.clone(),
        ));
    }
    for attachment in environment
        .network_attachments
        .iter()
        .filter(|row| &row.machine_id == machine_id)
    {
        if !environment
            .networks
            .iter()
            .any(|network| network.network_id == attachment.network_id)
        {
            return Err(conflict(
                input,
                "Delete network attachment references absent topology",
            ));
        }
        expected.push(record(
            OwnedResourceKind::NetworkAttachment,
            attachment.attachment_id.to_string(),
        ));
    }
    let expected_set = expected.iter().collect::<BTreeSet<_>>();
    if expected_set.len() != expected.len() {
        return Err(conflict(
            input,
            "Delete ownership plan minted one resource identity twice; no effects admitted",
        ));
    }
    let held = environment
        .ownership
        .iter()
        .filter(|row| row.machine_id.as_ref() == Some(machine_id))
        .collect::<Vec<_>>();
    let held_set = held.iter().copied().collect::<BTreeSet<_>>();
    if held_set.len() != held.len() || held_set != expected_set {
        return Err(failure(
            input,
            MachineErrorCode::UnsupportedOperation,
            "Delete ownership graph contains missing, unknown, unsupported, or repeated physical resources; no effects admitted",
        ));
    }
    Ok(expected)
}
