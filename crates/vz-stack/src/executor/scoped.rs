use super::create::PreparedCreate;
use super::dispatch::compute_topo_levels;
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::state_store::{
    StackContainerCreateIntent, StackContainerCreateSelector, StackContainerCreateStatus,
    StackContainerGenerationBinding, StackContainerRecoveryDisposition,
};
use vz_runtime_contract::{
    ContainerCreateReceipt, ContainerGenerationInspection, ContainerGenerationOwnership,
};

struct ScopedActivation {
    target: ServiceReplicaKey,
    intent: StackContainerCreateIntent,
    ownership: ContainerGenerationOwnership,
    image: String,
    config: vz_runtime_contract::RunConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedBatchManifest {
    schema_version: u32,
    scope: vz_runtime_contract::MachineWorkloadScope,
    session_id: String,
    operation_id: String,
    first_action_index: usize,
    actions_hash: String,
    spec: StackSpec,
    actions: Vec<ScopedManifestAction>,
    secret_inputs: BTreeMap<String, ScopedSecretInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedManifestAction {
    schema_version: u32,
    kind: String,
    target: ServiceReplicaKey,
    precondition: crate::reconcile::ReplicaPrecondition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopedSecretInput {
    sha256: String,
    file_name: String,
}

type LoadedSecretInputs = (BTreeMap<String, Vec<u8>>, BTreeMap<String, String>);

impl<R: ContainerRuntime> StackExecutor<R> {
    pub(super) fn prepare_scoped_batch_manifest(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<(), StackError> {
        let authority = self
            .scoped_authority
            .as_ref()
            .ok_or_else(|| scope_state_conflict("scoped manifest requires authority"))?;
        let manifest_actions = actions
            .iter()
            .map(ScopedManifestAction::from_action)
            .collect::<Vec<_>>();
        let owner_dir = scoped_manifest_owner_dir(&self.data_dir, &authority.scope, operation_id);
        let manifest_path = owner_dir.join("manifest.json");
        if manifest_path.exists() {
            validate_private_directory(&owner_dir)?;
            validate_private_file(&manifest_path, "scoped activation manifest")?;
            let bytes = std::fs::read(&manifest_path)?;
            let manifest: ScopedBatchManifest = serde_json::from_slice(&bytes)?;
            let persisted_actions = manifest
                .actions
                .iter()
                .map(ScopedManifestAction::to_action)
                .collect::<Result<Vec<_>, _>>()?;
            if manifest.schema_version != 3
                || manifest.scope != authority.scope
                || manifest.session_id != session_id
                || manifest.operation_id != operation_id
                || manifest.first_action_index != 0
                || manifest.actions_hash
                    != crate::reconcile::compute_actions_hash(&persisted_actions)
                || manifest.spec != *spec
            {
                return Err(scope_state_conflict(
                    "persisted scoped activation payload does not match resumed action batch",
                ));
            }
            let offset = first_action_index
                .checked_sub(manifest.first_action_index)
                .ok_or_else(|| {
                    scope_state_conflict(
                        "resumed action cursor precedes persisted activation manifest",
                    )
                })?;
            let end = offset.checked_add(manifest_actions.len()).ok_or_else(|| {
                StackError::InvalidSpec("resumed action range overflow".to_string())
            })?;
            if end != manifest.actions.len()
                || manifest.actions.get(offset..end) != Some(manifest_actions.as_slice())
            {
                return Err(scope_state_conflict(
                    "resumed actions do not match persisted activation manifest",
                ));
            }
            let (secret_inputs, secret_digests) =
                load_staged_secret_inputs(&owner_dir, &manifest.secret_inputs)?;
            self.scoped_secret_inputs = secret_inputs;
            self.scoped_secret_digests = secret_digests;
            self.scoped_secret_dir = Some(owner_dir.join("secrets"));
            return Ok(());
        }

        // The directory itself is a durable first-write sentinel. If it exists
        // without a valid manifest, this exact operation may already have
        // journal/runtime effects and must never be reconstructed from mutable
        // external inputs.
        if owner_dir.exists() {
            return Err(scope_state_conflict(
                "scoped activation manifest is missing after operation staging began",
            ));
        }
        if first_action_index != 0 {
            return Err(scope_state_conflict(
                "nonzero scoped resume cursor has no operation activation manifest",
            ));
        }
        if self.batch_has_matching_active_journal(
            actions,
            session_id,
            operation_id,
            first_action_index,
        )? {
            return Err(scope_state_conflict(
                "scoped activation manifest is missing for an active journal attempt",
            ));
        }

        let needs_activation_inputs = actions.iter().any(|action| {
            matches!(
                action,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        });
        let referenced_secrets = if needs_activation_inputs {
            spec.services
                .iter()
                .flat_map(|service| service.secrets.iter().map(|secret| secret.source.as_str()))
                .collect::<std::collections::BTreeSet<_>>()
        } else {
            std::collections::BTreeSet::new()
        };
        let mut secret_bytes = BTreeMap::new();
        for secret in spec
            .secrets
            .iter()
            .filter(|secret| referenced_secrets.contains(secret.name.as_str()))
        {
            validate_secret_file_name(&secret.name)?;
            secret_bytes.insert(secret.name.clone(), load_secret_source_bytes(secret)?);
        }
        let staging_root = owner_dir.parent().ok_or_else(|| {
            StackError::InvalidSpec("scoped activation owner directory has no parent".to_string())
        })?;
        if staging_root.exists() {
            validate_private_directory(staging_root)?;
        } else {
            create_private_directory(staging_root)?;
        }
        let temporary_owner = create_private_temp_directory(
            staging_root,
            owner_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("scoped-operation"),
        )?;
        let temporary_secrets = temporary_owner.join("secrets");
        create_private_directory(&temporary_secrets)?;
        let mut secret_inputs = BTreeMap::new();
        let mut secret_digests = BTreeMap::new();
        for (name, bytes) in &secret_bytes {
            let digest = format!("sha256:{:x}", Sha256::digest(bytes));
            atomic_write_private(&temporary_secrets.join(name), bytes)?;
            secret_inputs.insert(
                name.clone(),
                ScopedSecretInput {
                    sha256: digest.clone(),
                    file_name: name.clone(),
                },
            );
            secret_digests.insert(name.clone(), digest);
        }
        let manifest = ScopedBatchManifest {
            schema_version: 3,
            scope: authority.scope.clone(),
            session_id: session_id.to_string(),
            operation_id: operation_id.to_string(),
            first_action_index,
            actions_hash: crate::reconcile::compute_actions_hash(actions),
            spec: spec.clone(),
            actions: manifest_actions,
            secret_inputs,
        };
        atomic_write_private(
            &temporary_owner.join("manifest.json"),
            &serde_json::to_vec(&manifest)?,
        )?;
        sync_directory(&temporary_secrets)?;
        sync_directory(&temporary_owner)?;
        std::fs::rename(&temporary_owner, &owner_dir)?;
        sync_directory(staging_root)?;
        let secrets_dir = owner_dir.join("secrets");
        self.scoped_secret_inputs = secret_bytes;
        self.scoped_secret_digests = secret_digests;
        self.scoped_secret_dir = Some(secrets_dir);
        Ok(())
    }

    fn batch_has_matching_active_journal(
        &self,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<bool, StackError> {
        let authority = self
            .scoped_authority
            .as_ref()
            .ok_or_else(|| scope_state_conflict("scoped journal probe requires authority"))?;
        let records = self
            .store
            .list_stack_container_recovery_records_for_machine_workload(&authority.scope)?;
        for record in records {
            for (index, action) in actions.iter().enumerate() {
                if record.intent.service_name != action.target().service_name
                    || record.intent.replica_index != action.target().replica_index.get()
                {
                    continue;
                }
                let absolute_index = first_action_index.checked_add(index).ok_or_else(|| {
                    StackError::InvalidSpec("absolute action index overflow".to_string())
                })?;
                let execution_key = crate::reconcile::ReconcileActionExecutionKey::new(
                    session_id,
                    operation_id,
                    absolute_index,
                    action,
                )?;
                if execution_key.matches_activation_digest(&record.intent.action_digest)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub(super) fn execute_scoped_actions(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        service_map: &HashMap<&str, &ServiceSpec>,
        batch: (&str, &str, usize),
        skipped_mounts: Vec<crate::volume::SkippedMount>,
    ) -> Result<ExecutionResult, StackError> {
        let (session_id, operation_id, first_action_index) = batch;
        let authority = self
            .scoped_authority
            .clone()
            .ok_or_else(|| scope_state_conflict("scoped dispatch requires authority"))?;
        if authority.scope.stack_id != spec.name {
            return Err(scope_state_conflict("scoped stack identity changed"));
        }

        let creates: Vec<&Action> = actions
            .iter()
            .filter(|action| {
                matches!(
                    action,
                    Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
                )
            })
            .collect();
        let removes: Vec<&Action> = actions
            .iter()
            .filter(|action| matches!(action, Action::ServiceRemove { .. }))
            .collect();
        let mut result = ExecutionResult {
            skipped_mounts,
            ..ExecutionResult::default()
        };
        let mut outcome_failures: HashMap<ServiceReplicaKey, String> = HashMap::new();

        for level in compute_topo_levels(&creates, spec) {
            let mut activations = Vec::new();
            for action in level {
                let target = action.target();
                let service_name = target.service_name.as_str();
                let replica_index = target.replica_index.get();
                if let Err(error) = self.recover_stale_scoped_replica(target) {
                    record_action_error(&mut result, &mut outcome_failures, target, error);
                    continue;
                }
                let action_index = actions
                    .iter()
                    .position(|candidate| std::ptr::eq(candidate, action))
                    .ok_or_else(|| StackError::InvalidSpec("action index was lost".to_string()))?;
                let absolute_action_index = first_action_index
                    .checked_add(action_index)
                    .ok_or_else(|| {
                        StackError::InvalidSpec("absolute action index overflow".to_string())
                    })?;
                let Some(service) = service_map.get(service_name).copied() else {
                    record_action_error(
                        &mut result,
                        &mut outcome_failures,
                        target,
                        StackError::InvalidSpec(format!(
                            "service `{service_name}` is missing from the desired stack"
                        )),
                    );
                    continue;
                };
                if matches!(action, Action::ServiceCreate { .. }) {
                    match self.admit_existing_running_replica(target) {
                        Ok(true) => {
                            result.succeeded += 1;
                            continue;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            record_action_error(&mut result, &mut outcome_failures, target, error);
                            continue;
                        }
                    }
                }
                let prepared =
                    match self.prepare_create(spec, service_map, service_name, replica_index) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            record_action_error(&mut result, &mut outcome_failures, target, error);
                            continue;
                        }
                    };
                let action_digest = scoped_action_digest(
                    session_id,
                    operation_id,
                    absolute_action_index,
                    action,
                    &prepared,
                    spec,
                    &self.scoped_secret_digests,
                )?;
                let selector = StackContainerCreateSelector {
                    project_id: authority.scope.project_id.clone(),
                    environment_id: authority.scope.environment_id.clone(),
                    machine_id: authority.scope.machine_id.clone(),
                    machine_incarnation_id: authority.scope.machine_incarnation_id.clone(),
                    environment_generation: authority.environment_generation,
                    stack_id: authority.scope.stack_id.clone(),
                    service_name: service_name.to_string(),
                    replica_index,
                    requested_container_id: prepared.requested_container_id.clone(),
                    definition_digest: authority.definition_digest.clone(),
                    action_digest,
                    applied_config_digest: crate::reconcile::service_config_digest(service),
                };
                if matches!(action, Action::ServiceRecreate { .. })
                    && let Err(error) =
                        self.cleanup_recreate_predecessor(target, &selector.action_digest)
                {
                    record_action_error(&mut result, &mut outcome_failures, target, error);
                    continue;
                }
                let (intent, binding) = match self
                    .store
                    .resolve_or_begin_stack_container_create(&selector, unix_now())
                {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        record_action_error(&mut result, &mut outcome_failures, target, error);
                        continue;
                    }
                };
                match self.admit_scoped_activation(&intent, binding) {
                    Ok(Some(ownership)) => activations.push(ScopedActivation {
                        target: target.clone(),
                        intent,
                        ownership,
                        image: prepared.image,
                        config: prepared.run_config,
                    }),
                    Ok(None) => result.succeeded += 1,
                    Err(error) => {
                        record_action_error(&mut result, &mut outcome_failures, target, error)
                    }
                }
            }

            let mut pulled = HashSet::new();
            let mut ready = Vec::new();
            for activation in activations {
                if !pulled.contains(&activation.image) {
                    if let Err(error) = self.runtime.pull(&activation.image) {
                        let message = format!("image pull failed: {error}");
                        match self.fail_and_cleanup_scoped(&activation.intent, &message) {
                            Ok(()) => record_action_error(
                                &mut result,
                                &mut outcome_failures,
                                &activation.target,
                                error,
                            ),
                            Err(cleanup_error) => record_action_error(
                                &mut result,
                                &mut outcome_failures,
                                &activation.target,
                                cleanup_error,
                            ),
                        }
                        continue;
                    }
                    pulled.insert(activation.image.clone());
                }
                ready.push(activation);
            }

            let runtime = &self.runtime;
            let outcomes = std::thread::scope(|scope| {
                ready
                    .into_iter()
                    .map(|activation| {
                        let target = activation.target.clone();
                        let handle = scope.spawn(move || {
                            let outcome = runtime.activate_container_generation(
                                activation.ownership.clone(),
                                &activation.image,
                                activation.config.clone(),
                            );
                            (activation, outcome)
                        });
                        (target, handle)
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|(target, handle)| (target, handle.join()))
                    .collect::<Vec<_>>()
            });
            for (target, outcome) in outcomes {
                let (activation, outcome) = match outcome {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        let error =
                            StackError::Network("container activation thread panicked".to_string());
                        record_action_error(&mut result, &mut outcome_failures, &target, error);
                        continue;
                    }
                };
                match outcome {
                    Ok(receipt) => {
                        if let Err(error) = validate_exact_receipt(&activation.ownership, &receipt)
                        {
                            let message = error.to_string();
                            match self.fail_and_cleanup_scoped(&activation.intent, &message) {
                                Ok(()) => record_action_error(
                                    &mut result,
                                    &mut outcome_failures,
                                    &activation.target,
                                    error,
                                ),
                                Err(cleanup_error) => record_action_error(
                                    &mut result,
                                    &mut outcome_failures,
                                    &activation.target,
                                    cleanup_error,
                                ),
                            }
                            continue;
                        }
                        match self.store.publish_stack_container_create_success(
                            &activation.intent.scope.reservation_id,
                            false,
                            unix_now(),
                        ) {
                            Ok(_) => result.succeeded += 1,
                            Err(error) => {
                                let reason = format!(
                                    "runtime activation completed but journal publication failed: {error}"
                                );
                                match self.fail_and_cleanup_scoped(&activation.intent, &reason) {
                                    Ok(()) => record_action_error(
                                        &mut result,
                                        &mut outcome_failures,
                                        &activation.target,
                                        error,
                                    ),
                                    Err(cleanup_error) => record_action_error(
                                        &mut result,
                                        &mut outcome_failures,
                                        &activation.target,
                                        cleanup_error,
                                    ),
                                }
                            }
                        }
                    }
                    Err(failure) => {
                        let message = failure.error.to_string();
                        if failure
                            .cleanup
                            .as_ref()
                            .is_some_and(|cleanup| cleanup != &activation.ownership)
                        {
                            let error = self.block_scoped_intent(
                                &activation.intent,
                                "activation returned foreign cleanup ownership",
                            );
                            record_action_error(
                                &mut result,
                                &mut outcome_failures,
                                &activation.target,
                                error,
                            );
                            continue;
                        }
                        match self.fail_and_cleanup_scoped(&activation.intent, &message) {
                            Ok(()) => record_action_error(
                                &mut result,
                                &mut outcome_failures,
                                &activation.target,
                                failure.error,
                            ),
                            Err(cleanup_error) => record_action_error(
                                &mut result,
                                &mut outcome_failures,
                                &activation.target,
                                cleanup_error,
                            ),
                        }
                    }
                }
            }
        }

        for action in removes {
            match self.execute_scoped_remove(spec, action.target()) {
                Ok(()) => result.succeeded += 1,
                Err(error) => {
                    record_action_error(&mut result, &mut outcome_failures, action.target(), error)
                }
            }
        }
        result.outcomes = actions
            .iter()
            .enumerate()
            .map(|(relative_index, action)| {
                let result = outcome_failures
                    .get(action.target())
                    .map(|error| ActionOutcomeResult::Failed {
                        error: error.clone(),
                    })
                    .unwrap_or(ActionOutcomeResult::Succeeded);
                Ok(IndexedActionOutcome {
                    absolute_index: first_action_index.checked_add(relative_index).ok_or_else(
                        || StackError::InvalidSpec("absolute action index overflow".to_string()),
                    )?,
                    action_hash: crate::reconcile::compute_actions_hash(std::slice::from_ref(
                        action,
                    )),
                    action_kind: ReconcileActionKind::from_action(action),
                    target: action.target().clone(),
                    result,
                })
            })
            .collect::<Result<Vec<_>, StackError>>()?;
        Ok(result)
    }

    fn admit_scoped_activation(
        &mut self,
        intent: &StackContainerCreateIntent,
        binding: Option<StackContainerGenerationBinding>,
    ) -> Result<Option<ContainerGenerationOwnership>, StackError> {
        match intent.status {
            StackContainerCreateStatus::Running => {
                let binding = binding.ok_or_else(|| {
                    StackError::InvalidSpec("Running create is missing its binding".to_string())
                })?;
                match self
                    .runtime
                    .inspect_container_generation(&binding.ownership)?
                {
                    ContainerGenerationInspection::Published(found)
                        if found == binding.ownership =>
                    {
                        Ok(None)
                    }
                    ContainerGenerationInspection::Absent => {
                        self.cleanup_scoped_binding(intent, &binding)?;
                        Err(scope_state_conflict(
                            "journal Running generation was absent and has been terminalized",
                        ))
                    }
                    other => Err(self.block_scoped_intent(
                        intent,
                        &format!("journal Running does not match runtime state: {other:?}"),
                    )),
                }
            }
            StackContainerCreateStatus::CleanupPending => {
                let binding = binding.ok_or_else(|| {
                    StackError::InvalidSpec("cleanup-pending create is missing binding".to_string())
                })?;
                self.cleanup_scoped_binding(intent, &binding)?;
                Err(scope_state_conflict(
                    "prior scoped activation required cleanup; retry will allocate a new attempt",
                ))
            }
            StackContainerCreateStatus::Blocked => Err(scope_state_conflict(
                intent
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "scoped create is blocked".to_string()),
            )),
            StackContainerCreateStatus::Intent => {
                if binding.is_some() {
                    return Err(self.block_scoped_intent(
                        intent,
                        "Intent unexpectedly has a generation binding",
                    ));
                }
                let ownership = match self
                    .runtime
                    .inspect_container_reservation(&intent.scope, &intent.requested_container_id)?
                {
                    ContainerGenerationInspection::Absent => {
                        self.runtime.reserve_container_generation(
                            &intent.scope,
                            &intent.requested_container_id,
                        )?
                    }
                    ContainerGenerationInspection::ReservedUnpublished(ownership) => ownership,
                    ContainerGenerationInspection::Published(ownership) => {
                        let binding = self.bind_scoped_ownership(intent, ownership)?;
                        self.fail_and_cleanup_scoped(
                            intent,
                            "reservation was already published without journal Running proof",
                        )?;
                        return Err(scope_state_conflict(format!(
                            "published orphan generation {} was cleaned",
                            binding.ownership.generation
                        )));
                    }
                    other => {
                        return Err(self.block_scoped_intent(
                            intent,
                            &format!("reservation cannot be safely adopted: {other:?}"),
                        ));
                    }
                };
                let binding = self.bind_scoped_ownership(intent, ownership)?;
                Ok(Some(binding.ownership))
            }
            StackContainerCreateStatus::Reserved => {
                let binding = binding.ok_or_else(|| {
                    StackError::InvalidSpec("Reserved create is missing binding".to_string())
                })?;
                match self
                    .runtime
                    .inspect_container_generation(&binding.ownership)?
                {
                    ContainerGenerationInspection::ReservedUnpublished(found)
                        if found == binding.ownership =>
                    {
                        Ok(Some(binding.ownership))
                    }
                    ContainerGenerationInspection::Published(found)
                        if found == binding.ownership =>
                    {
                        self.fail_and_cleanup_scoped(
                            intent,
                            "generation was published without journal Running proof",
                        )?;
                        Err(scope_state_conflict(
                            "published orphan generation was cleaned",
                        ))
                    }
                    ContainerGenerationInspection::Absent => {
                        self.fail_and_cleanup_scoped(
                            intent,
                            "reserved generation disappeared before activation",
                        )?;
                        Err(scope_state_conflict("reserved generation disappeared"))
                    }
                    other => Err(self.block_scoped_intent(
                        intent,
                        &format!("bound generation cannot be safely used: {other:?}"),
                    )),
                }
            }
            StackContainerCreateStatus::Cleaned | StackContainerCreateStatus::Failed => Err(
                scope_state_conflict("terminal create attempt cannot be resumed"),
            ),
        }
    }

    fn admit_existing_running_replica(
        &mut self,
        target: &ServiceReplicaKey,
    ) -> Result<bool, StackError> {
        let authority = self.scoped_authority.as_ref().ok_or_else(|| {
            scope_state_conflict("scoped running-replica admission requires authority")
        })?;
        let mut matches = self
            .store
            .list_stack_container_recovery_records_for_machine_workload(&authority.scope)?
            .into_iter()
            .filter(|record| {
                record.intent.service_name == target.service_name
                    && record.intent.replica_index == target.replica_index.get()
                    && record.intent.scope.machine_incarnation_id.as_ref()
                        == Some(&authority.scope.machine_incarnation_id)
                    && record.intent.status == StackContainerCreateStatus::Running
            });
        let Some(record) = matches.next() else {
            return Ok(false);
        };
        if matches.next().is_some() {
            return Err(scope_state_conflict(format!(
                "replica `{}` has multiple Running reservations",
                exact_target_label(target)
            )));
        }
        match self.admit_scoped_activation(&record.intent, record.binding)? {
            None => Ok(true),
            Some(_) => Err(scope_state_conflict(
                "Running replica unexpectedly requested a second activation",
            )),
        }
    }

    fn recover_stale_scoped_replica(
        &mut self,
        target: &ServiceReplicaKey,
    ) -> Result<(), StackError> {
        let authority = self
            .scoped_authority
            .as_ref()
            .ok_or_else(|| scope_state_conflict("scoped recovery requires authority"))?;
        let records = self
            .store
            .list_stack_container_recovery_records_for_machine_workload(&authority.scope)?;
        for record in records.into_iter().filter(|record| {
            record.intent.service_name == target.service_name
                && record.intent.replica_index == target.replica_index.get()
        }) {
            match record.disposition {
                StackContainerRecoveryDisposition::Activatable => {}
                StackContainerRecoveryDisposition::CleanupOnly { .. } => {
                    let binding = record.binding.ok_or_else(|| {
                        self.block_scoped_intent(
                            &record.intent,
                            "cleanup-only recovery record is missing exact ownership",
                        )
                    })?;
                    self.cleanup_scoped_binding(&record.intent, &binding)?;
                }
                StackContainerRecoveryDisposition::Abandonable { stale_reason } => {
                    match self.runtime.inspect_container_reservation(
                        &record.intent.scope,
                        &record.intent.requested_container_id,
                    )? {
                        ContainerGenerationInspection::Absent => {
                            self.store.abandon_stale_stack_container_create(
                                &record.intent.scope.reservation_id,
                                &stale_reason,
                                unix_now(),
                            )?;
                        }
                        ContainerGenerationInspection::ReservedUnpublished(ownership)
                        | ContainerGenerationInspection::Published(ownership) => {
                            let binding = StackContainerGenerationBinding {
                                reservation_id: record.intent.scope.reservation_id.clone(),
                                service_name: record.intent.service_name.clone(),
                                ownership,
                                bound_at: unix_now().max(record.intent.updated_at),
                            };
                            let binding = self
                                .store
                                .bind_stack_container_generation_for_cleanup(&binding)?;
                            self.cleanup_scoped_binding(&record.intent, &binding)?;
                        }
                        other => {
                            return Err(self.block_scoped_intent(
                                &record.intent,
                                &format!("stale reservation is unsafe: {other:?}"),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn cleanup_recreate_predecessor(
        &mut self,
        target: &ServiceReplicaKey,
        action_digest: &str,
    ) -> Result<(), StackError> {
        let authority = self
            .scoped_authority
            .as_ref()
            .ok_or_else(|| scope_state_conflict("scoped replacement requires authority"))?;
        let records = self
            .store
            .list_stack_container_recovery_records_for_machine_workload(&authority.scope)?;
        let Some(record) = records.into_iter().find(|record| {
            record.intent.service_name == target.service_name
                && record.intent.replica_index == target.replica_index.get()
        }) else {
            return Ok(());
        };
        if record.intent.action_digest == action_digest {
            return Ok(());
        }
        if !matches!(
            record.disposition,
            StackContainerRecoveryDisposition::Activatable
        ) {
            return Err(scope_state_conflict(
                "recreate predecessor is not eligible for current-scope cleanup",
            ));
        }
        if let Some(binding) = record.binding {
            return self.cleanup_scoped_binding(&record.intent, &binding);
        }
        match self.runtime.inspect_container_reservation(
            &record.intent.scope,
            &record.intent.requested_container_id,
        )? {
            ContainerGenerationInspection::Absent => {
                self.store.publish_stack_container_create_failure(
                    &record.intent.scope.reservation_id,
                    "superseded unbound create had no runtime reservation",
                    unix_now(),
                )?;
                Ok(())
            }
            ContainerGenerationInspection::ReservedUnpublished(ownership)
            | ContainerGenerationInspection::Published(ownership) => {
                let binding = self.bind_scoped_ownership(&record.intent, ownership)?;
                self.cleanup_scoped_binding(&record.intent, &binding)
            }
            other => Err(self.block_scoped_intent(
                &record.intent,
                &format!("recreate predecessor ownership is unsafe: {other:?}"),
            )),
        }
    }

    fn bind_scoped_ownership(
        &self,
        intent: &StackContainerCreateIntent,
        ownership: ContainerGenerationOwnership,
    ) -> Result<StackContainerGenerationBinding, StackError> {
        if ownership.container_id != intent.requested_container_id
            || ownership.scope.as_deref() != Some(&intent.scope)
            || ownership.stack_id != intent.scope.stack_id
            || ownership.validate().is_err()
        {
            return Err(self.block_scoped_intent(
                intent,
                "runtime returned ownership outside the exact reservation",
            ));
        }
        self.store
            .bind_stack_container_generation(&StackContainerGenerationBinding {
                reservation_id: intent.scope.reservation_id.clone(),
                service_name: intent.service_name.clone(),
                ownership,
                bound_at: unix_now().max(intent.updated_at),
            })
    }

    fn fail_and_cleanup_scoped(
        &mut self,
        intent: &StackContainerCreateIntent,
        reason: &str,
    ) -> Result<(), StackError> {
        let binding = self
            .store
            .load_stack_container_generation_binding(&intent.scope.reservation_id)?;
        if let Some(binding) = binding {
            // A bound failure must enter CleanupPending while the immutable
            // ownership proof still fences deletion. It must never become a
            // terminal Failed row before exact runtime cleanup completes.
            self.store
                .begin_stack_container_cleanup(&intent.scope.reservation_id, unix_now())?;
            self.cleanup_scoped_binding(intent, &binding)?;
        } else {
            self.store.publish_stack_container_create_failure(
                &intent.scope.reservation_id,
                reason,
                unix_now(),
            )?;
        }
        Ok(())
    }

    fn cleanup_scoped_binding(
        &mut self,
        intent: &StackContainerCreateIntent,
        binding: &StackContainerGenerationBinding,
    ) -> Result<(), StackError> {
        let current = self
            .store
            .load_stack_container_create_intent(&intent.scope.reservation_id)?
            .ok_or_else(|| StackError::InvalidSpec("cleanup intent disappeared".to_string()))?;
        if current.status != StackContainerCreateStatus::CleanupPending {
            self.store
                .begin_stack_container_cleanup(&intent.scope.reservation_id, unix_now())?;
        }
        match self
            .runtime
            .inspect_container_generation(&binding.ownership)?
        {
            ContainerGenerationInspection::Absent => {}
            ContainerGenerationInspection::ReservedUnpublished(found)
                if found == binding.ownership =>
            {
                self.runtime.release_container_reservation(found)?;
            }
            ContainerGenerationInspection::Published(found) if found == binding.ownership => {
                let desired = self.store.load_desired_state(&intent.scope.stack_id)?;
                let service = desired.as_ref().and_then(|spec| {
                    spec.services
                        .iter()
                        .find(|service| service.name == intent.service_name)
                });
                let signal = service.and_then(|service| service.stop_signal.as_deref());
                let grace = service
                    .and_then(|service| service.stop_grace_period_secs)
                    .map(std::time::Duration::from_secs);
                self.runtime
                    .stop_and_remove_container_generation(found, signal, grace)?;
            }
            other => {
                return Err(self.block_scoped_intent(
                    intent,
                    &format!("cleanup ownership no longer matches runtime: {other:?}"),
                ));
            }
        }
        self.store
            .publish_stack_container_cleanup_success(&intent.scope.reservation_id, unix_now())?;
        Ok(())
    }

    fn execute_scoped_remove(
        &mut self,
        spec: &StackSpec,
        target: &ServiceReplicaKey,
    ) -> Result<(), StackError> {
        let authority = self
            .scoped_authority
            .clone()
            .ok_or_else(|| scope_state_conflict("scoped removal requires authority"))?;
        let records = self
            .store
            .list_stack_container_recovery_records_for_machine_workload(&authority.scope)?;
        let matching: Vec<_> = records
            .into_iter()
            .filter(|record| {
                record.intent.service_name == target.service_name
                    && record.intent.replica_index == target.replica_index.get()
                    && record.intent.scope.machine_incarnation_id.as_ref()
                        == Some(&authority.scope.machine_incarnation_id)
            })
            .collect();
        if matching.len() > 1 {
            return Err(scope_state_conflict(format!(
                "replica `{}` maps to multiple scoped replica reservations",
                exact_target_label(target)
            )));
        }
        if matching.is_empty() {
            let legacy = self
                .store
                .load_observed_state(&spec.name)?
                .into_iter()
                .any(|state| {
                    state.replica == *target
                        && (state.container_id.is_some() || state.failed_create_ownership.is_some())
                });
            if legacy {
                return Err(scope_state_conflict(format!(
                    "replica `{}` has no scoped journal cleanup authority",
                    exact_target_label(target)
                )));
            }
            self.ports.release_replica(target);
            return Ok(());
        }
        for record in matching {
            if let StackContainerRecoveryDisposition::Abandonable { stale_reason } =
                &record.disposition
            {
                match self.runtime.inspect_container_reservation(
                    &record.intent.scope,
                    &record.intent.requested_container_id,
                )? {
                    ContainerGenerationInspection::Absent => {
                        self.store.abandon_stale_stack_container_create(
                            &record.intent.scope.reservation_id,
                            stale_reason,
                            unix_now(),
                        )?;
                        continue;
                    }
                    ContainerGenerationInspection::ReservedUnpublished(ownership)
                    | ContainerGenerationInspection::Published(ownership) => {
                        let binding = StackContainerGenerationBinding {
                            reservation_id: record.intent.scope.reservation_id.clone(),
                            service_name: record.intent.service_name.clone(),
                            ownership,
                            bound_at: unix_now().max(record.intent.updated_at),
                        };
                        let binding = self
                            .store
                            .bind_stack_container_generation_for_cleanup(&binding)?;
                        self.cleanup_scoped_binding(&record.intent, &binding)?;
                        continue;
                    }
                    other => {
                        return Err(self.block_scoped_intent(
                            &record.intent,
                            &format!("stale unbound reservation is unsafe: {other:?}"),
                        ));
                    }
                }
            }
            match (record.intent.status, record.binding) {
                (StackContainerCreateStatus::Intent, None) => {
                    self.store.publish_stack_container_create_failure(
                        &record.intent.scope.reservation_id,
                        "create removed before runtime reservation",
                        unix_now(),
                    )?;
                }
                (StackContainerCreateStatus::Blocked, _) => {
                    return Err(scope_state_conflict(format!(
                        "replica `{}` has a blocked reservation",
                        exact_target_label(target)
                    )));
                }
                (_, Some(binding)) => self.cleanup_scoped_binding(&record.intent, &binding)?,
                _ => {
                    return Err(self.block_scoped_intent(
                        &record.intent,
                        "nonterminal create is missing cleanup binding",
                    ));
                }
            }
        }
        self.ports.release_replica(target);
        Ok(())
    }

    fn block_scoped_intent(&self, intent: &StackContainerCreateIntent, reason: &str) -> StackError {
        match self.store.publish_stack_container_blocked(
            &intent.scope.reservation_id,
            reason,
            unix_now(),
        ) {
            Ok(_) => scope_state_conflict(reason),
            Err(error) => error,
        }
    }
}

fn exact_target_label(target: &ServiceReplicaKey) -> String {
    format!("{}#{}", target.service_name, target.replica_index)
}

fn validate_exact_receipt(
    ownership: &ContainerGenerationOwnership,
    receipt: &ContainerCreateReceipt,
) -> Result<(), StackError> {
    if receipt.container_id != ownership.container_id
        || receipt.ownership.as_ref() != Some(ownership)
    {
        return Err(scope_state_conflict(
            "activation receipt does not match the exact reserved ownership",
        ));
    }
    Ok(())
}

fn record_action_error(
    result: &mut ExecutionResult,
    outcome_failures: &mut HashMap<ServiceReplicaKey, String>,
    target: &ServiceReplicaKey,
    error: StackError,
) {
    let message = error.to_string();
    result.failed += 1;
    result
        .errors
        .push((exact_target_label(target), message.clone()));
    outcome_failures.entry(target.clone()).or_insert(message);
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn scoped_action_digest(
    session_id: &str,
    operation_id: &str,
    absolute_action_index: usize,
    action: &Action,
    prepared: &PreparedCreate,
    spec: &StackSpec,
    secret_digests: &BTreeMap<String, String>,
) -> Result<String, StackError> {
    if prepared.target != *action.target() {
        return Err(scope_state_conflict(
            "prepared activation target differs from the exact action target",
        ));
    }
    let identity_prefix = crate::reconcile::ReconcileActionExecutionKey::new(
        session_id,
        operation_id,
        absolute_action_index,
        action,
    )?
    .activation_digest_prefix()?;
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"schema", b"vz.stack.activation-payload.v2");
    hash_field(&mut hasher, b"image", prepared.image.as_bytes());
    hash_field(
        &mut hasher,
        b"container_id",
        prepared.requested_container_id.as_bytes(),
    );
    hash_run_config(&mut hasher, &prepared.run_config)?;
    let service = spec
        .services
        .iter()
        .find(|service| service.name == prepared.target.service_name)
        .ok_or_else(|| StackError::InvalidSpec("prepared service disappeared".to_string()))?;
    hash_field(
        &mut hasher,
        b"secret_count",
        &u64::try_from(service.secrets.len())
            .map_err(|_| StackError::InvalidSpec("secret count exceeds u64".to_string()))?
            .to_le_bytes(),
    );
    for secret in &service.secrets {
        hash_field(&mut hasher, b"secret_source", secret.source.as_bytes());
        hash_field(&mut hasher, b"secret_target", secret.target.as_bytes());
        hash_field(&mut hasher, b"secret_mode", &secret.mode.to_le_bytes());
        hash_field(&mut hasher, b"secret_uid", &secret.uid.to_le_bytes());
        hash_field(&mut hasher, b"secret_gid", &secret.gid.to_le_bytes());
        let digest = secret_digests.get(&secret.source).ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "scoped activation manifest is missing digest for secret '{}'",
                secret.source
            ))
        })?;
        hash_field(&mut hasher, b"secret_sha256", digest.as_bytes());
    }
    Ok(format!("{identity_prefix}{:x}", hasher.finalize()))
}

fn hash_run_config(
    hasher: &mut Sha256,
    config: &vz_runtime_contract::RunConfig,
) -> Result<(), StackError> {
    hash_strings(hasher, b"cmd", &config.cmd);
    hash_optional_string(hasher, b"working_dir", config.working_dir.as_deref());
    let mut env = config.env.clone();
    env.sort();
    hash_pairs(hasher, b"env", &env);
    hash_optional_string(hasher, b"user", config.user.as_deref());
    hash_field(
        hasher,
        b"port_count",
        &(config.ports.len() as u64).to_le_bytes(),
    );
    for port in &config.ports {
        hash_field(hasher, b"port_host", &port.host.to_le_bytes());
        hash_field(hasher, b"port_container", &port.container.to_le_bytes());
        hash_field(hasher, b"port_protocol", port.protocol.as_str().as_bytes());
        hash_optional_string(hasher, b"port_target_host", port.target_host.as_deref());
    }
    hash_field(
        hasher,
        b"mount_count",
        &(config.mounts.len() as u64).to_le_bytes(),
    );
    for mount in &config.mounts {
        // Host source paths are deliberately excluded from persistent identity;
        // the exact StackSpec in the manifest binds their caller spelling.
        hash_field(
            hasher,
            b"mount_target",
            mount.target.as_os_str().as_encoded_bytes(),
        );
        match &mount.mount_type {
            vz_runtime_contract::MountType::Bind => hash_field(hasher, b"mount_type", b"bind"),
            vz_runtime_contract::MountType::Tmpfs => hash_field(hasher, b"mount_type", b"tmpfs"),
            vz_runtime_contract::MountType::Volume { volume_name } => {
                hash_field(hasher, b"mount_type", b"volume");
                hash_field(hasher, b"mount_volume", volume_name.as_bytes());
            }
        }
        let access = match mount.access {
            vz_runtime_contract::MountAccess::ReadWrite => b"rw".as_slice(),
            vz_runtime_contract::MountAccess::ReadOnly => b"ro".as_slice(),
        };
        hash_field(hasher, b"mount_access", access);
        hash_optional_string(hasher, b"mount_subpath", mount.subpath.as_deref());
    }
    hash_option_u8(hasher, b"cpus", config.cpus);
    hash_option_u64(hasher, b"memory_mb", config.memory_mb);
    hash_option_bool(hasher, b"network_enabled", config.network_enabled);
    let timeout_nanos = config.timeout.map(|duration| duration.as_nanos());
    match timeout_nanos {
        Some(value) => hash_field(hasher, b"timeout_nanos", &value.to_le_bytes()),
        None => hash_field(hasher, b"timeout_nanos", b"none"),
    }
    hash_optional_string(hasher, b"container_id", config.container_id.as_deref());
    match &config.init_process {
        Some(values) => hash_strings(hasher, b"init_process", values),
        None => hash_field(hasher, b"init_process", b"none"),
    }
    let mut annotations = config.oci_annotations.clone();
    annotations.sort();
    hash_pairs(hasher, b"oci_annotations", &annotations);
    let mut extra_hosts = config.extra_hosts.clone();
    extra_hosts.sort();
    hash_pairs(hasher, b"extra_hosts", &extra_hosts);
    let network_namespace = config
        .network_namespace_path
        .as_deref()
        .and_then(|path| Path::new(path).file_name())
        .and_then(|name| name.to_str());
    hash_optional_string(hasher, b"network_namespace_name", network_namespace);
    hash_option_i64(hasher, b"cpu_quota", config.cpu_quota);
    hash_option_u64(hasher, b"cpu_period", config.cpu_period);
    hash_bool(hasher, b"capture_logs", config.capture_logs);
    hash_strings(hasher, b"cap_add", &config.cap_add);
    hash_strings(hasher, b"cap_drop", &config.cap_drop);
    hash_bool(hasher, b"privileged", config.privileged);
    hash_bool(hasher, b"read_only_rootfs", config.read_only_rootfs);
    let mut sysctls = config.sysctls.clone();
    sysctls.sort();
    hash_pairs(hasher, b"sysctls", &sysctls);
    hash_field(
        hasher,
        b"ulimit_count",
        &(config.ulimits.len() as u64).to_le_bytes(),
    );
    for (name, soft, hard) in &config.ulimits {
        hash_field(hasher, b"ulimit_name", name.as_bytes());
        hash_field(hasher, b"ulimit_soft", &soft.to_le_bytes());
        hash_field(hasher, b"ulimit_hard", &hard.to_le_bytes());
    }
    hash_option_i64(hasher, b"pids_limit", config.pids_limit);
    hash_optional_string(hasher, b"hostname", config.hostname.as_deref());
    hash_optional_string(hasher, b"domainname", config.domainname.as_deref());
    hash_optional_string(hasher, b"stop_signal", config.stop_signal.as_deref());
    hash_option_u64(
        hasher,
        b"stop_grace_period_secs",
        config.stop_grace_period_secs,
    );
    hash_bool(hasher, b"share_host_network", config.share_host_network);
    hash_field(
        hasher,
        b"mount_tag_offset",
        &u64::try_from(config.mount_tag_offset)
            .map_err(|_| StackError::InvalidSpec("mount tag offset exceeds u64".to_string()))?
            .to_le_bytes(),
    );
    hash_strings(hasher, b"setup_commands", &config.setup_commands);
    Ok(())
}

fn hash_strings(hasher: &mut Sha256, name: &[u8], values: &[String]) {
    hash_field(hasher, name, &(values.len() as u64).to_le_bytes());
    for value in values {
        hash_field(hasher, name, value.as_bytes());
    }
}

fn hash_pairs(hasher: &mut Sha256, name: &[u8], values: &[(String, String)]) {
    hash_field(hasher, name, &(values.len() as u64).to_le_bytes());
    for (key, value) in values {
        hash_field(hasher, name, key.as_bytes());
        hash_field(hasher, name, value.as_bytes());
    }
}

fn hash_optional_string(hasher: &mut Sha256, name: &[u8], value: Option<&str>) {
    hash_field(hasher, name, value.unwrap_or("<none>").as_bytes());
}

fn hash_option_u8(hasher: &mut Sha256, name: &[u8], value: Option<u8>) {
    match value {
        Some(value) => hash_field(hasher, name, &[1, value]),
        None => hash_field(hasher, name, &[0]),
    }
}

fn hash_option_u64(hasher: &mut Sha256, name: &[u8], value: Option<u64>) {
    match value {
        Some(value) => {
            let mut bytes = vec![1];
            bytes.extend_from_slice(&value.to_le_bytes());
            hash_field(hasher, name, &bytes);
        }
        None => hash_field(hasher, name, &[0]),
    }
}

fn hash_option_i64(hasher: &mut Sha256, name: &[u8], value: Option<i64>) {
    match value {
        Some(value) => {
            let mut bytes = vec![1];
            bytes.extend_from_slice(&value.to_le_bytes());
            hash_field(hasher, name, &bytes);
        }
        None => hash_field(hasher, name, &[0]),
    }
}

fn hash_option_bool(hasher: &mut Sha256, name: &[u8], value: Option<bool>) {
    match value {
        Some(value) => hash_field(hasher, name, &[1, u8::from(value)]),
        None => hash_field(hasher, name, &[0]),
    }
}

fn hash_bool(hasher: &mut Sha256, name: &[u8], value: bool) {
    hash_field(hasher, name, &[u8::from(value)]);
}

fn hash_field(hasher: &mut Sha256, name: &[u8], value: &[u8]) {
    hasher.update((name.len() as u64).to_le_bytes());
    hasher.update(name);
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

impl ScopedManifestAction {
    fn from_action(action: &Action) -> Self {
        let kind = match action {
            Action::ServiceCreate { .. } => "create",
            Action::ServiceRecreate { .. } => "recreate",
            Action::ServiceRemove { .. } => "remove",
        };
        Self {
            schema_version: 3,
            kind: kind.to_string(),
            target: action.target().clone(),
            precondition: action.precondition().clone(),
        }
    }

    fn to_action(&self) -> Result<Action, StackError> {
        if self.schema_version != 3 {
            return Err(scope_state_conflict(
                "scoped manifest action uses an unsupported schema",
            ));
        }
        let action = match self.kind.as_str() {
            "create" => Action::ServiceCreate {
                target: self.target.clone(),
                precondition: self.precondition.clone(),
            },
            "recreate" => Action::ServiceRecreate {
                target: self.target.clone(),
                precondition: self.precondition.clone(),
            },
            "remove" => Action::ServiceRemove {
                target: self.target.clone(),
                precondition: self.precondition.clone(),
            },
            _ => {
                return Err(scope_state_conflict(
                    "scoped manifest action has an unknown kind",
                ));
            }
        };
        action.validate()?;
        Ok(action)
    }
}

fn scoped_manifest_owner_dir(
    data_dir: &std::path::Path,
    scope: &vz_runtime_contract::MachineWorkloadScope,
    operation_id: &str,
) -> PathBuf {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"schema", b"vz.stack.activation-manifest.v1");
    for (name, value) in [
        (b"project".as_slice(), scope.project_id.as_str().as_bytes()),
        (
            b"environment".as_slice(),
            scope.environment_id.as_str().as_bytes(),
        ),
        (b"machine".as_slice(), scope.machine_id.as_str().as_bytes()),
        (
            b"incarnation".as_slice(),
            scope.machine_incarnation_id.as_str().as_bytes(),
        ),
        (b"stack".as_slice(), scope.stack_id.as_bytes()),
        (b"operation".as_slice(), operation_id.as_bytes()),
    ] {
        hash_field(&mut hasher, name, value);
    }
    data_dir
        .join("scoped-activation")
        .join(format!("{:x}", hasher.finalize()))
}

fn validate_secret_file_name(name: &str) -> Result<(), StackError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(StackError::InvalidSpec(format!(
            "secret name `{name}` is not a safe staging file name"
        )));
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> Result<(), StackError> {
    std::fs::create_dir(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    sync_directory(path.parent().ok_or_else(|| {
        StackError::InvalidSpec("private staging directory has no parent".to_string())
    })?)
}

fn validate_private_directory(path: &Path) -> Result<(), StackError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(scope_state_conflict(
            "scoped activation staging path is not a private directory",
        ));
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(scope_state_conflict(
            "scoped activation staging directory permissions are not 0700",
        ));
    }
    Ok(())
}

fn validate_private_file(path: &Path, label: &str) -> Result<(), StackError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(scope_state_conflict(format!(
            "{label} is not a private regular file"
        )));
    }
    Ok(())
}

fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<(), StackError> {
    let parent = path
        .parent()
        .ok_or_else(|| StackError::InvalidSpec("private staging file has no parent".to_string()))?;
    validate_private_directory(parent)?;
    let (temporary, mut file) = loop {
        let temporary = parent.join(format!(".tmp-file-{}", temporary_token()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
        {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    };
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    sync_directory(parent)
}

fn create_private_temp_directory(parent: &Path, label: &str) -> Result<PathBuf, StackError> {
    loop {
        let path = parent.join(format!(".tmp-{label}-{}", temporary_token()));
        match std::fs::create_dir(&path) {
            Ok(()) => {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

fn temporary_token() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{}-{nanos}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn sync_directory(path: &Path) -> Result<(), StackError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn load_staged_secret_inputs(
    owner_dir: &Path,
    inputs: &BTreeMap<String, ScopedSecretInput>,
) -> Result<LoadedSecretInputs, StackError> {
    let secrets_dir = owner_dir.join("secrets");
    validate_private_directory(&secrets_dir)?;
    let mut bytes_by_name = BTreeMap::new();
    let mut digests = BTreeMap::new();
    for (name, input) in inputs {
        validate_secret_file_name(name)?;
        if input.file_name != *name {
            return Err(scope_state_conflict(
                "scoped activation secret staging reference is not canonical",
            ));
        }
        let path = secrets_dir.join(&input.file_name);
        validate_private_file(&path, &format!("staged secret `{name}`"))?;
        let bytes = std::fs::read(&path)?;
        let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
        if digest != input.sha256 {
            return Err(scope_state_conflict(format!(
                "staged secret `{name}` failed digest validation"
            )));
        }
        bytes_by_name.insert(name.clone(), bytes);
        digests.insert(name.clone(), digest);
    }
    Ok((bytes_by_name, digests))
}
