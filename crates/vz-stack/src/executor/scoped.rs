use super::create::{PreparedCreate, generated_runtime_container_id};
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
    ClaimedAllocatorNetworkIp, ClaimedAllocatorTarget, ClaimedCreateInput,
    ClaimedPredecessorInspection, ReconcileActionClaim, StackContainerCreateIntent,
    StackContainerCreateStatus, StackContainerGenerationBinding,
};
use vz_runtime_contract::{ContainerGenerationInspection, ContainerGenerationOwnership};

struct ScopedActivation {
    claim: ReconcileActionClaim,
    target: ServiceReplicaKey,
    intent: StackContainerCreateIntent,
    ownership: ContainerGenerationOwnership,
    image: String,
    config: vz_runtime_contract::RunConfig,
    initially_ready: bool,
}

enum ClaimedPreflightDecision {
    None,
    UnboundAbsent {
        claim: ReconcileActionClaim,
    },
    UnboundOwned {
        claim: ReconcileActionClaim,
        intent: StackContainerCreateIntent,
        binding: StackContainerGenerationBinding,
        inspection: ContainerGenerationInspection,
    },
    BoundCleanup {
        claim: ReconcileActionClaim,
        intent: StackContainerCreateIntent,
        binding: StackContainerGenerationBinding,
        inspection: ContainerGenerationInspection,
    },
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
    pub(crate) fn stage_scoped_batch_manifest(
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
        // The daemon intentionally does not create a stack runtime directory
        // before claimed execution.  The immutable manifest is the one
        // permitted pre-claim filesystem write, so its staging path must be
        // able to establish the otherwise-missing stack directory itself.
        // Runtime subdirectories (volumes, disks, and sandbox state) are still
        // created only after claim admission and whole-batch preflight.
        ensure_manifest_data_directory(&self.data_dir)?;
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

    pub(crate) fn require_scoped_batch_manifest(
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
        let manifest_path =
            scoped_manifest_owner_dir(&self.data_dir, &authority.scope, operation_id)
                .join("manifest.json");
        if !manifest_path.exists() {
            return Err(scope_state_conflict(
                "scoped activation manifest is missing after action claim admission",
            ));
        }
        self.stage_scoped_batch_manifest(
            spec,
            actions,
            session_id,
            operation_id,
            first_action_index,
        )
    }

    #[cfg(test)]
    pub(crate) fn prepare_scoped_batch_manifest(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<(), StackError> {
        self.stage_scoped_batch_manifest(
            spec,
            actions,
            session_id,
            operation_id,
            first_action_index,
        )
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
        claims: &[ReconcileActionClaim],
        service_map: &HashMap<&str, &ServiceSpec>,
        batch: (&str, &str, usize),
        skipped_mounts: Vec<crate::volume::SkippedMount>,
    ) -> Result<ExecutionResult, StackError> {
        let (_session_id, _operation_id, first_action_index) = batch;
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
                let action_index = actions
                    .iter()
                    .position(|candidate| std::ptr::eq(candidate, action))
                    .ok_or_else(|| StackError::InvalidSpec("action index was lost".to_string()))?;
                let claim = &claims[action_index];
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
                let prior_ports = self.ports.ports_for_replica(target).map(ToOwned::to_owned);
                let prepared =
                    match self.prepare_create(spec, service_map, service_name, replica_index) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            self.ports
                                .restore_replica_allocation(target, prior_ports.clone());
                            record_action_error(&mut result, &mut outcome_failures, target, error);
                            continue;
                        }
                    };
                let activation_payload_sha256 = match scoped_activation_payload_sha256(
                    &prepared,
                    spec,
                    &self.scoped_secret_digests,
                ) {
                    Ok(digest) => digest,
                    Err(error) => {
                        self.ports.restore_replica_allocation(target, prior_ports);
                        record_action_error(&mut result, &mut outcome_failures, target, error);
                        continue;
                    }
                };
                let input = ClaimedCreateInput {
                    requested_container_id: prepared.requested_container_id.clone(),
                    definition_digest: authority.definition_digest.clone(),
                    applied_config_digest: crate::reconcile::service_config_digest(service),
                    activation_payload_sha256,
                };
                let mut service_network_ips = self
                    .service_network_ips
                    .get(target)
                    .into_iter()
                    .flat_map(|networks| networks.iter())
                    .map(|(network_name, ip)| ClaimedAllocatorNetworkIp {
                        network_name: network_name.clone(),
                        ip: ip.clone(),
                    })
                    .collect::<Vec<_>>();
                service_network_ips.sort_by(|left, right| {
                    left.network_name
                        .cmp(&right.network_name)
                        .then_with(|| left.ip.cmp(&right.ip))
                });
                let allocation = ClaimedAllocatorTarget {
                    ports: self
                        .ports
                        .ports_for_replica(target)
                        .unwrap_or_default()
                        .to_vec(),
                    service_ip: self.service_ips.get(target).cloned(),
                    service_network_ips,
                    mount_tag_offset: self.mount_tag_offsets.get(service_name).copied(),
                };
                let intent = match self.store.resolve_or_begin_claimed_successor(
                    claim,
                    &input,
                    &allocation,
                    unix_now(),
                ) {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        self.ports.restore_replica_allocation(target, prior_ports);
                        record_action_error(&mut result, &mut outcome_failures, target, error);
                        continue;
                    }
                };
                match self.admit_claimed_activation(claim, &intent) {
                    Ok(Some(ownership)) => activations.push(ScopedActivation {
                        claim: claim.clone(),
                        target: target.clone(),
                        intent,
                        ownership,
                        image: prepared.image,
                        config: prepared.run_config,
                        initially_ready: service.healthcheck.is_none(),
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
                        match self.fail_and_cleanup_claimed_successor(
                            &activation.claim,
                            &activation.intent,
                            &message,
                        ) {
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
                        match self.store.publish_claimed_successor_success(
                            &activation.claim,
                            &activation.intent.scope.reservation_id,
                            &receipt,
                            activation.initially_ready,
                            unix_now(),
                        ) {
                            Ok(_) => result.succeeded += 1,
                            Err(error) => {
                                let reason = format!(
                                    "runtime activation completed but journal publication failed: {error}"
                                );
                                match self.fail_and_cleanup_claimed_successor(
                                    &activation.claim,
                                    &activation.intent,
                                    &reason,
                                ) {
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
                            let error = scope_state_conflict(
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
                        match self.fail_and_cleanup_claimed_successor(
                            &activation.claim,
                            &activation.intent,
                            &message,
                        ) {
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
            let action_index = actions
                .iter()
                .position(|candidate| std::ptr::eq(candidate, action))
                .ok_or_else(|| StackError::InvalidSpec("action index was lost".to_string()))?;
            match self.execute_claimed_remove(&claims[action_index], action.target()) {
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

    pub(super) fn preflight_claimed_predecessors(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        claims: &[ReconcileActionClaim],
    ) -> HashMap<ServiceReplicaKey, vz_runtime_contract::MachineError> {
        let mut failures = HashMap::new();
        let mut decisions = Vec::with_capacity(actions.len());
        for (action, claim) in actions.iter().zip(claims) {
            match self.inspect_claimed_predecessor_decision(spec, action, claim) {
                Ok(decision) => decisions.push((action.target().clone(), decision)),
                Err(error) => {
                    failures.insert(action.target().clone(), execution_machine_error(&error));
                }
            }
        }
        if !failures.is_empty() {
            return failures;
        }
        for (target, decision) in decisions {
            if let Err(error) = self.apply_claimed_preflight_decision(decision) {
                failures.insert(target, execution_machine_error(&error));
                break;
            }
        }
        failures
    }

    fn inspect_claimed_predecessor_decision(
        &self,
        spec: &StackSpec,
        action: &Action,
        claim: &ReconcileActionClaim,
    ) -> Result<ClaimedPreflightDecision, StackError> {
        let inspection = self.store.inspect_claimed_predecessor(claim)?;
        let claim_linked_successor = matches!(
            inspection,
            ClaimedPredecessorInspection::ClaimLinkedSuccessor { .. }
        );
        let decision = match inspection {
            ClaimedPredecessorInspection::ClaimLinkedSuccessor { intent, binding } => {
                if matches!(
                    intent.status,
                    StackContainerCreateStatus::Cleaned | StackContainerCreateStatus::Failed
                ) {
                    return Ok(ClaimedPreflightDecision::None);
                }
                match binding {
                    Some(binding) => {
                        let inspection = self
                            .runtime
                            .inspect_container_generation(&binding.ownership)?;
                        validate_exact_inspection(&binding.ownership, &inspection)?;
                    }
                    None => match self.runtime.inspect_container_reservation(
                        &intent.scope,
                        &intent.requested_container_id,
                    )? {
                        ContainerGenerationInspection::Absent => {}
                        ContainerGenerationInspection::ReservedUnpublished(ownership)
                        | ContainerGenerationInspection::Published(ownership) => {
                            exact_binding_for_intent(&intent, ownership)?;
                        }
                        other => {
                            return Err(scope_state_conflict(format!(
                                "claim-linked successor reservation is unsafe: {other:?}"
                            )));
                        }
                    },
                }
                Ok(ClaimedPreflightDecision::None)
            }
            ClaimedPredecessorInspection::NeverJournaled
            | ClaimedPredecessorInspection::ExactUnboundFailed { .. }
            | ClaimedPredecessorInspection::ExactBoundCleaned { .. } => {
                Ok(ClaimedPreflightDecision::None)
            }
            ClaimedPredecessorInspection::ExactUnboundNeedsInspection { intent } => {
                if !matches!(action, Action::ServiceRemove { .. }) {
                    return Err(scope_state_conflict(
                        "an unbound predecessor must be removed before a successor is created",
                    ));
                }
                match self
                    .runtime
                    .inspect_container_reservation(&intent.scope, &intent.requested_container_id)?
                {
                    ContainerGenerationInspection::Absent => {
                        Ok(ClaimedPreflightDecision::UnboundAbsent {
                            claim: claim.clone(),
                        })
                    }
                    ContainerGenerationInspection::ReservedUnpublished(ownership) => {
                        let binding = exact_binding_for_intent(&intent, ownership.clone())?;
                        Ok(ClaimedPreflightDecision::UnboundOwned {
                            claim: claim.clone(),
                            intent,
                            binding,
                            inspection: ContainerGenerationInspection::ReservedUnpublished(
                                ownership,
                            ),
                        })
                    }
                    ContainerGenerationInspection::Published(ownership) => {
                        let binding = exact_binding_for_intent(&intent, ownership.clone())?;
                        Ok(ClaimedPreflightDecision::UnboundOwned {
                            claim: claim.clone(),
                            intent,
                            binding,
                            inspection: ContainerGenerationInspection::Published(ownership),
                        })
                    }
                    other => Err(scope_state_conflict(format!(
                        "claimed unbound predecessor ownership is unsafe: {other:?}"
                    ))),
                }
            }
            ClaimedPredecessorInspection::ExactBoundNeedsCleanup { intent, binding }
            | ClaimedPredecessorInspection::ExactBoundCleanupPending { intent, binding } => {
                let inspection = self
                    .runtime
                    .inspect_container_generation(&binding.ownership)?;
                validate_exact_inspection(&binding.ownership, &inspection)?;
                Ok(ClaimedPreflightDecision::BoundCleanup {
                    claim: claim.clone(),
                    intent,
                    binding,
                    inspection,
                })
            }
        }?;
        if !claim_linked_successor
            && matches!(
                action,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        {
            self.inspect_fresh_claimed_successor_reservation(spec, action, claim, &decision)?;
        }
        Ok(decision)
    }

    fn inspect_fresh_claimed_successor_reservation(
        &self,
        spec: &StackSpec,
        action: &Action,
        claim: &ReconcileActionClaim,
        predecessor: &ClaimedPreflightDecision,
    ) -> Result<(), StackError> {
        let target = action.target();
        let service = spec
            .services
            .iter()
            .find(|service| service.name == target.service_name)
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "action target references unknown service `{}`",
                    target.service_name
                ))
            })?;
        let replicas = service.resources.replicas.max(1);
        if target.index() > replicas {
            return Err(StackError::InvalidSpec(format!(
                "action target `{}` exceeds service replica count {replicas}",
                target.display_name()
            )));
        }
        let requested_container_id = if let Some(base_name) = service.container_name.as_deref() {
            if replicas > 1 && target.index() > 1 {
                format!("{base_name}-{}", target.index())
            } else {
                base_name.to_string()
            }
        } else {
            generated_runtime_container_id(&spec.name, &target.service_name, target.index())
        };
        let scope = self
            .store
            .preview_claimed_successor_reservation(claim, &requested_container_id)?;
        match self
            .runtime
            .inspect_container_reservation(&scope, &requested_container_id)?
        {
            ContainerGenerationInspection::Absent => Ok(()),
            // A recreate normally reuses its predecessor's runtime ID. The
            // successor scope therefore sees that still-live exact predecessor
            // as foreign until pass two removes it. It is safe to admit only
            // when the earlier exact-generation inspection captured that same
            // ID; cleanup remains generation-qualified and fences a race.
            ContainerGenerationInspection::Foreign
                if matches!(
                    predecessor,
                    ClaimedPreflightDecision::BoundCleanup { binding, .. }
                        if binding.ownership.container_id == requested_container_id
                ) =>
            {
                Ok(())
            }
            other => Err(scope_state_conflict(format!(
                "fresh claimed successor reservation is unsafe before admission: {other:?}"
            ))),
        }
    }

    fn apply_claimed_preflight_decision(
        &mut self,
        decision: ClaimedPreflightDecision,
    ) -> Result<(), StackError> {
        match decision {
            ClaimedPreflightDecision::None => Ok(()),
            ClaimedPreflightDecision::UnboundAbsent { claim } => {
                self.store.publish_claimed_unbound_predecessor_failure(
                    &claim,
                    "claimed remove confirmed the exact reservation absent",
                    unix_now(),
                )?;
                Ok(())
            }
            ClaimedPreflightDecision::UnboundOwned {
                claim,
                intent,
                binding,
                inspection,
            } => {
                let binding = self
                    .store
                    .bind_claimed_predecessor_for_cleanup(&claim, &binding)?;
                self.cleanup_claimed_predecessor_with_inspection(
                    &claim, &intent, &binding, inspection,
                )
            }
            ClaimedPreflightDecision::BoundCleanup {
                claim,
                intent,
                binding,
                inspection,
            } => self
                .cleanup_claimed_predecessor_with_inspection(&claim, &intent, &binding, inspection),
        }
    }

    fn cleanup_claimed_predecessor_with_inspection(
        &mut self,
        claim: &ReconcileActionClaim,
        intent: &StackContainerCreateIntent,
        binding: &StackContainerGenerationBinding,
        inspection: ContainerGenerationInspection,
    ) -> Result<(), StackError> {
        self.store
            .begin_claimed_predecessor_cleanup(claim, unix_now())?;
        #[cfg(feature = "e2e-test-hooks")]
        let cleanup_outcome = match &inspection {
            ContainerGenerationInspection::Absent => "already_absent",
            ContainerGenerationInspection::ReservedUnpublished(_) => "reservation_released",
            ContainerGenerationInspection::Published(_) => "stopped_and_removed",
            ContainerGenerationInspection::Foreign => "foreign",
            ContainerGenerationInspection::Replacement => "replacement",
            ContainerGenerationInspection::LegacyUnscoped => "legacy_unscoped",
            ContainerGenerationInspection::Malformed(_) => "malformed",
        };
        self.cleanup_exact_runtime_generation(intent, &binding.ownership, inspection)?;
        #[cfg(feature = "e2e-test-hooks")]
        crate::teardown_e2e_boundary(
            "service_runtime_cleanup",
            &intent.scope.stack_id,
            self.teardown_e2e_operation_id
                .as_deref()
                .unwrap_or("unknown"),
            Some(&format!("{}#{}", intent.service_name, intent.replica_index)),
            serde_json::json!({
                "container_id": binding.ownership.container_id,
                "outcome": cleanup_outcome,
                "cleanup_progress_persisted": false,
            }),
        );
        self.store
            .complete_claimed_predecessor_cleanup(claim, unix_now())?;
        #[cfg(feature = "e2e-test-hooks")]
        crate::teardown_e2e_boundary(
            "service_cleanup_committed",
            &intent.scope.stack_id,
            self.teardown_e2e_operation_id
                .as_deref()
                .unwrap_or("unknown"),
            Some(&format!("{}#{}", intent.service_name, intent.replica_index)),
            serde_json::json!({
                "container_id": binding.ownership.container_id,
                "cleanup_progress_persisted": true,
            }),
        );
        Ok(())
    }

    fn admit_claimed_activation(
        &mut self,
        claim: &ReconcileActionClaim,
        intent: &StackContainerCreateIntent,
    ) -> Result<Option<ContainerGenerationOwnership>, StackError> {
        let binding = self
            .store
            .load_stack_container_generation_binding(&intent.scope.reservation_id)?;
        match intent.status {
            StackContainerCreateStatus::Running => {
                let binding = binding.ok_or_else(|| {
                    StackError::InvalidSpec("Running successor is missing binding".to_string())
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
                    other => Err(scope_state_conflict(format!(
                        "claimed Running successor does not match runtime: {other:?}"
                    ))),
                }
            }
            StackContainerCreateStatus::Intent => {
                if binding.is_some() {
                    return Err(scope_state_conflict(
                        "claimed Intent successor unexpectedly has a binding",
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
                    ContainerGenerationInspection::ReservedUnpublished(found) => found,
                    ContainerGenerationInspection::Published(found) => {
                        let binding = exact_binding_for_intent(intent, found)?;
                        self.store
                            .bind_claimed_successor_generation(claim, &binding)?;
                        self.fail_and_cleanup_claimed_successor(
                            claim,
                            intent,
                            "successor was published without journal Running proof",
                        )?;
                        return Err(scope_state_conflict(
                            "published successor orphan was cleaned",
                        ));
                    }
                    other => {
                        return Err(scope_state_conflict(format!(
                            "claimed successor reservation is unsafe: {other:?}"
                        )));
                    }
                };
                let binding = exact_binding_for_intent(intent, ownership)?;
                let binding = self
                    .store
                    .bind_claimed_successor_generation(claim, &binding)?;
                Ok(Some(binding.ownership))
            }
            StackContainerCreateStatus::Reserved => {
                let binding = binding.ok_or_else(|| {
                    StackError::InvalidSpec("Reserved successor is missing binding".to_string())
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
                        self.fail_and_cleanup_claimed_successor(
                            claim,
                            intent,
                            "successor was published without journal Running proof",
                        )?;
                        Err(scope_state_conflict(
                            "published successor orphan was cleaned",
                        ))
                    }
                    ContainerGenerationInspection::Absent => {
                        self.fail_and_cleanup_claimed_successor(
                            claim,
                            intent,
                            "reserved successor disappeared before activation",
                        )?;
                        Err(scope_state_conflict("reserved successor disappeared"))
                    }
                    other => Err(scope_state_conflict(format!(
                        "bound successor ownership is unsafe: {other:?}"
                    ))),
                }
            }
            StackContainerCreateStatus::Blocked | StackContainerCreateStatus::CleanupPending => {
                let binding = binding.ok_or_else(|| {
                    StackError::InvalidSpec("nonterminal successor is missing binding".to_string())
                })?;
                self.cleanup_claimed_successor(claim, intent, &binding)?;
                Err(scope_state_conflict(
                    "prior claimed activation required cleanup",
                ))
            }
            StackContainerCreateStatus::Cleaned | StackContainerCreateStatus::Failed => Err(
                scope_state_conflict("terminal claimed successor cannot be resumed"),
            ),
        }
    }

    fn fail_and_cleanup_claimed_successor(
        &mut self,
        claim: &ReconcileActionClaim,
        intent: &StackContainerCreateIntent,
        reason: &str,
    ) -> Result<(), StackError> {
        self.store.publish_claimed_successor_failure(
            claim,
            &intent.scope.reservation_id,
            reason,
            unix_now(),
        )?;
        let binding = self
            .store
            .load_stack_container_generation_binding(&intent.scope.reservation_id)?;
        if let Some(binding) = binding {
            self.cleanup_claimed_successor(claim, intent, &binding)?;
        }
        Ok(())
    }

    fn cleanup_claimed_successor(
        &mut self,
        claim: &ReconcileActionClaim,
        intent: &StackContainerCreateIntent,
        binding: &StackContainerGenerationBinding,
    ) -> Result<(), StackError> {
        let inspection = self
            .runtime
            .inspect_container_generation(&binding.ownership)?;
        match &inspection {
            ContainerGenerationInspection::Absent => {}
            ContainerGenerationInspection::ReservedUnpublished(found)
            | ContainerGenerationInspection::Published(found)
                if found == &binding.ownership => {}
            other => {
                return Err(scope_state_conflict(format!(
                    "claimed successor cleanup ownership is unsafe: {other:?}"
                )));
            }
        }
        self.store.begin_claimed_successor_cleanup(
            claim,
            &intent.scope.reservation_id,
            unix_now(),
        )?;
        self.cleanup_exact_runtime_generation(intent, &binding.ownership, inspection)?;
        self.store.complete_claimed_successor_cleanup(
            claim,
            &intent.scope.reservation_id,
            unix_now(),
        )?;
        Ok(())
    }

    fn cleanup_exact_runtime_generation(
        &self,
        intent: &StackContainerCreateIntent,
        ownership: &ContainerGenerationOwnership,
        inspection: ContainerGenerationInspection,
    ) -> Result<(), StackError> {
        match inspection {
            ContainerGenerationInspection::Absent => Ok(()),
            ContainerGenerationInspection::ReservedUnpublished(found) if found == *ownership => {
                self.runtime.release_container_reservation(found)?;
                Ok(())
            }
            ContainerGenerationInspection::Published(found) if found == *ownership => {
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
                Ok(())
            }
            other => Err(scope_state_conflict(format!(
                "exact cleanup inspection changed unexpectedly: {other:?}"
            ))),
        }
    }

    fn execute_claimed_remove(
        &mut self,
        claim: &ReconcileActionClaim,
        target: &ServiceReplicaKey,
    ) -> Result<(), StackError> {
        let release = self.store.release_claimed_allocator_target(claim)?;
        #[cfg(feature = "e2e-test-hooks")]
        crate::teardown_e2e_boundary(
            "allocator_released",
            self.workload_scope()
                .map_or("unknown", |scope| scope.stack_id.as_str()),
            self.teardown_e2e_operation_id
                .as_deref()
                .unwrap_or("unknown"),
            Some(&format!("{}#{}", target.service_name, target.index())),
            serde_json::json!({
                "already_released": release.already_released,
                "released_ports": release.released.ports.len(),
                "released_service_ip": release.released.service_ip,
                "released_network_ips": release.released.service_network_ips.len(),
            }),
        );
        #[cfg(not(feature = "e2e-test-hooks"))]
        let _ = release;
        self.ports.release_replica(target);
        self.service_ips.remove(target);
        self.service_network_ips.remove(target);
        Ok(())
    }
}

fn exact_binding_for_intent(
    intent: &StackContainerCreateIntent,
    ownership: ContainerGenerationOwnership,
) -> Result<StackContainerGenerationBinding, StackError> {
    if ownership.container_id != intent.requested_container_id
        || ownership.scope.as_deref() != Some(&intent.scope)
        || ownership.stack_id != intent.scope.stack_id
        || ownership.validate().is_err()
    {
        return Err(scope_state_conflict(
            "runtime returned ownership outside the exact reservation",
        ));
    }
    Ok(StackContainerGenerationBinding {
        reservation_id: intent.scope.reservation_id.clone(),
        service_name: intent.service_name.clone(),
        ownership,
        bound_at: unix_now().max(intent.updated_at),
    })
}

fn validate_exact_inspection(
    ownership: &ContainerGenerationOwnership,
    inspection: &ContainerGenerationInspection,
) -> Result<(), StackError> {
    match inspection {
        ContainerGenerationInspection::Absent => Ok(()),
        ContainerGenerationInspection::ReservedUnpublished(found)
        | ContainerGenerationInspection::Published(found)
            if found == ownership =>
        {
            Ok(())
        }
        other => Err(scope_state_conflict(format!(
            "claimed predecessor ownership no longer matches runtime: {other:?}"
        ))),
    }
}

fn exact_target_label(target: &ServiceReplicaKey) -> String {
    format!("{}#{}", target.service_name, target.replica_index)
}

fn record_action_error(
    result: &mut ExecutionResult,
    outcome_failures: &mut HashMap<ServiceReplicaKey, String>,
    target: &ServiceReplicaKey,
    error: StackError,
) {
    result.failed += 1;
    let message = record_execution_error(result, exact_target_label(target), &error);
    outcome_failures.entry(target.clone()).or_insert(message);
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn scoped_activation_payload_sha256(
    prepared: &PreparedCreate,
    spec: &StackSpec,
    secret_digests: &BTreeMap<String, String>,
) -> Result<String, StackError> {
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
    Ok(format!("{:x}", hasher.finalize()))
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

fn ensure_manifest_data_directory(path: &Path) -> Result<(), StackError> {
    let mut missing = Vec::new();
    let mut ancestor = path;
    loop {
        match std::fs::symlink_metadata(ancestor) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(ancestor.to_path_buf());
                ancestor = ancestor.parent().ok_or_else(|| {
                    StackError::InvalidSpec(
                        "scoped activation data path has no existing parent".to_string(),
                    )
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    if missing.is_empty() {
        return validate_private_directory(path);
    }

    // Refuse to traverse a symlink/non-directory anchor. The daemon's
    // configured runtime root may predate the private stack hierarchy and use
    // ordinary directory permissions; every missing component below that
    // trusted root is created privately one component at a time.
    validate_directory_anchor(ancestor)?;
    for directory in missing.iter().rev() {
        create_private_directory(directory)?;
    }
    validate_private_directory(path.parent().ok_or_else(|| {
        StackError::InvalidSpec("scoped activation data path has no stack root".to_string())
    })?)?;
    validate_private_directory(path)?;
    Ok(())
}

fn validate_directory_anchor(path: &Path) -> Result<(), StackError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(scope_state_conflict(
            "scoped activation data ancestor is not a real directory",
        ));
    }
    Ok(())
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
