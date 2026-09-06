//! Retained Up supervisor. Observation cancellation never drops boot effects.
//! A deadline is an observation/next-effect bound, not permission to abandon an
//! in-flight backend future. Uncertain effects keep their original ownership.
use crate::machine_runtime_activation::MachineRuntimeActivation;
use crate::{RuntimeDaemon, current_unix_secs};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedMutexGuard, watch};
use vz_runtime_contract::*;
use vz_stack::StackError;

mod native_readiness;
mod readiness;
mod supervisor;
#[cfg(test)]
mod tests;

/// Exact, authorized boot boundary for trusted backend instrumentation. This
/// observer can delay dispatch but cannot supply activation/readiness evidence.
#[derive(Debug, Clone)]
pub struct EnvironmentUpBootBoundary {
    pub admission: EnvironmentUpAdmission,
    pub operation: EnvironmentLifecycleOperation,
    pub machine_id: MachineId,
    pub owner: ResourceOwner,
}

#[tonic::async_trait]
pub trait EnvironmentUpBootObserver: Send + Sync {
    async fn before_dispatch(&self, boundary: &EnvironmentUpBootBoundary);
}

#[derive(Default)]
pub(crate) struct EnvironmentUpRuns(Mutex<HashMap<String, Arc<UpRun>>>);

struct UpRun {
    admission: EnvironmentUpAdmission,
    progress: watch::Sender<EnvironmentUpProgress>,
    fence: Mutex<Option<Arc<OwnedMutexGuard<()>>>>,
    // Failed registration must not drop the only activation reader.
    uncertain: Mutex<Vec<Arc<MachineRuntimeActivation>>>,
}

impl UpRun {
    fn preparing(&self, preparation: EnvironmentPreparationProgress) {
        self.progress.send_modify(|event| {
            if event.completion.is_some() {
                return;
            }
            event.sequence += 1;
            event.phase = "preparing".into();
            event.preparation = Some(preparation);
        });
    }

    fn publish(
        &self,
        phase: &str,
        operation: Option<EnvironmentLifecycleOperation>,
        completion: Option<EnvironmentUpCompletion>,
    ) {
        self.progress.send_modify(|event| {
            if event.completion.is_some() {
                return;
            }
            event.sequence += 1;
            event.phase = phase.into();
            event.preparation = None;
            event.operation = operation;
            event.completion = completion;
        });
    }
}

fn failure(
    metadata: &RequestMetadata,
    code: MachineErrorCode,
    message: impl ToString,
) -> MachineError {
    MachineError::new(
        code,
        message.to_string().chars().take(2048).collect(),
        metadata.request_id.clone(),
        BTreeMap::from([("operation".into(), "up_environment".into())]),
    )
}

impl RuntimeDaemon {
    /// Install trusted instrumentation before publishing this daemon owner.
    /// There is no RPC/CLI setting and no alternate/fake readiness provider.
    pub fn with_environment_up_boot_observer(
        mut self,
        observer: Arc<dyn EnvironmentUpBootObserver>,
    ) -> Self {
        self.environment_up_observer = Some(observer);
        self
    }
    /// Admission authorizes exact generated IDs in the transaction that creates
    /// them. Exact retries observe one retained run or its immutable receipt.
    pub async fn up_environment(
        self: &Arc<Self>,
        mut request: EnvironmentUpRequest,
        metadata: RequestMetadata,
    ) -> Result<watch::Receiver<EnvironmentUpProgress>, MachineError> {
        if request.selection.explicit.is_some() {
            request.selection.process_environment_id = None;
        }
        let hash = request
            .request_hash()
            .map_err(|error| failure(&metadata, MachineErrorCode::ValidationError, error))?;
        validate_supported(&request, &metadata)?;
        let request_id = metadata.request_id.as_deref().unwrap_or_default();
        let key = metadata.idempotency_key.as_deref().unwrap_or_default();
        if [request_id, key].iter().any(|value| {
            value.is_empty()
                || value.len() > 256
                || value.trim() != *value
                || value.chars().any(char::is_control)
        }) {
            return Err(failure(
                &metadata,
                MachineErrorCode::ValidationError,
                "Up requires bounded request and idempotency IDs without control characters",
            ));
        }
        let mut runs = self
            .environment_up_runs
            .0
            .lock()
            .map_err(|error| failure(&metadata, MachineErrorCode::InternalError, error))?;
        if runs.len() >= 1024 && !runs.contains_key(key) {
            return Err(failure(
                &metadata,
                MachineErrorCode::BackendUnavailable,
                "Up supervisor capacity exhausted; no admission performed",
            ));
        }
        let admission = self
            .with_state_store(|store| {
                store.reserve_environment_up_admission(
                    &request.definition,
                    &request.selection,
                    request_id,
                    key,
                    &hash,
                    current_unix_secs(),
                    |environment| self.authorize_up(&metadata, environment),
                )
            })
            .map_err(|error| error.to_machine_error(&metadata))?;
        if let Some(existing) = runs.get(key) {
            if existing.admission != admission {
                return Err(failure(
                    &metadata,
                    MachineErrorCode::StateConflict,
                    "Up run admission mismatch",
                ));
            }
            return Ok(existing.progress.subscribe());
        }
        let completion = self
            .with_state_store(|store| store.load_environment_up_completion(key))
            .map_err(|error| error.to_machine_error(&metadata))?;
        let initial = EnvironmentUpProgress {
            preparation: None,
            schema_version: 1,
            sequence: 0,
            admission: admission.clone(),
            phase: if completion.is_some() {
                "terminal"
            } else {
                "admitted"
            }
            .into(),
            operation: completion
                .as_ref()
                .and_then(|value| value.operation.clone()),
            completion,
        };
        let (progress, receiver) = watch::channel(initial);
        if receiver.borrow().completion.is_some() {
            return Ok(receiver);
        }
        let run = Arc::new(UpRun {
            admission,
            progress,
            fence: Mutex::new(None),
            uncertain: Mutex::new(Vec::new()),
        });
        runs.insert(key.into(), Arc::clone(&run));
        let daemon = Arc::clone(self);
        tokio::spawn(async move {
            daemon.supervise_up(request, metadata, run).await;
        });
        Ok(receiver)
    }

    fn authorize_up(
        &self,
        metadata: &RequestMetadata,
        environment: &EnvironmentInstance,
    ) -> Result<(), StackError> {
        if environment.legacy_migration.is_some() || !environment.networks.is_empty() || !environment.endpoints.is_empty()
            || environment.ownership.iter().any(|record| !matches!(&record.resource_kind,
                OwnedResourceKind::Machine | OwnedResourceKind::Incarnation | OwnedResourceKind::Disk)
                && !matches!(&record.resource_kind,OwnedResourceKind::Other(kind) if kind=="machine_runtime_store" || kind=="runtime_vm")
                && !(record.resource_kind == OwnedResourceKind::DockerContext
                    && environment.machines.iter().any(|machine| machine.docker_context.as_ref().is_some_and(|context|
                        context.name == record.resource_id
                        && context.owner.environment_id == record.environment_id
                        && context.owner.machine_id == record.machine_id
                        && context.owner.project_id == environment.project_id
                        && context.owner.environment_id == environment.environment_id
                        && context.owner.machine_id.as_ref() == Some(&machine.machine_id))))) {
            return Err(StackError::Machine {code:MachineErrorCode::UnsupportedOperation,
                message:"Up cannot apply unknown or unsupported existing topology resources".into()});
        }
        let mut machine_ids = environment
            .machines
            .iter()
            .map(|machine| machine.machine_id.clone())
            .collect::<Vec<_>>();
        machine_ids.sort();
        let scope = TopologyAuthorization {
            operation: TopologyOperation::Up,
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            definition_digest: environment.definition_digest.clone(),
            machine_ids,
        };
        match self.policy_hook.evaluate_topology(&scope, metadata) {
            Ok(PolicyDecision::Allow) => Ok(()),
            Ok(PolicyDecision::Deny { reason }) => Err(StackError::Machine {
                code: MachineErrorCode::PolicyDenied,
                message: reason,
            }),
            Err(error) => Err(StackError::Machine {
                code: MachineErrorCode::BackendUnavailable,
                message: format!("topology policy failed: {error}"),
            }),
        }
    }
}

fn validate_supported(
    request: &EnvironmentUpRequest,
    metadata: &RequestMetadata,
) -> Result<(), MachineError> {
    let spec = &request.definition.environment;
    if spec.machines.is_empty() || spec.machines.len() > 128 {
        return Err(failure(
            metadata,
            MachineErrorCode::ValidationError,
            "Up requires 1..128 Machines",
        ));
    }
    if !spec.networks.is_empty()
        || !spec.endpoints.is_empty()
        || spec
            .machines
            .iter()
            .any(|machine| machine.workspace.is_some())
    {
        return Err(failure(
            metadata,
            MachineErrorCode::UnsupportedOperation,
            "declared network, endpoint and workspace projection adapters remain required; this Up cannot apply them and performs no admission",
        ));
    }
    for machine in &spec.machines {
        if !matches!(
            machine.target.os,
            OperatingSystem::Linux | OperatingSystem::Macos
        ) || machine.target.arch != Architecture::Aarch64
        {
            return Err(failure(
                metadata,
                MachineErrorCode::UnsupportedOperation,
                "this Up adapter supports Linux and native macOS ARM64 on Apple silicon only",
            ));
        }
    }
    if request
        .path_hint
        .as_ref()
        .is_some_and(|value| value.len() > 4096 || value.chars().any(char::is_control))
    {
        return Err(failure(
            metadata,
            MachineErrorCode::ValidationError,
            "invalid bounded workspace diagnostic path",
        ));
    }
    Ok(())
}
