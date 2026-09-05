//! Selected-Environment Stop through the daemon-owned streamed lifecycle.

use std::collections::BTreeMap;
use std::env;
use std::fmt;

use clap::Args;
use serde::Serialize;
use serde_json::json;
use vz_cli::developer_environment_context::{VZ_ENVIRONMENT_ID, discover_existing_git_workspace};
use vz_cli::project_definition::discover_project_definition;
use vz_runtime_contract::{EnvironmentId, MachineError};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClientError, environment_stop_error_detail};

use super::runtime_daemon::{connect_existing_daemon_for_state_db, default_state_db_path};

#[derive(Args, Debug)]
pub struct DevStopArgs {
    /// Environment name or immutable ID; otherwise use process/worktree selection.
    #[arg(long, value_name = "NAME_OR_ID")]
    pub environment: Option<String>,
    /// Physical Stop deadline per Machine (1..300 seconds).
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=300))]
    pub timeout: u64,
    /// Stable request ID to resume after response loss. Pair with --idempotency-key.
    #[arg(long, requires = "idempotency_key")]
    pub request_id: Option<String>,
    /// Stable mutation key to resume after response loss. Pair with --request-id.
    #[arg(long, requires = "request_id")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StopCommandError {
    #[serde(skip)]
    emitted: bool,
    code: String,
    message: Box<str>,
    request_id: String,
    idempotency_key: String,
    details: BTreeMap<String, String>,
}

impl StopCommandError {
    pub fn already_emitted(&self) -> bool {
        self.emitted
    }
    pub fn to_json(&self) -> String {
        json!({"schema_version": 1, "error": self}).to_string()
    }
    pub fn exit_code(&self) -> i32 {
        match self.code.as_str() {
            "policy_denied" => 3,
            "timeout" => 4,
            "backend_unavailable" | "daemon_unavailable" => 5,
            _ => 2,
        }
    }
}
impl fmt::Display for StopCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for StopCommandError {}

pub async fn cmd_dev_stop(args: DevStopArgs, json_output: bool) -> Result<(), StopCommandError> {
    let token = uuid::Uuid::new_v4();
    let request_id = args
        .request_id
        .unwrap_or_else(|| format!("req-stop-{token}"));
    let idempotency_key = args
        .idempotency_key
        .unwrap_or_else(|| format!("stop-environment-{token}"));
    let local_error = |code: &str, message: String| StopCommandError {
        emitted: false,
        code: code.into(),
        message: message.into_boxed_str(),
        request_id: request_id.clone(),
        idempotency_key: idempotency_key.clone(),
        details: BTreeMap::new(),
    };
    let original_error = |error: MachineError| StopCommandError {
        emitted: false,
        code: error.code.as_str().into(),
        message: error.message.into_boxed_str(),
        request_id: error.request_id.unwrap_or_else(|| request_id.clone()),
        idempotency_key: idempotency_key.clone(),
        details: error.details,
    };
    let client_error = |error: DaemonClientError| {
        if let Some(original) = environment_stop_error_detail(&error) {
            return original_error(original);
        }
        let code = match &error {
            DaemonClientError::Grpc(status) if status.code() == tonic::Code::DeadlineExceeded => {
                "timeout"
            }
            DaemonClientError::IncompatibleProtocol { .. } => "invalid_daemon_response",
            _ => "backend_unavailable",
        };
        local_error(code, error.to_string())
    };
    if [&request_id, &idempotency_key].iter().any(|value| {
        value.trim().is_empty()
            || value.trim() != value.as_str()
            || value.len() > 256
            || value.chars().any(char::is_control)
    }) {
        return Err(local_error("validation_error", "request/idempotency IDs must be nonempty, at most 256 bytes, and have no control characters or surrounding whitespace".into()));
    }
    let cwd = env::current_dir()
        .map_err(|error| local_error("definition_read_failed", error.to_string()))?;
    let discovered = discover_project_definition(&cwd)
        .map_err(|error| local_error(error.code(), error.to_string()))?;
    let process_environment_id = if args.environment.is_some() {
        None
    } else {
        env::var_os(VZ_ENVIRONMENT_ID)
            .map(|raw| {
                let value = raw.into_string().map_err(|_| {
                    local_error("invalid_selector", "VZ_ENVIRONMENT_ID must be UTF-8".into())
                })?;
                EnvironmentId::new(value)
                    .map(|id| id.to_string())
                    .map_err(|_| {
                        local_error(
                            "invalid_selector",
                            "VZ_ENVIRONMENT_ID must be an immutable Environment ID".into(),
                        )
                    })
            })
            .transpose()?
    };
    // Stop acts on the whole Environment and deliberately ignores VZ_MACHINE_ID.
    let workspace_key = if args.environment.is_some() || process_environment_id.is_some() {
        None
    } else {
        discover_existing_git_workspace(&cwd)
            .map_err(|error| local_error("workspace_read_failed", error.to_string()))?
            .map(|workspace| workspace.workspace_key)
    };
    if json_output {
        println!(
            "{}",
            json!({"schema_version": 1, "record_type": "request_started", "operation": "stop_environment", "request_id": request_id, "idempotency_key": idempotency_key})
        );
    } else {
        println!(
            "Stop replay identity: --request-id {request_id} --idempotency-key {idempotency_key}"
        );
    }
    let mut client = connect_existing_daemon_for_state_db(&default_state_db_path())
        .await
        .map_err(|error| local_error("daemon_unavailable", error.to_string()))?;
    let mut stream = client
        .stop_environment_stream(runtime_v2::StopEnvironmentRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.clone(),
                idempotency_key: idempotency_key.clone(),
                trace_id: String::new(),
            }),
            project_id: discovered.definition.project_id.to_string(),
            environment: args.environment,
            process_environment_id,
            workspace_key,
            machine_timeout_millis: args.timeout * 1000,
        })
        .await
        .map_err(client_error)?;
    let mut terminal = None;
    while let Some(event) = stream.next_event().await.map_err(client_error)? {
        if json_output {
            println!(
                "{}",
                json!({"schema_version": 1, "record_type": "operation_progress", "request_id": event.request_id, "idempotency_key": idempotency_key,
                "sequence": event.sequence, "operation": event.operation,
                "terminal": event.terminal, "error": event.error})
            );
        } else if !event.terminal {
            println!(
                "Stopping Environment {} (operation {})",
                event.operation.environment_id, event.operation.operation_id
            );
        }
        if event.terminal {
            terminal = Some(event);
        }
    }
    let terminal = terminal.ok_or_else(|| {
        local_error(
            "invalid_daemon_response",
            "Stop stream omitted terminal receipt".into(),
        )
    })?;
    if let Some(error) = terminal.error {
        let mut error = original_error(error);
        error.emitted = json_output;
        return Err(error);
    }
    if !json_output {
        println!(
            "Stop operation {} completed for Environment {} (generation {}); identities and persistent state preserved.",
            terminal.operation.operation_id,
            terminal.operation.environment_id,
            terminal.operation.generation
        );
    }
    Ok(())
}
