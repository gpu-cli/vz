//! Selected-Environment Delete through the daemon-owned streamed lifecycle.

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

use super::runtime_daemon::{connect_delete_daemon_for_state_db, default_state_db_path};

#[derive(Args, Debug)]
pub struct DevDeleteArgs {
    /// Environment name or immutable ID; otherwise use process/worktree selection.
    #[arg(long, value_name = "NAME_OR_ID")]
    pub environment: Option<String>,
    /// Machine quiescence deadline (1..300 seconds); owned cleanup continues independently.
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
pub struct DeleteCommandError {
    #[serde(skip)]
    emitted: bool,
    code: String,
    message: Box<str>,
    request_id: String,
    idempotency_key: String,
    details: BTreeMap<String, String>,
}

impl DeleteCommandError {
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
impl fmt::Display for DeleteCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for DeleteCommandError {}

pub async fn cmd_dev_delete(
    args: DevDeleteArgs,
    json_output: bool,
) -> Result<(), DeleteCommandError> {
    let replay_pair_valid = args.request_id.is_some() == args.idempotency_key.is_some();
    let token = uuid::Uuid::new_v4();
    let request_id = args
        .request_id
        .unwrap_or_else(|| format!("req-delete-{token}"));
    let idempotency_key = args
        .idempotency_key
        .unwrap_or_else(|| format!("delete-environment-{token}"));
    let local_error = |code: &str, message: String| DeleteCommandError {
        emitted: false,
        code: code.into(),
        message: message.into_boxed_str(),
        request_id: request_id.clone(),
        idempotency_key: idempotency_key.clone(),
        details: BTreeMap::new(),
    };
    let original_error = |error: MachineError| DeleteCommandError {
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
    if !replay_pair_valid || !(1..=300).contains(&args.timeout) {
        return Err(local_error(
            "validation_error",
            "Delete requires timeout 1..300 seconds and paired request/idempotency IDs".into(),
        ));
    }
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
    // Delete acts on the whole Environment and deliberately ignores VZ_MACHINE_ID.
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
            json!({"schema_version": 1, "record_type": "request_started", "operation": "delete_environment", "request_id": request_id, "idempotency_key": idempotency_key})
        );
    } else {
        println!(
            "Delete replay identity: --request-id {request_id} --idempotency-key {idempotency_key}"
        );
    }
    let mut client = connect_delete_daemon_for_state_db(&default_state_db_path())
        .await
        .map_err(|error| local_error("daemon_unavailable", error.to_string()))?;
    let mut stream = client
        .delete_environment_stream(runtime_v2::DeleteEnvironmentRequest {
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
                "terminal": event.terminal, "error": event.error, "tombstone": event.tombstone})
            );
        } else if !event.terminal {
            println!(
                "Deleting Environment {} (operation {})",
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
            "Delete stream omitted terminal receipt".into(),
        )
    })?;
    if let Some(error) = terminal.error {
        let mut error = original_error(error);
        error.emitted = json_output;
        return Err(error);
    }
    let tombstone = terminal.tombstone.ok_or_else(|| {
        local_error(
            "invalid_daemon_response",
            "Delete success omitted its exact tombstone".into(),
        )
    })?;
    if !json_output {
        println!(
            "Delete operation {} completed for Environment {} (generation {}); owned state deleted.",
            terminal.operation.operation_id,
            tombstone.environment_id,
            tombstone.lifecycle_generation
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct DeleteParser {
        #[command(flatten)]
        args: DevDeleteArgs,
    }

    #[test]
    fn delete_parser_requires_paired_replay_ids() {
        for arguments in [
            vec!["delete", "--request-id", "req-delete"],
            vec!["delete", "--idempotency-key", "idem-delete"],
        ] {
            assert!(DeleteParser::try_parse_from(arguments).is_err());
        }
        let parsed = DeleteParser::try_parse_from([
            "delete",
            "--request-id",
            "req-delete",
            "--idempotency-key",
            "idem-delete",
        ]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn delete_parser_bounds_timeout_and_has_no_machine_or_force_selector() {
        for timeout in ["0", "301", "18446744073709551615", "-1"] {
            assert!(DeleteParser::try_parse_from(["delete", "--timeout", timeout]).is_err());
        }
        for timeout in ["1", "300"] {
            assert!(DeleteParser::try_parse_from(["delete", "--timeout", timeout]).is_ok());
        }
        assert!(DeleteParser::try_parse_from(["delete", "--machine", "dev"]).is_err());
        assert!(DeleteParser::try_parse_from(["delete", "--force"]).is_err());
    }

    #[test]
    fn delete_error_preserves_replay_identity_and_original_machine_details() {
        for (code, exit_code) in [
            ("policy_denied", 3),
            ("timeout", 4),
            ("backend_unavailable", 5),
            ("daemon_unavailable", 5),
            ("state_conflict", 2),
        ] {
            let error = DeleteCommandError {
                emitted: true,
                code: code.into(),
                message: "owned cleanup unproven".into(),
                request_id: "req-delete".into(),
                idempotency_key: "idem-delete".into(),
                details: BTreeMap::from([("machine_id".into(), "mach-owned".into())]),
            };
            assert_eq!(error.exit_code(), exit_code);
            assert!(error.already_emitted());
            let wire: serde_json::Value =
                serde_json::from_str(&error.to_json()).expect("error JSON");
            assert_eq!(wire["schema_version"], 1);
            assert_eq!(wire["error"]["request_id"], "req-delete");
            assert_eq!(wire["error"]["idempotency_key"], "idem-delete");
            assert_eq!(wire["error"]["details"]["machine_id"], "mach-owned");
            assert!(wire["error"].get("emitted").is_none());
        }
    }
}
