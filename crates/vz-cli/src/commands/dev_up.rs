//! Public Up is a streamed whole-Environment request, never legacy Run.
use super::runtime_daemon::{connect_up_daemon_for_state_db, default_state_db_path};
use clap::Args;
use serde::Serialize;
use serde_json::json;
use std::{collections::BTreeMap, env, fmt};
use vz_cli::developer_environment_context::{VZ_ENVIRONMENT_ID, discover_git_workspace};
use vz_cli::project_definition::discover_project_definition;
use vz_runtime_contract::{EnvironmentId, MachineError};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClientError, environment_stop_error_detail};

#[derive(Args, Debug)]
pub struct DevUpArgs {
    /// Existing Environment ID/name, or a new project-unique name.
    #[arg(long, value_name = "NAME_OR_ID")]
    pub environment: Option<String>,
    /// Deadline in seconds. Defaults to 3600 for macOS image preparation, 300 for Linux.
    #[arg(long,value_parser=clap::value_parser!(u64).range(1..=3600))]
    pub timeout: Option<u64>,
    /// Exact request ID for response-loss replay, paired with --idempotency-key.
    #[arg(long, requires = "idempotency_key")]
    pub request_id: Option<String>,
    /// Exact mutation key for response-loss replay, paired with --request-id.
    #[arg(long, requires = "request_id")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UpCommandError {
    #[serde(skip)]
    emitted: bool,
    code: String,
    message: Box<str>,
    request_id: String,
    idempotency_key: String,
    details: BTreeMap<String, String>,
}
impl UpCommandError {
    pub fn already_emitted(&self) -> bool {
        self.emitted
    }
    pub fn to_json(&self) -> String {
        json!({"schema_version":1,"error":self}).to_string()
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
impl fmt::Display for UpCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for UpCommandError {}

pub async fn cmd_dev_up(args: DevUpArgs, json_output: bool) -> Result<(), UpCommandError> {
    let token = uuid::Uuid::new_v4();
    let request_id = args.request_id.unwrap_or_else(|| format!("req-up-{token}"));
    let idempotency_key = args
        .idempotency_key
        .unwrap_or_else(|| format!("up-environment-{token}"));
    let local_error = |code: &str, message: String| UpCommandError {
        emitted: false,
        code: code.into(),
        message: message.into_boxed_str(),
        request_id: request_id.clone(),
        idempotency_key: idempotency_key.clone(),
        details: BTreeMap::new(),
    };
    let original_error = |error: MachineError| UpCommandError {
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
        value.is_empty()
            || value.len() > 256
            || value.trim() != value.as_str()
            || value.chars().any(char::is_control)
    }) {
        return Err(local_error("validation_error","request/idempotency IDs must be bounded, nonempty, and free of controls/surrounding whitespace".into()));
    }
    let cwd = env::current_dir()
        .map_err(|error| local_error("definition_read_failed", error.to_string()))?;
    // A missing/invalid nearest definition never starts the daemon or writes a token.
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
                            "VZ_ENVIRONMENT_ID requires an immutable Environment ID".into(),
                        )
                    })
            })
            .transpose()?
    };
    // Up always creates/refreshes the calling worktree binding on success,
    // including explicit selection. Its random token is never a path-derived ID.
    let workspace = discover_git_workspace(&cwd)
        .map_err(|error| local_error("workspace_read_failed", error.to_string()))?;
    if json_output {
        println!(
            "{}",
            json!({"schema_version":1,"record_type":"request_started","operation":"up_environment","request_id":request_id,"idempotency_key":idempotency_key})
        );
    } else {
        println!(
            "Up replay identity: --request-id {request_id} --idempotency-key {idempotency_key}"
        );
    }
    let mut client = connect_up_daemon_for_state_db(&default_state_db_path())
        .await
        .map_err(|error| local_error("daemon_unavailable", error.to_string()))?;
    let mut stream = client
        .up_environment_stream(runtime_v2::UpEnvironmentRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.clone(),
                idempotency_key: idempotency_key.clone(),
                trace_id: String::new(),
            }),
            definition: Some(vz_runtime_translate::project_definition_to_proto(
                &discovered.definition,
            )),
            environment: args.environment,
            process_environment_id,
            workspace_key: Some(workspace.workspace_key),
            path_hint: Some(cwd.to_string_lossy().into_owned()),
            timeout_millis: args.timeout.unwrap_or_else(|| {
                if discovered
                    .definition
                    .environment
                    .machines
                    .iter()
                    .any(|m| m.target.os == vz_runtime_contract::OperatingSystem::Macos)
                {
                    3600
                } else {
                    300
                }
            }) * 1000,
        })
        .await
        .map_err(client_error)?;
    let preparation_bar = indicatif::ProgressBar::hidden();
    if !json_output {
        preparation_bar.set_draw_target(indicatif::ProgressDrawTarget::stderr());
        preparation_bar.set_style(
            indicatif::ProgressStyle::with_template("{msg} [{bar:30}] {percent}%")
                .map_err(|e| local_error("progress_failed", e.to_string()))?,
        );
    }
    let mut terminal = None;
    while let Some(event) = stream.next_event().await.map_err(client_error)? {
        if json_output {
            println!(
                "{}",
                json!({"schema_version":1,"record_type":"operation_progress","progress":event})
            );
        } else if let Some(progress) = &event.preparation {
            if preparation_bar.is_finished() {
                preparation_bar.reset();
            }
            preparation_bar.set_message(progress.label.clone());
            preparation_bar.set_length(progress.total);
            preparation_bar.set_position(progress.completed);
            if preparation_bar.is_hidden() {
                println!(
                    "{}: {:.0}%",
                    progress.label,
                    100.0 * progress.completed as f64 / progress.total as f64
                );
            }
        } else if event.completion.is_none() {
            preparation_bar.finish_and_clear();
            println!(
                "Environment {}: {}",
                event.admission.environment_id, event.phase
            );
        }
        if let Some(completion) = event.completion {
            terminal = Some(completion);
        }
    }
    preparation_bar.finish_and_clear();
    let completion = terminal.ok_or_else(|| {
        local_error(
            "invalid_daemon_response",
            "Up stream omitted terminal receipt".into(),
        )
    })?;
    if let Some(error) = completion.error {
        let mut error = original_error(error);
        error.emitted = json_output;
        return Err(error);
    }
    if !json_output {
        println!(
            "Environment {} is Ready; identities and workspace binding are durable.",
            completion.admission.environment_id
        );
    }
    Ok(())
}
