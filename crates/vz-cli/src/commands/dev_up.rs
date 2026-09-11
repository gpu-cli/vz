//! Public Up is a streamed whole-Environment request, never legacy Run.
use super::runtime_daemon::{connect_up_daemon_for_state_db, default_state_db_path};
use clap::Args;
use serde::Serialize;
use serde_json::json;
use std::{collections::BTreeMap, env, fmt};
use vz_cli::developer_environment_context::{
    VZ_ENVIRONMENT_ID, discover_checked_out_branch, resolve_git_workspace,
};
use vz_cli::project_definition::discover_project_definition;
use vz_runtime_contract::{EnvironmentId, MachineError, fork_label_from_branch};
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
    /// Fork this Machine of the selected Environment instead of only reconciling.
    ///
    /// The fork is a new Machine in the SAME Environment, seeded from this one's
    /// disk, so it starts warm: dependencies installed, services running, Docker
    /// image store already populated.
    #[arg(long, value_name = "MACHINE")]
    pub fork_from: Option<String>,
    /// Address for the fork, `<machine>@<label>`. Defaults to this worktree's branch.
    #[arg(long = "as", value_name = "MACHINE@LABEL", requires = "fork_from")]
    pub fork_as: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UpCommandError {
    code: String,
    message: Box<str>,
    request_id: String,
    idempotency_key: String,
    details: BTreeMap<String, String>,
}
impl UpCommandError {
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
        code: code.into(),
        message: message.into_boxed_str(),
        request_id: request_id.clone(),
        idempotency_key: idempotency_key.clone(),
        details: BTreeMap::new(),
    };
    let original_error = |error: MachineError| UpCommandError {
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
    //
    // Resolving mints the token in memory and writes nothing: an Up the runtime
    // refuses must leave the worktree byte-identical, and this key is
    // persistent identity, so a token published behind a refusal would be a
    // binding artifact a later Up finds and adopts. It is published below, once
    // the daemon has reserved this Environment's identity.
    let pending_workspace = resolve_git_workspace(&cwd)
        .map_err(|error| local_error("workspace_read_failed", error.to_string()))?;
    // The fork address is resolved here rather than in the daemon because the
    // default comes from the caller's worktree, which the daemon cannot see. The
    // mapping from branch to label is a published rule (`fork_label_from_branch`)
    // so an agent can compute the name it will target without asking.
    let fork = args
        .fork_from
        .map(|fork_from| {
            let fork_as = match args.fork_as {
                Some(explicit) => explicit,
                None => {
                    let branch = discover_checked_out_branch(&cwd)
                        .map_err(|error| local_error("workspace_read_failed", error.to_string()))?
                        .ok_or_else(|| {
                            local_error(
                                "validation_error",
                                "this worktree has no checked-out branch to name the fork after; pass --as <machine>@<label>".into(),
                            )
                        })?;
                    let label = fork_label_from_branch(&branch).ok_or_else(|| {
                        local_error(
                            "validation_error",
                            format!("branch `{branch}` has no usable fork label; pass --as <machine>@<label>"),
                        )
                    })?;
                    format!("{fork_from}@{label}")
                }
            };
            let request = runtime_v2::MachineForkRequest { fork_from, fork_as };
            // Refuse a malformed address before starting the daemon: a fork the
            // caller could not later address is worse than no fork.
            vz_runtime_contract::MachineForkRequest {
                fork_from: request.fork_from.clone(),
                fork_as: request.fork_as.clone(),
            }
            .resolve()
            .map_err(|reason| local_error("validation_error", reason))?;
            Ok::<_, UpCommandError>(request)
        })
        .transpose()?;
    let workspace = pending_workspace.workspace().clone();
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
    // Read each declared binding's value out of THIS process's environment.
    //
    // The definition names the variable; the value never appears in it, and
    // never on a command line -- the gate's recorder writes every argv into a
    // receipt, so an argument is a published secret. A binding whose variable
    // is unset or empty fails here, before anything is admitted: delivering a
    // Machine an empty file where its definition promised a secret is worse
    // than refusing, because the Machine comes up looking correct.
    let mut secret_values = std::collections::HashMap::new();
    for binding in &discovered.definition.environment.secret_bindings {
        secret_values.insert(
            binding.name.clone(),
            resolve_secret_value(binding, &request_id, &idempotency_key)?,
        );
    }
    let mut stream = client
        .up_environment_stream(runtime_v2::UpEnvironmentRequest {
            secret_values,
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
            // Authoritative worktree root for workspace projections. Sent
            // canonicalised so the daemon hashes and resolves against a path
            // with no symlink components of its own.
            workspace_root: Some(
                workspace
                    .path_hint
                    .canonicalize()
                    .unwrap_or(workspace.path_hint)
                    .to_string_lossy()
                    .into_owned(),
            ),
            fork,
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
    let mut workspace_published = false;
    while let Some(event) = stream.next_event().await.map_err(client_error)? {
        // The first event carries the admission, which is the daemon saying it
        // reserved this Environment's identity in a durable transaction. That
        // is the earliest point at which publishing the token is not a
        // mutation behind a refusal, and the latest at which it is still
        // guaranteed: an Up that fails after admission still owns an
        // Environment this worktree must be able to name for status and delete.
        if !workspace_published {
            workspace_published = true;
            pending_workspace
                .commit()
                .map_err(|error| local_error("workspace_bind_failed", error.to_string()))?;
        }
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
        // The terminal receipt's failure is returned like every other refusal,
        // so `main` prints the same `{"schema_version":1,"error":{...}}`
        // envelope on stderr. A caller must not have to tell a refusal decided
        // before the stream (a gRPC status: an unsupported declaration) from one
        // decided inside it (this receipt: a host export port a sibling
        // Environment already holds). Both are Up failures, both carry the same
        // code, message and details, and an agent driving `--json up` reads
        // them from the same place.
        return Err(original_error(error));
    }
    if !json_output {
        println!(
            "Environment {} is Ready; identities and workspace binding are durable.",
            completion.admission.environment_id
        );
    }
    Ok(())
}

/// The bytes one declared SecretBinding resolves to on this host.
///
/// Failing closed is the whole contract here. A Machine handed an empty file
/// where its definition promised a secret comes up LOOKING correct, which is
/// strictly worse than an Up that refuses and says why -- the same failure
/// shape as a Machine reporting ready with a declared fabric port it never
/// configured.
///
/// The value is never put on a command line and never printed. The gate's
/// recorder writes every argv it runs into a receipt, so an argument is a
/// published secret; only the SOURCE's argv is ever named in an error.
fn resolve_secret_value(
    binding: &vz_runtime_contract::SecretBindingSpec,
    request_id: &str,
    idempotency_key: &str,
) -> Result<Vec<u8>, UpCommandError> {
    let refuse = |detail: String| UpCommandError {
        code: "validation_error".into(),
        message: format!(
            "SecretBinding `{}` for {}: {detail}",
            binding.name, binding.target_path
        )
        .into_boxed_str(),
        request_id: request_id.to_string(),
        idempotency_key: idempotency_key.to_string(),
        details: BTreeMap::new(),
    };
    if let Some(source_env) = &binding.source_env {
        let value = env::var(source_env).unwrap_or_default();
        if value.is_empty() {
            return Err(refuse(format!(
                "reads its value from `{source_env}`, which is unset or empty in this environment; \
                 set it before `vz up`"
            )));
        }
        return Ok(value.into_bytes());
    }
    let argv = binding
        .source_command
        .as_ref()
        .ok_or_else(|| refuse("declares neither `source_env` nor `source_command`".to_string()))?;
    let (program, arguments) = argv
        .split_first()
        .ok_or_else(|| refuse("declares an empty `source_command`".to_string()))?;
    // No shell: the program is spawned directly and the arguments are passed
    // verbatim, so nothing here is word-split, globbed, or able to become a
    // second command. stdin is closed because a secret tool that wants to
    // prompt must fail rather than hang an Up.
    let produced = std::process::Command::new(program)
        .args(arguments)
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|error| refuse(format!("could not run `{}`: {error}", argv.join(" "))))?;
    if !produced.status.success() {
        // stderr is quoted because a secret tool's refusal is the actionable
        // part ("not signed in", "item not found"); the VALUE only ever arrives
        // on stdout, which is not echoed here.
        let stderr = String::from_utf8_lossy(&produced.stderr);
        return Err(refuse(format!(
            "`{}` exited {}: {}",
            argv.join(" "),
            produced
                .status
                .code()
                .map_or_else(|| "by signal".to_string(), |code| code.to_string()),
            stderr.trim().chars().take(400).collect::<String>()
        )));
    }
    // A trailing newline is what every one of these tools prints and almost
    // never part of the secret, so exactly one is removed.
    let mut value = produced.stdout;
    if value.last() == Some(&b'\n') {
        value.pop();
        if value.last() == Some(&b'\r') {
            value.pop();
        }
    }
    if value.is_empty() {
        return Err(refuse(format!(
            "`{}` succeeded but printed nothing; an empty secret is a Machine that comes up \
             looking correct while holding nothing",
            argv.join(" ")
        )));
    }
    Ok(value)
}
