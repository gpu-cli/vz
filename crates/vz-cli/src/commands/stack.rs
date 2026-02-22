//! `vz stack` — multi-service stack lifecycle commands.
//!
//! Provides `up`, `down`, `ps`, `events`, `logs`, and `exec` subcommands
//! backed by the `vz-stack` control plane. The [`OciContainerRuntime`]
//! bridges the async `vz_oci::Runtime` to the sync [`ContainerRuntime`]
//! trait using `block_in_place` + `block_on`.
//!
//! ## Exec Architecture
//!
//! `vz stack up` (foreground mode) keeps the VM alive after convergence
//! and listens on a Unix socket at `~/.vz/stacks/<name>/control.sock`.
//! `vz stack exec` connects to that socket to execute commands inside
//! running service containers.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use tracing::info;

use vz_stack::{
    ApplyResult, ContainerLogs, ContainerRuntime, EventRecord, ExecutionResult,
    OrchestrationConfig, RoundReport, ServiceObservedState, ServicePhase, StackError, StackEvent,
    StackExecutor, StackOrchestrator, StackSpec, StateStore, parse_compose_with_dir,
};

/// Manage multi-service stacks from Compose files.
#[derive(Args, Debug)]
pub struct StackArgs {
    #[command(subcommand)]
    pub action: StackCommand,
}

#[derive(Subcommand, Debug)]
pub enum StackCommand {
    /// Start services defined in a compose file.
    Up(UpArgs),

    /// Stop and remove all services in a stack.
    Down(DownArgs),

    /// List services and their current status.
    Ps(PsArgs),

    /// Show stack lifecycle events.
    Events(EventsArgs),

    /// Show service logs (event history and container output).
    Logs(LogsArgs),

    /// Execute a command in a running service container.
    Exec(ExecArgs),

    /// Stop an individual service in a running stack.
    Stop(ServiceArgs),

    /// Start (recreate) an individual service in a running stack.
    Start(ServiceArgs),

    /// Restart an individual service in a running stack.
    Restart(ServiceArgs),
}

#[derive(Args, Debug)]
pub struct UpArgs {
    /// Path to compose YAML file.
    #[arg(short, long, default_value = "compose.yaml")]
    pub file: PathBuf,

    /// Stack name (defaults to parent directory name).
    #[arg(short = 'n', long)]
    pub name: Option<String>,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Show planned actions without executing them.
    #[arg(long)]
    pub dry_run: bool,

    /// Start services and return immediately without waiting for
    /// health checks to converge.
    #[arg(short, long)]
    pub detach: bool,
}

#[derive(Args, Debug)]
pub struct DownArgs {
    /// Stack name to stop.
    pub name: String,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Show planned actions without executing them.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug)]
pub struct PsArgs {
    /// Stack name to inspect.
    pub name: String,

    /// Output as JSON.
    #[arg(long)]
    pub json: bool,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct EventsArgs {
    /// Stack name to inspect.
    pub name: String,

    /// Show events since this event ID.
    #[arg(long, default_value_t = 0)]
    pub since: i64,

    /// Output as JSON.
    #[arg(long)]
    pub json: bool,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Stack name to show logs for.
    pub name: String,

    /// Filter logs to a specific service.
    #[arg(short, long)]
    pub service: Option<String>,

    /// Follow log output (poll for new events).
    #[arg(short, long)]
    pub follow: bool,

    /// Number of recent events to show (0 = all).
    #[arg(short = 'n', long, default_value_t = 50)]
    pub tail: usize,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct ExecArgs {
    /// Stack name.
    pub name: String,

    /// Service to execute the command in.
    pub service: String,

    /// Command and arguments to execute.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct ServiceArgs {
    /// Stack name.
    pub name: String,

    /// Service to act on.
    pub service: String,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

/// Action types for control socket requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ControlAction {
    Exec,
    Stop,
    Start,
    Restart,
}

/// JSON protocol for control socket requests.
#[derive(Debug, Serialize, Deserialize)]
struct ControlRequest {
    #[serde(default = "default_action")]
    action: ControlAction,
    service: String,
    #[serde(default)]
    cmd: Vec<String>,
}

fn default_action() -> ControlAction {
    ControlAction::Exec
}

/// JSON protocol for exec responses over the control socket.
#[derive(Debug, Serialize, Deserialize)]
struct ControlResponse {
    exit_code: i32,
    stdout: String,
    stderr: String,
    error: Option<String>,
}

pub async fn run(args: StackArgs) -> anyhow::Result<()> {
    match args.action {
        StackCommand::Up(args) => cmd_up(args).await,
        StackCommand::Down(args) => cmd_down(args).await,
        StackCommand::Ps(args) => cmd_ps(args).await,
        StackCommand::Events(args) => cmd_events(args).await,
        StackCommand::Logs(args) => cmd_logs(args).await,
        StackCommand::Exec(args) => cmd_exec(args).await,
        StackCommand::Stop(args) => cmd_service_action(args, ControlAction::Stop).await,
        StackCommand::Start(args) => cmd_service_action(args, ControlAction::Start).await,
        StackCommand::Restart(args) => cmd_service_action(args, ControlAction::Restart).await,
    }
}

// ── OCI container runtime bridge ──────────────────────────────────

/// Bridges the async `vz_oci::Runtime` to the sync [`ContainerRuntime`] trait.
///
/// Each method uses `tokio::task::block_in_place` + `Handle::block_on`
/// to call async OCI runtime methods from within the synchronous
/// executor context.
struct OciContainerRuntime {
    runtime: vz_oci::Runtime,
    handle: tokio::runtime::Handle,
}

impl OciContainerRuntime {
    fn new(oci_data_dir: &Path) -> anyhow::Result<Self> {
        let config = vz_oci::RuntimeConfig {
            data_dir: oci_data_dir.to_path_buf(),
            ..Default::default()
        };
        let runtime = vz_oci::Runtime::new(config);
        let handle = tokio::runtime::Handle::current();
        Ok(Self { runtime, handle })
    }

    /// Execute a command in a running container and capture output.
    fn exec_with_output(
        &self,
        container_id: &str,
        cmd: Vec<String>,
    ) -> Result<vz::protocol::ExecOutput, StackError> {
        tokio::task::block_in_place(|| {
            let exec_config = vz_oci::ExecConfig {
                cmd,
                ..Default::default()
            };
            self.handle
                .block_on(self.runtime.exec_container(container_id, exec_config))
                .map_err(|e| StackError::Network(format!("exec failed: {e}")))
        })
    }
}

impl ContainerRuntime for OciContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.pull(image))
                .map(|id| id.0)
                .map_err(|e| StackError::Network(format!("pull failed: {e}")))
        })
    }

    fn create(&self, image: &str, config: vz_oci::RunConfig) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.create_container(image, config))
                .map_err(|e| StackError::Network(format!("create failed: {e}")))
        })
    }

    fn stop(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.stop_container(container_id, false))
                .map(|_| ())
                .map_err(|e| StackError::Network(format!("stop failed: {e}")))
        })
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.remove_container(container_id))
                .map_err(|e| StackError::Network(format!("remove failed: {e}")))
        })
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        tokio::task::block_in_place(|| {
            let exec_config = vz_oci::ExecConfig {
                cmd: command.to_vec(),
                ..Default::default()
            };
            self.handle
                .block_on(self.runtime.exec_container(container_id, exec_config))
                .map(|output| output.exit_code)
                .map_err(|e| StackError::Network(format!("exec failed: {e}")))
        })
    }

    fn boot_shared_vm(
        &self,
        stack_id: &str,
        ports: &[vz_oci::PortMapping],
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.boot_shared_vm(stack_id, ports.to_vec()))
                .map_err(|e| StackError::Network(format!("boot_shared_vm failed: {e}")))
        })
    }

    fn network_setup(
        &self,
        stack_id: &str,
        services: &[vz_oci::NetworkServiceConfig],
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.network_setup(stack_id, services.to_vec()))
                .map_err(|e| StackError::Network(format!("network_setup failed: {e}")))
        })
    }

    fn network_teardown(&self, stack_id: &str, service_names: &[String]) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.runtime
                        .network_teardown(stack_id, service_names.to_vec()),
                )
                .map_err(|e| StackError::Network(format!("network_teardown failed: {e}")))
        })
    }

    fn create_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: vz_oci::RunConfig,
    ) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.runtime
                        .create_container_in_stack(stack_id, image, config),
                )
                .map_err(|e| StackError::Network(format!("create_in_stack failed: {e}")))
        })
    }

    fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.shutdown_shared_vm(stack_id))
                .map_err(|e| StackError::Network(format!("shutdown_shared_vm failed: {e}")))
        })
    }

    fn has_shared_vm(&self, stack_id: &str) -> bool {
        tokio::task::block_in_place(|| self.handle.block_on(self.runtime.has_shared_vm(stack_id)))
    }

    fn logs(&self, container_id: &str) -> Result<ContainerLogs, StackError> {
        let output = self.exec_with_output(
            container_id,
            vec![
                "tail".into(),
                "-n".into(),
                "100".into(),
                vz_oci::CONTAINER_LOG_FILE.into(),
            ],
        )?;
        Ok(ContainerLogs {
            output: if output.exit_code == 0 {
                output.stdout
            } else {
                String::new()
            },
        })
    }
}

// ── up ─────────────────────────────────────────────────────────────

async fn cmd_up(args: UpArgs) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&args.file)
        .with_context(|| format!("failed to read compose file: {}", args.file.display()))?;

    let compose_dir = args
        .file
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let stack_name = resolve_stack_name(args.name.as_deref(), &args.file)?;
    let spec = parse_compose_with_dir(&yaml, &stack_name, &compose_dir)
        .with_context(|| "failed to parse compose file")?;

    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &spec.name)?;
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("failed to create state directory: {}", state_dir.display()))?;

    let db_path = state_dir.join("state.db");

    info!(
        stack = %spec.name,
        services = spec.services.len(),
        "applying stack"
    );

    if args.dry_run {
        let store = StateStore::open(&db_path)
            .with_context(|| format!("failed to open state store: {}", db_path.display()))?;
        let health_statuses = HashMap::new();
        let result = vz_stack::apply(&spec, &store, &health_statuses)
            .with_context(|| "stack apply failed")?;
        print_apply_result(&result);
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    // Set up runtime, executor, and orchestrator.
    let oci_runtime =
        OciContainerRuntime::new(&state_dir).with_context(|| "failed to initialize OCI runtime")?;

    let exec_store =
        StateStore::open(&db_path).with_context(|| "failed to open execution state store")?;
    let reconcile_store =
        StateStore::open(&db_path).with_context(|| "failed to open reconciliation state store")?;

    let executor = StackExecutor::new(oci_runtime, exec_store, &state_dir);

    if args.detach {
        // Detach mode: single apply+execute pass, return immediately.
        let mut orchestrator = StackOrchestrator::new(
            executor,
            reconcile_store,
            OrchestrationConfig {
                max_rounds: 1,
                ..Default::default()
            },
        );

        let result = orchestrator
            .run(
                &spec,
                Some(&mut |report: &RoundReport| {
                    if let Some(ref apply) = report.apply_result.actions.first() {
                        let _ = apply; // suppress unused warning
                        print_apply_result(&report.apply_result);
                    }
                    if let Some(ref exec) = report.exec_result {
                        print_execution_result(exec);
                    }
                }),
            )
            .with_context(|| "stack up failed")?;

        println!(
            "\nDetached: {} ready, {} failed.",
            result.services_ready, result.services_failed
        );
    } else {
        // Foreground mode: full orchestration loop until convergence,
        // then keep the VM alive with a control socket for `vz stack exec`.
        let mut orchestrator =
            StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

        let result = orchestrator
            .run(
                &spec,
                Some(&mut |report: &RoundReport| {
                    print_round_report(report);
                }),
            )
            .with_context(|| "stack orchestration failed")?;

        if result.converged {
            println!(
                "\nStack converged in {} round(s): {} ready, {} failed.",
                result.rounds, result.services_ready, result.services_failed
            );
        } else {
            println!(
                "\nStack did not converge after {} rounds: {} ready, {} failed.",
                result.rounds, result.services_ready, result.services_failed
            );
        }

        if result.services_failed > 0 {
            bail!("{} service(s) failed", result.services_failed);
        }

        if !result.converged {
            bail!("stack did not converge");
        }

        // Keep the VM alive and listen for exec requests until ctrl-C.
        let sock_path = state_dir.join("control.sock");
        serve_control_socket(&sock_path, &spec, &mut orchestrator).await?;

        // Teardown on exit.
        info!(stack = %spec.name, "shutting down stack");
        let teardown_store = StateStore::open(&db_path)
            .with_context(|| "failed to open teardown state store")?;
        let empty_spec = StackSpec {
            name: spec.name.clone(),
            services: vec![],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
        };
        let health_statuses = HashMap::new();
        let teardown_actions = vz_stack::apply(&empty_spec, &teardown_store, &health_statuses)
            .with_context(|| "teardown apply failed")?;
        if !teardown_actions.actions.is_empty() {
            let mut teardown_executor = StackExecutor::new(
                OciContainerRuntime::new(&state_dir)
                    .with_context(|| "failed to create teardown runtime")?,
                StateStore::open(&db_path)
                    .with_context(|| "failed to open teardown exec store")?,
                &state_dir,
            );
            let _ = teardown_executor.execute(&empty_spec, &teardown_actions.actions);
        }
        println!("\nStack stopped.");
    }

    Ok(())
}

// ── control socket (for exec) ─────────────────────────────────────

/// Listen on a Unix socket for exec requests until ctrl-C.
///
/// The socket accepts one connection at a time. Each connection sends
/// a JSON [`ControlRequest`] (newline-terminated) and receives a JSON
/// [`ControlResponse`] (newline-terminated) back.
async fn serve_control_socket(
    sock_path: &Path,
    spec: &StackSpec,
    orchestrator: &mut StackOrchestrator<OciContainerRuntime>,
) -> anyhow::Result<()> {
    use tokio::net::UnixListener;

    // Clean up stale socket from a previous run.
    let _ = std::fs::remove_file(sock_path);

    let listener = UnixListener::bind(sock_path)
        .with_context(|| format!("failed to bind control socket: {}", sock_path.display()))?;

    println!("Listening for exec requests on {}", sock_path.display());
    println!("Press Ctrl+C to stop.\n");

    // Wait for ctrl-C in parallel with accepting connections.
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("ctrl-C received, shutting down");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        handle_control_connection(stream, spec, orchestrator).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to accept connection");
                    }
                }
            }
        }
    }

    // Remove socket file on exit.
    let _ = std::fs::remove_file(sock_path);
    Ok(())
}

/// Handle a single control socket connection (exec, stop, start, restart).
async fn handle_control_connection(
    stream: tokio::net::UnixStream,
    spec: &StackSpec,
    orchestrator: &mut StackOrchestrator<OciContainerRuntime>,
) {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let line = match lines.next_line().await {
        Ok(Some(line)) => line,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(error = %e, "failed to read from control socket");
            return;
        }
    };

    let request: ControlRequest = match serde_json::from_str(&line) {
        Ok(req) => req,
        Err(e) => {
            let resp = ControlResponse {
                exit_code: 1,
                stdout: String::new(),
                stderr: String::new(),
                error: Some(format!("invalid request: {e}")),
            };
            let _ = write_response(&mut writer, &resp).await;
            return;
        }
    };

    let resp = match request.action {
        ControlAction::Exec => handle_exec(spec, orchestrator, &request),
        ControlAction::Stop => handle_service_stop(spec, orchestrator, &request.service),
        ControlAction::Start => handle_service_start(spec, orchestrator, &request.service),
        ControlAction::Restart => {
            let stop_resp = handle_service_stop(spec, orchestrator, &request.service);
            if stop_resp.error.is_some() {
                stop_resp
            } else {
                handle_service_start(spec, orchestrator, &request.service)
            }
        }
    };

    let _ = write_response(&mut writer, &resp).await;
}

/// Handle an exec action: run a command inside a service container.
fn handle_exec(
    spec: &StackSpec,
    orchestrator: &StackOrchestrator<OciContainerRuntime>,
    request: &ControlRequest,
) -> ControlResponse {
    let store = orchestrator.executor().store();
    let observed = match store.load_observed_state(&spec.name) {
        Ok(o) => o,
        Err(e) => {
            return ControlResponse {
                exit_code: 1,
                stdout: String::new(),
                stderr: String::new(),
                error: Some(format!("failed to load state: {e}")),
            };
        }
    };

    let container_id = match observed
        .iter()
        .find(|o| o.service_name == request.service)
        .and_then(|s| s.container_id.as_deref())
    {
        Some(id) => id,
        None => {
            return ControlResponse {
                exit_code: 1,
                stdout: String::new(),
                stderr: String::new(),
                error: Some(format!("service '{}' is not running", request.service)),
            };
        }
    };

    info!(
        service = %request.service,
        container = %container_id,
        cmd = ?request.cmd,
        "exec request"
    );

    let runtime = orchestrator.executor().runtime();
    match runtime.exec_with_output(container_id, request.cmd.clone()) {
        Ok(output) => ControlResponse {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
            error: None,
        },
        Err(e) => ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(format!("{e}")),
        },
    }
}

/// Handle a stop action: remove an individual service from the running stack.
fn handle_service_stop(
    spec: &StackSpec,
    orchestrator: &mut StackOrchestrator<OciContainerRuntime>,
    service_name: &str,
) -> ControlResponse {
    info!(service = %service_name, "stop request");

    let actions = vec![vz_stack::Action::ServiceRemove {
        service_name: service_name.to_string(),
    }];

    match orchestrator.executor_mut().execute(spec, &actions) {
        Ok(result) if result.all_succeeded() => ControlResponse {
            exit_code: 0,
            stdout: format!("service '{service_name}' stopped\n"),
            stderr: String::new(),
            error: None,
        },
        Ok(result) => ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(format!(
                "failed to stop service '{}': {}",
                service_name,
                result
                    .errors
                    .iter()
                    .map(|(_, e)| e.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        },
        Err(e) => ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(format!("stop failed: {e}")),
        },
    }
}

/// Handle a start action: (re)create an individual service from the spec.
fn handle_service_start(
    spec: &StackSpec,
    orchestrator: &mut StackOrchestrator<OciContainerRuntime>,
    service_name: &str,
) -> ControlResponse {
    // Verify the service exists in the spec.
    if !spec.services.iter().any(|s| s.name == service_name) {
        return ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(format!(
                "service '{service_name}' not found in stack spec"
            )),
        };
    }

    info!(service = %service_name, "start request");

    let actions = vec![vz_stack::Action::ServiceCreate {
        service_name: service_name.to_string(),
    }];

    match orchestrator.executor_mut().execute(spec, &actions) {
        Ok(result) if result.all_succeeded() => ControlResponse {
            exit_code: 0,
            stdout: format!("service '{service_name}' started\n"),
            stderr: String::new(),
            error: None,
        },
        Ok(result) => ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(format!(
                "failed to start service '{}': {}",
                service_name,
                result
                    .errors
                    .iter()
                    .map(|(_, e)| e.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        },
        Err(e) => ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some(format!("start failed: {e}")),
        },
    }
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &ControlResponse,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut json = serde_json::to_string(resp)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

// ── exec ──────────────────────────────────────────────────────────

/// Connect to a running `vz stack up` session and execute a command.
async fn cmd_exec(args: ExecArgs) -> anyhow::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let sock_path = state_dir.join("control.sock");

    if !sock_path.exists() {
        bail!(
            "stack '{}' is not running in foreground mode.\n\
             Start it with: vz stack up -f <compose.yaml>",
            args.name
        );
    }

    let stream = UnixStream::connect(&sock_path)
        .await
        .with_context(|| format!("failed to connect to control socket: {}", sock_path.display()))?;

    let (reader, mut writer) = stream.into_split();

    // Send the exec request.
    let request = ControlRequest {
        action: ControlAction::Exec,
        service: args.service,
        cmd: args.command,
    };
    let mut json = serde_json::to_string(&request)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;

    // Read the response.
    let mut lines = BufReader::new(reader).lines();
    let line = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow::anyhow!("control socket closed without response"))?;

    let resp: ControlResponse = serde_json::from_str(&line)
        .with_context(|| "failed to parse control response")?;

    if let Some(err) = resp.error {
        bail!("{err}");
    }

    // Print captured output.
    if !resp.stdout.is_empty() {
        print!("{}", resp.stdout);
    }
    if !resp.stderr.is_empty() {
        eprint!("{}", resp.stderr);
    }

    std::process::exit(resp.exit_code);
}

// ── service start/stop/restart ─────────────────────────────────────

/// Send a service-level action (stop/start/restart) through the control socket.
async fn cmd_service_action(args: ServiceArgs, action: ControlAction) -> anyhow::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let sock_path = state_dir.join("control.sock");

    if !sock_path.exists() {
        bail!(
            "stack '{}' is not running in foreground mode.\n\
             Start it with: vz stack up -f <compose.yaml>",
            args.name
        );
    }

    let stream = UnixStream::connect(&sock_path)
        .await
        .with_context(|| format!("failed to connect to control socket: {}", sock_path.display()))?;

    let (reader, mut writer) = stream.into_split();

    let request = ControlRequest {
        action,
        service: args.service,
        cmd: vec![],
    };
    let mut json = serde_json::to_string(&request)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;

    let mut lines = BufReader::new(reader).lines();
    let line = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow::anyhow!("control socket closed without response"))?;

    let resp: ControlResponse = serde_json::from_str(&line)
        .with_context(|| "failed to parse control response")?;

    if let Some(err) = resp.error {
        bail!("{err}");
    }

    if !resp.stdout.is_empty() {
        print!("{}", resp.stdout);
    }

    Ok(())
}

// ── down ───────────────────────────────────────────────────────────

async fn cmd_down(args: DownArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    // Load current desired state to get the stack name, then apply empty spec.
    let current = store
        .load_desired_state(&args.name)
        .with_context(|| "failed to load desired state")?;

    let stack_name = current
        .as_ref()
        .map(|s| s.name.clone())
        .unwrap_or_else(|| args.name.clone());

    // Apply an empty spec to trigger removal of all services.
    let empty_spec = StackSpec {
        name: stack_name.clone(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
    };

    info!(stack = %stack_name, "tearing down stack");

    let health_statuses = HashMap::new();
    let result = vz_stack::apply(&empty_spec, &store, &health_statuses)
        .with_context(|| "stack teardown failed")?;

    print_apply_result(&result);

    if result.actions.is_empty() {
        return Ok(());
    }

    if args.dry_run {
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    // Execute removal actions through the OCI runtime.
    let oci_runtime =
        OciContainerRuntime::new(&state_dir).with_context(|| "failed to initialize OCI runtime")?;

    let exec_store =
        StateStore::open(&db_path).with_context(|| "failed to open execution state store")?;

    let mut executor = StackExecutor::new(oci_runtime, exec_store, &state_dir);
    let exec_result = executor
        .execute(&empty_spec, &result.actions)
        .with_context(|| "teardown execution failed")?;

    print_execution_result(&exec_result);

    Ok(())
}

// ── ps ─────────────────────────────────────────────────────────────

async fn cmd_ps(args: PsArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    let observed = store
        .load_observed_state(&args.name)
        .with_context(|| "failed to load observed state")?;

    if args.json {
        let json = serde_json::to_string_pretty(&observed)
            .with_context(|| "failed to serialize observed state")?;
        println!("{json}");
    } else {
        print_ps_table(&observed);
    }

    Ok(())
}

// ── events ─────────────────────────────────────────────────────────

async fn cmd_events(args: EventsArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    let records = if args.since > 0 {
        store
            .load_events_since(&args.name, args.since)
            .with_context(|| "failed to load events")?
    } else {
        store
            .load_event_records(&args.name)
            .with_context(|| "failed to load events")?
    };

    if args.json {
        for record in &records {
            let json = serde_json::to_string(&record.event)
                .with_context(|| "failed to serialize event")?;
            println!("{json}");
        }
    } else {
        print_events_table(&records);
    }

    Ok(())
}

// ── logs ──────────────────────────────────────────────────────────

async fn cmd_logs(args: LogsArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let sock_path = state_dir.join("control.sock");

    if !sock_path.exists() {
        bail!(
            "stack `{}` is not running (no control socket at {})",
            args.name,
            sock_path.display()
        );
    }

    // Determine which services to fetch logs for.
    let services = match &args.service {
        Some(svc) => vec![svc.clone()],
        None => {
            let db_path = state_dir.join("state.db");
            let store =
                StateStore::open(&db_path).with_context(|| "failed to open state store")?;
            let observed = store
                .load_observed_state(&args.name)
                .with_context(|| "failed to load observed state")?;
            observed
                .into_iter()
                .filter(|o| o.phase == ServicePhase::Running)
                .map(|o| o.service_name)
                .collect()
        }
    };

    if services.is_empty() {
        bail!("no running services in stack `{}`", args.name);
    }

    let log_file = vz_oci::CONTAINER_LOG_FILE;
    let multi = services.len() > 1;

    // Initial fetch: bounded tail -n <count>.
    for service in &services {
        let tail_n = args.tail.to_string();
        let output = exec_via_socket(
            &sock_path,
            service,
            &["tail", "-n", &tail_n, log_file],
        )
        .await?;
        print_log_output(&output, service, multi);
    }

    if !args.follow {
        return Ok(());
    }

    // Follow mode: track byte offsets per service, poll with tail -c +<offset>.
    // Get initial file sizes.
    let mut offsets: Vec<u64> = Vec::with_capacity(services.len());
    for service in &services {
        let size = get_file_size(&sock_path, service, log_file).await?;
        offsets.push(size);
    }

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        for (i, service) in services.iter().enumerate() {
            let offset_arg = format!("+{}", offsets[i] + 1);
            let output = exec_via_socket(
                &sock_path,
                service,
                &["tail", "-c", &offset_arg, log_file],
            )
            .await?;

            if !output.is_empty() {
                print_log_output(&output, service, multi);
                offsets[i] += output.len() as u64;
            }
        }
    }
}

/// Print log output, prefixing each line with service name for multi-service stacks.
fn print_log_output(output: &str, service: &str, multi: bool) {
    if output.is_empty() {
        return;
    }
    if multi {
        for line in output.lines() {
            println!("{service} | {line}");
        }
    } else {
        print!("{output}");
    }
}

/// Get file size inside a container via `wc -c`.
async fn get_file_size(
    sock_path: &Path,
    service: &str,
    path: &str,
) -> anyhow::Result<u64> {
    let output = exec_via_socket(sock_path, service, &["wc", "-c", path]).await?;
    // wc -c output: "  12345 /path/to/file\n"
    let size: u64 = output
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(size)
}

/// Execute a command in a service container via the control socket.
async fn exec_via_socket(
    sock_path: &Path,
    service: &str,
    cmd: &[&str],
) -> anyhow::Result<String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let stream = UnixStream::connect(sock_path)
        .await
        .with_context(|| format!("failed to connect to control socket: {}", sock_path.display()))?;

    let (reader, mut writer) = stream.into_split();

    let request = ControlRequest {
        action: ControlAction::Exec,
        service: service.to_string(),
        cmd: cmd.iter().map(|s| s.to_string()).collect(),
    };
    let mut json = serde_json::to_string(&request)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;

    let mut lines = BufReader::new(reader).lines();
    let line = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow::anyhow!("control socket closed without response"))?;

    let resp: ControlResponse =
        serde_json::from_str(&line).with_context(|| "failed to parse control response")?;

    if let Some(err) = resp.error {
        bail!("exec error for service '{service}': {err}");
    }

    Ok(resp.stdout)
}

/// Extract the service name from a stack event, if applicable.
#[cfg(test)]
fn event_service_name(event: &StackEvent) -> Option<&str> {
    match event {
        StackEvent::ServiceCreating { service_name, .. }
        | StackEvent::ServiceReady { service_name, .. }
        | StackEvent::ServiceStopping { service_name, .. }
        | StackEvent::ServiceStopped { service_name, .. }
        | StackEvent::ServiceFailed { service_name, .. }
        | StackEvent::PortConflict { service_name, .. }
        | StackEvent::HealthCheckPassed { service_name, .. }
        | StackEvent::HealthCheckFailed { service_name, .. }
        | StackEvent::DependencyBlocked { service_name, .. } => Some(service_name),
        StackEvent::StackApplyStarted { .. }
        | StackEvent::StackApplyCompleted { .. }
        | StackEvent::StackApplyFailed { .. }
        | StackEvent::VolumeCreated { .. }
        | StackEvent::StackDestroyed { .. } => None,
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Resolve the stack name from explicit flag or parent directory of compose file.
fn resolve_stack_name(
    explicit: Option<&str>,
    compose_path: &std::path::Path,
) -> anyhow::Result<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }

    // Use the parent directory name of the compose file.
    let parent = compose_path
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));

    parent.ok_or_else(|| anyhow::anyhow!("cannot determine stack name; use --name"))
}

/// Resolve the state directory for a stack.
fn resolve_state_dir(
    explicit: Option<&std::path::Path>,
    stack_name: &str,
) -> anyhow::Result<PathBuf> {
    if let Some(dir) = explicit {
        return Ok(dir.to_path_buf());
    }

    // Default: ~/.vz/stacks/<stack_name>/
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home)
        .join(".vz")
        .join("stacks")
        .join(stack_name))
}

fn print_round_report(report: &RoundReport) {
    if !report.apply_result.actions.is_empty() {
        print_apply_result(&report.apply_result);
    }

    if let Some(ref exec) = report.exec_result {
        print_execution_result(exec);
    }

    if let Some(ref health) = report.health_result {
        for name in &health.newly_ready {
            println!("  health ok  {name}");
        }
        for name in &health.newly_failed {
            println!("  health fail  {name}");
        }
    }

    if report.services_pending > 0 {
        println!(
            "  [{}/{} ready, {} pending]",
            report.services_ready,
            report.services_ready + report.services_pending + report.services_failed,
            report.services_pending
        );
    }
}

fn print_apply_result(result: &ApplyResult) {
    if result.actions.is_empty() && result.deferred.is_empty() {
        println!("No changes needed.");
        return;
    }

    for action in &result.actions {
        let verb = match action {
            vz_stack::Action::ServiceCreate { .. } => "create",
            vz_stack::Action::ServiceRecreate { .. } => "recreate",
            vz_stack::Action::ServiceRemove { .. } => "remove",
        };
        println!("  {verb:>10}  {}", action.service_name());
    }

    for deferred in &result.deferred {
        println!(
            "  deferred  {} (waiting on: {})",
            deferred.service_name,
            deferred.waiting_on.join(", "),
        );
    }

    println!(
        "\n{} action(s), {} deferred",
        result.actions.len(),
        result.deferred.len(),
    );
}

fn print_execution_result(result: &ExecutionResult) {
    if result.all_succeeded() {
        println!("\nAll {} action(s) succeeded.", result.succeeded);
    } else {
        println!(
            "\n{} succeeded, {} failed.",
            result.succeeded, result.failed
        );
        for (service, error) in &result.errors {
            println!("  error: {service}: {error}");
        }
    }
}

fn print_ps_table(observed: &[ServiceObservedState]) {
    if observed.is_empty() {
        println!("No services found.");
        return;
    }

    // Header.
    println!("{:<20} {:<14} {:<40}", "SERVICE", "STATUS", "CONTAINER ID");
    println!("{}", "-".repeat(74));

    for svc in observed {
        let status = match svc.phase {
            ServicePhase::Pending => "pending".to_string(),
            ServicePhase::Creating => "creating".to_string(),
            ServicePhase::Running if svc.ready => "running (ready)".to_string(),
            ServicePhase::Running => "running".to_string(),
            ServicePhase::Stopping => "stopping".to_string(),
            ServicePhase::Stopped => "stopped".to_string(),
            ServicePhase::Failed => "failed".to_string(),
        };
        let cid = svc.container_id.as_deref().unwrap_or("-");
        println!("{:<20} {:<14} {:<40}", svc.service_name, status, cid);
    }
}

fn print_events_table(records: &[EventRecord]) {
    if records.is_empty() {
        println!("No events found.");
        return;
    }

    println!("{:>6}  {:<24} EVENT", "ID", "TIME");
    println!("{}", "-".repeat(72));

    for record in records {
        let summary = format_event_summary(&record.event);
        println!("{:>6}  {:<24} {}", record.id, record.created_at, summary);
    }
}

fn format_event_summary(event: &StackEvent) -> String {
    match event {
        StackEvent::StackApplyStarted {
            stack_name,
            services_count,
        } => format!("apply started: {stack_name} ({services_count} services)"),
        StackEvent::StackApplyCompleted {
            succeeded, failed, ..
        } => format!("apply completed: {succeeded} ok, {failed} failed"),
        StackEvent::StackApplyFailed { error, .. } => format!("apply failed: {error}"),
        StackEvent::ServiceCreating { service_name, .. } => {
            format!("creating: {service_name}")
        }
        StackEvent::ServiceReady {
            service_name,
            runtime_id,
            ..
        } => format!("ready: {service_name} ({runtime_id})"),
        StackEvent::ServiceStopping { service_name, .. } => {
            format!("stopping: {service_name}")
        }
        StackEvent::ServiceStopped {
            service_name,
            exit_code,
            ..
        } => format!("stopped: {service_name} (exit {exit_code})"),
        StackEvent::ServiceFailed {
            service_name,
            error,
            ..
        } => format!("failed: {service_name}: {error}"),
        StackEvent::PortConflict {
            service_name, port, ..
        } => format!("port conflict: {service_name} port {port}"),
        StackEvent::VolumeCreated { volume_name, .. } => {
            format!("volume created: {volume_name}")
        }
        StackEvent::StackDestroyed { stack_name } => {
            format!("destroyed: {stack_name}")
        }
        StackEvent::HealthCheckPassed { service_name, .. } => {
            format!("health ok: {service_name}")
        }
        StackEvent::HealthCheckFailed {
            service_name,
            attempt,
            error,
            ..
        } => format!("health fail: {service_name} (attempt {attempt}): {error}"),
        StackEvent::DependencyBlocked {
            service_name,
            waiting_on,
            ..
        } => format!(
            "blocked: {service_name} waiting on {}",
            waiting_on.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn resolve_stack_name_explicit() {
        let name = resolve_stack_name(Some("myapp"), &PathBuf::from("compose.yaml")).unwrap();
        assert_eq!(name, "myapp");
    }

    #[test]
    fn format_event_summary_covers_all_variants() {
        let events = vec![
            StackEvent::StackApplyStarted {
                stack_name: "s".into(),
                services_count: 2,
            },
            StackEvent::StackApplyCompleted {
                stack_name: "s".into(),
                succeeded: 1,
                failed: 0,
            },
            StackEvent::StackApplyFailed {
                stack_name: "s".into(),
                error: "e".into(),
            },
            StackEvent::ServiceCreating {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::ServiceReady {
                stack_name: "s".into(),
                service_name: "web".into(),
                runtime_id: "ctr-1".into(),
            },
            StackEvent::ServiceStopping {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::ServiceStopped {
                stack_name: "s".into(),
                service_name: "web".into(),
                exit_code: 0,
            },
            StackEvent::ServiceFailed {
                stack_name: "s".into(),
                service_name: "web".into(),
                error: "oom".into(),
            },
            StackEvent::PortConflict {
                stack_name: "s".into(),
                service_name: "web".into(),
                port: 80,
            },
            StackEvent::VolumeCreated {
                stack_name: "s".into(),
                volume_name: "v".into(),
            },
            StackEvent::StackDestroyed {
                stack_name: "s".into(),
            },
            StackEvent::HealthCheckPassed {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::HealthCheckFailed {
                stack_name: "s".into(),
                service_name: "web".into(),
                attempt: 3,
                error: "timeout".into(),
            },
            StackEvent::DependencyBlocked {
                stack_name: "s".into(),
                service_name: "web".into(),
                waiting_on: vec!["db".into()],
            },
        ];

        for event in events {
            let summary = format_event_summary(&event);
            assert!(!summary.is_empty(), "empty summary for {event:?}");
        }
    }

    #[test]
    fn print_ps_table_empty() {
        // Just verify it doesn't panic.
        print_ps_table(&[]);
    }

    #[test]
    fn print_ps_table_with_services() {
        let observed = vec![
            ServiceObservedState {
                service_name: "web".into(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-abc".into()),
                last_error: None,
                ready: true,
            },
            ServiceObservedState {
                service_name: "db".into(),
                phase: ServicePhase::Pending,
                container_id: None,
                last_error: None,
                ready: false,
            },
        ];
        // Just verify it doesn't panic.
        print_ps_table(&observed);
    }

    #[test]
    fn print_events_table_empty() {
        print_events_table(&[]);
    }

    #[test]
    fn print_apply_result_empty() {
        let result = ApplyResult {
            actions: vec![],
            deferred: vec![],
        };
        print_apply_result(&result);
    }

    #[test]
    fn event_service_name_returns_name_for_service_events() {
        let event = StackEvent::ServiceCreating {
            stack_name: "s".into(),
            service_name: "web".into(),
        };
        assert_eq!(event_service_name(&event), Some("web"));

        let event = StackEvent::HealthCheckFailed {
            stack_name: "s".into(),
            service_name: "db".into(),
            attempt: 1,
            error: "timeout".into(),
        };
        assert_eq!(event_service_name(&event), Some("db"));
    }

    #[test]
    fn event_service_name_returns_none_for_stack_events() {
        let event = StackEvent::StackApplyStarted {
            stack_name: "s".into(),
            services_count: 2,
        };
        assert_eq!(event_service_name(&event), None);

        let event = StackEvent::VolumeCreated {
            stack_name: "s".into(),
            volume_name: "v".into(),
        };
        assert_eq!(event_service_name(&event), None);

        let event = StackEvent::StackDestroyed {
            stack_name: "s".into(),
        };
        assert_eq!(event_service_name(&event), None);
    }

    #[test]
    fn print_apply_result_with_actions() {
        let result = ApplyResult {
            actions: vec![
                vz_stack::Action::ServiceCreate {
                    service_name: "web".into(),
                },
                vz_stack::Action::ServiceRemove {
                    service_name: "old".into(),
                },
            ],
            deferred: vec![vz_stack::DeferredService {
                service_name: "app".into(),
                waiting_on: vec!["db".into()],
            }],
        };
        print_apply_result(&result);
    }

    #[test]
    fn control_request_serde_roundtrip() {
        let req = ControlRequest {
            action: ControlAction::Exec,
            service: "db".into(),
            cmd: vec!["psql".into(), "-U".into(), "app".into()],
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ControlRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, ControlAction::Exec);
        assert_eq!(parsed.service, "db");
        assert_eq!(parsed.cmd, vec!["psql", "-U", "app"]);
    }

    #[test]
    fn control_request_defaults_to_exec() {
        // Old-style request without action field should default to Exec.
        let json = r#"{"service":"web","cmd":["echo","hi"]}"#;
        let parsed: ControlRequest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.action, ControlAction::Exec);
    }

    #[test]
    fn control_request_stop_action() {
        let req = ControlRequest {
            action: ControlAction::Stop,
            service: "web".into(),
            cmd: vec![],
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ControlRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, ControlAction::Stop);
        assert_eq!(parsed.service, "web");
    }

    #[test]
    fn control_request_restart_action() {
        let req = ControlRequest {
            action: ControlAction::Restart,
            service: "cache".into(),
            cmd: vec![],
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ControlRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, ControlAction::Restart);
    }

    #[test]
    fn control_response_serde_roundtrip() {
        let resp = ControlResponse {
            exit_code: 0,
            stdout: "1 row\n".into(),
            stderr: String::new(),
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ControlResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.exit_code, 0);
        assert_eq!(parsed.stdout, "1 row\n");
        assert!(parsed.error.is_none());
    }

    #[test]
    fn control_response_with_error() {
        let resp = ControlResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: String::new(),
            error: Some("service 'web' is not running".into()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ControlResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.error.unwrap(), "service 'web' is not running");
    }

    #[tokio::test]
    async fn control_socket_roundtrip() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::{UnixListener, UnixStream};

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");

        let listener = UnixListener::bind(&sock_path).unwrap();

        // Spawn a server that echoes back a fixed response.
        let server_path = sock_path.clone();
        let server = tokio::spawn(async move {
            let _ = server_path; // keep the path alive
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let req: ControlRequest = serde_json::from_str(&line).unwrap();
            assert_eq!(req.service, "cache");
            assert_eq!(req.cmd, vec!["redis-cli", "PING"]);

            let resp = ControlResponse {
                exit_code: 0,
                stdout: "PONG\n".into(),
                stderr: String::new(),
                error: None,
            };
            let mut json = serde_json::to_string(&resp).unwrap();
            json.push('\n');
            writer.write_all(json.as_bytes()).await.unwrap();
            writer.flush().await.unwrap();
        });

        // Client sends a request and reads the response.
        let stream = UnixStream::connect(&sock_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();

        let req = ControlRequest {
            action: ControlAction::Exec,
            service: "cache".into(),
            cmd: vec!["redis-cli".into(), "PING".into()],
        };
        let mut json = serde_json::to_string(&req).unwrap();
        json.push('\n');
        writer.write_all(json.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut lines = BufReader::new(reader).lines();
        let line = lines.next_line().await.unwrap().unwrap();
        let resp: ControlResponse = serde_json::from_str(&line).unwrap();
        assert_eq!(resp.exit_code, 0);
        assert_eq!(resp.stdout, "PONG\n");
        assert!(resp.error.is_none());

        server.await.unwrap();
    }
}
