//! Stream ordinary execution into one selected, owned Developer Environment Machine.

use super::runtime_daemon::{connect_existing_daemon_for_state_db, default_state_db_path};
use base64::Engine;
use clap::Args;
use serde::Serialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    env, fmt,
    io::{IsTerminal, Write},
};
use tokio::io::AsyncReadExt;
use vz_cli::{
    developer_environment_context::{
        VZ_ENVIRONMENT_ID, VZ_MACHINE_ID, discover_existing_git_workspace,
    },
    project_definition::discover_project_definition,
};
use vz_runtime_contract::{
    EnvironmentId, MachineExecutionSpec, MachineExecutionState, MachineExecutionTerminal, MachineId,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClientError, MachineExecOutput, environment_stop_error_detail};

#[derive(Args, Debug)]
pub struct DevExecArgs {
    /// Environment name or immutable ID; otherwise process/worktree selection.
    #[arg(long, value_name = "NAME_OR_ID")]
    pub environment: Option<String>,
    /// Machine name or immutable ID within the selected Environment.
    #[arg(long, value_name = "NAME_OR_ID")]
    pub machine: Option<String>,
    /// Guest working directory (absolute inside the selected Machine).
    #[arg(long)]
    pub workdir: Option<String>,
    /// Guest user identity, interpreted by the target backend.
    #[arg(long)]
    pub user: Option<String>,
    /// Explicit guest environment variable; repeat for multiple values.
    #[arg(long="env",value_name="KEY=VALUE",value_parser=parse_env)]
    pub env: Vec<(String, String)>,
    /// Request an interactive guest terminal (requires a local terminal).
    #[arg(short = 't', long)]
    pub tty: bool,
    /// Send immediate stdin EOF instead of reading local stdin.
    #[arg(long)]
    pub no_stdin: bool,
    /// Execution deadline in seconds; timeout cancels and verifies guest cleanup.
    #[arg(long,default_value_t=86_400,value_parser=clap::value_parser!(u64).range(1..=86_400))]
    pub timeout: u64,
    /// Stable identity for exact receipt recovery; never starts duplicate work.
    #[arg(long, requires = "idempotency_key")]
    pub request_id: Option<String>,
    /// Stable reservation key, paired with --request-id.
    #[arg(long, requires = "request_id")]
    pub idempotency_key: Option<String>,
    /// Executable and arguments, passed directly without an implicit shell.
    #[arg(last=true,required=true,num_args=1..)]
    pub command: Vec<String>,
}

fn parse_env(value: &str) -> Result<(String, String), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    if key.is_empty() || key.contains('\0') || value.contains('\0') {
        return Err("invalid environment variable".into());
    }
    Ok((key.into(), value.into()))
}

#[derive(Debug, Serialize)]
pub struct ExecCommandError {
    code: String,
    message: Box<str>,
    request_id: String,
    idempotency_key: String,
    details: BTreeMap<String, String>,
}
impl ExecCommandError {
    pub fn to_json(&self) -> String {
        json!({"schema_version":1,"record_type":"execution_error","error":self}).to_string()
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
impl fmt::Display for ExecCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ExecCommandError {}

struct RawTerminal(bool);
impl Drop for RawTerminal {
    fn drop(&mut self) {
        if self.0 {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

/// Return the actual guest exit status; caller exits after terminal restoration.
pub async fn cmd_dev_exec(args: DevExecArgs, json_output: bool) -> Result<i32, ExecCommandError> {
    let token = uuid::Uuid::new_v4();
    let request_id = args
        .request_id
        .unwrap_or_else(|| format!("req-exec-{token}"));
    let idempotency_key = args
        .idempotency_key
        .unwrap_or_else(|| format!("machine-exec-{token}"));
    let local = |code: &str, message: String| ExecCommandError {
        code: code.into(),
        message: message.into_boxed_str(),
        request_id: request_id.clone(),
        idempotency_key: idempotency_key.clone(),
        details: BTreeMap::new(),
    };
    let client_error = |error: DaemonClientError| {
        if let Some(original) = environment_stop_error_detail(&error) {
            ExecCommandError {
                code: original.code.as_str().into(),
                message: original.message.into_boxed_str(),
                request_id: original.request_id.unwrap_or_else(|| request_id.clone()),
                idempotency_key: idempotency_key.clone(),
                details: original.details,
            }
        } else {
            local("backend_unavailable", error.to_string())
        }
    };
    for value in [&request_id, &idempotency_key] {
        if value.is_empty()
            || value.len() > 256
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(local("validation_error","request/idempotency IDs must be nonempty bounded strings without control characters or surrounding whitespace".into()));
        }
    }
    if args.tty && (!std::io::stdin().is_terminal() || json_output) {
        return Err(local(
            "validation_error",
            "--tty requires a local terminal and is incompatible with --json".into(),
        ));
    }
    let cwd =
        env::current_dir().map_err(|error| local("definition_read_failed", error.to_string()))?;
    let discovered = discover_project_definition(&cwd)
        .map_err(|error| local(error.code(), error.to_string()))?;
    let read_process = |variable: &str| {
        env::var_os(variable)
            .map(|value| {
                value
                    .into_string()
                    .map_err(|_| local("invalid_selector", format!("{variable} must be UTF-8")))
            })
            .transpose()
    };
    let process_environment_id = if args.environment.is_some() {
        None
    } else {
        read_process(VZ_ENVIRONMENT_ID)?
            .map(EnvironmentId::new)
            .transpose()
            .map_err(|error| local("invalid_selector", error.to_string()))?
            .map(|id| id.to_string())
    };
    let process_machine_id = if args.machine.is_some() {
        None
    } else {
        read_process(VZ_MACHINE_ID)?
            .map(MachineId::new)
            .transpose()
            .map_err(|error| local("invalid_selector", error.to_string()))?
            .map(|id| id.to_string())
    };
    let workspace_key = if args.environment.is_some() || process_environment_id.is_some() {
        None
    } else {
        discover_existing_git_workspace(&cwd)
            .map_err(|error| local("workspace_read_failed", error.to_string()))?
            .map(|workspace| workspace.workspace_key)
    };
    let mut dimensions = if args.tty {
        let (columns, rows) = crossterm::terminal::size()
            .map_err(|error| local("terminal_unavailable", error.to_string()))?;
        Some(MachineExecutionTerminal { rows, columns })
    } else {
        None
    };
    let mut environment = BTreeMap::new();
    for (key, value) in args.env {
        if environment.insert(key, value).is_some() {
            return Err(local("validation_error", "duplicate --env key".into()));
        }
    }
    let spec = MachineExecutionSpec {
        argv: args.command,
        environment,
        working_directory: args.workdir,
        user: args.user,
        terminal: dimensions,
        timeout_millis: args.timeout * 1000,
    };
    spec.validate()
        .map_err(|error| local("validation_error", error))?;
    if json_output {
        println!(
            "{}",
            json!({"schema_version":1,"record_type":"request_started","operation":"exec_machine","request_id":request_id,"idempotency_key":idempotency_key})
        );
        std::io::stdout()
            .flush()
            .map_err(|error| local("output_failed", error.to_string()))?;
    } else {
        eprintln!(
            "Exec receipt identity: --request-id {request_id} --idempotency-key {idempotency_key}"
        );
    }
    let mut client = connect_existing_daemon_for_state_db(&default_state_db_path())
        .await
        .map_err(|error| local("daemon_unavailable", error.to_string()))?;
    let mut stream = client
        .exec_machine_stream(
            runtime_v2::MachineExecOpen {
                project_id: discovered.definition.project_id.to_string(),
                environment: args.environment,
                process_environment_id,
                workspace_key,
                machine: args.machine,
                process_machine_id,
                spec: Some(vz_runtime_translate::machine_execution_spec_to_proto(&spec)),
            },
            runtime_v2::RequestMetadata {
                request_id: request_id.clone(),
                idempotency_key: idempotency_key.clone(),
                trace_id: String::new(),
            },
        )
        .await
        .map_err(client_error)?;
    let mut raw = RawTerminal(false);
    let mut stdin = tokio::io::stdin();
    let mut input = [0u8; 65536];
    let mut reading = false;
    let mut ready = false;
    let mut resize = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        tokio::select! {
            event=stream.next_event()=>{
                let event=event.map_err(client_error)?.ok_or_else(||local("invalid_daemon_response","execution stream ended without terminal receipt".into()))?;
                match event.output {
                    MachineExecOutput::Ready=>{
                        ready=true;
                        if args.tty {crossterm::terminal::enable_raw_mode().map_err(|error|local("terminal_unavailable",error.to_string()))?;raw.0=true;}
                        if args.no_stdin {stream.stdin_eof().await.map_err(client_error)?;}else{reading=true;}
                        if json_output {println!("{}",json!({"schema_version":1,"record_type":"execution_ready","scope":event.scope,"sequence":event.sequence}));}
                    },
                    MachineExecOutput::Stdout(bytes)|MachineExecOutput::Stderr(bytes)=>{
                        // Preserve output identity without assuming guest bytes are UTF-8.
                        if json_output {println!("{}",json!({"schema_version":1,"record_type":"execution_output","scope":event.scope,"sequence":event.sequence,"stream":event.output_stream,"base64":base64::engine::general_purpose::STANDARD.encode(&bytes)}));}
                        else if event.output_stream==Some("stdout") {let mut out=std::io::stdout().lock();out.write_all(&bytes).and_then(|()|out.flush()).map_err(|error|local("output_failed",error.to_string()))?;}
                        else{let mut out=std::io::stderr().lock();out.write_all(&bytes).and_then(|()|out.flush()).map_err(|error|local("output_failed",error.to_string()))?;}
                    },
                    MachineExecOutput::Receipt(receipt)=>{
                        drop(raw);
                        if json_output {println!("{}",json!({"schema_version":1,"record_type":"execution_receipt","sequence":event.sequence,"replayed":event.replayed,"receipt":receipt}));std::io::stdout().flush().map_err(|error|local("output_failed",error.to_string()))?;}
                        else {
                            if event.replayed {eprintln!("Recovered execution receipt only; historical stdout/stderr are not retained.");}
                            if let Some(reason)=&receipt.failure {eprintln!("Execution {}: {reason}",receipt.scope.execution_id);}
                        }
                        return Ok(match receipt.state {
                            MachineExecutionState::Completed if receipt.exit_code==Some(0) && receipt.failure.is_some()=>5,
                            MachineExecutionState::Completed=>receipt.exit_code.unwrap_or(5),
                            MachineExecutionState::Quiesced|MachineExecutionState::Uncertain=>5,
                            MachineExecutionState::Admitted=>return Err(local("invalid_daemon_response","terminal receipt remained Admitted".into()))
                        });
                    },
                }
            },
            result=stdin.read(&mut input),if reading=>{
                let count=result.map_err(|error|local("input_failed",error.to_string()))?;
                if count==0 {reading=false;stream.stdin_eof().await.map_err(client_error)?;}
                else{stream.stdin_write(input[..count].to_vec()).await.map_err(client_error)?;}
            },
            result=tokio::signal::ctrl_c()=>{result.map_err(|error|local("signal_failed",error.to_string()))?;stream.signal(2).await.map_err(client_error)?;},
            _=resize.tick(),if ready && dimensions.is_some()=>{
                let (columns,rows)=crossterm::terminal::size().map_err(|error|local("terminal_unavailable",error.to_string()))?;
                let current=MachineExecutionTerminal {rows,columns};
                if dimensions!=Some(current) {stream.resize(current).await.map_err(client_error)?;dimensions=Some(current);}
            },
        }
    }
}
