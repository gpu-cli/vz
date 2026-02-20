use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, timeout};
use vz::protocol::{
    self, ExecOutput, Handshake, HandshakeAck, OciContainerState, OciExecResult, OciPayload,
    Request, Response,
};
use vz::{Vm, VsockStream};

use crate::LinuxError;

const HEALTHCHECK_PING_ID: u64 = 1;
const EXEC_REQUEST_ID: u64 = 2;
const PORT_FORWARD_REQUEST_ID: u64 = 3;
const AGENT_CAPABILITY_USER_EXEC: &str = "user_exec";
const AGENT_CAPABILITY_PORT_FORWARD: &str = "port_forward";
const AGENT_CAPABILITY_OCI_LIFECYCLE: &str = "oci_lifecycle";
const OCI_RUNTIME_BINARY: &str = "/run/vz-oci/bin/youki";
const OCI_EXEC_REQUEST_ID: u64 = 4;
const OCI_ERROR_CODE_RUNTIME_FAILURE: i32 = 125;
const OCI_ERROR_CODE_EXEC_START_FAILURE: i32 = 127;

/// Options for guest command execution.
#[derive(Debug, Clone, Default)]
pub struct ExecOptions {
    /// Optional working directory inside the guest.
    pub working_dir: Option<String>,
    /// Environment variables for the process.
    pub env: Vec<(String, String)>,
    /// Optional user to run as.
    pub user: Option<String>,
}

/// Options for OCI exec requests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OciExecOptions {
    /// Environment variables for the process.
    pub env: Vec<(String, String)>,
    /// Optional working directory inside the container.
    pub cwd: Option<String>,
    /// Optional user identity inside the container.
    pub user: Option<String>,
}

/// Connect to guest agent, perform handshake, and ping.
pub async fn handshake_and_ping(vm: &Vm) -> Result<HandshakeAck, LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    let handshake = Handshake {
        protocol_version: protocol::PROTOCOL_VERSION,
        capabilities: vec![
            AGENT_CAPABILITY_USER_EXEC.to_string(),
            AGENT_CAPABILITY_PORT_FORWARD.to_string(),
        ],
    };

    protocol::write_frame(&mut writer, &handshake).await?;

    let ack: HandshakeAck = protocol::read_frame(&mut reader).await?;
    if ack.protocol_version == 0 {
        return Err(LinuxError::Protocol(
            "guest agent negotiated protocol version 0".to_string(),
        ));
    }

    protocol::write_frame(
        &mut writer,
        &Request::Ping {
            id: HEALTHCHECK_PING_ID,
        },
    )
    .await?;

    let response: Response = protocol::read_frame(&mut reader).await?;
    match response {
        Response::Pong { id } if id == HEALTHCHECK_PING_ID => Ok(ack),
        Response::Error { error, .. } => Err(LinuxError::Protocol(format!(
            "guest agent ping failed: {error}"
        ))),
        other => Err(LinuxError::Protocol(format!(
            "expected Pong response, got: {other:?}"
        ))),
    }
}

/// Open a dedicated guest port-forward vsock stream.
///
/// On success, the returned stream is a raw byte pipe to the requested
/// `target_port` inside the guest.
pub async fn open_port_forward_stream(
    vm: &Vm,
    target_port: u16,
    protocol_name: &str,
) -> Result<VsockStream, LinuxError> {
    let mut stream = vm.vsock_connect(protocol::AGENT_PORT).await?;

    let handshake = Handshake {
        protocol_version: protocol::PROTOCOL_VERSION,
        capabilities: vec![
            AGENT_CAPABILITY_USER_EXEC.to_string(),
            AGENT_CAPABILITY_PORT_FORWARD.to_string(),
        ],
    };
    protocol::write_frame(&mut stream, &handshake).await?;

    let ack: HandshakeAck = protocol::read_frame(&mut stream).await?;
    if ack.protocol_version == 0 {
        return Err(LinuxError::Protocol(
            "guest agent negotiated protocol version 0".to_string(),
        ));
    }

    if !ack
        .capabilities
        .iter()
        .any(|capability| capability == AGENT_CAPABILITY_PORT_FORWARD)
    {
        return Err(LinuxError::Protocol(
            "guest agent does not advertise port_forward capability".to_string(),
        ));
    }

    protocol::write_frame(
        &mut stream,
        &Request::PortForward {
            id: PORT_FORWARD_REQUEST_ID,
            target_port,
            protocol: protocol_name.to_string(),
        },
    )
    .await?;

    let response: Response = protocol::read_frame(&mut stream).await?;
    match response {
        Response::PortForwardReady { id } if id == PORT_FORWARD_REQUEST_ID => Ok(stream),
        Response::Error { id, error } if id == PORT_FORWARD_REQUEST_ID => Err(
            LinuxError::Protocol(format!("guest port forward request failed: {error}")),
        ),
        other => Err(LinuxError::Protocol(format!(
            "expected PortForwardReady response, got: {other:?}"
        ))),
    }
}

/// Execute a command in the guest and capture buffered stdout/stderr output.
pub async fn exec_capture(
    vm: &Vm,
    command: String,
    args: Vec<String>,
    request_timeout: Duration,
) -> Result<ExecOutput, LinuxError> {
    exec_capture_with_options(vm, command, args, request_timeout, ExecOptions::default()).await
}

/// Execute a command in the guest with explicit execution options.
pub async fn exec_capture_with_options(
    vm: &Vm,
    command: String,
    args: Vec<String>,
    request_timeout: Duration,
    options: ExecOptions,
) -> Result<ExecOutput, LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    let handshake = Handshake {
        protocol_version: protocol::PROTOCOL_VERSION,
        capabilities: vec![
            AGENT_CAPABILITY_USER_EXEC.to_string(),
            AGENT_CAPABILITY_PORT_FORWARD.to_string(),
        ],
    };
    protocol::write_frame(&mut writer, &handshake).await?;
    let ack: HandshakeAck = protocol::read_frame(&mut reader).await?;
    if ack.protocol_version == 0 {
        return Err(LinuxError::Protocol(
            "guest agent negotiated protocol version 0".to_string(),
        ));
    }

    let exec_id = EXEC_REQUEST_ID;
    let request = Request::Exec {
        id: exec_id,
        command,
        args,
        working_dir: options.working_dir,
        env: options.env,
        user: options.user,
    };
    protocol::write_frame(&mut writer, &request).await?;

    let started = Instant::now();
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();

    loop {
        let elapsed = started.elapsed();
        if elapsed >= request_timeout {
            return Err(LinuxError::Protocol(format!(
                "exec timed out after {:.3}s",
                request_timeout.as_secs_f64()
            )));
        }
        let remaining = request_timeout.saturating_sub(elapsed);

        let response: Response = timeout(remaining, protocol::read_frame(&mut reader))
            .await
            .map_err(|_| {
                LinuxError::Protocol(format!(
                    "timed out waiting for exec response after {:.3}s",
                    request_timeout.as_secs_f64()
                ))
            })??;

        match response {
            Response::Stdout { exec_id: id, data } if id == exec_id => {
                stdout_bytes.extend_from_slice(&data);
            }
            Response::Stderr { exec_id: id, data } if id == exec_id => {
                stderr_bytes.extend_from_slice(&data);
            }
            Response::ExitCode { exec_id: id, code } if id == exec_id => {
                return Ok(ExecOutput {
                    exit_code: code,
                    stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
                    stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
                });
            }
            Response::ExecError { id, error } if id == exec_id => {
                return Err(LinuxError::Protocol(format!(
                    "guest exec failed to start: {error}"
                )));
            }
            Response::Error { id, error } if id == exec_id => {
                return Err(LinuxError::Protocol(format!(
                    "guest returned error for exec request: {error}"
                )));
            }
            _ => {
                // Ignore frames unrelated to this exec_id.
            }
        }
    }
}

fn oci_handshake() -> Handshake {
    Handshake {
        protocol_version: protocol::PROTOCOL_VERSION,
        capabilities: vec![
            AGENT_CAPABILITY_USER_EXEC.to_string(),
            AGENT_CAPABILITY_PORT_FORWARD.to_string(),
            AGENT_CAPABILITY_OCI_LIFECYCLE.to_string(),
        ],
    }
}

fn ensure_handshake_protocol(ack: &HandshakeAck) -> Result<(), LinuxError> {
    if ack.protocol_version == 0 {
        return Err(LinuxError::Protocol(
            "guest agent negotiated protocol version 0".to_string(),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum OciSuccessPayload {
    Empty,
    State,
    Exec,
}

#[derive(Debug, Clone)]
struct OciCommandPlan {
    id: String,
    operation: &'static str,
    args: Vec<String>,
    success_payload: OciSuccessPayload,
}

#[derive(Debug, Deserialize)]
struct YoukiStateOutput {
    id: String,
    status: String,
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    bundle: Option<String>,
    #[serde(default, alias = "bundlePath", alias = "bundle_path")]
    bundle_path: Option<String>,
}

fn oci_command_plan(request: Request) -> OciCommandPlan {
    match request {
        Request::OciCreate { id, bundle_path } => OciCommandPlan {
            id: id.clone(),
            operation: "create",
            args: vec![
                "create".to_string(),
                id,
                "--bundle".to_string(),
                bundle_path,
            ],
            success_payload: OciSuccessPayload::Empty,
        },
        Request::OciStart { id } => OciCommandPlan {
            id: id.clone(),
            operation: "start",
            args: vec!["start".to_string(), id],
            success_payload: OciSuccessPayload::Empty,
        },
        Request::OciState { id } => OciCommandPlan {
            id: id.clone(),
            operation: "state",
            args: vec!["state".to_string(), id],
            success_payload: OciSuccessPayload::State,
        },
        Request::OciExec {
            id,
            command,
            args,
            env,
            cwd,
            user,
        } => {
            let mut runtime_args = vec!["exec".to_string(), id.clone()];
            if let Some(cwd) = cwd {
                runtime_args.push("--cwd".to_string());
                runtime_args.push(cwd);
            }
            for (key, value) in env {
                runtime_args.push("--env".to_string());
                runtime_args.push(format!("{key}={value}"));
            }
            if let Some(user) = user {
                runtime_args.push("--user".to_string());
                runtime_args.push(user);
            }
            runtime_args.push("--".to_string());
            runtime_args.push(command);
            runtime_args.extend(args);

            OciCommandPlan {
                id,
                operation: "exec",
                args: runtime_args,
                success_payload: OciSuccessPayload::Exec,
            }
        }
        Request::OciKill { id, signal } => OciCommandPlan {
            id: id.clone(),
            operation: "kill",
            args: vec!["kill".to_string(), id, signal],
            success_payload: OciSuccessPayload::Empty,
        },
        Request::OciDelete { id, force } => {
            let mut args = vec!["delete".to_string()];
            if force {
                args.push("--force".to_string());
            }
            args.push(id.clone());

            OciCommandPlan {
                id,
                operation: "delete",
                args,
                success_payload: OciSuccessPayload::Empty,
            }
        }
        _ => unreachable!("non-OCI request passed to OCI dispatcher"),
    }
}

fn map_oci_response(expected_id: &str, response: Response) -> Result<OciPayload, LinuxError> {
    match response {
        Response::OciOk { id, payload } => {
            if id != expected_id {
                return Err(LinuxError::Protocol(format!(
                    "guest returned OCI response for container '{id}', expected '{expected_id}'"
                )));
            }
            Ok(payload)
        }
        Response::OciError { id, code, message } => {
            if id != expected_id {
                return Err(LinuxError::Protocol(format!(
                    "guest returned OCI error for container '{id}', expected '{expected_id}'"
                )));
            }

            Err(LinuxError::Protocol(format!(
                "guest OCI request failed for container '{id}' (code {code}): {message}"
            )))
        }
        other => Err(LinuxError::Protocol(format!(
            "expected OciOk/OciError response for container '{expected_id}', got: {other:?}"
        ))),
    }
}

fn map_oci_failure_from_output(plan: &OciCommandPlan, output: &ExecOutput) -> Response {
    let code = if output.exit_code == 0 {
        OCI_ERROR_CODE_RUNTIME_FAILURE
    } else {
        output.exit_code
    };
    let detail = if !output.stderr.trim().is_empty() {
        output.stderr.trim().to_string()
    } else if !output.stdout.trim().is_empty() {
        output.stdout.trim().to_string()
    } else {
        format!("exit code {code}")
    };

    Response::OciError {
        id: plan.id.clone(),
        code,
        message: format!("youki {} failed: {detail}", plan.operation),
    }
}

fn parse_youki_state_payload(expected_id: &str, stdout: &str) -> Result<OciContainerState, String> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Err("empty stdout".to_string());
    }

    let parsed: YoukiStateOutput =
        serde_json::from_str(trimmed).map_err(|error| format!("invalid JSON: {error}"))?;
    if parsed.id != expected_id {
        return Err(format!(
            "state id '{}' did not match expected '{}'",
            parsed.id, expected_id
        ));
    }

    Ok(OciContainerState {
        id: parsed.id,
        status: parsed.status,
        pid: parsed.pid,
        bundle_path: parsed.bundle_path.or(parsed.bundle),
    })
}

fn map_youki_exec_output(plan: &OciCommandPlan, output: ExecOutput) -> Response {
    if !matches!(plan.success_payload, OciSuccessPayload::Exec) && output.exit_code != 0 {
        return map_oci_failure_from_output(plan, &output);
    }

    match plan.success_payload {
        OciSuccessPayload::Empty => Response::OciOk {
            id: plan.id.clone(),
            payload: OciPayload::Empty,
        },
        OciSuccessPayload::State => match parse_youki_state_payload(&plan.id, &output.stdout) {
            Ok(state) => Response::OciOk {
                id: plan.id.clone(),
                payload: OciPayload::State { state },
            },
            Err(error) => Response::OciError {
                id: plan.id.clone(),
                code: OCI_ERROR_CODE_RUNTIME_FAILURE,
                message: format!("youki state returned invalid state payload: {error}"),
            },
        },
        OciSuccessPayload::Exec => Response::OciOk {
            id: plan.id.clone(),
            payload: OciPayload::Exec {
                result: OciExecResult {
                    exit_code: output.exit_code,
                    stdout: output.stdout,
                    stderr: output.stderr,
                },
            },
        },
    }
}

async fn dispatch_oci_request<S>(mut stream: S, request: Request) -> Result<OciPayload, LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let plan = oci_command_plan(request);
    protocol::write_frame(&mut stream, &oci_handshake()).await?;
    let ack: HandshakeAck = protocol::read_frame(&mut stream).await?;
    ensure_handshake_protocol(&ack)?;

    protocol::write_frame(
        &mut stream,
        &Request::Exec {
            id: OCI_EXEC_REQUEST_ID,
            command: OCI_RUNTIME_BINARY.to_string(),
            args: plan.args.clone(),
            working_dir: None,
            env: Vec::new(),
            user: None,
        },
    )
    .await?;

    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();

    loop {
        let response: Response = protocol::read_frame(&mut stream).await?;
        match response {
            Response::Stdout { exec_id, data } if exec_id == OCI_EXEC_REQUEST_ID => {
                stdout_bytes.extend_from_slice(&data);
            }
            Response::Stderr { exec_id, data } if exec_id == OCI_EXEC_REQUEST_ID => {
                stderr_bytes.extend_from_slice(&data);
            }
            Response::ExitCode { exec_id, code } if exec_id == OCI_EXEC_REQUEST_ID => {
                let mapped = map_youki_exec_output(
                    &plan,
                    ExecOutput {
                        exit_code: code,
                        stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
                        stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
                    },
                );
                return map_oci_response(&plan.id, mapped);
            }
            Response::ExecError { id, error } if id == OCI_EXEC_REQUEST_ID => {
                return map_oci_response(
                    &plan.id,
                    Response::OciError {
                        id: plan.id.clone(),
                        code: OCI_ERROR_CODE_EXEC_START_FAILURE,
                        message: format!("youki {} failed to start: {error}", plan.operation),
                    },
                );
            }
            Response::Error { id, error } if id == OCI_EXEC_REQUEST_ID => {
                return map_oci_response(
                    &plan.id,
                    Response::OciError {
                        id: plan.id.clone(),
                        code: OCI_ERROR_CODE_RUNTIME_FAILURE,
                        message: format!("youki {} request failed: {error}", plan.operation),
                    },
                );
            }
            _ => {
                // Ignore frames unrelated to this request.
            }
        }
    }
}

fn expect_empty_payload(operation: &str, id: &str, payload: OciPayload) -> Result<(), LinuxError> {
    match payload {
        OciPayload::Empty => Ok(()),
        other => Err(LinuxError::Protocol(format!(
            "{operation} for container '{id}' expected empty OCI payload, got: {other:?}"
        ))),
    }
}

fn expect_state_payload(id: &str, payload: OciPayload) -> Result<OciContainerState, LinuxError> {
    match payload {
        OciPayload::State { state } => Ok(state),
        other => Err(LinuxError::Protocol(format!(
            "OCI state request for container '{id}' expected state payload, got: {other:?}"
        ))),
    }
}

fn expect_exec_payload(id: &str, payload: OciPayload) -> Result<OciExecResult, LinuxError> {
    match payload {
        OciPayload::Exec { result } => Ok(result),
        other => Err(LinuxError::Protocol(format!(
            "OCI exec request for container '{id}' expected exec payload, got: {other:?}"
        ))),
    }
}

async fn oci_create_with_stream<S>(
    stream: S,
    id: String,
    bundle_path: String,
) -> Result<(), LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = dispatch_oci_request(
        stream,
        Request::OciCreate {
            id: id.clone(),
            bundle_path,
        },
    )
    .await?;
    expect_empty_payload("OCI create", &id, payload)
}

async fn oci_start_with_stream<S>(stream: S, id: String) -> Result<(), LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = dispatch_oci_request(stream, Request::OciStart { id: id.clone() }).await?;
    expect_empty_payload("OCI start", &id, payload)
}

async fn oci_state_with_stream<S>(stream: S, id: String) -> Result<OciContainerState, LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = dispatch_oci_request(stream, Request::OciState { id: id.clone() }).await?;
    expect_state_payload(&id, payload)
}

async fn oci_exec_with_stream<S>(
    stream: S,
    id: String,
    command: String,
    args: Vec<String>,
    options: OciExecOptions,
) -> Result<OciExecResult, LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = dispatch_oci_request(
        stream,
        Request::OciExec {
            id: id.clone(),
            command,
            args,
            env: options.env,
            cwd: options.cwd,
            user: options.user,
        },
    )
    .await?;
    expect_exec_payload(&id, payload)
}

async fn oci_kill_with_stream<S>(stream: S, id: String, signal: String) -> Result<(), LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = dispatch_oci_request(
        stream,
        Request::OciKill {
            id: id.clone(),
            signal,
        },
    )
    .await?;
    expect_empty_payload("OCI kill", &id, payload)
}

async fn oci_delete_with_stream<S>(stream: S, id: String, force: bool) -> Result<(), LinuxError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = dispatch_oci_request(
        stream,
        Request::OciDelete {
            id: id.clone(),
            force,
        },
    )
    .await?;
    expect_empty_payload("OCI delete", &id, payload)
}

/// Dispatch `OciCreate` to the Linux guest agent.
pub async fn oci_create(vm: &Vm, id: String, bundle_path: String) -> Result<(), LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    oci_create_with_stream(stream, id, bundle_path).await
}

/// Dispatch `OciStart` to the Linux guest agent.
pub async fn oci_start(vm: &Vm, id: String) -> Result<(), LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    oci_start_with_stream(stream, id).await
}

/// Dispatch `OciState` to the Linux guest agent.
pub async fn oci_state(vm: &Vm, id: String) -> Result<OciContainerState, LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    oci_state_with_stream(stream, id).await
}

/// Dispatch `OciExec` to the Linux guest agent.
pub async fn oci_exec(
    vm: &Vm,
    id: String,
    command: String,
    args: Vec<String>,
    options: OciExecOptions,
) -> Result<OciExecResult, LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    oci_exec_with_stream(stream, id, command, args, options).await
}

/// Dispatch `OciKill` to the Linux guest agent.
pub async fn oci_kill(vm: &Vm, id: String, signal: String) -> Result<(), LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    oci_kill_with_stream(stream, id, signal).await
}

/// Dispatch `OciDelete` to the Linux guest agent.
pub async fn oci_delete(vm: &Vm, id: String, force: bool) -> Result<(), LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    oci_delete_with_stream(stream, id, force).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;
    use tokio::task::JoinHandle;
    use vz::protocol::{Handshake, HandshakeAck};

    fn test_handshake_ack() -> HandshakeAck {
        HandshakeAck {
            protocol_version: protocol::PROTOCOL_VERSION,
            agent_version: "test-agent".to_string(),
            os: "linux".to_string(),
            capabilities: vec![AGENT_CAPABILITY_OCI_LIFECYCLE.to_string()],
        }
    }

    fn expected_youki_exec(args: Vec<String>) -> Request {
        Request::Exec {
            id: OCI_EXEC_REQUEST_ID,
            command: OCI_RUNTIME_BINARY.to_string(),
            args,
            working_dir: None,
            env: Vec::new(),
            user: None,
        }
    }

    fn mock_youki_server(
        expected_request: Request,
        responses: Vec<Response>,
    ) -> (tokio::io::DuplexStream, JoinHandle<Handshake>) {
        let (client, mut server) = duplex(32 * 1024);
        let server_task = tokio::spawn(async move {
            let handshake: Handshake = protocol::read_frame(&mut server)
                .await
                .expect("read handshake");
            protocol::write_frame(&mut server, &test_handshake_ack())
                .await
                .expect("write handshake ack");

            let request: Request = protocol::read_frame(&mut server)
                .await
                .expect("read OCI request");
            assert_eq!(request, expected_request);

            for response in responses {
                protocol::write_frame(&mut server, &response)
                    .await
                    .expect("write OCI response");
            }
            handshake
        });

        (client, server_task)
    }

    #[tokio::test]
    async fn oci_empty_payload_handlers_invoke_youki_commands() {
        let id = "svc-web".to_string();
        let bundle_path = "/run/vz-oci/bundles/svc-web".to_string();

        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec![
                "create".to_string(),
                id.clone(),
                "--bundle".to_string(),
                bundle_path.clone(),
            ]),
            vec![Response::ExitCode {
                exec_id: OCI_EXEC_REQUEST_ID,
                code: 0,
            }],
        );
        oci_create_with_stream(client, id.clone(), bundle_path)
            .await
            .expect("dispatch oci create");
        let handshake = server_task.await.expect("join create server");
        assert!(
            handshake
                .capabilities
                .iter()
                .any(|capability| capability == AGENT_CAPABILITY_OCI_LIFECYCLE)
        );

        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec!["start".to_string(), id.clone()]),
            vec![Response::ExitCode {
                exec_id: OCI_EXEC_REQUEST_ID,
                code: 0,
            }],
        );
        oci_start_with_stream(client, id.clone())
            .await
            .expect("dispatch oci start");
        server_task.await.expect("join start server");

        let signal = "SIGTERM".to_string();
        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec!["kill".to_string(), id.clone(), signal.clone()]),
            vec![Response::ExitCode {
                exec_id: OCI_EXEC_REQUEST_ID,
                code: 0,
            }],
        );
        oci_kill_with_stream(client, id.clone(), signal)
            .await
            .expect("dispatch oci kill");
        server_task.await.expect("join kill server");

        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec![
                "delete".to_string(),
                "--force".to_string(),
                id.clone(),
            ]),
            vec![Response::ExitCode {
                exec_id: OCI_EXEC_REQUEST_ID,
                code: 0,
            }],
        );
        oci_delete_with_stream(client, "svc-web".to_string(), true)
            .await
            .expect("dispatch oci delete");
        server_task.await.expect("join delete server");
    }

    #[tokio::test]
    async fn oci_state_handler_returns_typed_state_payload() {
        let id = "svc-state".to_string();
        let expected_state = OciContainerState {
            id: id.clone(),
            status: "running".to_string(),
            pid: Some(4242),
            bundle_path: Some("/run/vz-oci/bundles/svc-state".to_string()),
        };
        let state_json = format!(
            r#"{{"id":"{id}","status":"running","pid":4242,"bundle":"/run/vz-oci/bundles/svc-state"}}"#
        );

        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec!["state".to_string(), id.clone()]),
            vec![
                Response::Stdout {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    data: state_json.into_bytes(),
                },
                Response::ExitCode {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    code: 0,
                },
            ],
        );

        let state = oci_state_with_stream(client, id)
            .await
            .expect("dispatch oci state");
        assert_eq!(state, expected_state);
        server_task.await.expect("join state server");
    }

    #[tokio::test]
    async fn oci_exec_handler_returns_typed_exec_payload() {
        let id = "svc-exec".to_string();
        let command = "/bin/sh".to_string();
        let args = vec!["-c".to_string(), "echo ready".to_string()];
        let options = OciExecOptions {
            env: vec![
                ("MODE".to_string(), "prod".to_string()),
                ("TRACE".to_string(), "1".to_string()),
            ],
            cwd: Some("/workspace".to_string()),
            user: Some("1000:1000".to_string()),
        };
        let expected_result = OciExecResult {
            exit_code: 7,
            stdout: "ready\n".to_string(),
            stderr: "warn\n".to_string(),
        };

        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec![
                "exec".to_string(),
                id.clone(),
                "--cwd".to_string(),
                "/workspace".to_string(),
                "--env".to_string(),
                "MODE=prod".to_string(),
                "--env".to_string(),
                "TRACE=1".to_string(),
                "--user".to_string(),
                "1000:1000".to_string(),
                "--".to_string(),
                command.clone(),
                args[0].clone(),
                args[1].clone(),
            ]),
            vec![
                Response::Stdout {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    data: b"ready\n".to_vec(),
                },
                Response::Stderr {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    data: b"warn\n".to_vec(),
                },
                Response::ExitCode {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    code: 7,
                },
            ],
        );

        let result = oci_exec_with_stream(client, id, command, args, options)
            .await
            .expect("dispatch oci exec");
        assert_eq!(result, expected_result);
        server_task.await.expect("join exec server");
    }

    #[tokio::test]
    async fn oci_dispatch_maps_non_zero_exit_to_protocol_error() {
        let id = "svc-error".to_string();
        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec!["start".to_string(), id.clone()]),
            vec![
                Response::Stderr {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    data: b"container already exists\n".to_vec(),
                },
                Response::ExitCode {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    code: 125,
                },
            ],
        );

        let error = oci_start_with_stream(client, "svc-error".to_string())
            .await
            .expect_err("expected OCI error");
        let message = match error {
            LinuxError::Protocol(message) => message,
            other => panic!("expected protocol error, got: {other:?}"),
        };
        assert!(message.contains("code 125"));
        assert!(message.contains("container already exists"));

        server_task.await.expect("join oci error server");
    }

    #[tokio::test]
    async fn oci_dispatch_maps_exec_start_failure_to_stable_code() {
        let id = "svc-missing-youki".to_string();
        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec![
                "create".to_string(),
                id.clone(),
                "--bundle".to_string(),
                "/run/vz-oci/bundles/svc-missing-youki".to_string(),
            ]),
            vec![Response::ExecError {
                id: OCI_EXEC_REQUEST_ID,
                error: "No such file or directory".to_string(),
            }],
        );

        let error = oci_create_with_stream(
            client,
            id,
            "/run/vz-oci/bundles/svc-missing-youki".to_string(),
        )
        .await
        .expect_err("expected OCI error");
        let message = match error {
            LinuxError::Protocol(message) => message,
            other => panic!("expected protocol error, got: {other:?}"),
        };
        assert!(message.contains("code 127"));
        assert!(message.contains("failed to start"));

        server_task.await.expect("join untyped response server");
    }

    #[tokio::test]
    async fn oci_state_invalid_json_maps_to_runtime_error_code() {
        let id = "svc-bad-state".to_string();
        let (client, server_task) = mock_youki_server(
            expected_youki_exec(vec!["state".to_string(), id.clone()]),
            vec![
                Response::Stdout {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    data: b"not-json".to_vec(),
                },
                Response::ExitCode {
                    exec_id: OCI_EXEC_REQUEST_ID,
                    code: 0,
                },
            ],
        );

        let error = oci_state_with_stream(client, id)
            .await
            .expect_err("expected invalid state payload error");
        let message = match error {
            LinuxError::Protocol(message) => message,
            other => panic!("expected protocol error, got: {other:?}"),
        };
        assert!(message.contains("code 125"));
        assert!(message.contains("invalid state payload"));

        server_task.await.expect("join bad state server");
    }
}
