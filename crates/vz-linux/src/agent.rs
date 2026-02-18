use std::time::Duration;

use tokio::time::{Instant, timeout};
use vz::protocol::{self, ExecOutput, Handshake, HandshakeAck, Request, Response};
use vz::{Vm, VsockStream};

use crate::LinuxError;

const HEALTHCHECK_PING_ID: u64 = 1;
const EXEC_REQUEST_ID: u64 = 2;
const PORT_FORWARD_REQUEST_ID: u64 = 3;

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

/// Connect to guest agent, perform handshake, and ping.
pub async fn handshake_and_ping(vm: &Vm) -> Result<HandshakeAck, LinuxError> {
    let stream = vm.vsock_connect(protocol::AGENT_PORT).await?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    let handshake = Handshake {
        protocol_version: protocol::PROTOCOL_VERSION,
        capabilities: vec!["user_exec".to_string(), "port_forward".to_string()],
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
        capabilities: vec!["user_exec".to_string(), "port_forward".to_string()],
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
        .any(|capability| capability == "port_forward")
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
        capabilities: vec!["user_exec".to_string(), "port_forward".to_string()],
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
