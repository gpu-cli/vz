use std::time::Duration;

use tokio::time::{Instant, timeout};
use vz::Vm;
use vz::protocol::{self, ExecOutput, Handshake, HandshakeAck, Request, Response};

use crate::LinuxError;

const HEALTHCHECK_PING_ID: u64 = 1;
const EXEC_REQUEST_ID: u64 = 2;

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

/// Execute a command in the guest and capture buffered stdout/stderr output.
pub async fn exec_capture(
    vm: &Vm,
    command: String,
    args: Vec<String>,
    request_timeout: Duration,
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
        working_dir: None,
        env: Vec::new(),
        user: None,
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
