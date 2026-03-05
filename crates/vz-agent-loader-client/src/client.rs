//! Host-side client for talking to vz-agent-loader over any async stream.
//!
//! The client is generic over any `AsyncRead + AsyncWrite` stream, so it works
//! with vz's vsock, mac-agent's vsock, Unix sockets (for testing), etc.

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tracing::debug;

use crate::protocol::*;

/// Client for the vz-agent-loader protocol.
///
/// Generic over the transport — works with any `AsyncRead + AsyncWrite` stream.
pub struct LoaderClient<S> {
    reader: BufReader<tokio::io::ReadHalf<S>>,
    writer: tokio::io::WriteHalf<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> LoaderClient<S> {
    /// Create a new client from an async stream (vsock, unix socket, etc.).
    pub fn new(stream: S) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    /// Send a request and read the response.
    async fn call(&mut self, request: &Request) -> io::Result<Response> {
        let mut json = serde_json::to_string(request)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        json.push('\n');

        self.writer.write_all(json.as_bytes()).await?;
        self.writer.flush().await?;

        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "loader closed connection",
            ));
        }

        serde_json::from_str(line.trim())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Ping the loader — returns protocol version and uptime.
    pub async fn ping(&mut self) -> io::Result<PongResponse> {
        match self.call(&Request::Ping).await? {
            Response::Pong(pong) => Ok(pong),
            Response::Error(e) => Err(io::Error::other(
                format!("{}: {}", e.code, e.message),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {other:?}"),
            )),
        }
    }

    /// Start a binary. Returns exec_id and pid.
    pub async fn exec(&mut self, request: ExecRequest) -> io::Result<ExecOkResponse> {
        debug!(binary = %request.binary, "sending exec request");
        match self.call(&Request::Exec(request)).await? {
            Response::ExecOk(ok) => Ok(ok),
            Response::Error(e) => Err(io::Error::other(
                format!("{}: {}", e.code, e.message),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {other:?}"),
            )),
        }
    }

    /// List running children.
    pub async fn list(&mut self) -> io::Result<ListOkResponse> {
        match self.call(&Request::List).await? {
            Response::ListOk(ok) => Ok(ok),
            Response::Error(e) => Err(io::Error::other(
                format!("{}: {}", e.code, e.message),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {other:?}"),
            )),
        }
    }

    /// Kill a child by exec_id.
    pub async fn kill(&mut self, exec_id: &str, signal: Option<i32>) -> io::Result<()> {
        let request = KillRequest {
            exec_id: exec_id.to_string(),
            signal: signal.unwrap_or(15),
        };
        match self.call(&Request::Kill(request)).await? {
            Response::KillOk => Ok(()),
            Response::Error(e) => Err(io::Error::other(
                format!("{}: {}", e.code, e.message),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {other:?}"),
            )),
        }
    }

    /// Register a service in the startup manifest (persists across reboots).
    /// Optionally starts the service immediately.
    pub async fn register(
        &mut self,
        service: ServiceEntry,
        start_now: bool,
    ) -> io::Result<RegisterOkResponse> {
        let request = RegisterRequest {
            service,
            start_now,
        };
        match self.call(&Request::Register(request)).await? {
            Response::RegisterOk(ok) => Ok(ok),
            Response::Error(e) => Err(io::Error::other(
                format!("{}: {}", e.code, e.message),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {other:?}"),
            )),
        }
    }

    /// Unregister a service from the startup manifest.
    /// Optionally kills it if running.
    pub async fn unregister(&mut self, name: &str, stop: bool) -> io::Result<()> {
        let request = UnregisterRequest {
            name: name.to_string(),
            stop,
        };
        match self.call(&Request::Unregister(request)).await? {
            Response::UnregisterOk => Ok(()),
            Response::Error(e) => Err(io::Error::other(
                format!("{}: {}", e.code, e.message),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected response: {other:?}"),
            )),
        }
    }
}

/// Convenience: start a binary with keep_alive and wait for it to be running.
pub fn service_entry(
    name: impl Into<String>,
    binary: impl Into<String>,
    keep_alive: bool,
) -> ServiceEntry {
    ServiceEntry {
        name: name.into(),
        binary: binary.into(),
        args: Vec::new(),
        env: Vec::new(),
        keep_alive,
    }
}
