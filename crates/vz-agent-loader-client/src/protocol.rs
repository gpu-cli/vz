//! Loader wire protocol — newline-delimited JSON over vsock.
//!
//! The host sends a [`Request`] as a single JSON line, the loader responds
//! with a [`Response`] as a single JSON line. One request, one response,
//! then the connection stays open for streaming child events or the host
//! can send another request.
//!
//! Design constraints:
//! - No protobuf, no gRPC — this binary must be maximally stable.
//! - Serde JSON is the only serialization dependency.
//! - Protocol versioned via `"v"` field for future evolution.

use serde::{Deserialize, Serialize};

/// Default vsock port for the loader.
pub const LOADER_PORT: u32 = 7420;

/// Protocol version. Bumped on breaking changes.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default path for the startup manifest.
/// The loader reads this on boot and auto-starts listed binaries.
/// The host can also update it at runtime via the `register` command.
pub const STARTUP_MANIFEST_PATH: &str = "/var/lib/vz-agent-loader/startup.json";

// ── Startup manifest (persisted to disk) ──────────────────────────

/// Startup manifest — binaries to auto-start on boot.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StartupManifest {
    /// Binaries to start on loader boot, keyed by a stable name.
    #[serde(default)]
    pub services: Vec<ServiceEntry>,
}

/// A service entry in the startup manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEntry {
    /// Stable name for this service (e.g. "vz-guest-agent", "mac-agent-guest-agent").
    pub name: String,

    /// Absolute path to the binary.
    pub binary: String,

    /// Command-line arguments.
    #[serde(default)]
    pub args: Vec<String>,

    /// Environment variables (KEY=VALUE).
    #[serde(default)]
    pub env: Vec<String>,

    /// Restart on exit.
    #[serde(default = "default_true")]
    pub keep_alive: bool,
}

fn default_true() -> bool {
    true
}

// ── Requests (host → loader) ──────────────────────────────────────

/// A request from the host to the loader.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    /// Start a binary and return its exec_id.
    #[serde(rename = "exec")]
    Exec(ExecRequest),

    /// List running children.
    #[serde(rename = "list")]
    List,

    /// Kill a running child by exec_id.
    #[serde(rename = "kill")]
    Kill(KillRequest),

    /// Register a service in the startup manifest (persists across reboots).
    #[serde(rename = "register")]
    Register(RegisterRequest),

    /// Unregister a service from the startup manifest.
    #[serde(rename = "unregister")]
    Unregister(UnregisterRequest),

    /// Check loader health / protocol version.
    #[serde(rename = "ping")]
    Ping,
}

/// Start a child process.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecRequest {
    /// Absolute path to the binary.
    pub binary: String,

    /// Command-line arguments (not including argv[0]).
    #[serde(default)]
    pub args: Vec<String>,

    /// Environment variables to set (key=value pairs).
    #[serde(default)]
    pub env: Vec<String>,

    /// If true, the loader will restart the process if it exits.
    #[serde(default)]
    pub keep_alive: bool,
}

/// Kill a running child.
#[derive(Debug, Serialize, Deserialize)]
pub struct KillRequest {
    /// The exec_id returned from a previous Exec response.
    pub exec_id: String,

    /// Signal number (default: SIGTERM = 15).
    #[serde(default = "default_signal")]
    pub signal: i32,
}

fn default_signal() -> i32 {
    15 // SIGTERM
}

/// Register a service in the startup manifest.
/// The service is also started immediately if `start_now` is true.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Service entry to register.
    pub service: ServiceEntry,

    /// Start the service immediately (default: true).
    #[serde(default = "default_true")]
    pub start_now: bool,
}

/// Unregister a service by name.
#[derive(Debug, Serialize, Deserialize)]
pub struct UnregisterRequest {
    /// Service name to remove from the startup manifest.
    pub name: String,

    /// Kill the service if running (default: true).
    #[serde(default = "default_true")]
    pub stop: bool,
}

// ── Responses (loader → host) ──────────────────────────────────────

/// A response from the loader to the host.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    /// Process started successfully.
    #[serde(rename = "exec_ok")]
    ExecOk(ExecOkResponse),

    /// List of running children.
    #[serde(rename = "list_ok")]
    ListOk(ListOkResponse),

    /// Kill sent successfully.
    #[serde(rename = "kill_ok")]
    KillOk,

    /// Service registered in startup manifest.
    #[serde(rename = "register_ok")]
    RegisterOk(RegisterOkResponse),

    /// Service unregistered from startup manifest.
    #[serde(rename = "unregister_ok")]
    UnregisterOk,

    /// Pong response with loader metadata.
    #[serde(rename = "pong")]
    Pong(PongResponse),

    /// A child process exited (async event, pushed to host).
    #[serde(rename = "child_exited")]
    ChildExited(ChildExitedEvent),

    /// An error occurred.
    #[serde(rename = "error")]
    Error(ErrorResponse),
}

/// Successful exec response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecOkResponse {
    /// Unique identifier for this child, used in kill/status.
    pub exec_id: String,

    /// OS PID of the child process.
    pub pid: u32,
}

/// List of running children.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListOkResponse {
    pub children: Vec<ChildInfo>,
}

/// Info about a running child.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChildInfo {
    pub exec_id: String,
    pub pid: u32,
    pub binary: String,
    pub keep_alive: bool,
}

/// Pong with loader metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct PongResponse {
    pub version: u32,
    pub loader_version: String,
    pub uptime_secs: u64,
}

/// A child process exited.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChildExitedEvent {
    pub exec_id: String,
    pub pid: u32,
    /// Exit code if the process exited normally.
    pub exit_code: Option<i32>,
    /// Signal number if the process was killed by a signal.
    pub signal: Option<i32>,
    /// Whether the loader will restart it (keep_alive).
    pub restarting: bool,
}

/// Registered service, optionally with exec info if started immediately.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterOkResponse {
    pub name: String,
    /// exec_id if the service was started immediately.
    pub exec_id: Option<String>,
    pub pid: Option<u32>,
}

/// Error response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_request_roundtrip() {
        let req = Request::Exec(ExecRequest {
            binary: "/usr/local/bin/my-agent".to_string(),
            args: vec!["--port".to_string(), "7425".to_string()],
            env: vec!["RUST_LOG=info".to_string()],
            keep_alive: true,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        match parsed {
            Request::Exec(e) => {
                assert_eq!(e.binary, "/usr/local/bin/my-agent");
                assert!(e.keep_alive);
                assert_eq!(e.args.len(), 2);
            }
            _ => panic!("expected Exec"),
        }
    }

    #[test]
    fn ping_roundtrip() {
        let req = Request::Ping;
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"type":"ping"}"#);
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Request::Ping));
    }

    #[test]
    fn error_response_roundtrip() {
        let resp = Response::Error(ErrorResponse {
            code: "not_found".to_string(),
            message: "exec_id xyz not found".to_string(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        match parsed {
            Response::Error(e) => assert_eq!(e.code, "not_found"),
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn kill_default_signal() {
        let json = r#"{"type":"kill","exec_id":"abc"}"#;
        let parsed: Request = serde_json::from_str(json).unwrap();
        match parsed {
            Request::Kill(k) => {
                assert_eq!(k.exec_id, "abc");
                assert_eq!(k.signal, 15);
            }
            _ => panic!("expected Kill"),
        }
    }
}
