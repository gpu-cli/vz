//! Vsock wire protocol types for host-guest communication.
//!
//! Defines the [`Request`] and [`Response`] enums used to communicate between
//! the host and guest agent over a vsock connection.
//!
//! Messages are serialized as JSON with `#[serde(tag = "type")]`
//! discriminators and framed as `len(u32 LE) + payload`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use crate::channel::{Channel, ChannelError, read_frame, write_frame};

/// Current protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default vsock port for the guest agent.
pub const AGENT_PORT: u32 = 7424;

/// Maximum frame size in bytes (16 MiB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Host-to-guest handshake sent on every new connection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Handshake {
    /// Protocol version the host supports.
    pub protocol_version: u32,
    /// Capabilities the host supports (extensibility mechanism).
    pub capabilities: Vec<String>,
}

/// Guest-to-host handshake acknowledgment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeAck {
    /// Highest protocol version the guest supports (up to the host's version).
    pub protocol_version: u32,
    /// Guest agent software version (e.g., "0.1.0").
    pub agent_version: String,
    /// Guest OS identifier (`"macos"` or `"linux"`).
    pub os: String,
    /// Capabilities the guest supports.
    pub capabilities: Vec<String>,
}

/// Output from a command executed inside the guest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecOutput {
    /// Exit code of the command (0 = success).
    pub exit_code: i32,
    /// Standard output collected as a string.
    pub stdout: String,
    /// Standard error collected as a string.
    pub stderr: String,
}

/// A streaming event from a running command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExecEvent {
    /// A chunk of stdout data.
    Stdout(#[serde(with = "base64_serde")] Vec<u8>),
    /// A chunk of stderr data.
    Stderr(#[serde(with = "base64_serde")] Vec<u8>),
    /// The command exited with the given code.
    Exit(i32),
}

/// A stream of events from a running command.
pub struct ExecStream {
    /// The exec ID for this running command.
    exec_id: u64,
    /// Whether we've seen the exit event.
    done: bool,
    /// Channel to the guest agent (None in test mode).
    channel: Option<Arc<Channel<Request, Response>>>,
}

impl ExecStream {
    /// Create a new stream for a running exec request.
    pub fn new(exec_id: u64, channel: Option<Arc<Channel<Request, Response>>>) -> Self {
        Self {
            exec_id,
            done: false,
            channel,
        }
    }

    /// Get the exec ID for this running command.
    pub fn exec_id(&self) -> u64 {
        self.exec_id
    }

    /// Read the next event from the stream.
    ///
    /// Returns `None` after the command has exited (after yielding `ExecEvent::Exit`).
    pub async fn next(&mut self) -> Option<ExecEvent> {
        if self.done {
            return None;
        }

        if let Some(ref channel) = self.channel {
            match channel.recv().await {
                Ok(resp) => match resp {
                    Response::Stdout { data, exec_id } if exec_id == self.exec_id => {
                        Some(ExecEvent::Stdout(data))
                    }
                    Response::Stderr { data, exec_id } if exec_id == self.exec_id => {
                        Some(ExecEvent::Stderr(data))
                    }
                    Response::ExitCode { code, exec_id } if exec_id == self.exec_id => {
                        self.done = true;
                        Some(ExecEvent::Exit(code))
                    }
                    Response::ExecError { .. } => {
                        self.done = true;
                        Some(ExecEvent::Exit(-1))
                    }
                    _ => Box::pin(self.next()).await,
                },
                Err(_) => {
                    self.done = true;
                    Some(ExecEvent::Exit(-1))
                }
            }
        } else {
            self.done = true;
            Some(ExecEvent::Exit(0))
        }
    }

    /// Collect all remaining events into an ExecOutput.
    pub async fn collect(mut self) -> Result<ExecOutput, ChannelError> {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code = -1;

        while let Some(event) = self.next().await {
            match event {
                ExecEvent::Stdout(data) => stdout.extend_from_slice(&data),
                ExecEvent::Stderr(data) => stderr.extend_from_slice(&data),
                ExecEvent::Exit(code) => exit_code = code,
            }
        }

        Ok(ExecOutput {
            exit_code,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }
}

/// Guest resource usage metrics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceStats {
    pub cpu_usage_percent: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub process_count: u32,
    pub load_average: [f64; 3],
}

/// Per-service network configuration for stack VM network setup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkServiceConfig {
    /// Service name, also used as the network namespace name.
    pub name: String,
    /// IP address with CIDR prefix (e.g., `"172.20.0.2/24"`).
    pub addr: String,
}

/// OCI runtime state for a container.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OciContainerState {
    /// Container identifier.
    pub id: String,
    /// OCI runtime status string (for example: `created`, `running`, `stopped`).
    pub status: String,
    /// Optional process ID reported by the runtime.
    pub pid: Option<u32>,
    /// Optional bundle path backing this container.
    pub bundle_path: Option<String>,
}

/// OCI exec result payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OciExecResult {
    /// Exit code returned by the OCI runtime exec operation.
    pub exit_code: i32,
    /// Captured stdout from the OCI runtime exec operation.
    pub stdout: String,
    /// Captured stderr from the OCI runtime exec operation.
    pub stderr: String,
}

/// Typed payloads for OCI lifecycle responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum OciPayload {
    /// No payload for successful operations that only need an acknowledgment.
    Empty,
    /// Container state payload from `OciState`.
    State {
        /// OCI runtime state for the requested container.
        state: OciContainerState,
    },
    /// Exec output payload from `OciExec`.
    Exec {
        /// Result of executing a command inside the running OCI container.
        result: OciExecResult,
    },
}

/// Request sent from host to guest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum Request {
    /// Execute a command in the guest.
    Exec {
        /// Unique request ID for correlating the response.
        id: u64,
        /// Program to execute.
        command: String,
        /// Arguments to the program.
        args: Vec<String>,
        /// Working directory (guest-side path). If `None`, uses the agent's cwd.
        working_dir: Option<String>,
        /// Environment variables as key-value pairs.
        env: Vec<(String, String)>,
        /// Run as this user. If `None`, runs as the agent's user (root).
        user: Option<String>,
    },
    /// Write data to a running process's stdin.
    StdinWrite {
        /// Unique request ID.
        id: u64,
        /// ID of the [`Exec`](Request::Exec) request whose stdin to write to.
        exec_id: u64,
        /// Raw bytes to write, base64-encoded in JSON.
        #[serde(with = "base64_serde")]
        data: Vec<u8>,
    },
    /// Close a running process's stdin. Fire-and-forget (no response).
    StdinClose {
        /// ID of the [`Exec`](Request::Exec) request whose stdin to close.
        exec_id: u64,
    },
    /// Send a signal to a running process. Fire-and-forget (no response).
    Signal {
        /// ID of the [`Exec`](Request::Exec) request to signal.
        exec_id: u64,
        /// Signal number (e.g., 15 for SIGTERM, 9 for SIGKILL).
        signal: i32,
    },
    /// Request system information from the guest.
    SystemInfo {
        /// Unique request ID.
        id: u64,
    },
    /// Request resource usage statistics from the guest.
    ResourceStats {
        /// Unique request ID.
        id: u64,
    },
    /// Ping the guest agent (health check).
    Ping {
        /// Unique request ID.
        id: u64,
    },
    /// Open a port forward stream over a secondary vsock connection.
    PortForward {
        /// Unique request ID.
        id: u64,
        /// Target port inside the guest.
        target_port: u16,
        /// Protocol string ("tcp" or "udp").
        protocol: String,
    },
    /// Create a container in the OCI runtime from a prepared bundle.
    OciCreate {
        /// OCI container identifier.
        id: String,
        /// Absolute path to the OCI bundle (`config.json` + rootfs).
        bundle_path: String,
    },
    /// Start an OCI container that has already been created.
    OciStart {
        /// OCI container identifier.
        id: String,
    },
    /// Query current OCI runtime state for a container.
    OciState {
        /// OCI container identifier.
        id: String,
    },
    /// Execute a command in a running OCI container.
    OciExec {
        /// OCI container identifier.
        id: String,
        /// Command to run.
        command: String,
        /// Command arguments.
        args: Vec<String>,
        /// Environment variables as key-value pairs.
        env: Vec<(String, String)>,
        /// Working directory inside the container.
        cwd: Option<String>,
        /// Optional user identity inside the container.
        user: Option<String>,
    },
    /// Send a signal to a running OCI container.
    OciKill {
        /// OCI container identifier.
        id: String,
        /// Signal name or value expected by the runtime (for example: `SIGTERM`).
        signal: String,
    },
    /// Delete an OCI container from runtime state.
    OciDelete {
        /// OCI container identifier.
        id: String,
        /// Force delete if true.
        force: bool,
    },
    /// Set up per-service network isolation for a stack.
    ///
    /// Creates a bridge, per-service network namespaces, veth pairs,
    /// and IP routes so that services can communicate via their
    /// assigned addresses.
    NetworkSetup {
        /// Unique request ID.
        id: u64,
        /// Stack identifier, used to name the bridge (`br-<stack_id>`).
        stack_id: String,
        /// Per-service network configuration.
        services: Vec<NetworkServiceConfig>,
    },
    /// Tear down the network resources created by [`NetworkSetup`].
    NetworkTeardown {
        /// Unique request ID.
        id: u64,
        /// Stack identifier whose network should be torn down.
        stack_id: String,
        /// Service names whose network namespaces should be removed.
        service_names: Vec<String>,
    },
}

/// Response sent from guest to host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum Response {
    /// Chunk of stdout from a running process.
    Stdout {
        /// ID of the originating [`Exec`](Request::Exec) request.
        exec_id: u64,
        /// Raw stdout bytes, base64-encoded in JSON.
        #[serde(with = "base64_serde")]
        data: Vec<u8>,
    },
    /// Chunk of stderr from a running process.
    Stderr {
        /// ID of the originating [`Exec`](Request::Exec) request.
        exec_id: u64,
        /// Raw stderr bytes, base64-encoded in JSON.
        #[serde(with = "base64_serde")]
        data: Vec<u8>,
    },
    /// Process exited with a code.
    ExitCode {
        /// ID of the originating [`Exec`](Request::Exec) request.
        exec_id: u64,
        /// Exit code (0 = success).
        code: i32,
    },
    /// An exec request failed to start.
    ExecError {
        /// ID of the originating [`Exec`](Request::Exec) request.
        id: u64,
        /// Human-readable error description.
        error: String,
    },
    /// System information response.
    SystemInfoResult {
        /// ID of the originating [`SystemInfo`](Request::SystemInfo) request.
        id: u64,
        /// Number of CPU cores.
        cpu_count: u32,
        /// Total memory in bytes.
        memory_bytes: u64,
        /// Free disk space in bytes.
        disk_free_bytes: u64,
        /// Guest OS version string.
        os_version: String,
    },
    /// Resource usage statistics response.
    ResourceStatsResult {
        /// ID of the originating [`ResourceStats`](Request::ResourceStats) request.
        id: u64,
        /// CPU usage as a percentage (0.0-100.0).
        cpu_usage_percent: f64,
        /// Used memory in bytes.
        memory_used_bytes: u64,
        /// Total memory in bytes.
        memory_total_bytes: u64,
        /// Used disk space in bytes.
        disk_used_bytes: u64,
        /// Total disk space in bytes.
        disk_total_bytes: u64,
        /// Number of running processes.
        process_count: u32,
        /// Load averages (1-min, 5-min, 15-min).
        load_average: [f64; 3],
    },
    /// Pong response to a [`Ping`](Request::Ping).
    Pong {
        /// ID of the originating [`Ping`](Request::Ping) request.
        id: u64,
    },
    /// Port forward handshake response.
    PortForwardReady {
        /// ID of the originating [`Request::PortForward`] request.
        id: u64,
    },
    /// Generic error response.
    Error {
        /// ID of the originating request.
        id: u64,
        /// Human-readable error description.
        error: String,
    },
    /// Generic success acknowledgment.
    Ok {
        /// ID of the originating request.
        id: u64,
    },
    /// OCI lifecycle success response.
    OciOk {
        /// OCI container identifier.
        id: String,
        /// Typed payload for the completed OCI operation.
        payload: OciPayload,
    },
    /// OCI lifecycle failure response.
    OciError {
        /// OCI container identifier.
        id: String,
        /// Runtime error code.
        code: i32,
        /// Human-readable error message.
        message: String,
    },
    /// Network setup completed successfully.
    NetworkSetupOk {
        /// ID of the originating [`NetworkSetup`](Request::NetworkSetup) request.
        id: u64,
    },
    /// Network setup failed.
    NetworkSetupError {
        /// ID of the originating [`NetworkSetup`](Request::NetworkSetup) request.
        id: u64,
        /// Human-readable error description.
        error: String,
    },
    /// Network teardown completed successfully.
    NetworkTeardownOk {
        /// ID of the originating [`NetworkTeardown`](Request::NetworkTeardown) request.
        id: u64,
    },
    /// Network teardown failed.
    NetworkTeardownError {
        /// ID of the originating [`NetworkTeardown`](Request::NetworkTeardown) request.
        id: u64,
        /// Human-readable error description.
        error: String,
    },
}

/// Serde helper for encoding `Vec<u8>` as base64 strings in JSON.
pub mod base64_serde {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize `Vec<u8>` as a base64-encoded string.
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    /// Deserialize a base64-encoded string into `Vec<u8>`.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_round_trip() {
        let handshake = Handshake {
            protocol_version: 1,
            capabilities: vec!["resource_stats".to_string()],
        };
        let json = serde_json::to_string(&handshake).expect("serialize");
        let deserialized: Handshake = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(handshake, deserialized);
    }

    #[test]
    fn handshake_ack_round_trip() {
        let ack = HandshakeAck {
            protocol_version: 1,
            agent_version: "0.1.0".to_string(),
            os: "macos".to_string(),
            capabilities: vec![],
        };
        let json = serde_json::to_string(&ack).expect("serialize");
        let deserialized: HandshakeAck = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ack, deserialized);
    }

    #[test]
    fn request_exec_round_trip() {
        let req = Request::Exec {
            id: 1,
            command: "cargo".to_string(),
            args: vec!["build".to_string(), "--release".to_string()],
            working_dir: Some("/mnt/workspace/my-project".to_string()),
            env: vec![("RUST_LOG".to_string(), "debug".to_string())],
            user: Some("dev".to_string()),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"Exec""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_port_forward_round_trip() {
        let req = Request::PortForward {
            id: 7,
            target_port: 8080,
            protocol: "tcp".to_string(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_stdin_write_base64_round_trip() {
        let req = Request::StdinWrite {
            id: 2,
            exec_id: 1,
            data: b"hello world\n".to_vec(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""data":"aGVsbG8gd29ybGQK""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn response_port_forward_ready_round_trip() {
        let resp = Response::PortForwardReady { id: 7 };
        let json = serde_json::to_string(&resp).expect("serialize");
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_resource_stats_result_round_trip() {
        let resp = Response::ResourceStatsResult {
            id: 6,
            cpu_usage_percent: 45.2,
            memory_used_bytes: 4_000_000_000,
            memory_total_bytes: 8_589_934_592,
            disk_used_bytes: 30_000_000_000,
            disk_total_bytes: 100_000_000_000,
            process_count: 142,
            load_average: [1.5, 2.0, 1.8],
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn request_oci_create_round_trip() {
        let req = Request::OciCreate {
            id: "svc-web".to_string(),
            bundle_path: "/run/vz-oci/bundles/svc-web".to_string(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"OciCreate""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_oci_start_round_trip() {
        let req = Request::OciStart {
            id: "svc-web".to_string(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"OciStart""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_oci_state_round_trip() {
        let req = Request::OciState {
            id: "svc-web".to_string(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"OciState""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_oci_exec_round_trip() {
        let req = Request::OciExec {
            id: "svc-web".to_string(),
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), "echo ready".to_string()],
            env: vec![
                (
                    "PATH".to_string(),
                    "/usr/local/bin:/usr/bin:/bin".to_string(),
                ),
                ("MODE".to_string(), "prod".to_string()),
            ],
            cwd: Some("/workspace".to_string()),
            user: Some("1000:1000".to_string()),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"OciExec""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_oci_kill_round_trip() {
        let req = Request::OciKill {
            id: "svc-web".to_string(),
            signal: "SIGTERM".to_string(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"OciKill""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_oci_delete_round_trip() {
        let req = Request::OciDelete {
            id: "svc-web".to_string(),
            force: true,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"OciDelete""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn response_oci_ok_round_trip() {
        let resp = Response::OciOk {
            id: "svc-web".to_string(),
            payload: OciPayload::State {
                state: OciContainerState {
                    id: "svc-web".to_string(),
                    status: "running".to_string(),
                    pid: Some(4242),
                    bundle_path: Some("/run/vz-oci/bundles/svc-web".to_string()),
                },
            },
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"OciOk""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_oci_error_round_trip() {
        let resp = Response::OciError {
            id: "svc-web".to_string(),
            code: 125,
            message: "bundle not found".to_string(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"OciError""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn request_network_setup_round_trip() {
        let req = Request::NetworkSetup {
            id: 10,
            stack_id: "my-stack".to_string(),
            services: vec![
                NetworkServiceConfig {
                    name: "web".to_string(),
                    addr: "172.20.0.2/24".to_string(),
                },
                NetworkServiceConfig {
                    name: "db".to_string(),
                    addr: "172.20.0.3/24".to_string(),
                },
            ],
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"NetworkSetup""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_network_teardown_round_trip() {
        let req = Request::NetworkTeardown {
            id: 11,
            stack_id: "my-stack".to_string(),
            service_names: vec!["web".to_string(), "db".to_string()],
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"NetworkTeardown""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn response_network_setup_ok_round_trip() {
        let resp = Response::NetworkSetupOk { id: 10 };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"NetworkSetupOk""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_network_setup_error_round_trip() {
        let resp = Response::NetworkSetupError {
            id: 10,
            error: "bridge creation failed".to_string(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"NetworkSetupError""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }
}
