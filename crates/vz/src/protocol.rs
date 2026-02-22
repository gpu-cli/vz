//! Protocol-agnostic data types shared between host and guest.
//!
//! These types are transport-neutral and used by both the legacy JSON
//! framing and the gRPC/protobuf transport.

use serde::{Deserialize, Serialize};

/// Default vsock port for the guest agent.
pub const AGENT_PORT: u32 = 7424;

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
#[derive(Debug, Clone, PartialEq)]
pub enum ExecEvent {
    /// A chunk of stdout data.
    Stdout(Vec<u8>),
    /// A chunk of stderr data.
    Stderr(Vec<u8>),
    /// The command exited with the given code.
    Exit(i32),
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
