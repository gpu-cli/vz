//! Vsock wire protocol types for host-guest communication.
//!
//! Defines the [`Request`] and [`Response`] enums used to communicate between
//! the host (vz-sandbox) and guest (vz-guest-agent) over a vsock connection.
//! Messages are serialized as JSON with `#[serde(tag = "type")]` discriminators.
//!
//! Binary data fields (stdout/stderr/stdin) use base64 encoding via the
//! [`base64_serde`] module.
//!
//! # Wire Format
//!
//! Each message is framed as a 4-byte little-endian u32 length prefix followed
//! by the JSON payload. See [`crate::channel`] for the framing implementation.
//!
//! # Connection Lifecycle
//!
//! 1. Host connects to guest on vsock port 7424
//! 2. Host sends [`Handshake`]
//! 3. Guest replies with [`HandshakeAck`]
//! 4. Host sends [`Request`] messages, guest replies with [`Response`] messages

use serde::{Deserialize, Serialize};

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
    /// Capabilities the guest supports.
    pub capabilities: Vec<String>,
}

/// Request sent from host to guest.
///
/// Each variant is tagged in JSON as `{"type": "VariantName", ...}`.
/// Requests with an `id` field produce exactly one correlated [`Response`].
/// Fire-and-forget requests ([`Signal`](Request::Signal), [`StdinClose`](Request::StdinClose))
/// have no `id` and produce no response.
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
        /// Requires the `"user_exec"` capability.
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
}

/// Response sent from guest to host.
///
/// Each variant is tagged in JSON as `{"type": "VariantName", ...}`.
/// Responses are correlated to requests via `id` or `exec_id` fields.
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
}

/// Serde helper for encoding `Vec<u8>` as base64 strings in JSON.
///
/// Apply to fields with `#[serde(with = "base64_serde")]`.
///
/// # Example
///
/// ```rust
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Message {
///     #[serde(with = "vz_sandbox::protocol::base64_serde")]
///     data: Vec<u8>,
/// }
/// ```
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
    fn request_stdin_write_base64_round_trip() {
        let req = Request::StdinWrite {
            id: 2,
            exec_id: 1,
            data: b"hello world\n".to_vec(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        // Verify base64 encoding is used (not raw bytes or array of numbers)
        assert!(json.contains(r#""data":"aGVsbG8gd29ybGQK""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_stdin_close_round_trip() {
        let req = Request::StdinClose { exec_id: 1 };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"StdinClose""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_signal_round_trip() {
        let req = Request::Signal {
            exec_id: 1,
            signal: 15,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"Signal""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_system_info_round_trip() {
        let req = Request::SystemInfo { id: 5 };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"SystemInfo""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_resource_stats_round_trip() {
        let req = Request::ResourceStats { id: 6 };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"ResourceStats""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn request_ping_round_trip() {
        let req = Request::Ping { id: 7 };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""type":"Ping""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn response_stdout_base64_round_trip() {
        let resp = Response::Stdout {
            exec_id: 1,
            data: b"   Compiling serde v1.0.197\n".to_vec(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"Stdout""#));
        // Verify base64 encoding
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_stderr_base64_round_trip() {
        let resp = Response::Stderr {
            exec_id: 1,
            data: b"warning: unused variable `x`\n".to_vec(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_exit_code_round_trip() {
        let resp = Response::ExitCode {
            exec_id: 1,
            code: 0,
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"ExitCode""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_exec_error_round_trip() {
        let resp = Response::ExecError {
            id: 1,
            error: "command not found: foobar".to_string(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_system_info_result_round_trip() {
        let resp = Response::SystemInfoResult {
            id: 5,
            cpu_count: 4,
            memory_bytes: 8_589_934_592,
            disk_free_bytes: 50_000_000_000,
            os_version: "macOS 14.3".to_string(),
        };
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
    fn response_pong_round_trip() {
        let resp = Response::Pong { id: 7 };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"Pong""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_error_round_trip() {
        let resp = Response::Error {
            id: 8,
            error: "unknown request".to_string(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn response_ok_round_trip() {
        let resp = Response::Ok { id: 9 };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains(r#""type":"Ok""#));
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn base64_serde_known_values() {
        // From the planning doc: "   Compiling serde v1.0.197\n" encodes to
        // "ICAgQ29tcGlsaW5nIHNlcmRlIHYxLjAuMTk3Cg=="
        let resp = Response::Stdout {
            exec_id: 1,
            data: b"   Compiling serde v1.0.197\n".to_vec(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains("ICAgQ29tcGlsaW5nIHNlcmRlIHYxLjAuMTk3Cg=="));
    }

    #[test]
    fn base64_serde_empty_data() {
        let req = Request::StdinWrite {
            id: 1,
            exec_id: 1,
            data: vec![],
        };
        let json = serde_json::to_string(&req).expect("serialize");
        assert!(json.contains(r#""data":"""#));
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn base64_serde_binary_data() {
        // Test with non-UTF-8 binary data
        let data: Vec<u8> = (0..=255).collect();
        let resp = Response::Stdout {
            exec_id: 1,
            data: data.clone(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let deserialized: Response = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(resp, deserialized);
        if let Response::Stdout {
            data: roundtripped, ..
        } = deserialized
        {
            assert_eq!(data, roundtripped);
        }
    }

    #[test]
    fn request_json_tag_format() {
        // Verify the tag appears as "type" key in JSON
        let req = Request::Ping { id: 1 };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed["type"], "Ping");
        assert_eq!(parsed["id"], 1);
    }

    #[test]
    fn response_json_tag_format() {
        let resp = Response::Pong { id: 1 };
        let json = serde_json::to_string(&resp).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed["type"], "Pong");
        assert_eq!(parsed["id"], 1);
    }

    #[test]
    fn handshake_default_values() {
        let handshake = Handshake {
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec![],
        };
        let json = serde_json::to_string(&handshake).expect("serialize");
        assert_eq!(json, r#"{"protocol_version":1,"capabilities":[]}"#);
    }

    #[test]
    fn exec_with_minimal_fields() {
        // Test Exec with None/empty optional fields
        let req = Request::Exec {
            id: 1,
            command: "ls".to_string(),
            args: vec![],
            working_dir: None,
            env: vec![],
            user: None,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let deserialized: Request = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn wire_example_from_spec() {
        // Reproduce the exact example from 03-vsock-protocol.md
        let json = r#"{ "type": "Stdout", "exec_id": 1, "data": "ICAgQ29tcGlsaW5nIHNlcmRlIHYxLjAuMTk3Cg==" }"#;
        let resp: Response = serde_json::from_str(json).expect("deserialize");
        if let Response::Stdout { exec_id, data } = resp {
            assert_eq!(exec_id, 1);
            assert_eq!(data, b"   Compiling serde v1.0.197\n");
        } else {
            panic!("expected Stdout variant");
        }
    }
}
