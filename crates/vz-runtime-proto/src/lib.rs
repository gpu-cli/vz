/// gRPC service definitions for the vz Runtime V2 control-plane protocol.
pub mod vz {
    pub mod runtime {
        pub mod v2 {
            #![allow(clippy::disallowed_methods)]
            #![allow(clippy::missing_docs_in_private_items)]
            #![allow(clippy::large_enum_variant)]
            #![allow(clippy::doc_markdown)]
            include!("generated/vz.runtime.v2.rs");
        }
    }
}

/// Convenience re-export so consumers can write `use vz_runtime_proto::*`.
pub use vz::runtime::v2::*;

/// Convenience re-export for runtime V2 types under a dedicated namespace.
pub mod runtime_v2 {
    pub use crate::vz::runtime::v2::*;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use prost::Message;
    use std::collections::BTreeMap;

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum RpcMode {
        Unary,
        ServerStreaming,
    }

    fn parse_runtime_v2_rpc_modes(proto: &str) -> BTreeMap<String, RpcMode> {
        let mut modes = BTreeMap::new();
        for line in proto.lines().map(str::trim) {
            if !line.starts_with("rpc ") {
                continue;
            }
            let Some((name, _rest)) = line
                .strip_prefix("rpc ")
                .and_then(|raw| raw.split_once('('))
            else {
                continue;
            };
            let mode = if line.contains("returns (stream ") {
                RpcMode::ServerStreaming
            } else {
                RpcMode::Unary
            };
            modes.insert(name.trim().to_string(), mode);
        }
        modes
    }

    fn expected_runtime_v2_rpc_modes() -> BTreeMap<String, RpcMode> {
        [
            ("CreateSandbox", RpcMode::ServerStreaming),
            ("GetSandbox", RpcMode::Unary),
            ("ListSandboxes", RpcMode::Unary),
            ("TerminateSandbox", RpcMode::ServerStreaming),
            ("OpenSandboxShell", RpcMode::ServerStreaming),
            ("CloseSandboxShell", RpcMode::ServerStreaming),
            ("OpenLease", RpcMode::Unary),
            ("GetLease", RpcMode::Unary),
            ("ListLeases", RpcMode::Unary),
            ("HeartbeatLease", RpcMode::Unary),
            ("CloseLease", RpcMode::Unary),
            ("CreateContainer", RpcMode::Unary),
            ("GetContainer", RpcMode::Unary),
            ("ListContainers", RpcMode::Unary),
            ("RemoveContainer", RpcMode::Unary),
            ("GetImage", RpcMode::Unary),
            ("ListImages", RpcMode::Unary),
            ("PullImage", RpcMode::ServerStreaming),
            ("PruneImages", RpcMode::ServerStreaming),
            ("CreateExecution", RpcMode::Unary),
            ("GetExecution", RpcMode::Unary),
            ("ListExecutions", RpcMode::Unary),
            ("CancelExecution", RpcMode::Unary),
            ("StreamExecOutput", RpcMode::ServerStreaming),
            ("WriteExecStdin", RpcMode::Unary),
            ("ResizeExecPty", RpcMode::Unary),
            ("SignalExec", RpcMode::Unary),
            ("CreateCheckpoint", RpcMode::Unary),
            ("GetCheckpoint", RpcMode::Unary),
            ("ListCheckpoints", RpcMode::Unary),
            ("RestoreCheckpoint", RpcMode::Unary),
            ("ForkCheckpoint", RpcMode::Unary),
            ("DiffCheckpoints", RpcMode::Unary),
            ("ExportCheckpoint", RpcMode::ServerStreaming),
            ("ImportCheckpoint", RpcMode::ServerStreaming),
            ("StartBuild", RpcMode::Unary),
            ("GetBuild", RpcMode::Unary),
            ("ListBuilds", RpcMode::Unary),
            ("CancelBuild", RpcMode::Unary),
            ("StreamBuildEvents", RpcMode::ServerStreaming),
            ("GetReceipt", RpcMode::Unary),
            ("ListEvents", RpcMode::Unary),
            ("StreamEvents", RpcMode::ServerStreaming),
            ("ApplyStack", RpcMode::ServerStreaming),
            ("TeardownStack", RpcMode::ServerStreaming),
            ("GetStackStatus", RpcMode::Unary),
            ("ListStackEvents", RpcMode::Unary),
            ("GetStackLogs", RpcMode::Unary),
            ("StopStackService", RpcMode::ServerStreaming),
            ("StartStackService", RpcMode::ServerStreaming),
            ("RestartStackService", RpcMode::ServerStreaming),
            ("CreateStackRunContainer", RpcMode::Unary),
            ("RemoveStackRunContainer", RpcMode::Unary),
            ("ReadFile", RpcMode::Unary),
            ("WriteFile", RpcMode::Unary),
            ("ListFiles", RpcMode::Unary),
            ("MakeDir", RpcMode::Unary),
            ("RemovePath", RpcMode::Unary),
            ("MovePath", RpcMode::Unary),
            ("CopyPath", RpcMode::Unary),
            ("ChmodPath", RpcMode::Unary),
            ("ChownPath", RpcMode::Unary),
            ("GetCapabilities", RpcMode::Unary),
        ]
        .into_iter()
        .map(|(name, mode)| (name.to_string(), mode))
        .collect()
    }

    // ── Runtime V2 proto-contract consistency tests ──────────────

    #[test]
    fn runtime_v2_services_cover_all_entity_types() {
        use crate::runtime_v2::*;

        // Verify key request message types exist by constructing defaults.
        let _ = CreateSandboxRequest::default();
        let _ = OpenLeaseRequest::default();
        let _ = CreateContainerRequest::default();
        let _ = GetImageRequest::default();
        let _ = CreateExecutionRequest::default();
        let _ = WriteExecStdinRequest::default();
        let _ = CreateCheckpointRequest::default();
        let _ = ExportCheckpointRequest::default();
        let _ = ImportCheckpointRequest::default();
        let _ = StartBuildRequest::default();
        let _ = GetReceiptRequest::default();
        let _ = ListEventsRequest::default();
        let _ = ApplyStackRequest::default();
        let _ = StackRunContainerRequest::default();
        let _ = GetCapabilitiesRequest::default();

        // Verify response types.
        let _ = SandboxResponse::default();
        let _ = LeaseResponse::default();
        let _ = ContainerResponse::default();
        let _ = ImageResponse::default();
        let _ = ExecutionResponse::default();
        let _ = CheckpointResponse::default();
        let _ = BuildResponse::default();
        let _ = ReceiptResponse::default();
        let _ = ListEventsResponse::default();
        let _ = ApplyStackResponse::default();
        let _ = TeardownStackResponse::default();
        let _ = GetStackStatusResponse::default();
        let _ = ListStackEventsResponse::default();
        let _ = GetStackLogsResponse::default();
        let _ = StackServiceActionResponse::default();
        let _ = StackRunContainerResponse::default();
        let _ = GetCapabilitiesResponse::default();

        // Verify payload types used in responses.
        let _ = SandboxPayload::default();
        let _ = LeasePayload::default();
        let _ = ContainerPayload::default();
        let _ = ImagePayload::default();
        let _ = ExecutionPayload::default();
        let _ = CheckpointPayload::default();
        let _ = ExportCheckpointCompletion::default();
        let _ = ImportCheckpointCompletion::default();
        let _ = BuildPayload::default();
        let _ = ReceiptPayload::default();
        let _ = StackServiceStatus::default();
        let _ = StackServiceLog::default();

        // Verify list responses.
        let _ = ListSandboxesResponse::default();
        let _ = ListLeasesResponse::default();
        let _ = ListContainersResponse::default();
        let _ = ListImagesResponse::default();
        let _ = ListExecutionsResponse::default();
        let _ = ListCheckpointsResponse::default();
        let _ = ListBuildsResponse::default();

        // Verify streaming types.
        let _ = ExecOutputEvent::default();
        let _ = ExportCheckpointEvent::default();
        let _ = ImportCheckpointEvent::default();
        let _ = BuildEvent::default();
        let _ = RuntimeEvent::default();
    }

    #[test]
    fn runtime_v2_request_metadata_fields() {
        use crate::runtime_v2::RequestMetadata;
        let meta = RequestMetadata {
            request_id: "req-123".into(),
            idempotency_key: "idem-456".into(),
            trace_id: "trace-789".into(),
        };
        assert_eq!(meta.request_id, "req-123");
        assert_eq!(meta.idempotency_key, "idem-456");
        assert_eq!(meta.trace_id, "trace-789");
    }

    #[test]
    fn runtime_v2_error_detail_fields() {
        use crate::runtime_v2::ErrorDetail;
        let err = ErrorDetail {
            code: "not_found".into(),
            message: "sandbox not found".into(),
            request_id: "req-abc".into(),
        };
        assert_eq!(err.code, "not_found");
        assert_eq!(err.message, "sandbox not found");
        assert_eq!(err.request_id, "req-abc");
    }

    #[test]
    fn runtime_v2_rpc_modes_are_explicit_and_stable() {
        let proto = include_str!("../proto/runtime_v2.proto");
        let observed = parse_runtime_v2_rpc_modes(proto);
        let expected = expected_runtime_v2_rpc_modes();
        assert_eq!(
            observed, expected,
            "Runtime V2 RPC mode contract changed; classify new RPCs and keep long-running surfaces stream-first."
        );
    }

    #[test]
    fn runtime_v2_all_rpc_request_types_have_metadata() {
        use crate::runtime_v2::*;

        // Every mutation/query request should have an optional metadata field.
        // Constructing defaults ensures the field exists (it will be None).
        let create_sandbox = CreateSandboxRequest::default();
        assert!(create_sandbox.metadata.is_none());

        let open_lease = OpenLeaseRequest::default();
        assert!(open_lease.metadata.is_none());

        let create_container = CreateContainerRequest::default();
        assert!(create_container.metadata.is_none());

        let create_execution = CreateExecutionRequest::default();
        assert!(create_execution.metadata.is_none());

        let write_exec_stdin = WriteExecStdinRequest::default();
        assert!(write_exec_stdin.metadata.is_none());

        let create_checkpoint = CreateCheckpointRequest::default();
        assert!(create_checkpoint.metadata.is_none());

        let start_build = StartBuildRequest::default();
        assert!(start_build.metadata.is_none());

        let list_events = ListEventsRequest::default();
        assert!(list_events.metadata.is_none());

        let apply_stack = ApplyStackRequest::default();
        assert!(apply_stack.metadata.is_none());

        let teardown_stack = TeardownStackRequest::default();
        assert!(teardown_stack.metadata.is_none());

        let get_stack_status = GetStackStatusRequest::default();
        assert!(get_stack_status.metadata.is_none());

        let list_stack_events = ListStackEventsRequest::default();
        assert!(list_stack_events.metadata.is_none());

        let get_stack_logs = GetStackLogsRequest::default();
        assert!(get_stack_logs.metadata.is_none());

        let stack_action = StackServiceActionRequest::default();
        assert!(stack_action.metadata.is_none());

        let stack_run = StackRunContainerRequest::default();
        assert!(stack_run.metadata.is_none());

        let get_caps = GetCapabilitiesRequest::default();
        assert!(get_caps.metadata.is_none());

        // GET/single-entity requests.
        let get_sandbox = GetSandboxRequest::default();
        assert!(get_sandbox.metadata.is_none());

        let get_lease = GetLeaseRequest::default();
        assert!(get_lease.metadata.is_none());

        let get_container = GetContainerRequest::default();
        assert!(get_container.metadata.is_none());

        let get_execution = GetExecutionRequest::default();
        assert!(get_execution.metadata.is_none());

        let get_checkpoint = GetCheckpointRequest::default();
        assert!(get_checkpoint.metadata.is_none());

        let get_build = GetBuildRequest::default();
        assert!(get_build.metadata.is_none());
    }

    #[test]
    fn runtime_v2_sandbox_payload_round_trip() {
        use crate::runtime_v2::SandboxPayload;
        let payload = SandboxPayload {
            sandbox_id: "sbx-test".into(),
            backend: "macos_vz".into(),
            state: "ready".into(),
            cpus: 4,
            memory_mb: 2048,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_100,
            labels: [("env".to_string(), "staging".to_string())]
                .into_iter()
                .collect(),
        };
        let encoded = payload.encode_to_vec();
        let decoded = SandboxPayload::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.sandbox_id, "sbx-test");
        assert_eq!(decoded.backend, "macos_vz");
        assert_eq!(decoded.cpus, 4);
        assert_eq!(decoded.memory_mb, 2048);
        assert_eq!(decoded.labels.get("env").unwrap(), "staging");
    }

    #[test]
    fn runtime_v2_checkpoint_payload_round_trip() {
        use crate::runtime_v2::CheckpointPayload;
        let payload = CheckpointPayload {
            checkpoint_id: "ckpt-test".into(),
            sandbox_id: "sbx-test".into(),
            parent_checkpoint_id: "ckpt-parent".into(),
            checkpoint_class: "fs_quick".into(),
            state: "ready".into(),
            compatibility_fingerprint: "kernel-6.1-arm64".into(),
            created_at: 1_700_000_000,
            retention_tag: "pre-session".into(),
            retention_protected: true,
            retention_gc_reason: String::new(),
            retention_expires_at: 0,
        };
        let encoded = payload.encode_to_vec();
        let decoded = CheckpointPayload::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.checkpoint_id, "ckpt-test");
        assert_eq!(decoded.compatibility_fingerprint, "kernel-6.1-arm64");
        assert_eq!(decoded.parent_checkpoint_id, "ckpt-parent");
        assert_eq!(decoded.retention_tag, "pre-session");
        assert!(decoded.retention_protected);
    }
}
