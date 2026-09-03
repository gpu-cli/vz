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

    fn parse_proto_block_values(proto: &str, kind: &str, name: &str) -> BTreeMap<String, u32> {
        let declaration = format!("{kind} {name} {{");
        let mut inside = false;
        let mut depth = 0_i32;
        let mut values = BTreeMap::new();

        for raw_line in proto.lines() {
            let line = raw_line.trim();
            if !inside {
                if line == declaration {
                    inside = true;
                    depth = 1;
                }
                continue;
            }

            depth += line.matches('{').count() as i32;
            depth -= line.matches('}').count() as i32;
            if depth == 0 {
                break;
            }

            let statement = line.split("//").next().unwrap_or_default().trim();
            let Some((left, right)) = statement.split_once('=') else {
                continue;
            };
            let Some(field_or_variant) = left.split_whitespace().last() else {
                continue;
            };
            let Some(number) = right
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u32>().ok())
            else {
                continue;
            };
            values.insert(field_or_variant.to_string(), number);
        }

        assert!(inside, "missing {kind} {name} in Runtime V2 schema");
        values
    }

    fn assert_proto_fields(proto: &str, message: &str, expected: &[(&str, u32)]) {
        let observed = parse_proto_block_values(proto, "message", message);
        let expected: BTreeMap<_, _> = expected
            .iter()
            .map(|(field, tag)| ((*field).to_string(), *tag))
            .collect();
        assert_eq!(
            observed, expected,
            "protobuf field inventory or tags changed for {message}"
        );
    }

    fn assert_proto_enum(proto: &str, name: &str, expected: &[(&str, u32)]) {
        let observed = parse_proto_block_values(proto, "enum", name);
        let expected: BTreeMap<_, _> = expected
            .iter()
            .map(|(variant, value)| ((*variant).to_string(), *value))
            .collect();
        assert_eq!(
            observed, expected,
            "protobuf enum inventory or discriminants changed for {name}"
        );
        let unspecified = expected
            .keys()
            .find(|variant| variant.ends_with("_UNSPECIFIED"))
            .unwrap_or_else(|| panic!("{name} must define an UNSPECIFIED variant"));
        assert_eq!(expected[unspecified], 0, "{name} UNSPECIFIED must be zero");
    }

    fn expected_runtime_v2_rpc_modes() -> BTreeMap<String, RpcMode> {
        [
            ("CreateSandbox", RpcMode::ServerStreaming),
            ("PrepareSpaceCache", RpcMode::ServerStreaming),
            ("ExportSpaceCache", RpcMode::ServerStreaming),
            ("ImportSpaceCache", RpcMode::ServerStreaming),
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
            ("ValidateLinuxVm", RpcMode::ServerStreaming),
            ("ListLinuxVmBases", RpcMode::Unary),
            ("GetLinuxVmBase", RpcMode::Unary),
            ("UpsertLinuxVmBase", RpcMode::ServerStreaming),
            ("DeleteLinuxVmBase", RpcMode::ServerStreaming),
            ("ApplyLinuxVmPatch", RpcMode::ServerStreaming),
            ("RollbackLinuxVmPatch", RpcMode::ServerStreaming),
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

    #[test]
    fn runtime_v2_topology_schema_inventory_and_tags_are_stable() {
        let proto = include_str!("../proto/runtime_v2.proto");

        for (message, fields) in [
            ("HostSpec", &[("os", 1), ("arch", 2)][..]),
            (
                "TargetSpec",
                &[
                    ("os", 1),
                    ("arch", 2),
                    ("image", 3),
                    ("version", 4),
                    ("channel", 5),
                    ("digest", 6),
                ],
            ),
            (
                "UnsupportedMachineCapability",
                &[("capability", 1), ("reason", 2)],
            ),
            ("CapabilitySet", &[("capabilities", 1), ("unsupported", 2)]),
            (
                "MachineResources",
                &[("cpus", 1), ("memory_mb", 2), ("disk_bytes", 3)],
            ),
            (
                "WorkspaceProjection",
                &[("binding", 1), ("target_path", 2), ("mode", 3)],
            ),
            (
                "MachineSpec",
                &[
                    ("schema_version", 1),
                    ("name", 2),
                    ("target", 3),
                    ("resources", 4),
                    ("requested_capabilities", 5),
                    ("workspace", 6),
                    ("profile", 7),
                ],
            ),
            (
                "NetworkSpec",
                &[("schema_version", 1), ("name", 2), ("kind", 3), ("cidr", 4)],
            ),
            (
                "EndpointSpec",
                &[
                    ("schema_version", 1),
                    ("name", 2),
                    ("machine", 3),
                    ("network", 4),
                    ("protocol", 5),
                    ("port", 6),
                    ("hostname", 7),
                ],
            ),
            (
                "EnvironmentSpec",
                &[
                    ("schema_version", 1),
                    ("machines", 2),
                    ("networks", 3),
                    ("endpoints", 4),
                ],
            ),
            (
                "ProjectDefinition",
                &[
                    ("schema_version", 1),
                    ("project_id", 2),
                    ("name", 3),
                    ("environment", 4),
                ],
            ),
            (
                "WorkspaceBinding",
                &[
                    ("schema_version", 1),
                    ("binding_id", 2),
                    ("project_id", 3),
                    ("environment_id", 4),
                    ("workspace_key", 5),
                    ("path_hint", 6),
                    ("name", 7),
                ],
            ),
            (
                "MachineIncarnation",
                &[
                    ("schema_version", 1),
                    ("incarnation_id", 2),
                    ("machine_id", 3),
                    ("generation", 4),
                    ("created_at", 5),
                ],
            ),
            (
                "MachineInstance",
                &[
                    ("schema_version", 1),
                    ("machine_id", 2),
                    ("environment_id", 3),
                    ("name", 4),
                    ("target", 5),
                    ("resources", 6),
                    ("requested_capabilities", 7),
                    ("negotiated_capabilities", 8),
                    ("backend", 9),
                    ("other_backend", 10),
                    ("incarnation", 11),
                    ("state", 12),
                    ("legacy_sandbox_id", 13),
                    ("profile", 14),
                ],
            ),
            (
                "NetworkInstance",
                &[
                    ("schema_version", 1),
                    ("network_id", 2),
                    ("environment_id", 3),
                    ("name", 4),
                ],
            ),
            (
                "EndpointInstance",
                &[
                    ("schema_version", 1),
                    ("endpoint_id", 2),
                    ("environment_id", 3),
                    ("machine_id", 4),
                    ("network_id", 5),
                    ("name", 6),
                ],
            ),
            (
                "OwnershipRecord",
                &[
                    ("schema_version", 1),
                    ("resource_kind", 2),
                    ("other_resource_kind", 3),
                    ("resource_id", 4),
                    ("environment_id", 5),
                    ("machine_id", 6),
                ],
            ),
            (
                "LegacyMigrationProvenance",
                &[
                    ("source_version", 1),
                    ("legacy_sandbox_id", 2),
                    ("unresolved_resources", 3),
                ],
            ),
            (
                "EnvironmentInstance",
                &[
                    ("schema_version", 1),
                    ("environment_id", 2),
                    ("project_id", 3),
                    ("name", 4),
                    ("definition_digest", 5),
                    ("state", 6),
                    ("bindings", 7),
                    ("machines", 8),
                    ("networks", 9),
                    ("endpoints", 10),
                    ("ownership", 11),
                    ("legacy_migration", 12),
                    ("created_at", 13),
                    ("updated_at", 14),
                ],
            ),
            (
                "ProjectState",
                &[
                    ("schema_version", 1),
                    ("definition", 2),
                    ("environments", 3),
                ],
            ),
            ("TopologyCandidate", &[("id", 1), ("name", 2)]),
            ("TopologyNotFoundDetail", &[("kind", 1), ("selector", 2)]),
            (
                "TopologyAmbiguityDetail",
                &[("kind", 1), ("selector", 2), ("candidates", 3)],
            ),
            (
                "TopologyUnsupportedTargetDetail",
                &[
                    ("host_os", 1),
                    ("host_arch", 2),
                    ("target_os", 3),
                    ("target_arch", 4),
                    ("requested_capabilities", 5),
                ],
            ),
            (
                "TopologyMissingCapabilityDetail",
                &[("machine_id", 1), ("capability", 2)],
            ),
            (
                "TopologyErrorDetail",
                &[
                    ("not_found", 1),
                    ("ambiguous", 2),
                    ("unsupported_target", 3),
                    ("missing_capability", 4),
                ],
            ),
        ] {
            assert_proto_fields(proto, message, fields);
        }
    }

    #[test]
    fn runtime_v2_topology_enum_inventory_is_stable_and_unspecified_is_zero() {
        let proto = include_str!("../proto/runtime_v2.proto");

        for (name, variants) in [
            (
                "OperatingSystem",
                &[
                    ("OPERATING_SYSTEM_UNSPECIFIED", 0),
                    ("OPERATING_SYSTEM_LINUX", 1),
                    ("OPERATING_SYSTEM_MACOS", 2),
                    ("OPERATING_SYSTEM_WINDOWS", 3),
                ][..],
            ),
            (
                "Architecture",
                &[
                    ("ARCHITECTURE_UNSPECIFIED", 0),
                    ("ARCHITECTURE_AARCH64", 1),
                    ("ARCHITECTURE_X86_64", 2),
                ],
            ),
            (
                "MachineCapability",
                &[
                    ("MACHINE_CAPABILITY_UNSPECIFIED", 0),
                    ("MACHINE_CAPABILITY_POSIX_EXEC", 1),
                    ("MACHINE_CAPABILITY_POSIX_PTY", 2),
                    ("MACHINE_CAPABILITY_SIGNALS", 3),
                    ("MACHINE_CAPABILITY_FILES", 4),
                    ("MACHINE_CAPABILITY_PORTS", 5),
                    ("MACHINE_CAPABILITY_DOCKER_ENGINE", 6),
                    ("MACHINE_CAPABILITY_COMPOSE", 7),
                    ("MACHINE_CAPABILITY_BUILDX", 8),
                    ("MACHINE_CAPABILITY_SNAPSHOT", 9),
                    ("MACHINE_CAPABILITY_SUSPEND", 10),
                    ("MACHINE_CAPABILITY_CHECKPOINT", 11),
                    ("MACHINE_CAPABILITY_GUI", 12),
                    ("MACHINE_CAPABILITY_WINDOWS_CONSOLE", 13),
                ],
            ),
            (
                "WorkspaceProjectionMode",
                &[
                    ("WORKSPACE_PROJECTION_MODE_UNSPECIFIED", 0),
                    ("WORKSPACE_PROJECTION_MODE_READ_WRITE", 1),
                    ("WORKSPACE_PROJECTION_MODE_READ_ONLY", 2),
                    ("WORKSPACE_PROJECTION_MODE_SNAPSHOT", 3),
                ],
            ),
            (
                "MachineProfile",
                &[
                    ("MACHINE_PROFILE_UNSPECIFIED", 0),
                    ("MACHINE_PROFILE_DEVELOPER", 1),
                    ("MACHINE_PROFILE_HARDENED", 2),
                ],
            ),
            (
                "NetworkKind",
                &[
                    ("NETWORK_KIND_UNSPECIFIED", 0),
                    ("NETWORK_KIND_PRIVATE", 1),
                    ("NETWORK_KIND_SIMULATED_PUBLIC", 2),
                ],
            ),
            (
                "EndpointProtocol",
                &[
                    ("ENDPOINT_PROTOCOL_UNSPECIFIED", 0),
                    ("ENDPOINT_PROTOCOL_TCP", 1),
                    ("ENDPOINT_PROTOCOL_UDP", 2),
                    ("ENDPOINT_PROTOCOL_HTTP", 3),
                    ("ENDPOINT_PROTOCOL_HTTPS", 4),
                ],
            ),
            (
                "EnvironmentState",
                &[
                    ("ENVIRONMENT_STATE_UNSPECIFIED", 0),
                    ("ENVIRONMENT_STATE_CREATING", 1),
                    ("ENVIRONMENT_STATE_RECONCILING", 2),
                    ("ENVIRONMENT_STATE_READY", 3),
                    ("ENVIRONMENT_STATE_STOPPED", 4),
                    ("ENVIRONMENT_STATE_DELETING", 5),
                    ("ENVIRONMENT_STATE_DELETED", 6),
                    ("ENVIRONMENT_STATE_FAILED", 7),
                ],
            ),
            (
                "MachineState",
                &[
                    ("MACHINE_STATE_UNSPECIFIED", 0),
                    ("MACHINE_STATE_CREATING", 1),
                    ("MACHINE_STATE_READY", 2),
                    ("MACHINE_STATE_STOPPED", 3),
                    ("MACHINE_STATE_FAILED", 4),
                ],
            ),
            (
                "MachineBackend",
                &[
                    ("MACHINE_BACKEND_UNSPECIFIED", 0),
                    ("MACHINE_BACKEND_MACOS_VIRTUALIZATION_LINUX", 1),
                    ("MACHINE_BACKEND_MACOS_NATIVE", 2),
                    ("MACHINE_BACKEND_LINUX_NATIVE", 3),
                    ("MACHINE_BACKEND_WINDOWS_LINUX", 4),
                    ("MACHINE_BACKEND_WINDOWS_NATIVE", 5),
                    ("MACHINE_BACKEND_OTHER", 6),
                ],
            ),
            (
                "OwnedResourceKind",
                &[
                    ("OWNED_RESOURCE_KIND_UNSPECIFIED", 0),
                    ("OWNED_RESOURCE_KIND_MACHINE", 1),
                    ("OWNED_RESOURCE_KIND_INCARNATION", 2),
                    ("OWNED_RESOURCE_KIND_DISK", 3),
                    ("OWNED_RESOURCE_KIND_SOCKET", 4),
                    ("OWNED_RESOURCE_KIND_DOCKER_CONTEXT", 5),
                    ("OWNED_RESOURCE_KIND_NETWORK", 6),
                    ("OWNED_RESOURCE_KIND_ENDPOINT", 7),
                    ("OWNED_RESOURCE_KIND_CREDENTIAL", 8),
                    ("OWNED_RESOURCE_KIND_FAULT", 9),
                    ("OWNED_RESOURCE_KIND_LEGACY_SANDBOX", 10),
                    ("OWNED_RESOURCE_KIND_OTHER", 11),
                ],
            ),
        ] {
            assert_proto_enum(proto, name, variants);
        }
    }

    #[test]
    fn runtime_v2_maximally_populated_project_state_round_trips() {
        use crate::runtime_v2::*;

        let host = HostSpec {
            os: OperatingSystem::Macos as i32,
            arch: Architecture::Aarch64 as i32,
        };
        let encoded_host = host.encode_to_vec();
        assert_eq!(HostSpec::decode(encoded_host.as_slice()).unwrap(), host);

        let all_capabilities = vec![
            MachineCapability::PosixExec as i32,
            MachineCapability::PosixPty as i32,
            MachineCapability::Signals as i32,
            MachineCapability::Files as i32,
            MachineCapability::Ports as i32,
            MachineCapability::DockerEngine as i32,
            MachineCapability::Compose as i32,
            MachineCapability::Buildx as i32,
            MachineCapability::Snapshot as i32,
            MachineCapability::Suspend as i32,
            MachineCapability::Checkpoint as i32,
            MachineCapability::Gui as i32,
            MachineCapability::WindowsConsole as i32,
        ];
        let requested_capabilities = CapabilitySet {
            capabilities: all_capabilities.clone(),
            unsupported: Vec::new(),
        };
        let negotiated_capabilities = CapabilitySet {
            capabilities: all_capabilities
                .iter()
                .copied()
                .filter(|capability| *capability != MachineCapability::WindowsConsole as i32)
                .collect(),
            unsupported: vec![UnsupportedMachineCapability {
                capability: MachineCapability::WindowsConsole as i32,
                reason: "not available on a Linux Machine".into(),
            }],
        };
        let target = TargetSpec {
            os: OperatingSystem::Linux as i32,
            arch: Architecture::Aarch64 as i32,
            image: "ubuntu:24.04".into(),
            version: Some("24.04.1".into()),
            channel: Some("stable".into()),
            digest: Some("sha256:target".into()),
        };
        let resources = MachineResources {
            cpus: Some(8),
            memory_mb: Some(32_768),
            disk_bytes: Some(512 * 1024 * 1024 * 1024),
        };
        let machine_spec = MachineSpec {
            schema_version: 1,
            name: "api".into(),
            profile: MachineProfile::Developer as i32,
            target: Some(target.clone()),
            resources: Some(resources),
            requested_capabilities: Some(requested_capabilities.clone()),
            workspace: Some(WorkspaceProjection {
                binding: "checkout".into(),
                target_path: "/workspace".into(),
                mode: WorkspaceProjectionMode::ReadWrite as i32,
            }),
        };
        let network_spec = NetworkSpec {
            schema_version: 1,
            name: "public-edge".into(),
            kind: NetworkKind::SimulatedPublic as i32,
            cidr: Some("10.44.0.0/24".into()),
        };
        let endpoint_spec = EndpointSpec {
            schema_version: 1,
            name: "api-https".into(),
            machine: "api".into(),
            network: "public-edge".into(),
            protocol: EndpointProtocol::Https as i32,
            port: 443,
            hostname: Some("api.shop.test".into()),
        };
        let definition = ProjectDefinition {
            schema_version: 1,
            project_id: "prj_demo".into(),
            name: "shop".into(),
            environment: Some(EnvironmentSpec {
                schema_version: 1,
                machines: vec![machine_spec],
                networks: vec![network_spec],
                endpoints: vec![endpoint_spec],
            }),
        };
        let binding = WorkspaceBinding {
            schema_version: 1,
            binding_id: "wsp_checkout".into(),
            project_id: "prj_demo".into(),
            environment_id: "env_agent_a".into(),
            workspace_key: "git-worktree:agent-a".into(),
            path_hint: Some("/relocatable/agent-a".into()),
            name: "checkout".into(),
        };
        let machine = MachineInstance {
            schema_version: 1,
            machine_id: "mac_api".into(),
            environment_id: "env_agent_a".into(),
            name: "api".into(),
            profile: MachineProfile::Developer as i32,
            target: Some(target),
            resources: Some(resources),
            requested_capabilities: Some(requested_capabilities),
            negotiated_capabilities: Some(negotiated_capabilities),
            backend: Some(MachineBackend::Other as i32),
            other_backend: Some("test-backend".into()),
            incarnation: Some(MachineIncarnation {
                schema_version: 1,
                incarnation_id: "inc_api_7".into(),
                machine_id: "mac_api".into(),
                generation: 7,
                created_at: 1_700_000_000,
            }),
            state: MachineState::Ready as i32,
            legacy_sandbox_id: Some("vz-run-shop-deadbeef".into()),
        };
        let environment = EnvironmentInstance {
            schema_version: 1,
            environment_id: "env_agent_a".into(),
            project_id: "prj_demo".into(),
            name: "agent-a".into(),
            definition_digest: "sha256:definition".into(),
            state: EnvironmentState::Ready as i32,
            bindings: vec![binding],
            machines: vec![machine],
            networks: vec![NetworkInstance {
                schema_version: 1,
                network_id: "net_public".into(),
                environment_id: "env_agent_a".into(),
                name: "public-edge".into(),
            }],
            endpoints: vec![EndpointInstance {
                schema_version: 1,
                endpoint_id: "ep_api_https".into(),
                environment_id: "env_agent_a".into(),
                machine_id: "mac_api".into(),
                network_id: "net_public".into(),
                name: "api-https".into(),
            }],
            ownership: vec![OwnershipRecord {
                schema_version: 1,
                resource_kind: OwnedResourceKind::Other as i32,
                other_resource_kind: Some("test-fixture".into()),
                resource_id: "fixture-1".into(),
                environment_id: "env_agent_a".into(),
                machine_id: Some("mac_api".into()),
            }],
            legacy_migration: Some(LegacyMigrationProvenance {
                source_version: "v0.3.20".into(),
                legacy_sandbox_id: "vz-run-shop-deadbeef".into(),
                unresolved_resources: vec!["published_ports".into()],
            }),
            created_at: 1_700_000_000,
            updated_at: 1_700_000_100,
        };
        let state = ProjectState {
            schema_version: 1,
            definition: Some(definition),
            environments: vec![environment],
        };

        let encoded = state.encode_to_vec();
        let decoded = ProjectState::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, state);
        assert_eq!(
            decoded.environments[0].machines[0]
                .negotiated_capabilities
                .as_ref()
                .unwrap()
                .unsupported[0]
                .reason,
            "not available on a Linux Machine"
        );
    }

    #[test]
    fn runtime_v2_structured_topology_errors_round_trip() {
        use crate::runtime_v2::topology_error_detail::Detail;
        use crate::runtime_v2::*;

        let ambiguous = TopologyErrorDetail {
            detail: Some(Detail::Ambiguous(TopologyAmbiguityDetail {
                kind: "environment".into(),
                selector: "agent".into(),
                candidates: vec![
                    TopologyCandidate {
                        id: "env_agent_a".into(),
                        name: "agent-a".into(),
                    },
                    TopologyCandidate {
                        id: "env_agent_b".into(),
                        name: "agent-b".into(),
                    },
                ],
            })),
        };
        let unsupported = TopologyErrorDetail {
            detail: Some(Detail::UnsupportedTarget(TopologyUnsupportedTargetDetail {
                host_os: OperatingSystem::Macos as i32,
                host_arch: Architecture::Aarch64 as i32,
                target_os: OperatingSystem::Windows as i32,
                target_arch: Architecture::Aarch64 as i32,
                requested_capabilities: vec![MachineCapability::WindowsConsole as i32],
            })),
        };
        let not_found = TopologyErrorDetail {
            detail: Some(Detail::NotFound(TopologyNotFoundDetail {
                kind: "machine".into(),
                selector: "missing".into(),
            })),
        };
        let missing_capability = TopologyErrorDetail {
            detail: Some(Detail::MissingCapability(TopologyMissingCapabilityDetail {
                machine_id: "mac_native".into(),
                capability: MachineCapability::DockerEngine as i32,
            })),
        };

        for detail in [ambiguous, unsupported, not_found, missing_capability] {
            let encoded = detail.encode_to_vec();
            assert_eq!(
                TopologyErrorDetail::decode(encoded.as_slice()).unwrap(),
                detail
            );
        }
    }
}
