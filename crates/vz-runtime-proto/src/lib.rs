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

    #[test]
    fn machine_docker_context_optional_wire_roundtrip() {
        use super::runtime_v2::*;
        let context = MachineDockerContextDescriptor {
            schema_version: 1,
            project_id: "prj_one".into(),
            environment_id: "env_one".into(),
            machine_id: "mch_one".into(),
            name: "vz-one".into(),
            endpoint: "unix:///private/one.sock".into(),
            config_dir: "/private/client".into(),
            engine_id: "engine-one".into(),
            incarnation_id: "inc_one".into(),
            incarnation_generation: 7,
        };
        let machine = MachineInstance {
            docker_context: Some(context.clone()),
            ..Default::default()
        };
        assert_eq!(
            MachineInstance::decode(machine.encode_to_vec().as_slice()).unwrap(),
            machine
        );
        let activation = MachineActivationEvidence {
            docker_context: Some(context),
            ..Default::default()
        };
        assert_eq!(
            MachineActivationEvidence::decode(activation.encode_to_vec().as_slice()).unwrap(),
            activation
        );
        assert!(
            MachineInstance::decode([].as_slice())
                .unwrap()
                .docker_context
                .is_none()
        );
        assert!(
            MachineActivationEvidence::decode([].as_slice())
                .unwrap()
                .docker_context
                .is_none()
        );
    }

    #[test]
    fn up_environment_wire_roundtrips_nonempty_request_and_terminal_receipt() {
        use super::runtime_v2::*;
        let request = UpEnvironmentRequest {
            metadata: Some(RequestMetadata {
                request_id: "req-up".into(),
                idempotency_key: "key-up".into(),
                trace_id: "trace".into(),
            }),
            definition: Some(ProjectDefinition {
                schema_version: 1,
                project_id: "prj_fixture".into(),
                name: "project".into(),
                environment: Some(EnvironmentSpec {
                    schema_version: 1,
                    default_machine: Some("app".into()),
                    ..Default::default()
                }),
            }),
            environment: Some("named".into()),
            process_environment_id: Some("env_ignored".into()),
            workspace_key: Some("opaque".into()),
            path_hint: Some("/diagnostic".into()),
            timeout_millis: 300_000,
        };
        assert_eq!(
            UpEnvironmentRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
            request
        );
        let admission = EnvironmentUpAdmission {
            schema_version: 1,
            project_id: "prj_fixture".into(),
            environment_id: "env_fixture".into(),
            machine_ids: vec!["mch_fixture".into()],
            definition_digest: "sha256:definition".into(),
            request_id: "req-up".into(),
            idempotency_key: "key-up".into(),
            request_hash: "sha256:request".into(),
            workspace_key: Some("opaque".into()),
            created_at: 7,
        };
        let event = UpEnvironmentEvent {
            preparation: None,
            schema_version: 1,
            sequence: 9,
            admission: Some(admission.clone()),
            phase: "terminal".into(),
            operation: None,
            completion: Some(EnvironmentUpCompletion {
                admission: Some(admission),
                operation: None,
                workspace_binding: None,
                error: Some(ErrorDetail::default()),
                completed_at: 8,
            }),
        };
        assert_eq!(
            UpEnvironmentEvent::decode(event.encode_to_vec().as_slice()).unwrap(),
            event
        );
    }
    use std::collections::BTreeMap;

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum RpcMode {
        Unary,
        ServerStreaming,
        BidirectionalStreaming,
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
            let mode = if line.contains("(stream ")
                && line.contains("returns (stream ")
                && line
                    .split("returns")
                    .next()
                    .unwrap_or_default()
                    .contains("(stream ")
            {
                RpcMode::BidirectionalStreaming
            } else if line.contains("returns (stream ") {
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
            ("GetProjectState", RpcMode::Unary),
            ("UpEnvironment", RpcMode::ServerStreaming),
            ("StopEnvironment", RpcMode::ServerStreaming),
            ("DeleteEnvironment", RpcMode::ServerStreaming),
            ("ExecMachine", RpcMode::BidirectionalStreaming),
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
        let _ = MachineWorkloadScope::default();
        let _ = StackRunContainerRequest::default();
        let _ = GetCapabilitiesRequest::default();
        let _ = GetProjectStateRequest::default();
        let _ = StopEnvironmentRequest::default();
        let _ = DeleteEnvironmentRequest::default();

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
        let _ = GetProjectStateResponse::default();
        let _ = StopEnvironmentEvent::default();
        let _ = DeleteEnvironmentEvent::default();

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
        let _ = MachineRuntimeIdentity::default();
        let _ = MachineActivationEvidence::default();
        let _ = EnvironmentLifecycleOperation::default();
        let _ = MachineLifecycleStep::default();
        let _ = MachineLifecycleStepAcknowledgement::default();
        let _ = OwnershipCleanupStep::default();
        let _ = OwnershipCleanupStepAcknowledgement::default();
        let _ = EnvironmentTombstone::default();
        let _ = TopologyLifecycleErrorDetail::default();

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
            details: std::collections::HashMap::from([(
                "sandbox_id".to_string(),
                "sb-123".to_string(),
            )]),
        };
        assert_eq!(err.code, "not_found");
        assert_eq!(err.message, "sandbox not found");
        assert_eq!(err.request_id, "req-abc");
        assert_eq!(
            err.details.get("sandbox_id").map(String::as_str),
            Some("sb-123")
        );
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
    fn runtime_v2_stack_rpc_modes_remain_compatible() {
        let observed = parse_runtime_v2_rpc_modes(include_str!("../proto/runtime_v2.proto"));

        for rpc in [
            "ApplyStack",
            "TeardownStack",
            "StopStackService",
            "StartStackService",
            "RestartStackService",
        ] {
            assert_eq!(observed.get(rpc), Some(&RpcMode::ServerStreaming), "{rpc}");
        }
        for rpc in ["CreateStackRunContainer", "RemoveStackRunContainer"] {
            assert_eq!(observed.get(rpc), Some(&RpcMode::Unary), "{rpc}");
        }
    }

    #[test]
    fn runtime_v2_machine_workload_scope_inventory_and_request_tags_are_stable() {
        let proto = include_str!("../proto/runtime_v2.proto");

        for (message, fields) in [
            (
                "MachineWorkloadScope",
                &[
                    ("schema_version", 1),
                    ("project_id", 2),
                    ("environment_id", 3),
                    ("machine_id", 4),
                    ("machine_incarnation_id", 5),
                    ("stack_id", 6),
                ][..],
            ),
            (
                "ApplyStackRequest",
                &[
                    ("metadata", 1),
                    ("stack_name", 2),
                    ("compose_yaml", 3),
                    ("compose_dir", 4),
                    ("dry_run", 5),
                    ("detach", 6),
                    ("scope", 7),
                ],
            ),
            (
                "TeardownStackRequest",
                &[
                    ("metadata", 1),
                    ("stack_name", 2),
                    ("dry_run", 3),
                    ("remove_volumes", 4),
                    ("scope", 5),
                ],
            ),
            (
                "GetStackStatusRequest",
                &[("metadata", 1), ("stack_name", 2), ("scope", 3)],
            ),
            (
                "ListStackEventsRequest",
                &[
                    ("metadata", 1),
                    ("stack_name", 2),
                    ("after", 3),
                    ("limit", 4),
                    ("scope", 5),
                ],
            ),
            (
                "GetStackLogsRequest",
                &[
                    ("metadata", 1),
                    ("stack_name", 2),
                    ("service", 3),
                    ("tail", 4),
                    ("scope", 5),
                ],
            ),
            (
                "StackServiceActionRequest",
                &[
                    ("metadata", 1),
                    ("stack_name", 2),
                    ("service_name", 3),
                    ("scope", 4),
                ],
            ),
            (
                "StackRunContainerRequest",
                &[
                    ("metadata", 1),
                    ("stack_name", 2),
                    ("service_name", 3),
                    ("run_service_name", 4),
                    ("scope", 5),
                ],
            ),
            (
                "StackServiceStatus",
                &[
                    ("service_name", 1),
                    ("phase", 2),
                    ("ready", 3),
                    ("container_id", 4),
                    ("last_error", 5),
                    ("replica_index", 6),
                ],
            ),
        ] {
            assert_proto_fields(proto, message, fields);
        }
    }

    #[test]
    fn runtime_v2_stack_service_status_replica_round_trip_and_zero_default() {
        use crate::runtime_v2::StackServiceStatus;

        assert_eq!(StackServiceStatus::default().replica_index, 0);
        let status = StackServiceStatus {
            service_name: "web".to_string(),
            replica_index: 2,
            phase: "running".to_string(),
            ready: true,
            container_id: "ctr-web-2".to_string(),
            last_error: String::new(),
        };

        let encoded = status.encode_to_vec();
        let decoded = StackServiceStatus::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, status);
        assert_eq!(decoded.service_name, "web");
        assert_eq!(decoded.replica_index, 2);
    }

    #[test]
    fn runtime_v2_machine_workload_scope_presence_and_round_trip() {
        use crate::runtime_v2::*;

        let scope = MachineWorkloadScope {
            schema_version: 1,
            project_id: "prj-01".into(),
            environment_id: "env-01".into(),
            machine_id: "mch-01".into(),
            machine_incarnation_id: "inc-01".into(),
            stack_id: "stk-01".into(),
        };

        macro_rules! assert_scope_round_trip {
            ($request:expr, $request_type:ty) => {{
                let request: $request_type = $request;
                let encoded = request.encode_to_vec();
                let decoded = <$request_type>::decode(encoded.as_slice()).unwrap();
                assert_eq!(decoded.scope, Some(scope.clone()));
            }};
        }

        assert!(ApplyStackRequest::default().scope.is_none());
        assert!(TeardownStackRequest::default().scope.is_none());
        assert!(GetStackStatusRequest::default().scope.is_none());
        assert!(ListStackEventsRequest::default().scope.is_none());
        assert!(GetStackLogsRequest::default().scope.is_none());
        assert!(StackServiceActionRequest::default().scope.is_none());
        assert!(StackRunContainerRequest::default().scope.is_none());

        assert_scope_round_trip!(
            ApplyStackRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            ApplyStackRequest
        );
        assert_scope_round_trip!(
            TeardownStackRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            TeardownStackRequest
        );
        assert_scope_round_trip!(
            GetStackStatusRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            GetStackStatusRequest
        );
        assert_scope_round_trip!(
            ListStackEventsRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            ListStackEventsRequest
        );
        assert_scope_round_trip!(
            GetStackLogsRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            GetStackLogsRequest
        );
        assert_scope_round_trip!(
            StackServiceActionRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            StackServiceActionRequest
        );
        assert_scope_round_trip!(
            StackRunContainerRequest {
                scope: Some(scope.clone()),
                ..Default::default()
            },
            StackRunContainerRequest
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
        assert!(MachineExecFrame::default().metadata.is_none());

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
                    ("default_machine", 5),
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
                "MachineRuntimeIdentity",
                &[("schema_version", 1), ("opaque_id", 2)],
            ),
            (
                "MachineDockerContextDescriptor",
                &[
                    ("schema_version", 1),
                    ("project_id", 2),
                    ("environment_id", 3),
                    ("machine_id", 4),
                    ("name", 5),
                    ("endpoint", 6),
                    ("config_dir", 7),
                    ("engine_id", 8),
                    ("incarnation_id", 9),
                    ("incarnation_generation", 10),
                ],
            ),
            (
                "MachineActivationEvidence",
                &[
                    ("schema_version", 1),
                    ("backend", 2),
                    ("other_backend", 3),
                    ("negotiated_capabilities", 4),
                    ("runtime_identity", 5),
                    ("incarnation", 6),
                    ("docker_context", 7),
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
                    ("runtime_identity", 15),
                    ("docker_context", 16),
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
            ("LifecycleStepSucceeded", &[]),
            ("LifecycleStepFailed", &[("reason", 1)]),
            ("LifecycleStepResult", &[("succeeded", 1), ("failed", 2)]),
            (
                "MachineLifecycleStep",
                &[
                    ("machine_id", 1),
                    ("initial_state", 2),
                    ("target_state", 3),
                    ("status", 4),
                    ("failure_reason", 5),
                    ("expected_incarnation", 6),
                    ("resulting_incarnation", 7),
                    ("resulting_activation", 8),
                ],
            ),
            (
                "MachineLifecycleStepAcknowledgement",
                &[
                    ("operation_id", 1),
                    ("generation", 2),
                    ("machine_id", 3),
                    ("initial_state", 4),
                    ("target_state", 5),
                    ("result", 6),
                    ("expected_incarnation", 7),
                    ("resulting_incarnation", 8),
                    ("resulting_activation", 9),
                ],
            ),
            (
                "OwnershipCleanupStep",
                &[("ownership", 1), ("status", 2), ("failure_reason", 3)],
            ),
            (
                "OwnershipCleanupStepAcknowledgement",
                &[
                    ("operation_id", 1),
                    ("generation", 2),
                    ("ownership", 3),
                    ("result", 4),
                ],
            ),
            (
                "EnvironmentLifecycleOperation",
                &[
                    ("schema_version", 1),
                    ("operation_id", 2),
                    ("project_id", 3),
                    ("environment_id", 4),
                    ("kind", 5),
                    ("generation", 6),
                    ("request_id", 7),
                    ("idempotency_key", 8),
                    ("request_hash", 9),
                    ("definition_digest", 10),
                    ("initial_state", 11),
                    ("requested_target", 12),
                    ("status", 13),
                    ("machine_steps", 14),
                    ("cleanup_steps", 15),
                    ("created_at", 16),
                    ("updated_at", 17),
                    ("completed_at", 18),
                ],
            ),
            (
                "EnvironmentTombstone",
                &[
                    ("schema_version", 1),
                    ("project_id", 2),
                    ("environment_id", 3),
                    ("name", 4),
                    ("definition_digest", 5),
                    ("delete_operation_id", 6),
                    ("lifecycle_generation", 7),
                    ("ownership_digest", 8),
                    ("deleted_at", 9),
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
                    ("lifecycle_generation", 15),
                    ("active_operation_id", 16),
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
            (
                "GetProjectStateRequest",
                &[("metadata", 1), ("project_id", 2)],
            ),
            (
                "GetProjectStateResponse",
                &[("request_id", 1), ("project", 2)],
            ),
            (
                "UpEnvironmentRequest",
                &[
                    ("metadata", 1),
                    ("definition", 2),
                    ("environment", 3),
                    ("process_environment_id", 4),
                    ("workspace_key", 5),
                    ("path_hint", 6),
                    ("timeout_millis", 7),
                ],
            ),
            (
                "EnvironmentUpAdmission",
                &[
                    ("schema_version", 1),
                    ("project_id", 2),
                    ("environment_id", 3),
                    ("machine_ids", 4),
                    ("definition_digest", 5),
                    ("request_id", 6),
                    ("idempotency_key", 7),
                    ("request_hash", 8),
                    ("workspace_key", 9),
                    ("created_at", 10),
                ],
            ),
            (
                "EnvironmentUpCompletion",
                &[
                    ("admission", 1),
                    ("operation", 2),
                    ("workspace_binding", 3),
                    ("error", 4),
                    ("completed_at", 5),
                ],
            ),
            (
                "UpEnvironmentEvent",
                &[
                    ("schema_version", 1),
                    ("sequence", 2),
                    ("admission", 3),
                    ("phase", 4),
                    ("operation", 5),
                    ("completion", 6),
                    ("preparation", 7),
                ],
            ),
            (
                "StopEnvironmentRequest",
                &[
                    ("metadata", 1),
                    ("project_id", 2),
                    ("environment", 3),
                    ("process_environment_id", 4),
                    ("workspace_key", 5),
                    ("machine_timeout_millis", 6),
                ],
            ),
            (
                "StopEnvironmentEvent",
                &[
                    ("schema_version", 1),
                    ("request_id", 2),
                    ("sequence", 3),
                    ("operation", 4),
                    ("terminal", 5),
                    ("error", 6),
                ],
            ),
            (
                "DeleteEnvironmentRequest",
                &[
                    ("metadata", 1),
                    ("project_id", 2),
                    ("environment", 3),
                    ("process_environment_id", 4),
                    ("workspace_key", 5),
                    ("machine_timeout_millis", 6),
                ],
            ),
            (
                "DeleteEnvironmentEvent",
                &[
                    ("schema_version", 1),
                    ("request_id", 2),
                    ("sequence", 3),
                    ("operation", 4),
                    ("terminal", 5),
                    ("error", 6),
                    ("tombstone", 7),
                ],
            ),
            ("TopologyCandidate", &[("id", 1), ("name", 2)]),
            ("TopologyNotFoundDetail", &[("kind", 1), ("selector", 2)]),
            (
                "TopologyAmbiguityDetail",
                &[("kind", 1), ("selector", 2), ("candidates", 3)],
            ),
            (
                "TopologySelectionRequiredDetail",
                &[("kind", 1), ("selector", 2), ("candidates", 3)],
            ),
            (
                "TopologyInvalidSelectorDetail",
                &[("kind", 1), ("selector", 2), ("reason", 3)],
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
                "TopologyInvalidMachineProfileDetail",
                &[("machine_id", 1), ("profile", 2), ("reason", 3)],
            ),
            (
                "TopologyInvalidCapabilityDeclarationDetail",
                &[("machine_id", 1), ("reason", 2)],
            ),
            (
                "TopologyContradictoryCapabilityDetail",
                &[("machine_id", 1), ("capability", 2)],
            ),
            (
                "TopologyInvalidLifecycleStateDetail",
                &[("environment_id", 1), ("reason", 2)],
            ),
            (
                "TopologyInvalidMachineIncarnationDetail",
                &[("machine_id", 1), ("reason", 2)],
            ),
            (
                "TopologyErrorDetail",
                &[
                    ("not_found", 1),
                    ("ambiguous", 2),
                    ("unsupported_target", 3),
                    ("missing_capability", 4),
                    ("invalid_machine_profile", 5),
                    ("invalid_capability_declaration", 6),
                    ("contradictory_capability", 7),
                    ("selection_required", 8),
                    ("invalid_selector", 9),
                    ("invalid_lifecycle_state", 10),
                    ("invalid_machine_incarnation", 11),
                ],
            ),
            (
                "TopologyLifecycleInvalidTransitionDetail",
                &[("environment_id", 1), ("operation", 2), ("state", 3)],
            ),
            (
                "TopologyLifecycleOperationConflictDetail",
                &[("environment_id", 1), ("active_operation_id", 2)],
            ),
            (
                "TopologyLifecycleGenerationMismatchDetail",
                &[("operation_id", 1), ("expected", 2), ("found", 3)],
            ),
            (
                "TopologyLifecycleOperationMismatchDetail",
                &[("environment_id", 1), ("expected", 2), ("found", 3)],
            ),
            (
                "TopologyLifecycleMachineStepNotFoundDetail",
                &[("operation_id", 1), ("machine_id", 2)],
            ),
            (
                "TopologyLifecycleMachineStepMismatchDetail",
                &[("machine_id", 1)],
            ),
            (
                "TopologyLifecycleOwnershipStepMismatchDetail",
                &[
                    ("operation_id", 1),
                    ("resource_kind", 2),
                    ("resource_id", 3),
                ],
            ),
            (
                "TopologyLifecycleOperationIncompleteDetail",
                &[("operation_id", 1)],
            ),
            (
                "TopologyLifecycleOperationFailedDetail",
                &[("operation_id", 1)],
            ),
            (
                "TopologyLifecycleDeleteRequiredDetail",
                &[("operation_id", 1)],
            ),
            (
                "TopologyLifecycleDeletedEnvironmentIsNotLiveDetail",
                &[("environment_id", 1)],
            ),
            ("TopologyLifecycleInvalidOperationDetail", &[("reason", 1)]),
            (
                "TopologyLifecycleErrorDetail",
                &[
                    ("invalid_transition", 1),
                    ("operation_conflict", 2),
                    ("generation_mismatch", 3),
                    ("operation_mismatch", 4),
                    ("machine_step_not_found", 5),
                    ("machine_step_mismatch", 6),
                    ("ownership_step_mismatch", 7),
                    ("operation_incomplete", 8),
                    ("operation_failed", 9),
                    ("delete_required", 10),
                    ("deleted_environment_is_not_live", 11),
                    ("invalid_operation", 12),
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
                    ("ENVIRONMENT_STATE_DEGRADED", 8),
                ],
            ),
            (
                "EnvironmentLifecycleKind",
                &[
                    ("ENVIRONMENT_LIFECYCLE_KIND_UNSPECIFIED", 0),
                    ("ENVIRONMENT_LIFECYCLE_KIND_UP", 1),
                    ("ENVIRONMENT_LIFECYCLE_KIND_STOP", 2),
                    ("ENVIRONMENT_LIFECYCLE_KIND_DELETE", 3),
                ],
            ),
            (
                "EnvironmentLifecycleStatus",
                &[
                    ("ENVIRONMENT_LIFECYCLE_STATUS_UNSPECIFIED", 0),
                    ("ENVIRONMENT_LIFECYCLE_STATUS_PLANNED", 1),
                    ("ENVIRONMENT_LIFECYCLE_STATUS_RUNNING", 2),
                    ("ENVIRONMENT_LIFECYCLE_STATUS_BLOCKED", 3),
                    ("ENVIRONMENT_LIFECYCLE_STATUS_SUCCEEDED", 4),
                    ("ENVIRONMENT_LIFECYCLE_STATUS_FAILED", 5),
                    ("ENVIRONMENT_LIFECYCLE_STATUS_SUPERSEDED", 6),
                ],
            ),
            (
                "LifecycleStepStatus",
                &[
                    ("LIFECYCLE_STEP_STATUS_UNSPECIFIED", 0),
                    ("LIFECYCLE_STEP_STATUS_PENDING", 1),
                    ("LIFECYCLE_STEP_STATUS_RUNNING", 2),
                    ("LIFECYCLE_STEP_STATUS_SUCCEEDED", 3),
                    ("LIFECYCLE_STEP_STATUS_FAILED", 4),
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
                default_machine: None,
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
            docker_context: None,
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
            runtime_identity: Some(MachineRuntimeIdentity {
                schema_version: 1,
                opaque_id: "test-backend:runtime:api:7".into(),
            }),
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
            lifecycle_generation: 9,
            active_operation_id: Some("lop_reconcile_9".into()),
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
    fn runtime_v2_lifecycle_records_preserve_presence_and_oneofs() {
        use crate::runtime_v2::lifecycle_step_result::Result as StepResult;
        use crate::runtime_v2::*;

        let incarnation = MachineIncarnation {
            schema_version: 1,
            incarnation_id: "inc_api_8".into(),
            machine_id: "mch_api".into(),
            generation: 8,
            created_at: 1_700_000_200,
        };
        let resulting_incarnation = MachineIncarnation {
            incarnation_id: "inc_api_9".into(),
            generation: 9,
            ..incarnation.clone()
        };
        let resulting_activation = MachineActivationEvidence {
            docker_context: None,
            schema_version: 1,
            backend: MachineBackend::MacosVirtualizationLinux as i32,
            other_backend: None,
            negotiated_capabilities: Some(CapabilitySet {
                capabilities: vec![
                    MachineCapability::PosixExec as i32,
                    MachineCapability::DockerEngine as i32,
                    MachineCapability::Compose as i32,
                    MachineCapability::Buildx as i32,
                ],
                unsupported: Vec::new(),
            }),
            runtime_identity: Some(MachineRuntimeIdentity {
                schema_version: 1,
                opaque_id: r#"{"schema_version":1,"stack_id":"vzr1-machine","incarnation_id":"00000000-0000-4000-8000-000000000009"}"#.into(),
            }),
            incarnation: Some(resulting_incarnation.clone()),
        };
        let operation = EnvironmentLifecycleOperation {
            schema_version: 1,
            operation_id: "lop_up_8".into(),
            project_id: "prj_demo".into(),
            environment_id: "env_agent_a".into(),
            kind: EnvironmentLifecycleKind::Up as i32,
            generation: 8,
            request_id: "req-up-8".into(),
            idempotency_key: "idem-up-8".into(),
            request_hash: "sha256:request".into(),
            definition_digest: "sha256:definition".into(),
            initial_state: EnvironmentState::Stopped as i32,
            requested_target: EnvironmentState::Ready as i32,
            status: EnvironmentLifecycleStatus::Running as i32,
            machine_steps: vec![MachineLifecycleStep {
                machine_id: "mch_api".into(),
                initial_state: MachineState::Stopped as i32,
                target_state: Some(MachineState::Ready as i32),
                expected_incarnation: Some(incarnation.clone()),
                resulting_incarnation: None,
                resulting_activation: None,
                status: LifecycleStepStatus::Pending as i32,
                failure_reason: None,
            }],
            cleanup_steps: vec![OwnershipCleanupStep {
                ownership: Some(OwnershipRecord {
                    schema_version: 1,
                    resource_kind: OwnedResourceKind::Disk as i32,
                    other_resource_kind: None,
                    resource_id: "disk_api".into(),
                    environment_id: "env_agent_a".into(),
                    machine_id: Some("mch_api".into()),
                }),
                status: LifecycleStepStatus::Failed as i32,
                failure_reason: Some("busy".into()),
            }],
            created_at: 1_700_000_100,
            updated_at: 1_700_000_200,
            completed_at: None,
        };
        let acknowledgement = MachineLifecycleStepAcknowledgement {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation,
            machine_id: "mch_api".into(),
            initial_state: MachineState::Stopped as i32,
            target_state: Some(MachineState::Ready as i32),
            expected_incarnation: Some(incarnation.clone()),
            resulting_incarnation: Some(resulting_incarnation),
            resulting_activation: Some(resulting_activation),
            result: Some(LifecycleStepResult {
                result: Some(StepResult::Succeeded(LifecycleStepSucceeded::default())),
            }),
        };

        let encoded = operation.encode_to_vec();
        let decoded = EnvironmentLifecycleOperation::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, operation);
        assert!(decoded.completed_at.is_none());
        assert!(decoded.machine_steps[0].target_state.is_some());
        assert!(decoded.machine_steps[0].resulting_incarnation.is_none());

        let encoded = acknowledgement.encode_to_vec();
        let decoded = MachineLifecycleStepAcknowledgement::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, acknowledgement);
        assert_eq!(
            decoded
                .resulting_activation
                .as_ref()
                .and_then(|evidence| evidence.runtime_identity.as_ref())
                .map(|identity| identity.opaque_id.as_str()),
            Some(
                r#"{"schema_version":1,"stack_id":"vzr1-machine","incarnation_id":"00000000-0000-4000-8000-000000000009"}"#
            )
        );
        assert!(matches!(
            decoded.result.and_then(|result| result.result),
            Some(StepResult::Succeeded(_))
        ));
    }

    #[test]
    fn runtime_v2_stop_generated_wire_tags_are_stable() {
        use crate::runtime_v2::*;
        // Independent wire bytes catch stale generated tags even when the
        // .proto inventory and same-generated-type round trips both pass.
        let request = StopEnvironmentRequest {
            metadata: Some(RequestMetadata::default()),
            project_id: "p".into(),
            environment: Some(String::new()),
            process_environment_id: Some(String::new()),
            workspace_key: Some(String::new()),
            machine_timeout_millis: 1,
        };
        assert_eq!(
            request.encode_to_vec(),
            [0x0a, 0, 0x12, 1, b'p', 0x1a, 0, 0x22, 0, 0x2a, 0, 0x30, 1]
        );
        let event = StopEnvironmentEvent {
            schema_version: 1,
            request_id: "r".into(),
            sequence: 1,
            operation: Some(EnvironmentLifecycleOperation::default()),
            terminal: true,
            error: Some(ErrorDetail::default()),
        };
        assert_eq!(
            event.encode_to_vec(),
            [0x08, 1, 0x12, 1, b'r', 0x18, 1, 0x22, 0, 0x28, 1, 0x32, 0]
        );
    }

    #[test]
    fn runtime_v2_delete_generated_wire_tags_and_tombstone_presence_are_stable() {
        use crate::runtime_v2::*;
        // Literal bytes independently bind generated field tags. This fixture
        // exercises wire presence, not semantic success/error coexistence.
        let request = DeleteEnvironmentRequest {
            metadata: Some(RequestMetadata::default()),
            project_id: "p".into(),
            environment: Some(String::new()),
            process_environment_id: Some(String::new()),
            workspace_key: Some(String::new()),
            machine_timeout_millis: 1,
        };
        assert_eq!(
            request.encode_to_vec(),
            [0x0a, 0, 0x12, 1, b'p', 0x1a, 0, 0x22, 0, 0x2a, 0, 0x30, 1]
        );
        let event = DeleteEnvironmentEvent {
            schema_version: 1,
            request_id: "r".into(),
            sequence: 1,
            operation: Some(EnvironmentLifecycleOperation::default()),
            terminal: true,
            error: Some(ErrorDetail::default()),
            tombstone: Some(EnvironmentTombstone::default()),
        };
        assert_eq!(
            event.encode_to_vec(),
            [
                0x08, 1, 0x12, 1, b'r', 0x18, 1, 0x22, 0, 0x28, 1, 0x32, 0, 0x3a, 0
            ]
        );
        assert!(
            DeleteEnvironmentEvent::decode([].as_slice())
                .unwrap()
                .tombstone
                .is_none()
        );
        assert_eq!(
            DeleteEnvironmentEvent::decode([0x3a, 0].as_slice())
                .unwrap()
                .tombstone,
            Some(EnvironmentTombstone::default())
        );
    }

    #[test]
    fn runtime_v2_delete_request_preserves_each_optional_selector_and_metadata() {
        use crate::runtime_v2::*;
        let values = [None, Some(String::new()), Some("exact-selector".into())];
        for environment in &values {
            for process_environment_id in &values {
                for workspace_key in &values {
                    let request = DeleteEnvironmentRequest {
                        metadata: Some(RequestMetadata {
                            request_id: "req-delete".into(),
                            idempotency_key: "idem-delete".into(),
                            trace_id: "trace-delete".into(),
                        }),
                        project_id: "prj_selected".into(),
                        environment: environment.clone(),
                        process_environment_id: process_environment_id.clone(),
                        workspace_key: workspace_key.clone(),
                        machine_timeout_millis: 300_000,
                    };
                    assert_eq!(
                        DeleteEnvironmentRequest::decode(request.encode_to_vec().as_slice())
                            .unwrap(),
                        request
                    );
                }
            }
        }
        assert!(
            DeleteEnvironmentRequest::decode([].as_slice())
                .unwrap()
                .metadata
                .is_none()
        );
    }

    #[test]
    fn runtime_v2_delete_progress_and_terminal_tombstone_round_trip_exact_sequence() {
        use crate::runtime_v2::*;
        for (status, step_status, sequence, failure) in [
            (
                EnvironmentLifecycleStatus::Running,
                LifecycleStepStatus::Pending,
                0,
                None,
            ),
            (
                EnvironmentLifecycleStatus::Succeeded,
                LifecycleStepStatus::Succeeded,
                u64::MAX,
                None,
            ),
            (
                EnvironmentLifecycleStatus::Blocked,
                LifecycleStepStatus::Failed,
                41,
                Some("owned cleanup uncertain"),
            ),
        ] {
            let succeeded = status == EnvironmentLifecycleStatus::Succeeded;
            let terminal = succeeded || failure.is_some();
            let operation = EnvironmentLifecycleOperation {
                schema_version: 1,
                operation_id: "lop_delete".into(),
                project_id: "prj_selected".into(),
                environment_id: "env_selected".into(),
                kind: EnvironmentLifecycleKind::Delete as i32,
                generation: 8,
                request_id: "req-delete".into(),
                idempotency_key: "idem-delete".into(),
                request_hash: "sha256:request".into(),
                definition_digest: "sha256:definition".into(),
                initial_state: EnvironmentState::Ready as i32,
                requested_target: EnvironmentState::Deleted as i32,
                status: status as i32,
                machine_steps: vec![MachineLifecycleStep {
                    machine_id: "mch_selected".into(),
                    initial_state: MachineState::Ready as i32,
                    target_state: None,
                    expected_incarnation: Some(MachineIncarnation {
                        schema_version: 1,
                        incarnation_id: "inc_original".into(),
                        machine_id: "mch_selected".into(),
                        generation: 7,
                        created_at: 100,
                    }),
                    resulting_incarnation: None,
                    resulting_activation: None,
                    status: step_status as i32,
                    failure_reason: failure.map(str::to_owned),
                }],
                cleanup_steps: vec![OwnershipCleanupStep {
                    ownership: Some(OwnershipRecord {
                        schema_version: 1,
                        resource_kind: OwnedResourceKind::Machine as i32,
                        other_resource_kind: None,
                        resource_id: "mch_selected".into(),
                        environment_id: "env_selected".into(),
                        machine_id: Some("mch_selected".into()),
                    }),
                    status: step_status as i32,
                    failure_reason: failure.map(str::to_owned),
                }],
                created_at: 101,
                updated_at: 102,
                completed_at: succeeded.then_some(102),
            };
            let event = DeleteEnvironmentEvent {
                schema_version: 1,
                request_id: "req-delete".into(),
                sequence,
                operation: Some(operation),
                terminal,
                error: failure.map(|message| ErrorDetail {
                    code: "backend_unavailable".into(),
                    message: message.into(),
                    request_id: "req-delete".into(),
                    details: Default::default(),
                }),
                tombstone: succeeded.then(|| EnvironmentTombstone {
                    schema_version: 1,
                    project_id: "prj_selected".into(),
                    environment_id: "env_selected".into(),
                    name: "selected".into(),
                    definition_digest: "sha256:definition".into(),
                    delete_operation_id: "lop_delete".into(),
                    lifecycle_generation: 8,
                    ownership_digest: "sha256:ownership".into(),
                    deleted_at: 102,
                }),
            };
            let decoded = DeleteEnvironmentEvent::decode(event.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, event);
            assert_eq!(decoded.sequence, sequence);
            assert_eq!(decoded.tombstone.is_some(), succeeded);
            assert!(
                decoded.operation.as_ref().unwrap().machine_steps[0]
                    .target_state
                    .is_none()
            );
        }
    }

    #[test]
    fn runtime_v2_machine_exec_wire_tags_and_stream_modes_are_stable() {
        use crate::runtime_v2::*;
        let proto = include_str!("../proto/runtime_v2.proto");
        assert_proto_fields(
            proto,
            "MachineExecFrame",
            &[
                ("metadata", 1),
                ("sequence", 2),
                ("execution_id", 3),
                ("open", 4),
                ("stdin", 5),
                ("stdin_eof", 6),
                ("signal", 7),
                ("resize", 8),
                ("cancel", 9),
            ],
        );
        assert_proto_fields(
            proto,
            "MachineExecutionScope",
            &[
                ("schema_version", 1),
                ("execution_id", 2),
                ("request_id", 3),
                ("idempotency_key", 4),
                ("request_hash", 5),
                ("project_id", 6),
                ("environment_id", 7),
                ("machine_id", 8),
                ("environment_generation", 9),
                ("incarnation", 10),
                ("runtime_identity", 11),
                ("definition_digest", 12),
            ],
        );
        assert_proto_fields(
            proto,
            "MachineExecEvent",
            &[
                ("schema_version", 1),
                ("scope", 2),
                ("sequence", 3),
                ("replayed", 4),
                ("ready", 5),
                ("stdout", 6),
                ("stderr", 7),
                ("receipt", 8),
            ],
        );
        let frame = MachineExecFrame {
            metadata: Some(RequestMetadata::default()),
            sequence: 1,
            execution_id: "x".into(),
            payload: Some(machine_exec_frame::Payload::Stdin(vec![0, 255])),
        };
        assert_eq!(
            frame.encode_to_vec(),
            [0x0a, 0, 0x10, 1, 0x1a, 1, b'x', 0x2a, 2, 0, 255]
        );
        let event = MachineExecEvent {
            schema_version: 1,
            scope: Some(MachineExecutionScope::default()),
            sequence: 1,
            replayed: false,
            payload: Some(machine_exec_event::Payload::Receipt(
                MachineExecutionReceipt::default(),
            )),
        };
        assert_eq!(event.encode_to_vec(), [0x08, 1, 0x12, 0, 0x18, 1, 0x42, 0]);
        assert_eq!(
            parse_runtime_v2_rpc_modes(include_str!("../proto/runtime_v2.proto"))
                .get("ExecMachine"),
            Some(&RpcMode::BidirectionalStreaming)
        );
    }

    #[test]
    fn runtime_v2_machine_exec_preserves_selector_presence_and_binary_controls() {
        use crate::runtime_v2::*;
        for selector in [None, Some(String::new()), Some("selected".into())] {
            let frame = MachineExecFrame {
                metadata: Some(RequestMetadata {
                    request_id: "req".into(),
                    idempotency_key: "idem".into(),
                    trace_id: "trace".into(),
                }),
                sequence: 0,
                execution_id: String::new(),
                payload: Some(machine_exec_frame::Payload::Open(MachineExecOpen {
                    project_id: "project".into(),
                    environment: selector.clone(),
                    process_environment_id: selector.clone(),
                    workspace_key: selector.clone(),
                    machine: selector.clone(),
                    process_machine_id: selector,
                    spec: Some(MachineExecutionSpec {
                        argv: vec!["/bin/sh".into()],
                        timeout_millis: 1,
                        ..Default::default()
                    }),
                })),
            };
            assert_eq!(
                MachineExecFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
                frame
            );
        }
        for payload in [
            machine_exec_frame::Payload::Stdin(vec![0, 255]),
            machine_exec_frame::Payload::StdinEof(true),
            machine_exec_frame::Payload::Signal(15),
            machine_exec_frame::Payload::Resize(MachineExecutionTerminal {
                rows: 24,
                columns: 80,
            }),
            machine_exec_frame::Payload::Cancel(true),
        ] {
            let frame = MachineExecFrame {
                payload: Some(payload),
                ..Default::default()
            };
            assert_eq!(
                MachineExecFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
                frame
            );
        }
    }

    #[test]
    fn runtime_v2_stop_request_preserves_selection_presence_and_metadata() {
        use crate::runtime_v2::*;
        for selector in [None, Some(String::new()), Some("env_selected".into())] {
            let request = StopEnvironmentRequest {
                metadata: Some(RequestMetadata {
                    request_id: "req-stop".into(),
                    idempotency_key: "idem-stop".into(),
                    trace_id: "trace-stop".into(),
                }),
                project_id: "prj_selected".into(),
                environment: selector.clone(),
                process_environment_id: selector.clone(),
                workspace_key: selector,
                machine_timeout_millis: 300_000,
            };
            let decoded =
                StopEnvironmentRequest::decode(request.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, request);
        }
    }

    #[test]
    fn runtime_v2_stop_progress_and_terminal_receipts_round_trip() {
        use crate::runtime_v2::*;
        for (status, step_status, terminal, failure) in [
            (
                EnvironmentLifecycleStatus::Running,
                LifecycleStepStatus::Pending,
                false,
                None,
            ),
            (
                EnvironmentLifecycleStatus::Succeeded,
                LifecycleStepStatus::Succeeded,
                true,
                None,
            ),
            (
                EnvironmentLifecycleStatus::Failed,
                LifecycleStepStatus::Failed,
                true,
                Some("exact teardown uncertain"),
            ),
        ] {
            let event = StopEnvironmentEvent {
                schema_version: 1,
                request_id: "req-stop".into(),
                sequence: u64::from(terminal),
                terminal,
                operation: Some(EnvironmentLifecycleOperation {
                    schema_version: 1,
                    operation_id: "lop_stop".into(),
                    project_id: "prj_selected".into(),
                    environment_id: "env_selected".into(),
                    kind: EnvironmentLifecycleKind::Stop as i32,
                    generation: 8,
                    request_id: "req-stop".into(),
                    idempotency_key: "idem-stop".into(),
                    request_hash: "sha256:request".into(),
                    definition_digest: "sha256:definition".into(),
                    initial_state: EnvironmentState::Ready as i32,
                    requested_target: EnvironmentState::Stopped as i32,
                    status: status as i32,
                    machine_steps: vec![MachineLifecycleStep {
                        machine_id: "mch_selected".into(),
                        initial_state: MachineState::Ready as i32,
                        target_state: Some(MachineState::Stopped as i32),
                        expected_incarnation: Some(MachineIncarnation {
                            schema_version: 1,
                            incarnation_id: "inc_original".into(),
                            machine_id: "mch_selected".into(),
                            generation: 7,
                            created_at: 100,
                        }),
                        resulting_incarnation: None,
                        resulting_activation: None,
                        status: step_status as i32,
                        failure_reason: failure.map(str::to_string),
                    }],
                    cleanup_steps: vec![],
                    created_at: 101,
                    updated_at: 102,
                    completed_at: terminal.then_some(102),
                }),
                error: failure.map(|message| ErrorDetail {
                    code: "backend_unavailable".into(),
                    message: message.into(),
                    request_id: "req-stop".into(),
                    details: std::collections::HashMap::from([(
                        "environment_id".into(),
                        "env_selected".into(),
                    )]),
                }),
            };
            let decoded = StopEnvironmentEvent::decode(event.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, event);
        }
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
        let selection_required = TopologyErrorDetail {
            detail: Some(Detail::SelectionRequired(TopologySelectionRequiredDetail {
                kind: "environment".into(),
                selector: "workspace binding".into(),
                candidates: vec![TopologyCandidate {
                    id: "env_agent".into(),
                    name: "agent".into(),
                }],
            })),
        };
        let invalid_selector = TopologyErrorDetail {
            detail: Some(Detail::InvalidSelector(TopologyInvalidSelectorDetail {
                kind: "environment".into(),
                selector: " ".into(),
                reason: "name must not be blank".into(),
            })),
        };
        let missing_capability = TopologyErrorDetail {
            detail: Some(Detail::MissingCapability(TopologyMissingCapabilityDetail {
                machine_id: "mac_native".into(),
                capability: MachineCapability::DockerEngine as i32,
            })),
        };
        let invalid_profile = TopologyErrorDetail {
            detail: Some(Detail::InvalidMachineProfile(
                TopologyInvalidMachineProfileDetail {
                    machine_id: "mac_native".into(),
                    profile: MachineProfile::Hardened as i32,
                    reason: "native targets support only the Developer profile".into(),
                },
            )),
        };
        let invalid_capability = TopologyErrorDetail {
            detail: Some(Detail::InvalidCapabilityDeclaration(
                TopologyInvalidCapabilityDeclarationDetail {
                    machine_id: "mac_native".into(),
                    reason: "non-Linux target cannot declare implicit Docker capability".into(),
                },
            )),
        };
        let contradictory_capability = TopologyErrorDetail {
            detail: Some(Detail::ContradictoryCapability(
                TopologyContradictoryCapabilityDetail {
                    machine_id: "mac_linux".into(),
                    capability: MachineCapability::Compose as i32,
                },
            )),
        };

        for detail in [
            ambiguous,
            unsupported,
            not_found,
            selection_required,
            invalid_selector,
            missing_capability,
            invalid_profile,
            invalid_capability,
            contradictory_capability,
        ] {
            let encoded = detail.encode_to_vec();
            assert_eq!(
                TopologyErrorDetail::decode(encoded.as_slice()).unwrap(),
                detail
            );
        }
    }
}
