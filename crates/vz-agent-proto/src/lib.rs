/// gRPC service definitions for the vz guest agent protocol.
pub mod vz {
    pub mod agent {
        pub mod v1 {
            #![allow(clippy::disallowed_methods)]
            #![allow(clippy::missing_docs_in_private_items)]
            #![allow(clippy::large_enum_variant)]
            #![allow(clippy::doc_markdown)]
            include!("generated/vz.agent.v1.rs");
        }
    }

    /// Runtime V2 control-plane gRPC service definitions.
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

/// Convenience re-export so consumers can write `use vz_agent_proto::*`.
pub use vz::agent::v1::*;

/// Convenience re-export for runtime V2 types under a dedicated namespace.
pub mod runtime_v2 {
    pub use crate::vz::runtime::v2::*;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use prost::Message;

    // ── Message encoding round-trips ────────────────────────────

    #[test]
    fn ping_round_trip() {
        let msg = PingRequest {};
        let encoded = msg.encode_to_vec();
        let decoded = PingRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn system_info_response_round_trip() {
        let msg = SystemInfoResponse {
            cpu_count: 4,
            memory_bytes: 8_589_934_592,
            disk_free_bytes: 50_000_000_000,
            os_version: "Linux 6.1".to_string(),
        };
        let encoded = msg.encode_to_vec();
        let decoded = SystemInfoResponse::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn resource_stats_response_round_trip() {
        let msg = ResourceStatsResponse {
            cpu_usage_percent: 45.2,
            memory_used_bytes: 4_000_000_000,
            memory_total_bytes: 8_589_934_592,
            disk_used_bytes: 30_000_000_000,
            disk_total_bytes: 100_000_000_000,
            process_count: 142,
            load_average: vec![1.5, 2.0, 1.8],
        };
        let encoded = msg.encode_to_vec();
        let decoded = ResourceStatsResponse::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn exec_request_round_trip() {
        let msg = ExecRequest {
            command: "cargo".to_string(),
            args: vec!["build".to_string(), "--release".to_string()],
            working_dir: "/workspace".to_string(),
            env: [("RUST_LOG".to_string(), "debug".to_string())]
                .into_iter()
                .collect(),
            user: "dev".to_string(),
            metadata: Some(TransportMetadata {
                request_id: "req_exec_1".to_string(),
                idempotency_key: "exec_container:req_exec_1".to_string(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = ExecRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn exec_event_stdout() {
        let msg = ExecEvent {
            event: Some(exec_event::Event::Stdout(b"hello world\n".to_vec())),
            sequence: 1,
            request_id: "req_exec_1".to_string(),
        };
        let encoded = msg.encode_to_vec();
        let decoded = ExecEvent::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
        // Verify raw bytes are preserved (no base64).
        match decoded.event {
            Some(exec_event::Event::Stdout(data)) => {
                assert_eq!(data, b"hello world\n");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn exec_event_exit_code() {
        let msg = ExecEvent {
            event: Some(exec_event::Event::ExitCode(0)),
            sequence: 2,
            request_id: "req_exec_1".to_string(),
        };
        let encoded = msg.encode_to_vec();
        let decoded = ExecEvent::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stdin_write_request_binary_data() {
        let msg = StdinWriteRequest {
            exec_id: 42,
            data: vec![0x00, 0xFF, 0x80, 0x7F],
            metadata: Some(TransportMetadata {
                request_id: "req_exec_2".to_string(),
                idempotency_key: String::new(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = StdinWriteRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg.data, decoded.data);
    }

    #[test]
    fn signal_request_round_trip() {
        let msg = SignalRequest {
            exec_id: 1,
            signal: 15, // SIGTERM
            metadata: Some(TransportMetadata {
                request_id: "req_exec_2".to_string(),
                idempotency_key: String::new(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = SignalRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    // ── Port Forward ────────────────────────────────────────────

    #[test]
    fn port_forward_open_frame() {
        let msg = PortForwardFrame {
            frame: Some(port_forward_frame::Frame::Open(PortForwardOpen {
                target_port: 8080,
                protocol: "tcp".to_string(),
                target_host: "172.20.0.2".to_string(),
                metadata: Some(TransportMetadata {
                    request_id: "req_pf_1".to_string(),
                    idempotency_key: String::new(),
                }),
            })),
        };
        let encoded = msg.encode_to_vec();
        let decoded = PortForwardFrame::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn port_forward_data_frame() {
        let msg = PortForwardFrame {
            frame: Some(port_forward_frame::Frame::Data(
                b"GET / HTTP/1.1\r\n\r\n".to_vec(),
            )),
        };
        let encoded = msg.encode_to_vec();
        let decoded = PortForwardFrame::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    // ── OCI Messages ────────────────────────────────────────────

    #[test]
    fn oci_create_request_round_trip() {
        let msg = OciCreateRequest {
            container_id: "svc-web".to_string(),
            bundle_path: "/run/vz-oci/bundles/svc-web".to_string(),
            metadata: Some(TransportMetadata {
                request_id: "req_create_1".to_string(),
                idempotency_key: "create_container:req_create_1".to_string(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = OciCreateRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn oci_state_response_round_trip() {
        let msg = OciStateResponse {
            container_id: "svc-web".to_string(),
            status: "running".to_string(),
            pid: 4242,
            bundle_path: "/run/vz-oci/bundles/svc-web".to_string(),
        };
        let encoded = msg.encode_to_vec();
        let decoded = OciStateResponse::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn oci_exec_round_trip() {
        let msg = OciExecRequest {
            container_id: "svc-web".to_string(),
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), "echo ready".to_string()],
            env: [
                (
                    "PATH".to_string(),
                    "/usr/local/bin:/usr/bin:/bin".to_string(),
                ),
                ("MODE".to_string(), "prod".to_string()),
            ]
            .into_iter()
            .collect(),
            working_dir: "/workspace".to_string(),
            user: "1000:1000".to_string(),
            metadata: Some(TransportMetadata {
                request_id: "req_exec_3".to_string(),
                idempotency_key: "exec_container:req_exec_3".to_string(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = OciExecRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn oci_exec_response_round_trip() {
        let msg = OciExecResponse {
            exit_code: 0,
            stdout: "ready\n".to_string(),
            stderr: String::new(),
        };
        let encoded = msg.encode_to_vec();
        let decoded = OciExecResponse::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn oci_kill_request_round_trip() {
        let msg = OciKillRequest {
            container_id: "svc-web".to_string(),
            signal: "SIGTERM".to_string(),
            metadata: Some(TransportMetadata {
                request_id: "req_kill_1".to_string(),
                idempotency_key: String::new(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = OciKillRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn oci_delete_force_round_trip() {
        let msg = OciDeleteRequest {
            container_id: "svc-web".to_string(),
            force: true,
            metadata: Some(TransportMetadata {
                request_id: "req_delete_1".to_string(),
                idempotency_key: String::new(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = OciDeleteRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    // ── Network Messages ────────────────────────────────────────

    #[test]
    fn network_setup_request_round_trip() {
        let msg = NetworkSetupRequest {
            stack_id: "my-stack".to_string(),
            services: vec![
                NetworkServiceConfig {
                    name: "web".to_string(),
                    addr: "172.20.0.2/24".to_string(),
                    network_name: "default".to_string(),
                },
                NetworkServiceConfig {
                    name: "db".to_string(),
                    addr: "172.20.0.3/24".to_string(),
                    network_name: "default".to_string(),
                },
            ],
            metadata: Some(TransportMetadata {
                request_id: "req_net_setup_1".to_string(),
                idempotency_key: String::new(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = NetworkSetupRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn network_teardown_request_round_trip() {
        let msg = NetworkTeardownRequest {
            stack_id: "my-stack".to_string(),
            service_names: vec!["web".to_string(), "db".to_string()],
            metadata: Some(TransportMetadata {
                request_id: "req_net_teardown_1".to_string(),
                idempotency_key: String::new(),
            }),
        };
        let encoded = msg.encode_to_vec();
        let decoded = NetworkTeardownRequest::decode(encoded.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }

    // ── Service trait existence ──────────────────────────────────

    #[test]
    fn service_modules_exist() {
        // Verify the generated service modules are accessible.
        // Client and server stubs are generated for all three services.
        let _ = std::any::type_name::<
            agent_service_client::AgentServiceClient<tonic::transport::Channel>,
        >();
        let _ = std::any::type_name::<
            oci_service_client::OciServiceClient<tonic::transport::Channel>,
        >();
        let _ = std::any::type_name::<
            network_service_client::NetworkServiceClient<tonic::transport::Channel>,
        >();
    }

    // ── Coverage: all proto message types instantiate ────────────

    #[test]
    fn all_request_types_instantiate() {
        let _ = PingRequest {};
        let _ = SystemInfoRequest {};
        let _ = ResourceStatsRequest {};
        let _ = ExecRequest::default();
        let _ = StdinWriteRequest::default();
        let _ = StdinCloseRequest::default();
        let _ = SignalRequest::default();
        let _ = PortForwardFrame::default();
        let _ = PortForwardOpen::default();
        let _ = OciCreateRequest::default();
        let _ = OciStartRequest::default();
        let _ = OciStateRequest::default();
        let _ = OciExecRequest::default();
        let _ = OciKillRequest::default();
        let _ = OciDeleteRequest::default();
        let _ = NetworkSetupRequest::default();
        let _ = NetworkTeardownRequest::default();
    }

    #[test]
    fn all_response_types_instantiate() {
        let _ = PingResponse {};
        let _ = SystemInfoResponse::default();
        let _ = ResourceStatsResponse::default();
        let _ = ExecEvent::default();
        let _ = StdinWriteResponse {};
        let _ = StdinCloseResponse {};
        let _ = SignalResponse {};
        let _ = OciCreateResponse {};
        let _ = OciStartResponse {};
        let _ = OciStateResponse::default();
        let _ = OciExecResponse::default();
        let _ = OciKillResponse {};
        let _ = OciDeleteResponse {};
        let _ = NetworkSetupResponse {};
        let _ = NetworkTeardownResponse {};
    }

    // ── Runtime V2 proto-contract consistency tests ──────────────

    #[test]
    fn runtime_v2_services_cover_all_entity_types() {
        use crate::runtime_v2::*;

        // Verify key request message types exist by constructing defaults.
        let _ = CreateSandboxRequest::default();
        let _ = OpenLeaseRequest::default();
        let _ = CreateContainerRequest::default();
        let _ = CreateExecutionRequest::default();
        let _ = CreateCheckpointRequest::default();
        let _ = StartBuildRequest::default();
        let _ = ListEventsRequest::default();
        let _ = GetCapabilitiesRequest::default();

        // Verify response types.
        let _ = SandboxResponse::default();
        let _ = LeaseResponse::default();
        let _ = ContainerResponse::default();
        let _ = ExecutionResponse::default();
        let _ = CheckpointResponse::default();
        let _ = BuildResponse::default();
        let _ = ListEventsResponse::default();
        let _ = GetCapabilitiesResponse::default();

        // Verify payload types used in responses.
        let _ = SandboxPayload::default();
        let _ = LeasePayload::default();
        let _ = ContainerPayload::default();
        let _ = ExecutionPayload::default();
        let _ = CheckpointPayload::default();
        let _ = BuildPayload::default();

        // Verify list responses.
        let _ = ListSandboxesResponse::default();
        let _ = ListLeasesResponse::default();
        let _ = ListContainersResponse::default();
        let _ = ListExecutionsResponse::default();
        let _ = ListCheckpointsResponse::default();
        let _ = ListBuildsResponse::default();

        // Verify streaming types.
        let _ = ExecOutputEvent::default();
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

        let create_checkpoint = CreateCheckpointRequest::default();
        assert!(create_checkpoint.metadata.is_none());

        let start_build = StartBuildRequest::default();
        assert!(start_build.metadata.is_none());

        let list_events = ListEventsRequest::default();
        assert!(list_events.metadata.is_none());

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
            created_at: 1700000000,
            updated_at: 1700000100,
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
            created_at: 1700000000,
        };
        let encoded = payload.encode_to_vec();
        let decoded = CheckpointPayload::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.checkpoint_id, "ckpt-test");
        assert_eq!(decoded.compatibility_fingerprint, "kernel-6.1-arm64");
        assert_eq!(decoded.parent_checkpoint_id, "ckpt-parent");
    }
}
