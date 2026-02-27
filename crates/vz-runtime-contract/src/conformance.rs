use serde::{Deserialize, Serialize};

use crate::{RuntimeCapabilities, RuntimeError, RuntimeOperation, SandboxBackend};

/// Canonical Runtime V2 operation surface expected from implementations.
pub const REQUIRED_RUNTIME_OPERATIONS: &[RuntimeOperation] = &RuntimeOperation::ALL;

/// Required idempotent mutation paths and their canonical operation names.
pub const REQUIRED_IDEMPOTENT_MUTATIONS: &[RuntimeOperation] = &[
    RuntimeOperation::CreateSandbox,
    RuntimeOperation::OpenLease,
    RuntimeOperation::PullImage,
    RuntimeOperation::StartBuild,
    RuntimeOperation::CreateContainer,
    RuntimeOperation::ExecContainer,
    RuntimeOperation::CreateCheckpoint,
    RuntimeOperation::ForkCheckpoint,
];

/// Canonical OpenAPI surface mapping for a runtime operation.
#[derive(Debug, Clone, Copy)]
pub struct OpenApiPrimitiveSurface {
    /// Relative OpenAPI path (path template for request construction).
    pub path: &'static str,
    /// Canonical error surface label expected for unsupported operations.
    pub surface: &'static str,
}

/// Shared conformance matrix for runtime primitive surface coverage.
///
/// This matrix is the authoritative coverage source for required operation
/// parity checks across external transports and CLI/manager paths.
#[derive(Debug, Clone, Copy)]
pub struct PrimitiveConformanceEntry {
    /// Runtime primitive this matrix row represents.
    pub operation: RuntimeOperation,
    /// OpenAPI surface claim for this primitive.
    pub openapi: Option<OpenApiPrimitiveSurface>,
    /// Whether the manager layer exposes/handles this primitive path.
    pub manager: bool,
    /// Whether gRPC transport metadata generation currently covers this primitive.
    pub grpc_metadata: bool,
    /// Whether stack CLI currently emits transport metadata assumptions for this primitive.
    pub cli: bool,
}

/// Coverage matrix for required sandbox/runtime operations.
///
/// Keep this list complete and aligned with `REQUIRED_RUNTIME_OPERATIONS`.
pub const PRIMITIVE_CONFORMANCE_MATRIX: &[PrimitiveConformanceEntry] = &[
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CreateSandbox,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/sandboxes",
            surface: "sandboxes",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::GetSandbox,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/sandboxes",
            surface: "sandboxes",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::TerminateSandbox,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/sandboxes",
            surface: "sandboxes",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::OpenLease,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/leases",
            surface: "leases",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::HeartbeatLease,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/leases",
            surface: "leases",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CloseLease,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/leases",
            surface: "leases",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::ResolveImage,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/images",
            surface: "images",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::PullImage,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/images",
            surface: "images",
        }),
        manager: true,
        grpc_metadata: false,
        cli: true,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::StartBuild,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/builds",
            surface: "builds",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::GetBuild,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/builds",
            surface: "builds",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::StreamBuildEvents,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/builds",
            surface: "builds",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CancelBuild,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/builds",
            surface: "builds",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CreateContainer,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/containers",
            surface: "containers",
        }),
        manager: true,
        grpc_metadata: true,
        cli: true,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::StartContainer,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/containers",
            surface: "containers",
        }),
        manager: false,
        grpc_metadata: true,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::StopContainer,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/containers",
            surface: "containers",
        }),
        manager: true,
        grpc_metadata: true,
        cli: true,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::RemoveContainer,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/containers",
            surface: "containers",
        }),
        manager: true,
        grpc_metadata: true,
        cli: true,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::GetContainerLogs,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/containers",
            surface: "containers",
        }),
        manager: true,
        grpc_metadata: false,
        cli: true,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::ExecContainer,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/executions",
            surface: "executions",
        }),
        manager: true,
        grpc_metadata: true,
        cli: true,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::WriteExecStdin,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/executions",
            surface: "executions",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::SignalExec,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/executions",
            surface: "executions",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::ResizeExecPty,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/executions",
            surface: "executions",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CancelExec,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/executions",
            surface: "executions",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CreateCheckpoint,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/checkpoints",
            surface: "checkpoints",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::RestoreCheckpoint,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/checkpoints",
            surface: "checkpoints",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::ForkCheckpoint,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/checkpoints",
            surface: "checkpoints",
        }),
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CreateVolume,
        openapi: None,
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::AttachVolume,
        openapi: None,
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::DetachVolume,
        openapi: None,
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::CreateNetworkDomain,
        openapi: None,
        manager: true,
        grpc_metadata: true,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::PublishPort,
        openapi: None,
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::ConnectContainer,
        openapi: None,
        manager: true,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::ListEvents,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/events/{stack_name}",
            surface: "events",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::GetReceipt,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/receipts/{receipt_id}",
            surface: "receipts",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
    PrimitiveConformanceEntry {
        operation: RuntimeOperation::GetCapabilities,
        openapi: Some(OpenApiPrimitiveSurface {
            path: "/v1/capabilities",
            surface: "capabilities",
        }),
        manager: false,
        grpc_metadata: false,
        cli: false,
    },
];

/// Generate the transport metadata payload for a sequence number and operation.
///
/// This is used by transports that need deterministic request metadata and
/// deterministic idempotency keys for repeatable retries.
pub fn transport_metadata_for_sequence(
    request_sequence: u64,
    operation: Option<RuntimeOperation>,
) -> (String, Option<String>) {
    let request_id = format!("req_{:016x}", request_sequence.saturating_add(1));
    let idempotency_key = operation
        .and_then(RuntimeOperation::idempotency_key_prefix)
        .map(|prefix| format!("{prefix}:{request_id}"));
    (request_id, idempotency_key)
}

/// Docker-compat command set supported by the Runtime V2 translation shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerShimCommand {
    Run,
    Exec,
    Ps,
    Logs,
    Pull,
    Build,
    Stop,
    Rm,
}

impl DockerShimCommand {
    /// V1 command coverage set in canonical order.
    pub const V1_ALL: [DockerShimCommand; 8] = [
        DockerShimCommand::Run,
        DockerShimCommand::Exec,
        DockerShimCommand::Ps,
        DockerShimCommand::Logs,
        DockerShimCommand::Pull,
        DockerShimCommand::Build,
        DockerShimCommand::Stop,
        DockerShimCommand::Rm,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            DockerShimCommand::Run => "run",
            DockerShimCommand::Exec => "exec",
            DockerShimCommand::Ps => "ps",
            DockerShimCommand::Logs => "logs",
            DockerShimCommand::Pull => "pull",
            DockerShimCommand::Build => "build",
            DockerShimCommand::Stop => "stop",
            DockerShimCommand::Rm => "rm",
        }
    }

    /// Canonical Runtime V2 operation mapped from this shim command.
    ///
    /// `None` indicates a read-only shim command handled via backend listing
    /// and not yet represented by a dedicated Runtime V2 operation enum variant.
    pub const fn runtime_operation(self) -> Option<RuntimeOperation> {
        match self {
            DockerShimCommand::Run => Some(RuntimeOperation::CreateContainer),
            DockerShimCommand::Exec => Some(RuntimeOperation::ExecContainer),
            DockerShimCommand::Ps => None,
            DockerShimCommand::Logs => Some(RuntimeOperation::GetContainerLogs),
            DockerShimCommand::Pull => Some(RuntimeOperation::PullImage),
            DockerShimCommand::Build => Some(RuntimeOperation::StartBuild),
            DockerShimCommand::Stop => Some(RuntimeOperation::StopContainer),
            DockerShimCommand::Rm => Some(RuntimeOperation::RemoveContainer),
        }
    }
}

/// Runtime operations every backend adapter must preserve with shared semantics.
///
/// This is the backend-facing subset of [`REQUIRED_RUNTIME_OPERATIONS`].
pub const REQUIRED_BACKEND_ADAPTER_OPERATIONS: &[RuntimeOperation] = &[
    RuntimeOperation::CreateSandbox,
    RuntimeOperation::TerminateSandbox,
    RuntimeOperation::CreateContainer,
    RuntimeOperation::StartContainer,
    RuntimeOperation::StopContainer,
    RuntimeOperation::RemoveContainer,
    RuntimeOperation::GetContainerLogs,
    RuntimeOperation::ExecContainer,
    RuntimeOperation::WriteExecStdin,
    RuntimeOperation::SignalExec,
    RuntimeOperation::ResizeExecPty,
    RuntimeOperation::CancelExec,
    RuntimeOperation::CreateCheckpoint,
    RuntimeOperation::RestoreCheckpoint,
    RuntimeOperation::ForkCheckpoint,
    RuntimeOperation::AttachVolume,
    RuntimeOperation::DetachVolume,
    RuntimeOperation::CreateNetworkDomain,
    RuntimeOperation::ConnectContainer,
    RuntimeOperation::PublishPort,
    RuntimeOperation::GetCapabilities,
];

/// Canonical capability matrix fields that may vary across backends.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackendCapabilityMatrix {
    pub fs_quick_checkpoint: bool,
    pub vm_full_checkpoint: bool,
    pub checkpoint_fork: bool,
    pub docker_compat: bool,
    pub compose_adapter: bool,
    pub gpu_passthrough: bool,
    pub live_resize: bool,
}

impl BackendCapabilityMatrix {
    /// Stable field names exposed by the Runtime V2 backend capability matrix.
    pub const FIELD_NAMES: [&'static str; 7] = [
        "fs_quick_checkpoint",
        "vm_full_checkpoint",
        "checkpoint_fork",
        "docker_compat",
        "compose_adapter",
        "gpu_passthrough",
        "live_resize",
    ];

    pub const fn from_runtime_capabilities(capabilities: RuntimeCapabilities) -> Self {
        Self {
            fs_quick_checkpoint: capabilities.fs_quick_checkpoint,
            vm_full_checkpoint: capabilities.vm_full_checkpoint,
            checkpoint_fork: capabilities.checkpoint_fork,
            docker_compat: capabilities.docker_compat,
            compose_adapter: capabilities.compose_adapter,
            gpu_passthrough: capabilities.gpu_passthrough,
            live_resize: capabilities.live_resize,
        }
    }
}

/// Project backend capabilities into the canonical backend matrix shape.
pub const fn backend_capability_matrix(
    capabilities: RuntimeCapabilities,
) -> BackendCapabilityMatrix {
    BackendCapabilityMatrix::from_runtime_capabilities(capabilities)
}

/// Canonical Runtime V2 capability surface for first-party backend adapters.
pub fn canonical_backend_capabilities(backend: &SandboxBackend) -> RuntimeCapabilities {
    let mut capabilities = RuntimeCapabilities::stack_baseline();
    match backend {
        SandboxBackend::MacosVz | SandboxBackend::LinuxFirecracker => {
            capabilities.fs_quick_checkpoint = true;
            capabilities.vm_full_checkpoint = false;
            capabilities.checkpoint_fork = true;
        }
        SandboxBackend::Other(_) => {}
    }
    capabilities
}

/// Validate backend adapter operation parity rules that are independent of capabilities.
pub fn validate_backend_adapter_contract_surface() -> Result<(), RuntimeError> {
    for operation in REQUIRED_BACKEND_ADAPTER_OPERATIONS {
        if operation.requires_idempotency_key() && operation.idempotency_key_prefix().is_none() {
            return Err(RuntimeError::InvalidConfig(format!(
                "backend adapter operation `{}` requires idempotency key metadata",
                operation.as_str()
            )));
        }
    }

    Ok(())
}

/// Validate backend adapter capability parity requirements shared across runtimes.
pub fn validate_backend_adapter_parity(
    capabilities: RuntimeCapabilities,
) -> Result<(), RuntimeError> {
    let matrix = backend_capability_matrix(capabilities);
    if !matrix.fs_quick_checkpoint {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::CreateCheckpoint.as_str().to_string(),
            reason: "backend parity requires fs_quick_checkpoint baseline".to_string(),
        });
    }
    if !matrix.checkpoint_fork {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::ForkCheckpoint.as_str().to_string(),
            reason: "backend parity requires checkpoint_fork baseline".to_string(),
        });
    }
    if !capabilities.shared_vm {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::CreateContainer.as_str().to_string(),
            reason: "backend parity requires shared_vm baseline".to_string(),
        });
    }
    if !capabilities.stack_networking {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::CreateNetworkDomain.as_str().to_string(),
            reason: "backend parity requires stack_networking baseline".to_string(),
        });
    }
    if !capabilities.container_logs {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::GetContainerLogs.as_str().to_string(),
            reason: "backend parity requires container_logs baseline".to_string(),
        });
    }

    Ok(())
}
