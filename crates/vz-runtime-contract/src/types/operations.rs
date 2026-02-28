use serde::{Deserialize, Serialize};

use super::RuntimeCapabilities;

/// Backend-declared runtime capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Supports full VM checkpoint captures.
    VmFullCheckpoint,
    /// Supports checkpoint fork semantics.
    CheckpointFork,
    /// Supports Docker compatibility adapter.
    DockerCompat,
    /// Supports Compose adapter semantics.
    ComposeAdapter,
    /// Supports build cache export/import.
    BuildCacheExport,
    /// Supports GPU passthrough.
    GpuPassthrough,
    /// Supports fs-focused quick checkpoints.
    FsQuickCheckpoint,
    /// Supports shared multi-service VM mode.
    SharedVm,
    /// Supports stack network setup/teardown APIs.
    StackNetworking,
    /// Supports runtime log retrieval.
    ContainerLogs,
    /// Supports live resize operations.
    LiveResize,
}

impl RuntimeCapabilities {
    /// Convert bool flags to a stable capability list.
    pub fn to_capability_list(self) -> Vec<Capability> {
        let mut list = Vec::new();
        if self.vm_full_checkpoint {
            list.push(Capability::VmFullCheckpoint);
        }
        if self.checkpoint_fork {
            list.push(Capability::CheckpointFork);
        }
        if self.docker_compat {
            list.push(Capability::DockerCompat);
        }
        if self.compose_adapter {
            list.push(Capability::ComposeAdapter);
        }
        if self.build_cache_export {
            list.push(Capability::BuildCacheExport);
        }
        if self.gpu_passthrough {
            list.push(Capability::GpuPassthrough);
        }
        if self.fs_quick_checkpoint {
            list.push(Capability::FsQuickCheckpoint);
        }
        if self.shared_vm {
            list.push(Capability::SharedVm);
        }
        if self.stack_networking {
            list.push(Capability::StackNetworking);
        }
        if self.container_logs {
            list.push(Capability::ContainerLogs);
        }
        if self.live_resize {
            list.push(Capability::LiveResize);
        }
        list
    }
}

/// Canonical Runtime V2 operation surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOperation {
    /// Create a new sandbox.
    CreateSandbox,
    /// Get sandbox details by identifier.
    GetSandbox,
    /// Terminate an existing sandbox.
    TerminateSandbox,
    /// Open a lease for sandbox operations.
    OpenLease,
    /// Heartbeat an existing lease.
    HeartbeatLease,
    /// Close an existing lease.
    CloseLease,
    /// Resolve an image reference to immutable digest.
    ResolveImage,
    /// Pull an image reference.
    PullImage,
    /// Start asynchronous build operation.
    StartBuild,
    /// Get build status/details.
    GetBuild,
    /// Stream build events.
    StreamBuildEvents,
    /// Cancel a running build.
    CancelBuild,
    /// Create a container.
    CreateContainer,
    /// Start a created container.
    StartContainer,
    /// Stop a running container.
    StopContainer,
    /// Remove a container.
    RemoveContainer,
    /// Retrieve container logs.
    GetContainerLogs,
    /// Execute command in container.
    ExecContainer,
    /// Write stdin to running exec.
    WriteExecStdin,
    /// Signal running exec.
    SignalExec,
    /// Resize PTY for running exec.
    ResizeExecPty,
    /// Cancel running exec.
    CancelExec,
    /// Create checkpoint.
    CreateCheckpoint,
    /// Restore checkpoint.
    RestoreCheckpoint,
    /// Fork checkpoint into new lineage.
    ForkCheckpoint,
    /// Create new volume.
    CreateVolume,
    /// Attach volume to workload.
    AttachVolume,
    /// Detach volume from workload.
    DetachVolume,
    /// Create isolated network domain.
    CreateNetworkDomain,
    /// Publish ingress port.
    PublishPort,
    /// Connect container to network domain.
    ConnectContainer,
    /// List events from a cursor.
    ListEvents,
    /// Get immutable operation receipt.
    GetReceipt,
    /// Query backend capabilities.
    GetCapabilities,
}

impl RuntimeOperation {
    /// All required Runtime V2 operations.
    pub const ALL: [RuntimeOperation; 34] = [
        RuntimeOperation::CreateSandbox,
        RuntimeOperation::GetSandbox,
        RuntimeOperation::TerminateSandbox,
        RuntimeOperation::OpenLease,
        RuntimeOperation::HeartbeatLease,
        RuntimeOperation::CloseLease,
        RuntimeOperation::ResolveImage,
        RuntimeOperation::PullImage,
        RuntimeOperation::StartBuild,
        RuntimeOperation::GetBuild,
        RuntimeOperation::StreamBuildEvents,
        RuntimeOperation::CancelBuild,
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
        RuntimeOperation::CreateVolume,
        RuntimeOperation::AttachVolume,
        RuntimeOperation::DetachVolume,
        RuntimeOperation::CreateNetworkDomain,
        RuntimeOperation::PublishPort,
        RuntimeOperation::ConnectContainer,
        RuntimeOperation::ListEvents,
        RuntimeOperation::GetReceipt,
        RuntimeOperation::GetCapabilities,
    ];

    /// Whether this operation requires an idempotency key for retries.
    pub const fn requires_idempotency_key(self) -> bool {
        matches!(
            self,
            RuntimeOperation::CreateSandbox
                | RuntimeOperation::OpenLease
                | RuntimeOperation::PullImage
                | RuntimeOperation::StartBuild
                | RuntimeOperation::CreateContainer
                | RuntimeOperation::ExecContainer
                | RuntimeOperation::CreateCheckpoint
                | RuntimeOperation::ForkCheckpoint
        )
    }

    /// Canonical idempotency key prefix for this operation, if required.
    pub const fn idempotency_key_prefix(self) -> Option<&'static str> {
        match self {
            RuntimeOperation::CreateSandbox => Some("create_sandbox"),
            RuntimeOperation::OpenLease => Some("open_lease"),
            RuntimeOperation::PullImage => Some("pull_image"),
            RuntimeOperation::StartBuild => Some("start_build"),
            RuntimeOperation::CreateContainer => Some("create_container"),
            RuntimeOperation::ExecContainer => Some("exec_container"),
            RuntimeOperation::CreateCheckpoint => Some("create_checkpoint"),
            RuntimeOperation::ForkCheckpoint => Some("fork_checkpoint"),
            _ => None,
        }
    }

    /// Canonical operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            RuntimeOperation::CreateSandbox => "create_sandbox",
            RuntimeOperation::GetSandbox => "get_sandbox",
            RuntimeOperation::TerminateSandbox => "terminate_sandbox",
            RuntimeOperation::OpenLease => "open_lease",
            RuntimeOperation::HeartbeatLease => "heartbeat_lease",
            RuntimeOperation::CloseLease => "close_lease",
            RuntimeOperation::ResolveImage => "resolve_image",
            RuntimeOperation::PullImage => "pull_image",
            RuntimeOperation::StartBuild => "start_build",
            RuntimeOperation::GetBuild => "get_build",
            RuntimeOperation::StreamBuildEvents => "stream_build_events",
            RuntimeOperation::CancelBuild => "cancel_build",
            RuntimeOperation::CreateContainer => "create_container",
            RuntimeOperation::StartContainer => "start_container",
            RuntimeOperation::StopContainer => "stop_container",
            RuntimeOperation::RemoveContainer => "remove_container",
            RuntimeOperation::GetContainerLogs => "get_container_logs",
            RuntimeOperation::ExecContainer => "exec_container",
            RuntimeOperation::WriteExecStdin => "write_exec_stdin",
            RuntimeOperation::SignalExec => "signal_exec",
            RuntimeOperation::ResizeExecPty => "resize_exec_pty",
            RuntimeOperation::CancelExec => "cancel_exec",
            RuntimeOperation::CreateCheckpoint => "create_checkpoint",
            RuntimeOperation::RestoreCheckpoint => "restore_checkpoint",
            RuntimeOperation::ForkCheckpoint => "fork_checkpoint",
            RuntimeOperation::CreateVolume => "create_volume",
            RuntimeOperation::AttachVolume => "attach_volume",
            RuntimeOperation::DetachVolume => "detach_volume",
            RuntimeOperation::CreateNetworkDomain => "create_network_domain",
            RuntimeOperation::PublishPort => "publish_port",
            RuntimeOperation::ConnectContainer => "connect_container",
            RuntimeOperation::ListEvents => "list_events",
            RuntimeOperation::GetReceipt => "get_receipt",
            RuntimeOperation::GetCapabilities => "get_capabilities",
        }
    }
}
