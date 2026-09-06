//! Target-specific runtime ownership behind the shared Machine lifecycle.
use crate::native_macos::runtime::{NativeMacosLease, NativeMacosRuntime};
use std::sync::Arc;
use vz_oci_macos::{MacosOciError as Error, MacosRuntimeBackend, Runtime, SharedVmLifecycleLease};
use vz_runtime_contract::{StackRuntimeShutdownOutcome, StackRuntimeShutdownRequest};

pub enum MachineBackendRuntime {
    Linux(Box<MacosRuntimeBackend>),
    Native(Arc<NativeMacosRuntime>),
}
impl MachineBackendRuntime {
    pub fn new(runtime: Runtime) -> Self {
        Self::Linux(Box::new(MacosRuntimeBackend::new(runtime)))
    }
    pub fn linux(&self) -> Result<&Runtime, Error> {
        match self {
            Self::Linux(runtime) => Ok(runtime.inner()),
            Self::Native(_) => Err(Error::InvalidConfig(
                "native macOS has no Linux OCI runtime".into(),
            )),
        }
    }
    pub async fn shutdown_shared_vm_with_receipt_exact(
        &self,
        request: &StackRuntimeShutdownRequest,
    ) -> Result<
        (
            StackRuntimeShutdownOutcome,
            Option<vz_linux::DockerShutdownComplete>,
        ),
        Error,
    > {
        match self {
            Self::Linux(runtime) => {
                runtime
                    .inner()
                    .shutdown_shared_vm_with_receipt_exact(request)
                    .await
            }
            Self::Native(runtime) => {
                runtime.stop_exact(request).await?;
                Ok((StackRuntimeShutdownOutcome::Stopped, None))
            }
        }
    }
}

pub enum MachineExecutionLease {
    Linux(SharedVmLifecycleLease),
    Native(NativeMacosLease),
}
impl MachineExecutionLease {
    pub fn runtime_identity(&self) -> &vz_runtime_contract::StackRuntimeIdentity {
        match self {
            Self::Linux(lease) => lease.runtime_identity(),
            Self::Native(lease) => lease.identity(),
        }
    }
    pub async fn prepare_machine_exec_request(&self) -> Result<String, Error> {
        match self {
            Self::Linux(l) => l.prepare_machine_exec_request().await,
            Self::Native(l) => Ok(l.client().await?.prepare_machine_exec_request().await?),
        }
    }
    pub async fn machine_exec_stdin(&self, id: u64, bytes: &[u8]) -> Result<(), Error> {
        match self {
            Self::Linux(l) => l.machine_exec_stdin(id, bytes).await,
            Self::Native(l) => Ok(l.client().await?.stdin_write(id, bytes).await?),
        }
    }
    pub async fn machine_exec_stdin_close(&self, id: u64) -> Result<(), Error> {
        match self {
            Self::Linux(l) => l.machine_exec_stdin_close(id).await,
            Self::Native(l) => Ok(l.client().await?.stdin_close(id).await?),
        }
    }
    pub async fn machine_exec_signal(&self, id: u64, signal: i32) -> Result<(), Error> {
        match self {
            Self::Linux(l) => l.machine_exec_signal(id, signal).await,
            Self::Native(l) => Ok(l.client().await?.signal(id, signal).await?),
        }
    }
    pub async fn machine_exec_resize(&self, id: u64, rows: u32, columns: u32) -> Result<(), Error> {
        match self {
            Self::Linux(l) => l.machine_exec_resize(id, rows, columns).await,
            Self::Native(l) => Ok(l.client().await?.resize_exec_pty(id, rows, columns).await?),
        }
    }
    pub async fn reconcile_machine_exec(
        &self,
        request: String,
    ) -> Result<vz_linux::ReconcileExecResponse, Error> {
        match self {
            Self::Linux(l) => l.reconcile_machine_exec(request).await,
            Self::Native(l) => Ok(l.client().await?.reconcile_exec_request(request).await?),
        }
    }
    pub async fn cancel_machine_exec(&self, id: u64) -> Result<i32, Error> {
        match self {
            Self::Linux(l) => l.cancel_machine_exec(id).await,
            Self::Native(l) => Ok(l.client().await?.cancel_exec(id).await?.exit_code),
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn start_machine_exec(
        &self,
        gate: vz_linux::ContainerExecDispatchGate,
        request: String,
        command: String,
        args: Vec<String>,
        options: vz_linux::ExecOptions,
        pty: Option<(u32, u32)>,
    ) -> Result<(vz_linux::GrpcExecStream, u64), vz_linux::ContainerExecStartError> {
        match self {
            Self::Linux(l) => {
                l.start_machine_exec(gate, request, command, args, options, pty)
                    .await
            }
            Self::Native(l) => {
                l.client()
                    .await
                    .map_err(|e| {
                        vz_linux::ContainerExecStartError::Definite(vz_linux::LinuxError::Protocol(
                            e.to_string(),
                        ))
                    })?
                    .exec_machine_stream_ready_for_request(
                        gate, request, command, args, options, pty,
                    )
                    .await
            }
        }
    }
}
