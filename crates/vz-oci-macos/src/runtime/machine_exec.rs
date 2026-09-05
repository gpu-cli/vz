//! Supervised ordinary execution against an exact, already leased Machine VM.

use super::{LinuxVm, OciError, SharedVmLifecycleLease};
use std::sync::Arc;
use vz_linux::{ContainerExecDispatchGate, ContainerExecStartError, ExecOptions, GrpcExecStream};

impl SharedVmLifecycleLease {
    async fn machine_exec_vm(&self) -> Result<Arc<LinuxVm>, OciError> {
        let record = self
            .stack_vms
            .lock()
            .await
            .get(&self.runtime_identity.stack_id)
            .cloned();
        super::stack_vm::require_exact_stack_runtime(
            record.as_ref().map(|record| &record.identity),
            &self.runtime_identity,
        )?;
        record
            .map(|record| record.vm)
            .ok_or_else(|| OciError::SharedRuntimeAbsent {
                stack_id: self.runtime_identity.stack_id.clone(),
            })
    }

    /// Allocate an authenticated single-use request ticket in this exact boot.
    pub async fn prepare_machine_exec_request(&self) -> Result<String, OciError> {
        Ok(self
            .machine_exec_vm()
            .await?
            .prepare_machine_exec_request()
            .await?)
    }

    /// Start an ordinary guest process, retaining this lease until terminal proof.
    /// No OCI container, replacement VM, or host process is selected.
    pub async fn start_machine_exec(
        &self,
        gate: ContainerExecDispatchGate,
        request_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        pty: Option<(u32, u32)>,
    ) -> Result<(GrpcExecStream, u64), ContainerExecStartError> {
        let vm = self.machine_exec_vm().await.map_err(|error| {
            ContainerExecStartError::Definite(vz_linux::LinuxError::Protocol(error.to_string()))
        })?;
        vm.exec_machine_stream_ready_classified_for_request(
            gate, request_id, command, args, options, pty,
        )
        .await
    }

    /// Write bytes to this exact boot's supervised process.
    pub async fn machine_exec_stdin(&self, id: u64, bytes: &[u8]) -> Result<(), OciError> {
        Ok(self.machine_exec_vm().await?.stdin_write(id, bytes).await?)
    }
    /// Close stdin without cancelling the supervised process or its output.
    pub async fn machine_exec_stdin_close(&self, id: u64) -> Result<(), OciError> {
        Ok(self.machine_exec_vm().await?.stdin_close(id).await?)
    }
    /// Forward a process signal within the exact leased boot.
    pub async fn machine_exec_signal(&self, id: u64, signal: i32) -> Result<(), OciError> {
        Ok(self.machine_exec_vm().await?.signal(id, signal).await?)
    }
    /// Resize the exact supervised process's terminal (rows, columns).
    pub async fn machine_exec_resize(
        &self,
        id: u64,
        rows: u32,
        columns: u32,
    ) -> Result<(), OciError> {
        Ok(self
            .machine_exec_vm()
            .await?
            .resize_exec_pty(id, rows, columns)
            .await?)
    }
    /// Cancel and return a guest-confirmed, reaped shell-compatible exit status.
    pub async fn cancel_machine_exec(&self, id: u64) -> Result<i32, OciError> {
        Ok(self
            .machine_exec_vm()
            .await?
            .cancel_exec(id)
            .await?
            .exit_code)
    }
    /// Reconcile an exact single-use ticket after ambiguous dispatch.
    pub async fn reconcile_machine_exec(
        &self,
        request_id: String,
    ) -> Result<vz_linux::ReconcileExecResponse, OciError> {
        Ok(self
            .machine_exec_vm()
            .await?
            .reconcile_exec_request(request_id)
            .await?)
    }
}
