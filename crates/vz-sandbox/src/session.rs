//! Active sandbox session with command execution.

/// Output from a command executed inside the sandbox.
#[derive(Debug)]
pub struct ExecOutput {
    /// Exit code of the command.
    pub exit_code: i32,
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
}

/// An active sandbox session with a mounted project.
///
/// Provides command execution inside the VM and access to
/// the vsock communication channel.
pub struct SandboxSession {
    // Will hold: Vm handle, VsockStream, project path info
}

impl SandboxSession {
    /// Execute a command inside the sandbox.
    ///
    /// Commands run in the VM's shell with the working directory
    /// set to the mounted project directory.
    pub async fn exec(&self, cmd: &str) -> anyhow::Result<ExecOutput> {
        let _ = cmd;
        // TODO: Phase 2
        // Execute via SSH or guest agent over vsock
        todo!("Phase 2: implement exec")
    }

    /// Get the vsock channel for custom protocols.
    ///
    /// Use this to implement tool forwarding or other
    /// host↔guest communication patterns.
    pub fn vsock_stream(&self) -> &vz::VsockStream {
        // TODO: Phase 2
        todo!("Phase 2: implement vsock_stream accessor")
    }

    /// Path where the project is mounted inside the VM.
    pub fn project_path(&self) -> &str {
        // TODO: Phase 2
        // e.g., "/mnt/workspace/my-project"
        todo!("Phase 2: implement project_path")
    }
}
