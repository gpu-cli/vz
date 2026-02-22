//! Container lifecycle management for Linux-native backend.
//!
//! Orchestrates OCI runtime binary invocations for the full container
//! lifecycle: create → start → exec → stop → delete.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::config::LinuxNativeConfig;
use crate::error::LinuxNativeError;
use crate::process::{self, ExecOptions, OciState};
use vz_oci::bundle::{self, BundleSpec};

/// Container lifecycle manager for Linux-native execution.
///
/// Manages OCI bundles and shells out to an OCI runtime binary
/// (youki, runc) for container operations.
pub struct ContainerRuntime {
    config: LinuxNativeConfig,
}

impl ContainerRuntime {
    /// Create a new container runtime with the given configuration.
    pub fn new(config: LinuxNativeConfig) -> Self {
        Self { config }
    }

    /// Create a container from a rootfs directory with the given spec.
    ///
    /// This generates an OCI bundle and calls `<runtime> create`.
    pub async fn create(
        &self,
        container_id: &str,
        rootfs_dir: &Path,
        spec: BundleSpec,
    ) -> Result<(), LinuxNativeError> {
        let bundle_dir = self.bundle_path(container_id);
        let state_dir = self.config.state_dir();

        info!(container_id, ?bundle_dir, "creating container");

        std::fs::create_dir_all(&state_dir)?;

        bundle::write_oci_bundle(&bundle_dir, rootfs_dir, spec)?;

        process::oci_create(
            self.config.runtime.binary_name(),
            container_id,
            &bundle_dir,
            &state_dir,
        )
        .await?;

        debug!(container_id, "container created");
        Ok(())
    }

    /// Start a previously created container.
    pub async fn start(&self, container_id: &str) -> Result<(), LinuxNativeError> {
        let state_dir = self.config.state_dir();
        info!(container_id, "starting container");

        process::oci_start(self.config.runtime.binary_name(), container_id, &state_dir).await?;

        debug!(container_id, "container started");
        Ok(())
    }

    /// Get the state of a container.
    pub async fn state(&self, container_id: &str) -> Result<OciState, LinuxNativeError> {
        let state_dir = self.config.state_dir();
        process::oci_state(self.config.runtime.binary_name(), container_id, &state_dir).await
    }

    /// Execute a command inside a running container.
    pub async fn exec(
        &self,
        container_id: &str,
        cmd: &[String],
        env: &[(String, String)],
        cwd: Option<&str>,
        user: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<process::ProcessOutput, LinuxNativeError> {
        let state_dir = self.config.state_dir();
        debug!(container_id, ?cmd, "executing in container");

        process::oci_exec(ExecOptions {
            runtime_binary: self.config.runtime.binary_name(),
            container_id,
            state_dir: &state_dir,
            cmd,
            env,
            cwd,
            user,
            timeout,
        })
        .await
    }

    /// Stop a running container by sending SIGTERM, waiting, then SIGKILL.
    pub async fn stop(&self, container_id: &str, force: bool) -> Result<(), LinuxNativeError> {
        let state_dir = self.config.state_dir();
        let runtime = self.config.runtime.binary_name();

        if force {
            info!(container_id, "force-stopping container");
            process::oci_kill(runtime, container_id, &state_dir, "SIGKILL").await?;
        } else {
            info!(container_id, "stopping container");
            process::oci_kill(runtime, container_id, &state_dir, "SIGTERM").await?;

            // Wait briefly for graceful shutdown, then force kill.
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Check if still running.
            match process::oci_state(runtime, container_id, &state_dir).await {
                Ok(state) if state.status == "running" => {
                    warn!(
                        container_id,
                        "container still running after SIGTERM, sending SIGKILL"
                    );
                    process::oci_kill(runtime, container_id, &state_dir, "SIGKILL").await?;
                }
                _ => {
                    debug!(container_id, "container stopped after SIGTERM");
                }
            }
        }

        Ok(())
    }

    /// Delete a container and clean up its bundle directory.
    pub async fn delete(&self, container_id: &str, force: bool) -> Result<(), LinuxNativeError> {
        let state_dir = self.config.state_dir();
        let runtime = self.config.runtime.binary_name();

        info!(container_id, "deleting container");

        process::oci_delete(runtime, container_id, &state_dir, force).await?;

        // Clean up bundle directory.
        let bundle_dir = self.bundle_path(container_id);
        if bundle_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&bundle_dir) {
                warn!(container_id, error = %e, "failed to clean up bundle directory");
            }
        }

        debug!(container_id, "container deleted");
        Ok(())
    }

    /// Convenience: create + start a container in one call.
    pub async fn create_and_start(
        &self,
        container_id: &str,
        rootfs_dir: &Path,
        spec: BundleSpec,
    ) -> Result<(), LinuxNativeError> {
        self.create(container_id, rootfs_dir, spec).await?;
        self.start(container_id).await?;
        Ok(())
    }

    /// Get the bundle directory path for a container.
    fn bundle_path(&self, container_id: &str) -> PathBuf {
        self.config.bundle_dir().join(container_id)
    }
}
