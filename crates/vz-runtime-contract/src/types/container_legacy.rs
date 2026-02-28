use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::ContractInvariantError;

/// Runtime status for a tracked container.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContainerStatus {
    /// Container metadata created, but execution hasn't started yet.
    Created,
    /// Container is currently running.
    Running,
    /// Container exited with an exit code.
    Stopped {
        /// Exit code from the container command.
        exit_code: i32,
    },
}

/// Container metadata record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerInfo {
    /// Container identifier.
    pub id: String,
    /// Original image reference used for creation.
    pub image: String,
    /// Resolved image digest identifier.
    pub image_id: String,
    /// Container lifecycle status.
    pub status: ContainerStatus,
    /// Unix epoch seconds when metadata was created.
    pub created_unix_secs: u64,
    /// Unix epoch seconds when the container was started, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_unix_secs: Option<u64>,
    /// Unix epoch seconds when the container stopped, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_unix_secs: Option<u64>,
    /// Assembled rootfs path for this container, when known.
    pub rootfs_path: Option<PathBuf>,
    /// Host process ID currently managing this container, if running.
    pub host_pid: Option<u32>,
}

impl ContainerInfo {
    /// Verify the lifecycle timestamps are internally consistent for this status.
    pub fn ensure_lifecycle_consistency(&self) -> Result<(), ContractInvariantError> {
        match &self.status {
            ContainerStatus::Created => {
                if self.started_unix_secs.is_some() {
                    return Err(
                        self.lifecycle_error("created containers must not report a start time")
                    );
                }
                if self.stopped_unix_secs.is_some() {
                    return Err(
                        self.lifecycle_error("created containers must not report a stop time")
                    );
                }
            }
            ContainerStatus::Running => {
                let started = match self.started_unix_secs {
                    Some(val) => val,
                    None => {
                        return Err(
                            self.lifecycle_error("running containers must record a start time")
                        );
                    }
                };
                if self.stopped_unix_secs.is_some() {
                    return Err(
                        self.lifecycle_error("running containers must not report a stop time")
                    );
                }
                if started < self.created_unix_secs {
                    return Err(self.lifecycle_error("start time cannot precede create time"));
                }
            }
            ContainerStatus::Stopped { .. } => {
                let started = match self.started_unix_secs {
                    Some(val) => val,
                    None => {
                        return Err(
                            self.lifecycle_error("stopped containers must record a start time")
                        );
                    }
                };
                let stopped = match self.stopped_unix_secs {
                    Some(val) => val,
                    None => {
                        return Err(
                            self.lifecycle_error("stopped containers must record a stop time")
                        );
                    }
                };
                if started > stopped {
                    return Err(self.lifecycle_error("stop time cannot precede start time"));
                }
                if started < self.created_unix_secs {
                    return Err(self.lifecycle_error("start time cannot precede create time"));
                }
            }
        }

        Ok(())
    }

    fn lifecycle_error(&self, details: &str) -> ContractInvariantError {
        ContractInvariantError::LifecycleInconsistency {
            container_id: self.id.clone(),
            details: details.to_string(),
        }
    }
}
