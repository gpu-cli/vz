use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ContractInvariantError;

/// Immutable image reference resolved by digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Image {
    /// User-provided image reference (tag or digest).
    pub image_ref: String,
    /// Resolved immutable image digest.
    pub resolved_digest: String,
    /// Target platform identifier.
    pub platform: String,
    /// Source registry name/host.
    pub source_registry: String,
    /// Pull completion timestamp in unix epoch seconds.
    pub pulled_at: u64,
}

impl Image {
    /// Validate digest immutability expectations for runtime execution.
    pub fn ensure_digest_immutable(&self) -> Result<(), ContractInvariantError> {
        if !self.resolved_digest.starts_with("sha256:") {
            return Err(ContractInvariantError::ImageDigestInvariant {
                image_ref: self.image_ref.clone(),
                details: "resolved digest must use sha256:<hex> form".to_string(),
            });
        }

        if self.resolved_digest.len() <= "sha256:".len() {
            return Err(ContractInvariantError::ImageDigestInvariant {
                image_ref: self.image_ref.clone(),
                details: "resolved digest must include digest bytes".to_string(),
            });
        }

        Ok(())
    }
}

/// Build request details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BuildSpec {
    /// Build context URI or path.
    pub context: String,
    /// Optional Dockerfile path in the context.
    pub dockerfile: Option<String>,
    /// Optional multi-stage target to build.
    pub target: Option<String>,
    /// Build arguments supplied to the builder.
    pub args: BTreeMap<String, String>,
    /// Optional cache sources (for example registry references).
    #[serde(default)]
    pub cache_from: Vec<String>,
    /// Optional image tag/name to publish the build output under.
    ///
    /// When unset, backends may derive a deterministic internal tag.
    pub image_tag: Option<String>,
    /// Build secrets forwarded to builder (`id=...,src=...`).
    #[serde(default)]
    pub secrets: Vec<String>,
    /// Disable builder cache for this build request.
    #[serde(default)]
    pub no_cache: bool,
    /// Push built image to registry instead of local import.
    #[serde(default)]
    pub push: bool,
    /// Optional OCI tar output destination path on host.
    pub output_oci_tar_dest: Option<String>,
}

/// Build lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// Build accepted but not yet started.
    Queued,
    /// Build is currently running.
    Running,
    /// Build completed successfully.
    Succeeded,
    /// Build completed with failure.
    Failed,
    /// Build canceled before completion.
    Canceled,
}

impl BuildState {
    /// Whether this state is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    fn can_transition_to(self, next: BuildState) -> bool {
        matches!(
            (self, next),
            (BuildState::Queued, BuildState::Running)
                | (BuildState::Queued, BuildState::Canceled)
                | (BuildState::Queued, BuildState::Failed)
                | (BuildState::Running, BuildState::Succeeded)
                | (BuildState::Running, BuildState::Failed)
                | (BuildState::Running, BuildState::Canceled)
        )
    }
}

/// Asynchronous image build operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Build {
    /// Build identifier.
    pub build_id: String,
    /// Sandbox where build executes.
    pub sandbox_id: String,
    /// Build request specification.
    pub build_spec: BuildSpec,
    /// Current build state.
    pub state: BuildState,
    /// Resulting image digest, available on success.
    pub result_digest: Option<String>,
    /// Build start timestamp in unix epoch seconds.
    pub started_at: u64,
    /// Build end timestamp when terminal.
    pub ended_at: Option<u64>,
}

impl Build {
    /// Validate core build invariants against current state.
    pub fn ensure_lifecycle_consistency(&self) -> Result<(), ContractInvariantError> {
        if let Some(ended_at) = self.ended_at {
            if ended_at < self.started_at {
                return Err(ContractInvariantError::BuildLifecycleInconsistency {
                    build_id: self.build_id.clone(),
                    details: "end time cannot precede start time".to_string(),
                });
            }
        }

        match self.state {
            BuildState::Succeeded => {
                if self.result_digest.is_none() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "successful builds must include a result digest".to_string(),
                    });
                }
                if self.ended_at.is_none() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "successful builds must include an end time".to_string(),
                    });
                }
            }
            BuildState::Failed | BuildState::Canceled => {
                if self.ended_at.is_none() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "terminal builds must include an end time".to_string(),
                    });
                }
            }
            BuildState::Queued | BuildState::Running => {
                if self.ended_at.is_some() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "non-terminal builds cannot include an end time".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Transition to a new build state if allowed.
    pub fn transition_to(&mut self, next: BuildState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::BuildStateTransition {
                build_id: self.build_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}
