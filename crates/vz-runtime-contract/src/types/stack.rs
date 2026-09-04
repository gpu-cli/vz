use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{EnvironmentId, MachineId, MachineIncarnationId, ProjectId};

pub const MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION: u32 = 1;
/// Schema version for generation-qualified container lifecycle proofs and receipts.
pub const CONTAINER_GENERATION_LIFECYCLE_SCHEMA_VERSION: u32 = 1;
/// Maximum timeout accepted by the bounded generation-qualified exec contract.
pub const MAX_CONTAINER_GENERATION_EXEC_TIMEOUT_MILLIS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// Maximum combined stdout and stderr retained by one exact exec operation.
pub const MAX_CONTAINER_GENERATION_EXEC_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum duration of one bounded lifecycle snapshot inspection.
pub const MAX_CONTAINER_GENERATION_LIFECYCLE_INSPECT_TIMEOUT_MILLIS: u64 = 60_000;

/// Exact topology scope for workloads placed on one current Machine incarnation.
///
/// Reservation identity is deliberately absent: callers reuse this scope for the
/// Machine workload, then mint a distinct durable reservation for each container
/// create intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineWorkloadScope {
    pub schema_version: u32,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub machine_id: MachineId,
    pub machine_incarnation_id: MachineIncarnationId,
    pub stack_id: String,
}

impl MachineWorkloadScope {
    /// Validate IDs that may have crossed an untrusted serialization boundary.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported Machine workload scope schema version {}",
                self.schema_version
            ));
        }
        ProjectId::new(self.project_id.as_str()).map_err(|error| error.to_string())?;
        EnvironmentId::new(self.environment_id.as_str()).map_err(|error| error.to_string())?;
        MachineId::new(self.machine_id.as_str()).map_err(|error| error.to_string())?;
        MachineIncarnationId::new(self.machine_incarnation_id.as_str())
            .map_err(|error| error.to_string())?;
        validate_stack_id(&self.stack_id)
    }

    /// Bind a caller-persisted reservation identity to this workload scope.
    pub fn container_generation_scope(
        &self,
        reservation_id: impl Into<String>,
    ) -> Result<ContainerGenerationScope, String> {
        self.validate()?;
        let scope = ContainerGenerationScope {
            reservation_id: reservation_id.into(),
            project_id: self.project_id.clone(),
            environment_id: self.environment_id.clone(),
            machine_id: self.machine_id.clone(),
            machine_incarnation_id: Some(self.machine_incarnation_id.clone()),
            stack_id: self.stack_id.clone(),
        };
        scope.validate()?;
        Ok(scope)
    }
}

/// Exact topology and operation scope that admits one container generation.
///
/// `reservation_id` is allocated before the runtime call. It lets recovery
/// distinguish a retried create from an older reservation even when every
/// human-facing name is reused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationScope {
    /// Unique, immutable identifier for this create reservation.
    pub reservation_id: String,
    /// Owning Project identity.
    pub project_id: ProjectId,
    /// Owning Developer Environment identity.
    pub environment_id: EnvironmentId,
    /// Owning Machine identity.
    pub machine_id: MachineId,
    /// Machine incarnation active when the generation was admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_incarnation_id: Option<MachineIncarnationId>,
    /// Stack identity within the owning Machine.
    pub stack_id: String,
}

impl ContainerGenerationScope {
    /// Construct a topology-native generation scope from a current Machine workload.
    pub fn for_machine_workload(
        workload: &MachineWorkloadScope,
        reservation_id: impl Into<String>,
    ) -> Result<Self, String> {
        workload.container_generation_scope(reservation_id)
    }

    /// Mint an explicitly synthetic topology scope for the pre-topology stack API.
    ///
    /// This exists only to keep legacy stack-name callers on the exact scoped
    /// generation path while topology identity is plumbed through those callers.
    /// Every invocation receives a fresh reservation identity.
    pub fn synthetic_legacy_stack(stack_id: &str) -> Result<Self, String> {
        validate_stack_id(stack_id)?;
        let scope = Self {
            reservation_id: format!("legacy-stack-compat-{}", Uuid::new_v4().simple()),
            project_id: ProjectId::new("prj_synthetic_legacy_stack_compat")
                .map_err(|error| error.to_string())?,
            environment_id: EnvironmentId::new("env_synthetic_legacy_stack_compat")
                .map_err(|error| error.to_string())?,
            machine_id: MachineId::new("mch_synthetic_legacy_stack_compat")
                .map_err(|error| error.to_string())?,
            machine_incarnation_id: None,
            stack_id: stack_id.to_string(),
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Validate fields whose typed wrappers may have come from untrusted JSON.
    pub fn validate(&self) -> Result<(), String> {
        validate_text("reservation_id", &self.reservation_id)?;
        ProjectId::new(self.project_id.as_str()).map_err(|error| error.to_string())?;
        EnvironmentId::new(self.environment_id.as_str()).map_err(|error| error.to_string())?;
        MachineId::new(self.machine_id.as_str()).map_err(|error| error.to_string())?;
        if let Some(incarnation_id) = &self.machine_incarnation_id {
            MachineIncarnationId::new(incarnation_id.as_str())
                .map_err(|error| error.to_string())?;
        }
        validate_stack_id(&self.stack_id)
    }
}

/// Durable state observed for one exact topology-scoped create reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerGenerationInspection {
    /// No active generation exists for the requested container ID.
    Absent,
    /// The exact reservation owns a generation but has not published metadata.
    ReservedUnpublished(ContainerGenerationOwnership),
    /// The exact reservation owns a published generation.
    Published(ContainerGenerationOwnership),
    /// The container ID is owned by another scoped reservation.
    Foreign,
    /// A caller-supplied generation was replaced by a different generation.
    Replacement,
    /// The active generation predates scoped ownership and cannot be adopted.
    LegacyUnscoped,
    /// Durable generation metadata is malformed and cannot be trusted.
    Malformed(String),
}

/// Result of abandoning an exact unpublished reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerGenerationReleaseOutcome {
    Released,
    AlreadyAbsent,
}

/// Runtime-issued proof that one stack reserved a specific container-ID generation.
///
/// The tuple is intentionally generation-qualified: callers must never use the
/// container ID alone to clean up a failed create because a later lifecycle may
/// have reused the same ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationOwnership {
    /// Caller-selected runtime container identifier.
    pub container_id: String,
    /// Monotonic durable generation reserved for this create transaction.
    pub generation: u64,
    /// Stack/sandbox scope that reserved the generation.
    pub stack_id: String,
    /// Exact topology reservation that admitted this generation.
    ///
    /// Missing scope is accepted only while decoding legacy records. It never
    /// authorizes cleanup or adoption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Box<ContainerGenerationScope>>,
}

impl ContainerGenerationOwnership {
    /// Validate that the ownership envelope names the exact durable scope.
    pub fn validate(&self) -> Result<(), String> {
        validate_text("container_id", &self.container_id)?;
        if self.generation == 0 {
            return Err(
                "container generation ownership must name a non-zero generation".to_string(),
            );
        }
        let scope = self.scope.as_ref().ok_or_else(|| {
            "container generation ownership is legacy-unscoped and quarantined".to_string()
        })?;
        scope.validate()?;
        if self.stack_id != scope.stack_id {
            return Err(
                "container generation ownership stack_id disagrees with durable scope".to_string(),
            );
        }
        Ok(())
    }
}

/// Stable identity of one guest kernel object used to prove a running generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationKernelObjectIdentity {
    /// Device number reported by `stat(2)`.
    pub device: u64,
    /// Non-zero inode number reported by `stat(2)`.
    pub inode: u64,
}

impl ContainerGenerationKernelObjectIdentity {
    /// Reject an identity that cannot name a concrete kernel object.
    pub fn validate(&self, field: &str) -> Result<(), String> {
        if self.inode == 0 {
            return Err(format!(
                "container generation {field} identity must name a non-zero inode"
            ));
        }
        Ok(())
    }
}

/// Immutable namespace identities captured for one running container generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationNamespaceIdentity {
    pub mount: ContainerGenerationKernelObjectIdentity,
    pub network: ContainerGenerationKernelObjectIdentity,
    pub pid: ContainerGenerationKernelObjectIdentity,
    pub ipc: ContainerGenerationKernelObjectIdentity,
    pub uts: ContainerGenerationKernelObjectIdentity,
}

impl ContainerGenerationNamespaceIdentity {
    /// Validate every namespace identity required for exact exec admission.
    pub fn validate(&self) -> Result<(), String> {
        self.mount.validate("mount namespace")?;
        self.network.validate("network namespace")?;
        self.pid.validate("PID namespace")?;
        self.ipc.validate("IPC namespace")?;
        self.uts.validate("UTS namespace")
    }
}

/// Exact authority expected by a caller performing a lifecycle observation.
///
/// The session and supervisor identities are part of the authority boundary, and the
/// caller-issued freshness nonce fences one specific inspection request. Matching topology
/// ownership alone must never let an older observation certify the current generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationLifecycleContext {
    pub ownership: ContainerGenerationOwnership,
    pub host_runtime_session_id: String,
    pub guest_supervisor_id: String,
    /// Caller-issued nonce unique to this lifecycle observation request.
    pub freshness_nonce: String,
}

impl ContainerGenerationLifecycleContext {
    /// Validate the expected authority before comparing an observation with it.
    pub fn validate(&self) -> Result<(), String> {
        self.ownership.validate()?;
        require_machine_incarnation(&self.ownership)?;
        validate_text("host_runtime_session_id", &self.host_runtime_session_id)?;
        validate_text("guest_supervisor_id", &self.guest_supervisor_id)?;
        validate_text("freshness_nonce", &self.freshness_nonce)
    }
}

/// Guest- and host-qualified proof that one exact container generation is running.
///
/// Durable publication alone is intentionally insufficient. A valid proof joins the
/// topology ownership envelope to the current host runtime session, the guest lifecycle
/// supervisor, and immutable kernel identities captured for the container init.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationRunningProof {
    pub schema_version: u32,
    pub ownership: ContainerGenerationOwnership,
    pub host_runtime_session_id: String,
    pub guest_supervisor_id: String,
    pub guest_observation_sequence: u64,
    pub init_pid: u32,
    pub init_start_time: u64,
    pub cgroup_path: String,
    pub cgroup: ContainerGenerationKernelObjectIdentity,
    pub namespaces: ContainerGenerationNamespaceIdentity,
    pub root: ContainerGenerationKernelObjectIdentity,
    pub observed_unix_secs: u64,
}

impl ContainerGenerationRunningProof {
    /// Validate every authority-bearing component of the running proof.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CONTAINER_GENERATION_LIFECYCLE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported container generation lifecycle schema version {}",
                self.schema_version
            ));
        }
        self.ownership.validate()?;
        require_machine_incarnation(&self.ownership)?;
        validate_text("host_runtime_session_id", &self.host_runtime_session_id)?;
        validate_text("guest_supervisor_id", &self.guest_supervisor_id)?;
        validate_observation_sequence(self.guest_observation_sequence)?;
        if self.init_pid == 0 {
            return Err(
                "container generation running proof requires a non-zero init PID".to_string(),
            );
        }
        if self.init_start_time == 0 {
            return Err(
                "container generation running proof requires a non-zero init start time"
                    .to_string(),
            );
        }
        validate_absolute_guest_path("cgroup_path", &self.cgroup_path)?;
        self.cgroup.validate("cgroup")?;
        self.namespaces.validate()?;
        self.root.validate("root")?;
        if self.observed_unix_secs == 0 {
            return Err(
                "container generation running proof requires a non-zero observation timestamp"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Validate stable authority dimensions; freshness is checked by the observation envelope.
    fn validate_authority_for(
        &self,
        expected: &ContainerGenerationLifecycleContext,
    ) -> Result<(), String> {
        self.validate()?;
        expected.validate()?;
        validate_expected_ownership(&self.ownership, expected)?;
        if self.host_runtime_session_id != expected.host_runtime_session_id {
            return Err(
                "container generation running proof belongs to a different host runtime session"
                    .to_string(),
            );
        }
        if self.guest_supervisor_id != expected.guest_supervisor_id {
            return Err(
                "container generation running proof belongs to a different guest supervisor"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Compare the immutable kernel process identity while ignoring observation sequence/time.
    pub fn proves_same_live_process(&self, other: &Self) -> Result<bool, String> {
        self.validate()?;
        other.validate()?;
        Ok(self.schema_version == other.schema_version
            && self.ownership == other.ownership
            && self.host_runtime_session_id == other.host_runtime_session_id
            && self.guest_supervisor_id == other.guest_supervisor_id
            && self.init_pid == other.init_pid
            && self.init_start_time == other.init_start_time
            && self.cgroup_path == other.cgroup_path
            && self.cgroup == other.cgroup
            && self.namespaces == other.namespaces
            && self.root == other.root)
    }
}

/// Exact Linux kernel wait disposition of a reaped generation-bound process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerGenerationExitDisposition {
    /// The process called `_exit` or returned normally with this status.
    Exited { code: i32 },
    /// The process was terminated by a signal.
    Signaled { signal: u32, core_dumped: bool },
}

impl ContainerGenerationExitDisposition {
    /// Return the shell-compatible status while rejecting impossible wait results.
    pub fn normalized_exit_code(self) -> Result<i32, String> {
        match self {
            Self::Exited { code } if (0..=255).contains(&code) => Ok(code),
            Self::Exited { code } => Err(format!(
                "container generation exit code {code} is outside 0..=255"
            )),
            Self::Signaled { signal, .. } if (1..=64).contains(&signal) => Ok(128 + signal as i32),
            Self::Signaled { signal, .. } => Err(format!(
                "Linux container generation exit signal {signal} is outside 1..=64"
            )),
        }
    }
}

/// Kernel observation mechanism that produced an exact exit receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerGenerationExitReceiptProvenance {
    /// A generation-owning guest supervisor reaped the init with `waitid`/`waitpid`.
    GuestSupervisorWait,
}

/// Durable, generation-qualified receipt for one exactly reaped container init.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationExitReceipt {
    pub schema_version: u32,
    pub running_proof: ContainerGenerationRunningProof,
    pub receipt_sequence: u64,
    pub disposition: ContainerGenerationExitDisposition,
    /// Persisted shell-compatible projection of `disposition`.
    pub normalized_exit_code: i32,
    pub exited_unix_secs: u64,
    pub provenance: ContainerGenerationExitReceiptProvenance,
    /// Canonical content identifier: SHA-256 of every other immutable receipt field.
    ///
    /// This detects accidental mutation and provides stable correlation. It does not
    /// authenticate the issuer; transport and durable-store authentication do that.
    pub content_digest: String,
}

impl ContainerGenerationExitReceipt {
    /// Validate identity, ordering, and the exact-to-normalized exit mapping.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CONTAINER_GENERATION_LIFECYCLE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported container generation lifecycle schema version {}",
                self.schema_version
            ));
        }
        self.running_proof.validate()?;
        if self.receipt_sequence <= self.running_proof.guest_observation_sequence {
            return Err(
                "container generation exit receipt sequence must follow its running observation"
                    .to_string(),
            );
        }
        if self.receipt_sequence == u64::MAX {
            return Err("container generation exit receipt sequence is exhausted".to_string());
        }
        let expected = self.disposition.normalized_exit_code()?;
        if self.normalized_exit_code != expected {
            return Err(format!(
                "container generation normalized exit code {} disagrees with exact disposition {expected}",
                self.normalized_exit_code
            ));
        }
        if self.exited_unix_secs < self.running_proof.observed_unix_secs {
            return Err(
                "container generation exit timestamp precedes its running observation".to_string(),
            );
        }
        let expected_digest = self.computed_content_digest()?;
        if self.content_digest != expected_digest {
            return Err(
                "container generation exit receipt digest does not match its immutable fields"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Validate stable authority dimensions; freshness is checked by the observation envelope.
    fn validate_authority_for(
        &self,
        expected: &ContainerGenerationLifecycleContext,
    ) -> Result<(), String> {
        self.validate()?;
        self.running_proof.validate_authority_for(expected)
    }

    /// Compute the canonical digest over immutable receipt material.
    pub fn computed_content_digest(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct DigestMaterial<'a> {
            schema_version: u32,
            running_proof: &'a ContainerGenerationRunningProof,
            receipt_sequence: u64,
            disposition: ContainerGenerationExitDisposition,
            normalized_exit_code: i32,
            exited_unix_secs: u64,
            provenance: ContainerGenerationExitReceiptProvenance,
        }

        let bytes = serde_json::to_vec(&DigestMaterial {
            schema_version: self.schema_version,
            running_proof: &self.running_proof,
            receipt_sequence: self.receipt_sequence,
            disposition: self.disposition,
            normalized_exit_code: self.normalized_exit_code,
            exited_unix_secs: self.exited_unix_secs,
            provenance: self.provenance,
        })
        .map_err(|error| format!("cannot serialize container exit receipt digest: {error}"))?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    /// Stable identifier derived from the immutable receipt contents.
    pub fn receipt_id(&self) -> &str {
        &self.content_digest
    }

    /// Exact ownership to which this receipt is permanently bound.
    pub fn ownership(&self) -> &ContainerGenerationOwnership {
        &self.running_proof.ownership
    }
}

/// Authoritative reason why a published generation cannot still be live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerGenerationInactiveReason {
    MachineStopped,
    GuestConfirmedAbsent,
    NeverActivated,
}

/// Typed reason why exact lifecycle state cannot currently be established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerGenerationUnavailableReason {
    /// The prior host runtime ended, but the guest workload requires revalidation.
    RuntimeSessionEnded,
    RuntimeSessionOwnedElsewhere,
    MachineUnavailable,
    GuestAgentUnavailable,
    LifecycleSupervisorUnavailable,
    ObservationTimedOut,
    ExitStatusUnknown,
}

/// Lifecycle state for one requested, topology-scoped container generation.
///
/// `Inactive` and `Unavailable` are intentionally distinct from `Exited`: neither
/// carries an exit receipt and neither can satisfy a successful-completion predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerGenerationLifecycleInspection {
    Absent,
    ReservedUnpublished(ContainerGenerationOwnership),
    Created(ContainerGenerationOwnership),
    Running(ContainerGenerationRunningProof),
    Exited(ContainerGenerationExitReceipt),
    Inactive {
        ownership: ContainerGenerationOwnership,
        reason: ContainerGenerationInactiveReason,
    },
    Unavailable {
        ownership: ContainerGenerationOwnership,
        reason: ContainerGenerationUnavailableReason,
    },
    Foreign {
        current: ContainerGenerationOwnership,
    },
    Replacement {
        current: ContainerGenerationOwnership,
    },
    LegacyUnscoped,
    Malformed(String),
}

impl ContainerGenerationLifecycleInspection {
    /// Validate nested authority and reject unusable diagnostics.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::ReservedUnpublished(ownership)
            | Self::Created(ownership)
            | Self::Inactive { ownership, .. }
            | Self::Unavailable { ownership, .. } => ownership.validate(),
            Self::Running(proof) => proof.validate(),
            Self::Exited(receipt) => receipt.validate(),
            Self::Foreign { current } | Self::Replacement { current } => {
                current.validate()?;
                require_machine_incarnation(current)
            }
            Self::Malformed(reason) => validate_diagnostic("malformed lifecycle reason", reason),
            Self::Absent | Self::LegacyUnscoped => Ok(()),
        }
    }
}

/// Observer-qualified result for every lifecycle classification, including absence.
///
/// Unit classifications are meaningful only inside this envelope. Requested ownership,
/// runtime session, guest supervisor, caller freshness nonce, sequence, and timestamp prevent
/// a stale `Absent` or diagnostic from being reused as a current observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationLifecycleObservation {
    pub schema_version: u32,
    pub requested_ownership: ContainerGenerationOwnership,
    pub host_runtime_session_id: String,
    pub guest_supervisor_id: String,
    /// Echo of the caller-issued request nonce; it is not a random issuer credential.
    pub freshness_nonce: String,
    pub guest_observation_sequence: u64,
    pub observed_unix_secs: u64,
    pub inspection: ContainerGenerationLifecycleInspection,
}

impl ContainerGenerationLifecycleObservation {
    /// Validate the envelope and its classification evidence without granting current authority.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CONTAINER_GENERATION_LIFECYCLE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported container generation lifecycle schema version {}",
                self.schema_version
            ));
        }
        self.requested_ownership.validate()?;
        require_machine_incarnation(&self.requested_ownership)?;
        validate_text("host_runtime_session_id", &self.host_runtime_session_id)?;
        validate_text("guest_supervisor_id", &self.guest_supervisor_id)?;
        validate_text("freshness_nonce", &self.freshness_nonce)?;
        validate_observation_sequence(self.guest_observation_sequence)?;
        if self.observed_unix_secs == 0 {
            return Err(
                "container generation lifecycle observation requires a non-zero timestamp"
                    .to_string(),
            );
        }
        self.inspection.validate()?;

        match &self.inspection {
            ContainerGenerationLifecycleInspection::ReservedUnpublished(ownership)
            | ContainerGenerationLifecycleInspection::Created(ownership)
            | ContainerGenerationLifecycleInspection::Inactive { ownership, .. }
            | ContainerGenerationLifecycleInspection::Unavailable { ownership, .. } => {
                validate_requested_ownership(ownership, &self.requested_ownership)
            }
            ContainerGenerationLifecycleInspection::Running(proof) => {
                validate_requested_ownership(&proof.ownership, &self.requested_ownership)?;
                validate_observer_identity(
                    &proof.host_runtime_session_id,
                    &proof.guest_supervisor_id,
                    self,
                )?;
                if proof.guest_observation_sequence != self.guest_observation_sequence
                    || proof.observed_unix_secs != self.observed_unix_secs
                {
                    return Err(
                        "running proof sequence/timestamp disagrees with its observation envelope"
                            .to_string(),
                    );
                }
                Ok(())
            }
            ContainerGenerationLifecycleInspection::Exited(receipt) => {
                validate_requested_ownership(receipt.ownership(), &self.requested_ownership)?;
                validate_observer_identity(
                    &receipt.running_proof.host_runtime_session_id,
                    &receipt.running_proof.guest_supervisor_id,
                    self,
                )?;
                if self.guest_observation_sequence < receipt.receipt_sequence
                    || self.observed_unix_secs < receipt.exited_unix_secs
                {
                    return Err(
                        "exit receipt is newer than its lifecycle observation envelope".to_string(),
                    );
                }
                Ok(())
            }
            ContainerGenerationLifecycleInspection::Foreign { current } => {
                require_machine_incarnation(current)?;
                validate_current_container_id_for_request(current, &self.requested_ownership)?;
                if current.scope == self.requested_ownership.scope {
                    return Err(
                        "foreign lifecycle classification carries the requested ownership scope"
                            .to_string(),
                    );
                }
                Ok(())
            }
            ContainerGenerationLifecycleInspection::Replacement { current } => {
                require_machine_incarnation(current)?;
                validate_current_container_id_for_request(current, &self.requested_ownership)?;
                if current.generation <= self.requested_ownership.generation {
                    return Err(
                        "replacement lifecycle classification must carry a newer generation"
                            .to_string(),
                    );
                }
                Ok(())
            }
            ContainerGenerationLifecycleInspection::Absent
            | ContainerGenerationLifecycleInspection::LegacyUnscoped
            | ContainerGenerationLifecycleInspection::Malformed(_) => Ok(()),
        }
    }

    /// Validate that the whole observation was made for the caller's current authority.
    pub fn validate_for(
        &self,
        expected: &ContainerGenerationLifecycleContext,
    ) -> Result<(), String> {
        self.validate()?;
        expected.validate()?;
        validate_requested_ownership(&self.requested_ownership, &expected.ownership)?;
        if self.host_runtime_session_id != expected.host_runtime_session_id {
            return Err(
                "container generation lifecycle observation belongs to a different host runtime session"
                    .to_string(),
            );
        }
        if self.guest_supervisor_id != expected.guest_supervisor_id {
            return Err(
                "container generation lifecycle observation belongs to a different guest supervisor"
                    .to_string(),
            );
        }
        if self.freshness_nonce != expected.freshness_nonce {
            return Err(
                "container generation lifecycle observation carries a different freshness nonce"
                    .to_string(),
            );
        }
        match &self.inspection {
            ContainerGenerationLifecycleInspection::Running(proof) => {
                proof.validate_authority_for(expected)
            }
            ContainerGenerationLifecycleInspection::Exited(receipt) => {
                receipt.validate_authority_for(expected)
            }
            _ => Ok(()),
        }
    }

    /// Decide successful completion only from an envelope matching current authority.
    pub fn is_successful_exit_for(
        &self,
        expected: &ContainerGenerationLifecycleContext,
    ) -> Result<bool, String> {
        self.validate_for(expected)?;
        Ok(matches!(
            self.inspection,
            ContainerGenerationLifecycleInspection::Exited(ContainerGenerationExitReceipt {
                disposition: ContainerGenerationExitDisposition::Exited { code: 0 },
                ..
            })
        ))
    }
}

/// Bounded snapshot request for one exact container generation lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationLifecycleInspectRequest {
    pub schema_version: u32,
    pub context: ContainerGenerationLifecycleContext,
    pub timeout_millis: u64,
}

impl ContainerGenerationLifecycleInspectRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        self.context.validate()?;
        if !(1..=MAX_CONTAINER_GENERATION_LIFECYCLE_INSPECT_TIMEOUT_MILLIS)
            .contains(&self.timeout_millis)
        {
            return Err(format!(
                "generation lifecycle inspect timeout_millis must be within 1..={MAX_CONTAINER_GENERATION_LIFECYCLE_INSPECT_TIMEOUT_MILLIS}"
            ));
        }
        Ok(())
    }

    pub fn validate_observation(
        &self,
        observation: &ContainerGenerationLifecycleObservation,
    ) -> Result<(), String> {
        self.validate()?;
        observation.validate_for(&self.context)
    }
}

/// Stream subscription for lifecycle changes to one exact container generation.
///
/// A backend emits an initial current observation followed by strictly increasing guest
/// observation sequences until cancellation or transport failure. `after_guest_observation_sequence`
/// resumes strictly after a previously accepted event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationLifecycleWatchRequest {
    pub schema_version: u32,
    pub context: ContainerGenerationLifecycleContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_guest_observation_sequence: Option<u64>,
}

impl ContainerGenerationLifecycleWatchRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        self.context.validate()?;
        if let Some(sequence) = self.after_guest_observation_sequence {
            validate_observation_sequence(sequence)?;
        }
        Ok(())
    }

    pub fn validate_next(
        &self,
        event: &ContainerGenerationLifecycleWatchEvent,
        previous: Option<&ContainerGenerationLifecycleWatchEvent>,
    ) -> Result<(), String> {
        self.validate()?;
        event.validate_for(self)?;
        if let Some(previous) = previous {
            previous.validate_for(self)?;
            if event.observation.observed_unix_secs < previous.observation.observed_unix_secs {
                return Err(
                    "container generation lifecycle watch timestamp moved backwards".to_string(),
                );
            }
        }
        let lower_bound = previous
            .map(|event| event.observation.guest_observation_sequence)
            .or(self.after_guest_observation_sequence);
        if lower_bound
            .is_some_and(|sequence| event.observation.guest_observation_sequence <= sequence)
        {
            return Err(
                "container generation lifecycle watch sequence did not advance".to_string(),
            );
        }
        Ok(())
    }
}

/// One event from a generation-qualified lifecycle watch stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationLifecycleWatchEvent {
    pub schema_version: u32,
    pub observation: ContainerGenerationLifecycleObservation,
}

impl ContainerGenerationLifecycleWatchEvent {
    pub fn validate_for(
        &self,
        request: &ContainerGenerationLifecycleWatchRequest,
    ) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        request.validate()?;
        self.observation.validate_for(&request.context)
    }
}

/// Exact, freshness-fenced command execution request for a proven running generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationExecRequest {
    pub schema_version: u32,
    pub context: ContainerGenerationLifecycleContext,
    pub running_observation: ContainerGenerationLifecycleObservation,
    pub execution_id: String,
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    pub timeout_millis: u64,
    pub max_output_bytes: u64,
}

impl ContainerGenerationExecRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        self.context.validate()?;
        self.running_observation.validate_for(&self.context)?;
        if !matches!(
            self.running_observation.inspection,
            ContainerGenerationLifecycleInspection::Running(_)
        ) {
            return Err(
                "generation-qualified exec requires a current running lifecycle proof".to_string(),
            );
        }
        validate_text("execution_id", &self.execution_id)?;
        validate_exec_command(&self.command)?;
        if let Some(directory) = &self.working_directory {
            validate_absolute_guest_path("working_directory", directory)?;
        }
        for (key, value) in &self.environment {
            validate_exec_environment(key, value)?;
        }
        if let Some(user) = &self.user {
            validate_exec_field("user", user, false)?;
        }
        if !(1..=MAX_CONTAINER_GENERATION_EXEC_TIMEOUT_MILLIS).contains(&self.timeout_millis) {
            return Err(format!(
                "generation-qualified exec timeout_millis must be within 1..={MAX_CONTAINER_GENERATION_EXEC_TIMEOUT_MILLIS}"
            ));
        }
        if !(1..=MAX_CONTAINER_GENERATION_EXEC_OUTPUT_BYTES).contains(&self.max_output_bytes) {
            return Err(format!(
                "generation-qualified exec max_output_bytes must be within 1..={MAX_CONTAINER_GENERATION_EXEC_OUTPUT_BYTES}"
            ));
        }
        Ok(())
    }

    pub fn running_proof(&self) -> Result<&ContainerGenerationRunningProof, String> {
        match &self.running_observation.inspection {
            ContainerGenerationLifecycleInspection::Running(proof) => Ok(proof),
            _ => Err(
                "generation-qualified exec requires a current running lifecycle proof".to_string(),
            ),
        }
    }

    /// Domain-separated canonical digest binding every serialized request field.
    ///
    /// This is a stable correlation/fencing identifier, not issuer authentication.
    pub fn computed_request_digest(&self) -> Result<String, String> {
        self.validate()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| format!("cannot serialize generation exec request: {error}"))?;
        let mut digest = Sha256::new();
        digest.update(b"vz.container-generation-exec-request.v1\0");
        digest.update(encoded);
        Ok(format!("sha256:{:x}", digest.finalize()))
    }
}

/// Why the exact exec process reached its reaped terminal wait disposition.
///
/// `TimedOut` and `Canceled` remain distinct even when the final wait result is exit code zero;
/// neither is successful command completion. A backend may emit any variant only after the
/// generation-fenced exec process has been reaped, excluding late results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerGenerationExecCompletion {
    Completed {
        disposition: ContainerGenerationExitDisposition,
    },
    TimedOut {
        final_disposition: ContainerGenerationExitDisposition,
    },
    Canceled {
        final_disposition: ContainerGenerationExitDisposition,
    },
}

impl ContainerGenerationExecCompletion {
    fn disposition(self) -> ContainerGenerationExitDisposition {
        match self {
            Self::Completed { disposition } => disposition,
            Self::TimedOut { final_disposition } | Self::Canceled { final_disposition } => {
                final_disposition
            }
        }
    }

    fn is_success(self) -> bool {
        matches!(
            self,
            Self::Completed {
                disposition: ContainerGenerationExitDisposition::Exited { code: 0 }
            }
        )
    }
}

/// Bounded captured output and exact wait result for one generation-qualified exec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationExecResult {
    pub schema_version: u32,
    pub context: ContainerGenerationLifecycleContext,
    /// Fresh Running observation obtained atomically at backend exec admission.
    pub admission_observation: ContainerGenerationLifecycleObservation,
    pub execution_id: String,
    /// Canonical digest of the entire originating exec request.
    pub request_digest: String,
    /// Guest-supervisor sequence assigned after the exact exec process was reaped.
    pub completion_sequence: u64,
    pub started_unix_millis: u64,
    pub finished_unix_millis: u64,
    pub completion: ContainerGenerationExecCompletion,
    pub normalized_exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// Domain-separated digest of every other immutable terminal result field.
    pub terminal_receipt_digest: String,
}

impl ContainerGenerationExecResult {
    pub fn validate(&self) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        self.context.validate()?;
        self.admission_observation.validate_for(&self.context)?;
        if !matches!(
            self.admission_observation.inspection,
            ContainerGenerationLifecycleInspection::Running(_)
        ) {
            return Err(
                "generation-qualified exec admission requires a current running lifecycle proof"
                    .to_string(),
            );
        }
        validate_text("execution_id", &self.execution_id)?;
        validate_sha256_digest("request_digest", &self.request_digest)?;
        validate_observation_sequence(self.completion_sequence)?;
        if self.completion_sequence <= self.admission_observation.guest_observation_sequence {
            return Err(
                "generation-qualified exec completion sequence must follow admission".to_string(),
            );
        }
        if self.started_unix_millis == 0 {
            return Err(
                "generation-qualified exec result requires a non-zero start timestamp".to_string(),
            );
        }
        if self.finished_unix_millis < self.started_unix_millis {
            return Err(
                "generation-qualified exec finish timestamp precedes its start".to_string(),
            );
        }
        if self.started_unix_millis / 1_000 < self.admission_observation.observed_unix_secs {
            return Err("generation-qualified exec start timestamp precedes admission".to_string());
        }
        let expected_code = self.completion.disposition().normalized_exit_code()?;
        if self.normalized_exit_code != expected_code {
            return Err(
                "generation-qualified exec normalized exit code disagrees with exact disposition"
                    .to_string(),
            );
        }
        validate_captured_output_size(
            &self.stdout,
            &self.stderr,
            MAX_CONTAINER_GENERATION_EXEC_OUTPUT_BYTES,
        )?;
        let expected_digest = self.computed_terminal_receipt_digest()?;
        if self.terminal_receipt_digest != expected_digest {
            return Err(
                "generation-qualified exec terminal receipt digest does not match its immutable fields"
                    .to_string(),
            );
        }
        Ok(())
    }

    pub fn validate_for(&self, request: &ContainerGenerationExecRequest) -> Result<(), String> {
        self.validate()?;
        request.validate()?;
        if self.context != request.context
            || self.execution_id != request.execution_id
            || self.request_digest != request.computed_request_digest()?
        {
            return Err(
                "generation-qualified exec result does not belong to the exact request".to_string(),
            );
        }
        let requested_proof = request.running_proof()?;
        let admitted_proof = match &self.admission_observation.inspection {
            ContainerGenerationLifecycleInspection::Running(proof) => proof,
            _ => unreachable!("result validation already required Running admission"),
        };
        if !admitted_proof.proves_same_live_process(requested_proof)?
            || self.admission_observation.guest_observation_sequence
                <= request.running_observation.guest_observation_sequence
            || self.admission_observation.observed_unix_secs
                < request.running_observation.observed_unix_secs
        {
            return Err(
                "generation-qualified exec admission did not freshly revalidate the exact generation"
                    .to_string(),
            );
        }
        validate_captured_output_size(&self.stdout, &self.stderr, request.max_output_bytes)
    }

    pub fn is_successful_exit_for(
        &self,
        request: &ContainerGenerationExecRequest,
    ) -> Result<bool, String> {
        self.validate_for(request)?;
        Ok(self.completion.is_success())
    }

    /// Canonical terminal receipt digest; correlation/fencing only, not issuer authentication.
    pub fn computed_terminal_receipt_digest(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct DigestMaterial<'a> {
            schema_version: u32,
            context: &'a ContainerGenerationLifecycleContext,
            admission_observation: &'a ContainerGenerationLifecycleObservation,
            execution_id: &'a str,
            request_digest: &'a str,
            completion_sequence: u64,
            started_unix_millis: u64,
            finished_unix_millis: u64,
            completion: ContainerGenerationExecCompletion,
            normalized_exit_code: i32,
            stdout: &'a [u8],
            stderr: &'a [u8],
            stdout_truncated: bool,
            stderr_truncated: bool,
        }

        let material = DigestMaterial {
            schema_version: self.schema_version,
            context: &self.context,
            admission_observation: &self.admission_observation,
            execution_id: &self.execution_id,
            request_digest: &self.request_digest,
            completion_sequence: self.completion_sequence,
            started_unix_millis: self.started_unix_millis,
            finished_unix_millis: self.finished_unix_millis,
            completion: self.completion,
            normalized_exit_code: self.normalized_exit_code,
            stdout: &self.stdout,
            stderr: &self.stderr,
            stdout_truncated: self.stdout_truncated,
            stderr_truncated: self.stderr_truncated,
        };
        let encoded = serde_json::to_vec(&material)
            .map_err(|error| format!("cannot serialize exec terminal receipt: {error}"))?;
        let mut digest = Sha256::new();
        digest.update(b"vz.container-generation-exec-terminal-receipt.v1\0");
        digest.update(encoded);
        Ok(format!("sha256:{:x}", digest.finalize()))
    }

    pub fn terminal_receipt_id(&self) -> &str {
        &self.terminal_receipt_digest
    }
}

/// Exact cancellation request for one generation-qualified exec payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationExecCancelRequest {
    pub schema_version: u32,
    pub context: ContainerGenerationLifecycleContext,
    pub execution_id: String,
    pub exec_request_digest: String,
    pub cancellation_nonce: String,
}

impl ContainerGenerationExecCancelRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        self.context.validate()?;
        validate_text("execution_id", &self.execution_id)?;
        validate_sha256_digest("exec_request_digest", &self.exec_request_digest)?;
        validate_text("cancellation_nonce", &self.cancellation_nonce)
    }

    pub fn validate_for(&self, request: &ContainerGenerationExecRequest) -> Result<(), String> {
        self.validate()?;
        request.validate()?;
        if self.context != request.context
            || self.execution_id != request.execution_id
            || self.exec_request_digest != request.computed_request_digest()?
        {
            return Err(
                "generation-qualified exec cancellation does not identify the exact request"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Whether cancellation caused terminal reaping or observed an existing terminal result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerGenerationExecCancelDisposition {
    CanceledAndReaped,
    AlreadyTerminal,
}

/// Exact cancellation outcome. No successful result may be published after this is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerGenerationExecCancelOutcome {
    pub schema_version: u32,
    pub cancellation: ContainerGenerationExecCancelRequest,
    pub disposition: ContainerGenerationExecCancelDisposition,
    pub terminal_result: ContainerGenerationExecResult,
}

impl ContainerGenerationExecCancelOutcome {
    pub fn validate(&self) -> Result<(), String> {
        validate_lifecycle_schema_version(self.schema_version)?;
        self.cancellation.validate()?;
        self.terminal_result.validate()?;
        if self.terminal_result.context != self.cancellation.context
            || self.terminal_result.execution_id != self.cancellation.execution_id
            || self.terminal_result.request_digest != self.cancellation.exec_request_digest
        {
            return Err(
                "generation-qualified exec cancellation outcome belongs to a different exec"
                    .to_string(),
            );
        }
        if self.disposition == ContainerGenerationExecCancelDisposition::CanceledAndReaped
            && !matches!(
                self.terminal_result.completion,
                ContainerGenerationExecCompletion::Canceled { .. }
            )
        {
            return Err(
                "canceled-and-reaped outcome requires a typed Canceled terminal result".to_string(),
            );
        }
        Ok(())
    }

    pub fn validate_for(&self, request: &ContainerGenerationExecRequest) -> Result<(), String> {
        self.validate()?;
        self.cancellation.validate_for(request)?;
        self.terminal_result.validate_for(request)
    }

    /// Enforce terminal single-assignment after this cancellation fence is established.
    ///
    /// Exact byte-for-byte replays of the established terminal receipt are accepted. Any other
    /// independently valid terminal result for the same request is a conflicting late result.
    pub fn validate_terminal_replay(
        &self,
        candidate: &ContainerGenerationExecResult,
        request: &ContainerGenerationExecRequest,
    ) -> Result<(), String> {
        self.validate_for(request)?;
        candidate.validate_for(request)?;
        if candidate.terminal_receipt_digest != self.terminal_result.terminal_receipt_digest
            || candidate != &self.terminal_result
        {
            return Err(
                "conflicting late terminal result after generation-qualified exec cancellation"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Durable identity of the single assigned terminal result.
    pub fn terminal_receipt_id(&self) -> &str {
        self.terminal_result.terminal_receipt_id()
    }
}

fn validate_lifecycle_schema_version(schema_version: u32) -> Result<(), String> {
    if schema_version != CONTAINER_GENERATION_LIFECYCLE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported container generation lifecycle schema version {schema_version}"
        ));
    }
    Ok(())
}

fn validate_sha256_digest(field: &str, digest: &str) -> Result<(), String> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(format!("{field} must be a canonical sha256 digest"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{field} must be a canonical lowercase sha256 digest"
        ));
    }
    Ok(())
}

fn validate_exec_command(command: &[String]) -> Result<(), String> {
    if command.is_empty() || command.len() > 256 {
        return Err("generation-qualified exec command must contain 1..=256 arguments".to_string());
    }
    for (index, argument) in command.iter().enumerate() {
        validate_exec_field("command argument", argument, index != 0)?;
    }
    Ok(())
}

fn validate_exec_environment(key: &str, value: &str) -> Result<(), String> {
    if key.is_empty()
        || key.len() > 256
        || key.contains('=')
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(
            "generation-qualified exec environment keys must contain 1..=256 ASCII alphanumeric or '_' bytes"
                .to_string(),
        );
    }
    validate_exec_field("environment value", value, true)
}

fn validate_exec_field(field: &str, value: &str, allow_empty: bool) -> Result<(), String> {
    if (!allow_empty && value.is_empty()) || value.len() > 128 * 1024 || value.contains('\0') {
        return Err(format!(
            "generation-qualified exec {field} must contain at most 131072 bytes and no NUL"
        ));
    }
    Ok(())
}

fn validate_captured_output_size(stdout: &[u8], stderr: &[u8], limit: u64) -> Result<(), String> {
    let total = stdout
        .len()
        .checked_add(stderr.len())
        .and_then(|size| u64::try_from(size).ok())
        .ok_or_else(|| "generation-qualified exec output size overflowed".to_string())?;
    if total > limit {
        return Err(format!(
            "generation-qualified exec captured output {total} exceeds limit {limit}"
        ));
    }
    Ok(())
}

fn require_machine_incarnation(ownership: &ContainerGenerationOwnership) -> Result<(), String> {
    if ownership
        .scope
        .as_ref()
        .and_then(|scope| scope.machine_incarnation_id.as_ref())
        .is_none()
    {
        return Err(
            "container generation lifecycle authority requires a Machine incarnation".to_string(),
        );
    }
    Ok(())
}

fn validate_expected_ownership(
    actual: &ContainerGenerationOwnership,
    expected: &ContainerGenerationLifecycleContext,
) -> Result<(), String> {
    if actual != &expected.ownership {
        return Err(
            "container generation lifecycle observation belongs to different ownership".to_string(),
        );
    }
    Ok(())
}

fn validate_requested_ownership(
    actual: &ContainerGenerationOwnership,
    requested: &ContainerGenerationOwnership,
) -> Result<(), String> {
    if actual != requested {
        return Err(
            "container generation lifecycle classification belongs to different requested ownership"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_current_container_id_for_request(
    current: &ContainerGenerationOwnership,
    requested: &ContainerGenerationOwnership,
) -> Result<(), String> {
    if current.container_id != requested.container_id {
        return Err(
            "container generation lifecycle classification names a different container ID"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_observer_identity(
    host_runtime_session_id: &str,
    guest_supervisor_id: &str,
    observation: &ContainerGenerationLifecycleObservation,
) -> Result<(), String> {
    if host_runtime_session_id != observation.host_runtime_session_id {
        return Err(
            "lifecycle evidence belongs to a different host runtime session than its observation envelope"
                .to_string(),
        );
    }
    if guest_supervisor_id != observation.guest_supervisor_id {
        return Err(
            "lifecycle evidence belongs to a different guest supervisor than its observation envelope"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_observation_sequence(sequence: u64) -> Result<(), String> {
    if sequence == 0 {
        return Err(
            "container generation lifecycle observation requires a non-zero guest observation sequence"
                .to_string(),
        );
    }
    if sequence == u64::MAX {
        return Err(
            "container generation lifecycle guest observation sequence is exhausted".to_string(),
        );
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.len() > 128
        || trimmed != value
        || value.chars().any(char::is_control)
    {
        return Err(format!(
            "{field} must contain 1..=128 bytes with no leading/trailing whitespace or control characters"
        ));
    }
    Ok(())
}

fn validate_absolute_guest_path(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 4096 || !value.starts_with('/') || value.contains('\0') {
        return Err(format!(
            "{field} must be an absolute guest path containing 1..=4096 bytes and no NUL"
        ));
    }
    let mut components = value.split('/');
    if components.next() != Some("") {
        return Err(format!("{field} must be an absolute guest path"));
    }
    let components = components.collect::<Vec<_>>();
    if components.is_empty()
        || components
            .iter()
            .any(|component| component.is_empty() || matches!(*component, "." | ".."))
    {
        return Err(format!(
            "{field} must be canonical and may not be root or contain empty, '.' or '..' components"
        ));
    }
    Ok(())
}

fn validate_diagnostic(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > 4096 || value.contains('\0') {
        return Err(format!(
            "{field} must contain 1..=4096 non-blank bytes and no NUL"
        ));
    }
    Ok(())
}

fn validate_stack_id(value: &str) -> Result<(), String> {
    validate_text("stack_id", value)?;
    if matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(
            "stack_id must be one safe path component using only ASCII letters, digits, '-', '_', or '.'"
                .to_string(),
        );
    }
    Ok(())
}

/// Successful container creation result with optional generation ownership proof.
///
/// Backends that implement generation-owned cleanup return `Some`; compatibility
/// backends may return `None` and therefore cannot authorize failed-create cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerCreateReceipt {
    /// Runtime container identifier returned by the backend.
    pub container_id: String,
    /// Runtime-issued generation ownership, when supported by the backend.
    pub ownership: Option<ContainerGenerationOwnership>,
}

/// Container creation failure that may retain cleanup ownership.
///
/// `cleanup` is present only when the backend actually admitted the create and
/// reserved the reported generation. Admission failures such as a foreign
/// duplicate must return `None`.
#[derive(Debug)]
pub struct OwnedCreateError<E> {
    /// Underlying backend or adapter error.
    pub error: E,
    /// Exact failed generation the caller may attempt to clean up.
    pub cleanup: Option<ContainerGenerationOwnership>,
}

impl<E> OwnedCreateError<E> {
    /// Construct a failure that carries no cleanup authority.
    pub fn unowned(error: E) -> Self {
        Self {
            error,
            cleanup: None,
        }
    }

    /// Transform the underlying error while preserving cleanup ownership.
    pub fn map_error<T>(self, map: impl FnOnce(E) -> T) -> OwnedCreateError<T> {
        OwnedCreateError {
            error: map(self.error),
            cleanup: self.cleanup,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for OwnedCreateError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl<E: std::error::Error + 'static> std::error::Error for OwnedCreateError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Result of generation-qualified failed-create cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationCleanupOutcome {
    /// The exact owned generation and its artifacts were removed.
    Removed,
    /// The generation was already fully absent and no replacement was touched.
    AlreadyAbsent,
}

/// Cached image reference and manifest identifier pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageInfo {
    /// Human-readable image reference, for example `ubuntu:latest`.
    pub reference: String,
    /// Image identifier used by stored manifests/configs (digest form).
    pub image_id: String,
}

/// Summary of a local image prune pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneResult {
    /// Number of stale reference mappings that were removed.
    pub removed_refs: usize,
    /// Number of manifest JSON files removed.
    pub removed_manifests: usize,
    /// Number of config JSON files removed.
    pub removed_configs: usize,
    /// Number of unpacked layer directories removed.
    pub removed_layer_dirs: usize,
}

// ── Network types ─────────────────────────────────────────────────

/// Per-service network configuration for stack networking.
///
/// Each entry represents one service on one network. A service that belongs
/// to multiple custom networks will have multiple `NetworkServiceConfig`
/// entries (one per network), each with a different `network_name` and
/// subnet-specific `addr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkServiceConfig {
    /// Service name.
    pub name: String,
    /// IP address assigned to this service (CIDR, e.g., `"172.20.0.2/24"`).
    pub addr: String,
    /// Logical network this entry belongs to (e.g., `"default"`, `"frontend"`).
    pub network_name: String,
}

/// Aggregate resource hints for sizing a shared stack VM.
///
/// When multiple services define CPU/memory limits, the stack executor
/// computes an aggregate and passes it to the runtime backend so the
/// shared VM gets enough CPU cores and memory.
#[derive(Debug, Clone, Default)]
pub struct StackResourceHint {
    /// Suggested CPU cores for the VM (max of all service limits, ceiling).
    pub cpus: Option<u8>,
    /// Suggested memory in MB for the VM (sum of all service limits).
    pub memory_mb: Option<u64>,
    /// Host directories to share as VirtioFS mounts inside the VM.
    ///
    /// Each entry is `(tag, host_path, read_only)`. The tag is used as the
    /// VirtioFS mount tag and the init script mounts it at `/mnt/{tag}`.
    /// Named volumes and bind mounts from all services are collected here
    /// so the shared VM can set them up at boot time (VirtioFS shares are
    /// static and must be configured before the VM starts).
    pub volume_mounts: Vec<StackVolumeMount>,
    /// Optional path to a disk image to attach as a VirtioBlock device.
    ///
    /// Used for persistent named volumes: the image contains an ext4
    /// filesystem mounted at `/run/vz-oci/volumes` inside the guest VM.
    pub disk_image_path: Option<PathBuf>,
}

/// A host directory to expose inside the shared VM via VirtioFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackVolumeMount {
    /// VirtioFS mount tag (e.g., `"vz-mount-0"`).
    pub tag: String,
    /// Absolute path on the host.
    pub host_path: std::path::PathBuf,
    /// Target path inside the guest where this mount should appear.
    ///
    /// When set, the init script bind-mounts the VirtioFS share from
    /// `/mnt/{tag}` to this path inside the chroot. Communicated to the
    /// guest via kernel cmdline parameter `vz.mount.{N}={guest_path}`.
    pub guest_path: Option<String>,
    /// Whether the mount is read-only.
    pub read_only: bool,
}

/// Container log output.
#[derive(Debug, Clone, Default)]
pub struct ContainerLogs {
    /// Combined stdout/stderr output.
    pub output: String,
}
