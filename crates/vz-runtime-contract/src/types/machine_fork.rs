//! Forking one Machine inside its Environment.
//!
//! A fork is a new Machine seeded from an existing Machine's disk, in the *same*
//! Environment, with its own identity. It exists so that N parallel worktrees
//! can each hold a Machine that is already warm — dependencies installed,
//! services running, Docker image store populated — instead of paying N cold
//! boots. See `planning/developer-environments/11-worktree-parallelism.md`.
//!
//! ## Why the Environment is not the fork unit
//!
//! Forking the whole Environment was the obvious shape and is the wrong one.
//! Native macOS guests are licence-capped at two per host, so replicating an
//! Environment that contains one is impossible rather than merely expensive;
//! and much of an Environment — the seeded database, the shared caches — *should*
//! stay shared across parallel worktrees rather than be duplicated. Forking one
//! Machine lets some things fork while the rest stays shared.
//!
//! ## Identity, and why nothing needs allocating
//!
//! Everything a fork needs follows from a fresh `machine_id`:
//!
//! * **Address.** The fabric derives a Machine's IPv4 host offset from
//!   `[environment_id, network_id, attachment_id]` and its MAC from
//!   `[environment_id, machine_id, network_id]` (see
//!   `vz_runtimed::environment_switch::plan`). A fork mints a new attachment per
//!   network its parent is on, so it lands on a different address of the same
//!   subnet without a lease table, an allocator, or a collision to resolve.
//! * **Docker.** The engine, containerd, BuildKit state and image store are
//!   guest-side files, so they arrive with the cloned disk. The host half — relay
//!   socket, context name, `engine_id` — is re-minted from the new identity by
//!   the ordinary Up path, exactly as it is for any other new Machine, so a fork
//!   never inherits its parent's context binding.
//!
//! ## What a fork deliberately does NOT mint
//!
//! Endpoints, host exports and host imports are Environment-unique *declared
//! service coordinates*, and a host export additionally owns a host port. Two
//! forks cannot both publish `api` on host port 8080, so a fork republishes
//! none of them. It is reachable by its derived fabric address and by
//! `<machine>@<label>` through `vz exec`; it does not answer its parent's
//! declared names. This is the same reason the design keeps one workspace
//! projection per Machine rather than several worktrees inside one Machine.
//!
//! ## Forks are runtime objects
//!
//! A fork is not in the project definition, so `vz up` sees Machines the
//! definition does not declare. The rule, held identically by reconciliation
//! (criterion 22) and by this module: reconcile leaves forks alone, and only
//! `vz delete --machine <machine>@<label>` removes one. That is enforced in
//! `validate_definition_instance`, which compares the definition against the
//! *declared* Machines only, and in [`MachineForkOrigin`], which is the record
//! that says a Machine is runtime-minted rather than declared.

use serde::{Deserialize, Serialize};

use super::topology::{
    EgressId, EgressInstance, EgressPolicy, EnvironmentInstance, MachineId, MachineInstance,
    MachineState, NetworkAttachmentId, NetworkAttachmentInstance, OwnedResourceKind,
    OwnershipRecord, TOPOLOGY_SCHEMA_VERSION, TopologyValidationError,
};

/// Separator between a Machine name and a fork label in a fork address.
///
/// `@` rather than `/` or `:` because a Machine name may not contain it, a git
/// branch name may not contain it either, and it survives a shell word without
/// quoting.
pub const FORK_ADDRESS_SEPARATOR: char = '@';

/// Longest caller-supplied fork label.
///
/// Bounded well below the 128-byte Machine-name limit so that
/// `<parent>@<label>` fits whatever parent name a definition chose.
pub const MAX_FORK_LABEL_LENGTH: usize = 64;

/// Whether `value` is a well-formed fork label.
///
/// Labels are caller-supplied rather than ordinal because ordinals shift as
/// forks come and go and an agent must be able to *predict* the name it will
/// target. The charset is therefore the one a branch name normalises into
/// losslessly enough to stay predictable: ASCII alphanumerics plus `.`, `_` and
/// `-`, starting with an alphanumeric. A branch name carrying anything else —
/// `/` most often — is normalised by the caller before it gets here, so that
/// the mapping from branch to label is a rule an agent can apply itself rather
/// than a lookup it must perform.
pub fn is_valid_fork_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_FORK_LABEL_LENGTH
        && value.starts_with(|c: char| c.is_ascii_alphanumeric())
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Normalise an arbitrary worktree branch name into a fork label.
///
/// Deterministic and total, because it is a rule agents apply rather than a
/// service they call: every character outside the label charset becomes `-`,
/// runs of `-` collapse, leading non-alphanumerics are dropped, and the result
/// is truncated to [`MAX_FORK_LABEL_LENGTH`]. `feat/third-environment` becomes
/// `feat-third-environment`. Returns `None` when nothing usable survives, which
/// is the caller's cue to require an explicit `--as`.
pub fn fork_label_from_branch(branch: &str) -> Option<String> {
    let mut label = String::with_capacity(branch.len().min(MAX_FORK_LABEL_LENGTH));
    for character in branch.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
            label.push(character);
        } else if !label.ends_with('-') {
            label.push('-');
        }
        if label.len() >= MAX_FORK_LABEL_LENGTH {
            break;
        }
    }
    let trimmed = label
        .trim_start_matches(|c: char| !c.is_ascii_alphanumeric())
        .trim_end_matches(|c: char| !c.is_ascii_alphanumeric());
    let trimmed: String = trimmed.chars().take(MAX_FORK_LABEL_LENGTH).collect();
    is_valid_fork_label(&trimmed).then_some(trimmed)
}

/// One `<machine>` or `<machine>@<label>` selector, parsed.
///
/// Parsing is separate from resolution on purpose: the daemon resolves, but the
/// CLI has to be able to say "that is not a fork address" before it opens a
/// connection, and both halves must agree on the grammar exactly.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MachineForkAddress {
    /// The declared Machine's name — the parent, when a label is present.
    pub machine: String,
    /// Absent for a declared Machine, present for one of its forks.
    pub label: Option<String>,
}

impl MachineForkAddress {
    /// Parse `<machine>` or `<machine>@<label>`.
    ///
    /// Rejects a second separator rather than treating it as part of the label:
    /// `a@b@c` would otherwise read as a fork of a fork, which this release does
    /// not mint, and silently accepting the spelling would let a name exist that
    /// nothing can reproduce.
    pub fn parse(value: &str) -> Result<Self, TopologyValidationError> {
        let invalid = |reason: &str| TopologyValidationError::InvalidIdentifier {
            kind: "machine_fork_address".to_string(),
            value: value.to_string(),
            reason: reason.to_string(),
        };
        match value.split_once(FORK_ADDRESS_SEPARATOR) {
            None => {
                if value.trim().is_empty() {
                    return Err(invalid("Machine selector must not be empty"));
                }
                Ok(Self {
                    machine: value.to_string(),
                    label: None,
                })
            }
            Some((machine, label)) => {
                if machine.trim().is_empty() {
                    return Err(invalid("fork address must name a parent Machine"));
                }
                if label.contains(FORK_ADDRESS_SEPARATOR) {
                    return Err(invalid("a fork address carries exactly one label"));
                }
                if !is_valid_fork_label(label) {
                    return Err(invalid(
                        "fork label must be 1..=64 ASCII alphanumerics, `.`, `_` or `-`, starting alphanumeric",
                    ));
                }
                Ok(Self {
                    machine: machine.to_string(),
                    label: Some(label.to_string()),
                })
            }
        }
    }

    /// Render back to the wire spelling.
    pub fn to_selector(&self) -> String {
        match &self.label {
            Some(label) => format!("{}{FORK_ADDRESS_SEPARATOR}{label}", self.machine),
            None => self.machine.clone(),
        }
    }
}

impl std::fmt::Display for MachineForkAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_selector())
    }
}

/// Persisted lineage of a forked Machine.
///
/// `parent_name` is stored rather than derived from `parent_machine_id` so that
/// the fork's own `name` is verifiable from the record alone: a fork's name is
/// exactly `<parent_name>@<label>`, and validation asserts it. That keeps the
/// address a Machine answers to a property of durable state rather than of a
/// join that could go stale.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MachineForkOrigin {
    pub schema_version: u32,
    pub parent_machine_id: MachineId,
    pub parent_name: String,
    pub label: String,
}

impl MachineForkOrigin {
    /// The name the forked Machine must carry.
    pub fn forked_name(&self) -> String {
        format!("{}{FORK_ADDRESS_SEPARATOR}{}", self.parent_name, self.label)
    }

    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        if self.schema_version != TOPOLOGY_SCHEMA_VERSION {
            return Err(TopologyValidationError::UnsupportedSchemaVersion {
                found: self.schema_version,
                supported: TOPOLOGY_SCHEMA_VERSION,
            });
        }
        // `MachineId::validate` is private to the identifier macro, so the
        // public constructor is the validation: it applies the same rule.
        MachineId::new(self.parent_machine_id.as_str()).map_err(|error| {
            TopologyValidationError::InvalidIdentifier {
                kind: "machine_fork.parent_machine_id".to_string(),
                value: self.parent_machine_id.to_string(),
                reason: error.to_string(),
            }
        })?;
        if self.parent_name.trim().is_empty()
            || self.parent_name.contains(FORK_ADDRESS_SEPARATOR)
            || !is_valid_fork_label(&self.label)
        {
            return Err(TopologyValidationError::InvalidName {
                kind: "machine_fork".to_string(),
                value: self.forked_name(),
            });
        }
        Ok(())
    }
}

/// Everything one fork mints, as one set, before any of it is persisted.
///
/// A plan rather than a mutation so that the whole set can be validated — and
/// refused — before a single row exists. Delete compares an exact expected set
/// and refuses duplicates, so a half-applied fork is state that could never be
/// reclaimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineForkPlan {
    pub machine: MachineInstance,
    pub network_attachments: Vec<NetworkAttachmentInstance>,
    /// Present exactly when the parent carries one: Offline is the absence of an
    /// external attachment, not a filter applied to one.
    pub egress: Option<EgressInstance>,
    pub ownership: Vec<OwnershipRecord>,
}

impl MachineForkPlan {
    /// The forked Machine's immutable id.
    pub fn machine_id(&self) -> &MachineId {
        &self.machine.machine_id
    }

    /// The Machine this fork was seeded from.
    pub fn parent_machine_id(&self) -> Option<&MachineId> {
        self.machine
            .fork
            .as_ref()
            .map(|origin| &origin.parent_machine_id)
    }

    /// Append this plan to an Environment aggregate, leaving it valid.
    ///
    /// The caller re-validates; this only places rows.
    pub fn apply(self, environment: &mut EnvironmentInstance) {
        environment.machines.push(self.machine);
        environment
            .network_attachments
            .extend(self.network_attachments);
        environment.egress.extend(self.egress);
        environment.ownership.extend(self.ownership);
    }
}

impl EnvironmentInstance {
    /// Every fork of `parent`, by label, in label order.
    pub fn forks_of(&self, parent: &MachineId) -> Vec<&MachineInstance> {
        let mut forks: Vec<_> = self
            .machines
            .iter()
            .filter(|machine| {
                machine
                    .fork
                    .as_ref()
                    .is_some_and(|origin| origin.parent_machine_id == *parent)
            })
            .collect();
        forks.sort_by(|left, right| left.name.cmp(&right.name));
        forks
    }

    /// Resolve a `<machine>` / `<machine>@<label>` address to one Machine.
    ///
    /// Exact-name resolution only. Ambiguity is impossible here because
    /// `machine_instances` holds `UNIQUE(environment_id, name)`; the *selector*
    /// ambiguity the CLI fails closed on is between a name and an id, and is
    /// resolved by the caller.
    pub fn machine_by_address(&self, address: &MachineForkAddress) -> Option<&MachineInstance> {
        let wanted = address.to_selector();
        self.machines.iter().find(|machine| machine.name == wanted)
    }

    /// Plan a fork of `parent` labelled `label`.
    ///
    /// Refuses, in this order: an unusable label; an unknown parent; a parent
    /// that is itself a fork; and a label already taken on that parent. Nothing
    /// is mutated on any path.
    pub fn plan_machine_fork(
        &self,
        parent: &MachineId,
        label: &str,
    ) -> Result<MachineForkPlan, TopologyValidationError> {
        if !is_valid_fork_label(label) {
            return Err(TopologyValidationError::InvalidName {
                kind: "machine_fork_label".to_string(),
                value: label.to_string(),
            });
        }
        let parent_machine = self
            .machines
            .iter()
            .find(|machine| machine.machine_id == *parent)
            .ok_or_else(|| TopologyValidationError::MissingReference {
                kind: "machine".to_string(),
                value: parent.to_string(),
            })?;
        // A fork of a fork would need a two-label address and a lineage chain
        // that Delete would have to walk. Neither is minted in 0.4, so the
        // spelling is refused rather than silently flattened onto the
        // grandparent, which would make `backend@a@b` and `backend@b`
        // indistinguishable after the fact.
        if parent_machine.fork.is_some() {
            return Err(TopologyValidationError::InvalidIdentifier {
                kind: "machine_fork_parent".to_string(),
                value: parent_machine.name.clone(),
                reason: "a fork is seeded from a declared Machine, never from another fork".into(),
            });
        }
        let origin = MachineForkOrigin {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            parent_machine_id: parent_machine.machine_id.clone(),
            parent_name: parent_machine.name.clone(),
            label: label.to_string(),
        };
        origin.validate()?;
        let name = origin.forked_name();
        // The store holds `length(trim(name)) BETWEEN 1 AND 128`, so a name that
        // would be refused there is refused here, before any disk is cloned.
        if name.len() > 128 {
            return Err(TopologyValidationError::InvalidName {
                kind: "machine".to_string(),
                value: name,
            });
        }
        if self.machines.iter().any(|machine| machine.name == name) {
            return Err(TopologyValidationError::Duplicate {
                kind: "machine_name".to_string(),
                value: name,
            });
        }

        let machine_id = MachineId::generate();
        let machine = MachineInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machine_id: machine_id.clone(),
            environment_id: self.environment_id.clone(),
            name,
            profile: parent_machine.profile,
            target: parent_machine.target.clone(),
            resources: parent_machine.resources.clone(),
            requested_capabilities: parent_machine.requested_capabilities.clone(),
            // Negotiation, backend placement, incarnation, runtime token and
            // Docker context are all re-established by the fork's own boot. None
            // of them is inherited: a context bound to the parent's incarnation
            // pointing at a fork's engine is precisely the confusion this
            // release must not ship.
            negotiated_capabilities: Default::default(),
            backend: None,
            incarnation: None,
            runtime_identity: None,
            docker_context: None,
            state: MachineState::Creating,
            legacy_sandbox_id: None,
            fork: Some(origin),
        };

        // One fresh attachment per network the parent is on. The new
        // `attachment_id` is what moves the fork to a different host address of
        // the same subnet; the network membership is copied so that a fork is a
        // sibling on exactly the parent's fabric.
        let mut network_attachments: Vec<_> = self
            .network_attachments
            .iter()
            .filter(|attachment| attachment.machine_id == parent_machine.machine_id)
            .map(|attachment| NetworkAttachmentInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                attachment_id: NetworkAttachmentId::generate(),
                environment_id: self.environment_id.clone(),
                machine_id: machine_id.clone(),
                network_id: attachment.network_id.clone(),
            })
            .collect();
        network_attachments
            .sort_by(|left, right| left.network_id.as_str().cmp(right.network_id.as_str()));

        let egress = self
            .egress
            .iter()
            .find(|egress| egress.machine_id == parent_machine.machine_id)
            .map(|parent_egress| EgressInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                egress_id: EgressId::generate(),
                environment_id: self.environment_id.clone(),
                machine_id: machine_id.clone(),
                policy: parent_egress.policy,
            });
        debug_assert!(
            egress.is_some() || parent_machine_is_offline(self, &parent_machine.machine_id),
            "a non-Offline parent must hand its fork an egress record"
        );

        let mut ownership = vec![
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Machine,
                resource_id: machine_id.to_string(),
                environment_id: self.environment_id.clone(),
                machine_id: Some(machine_id.clone()),
            },
            // The record that makes a fork reclaimable and un-prunable in one
            // stroke: Delete traverses it to know the Machine's seeded disk is
            // owned, and reconcile reads it to know the Machine is a runtime
            // object the definition was never meant to declare.
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::MachineFork,
                resource_id: machine_id.to_string(),
                environment_id: self.environment_id.clone(),
                machine_id: Some(machine_id.clone()),
            },
        ];
        ownership.extend(
            network_attachments
                .iter()
                .map(|attachment| OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::NetworkAttachment,
                    resource_id: attachment.attachment_id.to_string(),
                    environment_id: self.environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                }),
        );

        Ok(MachineForkPlan {
            machine,
            network_attachments,
            egress,
            ownership,
        })
    }
}

fn parent_machine_is_offline(environment: &EnvironmentInstance, parent: &MachineId) -> bool {
    !environment
        .egress
        .iter()
        .any(|egress| egress.machine_id == *parent && egress.policy != EgressPolicy::Offline)
}

#[cfg(test)]
#[path = "machine_fork_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "machine_fork_lifecycle_tests.rs"]
mod lifecycle_tests;
