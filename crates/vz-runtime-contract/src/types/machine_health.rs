//! Answer-time supervision health for one persisted Machine.
//!
//! Health is deliberately **not** a field of [`MachineInstance`]. A
//! `MachineInstance` is durable record: everything on it survives a daemon
//! restart and must mean the same thing when it is read back. Supervision is
//! the opposite — it is a property of the process that answered, valid only for
//! the answer that carried it — so persisting it would produce a record that is
//! stale the moment the daemon exits and true-looking forever after.
//!
//! It therefore travels beside the aggregate as a separate observation, joined
//! to the record by `machine_id`, and the daemon recomputes it on every reply.

use serde::{Deserialize, Serialize};

use super::topology::{EnvironmentId, MachineId};

/// Schema version of [`MachineHealthObservation`].
pub const MACHINE_HEALTH_SCHEMA_VERSION: u32 = 1;

/// What the answering daemon can see of one Machine's supervision.
///
/// # What this observes
///
/// Exactly one question: does the daemon that produced this answer still hold
/// the live supervised runtime session it registered when it booted this
/// Machine, and does that session name the same runtime identity the persisted
/// record names? That is the daemon reading its own in-memory registry at reply
/// time, which is why it can disagree with the persisted `MachineState` — and
/// why it is worth reporting at all.
///
/// # What this does not observe
///
/// Nothing inside the guest. It is not a ping, not a guest-agent round trip,
/// not a Docker Engine probe, not a service or endpoint health check, and not a
/// statement that any declared workload is serving. A `Supervised` Machine can
/// still be one whose application is wedged; that is a different question, and
/// answering it would require reaching into the guest, which routine `status`
/// deliberately does not do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineHealth {
    /// The answering daemon holds a live session for this Machine, that session
    /// still owns its runtime resources, and its runtime identity is exactly
    /// the one persisted on the Machine record.
    Supervised,
    /// The persisted record is `Ready` — a state only a successful activation
    /// produces — but the answering daemon holds no session for this Machine.
    /// The usual cause is that this daemon is not the one that booted it.
    Unsupervised,
    /// A session exists but does not agree with the record: it names a
    /// different runtime identity, a failed Up is bound to it, it released its
    /// resources without a positive teardown receipt (a teardown is in flight,
    /// or one failed), or it holds a positive teardown receipt for a Machine
    /// the record still calls `Ready`.
    Diverged,
    /// No live supervision is expected and none is claimed. Either the daemon
    /// holds no session and the record does not claim `Ready`, or it holds the
    /// spent session a positive Stop leaves behind and the record agrees the
    /// Machine is down. This is the ordinary reading of a stopped, failed, or
    /// still-creating Machine.
    Inactive,
    /// The answering daemon could not read its own supervision registry, so it
    /// reports that rather than guessing one of the four answers above. Also
    /// what a client records for a Machine the daemon returned no observation
    /// for at all.
    Unobservable,
}

impl MachineHealth {
    /// The stable wire/JSON spelling, for diagnostics and status projections.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supervised => "supervised",
            Self::Unsupervised => "unsupervised",
            Self::Diverged => "diverged",
            Self::Inactive => "inactive",
            Self::Unobservable => "unobservable",
        }
    }
}

impl std::fmt::Display for MachineHealth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One Machine's health as the answering daemon saw it, at reply time.
///
/// Carries its own `environment_id` because a project answer spans several
/// Environments and a `machine_id` is only unique within one of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineHealthObservation {
    pub schema_version: u32,
    pub environment_id: EnvironmentId,
    pub machine_id: MachineId,
    pub health: MachineHealth,
}

impl MachineHealthObservation {
    pub fn new(
        environment_id: EnvironmentId,
        machine_id: MachineId,
        health: MachineHealth,
    ) -> Self {
        Self {
            schema_version: MACHINE_HEALTH_SCHEMA_VERSION,
            environment_id,
            machine_id,
            health,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn observation(health: MachineHealth) -> MachineHealthObservation {
        MachineHealthObservation::new(
            EnvironmentId::new("env_health".to_string()).unwrap(),
            MachineId::new("mch_health".to_string()).unwrap(),
            health,
        )
    }

    #[test]
    fn every_health_reading_has_a_distinct_stable_spelling() {
        let all = [
            MachineHealth::Supervised,
            MachineHealth::Unsupervised,
            MachineHealth::Diverged,
            MachineHealth::Inactive,
            MachineHealth::Unobservable,
        ];
        let spellings: std::collections::BTreeSet<&str> =
            all.iter().map(|health| health.as_str()).collect();
        assert_eq!(
            spellings.len(),
            all.len(),
            "spellings collided: {spellings:?}"
        );
        for health in all {
            let json = serde_json::to_value(health).unwrap();
            assert_eq!(json, serde_json::Value::String(health.as_str().to_string()));
        }
    }

    #[test]
    fn an_observation_round_trips_through_json() {
        let original = observation(MachineHealth::Diverged);
        let text = serde_json::to_string(&original).unwrap();
        let decoded: MachineHealthObservation = serde_json::from_str(&text).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.schema_version, MACHINE_HEALTH_SCHEMA_VERSION);
    }
}
