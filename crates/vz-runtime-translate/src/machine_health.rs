//! Translation for answer-time Machine supervision health.
//!
//! Health is not part of the persisted aggregate, so it has its own translation
//! pair rather than riding inside `machine_instance_*`: an observation that
//! reached `MachineInstance` would be persisted by the next writer that stored
//! the record it arrived on.

use vz_runtime_contract::{
    EnvironmentId, MACHINE_HEALTH_SCHEMA_VERSION, MachineHealth, MachineHealthObservation,
    MachineId,
};
use vz_runtime_proto::runtime_v2;

use super::{TranslationError, invalid_enum};

pub fn machine_health_to_proto(value: MachineHealth) -> runtime_v2::MachineHealth {
    match value {
        MachineHealth::Supervised => runtime_v2::MachineHealth::Supervised,
        MachineHealth::Unsupervised => runtime_v2::MachineHealth::Unsupervised,
        MachineHealth::Diverged => runtime_v2::MachineHealth::Diverged,
        MachineHealth::Inactive => runtime_v2::MachineHealth::Inactive,
        MachineHealth::Unobservable => runtime_v2::MachineHealth::Unobservable,
    }
}

pub fn machine_health_from_proto(
    raw: i32,
    field: &'static str,
) -> Result<MachineHealth, TranslationError> {
    match runtime_v2::MachineHealth::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::MachineHealth::Supervised => Ok(MachineHealth::Supervised),
        runtime_v2::MachineHealth::Unsupervised => Ok(MachineHealth::Unsupervised),
        runtime_v2::MachineHealth::Diverged => Ok(MachineHealth::Diverged),
        runtime_v2::MachineHealth::Inactive => Ok(MachineHealth::Inactive),
        runtime_v2::MachineHealth::Unobservable => Ok(MachineHealth::Unobservable),
        // Unspecified is never a reading: a daemon that cannot see its registry
        // reports `Unobservable` deliberately, so a zero here is a peer that
        // omitted the field rather than one that answered "unknown".
        runtime_v2::MachineHealth::Unspecified => Err(invalid_enum(field, raw)),
    }
}

pub fn machine_health_observation_to_proto(
    observation: &MachineHealthObservation,
) -> runtime_v2::MachineHealthObservation {
    runtime_v2::MachineHealthObservation {
        schema_version: observation.schema_version,
        environment_id: observation.environment_id.to_string(),
        machine_id: observation.machine_id.to_string(),
        health: machine_health_to_proto(observation.health) as i32,
    }
}

pub fn machine_health_observation_from_proto(
    observation: &runtime_v2::MachineHealthObservation,
) -> Result<MachineHealthObservation, TranslationError> {
    Ok(MachineHealthObservation {
        schema_version: observation.schema_version,
        environment_id: EnvironmentId::new(observation.environment_id.clone())?,
        machine_id: MachineId::new(observation.machine_id.clone())?,
        health: machine_health_from_proto(observation.health, "machine_health_observation.health")?,
    })
}

/// Reject an observation set that does not describe exactly the Machines in the
/// aggregate it arrived with.
///
/// A health list is only meaningful as a join onto the persisted record. A
/// missing entry would silently become "no reading" and an extra or duplicated
/// entry would name a Machine the answer does not contain, so both are protocol
/// errors rather than something a client repairs by guessing.
pub fn machine_health_observations_from_proto(
    observations: &[runtime_v2::MachineHealthObservation],
    expected: &[(EnvironmentId, MachineId)],
) -> Result<Vec<MachineHealthObservation>, TranslationError> {
    let decoded = observations
        .iter()
        .map(machine_health_observation_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let observed: std::collections::BTreeSet<(String, String)> = decoded
        .iter()
        .map(|row| (row.environment_id.to_string(), row.machine_id.to_string()))
        .collect();
    let wanted: std::collections::BTreeSet<(String, String)> = expected
        .iter()
        .map(|(environment, machine)| (environment.to_string(), machine.to_string()))
        .collect();
    if observed.len() != decoded.len() || observed != wanted {
        return Err(TranslationError::InvalidValue {
            field: "get_project_state_response.machine_health",
            value: format!(
                "{} observation(s) for {} Machine(s)",
                decoded.len(),
                wanted.len()
            ),
        });
    }
    for row in &decoded {
        if row.schema_version != MACHINE_HEALTH_SCHEMA_VERSION {
            return Err(TranslationError::InvalidValue {
                field: "machine_health_observation.schema_version",
                value: row.schema_version.to_string(),
            });
        }
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    const ALL: [MachineHealth; 5] = [
        MachineHealth::Supervised,
        MachineHealth::Unsupervised,
        MachineHealth::Diverged,
        MachineHealth::Inactive,
        MachineHealth::Unobservable,
    ];

    fn ids(environment: &str, machine: &str) -> (EnvironmentId, MachineId) {
        (
            EnvironmentId::new(environment.to_string()).unwrap(),
            MachineId::new(machine.to_string()).unwrap(),
        )
    }

    #[test]
    fn every_health_reading_round_trips_losslessly() {
        for health in ALL {
            let wire = machine_health_to_proto(health) as i32;
            assert_eq!(
                machine_health_from_proto(wire, "test").unwrap(),
                health,
                "reading {health} did not survive the wire"
            );
        }
    }

    #[test]
    fn an_unspecified_or_unknown_reading_is_rejected() {
        assert!(machine_health_from_proto(0, "test").is_err());
        assert!(machine_health_from_proto(99, "test").is_err());
    }

    #[test]
    fn an_observation_round_trips_losslessly() {
        let (environment, machine) = ids("env_alpha", "mch_alpha");
        let original =
            MachineHealthObservation::new(environment, machine, MachineHealth::Unsupervised);
        let decoded =
            machine_health_observation_from_proto(&machine_health_observation_to_proto(&original))
                .unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn a_health_set_that_does_not_cover_the_aggregate_is_refused() {
        let (environment, first) = ids("env_alpha", "mch_alpha");
        let (_, second) = ids("env_alpha", "mch_beta");
        let expected = vec![
            (environment.clone(), first.clone()),
            (environment.clone(), second.clone()),
        ];
        let one = [machine_health_observation_to_proto(
            &MachineHealthObservation::new(
                environment.clone(),
                first.clone(),
                MachineHealth::Supervised,
            ),
        )];
        assert!(
            machine_health_observations_from_proto(&one, &expected).is_err(),
            "a missing Machine's reading must not decode as no reading"
        );
        let duplicated = [
            one[0].clone(),
            one[0].clone(),
            machine_health_observation_to_proto(&MachineHealthObservation::new(
                environment.clone(),
                second.clone(),
                MachineHealth::Supervised,
            )),
        ];
        assert!(
            machine_health_observations_from_proto(&duplicated, &expected).is_err(),
            "two readings for one Machine must not decode"
        );
        let stranger = [
            one[0].clone(),
            machine_health_observation_to_proto(&MachineHealthObservation::new(
                environment,
                MachineId::new("mch_stranger".to_string()).unwrap(),
                MachineHealth::Supervised,
            )),
        ];
        assert!(
            machine_health_observations_from_proto(&stranger, &expected).is_err(),
            "a reading for a Machine outside the aggregate must not decode"
        );
    }

    #[test]
    fn an_exact_health_set_decodes() {
        let (environment, first) = ids("env_alpha", "mch_alpha");
        let (_, second) = ids("env_alpha", "mch_beta");
        let expected = vec![
            (environment.clone(), first.clone()),
            (environment.clone(), second.clone()),
        ];
        let wire = [
            machine_health_observation_to_proto(&MachineHealthObservation::new(
                environment.clone(),
                first,
                MachineHealth::Supervised,
            )),
            machine_health_observation_to_proto(&MachineHealthObservation::new(
                environment,
                second,
                MachineHealth::Inactive,
            )),
        ];
        let decoded = machine_health_observations_from_proto(&wire, &expected).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].health, MachineHealth::Supervised);
        assert_eq!(decoded[1].health, MachineHealth::Inactive);
    }
}
