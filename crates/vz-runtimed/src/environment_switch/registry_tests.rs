//! Ownership is the point of this module, so every test is about who may act on
//! a switch, not about forwarding.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::environment_runtime_controller::EnvironmentRuntimeController;
use crate::environment_switch::{MacAddress, PortId};
use vz_runtime_contract::{MachineId, ProjectId};

const PROJECT: &str = "prj_0123456789abcdef0123456789abcdef";
const ENVIRONMENT: &str = "env_0123456789abcdef0123456789abcdef";
const OTHER_ENVIRONMENT: &str = "env_fedcba9876543210fedcba9876543210";

fn ids(environment: &str) -> (ProjectId, EnvironmentId) {
    (
        ProjectId::new(PROJECT.to_string()).unwrap(),
        EnvironmentId::new(environment.to_string()).unwrap(),
    )
}

fn owner(environment: &str) -> ResourceOwner {
    let (project_id, environment_id) = ids(environment);
    ResourceOwner {
        project_id,
        environment_id,
        machine_id: None,
    }
}

fn switch(ports: u32) -> NetworkSwitch {
    let members: Vec<(PortId, MacAddress)> = (1..=ports)
        .map(|index| {
            (
                PortId(index),
                MacAddress::derive(ENVIRONMENT, &format!("machine-{index}"), "net"),
            )
        })
        .collect();
    NetworkSwitch::start("declared", members).unwrap().0
}

async fn lease(
    controller: &EnvironmentRuntimeController,
    environment: &str,
) -> crate::environment_runtime_controller::EnvironmentControllerLease {
    let (project_id, environment_id) = ids(environment);
    controller
        .acquire(&project_id, &environment_id)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_switch_is_owned_by_its_environment_until_it_is_stopped() {
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let owner = owner(ENVIRONMENT);
    switches
        .install(&held, &owner, "private", switch(2))
        .await
        .unwrap();
    assert_eq!(
        switches.networks(&owner.environment_id).await,
        vec!["private".to_string()]
    );
    let receipt = switches.stop(&held, &owner).await.unwrap();
    assert_eq!(receipt.networks.len(), 1);
    assert!(receipt.networks.contains_key("private"));
    assert!(switches.networks(&owner.environment_id).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_network_cannot_have_two_switches() {
    // Two switches on one network would each hold half its ports and neither
    // would forward between them, which reads as packet loss rather than as the
    // mistake it is.
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let owner = owner(ENVIRONMENT);
    switches
        .install(&held, &owner, "private", switch(1))
        .await
        .unwrap();
    let refused = switches.install(&held, &owner, "private", switch(1)).await;
    assert_eq!(
        refused,
        Err(conflict(
            "network `private` already has a switch in this Environment"
        ))
    );
    switches.stop(&held, &owner).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lease_cannot_act_on_another_environments_switches() {
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let foreign = owner(OTHER_ENVIRONMENT);
    assert!(matches!(
        switches
            .install(&held, &foreign, "private", switch(1))
            .await,
        Err(SwitchRegistryError::Conflict(_))
    ));
    assert!(matches!(
        switches.stop(&held, &foreign).await,
        Err(SwitchRegistryError::Conflict(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_controller_cannot_adopt_established_switches() {
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let owner = owner(ENVIRONMENT);
    switches
        .install(&held, &owner, "private", switch(1))
        .await
        .unwrap();

    let other_controller = EnvironmentRuntimeController::default();
    let other = lease(&other_controller, ENVIRONMENT).await;
    assert_eq!(
        switches.install(&other, &owner, "second", switch(1)).await,
        Err(conflict(
            "switch registry belongs to another Environment controller"
        ))
    );
    assert_eq!(
        switches.stop(&other, &owner).await,
        Err(conflict(
            "switch registry belongs to another Environment controller"
        ))
    );
    // The original controller still owns what it established.
    assert_eq!(
        switches.stop(&held, &owner).await.unwrap().networks.len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_twice_reports_nothing_the_second_time() {
    // Switches are removed from the registry before being joined, so a repeated
    // stop finds nothing rather than joining a task that is already gone.
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let owner = owner(ENVIRONMENT);
    switches
        .install(&held, &owner, "private", switch(2))
        .await
        .unwrap();
    assert_eq!(
        switches.stop(&held, &owner).await.unwrap().networks.len(),
        1
    );
    assert_eq!(
        switches.stop(&held, &owner).await.unwrap(),
        EnvironmentSwitchShutdown::default()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_environment_can_own_a_switch_for_each_of_its_networks() {
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let owner = owner(ENVIRONMENT);
    for network in ["private", "build", "data"] {
        switches
            .install(&held, &owner, network, switch(1))
            .await
            .unwrap();
    }
    assert_eq!(switches.networks(&owner.environment_id).await.len(), 3);
    let receipt = switches.stop(&held, &owner).await.unwrap();
    assert_eq!(receipt.networks.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_switch_needs_a_bounded_network_name() {
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let owner = owner(ENVIRONMENT);
    for name in ["", "   ", &"n".repeat(129)] {
        assert!(matches!(
            switches.install(&held, &owner, name, switch(1)).await,
            Err(SwitchRegistryError::Conflict(_))
        ));
    }
}

/// Referenced so the import is not dead; a Machine-scoped owner is refused the
/// same way an Environment-scoped one from elsewhere is.
#[tokio::test(flavor = "multi_thread")]
async fn a_machine_scoped_owner_still_has_to_match_the_lease() {
    let switches = EnvironmentSwitches::default();
    let controller = EnvironmentRuntimeController::default();
    let held = lease(&controller, ENVIRONMENT).await;
    let mut scoped = owner(ENVIRONMENT);
    scoped.machine_id =
        Some(MachineId::new("mch_0123456789abcdef0123456789abcdef".to_string()).unwrap());
    // A Machine-scoped owner of the same Environment is still that Environment.
    switches
        .install(&held, &scoped, "private", switch(1))
        .await
        .unwrap();
    switches.stop(&held, &scoped).await.unwrap();
}
