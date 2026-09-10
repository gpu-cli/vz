//! Minting and reclaiming one forked Machine inside a live Environment.
//!
//! `instantiate_environment` mints an Environment's entire identity graph at
//! once, in the transaction that creates the Environment. Forking needs the
//! opposite shape: mint identity for *one* Machine, into an Environment that
//! already exists, without re-provisioning anything the definition declares.
//! That is what this module is, and it is deliberately the only place that adds
//! a Machine to a live Environment.
//!
//! Both operations here are exact. The fork inserts precisely the rows
//! [`EnvironmentInstance::plan_machine_fork`] planned and re-validates the whole
//! aggregate before committing, so a half-applied fork — state that Delete's
//! exact-set comparison could never reconcile — is not reachable. The reclaim
//! removes precisely that Machine's rows and refuses on any count but the one it
//! expected, for the same reason `delete_exact_environment` does.

use rusqlite::params;
use vz_runtime_contract::{
    EnvironmentInstance, EnvironmentState, MachineErrorCode, MachineForkPlan, MachineId,
    OwnershipRecord,
};

use super::StateStore;
use crate::StackError;

#[cfg(test)]
#[path = "machine_fork_tests.rs"]
mod tests;

fn conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

impl StateStore {
    /// Mint one fork of `parent`, labelled `label`, inside `environment_id`.
    ///
    /// Returns the plan that was applied, so the caller can seed the fork's
    /// disk from its parent's using identities it did not have to guess.
    ///
    /// Idempotent by name: re-running with a label that already names a fork of
    /// the same parent returns that fork's plan-shaped view rather than minting
    /// a second Machine, because an agent that lost the response must not end up
    /// with two warm copies it did not ask for.
    pub fn fork_machine_in_environment(
        &self,
        environment_id: &str,
        parent: &MachineId,
        label: &str,
        now: u64,
    ) -> Result<MachineForkPlan, StackError> {
        self.with_immediate_transaction(|store| {
            let before = store
                .load_environment_instance(environment_id)?
                .ok_or_else(|| conflict(format!("Environment `{environment_id}` not found")))?;
            // A fork mints identity, and identity must not be minted while a
            // lifecycle operation is deciding what this Environment owns.
            if matches!(
                before.state,
                EnvironmentState::Reconciling | EnvironmentState::Deleting
            ) {
                return Err(conflict(format!(
                    "Environment `{environment_id}` is {:?}; forking requires a settled Environment",
                    before.state
                )));
            }
            let plan = before
                .plan_machine_fork(parent, label)
                .map_err(|error| conflict(error.to_string()))?;

            let mut after = before.clone();
            plan.clone().apply(&mut after);
            after.updated_at = after.updated_at.max(now);
            after
                .validate()
                .map_err(|error| conflict(error.to_string()))?;

            let machine = &plan.machine;
            store.conn.execute(
                "INSERT INTO machine_instances
                    (machine_id, environment_id, schema_version, name, state, instance_json,
                     legacy_sandbox_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
                params![
                    machine.machine_id.as_str(),
                    machine.environment_id.as_str(),
                    machine.schema_version,
                    machine.name,
                    serde_json::to_string(&machine.state)?,
                    serde_json::to_string(machine)?,
                ],
            )?;
            for attachment in &plan.network_attachments {
                store.conn.execute(
                    "INSERT INTO environment_network_attachments
                        (attachment_id, environment_id, machine_id, network_id, schema_version,
                         instance_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        attachment.attachment_id.as_str(),
                        attachment.environment_id.as_str(),
                        attachment.machine_id.as_str(),
                        attachment.network_id.as_str(),
                        attachment.schema_version,
                        serde_json::to_string(attachment)?,
                    ],
                )?;
            }
            if let Some(egress) = &plan.egress {
                store.conn.execute(
                    "INSERT INTO environment_machine_egress
                        (egress_id, environment_id, machine_id, schema_version, policy,
                         instance_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        egress.egress_id.as_str(),
                        egress.environment_id.as_str(),
                        egress.machine_id.as_str(),
                        egress.schema_version,
                        serde_json::to_string(&egress.policy)?,
                        serde_json::to_string(egress)?,
                    ],
                )?;
            }
            for record in &plan.ownership {
                store.conn.execute(
                    "INSERT INTO topology_ownership
                        (resource_kind, resource_id, environment_id, machine_id, schema_version,
                         record_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        serde_json::to_string(&record.resource_kind)?,
                        record.resource_id,
                        record.environment_id.as_str(),
                        record.machine_id.as_ref().map(MachineId::as_str),
                        record.schema_version,
                        serde_json::to_string(record)?,
                    ],
                )?;
            }
            store.replace_environment_snapshot(&before, &after)?;
            Ok(plan)
        })
    }

    /// Remove exactly one forked Machine and everything it owns.
    ///
    /// Refuses a Machine the definition declares. `vz delete` without
    /// `--machine` means the Environment, and a declared Machine is part of the
    /// definition rather than something a caller may subtract from it; letting
    /// this remove one would put the Environment permanently at odds with its
    /// own `vz.json`.
    ///
    /// The reclaim is exact in the same sense `delete_exact_environment` is:
    /// every removal asserts its row count, so a sibling fork that shared a row
    /// by mistake surfaces as a refusal rather than as a silent extra deletion.
    /// `expected_ownership` is the set the caller proved it had already released
    /// on the host, so the store never removes a record for a resource that is
    /// still live.
    pub fn delete_exact_machine_fork(
        &self,
        environment_id: &str,
        machine_id: &MachineId,
        expected_ownership: &[OwnershipRecord],
        now: u64,
    ) -> Result<EnvironmentInstance, StackError> {
        self.with_immediate_transaction(|store| {
            let before = store
                .load_environment_instance(environment_id)?
                .ok_or_else(|| conflict(format!("Environment `{environment_id}` not found")))?;
            let machine = before
                .machines
                .iter()
                .find(|machine| machine.machine_id == *machine_id)
                .ok_or_else(|| {
                    conflict(format!(
                        "Machine `{machine_id}` is not in Environment `{environment_id}`"
                    ))
                })?;
            if machine.fork.is_none() {
                return Err(StackError::Machine {
                    code: MachineErrorCode::UnsupportedOperation,
                    message: format!(
                        "Machine `{}` is declared by the project definition; only a fork can be deleted on its own",
                        machine.name
                    ),
                });
            }

            // Everything this Machine owns, as the store actually holds it. The
            // caller's expected set must match exactly: a record it did not
            // release is a live resource, and one it released that is not here
            // is state it changed underneath us.
            let held: Vec<OwnershipRecord> = before
                .ownership
                .iter()
                .filter(|record| record.machine_id.as_ref() == Some(machine_id))
                .cloned()
                .collect();
            let held_set: std::collections::BTreeSet<_> = held.iter().collect();
            let expected_set: std::collections::BTreeSet<_> = expected_ownership.iter().collect();
            if held_set.len() != held.len()
                || expected_set.len() != expected_ownership.len()
                || held_set != expected_set
            {
                return Err(conflict(format!(
                    "fork `{}` ownership graph does not match the released set exactly; no rows removed",
                    machine.name
                )));
            }

            for record in &held {
                let affected = store.conn.execute(
                    "DELETE FROM topology_ownership
                     WHERE resource_kind = ?1 AND resource_id = ?2 AND environment_id = ?3
                       AND machine_id IS ?4",
                    params![
                        serde_json::to_string(&record.resource_kind)?,
                        record.resource_id,
                        record.environment_id.as_str(),
                        record.machine_id.as_ref().map(MachineId::as_str),
                    ],
                )?;
                if affected != 1 {
                    return Err(conflict(format!(
                        "owned resource `{}` changed during exact fork delete",
                        record.resource_id
                    )));
                }
            }

            // Attachments and egress cascade from the Machine row, exactly as
            // they do for a whole-Environment delete. Counting them first and
            // asserting the cascade afterwards is what turns "probably gone"
            // into "provably gone".
            let attachments = before
                .network_attachments
                .iter()
                .filter(|attachment| attachment.machine_id == *machine_id)
                .count();
            let egress = before
                .egress
                .iter()
                .filter(|egress| egress.machine_id == *machine_id)
                .count();
            let affected = store.conn.execute(
                "DELETE FROM machine_instances WHERE machine_id = ?1 AND environment_id = ?2",
                params![machine_id.as_str(), environment_id],
            )?;
            if affected != 1 {
                return Err(conflict(format!(
                    "fork `{machine_id}` changed during exact delete"
                )));
            }
            for (table, expected) in [
                ("environment_network_attachments", attachments),
                ("environment_machine_egress", egress),
            ] {
                let remaining: usize = store.conn.query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE machine_id = ?1"),
                    params![machine_id.as_str()],
                    |row| row.get(0),
                )?;
                if remaining != 0 {
                    return Err(conflict(format!(
                        "fork `{machine_id}` left {remaining} of {expected} `{table}` rows behind"
                    )));
                }
            }

            let mut after = before.clone();
            after.machines.retain(|entry| entry.machine_id != *machine_id);
            after
                .network_attachments
                .retain(|entry| entry.machine_id != *machine_id);
            after.egress.retain(|entry| entry.machine_id != *machine_id);
            after
                .ownership
                .retain(|entry| entry.machine_id.as_ref() != Some(machine_id));
            after.updated_at = after.updated_at.max(now);
            after
                .validate()
                .map_err(|error| conflict(error.to_string()))?;
            store.replace_environment_snapshot(&before, &after)?;
            Ok(after)
        })
    }

    /// Rewrite one Environment's durable snapshot, refusing a concurrent change.
    ///
    /// Fenced on `(lifecycle_generation, active_operation_id)` rather than on
    /// the snapshot bytes. The snapshot is a denormalised cache that a loader
    /// cross-checks against the row projections but does not re-serialise
    /// canonically, so comparing its bytes would refuse a perfectly consistent
    /// second fork. The fencing columns are the authority on whether anything
    /// has decided this Environment's ownership since `before` was read, and
    /// they are what every other lifecycle mutation compares.
    fn replace_environment_snapshot(
        &self,
        before: &EnvironmentInstance,
        after: &EnvironmentInstance,
    ) -> Result<(), StackError> {
        let affected = self.conn.execute(
            "UPDATE environment_instances
             SET instance_json = ?1, updated_at = ?2
             WHERE environment_id = ?3
               AND lifecycle_generation = ?4
               AND active_operation_id IS ?5",
            params![
                serde_json::to_string(after)?,
                after.updated_at as i64,
                after.environment_id.as_str(),
                before.lifecycle_generation as i64,
                before.active_operation_id.as_ref().map(|id| id.as_str()),
            ],
        )?;
        if affected != 1 {
            return Err(conflict(format!(
                "Environment `{}` changed while applying a Machine fork mutation",
                after.environment_id
            )));
        }
        self.refresh_project_timestamps(after.project_id.as_str())
    }
}
