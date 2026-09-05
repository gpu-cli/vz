//! Versioned, non-expiring Machine execution ledger in durable control metadata.
//! Entries are typed and transactionally claimed before dispatch. They are not
//! sandbox Execution rows and cannot be expired by legacy idempotency GC.

use rusqlite::params;
use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    MachineErrorCode, MachineExecutionReceipt, MachineExecutionScope, MachineExecutionState,
    MachineState,
};

use super::StateStore;
use crate::StackError;

const PREFIX: &str = "machine-exec-v1/";

#[cfg(test)]
#[path = "machine_execution_tests.rs"]
mod tests;

fn conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

fn key(idempotency_key: &str) -> String {
    format!("{PREFIX}{:x}", Sha256::digest(idempotency_key.as_bytes()))
}

impl StateStore {
    /// Read a typed non-expiring receipt, including uncertain admissions after restart.
    pub fn load_machine_execution(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<MachineExecutionReceipt>, StackError> {
        let Some(encoded) = self.get_control_metadata(&key(idempotency_key))? else {
            return Ok(None);
        };
        let receipt: MachineExecutionReceipt = serde_json::from_str(&encoded)?;
        receipt.validate().map_err(conflict)?;
        if receipt.scope.idempotency_key != idempotency_key {
            return Err(conflict("Machine execution ledger key/scope mismatch"));
        }
        Ok(Some(receipt))
    }

    /// Atomic exact-generation check and insert-only admission. Repeated keys
    /// never authorize another process, including after restart or timeout.
    pub fn claim_machine_execution(
        &self,
        receipt: &MachineExecutionReceipt,
    ) -> Result<(), StackError> {
        receipt.validate().map_err(conflict)?;
        if receipt.state != MachineExecutionState::Admitted {
            return Err(conflict("new execution claim must be Admitted"));
        }
        self.with_immediate_transaction(|store| {
            if store.load_machine_execution(&receipt.scope.idempotency_key)?.is_some() {
                return Err(conflict("execution idempotency key was already admitted; effects must not be retried"));
            }
            let scope = &receipt.scope;
            let project = store.load_project_state(scope.project_id.as_str())?
                .ok_or_else(|| conflict("execution Project disappeared"))?;
            let environment = project.environments.iter().find(|environment| environment.environment_id == scope.environment_id)
                .ok_or_else(|| conflict("execution Environment disappeared"))?;
            let machine = environment.machines.iter().find(|machine| machine.machine_id == scope.machine_id)
                .ok_or_else(|| conflict("execution Machine disappeared"))?;
            if environment.lifecycle_generation != scope.environment_generation
                || environment.definition_digest != scope.definition_digest
                || environment.active_operation_id.is_some()
                || machine.state != MachineState::Ready
                || machine.incarnation.as_ref() != Some(&scope.incarnation)
                || machine.runtime_identity.as_ref() != Some(&scope.runtime_identity)
            { return Err(conflict("execution requires the exact current Ready Machine with no lifecycle transition")); }
            store.conn.execute("INSERT INTO control_metadata(key, value) VALUES (?1, ?2)",
                params![key(&scope.idempotency_key), serde_json::to_string(receipt)?])?;
            Ok(())
        })
    }

    /// Publish only an exact admitted owner's terminal or uncertainty receipt.
    /// Lifecycle changes do not invalidate evidence about that original process.
    pub fn finish_machine_execution(
        &self,
        expected: &MachineExecutionScope,
        receipt: &MachineExecutionReceipt,
    ) -> Result<(), StackError> {
        receipt.validate().map_err(conflict)?;
        if &receipt.scope != expected || receipt.state == MachineExecutionState::Admitted {
            return Err(conflict(
                "execution completion changed exact scope or omitted outcome",
            ));
        }
        self.with_immediate_transaction(|store| {
            let previous = store.load_machine_execution(&expected.idempotency_key)?
                .ok_or_else(|| conflict("execution completion has no durable admission"))?;
            if previous.scope != *expected || previous.created_at != receipt.created_at || previous.updated_at > receipt.updated_at {
                return Err(conflict("execution completion does not match exact durable admission"));
            }
            if matches!(previous.state, MachineExecutionState::Completed | MachineExecutionState::Quiesced) {
                return if previous == *receipt { Ok(()) } else { Err(conflict("terminal execution receipt is immutable")) };
            }
            let changed = store.conn.execute("UPDATE control_metadata SET value=?1, updated_at=datetime('now') WHERE key=?2 AND value=?3",
                params![serde_json::to_string(receipt)?, key(&expected.idempotency_key), serde_json::to_string(&previous)?])?;
            if changed != 1 { return Err(conflict("execution receipt changed or was tampered with")); }
            Ok(())
        })
    }
}
