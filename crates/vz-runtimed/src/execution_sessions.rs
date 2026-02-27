use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task::AbortHandle;
use vz_runtime_proto::runtime_v2;

const EXEC_OUTPUT_BUFFER_SIZE: usize = 128;

#[derive(Debug, Error)]
pub(crate) enum ExecutionSessionRegistryError {
    #[error("execution session registry lock poisoned")]
    LockPoisoned,
    #[error("execution session not found: {execution_id}")]
    NotFound { execution_id: String },
}

struct ExecutionSession {
    output_tx: broadcast::Sender<runtime_v2::ExecOutputEvent>,
    next_sequence: AtomicU64,
    task_abort: Option<AbortHandle>,
}

impl ExecutionSession {
    fn new() -> Self {
        let (output_tx, _) = broadcast::channel(EXEC_OUTPUT_BUFFER_SIZE);
        Self {
            output_tx,
            next_sequence: AtomicU64::new(1),
            task_abort: None,
        }
    }

    fn next_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn subscribe(&self) -> broadcast::Receiver<runtime_v2::ExecOutputEvent> {
        self.output_tx.subscribe()
    }

    fn publish(
        &self,
        payload: runtime_v2::exec_output_event::Payload,
    ) -> Result<(), ExecutionSessionRegistryError> {
        let _ = self.output_tx.send(runtime_v2::ExecOutputEvent {
            payload: Some(payload),
            sequence: self.next_sequence(),
        });
        Ok(())
    }

    fn set_task_abort(&mut self, task_abort: AbortHandle) {
        self.task_abort = Some(task_abort);
    }

    fn abort_task(&self) -> bool {
        match &self.task_abort {
            Some(handle) => {
                handle.abort();
                true
            }
            None => false,
        }
    }
}

#[derive(Default)]
pub(crate) struct ExecutionSessionRegistry {
    sessions: RwLock<HashMap<String, ExecutionSession>>,
}

impl ExecutionSessionRegistry {
    pub(crate) fn register(&self, execution_id: &str) -> Result<(), ExecutionSessionRegistryError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        sessions
            .entry(execution_id.to_string())
            .or_insert_with(ExecutionSession::new);
        Ok(())
    }

    pub(crate) fn contains(
        &self,
        execution_id: &str,
    ) -> Result<bool, ExecutionSessionRegistryError> {
        let sessions = self
            .sessions
            .read()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        Ok(sessions.contains_key(execution_id))
    }

    pub(crate) fn subscribe(
        &self,
        execution_id: &str,
    ) -> Result<broadcast::Receiver<runtime_v2::ExecOutputEvent>, ExecutionSessionRegistryError>
    {
        let sessions = self
            .sessions
            .read()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        let session =
            sessions
                .get(execution_id)
                .ok_or_else(|| ExecutionSessionRegistryError::NotFound {
                    execution_id: execution_id.to_string(),
                })?;
        Ok(session.subscribe())
    }

    pub(crate) fn publish_exit_code(
        &self,
        execution_id: &str,
        exit_code: i32,
    ) -> Result<(), ExecutionSessionRegistryError> {
        self.publish(
            execution_id,
            runtime_v2::exec_output_event::Payload::ExitCode(exit_code),
        )
    }

    pub(crate) fn publish_stdout(
        &self,
        execution_id: &str,
        stdout: Vec<u8>,
    ) -> Result<(), ExecutionSessionRegistryError> {
        self.publish(
            execution_id,
            runtime_v2::exec_output_event::Payload::Stdout(stdout),
        )
    }

    pub(crate) fn publish_stderr(
        &self,
        execution_id: &str,
        stderr: Vec<u8>,
    ) -> Result<(), ExecutionSessionRegistryError> {
        self.publish(
            execution_id,
            runtime_v2::exec_output_event::Payload::Stderr(stderr),
        )
    }

    pub(crate) fn publish_error(
        &self,
        execution_id: &str,
        error: String,
    ) -> Result<(), ExecutionSessionRegistryError> {
        self.publish(
            execution_id,
            runtime_v2::exec_output_event::Payload::Error(error),
        )
    }

    fn publish(
        &self,
        execution_id: &str,
        payload: runtime_v2::exec_output_event::Payload,
    ) -> Result<(), ExecutionSessionRegistryError> {
        let sessions = self
            .sessions
            .read()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        let session =
            sessions
                .get(execution_id)
                .ok_or_else(|| ExecutionSessionRegistryError::NotFound {
                    execution_id: execution_id.to_string(),
                })?;
        session.publish(payload)
    }

    pub(crate) fn attach_task_abort(
        &self,
        execution_id: &str,
        task_abort: AbortHandle,
    ) -> Result<(), ExecutionSessionRegistryError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        let session = sessions.get_mut(execution_id).ok_or_else(|| {
            ExecutionSessionRegistryError::NotFound {
                execution_id: execution_id.to_string(),
            }
        })?;
        session.set_task_abort(task_abort);
        Ok(())
    }

    pub(crate) fn abort_task(
        &self,
        execution_id: &str,
    ) -> Result<bool, ExecutionSessionRegistryError> {
        let sessions = self
            .sessions
            .read()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        let session =
            sessions
                .get(execution_id)
                .ok_or_else(|| ExecutionSessionRegistryError::NotFound {
                    execution_id: execution_id.to_string(),
                })?;
        Ok(session.abort_task())
    }

    pub(crate) fn remove(&self, execution_id: &str) -> Result<(), ExecutionSessionRegistryError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| ExecutionSessionRegistryError::LockPoisoned)?;
        sessions.remove(execution_id);
        Ok(())
    }
}
