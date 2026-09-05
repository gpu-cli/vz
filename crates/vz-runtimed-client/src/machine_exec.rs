//! Strict bidirectional exact-Machine execution, independent of legacy Execution.
use crate::{DaemonClient, DaemonClientError, Result};
use std::time::Duration;
use tokio::sync::mpsc;
use vz_runtime_contract::{
    EnvironmentId, MachineExecutionReceipt, MachineExecutionScope, MachineExecutionState,
    MachineExecutionTerminal, MachineId, ProjectId,
};
use vz_runtime_proto::runtime_v2;

#[cfg(test)]
#[path = "machine_exec_tests.rs"]
mod tests;

#[derive(Debug, Clone)]
pub enum MachineExecOutput {
    Ready,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Receipt(Box<MachineExecutionReceipt>),
}
#[derive(Debug, Clone)]
pub struct MachineExecEvent {
    pub scope: MachineExecutionScope,
    pub sequence: u64,
    pub replayed: bool,
    pub output: MachineExecOutput,
    pub output_stream: Option<&'static str>,
}

struct Validator {
    spec: vz_runtime_contract::MachineExecutionSpec,
    project: ProjectId,
    environment: Option<EnvironmentId>,
    machine: Option<MachineId>,
    metadata: runtime_v2::RequestMetadata,
    scope: Option<MachineExecutionScope>,
    sequence: u64,
    ready: bool,
    terminal: bool,
}
fn invalid(reason: impl Into<String>) -> DaemonClientError {
    DaemonClientError::IncompatibleProtocol {
        reason: reason.into(),
    }
}

impl Validator {
    fn event(&mut self, event: runtime_v2::MachineExecEvent) -> Result<MachineExecEvent> {
        if self.terminal || event.schema_version != 1 || event.sequence != self.sequence {
            return Err(invalid(
                "Machine Exec schema, sequence, or terminal ordering mismatch",
            ));
        }
        let scope = vz_runtime_translate::machine_execution_scope_from_proto(
            event
                .scope
                .as_ref()
                .ok_or_else(|| invalid("Machine Exec omitted exact scope"))?,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let expected_hash = self
            .spec
            .request_hash(&self.project, &scope.environment_id, &scope.machine_id)
            .map_err(invalid)?;
        if scope.request_hash != expected_hash
            || scope.project_id != self.project
            || scope.request_id != self.metadata.request_id
            || scope.idempotency_key != self.metadata.idempotency_key
            || self
                .environment
                .as_ref()
                .is_some_and(|id| id != &scope.environment_id)
            || self
                .machine
                .as_ref()
                .is_some_and(|id| id != &scope.machine_id)
            || self
                .scope
                .as_ref()
                .is_some_and(|expected| expected != &scope)
        {
            return Err(invalid(
                "Machine Exec changed its complete immutable execution scope",
            ));
        }
        let (output, output_stream) = match event.payload {
            Some(runtime_v2::machine_exec_event::Payload::Ready(true))
                if !self.ready && self.sequence == 0 && !event.replayed =>
            {
                self.ready = true;
                (MachineExecOutput::Ready, None)
            }
            Some(runtime_v2::machine_exec_event::Payload::Stdout(bytes))
                if self.ready && !event.replayed && !bytes.is_empty() && bytes.len() <= 65536 =>
            {
                (MachineExecOutput::Stdout(bytes), Some("stdout"))
            }
            Some(runtime_v2::machine_exec_event::Payload::Stderr(bytes))
                if self.ready && !event.replayed && !bytes.is_empty() && bytes.len() <= 65536 =>
            {
                (MachineExecOutput::Stderr(bytes), Some("stderr"))
            }
            Some(runtime_v2::machine_exec_event::Payload::Receipt(wire)) => {
                let receipt = vz_runtime_translate::machine_execution_receipt_from_proto(&wire)
                    .map_err(|error| invalid(error.to_string()))?;
                if receipt.scope != scope
                    || receipt.state == MachineExecutionState::Admitted
                    || (event.replayed
                        && (self.sequence != 0
                            || !matches!(
                                receipt.state,
                                MachineExecutionState::Completed | MachineExecutionState::Quiesced
                            )))
                {
                    return Err(invalid(
                        "Machine Exec terminal receipt/replay contract mismatch",
                    ));
                }
                self.terminal = true;
                (MachineExecOutput::Receipt(Box::new(receipt)), None)
            }
            _ => {
                return Err(invalid(
                    "Machine Exec output arrived before readiness, exceeded bounds, or changed replay mode",
                ));
            }
        };
        self.scope = Some(scope.clone());
        self.sequence += 1;
        Ok(MachineExecEvent {
            scope,
            sequence: event.sequence,
            replayed: event.replayed,
            output,
            output_stream,
        })
    }
}

pub struct MachineExecStream {
    incoming: tonic::Streaming<runtime_v2::MachineExecEvent>,
    controls: mpsc::Sender<runtime_v2::MachineExecFrame>,
    validation: Validator,
    control_sequence: u64,
    stdin_closed: bool,
    terminal_requested: bool,
    pty: bool,
    deadline: tokio::time::Instant,
}
impl MachineExecStream {
    /// EOF without a validated terminal receipt is failure. Dropping the stream
    /// closes observation and asks the retained daemon supervisor to cancel/reap.
    pub async fn next_event(&mut self) -> Result<Option<MachineExecEvent>> {
        let next=tokio::time::timeout_at(self.deadline,self.incoming.message()).await.map_err(|_|invalid("Machine Exec observation deadline expired; exact reservation remains non-retryable"))?.map_err(|error|DaemonClientError::Grpc(Box::new(error)))?;
        match next {
            Some(event) => self.validation.event(event).map(Some),
            None if self.validation.terminal => Ok(None),
            None => Err(invalid(
                "Machine Exec ended without positive terminal receipt",
            )),
        }
    }
    async fn control(&mut self, payload: runtime_v2::machine_exec_frame::Payload) -> Result<()> {
        if !self.validation.ready || self.validation.terminal || self.terminal_requested {
            return Err(invalid(
                "Machine Exec control requires a live validated Ready session",
            ));
        }
        let scope = self
            .validation
            .scope
            .as_ref()
            .ok_or_else(|| invalid("Machine Exec scope unavailable"))?;
        let frame = runtime_v2::MachineExecFrame {
            metadata: Some(self.validation.metadata.clone()),
            sequence: self.control_sequence,
            execution_id: scope.execution_id.clone(),
            payload: Some(payload),
        };
        tokio::time::timeout(Duration::from_secs(5), self.controls.send(frame))
            .await
            .map_err(|_| invalid("Machine Exec control backpressure timed out"))?
            .map_err(|_| invalid("Machine Exec control stream closed"))?;
        self.control_sequence += 1;
        Ok(())
    }
    pub async fn stdin_write(&mut self, bytes: Vec<u8>) -> Result<()> {
        if self.stdin_closed || bytes.is_empty() || bytes.len() > 65536 {
            return Err(invalid("invalid Machine Exec stdin frame"));
        }
        self.control(runtime_v2::machine_exec_frame::Payload::Stdin(bytes))
            .await
    }
    pub async fn stdin_eof(&mut self) -> Result<()> {
        if self.stdin_closed {
            return Err(invalid("duplicate Machine Exec stdin EOF"));
        }
        self.control(runtime_v2::machine_exec_frame::Payload::StdinEof(true))
            .await?;
        self.stdin_closed = true;
        Ok(())
    }
    pub async fn signal(&mut self, signal: i32) -> Result<()> {
        if !(1..=64).contains(&signal) {
            return Err(invalid("invalid Machine Exec signal"));
        }
        self.control(runtime_v2::machine_exec_frame::Payload::Signal(signal))
            .await
    }
    pub async fn resize(&mut self, terminal: MachineExecutionTerminal) -> Result<()> {
        if !self.pty || terminal.rows == 0 || terminal.columns == 0 {
            return Err(invalid("invalid Machine Exec resize"));
        }
        self.control(runtime_v2::machine_exec_frame::Payload::Resize(
            runtime_v2::MachineExecutionTerminal {
                rows: terminal.rows.into(),
                columns: terminal.columns.into(),
            },
        ))
        .await
    }
    pub async fn cancel(&mut self) -> Result<()> {
        self.control(runtime_v2::machine_exec_frame::Payload::Cancel(true))
            .await?;
        self.terminal_requested = true;
        Ok(())
    }
}

impl DaemonClient {
    pub async fn exec_machine_stream(
        &mut self,
        open: runtime_v2::MachineExecOpen,
        metadata: runtime_v2::RequestMetadata,
    ) -> Result<MachineExecStream> {
        let project =
            ProjectId::new(open.project_id.clone()).map_err(|error| invalid(error.to_string()))?;
        for value in [&metadata.request_id, &metadata.idempotency_key] {
            if value.is_empty()
                || value.len() > 256
                || value.trim() != value
                || value.chars().any(char::is_control)
            {
                return Err(invalid(
                    "Machine Exec requires bounded stable request and idempotency IDs",
                ));
            }
        }
        let spec = vz_runtime_translate::machine_execution_spec_from_proto(
            open.spec
                .as_ref()
                .ok_or_else(|| invalid("Machine Exec spec required"))?,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let environment = if open.environment.is_some() {
            None
        } else {
            open.process_environment_id
                .as_ref()
                .map(|id| EnvironmentId::new(id.clone()))
                .transpose()
                .map_err(|error| invalid(error.to_string()))?
        };
        let machine = if open.machine.is_some() {
            None
        } else {
            open.process_machine_id
                .as_ref()
                .map(|id| MachineId::new(id.clone()))
                .transpose()
                .map_err(|error| invalid(error.to_string()))?
        };
        let (controls, outgoing) = mpsc::channel(16);
        controls
            .try_send(runtime_v2::MachineExecFrame {
                metadata: Some(metadata.clone()),
                sequence: 0,
                execution_id: String::new(),
                payload: Some(runtime_v2::machine_exec_frame::Payload::Open(open)),
            })
            .map_err(|_| invalid("cannot prepare Machine Exec Open frame"))?;
        let incoming = tokio::time::timeout(
            Duration::from_secs(35),
            self.topology_client.exec_machine(tonic::Request::new(
                tokio_stream::wrappers::ReceiverStream::new(outgoing),
            )),
        )
        .await
        .map_err(|_| invalid("Machine Exec admission observation exceeded 35 seconds"))?
        .map_err(|status| {
            crate::transport::status_to_client_error(&self.config.socket_path, status)
        })?
        .into_inner();
        let pty = spec.terminal.is_some();
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(spec.timeout_millis)
            + Duration::from_secs(30);
        Ok(MachineExecStream {
            incoming,
            controls,
            validation: Validator {
                spec,
                project,
                environment,
                machine,
                metadata,
                scope: None,
                sequence: 0,
                ready: false,
                terminal: false,
            },
            control_sequence: 1,
            stdin_closed: false,
            terminal_requested: false,
            pty,
            deadline,
        })
    }
}
