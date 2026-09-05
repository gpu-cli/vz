use super::*;

#[allow(clippy::result_large_err)]
fn invalid(metadata: &vz_runtime_contract::RequestMetadata, message: impl Into<String>) -> Status {
    status_from_machine_error(MachineError::new(
        MachineErrorCode::ValidationError,
        message.into(),
        metadata.request_id.clone(),
        BTreeMap::new(),
    ))
}

pub(super) async fn handle(daemon:Arc<RuntimeDaemon>,request:Request<tonic::Streaming<runtime_v2::MachineExecFrame>>)->Result<Response<<TopologyServiceImpl as runtime_v2::topology_service_server::TopologyService>::ExecMachineStream>,Status>{
    let intercepted = request_id_from_extensions(&request);
    let mut stream = request.into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .map_err(|_| {
            Status::deadline_exceeded("Machine Exec requires an Open frame within five seconds")
        })??
        .ok_or_else(|| Status::invalid_argument("Machine Exec missing Open frame"))?;
    let metadata = normalize_metadata(first.metadata.as_ref(), intercepted);
    if first
        .metadata
        .as_ref()
        .is_none_or(|wire| wire.request_id.is_empty() || wire.idempotency_key.is_empty())
    {
        return Err(invalid(
            &metadata,
            "Machine Exec Open requires explicit stable request and idempotency IDs",
        ));
    }
    if first.sequence != 0 || !first.execution_id.is_empty() {
        return Err(invalid(
            &metadata,
            "Machine Exec Open requires sequence zero and empty execution ID",
        ));
    }
    let Some(runtime_v2::machine_exec_frame::Payload::Open(open)) = first.payload else {
        return Err(invalid(&metadata, "first Machine Exec frame must be Open"));
    };
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (daemon, open);
        Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            "Machine execution adapter is currently Linux-on-macOS only".into(),
            metadata.request_id,
            BTreeMap::new(),
        )))
    }
    #[cfg(target_os = "macos")]
    {
        use crate::machine_exec::*;
        use tokio_stream::StreamExt;
        use vz_runtime_contract::{
            EnvironmentId, EnvironmentSelectionContext, EnvironmentSelector, MachineId, ProjectId,
        };
        let project_id = ProjectId::new(open.project_id)
            .map_err(|error| invalid(&metadata, error.to_string()))?;
        let process_environment_id = if open.environment.is_some() {
            None
        } else {
            open.process_environment_id
                .map(EnvironmentId::new)
                .transpose()
                .map_err(|error| invalid(&metadata, error.to_string()))?
        };
        let process_machine_id = if open.machine.is_some() {
            None
        } else {
            open.process_machine_id
                .map(MachineId::new)
                .transpose()
                .map_err(|error| invalid(&metadata, error.to_string()))?
        };
        let spec = vz_runtime_translate::machine_execution_spec_from_proto(
            open.spec
                .as_ref()
                .ok_or_else(|| invalid(&metadata, "Machine Exec spec is required"))?,
        )
        .map_err(|error| invalid(&metadata, error.to_string()))?;
        let input = MachineExecInput {
            project_id,
            selection: EnvironmentSelectionContext {
                explicit: open.environment.map(EnvironmentSelector::NameOrId),
                process_environment_id,
                workspace_key: open.workspace_key,
            },
            machine: open.machine,
            process_machine_id,
            metadata: metadata.clone(),
            spec,
        };
        let (control_sender, control_receiver) = tokio::sync::mpsc::channel(16);
        let events = daemon
            .exec_machine(input, control_receiver)
            .await
            .map_err(status_from_machine_error)?;
        // The reader observes disconnect and invalid frames; dropping this
        // response does not abort the separately retained process supervisor.
        tokio::spawn(async move {
            loop {
                let next =
                    tokio::select! {()=control_sender.closed()=>break,next=stream.message()=>next};
                let frame = match next {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(_) => break,
                };
                let decoded = decode_control(frame, &metadata);
                let failed = decoded.is_err();
                if control_sender.send(decoded).await.is_err() || failed {
                    break;
                }
            }
        });
        #[allow(clippy::result_large_err)]
        let events = tokio_stream::wrappers::ReceiverStream::new(events).map(|event| {
            event
                .map(|event| runtime_v2::MachineExecEvent {
                    schema_version: 1,
                    scope: Some(vz_runtime_translate::machine_execution_scope_to_proto(
                        &event.scope,
                    )),
                    sequence: event.sequence,
                    replayed: event.replayed,
                    payload: Some(match event.payload {
                        MachineExecPayload::Ready => {
                            runtime_v2::machine_exec_event::Payload::Ready(true)
                        }
                        MachineExecPayload::Stdout(bytes) => {
                            runtime_v2::machine_exec_event::Payload::Stdout(bytes)
                        }
                        MachineExecPayload::Stderr(bytes) => {
                            runtime_v2::machine_exec_event::Payload::Stderr(bytes)
                        }
                        MachineExecPayload::Receipt(receipt) => {
                            runtime_v2::machine_exec_event::Payload::Receipt(
                                vz_runtime_translate::machine_execution_receipt_to_proto(&receipt),
                            )
                        }
                    }),
                })
                .map_err(status_from_machine_error)
        });
        Ok(Response::new(Box::pin(events)))
    }
}

#[cfg(target_os = "macos")]
fn decode_control(
    frame: runtime_v2::MachineExecFrame,
    original: &vz_runtime_contract::RequestMetadata,
) -> Result<crate::machine_exec::MachineExecControlFrame, MachineError> {
    use crate::machine_exec::{MachineExecControl, MachineExecControlFrame};
    let fail = |message: String| {
        MachineError::new(
            MachineErrorCode::ValidationError,
            message,
            original.request_id.clone(),
            BTreeMap::new(),
        )
    };
    let metadata = frame
        .metadata
        .ok_or_else(|| fail("control metadata is required".into()))?;
    let control = match frame.payload {
        Some(runtime_v2::machine_exec_frame::Payload::Stdin(bytes))
            if !bytes.is_empty() && bytes.len() <= 65536 =>
        {
            MachineExecControl::Stdin(bytes)
        }
        Some(runtime_v2::machine_exec_frame::Payload::StdinEof(true)) => {
            MachineExecControl::StdinEof
        }
        Some(runtime_v2::machine_exec_frame::Payload::Signal(signal)) => {
            MachineExecControl::Signal(signal)
        }
        Some(runtime_v2::machine_exec_frame::Payload::Resize(terminal)) => {
            MachineExecControl::Resize(
                vz_runtime_translate::machine_execution_terminal_from_proto(&terminal)
                    .map_err(|error| fail(error.to_string()))?,
            )
        }
        Some(runtime_v2::machine_exec_frame::Payload::Cancel(true)) => MachineExecControl::Cancel,
        _ => return Err(fail("invalid Machine execution control payload".into())),
    };
    Ok(MachineExecControlFrame {
        request_id: metadata.request_id,
        idempotency_key: metadata.idempotency_key,
        execution_id: frame.execution_id,
        sequence: frame.sequence,
        control,
    })
}
