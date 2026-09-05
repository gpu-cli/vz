//! Every admitted process either produces terminal proof or keeps its VM reader.
use super::*;
use tokio::time::{Instant, timeout};
use vz_linux::{
    ContainerExecDispatchGate, ContainerExecStartError, ExecOptions, MachineExecOutputEvent,
};

const CONTROL_WAIT: Duration = Duration::from_secs(5);
const REAP_WAIT: Duration = Duration::from_secs(10);

/// Panic/early-return safety: loss of the supervisor cannot release uncertainty.
struct Ownership {
    daemon: Arc<RuntimeDaemon>,
    activation: Option<Arc<MachineRuntimeActivation>>,
    activity: Arc<MachineExecutionActivity>,
    receipt: MachineExecutionReceipt,
}
impl Drop for Ownership {
    fn drop(&mut self) {
        if let Some(activation) = self.activation.take() {
            let reason = "execution supervisor ended without positive terminal proof".to_string();
            self.activity.retain_uncertain(activation, reason.clone());
            self.receipt.state = MachineExecutionState::Uncertain;
            self.receipt.failure = Some(reason);
            self.receipt.exit_code = None;
            self.receipt.updated_at = crate::current_unix_secs().max(self.receipt.updated_at);
            if let Err(error) = self.daemon.with_state_store(|store| {
                store.finish_machine_execution(&self.receipt.scope, &self.receipt)
            }) {
                tracing::error!(%error,execution_id=%self.receipt.scope.execution_id,"unable to persist lost execution supervisor; original activation retained");
            }
        }
    }
}

enum Proof {
    Quiesced,
    Reaped(i32),
    Uncertain,
}

async fn reconcile(activation: &MachineRuntimeActivation, ticket: &str) -> Proof {
    use vz_linux::reconcile_exec_response::Outcome;
    match timeout(
        REAP_WAIT,
        activation
            .execution_lease()
            .reconcile_machine_exec(ticket.into()),
    )
    .await
    {
        Ok(Ok(response)) if response.exec_request_id == ticket => {
            match Outcome::try_from(response.outcome) {
                Ok(Outcome::FencedNeverStarted) if response.exec_id == 0 => Proof::Quiesced,
                Ok(Outcome::TerminalReaped)
                    if response.exec_id > 0 && (0..=255).contains(&response.exit_code) =>
                {
                    Proof::Reaped(response.exit_code)
                }
                _ => Proof::Uncertain,
            }
        }
        _ => Proof::Uncertain,
    }
}

async fn emit(
    sender: &mpsc::Sender<Result<MachineExecEvent, MachineError>>,
    scope: &MachineExecutionScope,
    sequence: &mut u64,
    payload: MachineExecPayload,
) -> Result<(), String> {
    let event = MachineExecEvent {
        scope: scope.clone(),
        sequence: *sequence,
        replayed: false,
        payload,
    };
    timeout(CONTROL_WAIT, sender.send(Ok(event)))
        .await
        .map_err(|_| "execution observer backpressure exceeded five seconds".to_string())?
        .map_err(|_| "execution observer disconnected".to_string())?;
    *sequence += 1;
    Ok(())
}

async fn emit_live(
    sender: &mpsc::Sender<Result<MachineExecEvent, MachineError>>,
    scope: &MachineExecutionScope,
    sequence: &mut u64,
    payload: MachineExecPayload,
    deadline: Instant,
    cancellation: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    tokio::select! {
        biased;
        _=async {let _=cancellation.wait_for(|cancel|*cancel).await;}=>Err("Machine Stop cancelled execution output".into()),
        ()=tokio::time::sleep_until(deadline)=>Err("execution deadline expired during output".into()),
        result=emit(sender,scope,sequence,payload)=>result,
    }
}

fn bounded_diagnostic(reason: String) -> String {
    reason.chars().take(2048).collect()
}

fn validate_control(
    scope: &MachineExecutionScope,
    frame: &MachineExecControlFrame,
    sequence: u64,
    stdin_closed: bool,
    pty: bool,
) -> Result<(), String> {
    if frame.request_id != scope.request_id
        || frame.idempotency_key != scope.idempotency_key
        || frame.execution_id != scope.execution_id
        || frame.sequence != sequence
    {
        return Err("Machine execution control correlation or sequence mismatch".into());
    }
    match &frame.control {
        MachineExecControl::Stdin(bytes)
            if stdin_closed || bytes.is_empty() || bytes.len() > 65536 =>
        {
            Err("invalid bounded stdin frame or stdin already closed".into())
        }
        MachineExecControl::StdinEof if stdin_closed => Err("duplicate stdin EOF".into()),
        MachineExecControl::Signal(signal) if !(1..=64).contains(signal) => {
            Err("invalid guest signal".into())
        }
        MachineExecControl::Resize(terminal)
            if !pty || terminal.rows == 0 || terminal.columns == 0 =>
        {
            Err("invalid terminal resize".into())
        }
        _ => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drive(
    daemon: Arc<RuntimeDaemon>,
    input: MachineExecInput,
    receipt: MachineExecutionReceipt,
    activation: Arc<MachineRuntimeActivation>,
    activity: Arc<MachineExecutionActivity>,
    mut controls: mpsc::Receiver<Result<MachineExecControlFrame, MachineError>>,
    sender: mpsc::Sender<Result<MachineExecEvent, MachineError>>,
) {
    let execution_activation = Arc::clone(&activation);
    let mut owner = Ownership {
        daemon,
        activation: Some(activation),
        activity,
        receipt,
    };
    // The local clone is always dropped before publishing the activity's proof.
    let activation = execution_activation;
    let mut cancellation = owner.activity.cancellation();
    let deadline = Instant::now() + Duration::from_millis(input.spec.timeout_millis);
    let mut sequence = 0;
    let (proof, reason) = run(
        &input,
        &owner.receipt.scope,
        &activation,
        &mut cancellation,
        deadline,
        &mut controls,
        &sender,
        &mut sequence,
    )
    .await;
    drop(activation);
    owner.receipt.updated_at = crate::current_unix_secs().max(owner.receipt.created_at);
    owner.receipt.failure = reason.map(bounded_diagnostic);
    match proof {
        Proof::Quiesced => {
            owner.receipt.state = MachineExecutionState::Quiesced;
            owner.receipt.failure.get_or_insert_with(|| {
                "guest confirmed no remaining live work; exit history is unavailable".into()
            });
        }
        Proof::Reaped(code) => {
            owner.receipt.state = MachineExecutionState::Completed;
            owner.receipt.exit_code = Some(code);
        }
        Proof::Uncertain => {
            owner.receipt.state = MachineExecutionState::Uncertain;
            owner.receipt.failure.get_or_insert_with(|| {
                "guest terminal proof unavailable; original activation retained".into()
            });
        }
    }
    // Keep the exact process receipt durable before claiming observation success.
    let saved = owner.daemon.with_state_store(|store| {
        store.finish_machine_execution(&owner.receipt.scope, &owner.receipt)
    });
    if let Some(activation) = owner.activation.take() {
        if matches!(proof, Proof::Uncertain) {
            owner.activity.retain_uncertain(
                activation,
                owner.receipt.failure.clone().unwrap_or_default(),
            );
        } else {
            drop(activation);
            owner.activity.complete();
        }
    }
    match saved {
        Ok(()) => {
            let _ = emit(
                &sender,
                &owner.receipt.scope,
                &mut sequence,
                MachineExecPayload::Receipt(Box::new(owner.receipt.clone())),
            )
            .await;
        }
        Err(error) => {
            tracing::error!(%error,execution_id=%owner.receipt.scope.execution_id,"guest proof could not be persisted; durable admission remains non-retryable");
            let _ = timeout(CONTROL_WAIT, sender.send(Err(state_error(&input, error)))).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    input: &MachineExecInput,
    scope: &MachineExecutionScope,
    activation: &MachineRuntimeActivation,
    cancellation: &mut tokio::sync::watch::Receiver<bool>,
    deadline: Instant,
    controls: &mut mpsc::Receiver<Result<MachineExecControlFrame, MachineError>>,
    sender: &mpsc::Sender<Result<MachineExecEvent, MachineError>>,
    sequence: &mut u64,
) -> (Proof, Option<String>) {
    if *cancellation.borrow() || sender.is_closed() {
        return (
            Proof::Quiesced,
            Some("execution cancelled before ticket allocation".into()),
        );
    }
    let ticket = match timeout(
        CONTROL_WAIT.min(deadline.saturating_duration_since(Instant::now())),
        activation.execution_lease().prepare_machine_exec_request(),
    )
    .await
    {
        Ok(Ok(ticket)) => ticket,
        Ok(Err(error)) => return (Proof::Quiesced, Some(error.to_string())),
        Err(_) => {
            return (
                Proof::Quiesced,
                Some("guest ticket allocation timed out before dispatch".into()),
            );
        }
    };
    let gate = ContainerExecDispatchGate::new(deadline);
    let start = activation.execution_lease().start_machine_exec(
        gate.clone(),
        ticket.clone(),
        input.spec.argv[0].clone(),
        input.spec.argv[1..].to_vec(),
        ExecOptions {
            working_dir: input.spec.working_directory.clone(),
            env: input.spec.environment.clone().into_iter().collect(),
            user: input.spec.user.clone(),
        },
        input
            .spec
            .terminal
            .map(|terminal| (terminal.rows.into(), terminal.columns.into())),
    );
    let started = {
        tokio::pin!(start);
        tokio::select! {
            result=&mut start=>Some(result),
            _=async {let _=cancellation.wait_for(|cancel|*cancel).await;}=>None,
            ()=sender.closed()=>None,
            _=controls.recv()=>None,
            ()=tokio::time::sleep_until(deadline)=>None,
        }
    };
    let (mut stream, id) = match started {
        Some(Ok(started)) => started,
        Some(Err(ContainerExecStartError::Definite(error))) => {
            return (Proof::Quiesced, Some(error.to_string()));
        }
        Some(Err(ContainerExecStartError::Ambiguous(error))) => {
            return (
                reconcile(activation, &ticket).await,
                Some(format!("ambiguous start: {error}")),
            );
        }
        None => {
            let proof = if gate.cancel_before_dispatch() {
                Proof::Quiesced
            } else {
                reconcile(activation, &ticket).await
            };
            return (proof,Some("execution interrupted before readiness; exact ticket reconciled when dispatched".into()));
        }
    };
    let mut stdin_closed = false;
    let mut control_sequence = 1;
    let mut reason = emit_live(
        sender,
        scope,
        sequence,
        MachineExecPayload::Ready,
        deadline,
        cancellation,
    )
    .await
    .err();
    while reason.is_none() {
        tokio::select! {
            _=async {let _=cancellation.wait_for(|cancel|*cancel).await;}=>reason=Some("Machine Stop cancelled the execution".into()),
            ()=sender.closed()=>reason=Some("execution observer disconnected".into()),
            ()=tokio::time::sleep_until(deadline)=>reason=Some("execution deadline expired".into()),
            next=controls.recv()=>{
                let frame=match next {Some(Ok(frame))=>frame,Some(Err(error))=>{reason=Some(format!("invalid execution control stream: {}",error.message));continue;},None=>{reason=Some("execution control stream disconnected".into());continue;}};
                if let Err(error)=validate_control(scope,&frame,control_sequence,stdin_closed,input.spec.terminal.is_some()){reason=Some(error);continue;}
                control_sequence+=1;
                let lease=activation.execution_lease();
                let result=timeout(CONTROL_WAIT.min(deadline.saturating_duration_since(Instant::now())),async {
                    match frame.control {
                        MachineExecControl::Stdin(bytes)=>lease.machine_exec_stdin(id,&bytes).await,
                        MachineExecControl::StdinEof=>{stdin_closed=true;lease.machine_exec_stdin_close(id).await},
                        MachineExecControl::Signal(signal)=>lease.machine_exec_signal(id,signal).await,
                        MachineExecControl::Resize(terminal)=>lease.machine_exec_resize(id,terminal.rows.into(),terminal.columns.into()).await,
                        MachineExecControl::Cancel=>{reason=Some("client requested execution cancellation".into());Ok(())},
                    }
                }).await;
                match result {Ok(Ok(()))=>{},Ok(Err(error))=>reason=Some(format!("guest control failed: {error}")),Err(_)=>reason=Some("guest control timed out".into())}
            },
            event=stream.next_checked()=>match event {
                Ok(Some(MachineExecOutputEvent::Exit(code))) if (0..=255).contains(&code)=>return (Proof::Reaped(code),None),
                Ok(Some(MachineExecOutputEvent::Stdout(bytes)))=>{for chunk in bytes.chunks(65536){if let Err(error)=emit_live(sender,scope,sequence,MachineExecPayload::Stdout(chunk.to_vec()),deadline,cancellation).await{reason=Some(error);break;}}},
                Ok(Some(MachineExecOutputEvent::Stderr(bytes)))=>{for chunk in bytes.chunks(65536){if let Err(error)=emit_live(sender,scope,sequence,MachineExecPayload::Stderr(chunk.to_vec()),deadline,cancellation).await{reason=Some(error);break;}}},
                Ok(_)=>reason=Some("guest stream ended without a valid terminal exit".into()),
                Err(error)=>reason=Some(format!("guest stream failed: {error}")),
            },
        }
    }
    // Dropping observation is never itself terminal proof. Cancellation targets
    // the exact addressable guest process, then reconciliation fences its ticket.
    let proof = match timeout(
        REAP_WAIT,
        activation.execution_lease().cancel_machine_exec(id),
    )
    .await
    {
        Ok(Ok(code)) if (0..=255).contains(&code) => Proof::Reaped(code),
        _ => reconcile(activation, &ticket).await,
    };
    drop(stream);
    (proof, reason)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn multibyte_diagnostics_stay_within_receipt_byte_limit() {
        let bounded = bounded_diagnostic("🦀".repeat(10_000));
        assert_eq!(bounded.len(), 8192);
        let (_, _, scope) = super::super::tests::fixture();
        MachineExecutionReceipt {
            scope,
            state: MachineExecutionState::Quiesced,
            exit_code: None,
            failure: Some(bounded),
            output_replay_available: false,
            created_at: 1,
            updated_at: 2,
        }
        .validate()
        .unwrap();
    }

    #[tokio::test]
    async fn blocked_multi_chunk_output_observes_stop_without_per_chunk_delay() {
        let (_, _, scope) = super::super::tests::fixture();
        let (sender, _receiver) = mpsc::channel(1);
        let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
        let mut sequence = 0;
        let deadline = Instant::now() + Duration::from_secs(10);
        emit_live(
            &sender,
            &scope,
            &mut sequence,
            MachineExecPayload::Stdout(vec![1; 65536]),
            deadline,
            &mut cancelled,
        )
        .await
        .unwrap();
        let signal = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            cancel.send_replace(true);
        });
        let result = timeout(
            Duration::from_secs(1),
            emit_live(
                &sender,
                &scope,
                &mut sequence,
                MachineExecPayload::Stdout(vec![2; 65536]),
                deadline,
                &mut cancelled,
            ),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().contains("Stop"));
        assert_eq!(sequence, 1);
        signal.await.unwrap();
    }

    #[tokio::test]
    async fn blocked_output_obeys_absolute_execution_deadline() {
        let (_, _, scope) = super::super::tests::fixture();
        let (sender, _receiver) = mpsc::channel(1);
        let (_cancel, mut cancelled) = tokio::sync::watch::channel(false);
        let mut sequence = 0;
        emit(
            &sender,
            &scope,
            &mut sequence,
            MachineExecPayload::Stdout(vec![1]),
        )
        .await
        .unwrap();
        let result = timeout(
            Duration::from_secs(1),
            emit_live(
                &sender,
                &scope,
                &mut sequence,
                MachineExecPayload::Stdout(vec![2]),
                Instant::now() + Duration::from_millis(20),
                &mut cancelled,
            ),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().contains("deadline"));
        assert_eq!(sequence, 1);
    }

    #[test]
    fn controls_require_exact_scope_sequence_and_stdin_terminal_state() {
        let (_, _, scope) = super::super::tests::fixture();
        let valid = MachineExecControlFrame {
            request_id: scope.request_id.clone(),
            idempotency_key: scope.idempotency_key.clone(),
            execution_id: scope.execution_id.clone(),
            sequence: 1,
            control: MachineExecControl::Stdin(vec![0, 255]),
        };
        validate_control(&scope, &valid, 1, false, false).unwrap();
        for mutation in 0..6 {
            let mut frame = valid.clone();
            match mutation {
                0 => frame.request_id.push('x'),
                1 => frame.idempotency_key.push('x'),
                2 => frame.execution_id.push('x'),
                3 => frame.sequence += 1,
                4 => frame.control = MachineExecControl::Signal(0),
                _ => {
                    frame.control = MachineExecControl::Resize(MachineExecutionTerminal {
                        rows: 24,
                        columns: 80,
                    })
                }
            };
            assert!(validate_control(&scope, &frame, 1, false, false).is_err());
        }
        assert!(validate_control(&scope, &valid, 1, true, false).is_err());
    }
}
