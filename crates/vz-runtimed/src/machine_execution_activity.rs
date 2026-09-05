//! Runtime-owned execution cancellation and positive terminal-proof draining.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

use crate::machine_runtime_activation::MachineRuntimeActivation;

#[derive(Default)]
pub(crate) struct MachineExecutionActivities {
    state: Mutex<Book>,
}

#[derive(Default)]
struct Book {
    closing: bool,
    active: HashMap<String, Arc<MachineExecutionActivity>>,
}

pub(crate) struct MachineExecutionActivity {
    cancel: watch::Sender<bool>,
    result: watch::Sender<Option<Result<(), String>>>,
    // A bounded failure is not evidence that the guest process was reaped.
    // Keep its original boot/store reader until explicit recovery establishes it.
    uncertain_activation: Mutex<Option<Arc<MachineRuntimeActivation>>>,
}

impl MachineExecutionActivities {
    pub(crate) fn register(
        &self,
        execution_id: &str,
    ) -> Result<Arc<MachineExecutionActivity>, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "execution activity registry poisoned")?;
        if state.closing {
            return Err("Machine is stopping; new execution admission is closed".into());
        }
        state
            .active
            .retain(|_, activity| !matches!(&*activity.result.borrow(), Some(Ok(()))));
        if state.active.len() >= 128 || state.active.contains_key(execution_id) {
            return Err(
                "Machine execution capacity exceeded or execution already registered".into(),
            );
        }
        let activity = Arc::new(MachineExecutionActivity {
            cancel: watch::channel(false).0,
            result: watch::channel(None).0,
            uncertain_activation: Mutex::new(None),
        });
        state
            .active
            .insert(execution_id.into(), Arc::clone(&activity));
        Ok(activity)
    }

    /// Stop closes admission first, signals every sibling process, then awaits
    /// positive terminal proof. The enclosing Stop owns the bounded deadline.
    pub(crate) async fn cancel_and_drain(&self) -> Result<(), String> {
        let activities = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "execution activity registry poisoned")?;
            state.closing = true;
            state.active.values().cloned().collect::<Vec<_>>()
        };
        for activity in &activities {
            activity.cancel();
        }
        for activity in activities {
            activity.terminal().await?;
        }
        Ok(())
    }
}

impl MachineExecutionActivity {
    pub(crate) fn cancellation(&self) -> watch::Receiver<bool> {
        self.cancel.subscribe()
    }
    pub(crate) fn cancel(&self) {
        self.cancel.send_replace(true);
    }

    /// Caller must first drop its execution reader after positive guest proof.
    pub(crate) fn complete(&self) {
        self.result.send_replace(Some(Ok(())));
    }

    pub(crate) fn retain_uncertain(
        &self,
        activation: Arc<MachineRuntimeActivation>,
        reason: String,
    ) {
        // Even a poisoned bookkeeping lock must retain the uncertain reader.
        *self
            .uncertain_activation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(activation);
        self.result.send_replace(Some(Err(reason)));
    }

    async fn terminal(&self) -> Result<(), String> {
        let mut result = self.result.subscribe();
        loop {
            if let Some(result) = result.borrow().clone() {
                return result;
            }
            result
                .changed()
                .await
                .map_err(|_| "execution lost terminal proof")?;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn stop_signals_all_executions_before_waiting_and_requires_each_receipt() {
        let book = Arc::new(MachineExecutionActivities::default());
        let first = book.register("first").unwrap();
        let second = book.register("second").unwrap();
        let worker = Arc::clone(&book);
        let drain = tokio::spawn(async move { worker.cancel_and_drain().await });
        let mut cancelled = second.cancellation();
        tokio::time::timeout(Duration::from_secs(1), cancelled.wait_for(|flag| *flag))
            .await
            .unwrap()
            .unwrap();
        assert!(*first.cancellation().borrow());
        assert!(book.register("late").is_err());
        first.complete();
        assert!(!drain.is_finished());
        second.complete();
        drain.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn uncertain_execution_cannot_be_mistaken_for_clean_shutdown() {
        let book = MachineExecutionActivities::default();
        let activity = book.register("uncertain").unwrap();
        activity
            .result
            .send_replace(Some(Err("guest reap unproven".into())));
        assert!(
            book.cancel_and_drain()
                .await
                .unwrap_err()
                .contains("unproven")
        );
    }

    #[test]
    fn capacity_is_bounded_and_completed_entries_can_be_reclaimed() {
        let book = MachineExecutionActivities::default();
        for index in 0..128 {
            book.register(&index.to_string()).unwrap();
        }
        assert!(book.register("overflow").is_err());
        book.state.lock().unwrap().active["0"].complete();
        assert!(book.register("next").is_ok());
    }
}
