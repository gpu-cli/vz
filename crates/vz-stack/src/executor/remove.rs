use super::*;

impl<R: ContainerRuntime> StackExecutor<R> {
    /// Execute a service removal: stop + remove container, release ports, update state.
    pub(super) fn execute_remove(
        &mut self,
        spec: &StackSpec,
        service_name: &str,
    ) -> Result<(), StackError> {
        // Find current container_id from observed state.
        let observed = self.store.load_observed_state(&spec.name)?;
        let container_id = observed
            .iter()
            .find(|o| o.service_name == service_name)
            .and_then(|o| o.container_id.clone());

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopping {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
            },
        )?;

        // Look up stop_signal and stop_grace_period from the service spec.
        let svc_spec = spec.services.iter().find(|s| s.name == service_name);
        let stop_signal = svc_spec.and_then(|s| s.stop_signal.as_deref());
        let stop_grace_period = svc_spec
            .and_then(|s| s.stop_grace_period_secs)
            .map(std::time::Duration::from_secs);

        // Stop and remove if we have a container.
        let mut stop_error = None;
        if let Some(ref cid) = container_id {
            info!(service = %service_name, container = %cid, "stopping container");
            if let Err(e) = self.runtime.stop(cid, stop_signal, stop_grace_period) {
                error!(service = %service_name, error = %e, "VZ_STACK_TEARDOWN_VIOLATION:STOP_FAILED failed to stop container");
                // Continue with remove so a stopped/absent runtime object can
                // still be cleaned, but retain the error: a teardown must not
                // report success after any lifecycle operation failed.
                stop_error = Some(e);
            }

            info!(service = %service_name, container = %cid, "removing container");
            match self.runtime.remove(cid) {
                Ok(()) => {}
                Err(e) if e.machine_code() == vz_runtime_contract::MachineErrorCode::NotFound => {
                    info!(service = %service_name, container = %cid, "container already absent; treating remove as complete");
                }
                Err(e) => {
                    error!(service = %service_name, error = %e, "VZ_STACK_TEARDOWN_VIOLATION:REMOVE_FAILED failed to remove container");
                    let cleanup_message = match stop_error.as_ref() {
                        Some(stop_error) => format!(
                            "container cleanup failed after both lifecycle operations: stop: {stop_error}; remove: {e}"
                        ),
                        None => format!("container cleanup failed: {e}"),
                    };
                    let cleanup_error = match stop_error.as_ref() {
                        Some(_) => StackError::Network(cleanup_message.clone()),
                        None => e,
                    };
                    self.mark_failed_with_container(
                        spec,
                        service_name,
                        &cleanup_message,
                        Some(cid),
                    )?;
                    return Err(cleanup_error);
                }
            }
        }

        // Release allocated ports.
        self.ports.release(service_name);

        // Update state to Stopped.
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Stopped,
                container_id: None,
                last_error: None,
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopped {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                exit_code: 0,
            },
        )?;

        info!(service = %service_name, "service stopped");
        match stop_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    /// Mark a service as failed with an error message.
    pub(super) fn mark_failed(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
    ) -> Result<(), StackError> {
        self.mark_failed_with_container(spec, service_name, error_msg, None)
    }

    /// Mark a service as failed while retaining a runtime container identifier
    /// when cleanup must be retried before the next deterministic-ID create.
    pub(super) fn mark_failed_with_container(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
        container_id: Option<&str>,
    ) -> Result<(), StackError> {
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Failed,
                container_id: container_id.map(str::to_string),
                last_error: Some(error_msg.to_string()),
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceFailed {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                error: error_msg.to_string(),
            },
        )?;

        Ok(())
    }
}
