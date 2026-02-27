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
        if let Some(ref cid) = container_id {
            info!(service = %service_name, container = %cid, "stopping container");
            if let Err(e) = self.runtime.stop(cid, stop_signal, stop_grace_period) {
                error!(service = %service_name, error = %e, "failed to stop container");
                // Continue with remove attempt.
            }

            info!(service = %service_name, container = %cid, "removing container");
            if let Err(e) = self.runtime.remove(cid) {
                error!(service = %service_name, error = %e, "failed to remove container");
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
        Ok(())
    }
    /// Mark a service as failed with an error message.
    pub(super) fn mark_failed(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
    ) -> Result<(), StackError> {
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Failed,
                container_id: None,
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
