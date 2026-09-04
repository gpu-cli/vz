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
        let service_state = observed
            .iter()
            .find(|state| state.service_name == service_name);
        let container_id = service_state.and_then(|state| state.container_id.clone());
        let container_ownership =
            service_state.and_then(|state| state.failed_create_ownership.clone());

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopping {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
            },
        )?;

        // Stop and remove if we have a container.
        if let Some(ownership) = container_ownership.as_ref() {
            if ownership.stack_id != spec.name
                || container_id.as_deref() != Some(ownership.container_id.as_str())
                || ownership.validate().is_err()
            {
                let error = StackError::InvalidSpec(format!(
                    "invalid container ownership for service '{service_name}'"
                ));
                self.mark_failed_with_container_and_ownership(
                    spec,
                    service_name,
                    &error.to_string(),
                    container_id.as_deref(),
                    container_ownership,
                )?;
                return Err(error);
            }
            let svc_spec = spec
                .services
                .iter()
                .find(|service| service.name == service_name);
            let signal = svc_spec.and_then(|service| service.stop_signal.as_deref());
            let grace_period = svc_spec
                .and_then(|service| service.stop_grace_period_secs)
                .map(std::time::Duration::from_secs);
            let cleanup_result = self.runtime.stop_and_remove_container_generation(
                ownership.clone(),
                signal,
                grace_period,
            );
            if let Err(error) = cleanup_result {
                self.mark_failed_with_ownership(
                    spec,
                    service_name,
                    &error.to_string(),
                    container_ownership,
                )?;
                return Err(error);
            }
        } else if let Some(cid) = container_id.as_deref() {
            let error = StackError::InvalidSpec(format!(
                "container ownership is missing for service '{service_name}'; refusing ID-only cleanup"
            ));
            self.mark_failed_with_container(spec, service_name, &error.to_string(), Some(cid))?;
            return Err(error);
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
                failed_create_ownership: None,
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
                failed_create_ownership: None,
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

    /// Mark a failed lifecycle step while retaining exact runtime-issued cleanup
    /// authority. The token, rather than the container name, is what permits a
    /// later reconciliation pass to remove the generation.
    pub(super) fn mark_failed_with_ownership(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
        ownership: Option<vz_runtime_contract::ContainerGenerationOwnership>,
    ) -> Result<(), StackError> {
        let container_id = ownership
            .as_ref()
            .map(|ownership| ownership.container_id.clone());
        self.mark_failed_with_container_and_ownership(
            spec,
            service_name,
            error_msg,
            container_id.as_deref(),
            ownership,
        )
    }

    fn mark_failed_with_container_and_ownership(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
        container_id: Option<&str>,
        ownership: Option<vz_runtime_contract::ContainerGenerationOwnership>,
    ) -> Result<(), StackError> {
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Failed,
                container_id: container_id.map(str::to_string),
                failed_create_ownership: ownership,
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
