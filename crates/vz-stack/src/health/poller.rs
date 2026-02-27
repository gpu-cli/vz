use super::*;

fn resolve_healthcheck_command(test: &[String]) -> Vec<String> {
    match test.first().map(|s| s.as_str()) {
        Some("CMD") => test[1..].to_vec(),
        Some("CMD-SHELL") => {
            let shell_cmd = test[1..].join(" ");
            vec!["/bin/sh".to_string(), "-c".to_string(), shell_cmd]
        }
        _ => test.to_vec(),
    }
}

/// Default health check interval when not specified (30s).
const DEFAULT_INTERVAL_SECS: u64 = 30;
/// Default health check timeout when not specified (30s).
const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Default health check retries threshold when not specified.
const DEFAULT_RETRIES: u32 = 3;

/// Build a detailed health check error message including the command,
/// exit code, stdout/stderr output, and retry progress.
pub(super) fn build_health_check_error(
    cmd_display: &str,
    exit_code: i32,
    exec_error: Option<&str>,
    stdout: &str,
    stderr: &str,
    attempt: u32,
    retries: u32,
) -> String {
    let mut msg = String::new();

    // Command and exit status.
    if let Some(err) = exec_error {
        msg.push_str(&format!("{cmd_display} \u{2192} {err}"));
    } else {
        msg.push_str(&format!("{cmd_display} \u{2192} exit code {exit_code}"));
    }

    // Append stderr/stdout snippets if available.
    let stderr_trimmed = stderr.trim();
    let stdout_trimmed = stdout.trim();
    if !stderr_trimmed.is_empty() {
        // Truncate to last line to keep event payload manageable.
        let last_line = stderr_trimmed.lines().last().unwrap_or(stderr_trimmed);
        if last_line.len() > 120 {
            msg.push_str(&format!(" (stderr: {}...)", &last_line[..117]));
        } else {
            msg.push_str(&format!(" (stderr: {last_line})"));
        }
    } else if !stdout_trimmed.is_empty() {
        let last_line = stdout_trimmed.lines().last().unwrap_or(stdout_trimmed);
        if last_line.len() > 120 {
            msg.push_str(&format!(" (stdout: {}...)", &last_line[..117]));
        } else {
            msg.push_str(&format!(" (stdout: {last_line})"));
        }
    }

    // Retry progress.
    msg.push_str(&format!(" [{attempt}/{retries}]"));

    msg
}
impl HealthPoller {
    /// Create a new poller with no tracked state.
    pub fn new() -> Self {
        Self {
            statuses: HashMap::new(),
            start_times: HashMap::new(),
        }
    }

    /// Access the current health statuses (keyed by service name).
    pub fn statuses(&self) -> &HashMap<String, HealthStatus> {
        &self.statuses
    }

    /// Restore health poller state from the state store after a crash/restart.
    ///
    /// Rehydrates `statuses` and `start_times` from the persisted
    /// [`HealthPollState`](crate::state_store::HealthPollState) checkpoint
    /// so the poller can resume without losing debounce context.
    pub fn restore_from_store(
        &mut self,
        store: &StateStore,
        stack_name: &str,
    ) -> Result<(), StackError> {
        let persisted = store.load_health_poller_state(stack_name)?;
        for (service_name, poll_state) in persisted {
            let last_check = poll_state
                .last_check_millis
                .map(|ms| Instant::now() - Duration::from_millis(ms.unsigned_abs()));
            self.statuses.insert(
                service_name.clone(),
                HealthStatus {
                    service_name: poll_state.service_name,
                    consecutive_passes: poll_state.consecutive_passes,
                    consecutive_failures: poll_state.consecutive_failures,
                    last_check,
                },
            );
            if let Some(start_ms) = poll_state.start_time_millis {
                let start = Instant::now() - Duration::from_millis(start_ms.unsigned_abs());
                self.start_times.insert(service_name, start);
            }
        }
        Ok(())
    }

    /// Compute the minimum poll interval across all health-checked
    /// services in the spec, in seconds. Returns `None` if no
    /// services have health checks.
    pub fn min_interval(&self, spec: &StackSpec) -> Option<u64> {
        spec.services
            .iter()
            .filter_map(|s| s.healthcheck.as_ref())
            .map(|hc| hc.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS))
            .min()
    }

    /// Run one health check cycle for all running services with
    /// health checks.
    ///
    /// For each service that is Running and has a `HealthCheckSpec`:
    /// - Skips if still within the `start_period_secs` grace window.
    /// - Executes the health check command via `runtime.exec()`.
    /// - Records pass/fail in [`HealthStatus`].
    /// - On first pass: sets `observed.ready = true` and emits
    ///   [`StackEvent::HealthCheckPassed`].
    /// - On consecutive failures exceeding `retries`: marks service
    ///   as `Failed` and emits [`StackEvent::HealthCheckFailed`].
    pub fn poll_all<R: ContainerRuntime>(
        &mut self,
        runtime: &R,
        store: &StateStore,
        spec: &StackSpec,
    ) -> Result<HealthPollResult, StackError> {
        let observed = store.load_observed_state(&spec.name)?;
        let observed_map: HashMap<&str, &ServiceObservedState> = observed
            .iter()
            .map(|o| (o.service_name.as_str(), o))
            .collect();

        let mut result = HealthPollResult::default();
        let now = Instant::now();

        for svc in &spec.services {
            let Some(hc) = &svc.healthcheck else {
                continue;
            };

            let Some(obs) = observed_map.get(svc.name.as_str()) else {
                continue;
            };

            // Only check Running services.
            if obs.phase != ServicePhase::Running {
                continue;
            }

            let Some(ref container_id) = obs.container_id else {
                continue;
            };

            // Track when we first saw this service running.
            let start_time = *self.start_times.entry(svc.name.clone()).or_insert(now);

            // Respect start_period grace.
            let start_period = hc.start_period_secs.unwrap_or(0);
            let elapsed = now.duration_since(start_time).as_secs();
            if elapsed < start_period {
                debug!(
                    service = %svc.name,
                    remaining = start_period - elapsed,
                    "within start period grace, skipping health check"
                );
                continue;
            }

            // Respect the health check interval — skip if we checked
            // this service too recently.
            let interval = Duration::from_secs(hc.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS));
            {
                let status = self
                    .statuses
                    .entry(svc.name.clone())
                    .or_insert_with(|| HealthStatus::new(&svc.name));
                if let Some(last) = status.last_check {
                    if now.duration_since(last) < interval {
                        continue;
                    }
                }
            }

            // Execute health check command with timeout enforcement.
            // Docker convention: ["CMD", "arg1", ...] → exec directly,
            // ["CMD-SHELL", "cmd"] → exec through /bin/sh -c.
            let cmd = resolve_healthcheck_command(&hc.test);
            let timeout = Duration::from_secs(hc.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
            debug!(service = %svc.name, cmd = ?cmd, timeout_secs = timeout.as_secs(), "running health check");

            let exit_code = {
                let (tx, rx) = std::sync::mpsc::channel();
                let cid = container_id.clone();
                let cmd_clone = cmd.clone();
                std::thread::scope(|s| {
                    s.spawn(|| {
                        let result = runtime.exec_with_output(&cid, &cmd_clone);
                        let _ = tx.send(result);
                    });
                    match rx.recv_timeout(timeout) {
                        Ok(Ok((code, stdout, stderr))) => {
                            if code != 0 {
                                debug!(
                                    service = %svc.name,
                                    exit_code = code,
                                    cmd = ?cmd,
                                    stdout = %stdout,
                                    stderr = %stderr,
                                    "health check returned non-zero"
                                );
                            }
                            (code, None, stdout, stderr)
                        }
                        Ok(Err(e)) => {
                            debug!(service = %svc.name, error = %e, "health check exec failed");
                            (
                                1,
                                Some(format!("exec error: {e}")),
                                String::new(),
                                String::new(),
                            )
                        }
                        Err(_) => {
                            debug!(service = %svc.name, timeout_secs = timeout.as_secs(), "health check timed out");
                            (
                                1,
                                Some(format!("timed out after {}s", timeout.as_secs())),
                                String::new(),
                                String::new(),
                            )
                        }
                    }
                })
            };

            let status = self
                .statuses
                .entry(svc.name.clone())
                .or_insert_with(|| HealthStatus::new(&svc.name));
            status.last_check = Some(now);

            result.checks_run += 1;

            let (code, exec_error, hc_stdout, hc_stderr) = exit_code;
            if code == 0 {
                let was_ready = status.consecutive_passes >= 1;
                status.record_pass();

                if !was_ready {
                    // First pass — mark ready.
                    info!(service = %svc.name, "health check passed, service ready");
                    store.save_observed_state(
                        &spec.name,
                        &ServiceObservedState {
                            service_name: svc.name.clone(),
                            phase: ServicePhase::Running,
                            container_id: Some(container_id.clone()),
                            last_error: None,
                            ready: true,
                        },
                    )?;
                    store.emit_event(
                        &spec.name,
                        &StackEvent::HealthCheckPassed {
                            stack_name: spec.name.clone(),
                            service_name: svc.name.clone(),
                        },
                    )?;
                    result.newly_ready.push(svc.name.clone());
                }
            } else {
                status.record_failure();

                let retries = hc.retries.unwrap_or(DEFAULT_RETRIES);

                // Build detailed error message with command, exit code, and output.
                let cmd_display = cmd.join(" ");
                let error_msg = build_health_check_error(
                    &cmd_display,
                    code,
                    exec_error.as_deref(),
                    &hc_stdout,
                    &hc_stderr,
                    status.consecutive_failures,
                    retries,
                );

                // Emit event for every failure.
                store.emit_event(
                    &spec.name,
                    &StackEvent::HealthCheckFailed {
                        stack_name: spec.name.clone(),
                        service_name: svc.name.clone(),
                        attempt: status.consecutive_failures,
                        error: error_msg,
                    },
                )?;

                if status.consecutive_failures >= retries {
                    // Retries exhausted — mark unhealthy but keep running.
                    // Docker Compose semantics: container stays running, health
                    // checks continue indefinitely, and a future pass can
                    // promote the service back to healthy/ready.

                    // Read container output from VM-level log directory.
                    let log_output = match runtime.logs(container_id) {
                        Ok(logs) if !logs.output.is_empty() => {
                            let lines: Vec<&str> = logs.output.lines().rev().take(30).collect();
                            let lines: Vec<&str> = lines.into_iter().rev().collect();
                            lines.join("\n")
                        }
                        Ok(_) => "(no output captured)".to_string(),
                        Err(e) => format!("(logs error: {e})"),
                    };
                    info!(
                        service = %svc.name,
                        failures = status.consecutive_failures,
                        retries,
                        container_output = %log_output,
                        "health check retries exhausted, service unhealthy (will keep checking). \
                         Suggestions: ensure the service is listening on the configured port, \
                         verify the health check command is correct, check service logs with \
                         'vz stack logs <stack> -s {}'", svc.name
                    );
                    result.newly_failed.push(svc.name.clone());
                    // Reset counter so we keep polling on subsequent cycles.
                    status.consecutive_failures = 0;
                } else {
                    debug!(
                        service = %svc.name,
                        failures = status.consecutive_failures,
                        retries,
                        "health check failed, will retry"
                    );
                }
            }
        }

        Ok(result)
    }

    /// Clear tracked state for a service (e.g., when it is removed).
    pub fn clear(&mut self, service_name: &str) {
        self.statuses.remove(service_name);
        self.start_times.remove(service_name);
    }

    /// Clear all tracked state.
    pub fn clear_all(&mut self) {
        self.statuses.clear();
        self.start_times.clear();
    }
}
