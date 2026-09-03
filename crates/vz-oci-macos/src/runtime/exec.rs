use super::networking::ensure_interactive_exec_pty_prerequisites;
use super::oci_lifecycle::parse_signal_number;
use super::*;
use tracing::debug;

fn exec_control_debug_enabled() -> bool {
    std::env::var("VZ_OCI_EXEC_CONTROL_DEBUG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

impl Runtime {
    pub async fn exec_container(&self, id: &str, exec: ExecConfig) -> Result<ExecOutput, OciError> {
        self.exec_container_streaming(id, exec, |_| {}).await
    }

    /// Execute a command inside an already-running container and emit
    /// incremental output events when available.
    pub async fn exec_container_streaming<F>(
        &self,
        id: &str,
        exec: ExecConfig,
        mut on_event: F,
    ) -> Result<ExecOutput, OciError>
    where
        F: FnMut(InteractiveExecEvent),
    {
        let debug = exec_control_debug_enabled();
        let vm = self
            .vm_handles
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no active VM handle for container '{id}'; container may not be running"
                ))
            })?;

        let (command, args) = exec
            .cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("exec command must not be empty".to_string()))?;

        let timeout = exec.timeout.unwrap_or(self.config.exec_timeout);
        let execution_id = exec.execution_id.clone();
        let mut merged_env = self
            .container_exec_env
            .lock()
            .await
            .get(id)
            .cloned()
            .unwrap_or_default();
        for (key, value) in &exec.env {
            if let Some((_, existing_value)) = merged_env.iter_mut().find(|(k, _)| k == key) {
                *existing_value = value.clone();
            } else {
                merged_env.push((key.clone(), value.clone()));
            }
        }

        if exec.pty {
            let Some(execution_id) = execution_id else {
                return Err(OciError::ExecutionControlUnsupported {
                    operation: "exec_container".to_string(),
                    reason: "interactive exec requires execution_id".to_string(),
                });
            };

            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            let term_rows = u32::from(exec.term_rows.unwrap_or(DEFAULT_INTERACTIVE_EXEC_ROWS));
            let term_cols = u32::from(exec.term_cols.unwrap_or(DEFAULT_INTERACTIVE_EXEC_COLS));
            let options = ExecOptions {
                working_dir: exec.working_dir.clone(),
                env: merged_env,
                user: exec.user.clone(),
            };

            let vm_key = Arc::as_ptr(&vm) as usize;
            let should_prepare_pty = {
                let mut prepared = self.interactive_pty_prep_vms.lock().await;
                prepared.insert(vm_key)
            };

            if should_prepare_pty {
                if debug {
                    debug!(
                        "[vz-oci-macos exec-control] interactive exec preparing pty prerequisites execution_id={execution_id} timeout_secs={:.3}",
                        timeout.as_secs_f64()
                    );
                }
                ensure_interactive_exec_pty_prerequisites(vm.as_ref(), timeout).await;
                if debug {
                    debug!(
                        "[vz-oci-macos exec-control] interactive exec prerequisite step complete execution_id={execution_id}"
                    );
                }
            } else if debug {
                debug!(
                    "[vz-oci-macos exec-control] interactive exec skipping pty prerequisite step execution_id={execution_id}"
                );
            }

            if debug {
                debug!(
                    "[vz-oci-macos exec-control] interactive exec invoking guest exec RPC execution_id={execution_id} command={:?} args={:?} rows={} cols={}",
                    command, args, term_rows, term_cols
                );
            }
            let (mut stream, guest_exec_id) = tokio::time::timeout(
                timeout,
                vm.exec_container_interactive(
                    id.to_string(),
                    command,
                    &arg_refs,
                    options,
                    term_rows,
                    term_cols,
                ),
            )
            .await
            .map_err(|_| {
                OciError::InvalidConfig(format!(
                    "exec timed out after {:.3}s",
                    timeout.as_secs_f64()
                ))
            })??;
            if debug {
                debug!(
                    "[vz-oci-macos exec-control] interactive exec guest exec RPC ready execution_id={execution_id} guest_exec_id={guest_exec_id}"
                );
            }

            self.exec_sessions.lock().await.insert(
                execution_id.clone(),
                InteractiveExecSession {
                    vm: Arc::clone(&vm),
                    guest_exec_id,
                    pty_enabled: true,
                },
            );

            let stream_result = tokio::time::timeout(timeout, async {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                let mut saw_exit = false;
                let mut exit_code = -1;

                while let Some(event) = stream.next().await {
                    match event {
                        ExecEvent::Stdout(data) => {
                            on_event(InteractiveExecEvent::Stdout(data.clone()));
                            stdout.extend_from_slice(&data);
                        }
                        ExecEvent::Stderr(data) => {
                            on_event(InteractiveExecEvent::Stderr(data.clone()));
                            stderr.extend_from_slice(&data);
                        }
                        ExecEvent::Exit(code) => {
                            on_event(InteractiveExecEvent::Exit(code));
                            saw_exit = true;
                            exit_code = code;
                            break;
                        }
                    }
                }

                if !saw_exit {
                    return Err(OciError::InvalidConfig(
                        "interactive exec stream ended without exit code".to_string(),
                    ));
                }

                Ok(ExecOutput {
                    exit_code,
                    stdout: String::from_utf8_lossy(&stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&stderr).into_owned(),
                })
            })
            .await;

            let result = match stream_result {
                Ok(result) => result,
                Err(_) => {
                    let _ = vm.signal(guest_exec_id, 15).await;
                    Err(OciError::InvalidConfig(format!(
                        "exec timed out after {:.3}s",
                        timeout.as_secs_f64()
                    )))
                }
            };

            self.exec_sessions.lock().await.remove(&execution_id);
            return result;
        }

        // Always set a container cwd so execution never inherits the guest
        // agent's cwd from outside the target mount namespace.
        let wd = exec
            .working_dir
            .filter(|directory| !directory.is_empty())
            .unwrap_or_else(|| "/".to_string());
        let options = ExecOptions {
            working_dir: Some(wd),
            env: merged_env,
            user: exec.user.clone(),
        };

        tokio::time::timeout(timeout, async {
            let mut stream = vm
                .exec_container_stream_with_options(
                    id.to_string(),
                    command.clone(),
                    args.to_vec(),
                    options,
                )
                .await?;
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let mut saw_exit = false;
            let mut exit_code = -1;
            while let Some(event) = stream.next().await {
                match event {
                    ExecEvent::Stdout(data) => {
                        on_event(InteractiveExecEvent::Stdout(data.clone()));
                        stdout.extend_from_slice(&data);
                    }
                    ExecEvent::Stderr(data) => {
                        on_event(InteractiveExecEvent::Stderr(data.clone()));
                        stderr.extend_from_slice(&data);
                    }
                    ExecEvent::Exit(code) => {
                        on_event(InteractiveExecEvent::Exit(code));
                        saw_exit = true;
                        exit_code = code;
                        break;
                    }
                }
            }
            if !saw_exit {
                return Err(OciError::InvalidConfig(
                    "container exec stream ended without exit code".to_string(),
                ));
            }
            Ok(ExecOutput {
                exit_code,
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            })
        })
        .await
        .map_err(|_| {
            OciError::InvalidConfig(format!(
                "exec timed out after {:.3}s",
                timeout.as_secs_f64()
            ))
        })?
    }

    /// Execute through the bounded OCI unary compatibility RPC.
    #[doc(hidden)]
    pub async fn exec_container_oci_unary(
        &self,
        id: &str,
        exec: ExecConfig,
    ) -> Result<ExecOutput, OciError> {
        if exec.pty {
            return Err(OciError::ExecutionControlUnsupported {
                operation: "exec_container_oci_unary".to_string(),
                reason: "OCI unary exec does not support PTY allocation".to_string(),
            });
        }
        let vm = self
            .vm_handles
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no active VM handle for container '{id}'; container may not be running"
                ))
            })?;
        let (command, args) = exec
            .cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("exec command must not be empty".to_string()))?;
        let timeout = exec.timeout.unwrap_or(self.config.exec_timeout);
        let mut merged_env = self
            .container_exec_env
            .lock()
            .await
            .get(id)
            .cloned()
            .unwrap_or_default();
        for (key, value) in exec.env {
            if let Some((_, existing_value)) = merged_env.iter_mut().find(|(k, _)| k == &key) {
                *existing_value = value;
            } else {
                merged_env.push((key, value));
            }
        }
        tokio::time::timeout(
            timeout,
            vm.oci_exec(
                id.to_string(),
                command.clone(),
                args.to_vec(),
                OciExecOptions {
                    env: merged_env,
                    cwd: exec.working_dir,
                    user: exec.user,
                },
            ),
        )
        .await
        .map_err(|_| {
            OciError::InvalidConfig(format!(
                "OCI unary exec timed out after {:.3}s",
                timeout.as_secs_f64()
            ))
        })?
        .map_err(OciError::from)
    }

    /// Write stdin bytes into an active interactive execution session.
    pub async fn write_exec_stdin(&self, execution_id: &str, data: &[u8]) -> Result<(), OciError> {
        let debug = exec_control_debug_enabled();
        if debug {
            debug!(
                "[vz-oci-macos exec-control] write_exec_stdin start execution_id={execution_id} bytes={}",
                data.len()
            );
        }
        let session = self.require_exec_session(execution_id).await?;
        if !session.pty_enabled {
            return Err(OciError::ExecutionControlUnsupported {
                operation: "write_exec_stdin".to_string(),
                reason: "execution session is not interactive".to_string(),
            });
        }
        let write_result = session
            .vm
            .stdin_write(session.guest_exec_id, data)
            .await
            .map_err(OciError::from);
        if debug {
            match &write_result {
                Ok(()) => debug!(
                    "[vz-oci-macos exec-control] write_exec_stdin complete execution_id={execution_id} guest_exec_id={}",
                    session.guest_exec_id
                ),
                Err(error) => debug!(
                    "[vz-oci-macos exec-control] write_exec_stdin failed execution_id={execution_id} guest_exec_id={} error={error}",
                    session.guest_exec_id
                ),
            }
        }
        write_result
    }

    /// Send a signal into an active interactive execution session.
    pub async fn signal_exec(&self, execution_id: &str, signal: &str) -> Result<(), OciError> {
        let session = self.require_exec_session(execution_id).await?;
        let Some(signal_num) = parse_signal_number(signal) else {
            return Err(OciError::InvalidConfig(format!(
                "unsupported signal '{signal}'"
            )));
        };
        session
            .vm
            .signal(session.guest_exec_id, signal_num)
            .await
            .map_err(OciError::from)
    }

    /// Resize PTY dimensions for an active interactive execution session.
    pub async fn resize_exec_pty(
        &self,
        execution_id: &str,
        cols: u16,
        rows: u16,
    ) -> Result<(), OciError> {
        let session = self.require_exec_session(execution_id).await?;
        if !session.pty_enabled {
            return Err(OciError::ExecutionControlUnsupported {
                operation: "resize_exec_pty".to_string(),
                reason: "execution session has no PTY".to_string(),
            });
        }
        session
            .vm
            .resize_exec_pty(session.guest_exec_id, u32::from(rows), u32::from(cols))
            .await
            .map_err(OciError::from)
    }

    /// Cancel an active interactive execution session.
    pub async fn cancel_exec(&self, execution_id: &str) -> Result<(), OciError> {
        let session = self.require_exec_session(execution_id).await?;
        session
            .vm
            .signal(session.guest_exec_id, 15)
            .await
            .map_err(OciError::from)
    }

    async fn require_exec_session(
        &self,
        execution_id: &str,
    ) -> Result<InteractiveExecSession, OciError> {
        self.exec_sessions
            .lock()
            .await
            .get(execution_id)
            .cloned()
            .ok_or_else(|| OciError::ExecutionSessionNotFound {
                execution_id: execution_id.to_string(),
            })
    }

    /// Execute a command at the VM level (not inside a container namespace).
    ///
    /// Uses the guest agent's direct exec path. This works even
    /// when the container's init process has exited, making it suitable for
    /// reading logs from the VM-level log directory.
    pub async fn exec_host(
        &self,
        container_id: &str,
        exec: ExecConfig,
    ) -> Result<ExecOutput, OciError> {
        let vm = self
            .vm_handles
            .lock()
            .await
            .get(container_id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no active VM handle for container '{container_id}'"
                ))
            })?;

        let (command, args) = exec
            .cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("exec command must not be empty".to_string()))?;

        let timeout = exec.timeout.unwrap_or(self.config.exec_timeout);

        let result = vm
            .exec_collect(command.clone(), args.to_vec(), timeout)
            .await
            .map_err(OciError::from)?;

        Ok(ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }
}
