use super::bundle::container_log_dir;
use super::*;

pub(super) type OciLifecycleFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, OciError>> + 'a>>;

pub(super) fn parse_signal_number(signal: &str) -> Option<i32> {
    let trimmed = signal.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = trimmed.parse::<i32>() {
        return (parsed > 0).then_some(parsed);
    }

    let upper = trimmed.to_ascii_uppercase();
    let normalized = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
    match normalized {
        "HUP" => Some(1),
        "INT" => Some(2),
        "QUIT" => Some(3),
        "KILL" => Some(9),
        "TERM" => Some(15),
        "USR1" => Some(10),
        "USR2" => Some(12),
        "PIPE" => Some(13),
        "ALRM" => Some(14),
        "CHLD" => Some(17),
        "CONT" => Some(18),
        "STOP" => Some(19),
        "TSTP" => Some(20),
        "TTIN" => Some(21),
        "TTOU" => Some(22),
        "WINCH" => Some(28),
        _ => None,
    }
}

pub(super) trait OciLifecycleOps {
    fn oci_create<'a>(&'a self, id: String, bundle_path: String) -> OciLifecycleFuture<'a, ()>;
    fn oci_start<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, ()>;
    fn oci_exec<'a>(
        &'a self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> OciLifecycleFuture<'a, ExecOutput>;
    fn oci_kill<'a>(&'a self, id: String, signal: String) -> OciLifecycleFuture<'a, ()>;
    fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState>;
    fn oci_delete<'a>(&'a self, id: String, force: bool) -> OciLifecycleFuture<'a, ()>;
}

impl OciLifecycleOps for LinuxVm {
    fn oci_create<'a>(&'a self, id: String, bundle_path: String) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move {
            self.oci_create(id, bundle_path)
                .await
                .map_err(OciError::from)
        })
    }

    fn oci_start<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move { self.oci_start(id).await.map_err(OciError::from) })
    }

    fn oci_exec<'a>(
        &'a self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> OciLifecycleFuture<'a, ExecOutput> {
        Box::pin(async move {
            let result = self
                .oci_exec(id, command, args, options)
                .await
                .map_err(OciError::from)?;
            Ok(ExecOutput {
                exit_code: result.exit_code,
                stdout: result.stdout,
                stderr: result.stderr,
            })
        })
    }

    fn oci_kill<'a>(&'a self, id: String, signal: String) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move { self.oci_kill(id, signal).await.map_err(OciError::from) })
    }

    fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
        Box::pin(async move { self.oci_state(id).await.map_err(OciError::from) })
    }

    fn oci_delete<'a>(&'a self, id: String, force: bool) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move { self.oci_delete(id, force).await.map_err(OciError::from) })
    }
}

pub(super) async fn run_oci_lifecycle(
    vm: &impl OciLifecycleOps,
    container_id: String,
    bundle_guest_path: String,
    command: String,
    args: Vec<String>,
    options: OciExecOptions,
) -> Result<ExecOutput, OciError> {
    vm.oci_create(container_id.clone(), bundle_guest_path)
        .await?;

    if let Err(start_error) = vm.oci_start(container_id.clone()).await {
        let _ = vm.oci_delete(container_id, true).await;
        return Err(start_error);
    }

    let exec = vm
        .oci_exec(container_id.clone(), command, args, options)
        .await;
    let delete = vm.oci_delete(container_id, true).await;

    match (exec, delete) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(exec_err), Ok(())) => Err(exec_err),
        (Ok(_), Err(delete_err)) => Err(delete_err),
        (Err(exec_err), Err(_delete_err)) => Err(exec_err),
    }
}

pub(super) struct LogRotationTask {
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl LogRotationTask {
    pub(super) async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

pub(super) fn spawn_log_rotation_task(
    container_id: String,
    vm: Arc<LinuxVm>,
    rotation: ComposeLogRotation,
) -> LogRotationTask {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task_container_id = container_id.clone();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(LOG_ROTATION_POLL_INTERVAL) => {
                    if let Err(error) = run_log_rotation_tick(vm.as_ref(), &task_container_id, rotation).await {
                        warn!(
                            container_id = %task_container_id,
                            error = %error,
                            "compose log-rotation tick failed"
                        );
                    }
                }
            }
        }
    });

    LogRotationTask { shutdown_tx, task }
}

async fn run_log_rotation_tick(
    vm: &LinuxVm,
    container_id: &str,
    rotation: ComposeLogRotation,
) -> Result<(), OciError> {
    let script = build_log_rotation_script(container_id, rotation);
    let output = vm
        .exec_capture(
            "/bin/busybox".to_string(),
            vec!["sh".to_string(), "-c".to_string(), script],
            LOG_ROTATION_COMMAND_TIMEOUT,
        )
        .await
        .map_err(OciError::from)?;

    if output.exit_code != 0 {
        let detail = if output.stderr.trim().is_empty() {
            output.stdout.trim().to_string()
        } else {
            output.stderr.trim().to_string()
        };
        return Err(OciError::InvalidConfig(format!(
            "compose log-rotation command failed for container `{container_id}` (exit {}): {detail}",
            output.exit_code
        )));
    }

    Ok(())
}

pub(super) fn build_log_rotation_script(
    container_id: &str,
    rotation: ComposeLogRotation,
) -> String {
    let log_path = format!("{}/output.log", container_log_dir(container_id));
    let archives = rotation.max_files.saturating_sub(1);

    if archives == 0 {
        return format!(
            "set -eu\n\
             log=\"{log_path}\"\n\
             [ -f \"$log\" ] || exit 0\n\
             size=$(/bin/busybox wc -c < \"$log\" | /bin/busybox tr -d '[:space:]')\n\
             if [ \"$size\" -ge {max_size} ]; then\n\
               : > \"$log\"\n\
             fi\n",
            max_size = rotation.max_size_bytes,
        );
    }

    let rotate_from = archives.saturating_sub(1);
    format!(
        "set -eu\n\
         log=\"{log_path}\"\n\
         [ -f \"$log\" ] || exit 0\n\
         size=$(/bin/busybox wc -c < \"$log\" | /bin/busybox tr -d '[:space:]')\n\
         if [ \"$size\" -lt {max_size} ]; then\n\
           exit 0\n\
         fi\n\
         /bin/busybox rm -f \"$log.{archives}\"\n\
         i={rotate_from}\n\
         while [ \"$i\" -ge 1 ]; do\n\
           if [ -f \"$log.$i\" ]; then\n\
             next=$((i + 1))\n\
             /bin/busybox mv \"$log.$i\" \"$log.$next\"\n\
           fi\n\
           i=$((i - 1))\n\
         done\n\
         /bin/busybox cp \"$log\" \"$log.1\"\n\
         : > \"$log\"\n",
        max_size = rotation.max_size_bytes,
    )
}
