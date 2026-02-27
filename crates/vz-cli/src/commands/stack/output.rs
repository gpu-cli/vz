use super::*;

pub(super) fn print_ps_table(observed: &[ServiceObservedState], desired: Option<&StackSpec>) {
    if observed.is_empty() {
        println!("No services found.");
        return;
    }

    // Create a map of service name to ports for quick lookup
    let ports_map: std::collections::HashMap<&str, Vec<String>> = desired
        .map(|spec| {
            spec.services
                .iter()
                .map(|s| {
                    let ports = s
                        .ports
                        .iter()
                        .map(|p| {
                            if let Some(hp) = p.host_port {
                                format!("{}:{}", hp, p.container_port)
                            } else {
                                format!("{}", p.container_port)
                            }
                        })
                        .collect();
                    (s.name.as_str(), ports)
                })
                .collect()
        })
        .unwrap_or_default();

    // Header.
    let name_width = 14;
    let status_width = 14;
    let health_width = 8;
    let cpu_width = 8;
    let mem_width = 10;
    let ports_width = 16;
    let container_width = 20;

    println!(
        "{:<wn$} {:<ws$} {:<wh$} {:<wc$} {:<wm$} {:<wp$} {:<wcid$}",
        "SERVICE",
        "STATUS",
        "HEALTH",
        "CPU",
        "MEMORY",
        "PORTS",
        "CONTAINER",
        wn = name_width,
        ws = status_width,
        wh = health_width,
        wc = cpu_width,
        wm = mem_width,
        wp = ports_width,
        wcid = container_width
    );
    println!(
        "{}",
        "-".repeat(
            name_width
                + status_width
                + health_width
                + cpu_width
                + mem_width
                + ports_width
                + container_width
                + 6
        )
    );

    for svc in observed {
        let status = match svc.phase {
            ServicePhase::Pending => "pending".to_string(),
            ServicePhase::Creating => "creating".to_string(),
            ServicePhase::Running if svc.ready => "running".to_string(),
            ServicePhase::Running => "running".to_string(),
            ServicePhase::Stopping => "stopping".to_string(),
            ServicePhase::Stopped => "stopped".to_string(),
            ServicePhase::Failed => "failed".to_string(),
        };

        let health = if svc.phase == ServicePhase::Failed {
            "\u{2717} fail".to_string()
        } else if svc.ready {
            "\u{2713} ok".to_string()
        } else if svc.phase == ServicePhase::Running {
            "-".to_string()
        } else {
            "-".to_string()
        };

        // Resource usage: not yet available from the runtime backend.
        let cpu = "N/A";
        let mem = "N/A";

        let ports = ports_map
            .get(svc.service_name.as_str())
            .map(|p| p.join(", "))
            .unwrap_or_else(|| "-".to_string());

        let cid = svc.container_id.as_deref().unwrap_or("-");
        println!(
            "{:<wn$} {:<ws$} {:<wh$} {:<wc$} {:<wm$} {:<wp$} {:<wcid$}",
            svc.service_name,
            status,
            health,
            cpu,
            mem,
            ports,
            cid,
            wn = name_width,
            ws = status_width,
            wh = health_width,
            wc = cpu_width,
            wm = mem_width,
            wp = ports_width,
            wcid = container_width
        );
    }

    // Note about resource usage
    println!();
    println!("Note: CPU/Memory usage requires runtime metrics (not yet available)");
}

pub(super) fn print_events_table(records: &[EventRecord]) {
    if records.is_empty() {
        println!("No events found.");
        return;
    }

    println!("{:>6}  {:<24} EVENT", "ID", "TIME");
    println!("{}", "-".repeat(72));

    for record in records {
        let summary = format_event_summary(&record.event);
        println!("{:>6}  {:<24} {}", record.id, record.created_at, summary);
    }
}

pub(super) fn format_event_summary(event: &StackEvent) -> String {
    match event {
        StackEvent::StackApplyStarted {
            stack_name,
            services_count,
        } => format!("apply started: {stack_name} ({services_count} services)"),
        StackEvent::StackApplyCompleted {
            succeeded, failed, ..
        } => format!("apply completed: {succeeded} ok, {failed} failed"),
        StackEvent::StackApplyFailed { error, .. } => format!("apply failed: {error}"),
        StackEvent::ServiceCreating { service_name, .. } => {
            format!("creating: {service_name}")
        }
        StackEvent::ServiceReady {
            service_name,
            runtime_id,
            ..
        } => format!("ready: {service_name} ({runtime_id})"),
        StackEvent::ServiceStopping { service_name, .. } => {
            format!("stopping: {service_name}")
        }
        StackEvent::ServiceStopped {
            service_name,
            exit_code,
            ..
        } => format!("stopped: {service_name} (exit {exit_code})"),
        StackEvent::ServiceFailed {
            service_name,
            error,
            ..
        } => format!("failed: {service_name}: {error}"),
        StackEvent::PortConflict {
            service_name, port, ..
        } => format!("port conflict: {service_name} port {port}"),
        StackEvent::VolumeCreated { volume_name, .. } => {
            format!("volume created: {volume_name}")
        }
        StackEvent::StackDestroyed { stack_name } => {
            format!("destroyed: {stack_name}")
        }
        StackEvent::HealthCheckPassed { service_name, .. } => {
            format!("health ok: {service_name}")
        }
        StackEvent::HealthCheckFailed {
            service_name,
            attempt,
            error,
            ..
        } => format!("health fail: {service_name} (attempt {attempt}): {error}"),
        StackEvent::DependencyBlocked {
            service_name,
            waiting_on,
            ..
        } => format!(
            "blocked: {service_name} waiting on {}",
            waiting_on.join(", ")
        ),
        StackEvent::MountTopologyRecreateRequired {
            service_name,
            previous_digest,
            desired_digest,
            ..
        } => format!(
            "mount recreate: {service_name} ({:?} -> {desired_digest})",
            previous_digest.as_deref().unwrap_or("<none>")
        ),
        StackEvent::SandboxCreating { sandbox_id, .. } => {
            format!("sandbox creating: {sandbox_id}")
        }
        StackEvent::SandboxReady { sandbox_id, .. } => {
            format!("sandbox ready: {sandbox_id}")
        }
        StackEvent::SandboxDraining { sandbox_id, .. } => {
            format!("sandbox draining: {sandbox_id}")
        }
        StackEvent::SandboxTerminated { sandbox_id, .. } => {
            format!("sandbox terminated: {sandbox_id}")
        }
        StackEvent::SandboxFailed {
            sandbox_id, error, ..
        } => format!("sandbox failed: {sandbox_id}: {error}"),
        StackEvent::LeaseOpened { lease_id, .. } => {
            format!("lease opened: {lease_id}")
        }
        StackEvent::LeaseHeartbeat { lease_id } => {
            format!("lease heartbeat: {lease_id}")
        }
        StackEvent::LeaseExpired { lease_id } => {
            format!("lease expired: {lease_id}")
        }
        StackEvent::LeaseClosed { lease_id } => {
            format!("lease closed: {lease_id}")
        }
        StackEvent::LeaseFailed { lease_id, error } => {
            format!("lease failed: {lease_id}: {error}")
        }
        StackEvent::ExecutionQueued {
            execution_id,
            container_id,
        } => format!("execution queued: {execution_id} for {container_id}"),
        StackEvent::ExecutionRunning { execution_id } => {
            format!("execution running: {execution_id}")
        }
        StackEvent::ExecutionExited {
            execution_id,
            exit_code,
        } => format!("execution exited: {execution_id} (code {exit_code})"),
        StackEvent::ExecutionFailed {
            execution_id,
            error,
        } => format!("execution failed: {execution_id}: {error}"),
        StackEvent::ExecutionCanceled { execution_id } => {
            format!("execution canceled: {execution_id}")
        }
        StackEvent::ExecutionResized {
            execution_id,
            cols,
            rows,
        } => format!("execution resized: {execution_id} ({cols}x{rows})"),
        StackEvent::ExecutionSignaled {
            execution_id,
            signal,
        } => format!("execution signaled: {execution_id} ({signal})"),
        StackEvent::CheckpointCreating {
            checkpoint_id,
            class,
            ..
        } => format!("checkpoint creating: {checkpoint_id} ({class})"),
        StackEvent::CheckpointReady { checkpoint_id } => {
            format!("checkpoint ready: {checkpoint_id}")
        }
        StackEvent::CheckpointFailed {
            checkpoint_id,
            error,
        } => format!("checkpoint failed: {checkpoint_id}: {error}"),
        StackEvent::CheckpointRestored {
            checkpoint_id,
            sandbox_id,
        } => format!("checkpoint restored: {checkpoint_id} -> {sandbox_id}"),
        StackEvent::CheckpointForked {
            parent_checkpoint_id,
            new_checkpoint_id,
            ..
        } => format!("checkpoint forked: {parent_checkpoint_id} -> {new_checkpoint_id}"),
        StackEvent::BuildQueued {
            sandbox_id,
            build_id,
        } => format!("build queued: {build_id} for {sandbox_id}"),
        StackEvent::BuildRunning { build_id } => {
            format!("build running: {build_id}")
        }
        StackEvent::BuildSucceeded {
            build_id,
            result_digest,
        } => format!("build succeeded: {build_id} ({result_digest})"),
        StackEvent::BuildFailed { build_id, error } => {
            format!("build failed: {build_id}: {error}")
        }
        StackEvent::BuildCanceled { build_id } => {
            format!("build canceled: {build_id}")
        }
        StackEvent::ContainerCreated {
            container_id,
            sandbox_id,
        } => format!("container created: {container_id} in {sandbox_id}"),
        StackEvent::ContainerStarting { container_id } => {
            format!("container starting: {container_id}")
        }
        StackEvent::ContainerRunning { container_id } => {
            format!("container running: {container_id}")
        }
        StackEvent::ContainerStopping { container_id } => {
            format!("container stopping: {container_id}")
        }
        StackEvent::ContainerExited {
            container_id,
            exit_code,
        } => format!("container exited: {container_id} (code {exit_code})"),
        StackEvent::ContainerFailed {
            container_id,
            error,
        } => format!("container failed: {container_id}: {error}"),
        StackEvent::ContainerRemoved { container_id } => {
            format!("container removed: {container_id}")
        }
        StackEvent::DriftDetected {
            category,
            description,
            severity,
            ..
        } => format!("drift [{severity}] {category}: {description}"),
        StackEvent::OrphanCleaned { container_id, .. } => {
            format!("orphan cleaned: {container_id}")
        }
    }
}
