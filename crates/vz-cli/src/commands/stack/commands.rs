use super::api::*;
use super::helpers::*;
use super::output::*;
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ControlAction {
    Stop,
    Start,
    Restart,
}
pub(super) async fn cmd_up(args: UpArgs) -> anyhow::Result<()> {
    let registry_auth = resolve_stack_registry_auth(&args.auth)?;
    if registry_auth.is_some() {
        bail!("registry auth flags are not supported for daemon stack apply yet");
    }
    if !args.no_tui {
        debug!("daemon stack mode ignores TUI control socket flow");
    }

    let file = resolve_compose_file(args.file)?;
    let yaml = std::fs::read_to_string(&file)
        .with_context(|| format!("failed to read compose file: {}", file.display()))?;

    let compose_dir = file
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let stack_name = resolve_stack_name(args.name.as_deref(), &file)?;
    let compose_dir = compose_dir.to_string_lossy().to_string();
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let mut stream = client
                .apply_stack_stream(runtime_v2::ApplyStackRequest {
                    metadata: None,
                    stack_name: stack_name.clone(),
                    compose_yaml: yaml.clone(),
                    compose_dir: compose_dir.clone(),
                    dry_run: args.dry_run,
                    detach: args.detach,
                })
                .await
                .with_context(|| format!("failed to apply stack `{stack_name}` via daemon"))?;
            let mut completion = None;
            while let Some(event) = stream
                .message()
                .await
                .with_context(|| format!("failed to read apply stack stream for `{stack_name}`"))?
            {
                match event.payload {
                    Some(runtime_v2::apply_stack_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::apply_stack_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            completion
                .ok_or_else(|| anyhow!("daemon apply_stack stream ended without completion"))?
                .response
                .ok_or_else(|| anyhow!("daemon apply_stack completion missing response payload"))?
        }
        ControlPlaneTransport::ApiHttp => {
            let response = api_apply_stack(ApiApplyStackRequest {
                stack_name: stack_name.clone(),
                compose_yaml: yaml,
                compose_dir,
                dry_run: args.dry_run,
                detach: args.detach,
            })
            .await?;
            runtime_v2::ApplyStackResponse {
                request_id: String::new(),
                stack_name: response.stack_name,
                changed_actions: response.changed_actions,
                converged: response.converged,
                services_ready: response.services_ready,
                services_failed: response.services_failed,
                services: response
                    .services
                    .into_iter()
                    .map(stack_service_status_from_api)
                    .collect(),
            }
        }
    };

    let observed = observed_from_stack_statuses(&response.services);

    if args.dry_run {
        println!(
            "Plan for stack `{}`: {} action(s) would change.",
            response.stack_name, response.changed_actions
        );
        if !observed.is_empty() {
            print_ps_table(&observed, None);
        }
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    if observed.is_empty() && response.changed_actions == 0 {
        println!("No changes needed.");
    } else {
        print_ps_table(&observed, None);
        println!();
        println!(
            "Applied stack `{}` with {} changed action(s).",
            response.stack_name, response.changed_actions
        );
    }

    if response.services_failed > 0 {
        bail!("{} service(s) failed", response.services_failed);
    }
    if !args.detach && !response.converged {
        bail!("stack did not converge");
    }

    Ok(())
}

// ── exec ──────────────────────────────────────────────────────────

/// Connect to a running `vz stack up` session and execute a command.
pub(super) async fn cmd_exec(args: ExecArgs) -> anyhow::Result<()> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let status = client
                .get_stack_status(runtime_v2::GetStackStatusRequest {
                    metadata: None,
                    stack_name: args.name.clone(),
                })
                .await
                .with_context(|| {
                    format!("failed to load stack status for `{}` via daemon", args.name)
                })?;
            let container_id =
                resolve_service_container_id(&args.name, &args.service, &status.services)?;
            execute_stack_container_command_daemon(&mut client, container_id, &args.command)
                .await
                .with_context(|| {
                    format!(
                        "failed to execute command for stack `{}` service `{}`",
                        args.name, args.service
                    )
                })
        }
        ControlPlaneTransport::ApiHttp => {
            let status = api_get_stack_status(&args.name).await.with_context(|| {
                format!("failed to load stack status for `{}` via api", args.name)
            })?;
            let services = status
                .services
                .into_iter()
                .map(stack_service_status_from_api)
                .collect::<Vec<_>>();
            let container_id = resolve_service_container_id(&args.name, &args.service, &services)?;
            execute_stack_container_command_api(container_id, &args.command)
                .await
                .with_context(|| {
                    format!(
                        "failed to execute command for stack `{}` service `{}`",
                        args.name, args.service
                    )
                })
        }
    }
}

// ── service start/stop/restart ─────────────────────────────────────

/// Send a daemon-backed service-level action (stop/start/restart).
pub(super) async fn cmd_service_action(
    args: ServiceArgs,
    action: ControlAction,
) -> anyhow::Result<()> {
    let service = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let request = runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: args.name.clone(),
                service_name: args.service.clone(),
            };
            let mut stream = match action {
                ControlAction::Stop => client
                    .stop_stack_service_stream(request)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to stop service `{}` in stack `{}` via daemon",
                            args.service, args.name
                        )
                    })?,
                ControlAction::Start => client
                    .start_stack_service_stream(request)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to start service `{}` in stack `{}` via daemon",
                            args.service, args.name
                        )
                    })?,
                ControlAction::Restart => client
                    .restart_stack_service_stream(request)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to restart service `{}` in stack `{}` via daemon",
                            args.service, args.name
                        )
                    })?,
            };
            let mut completion = None;
            while let Some(event) = stream.message().await.with_context(|| {
                format!(
                    "failed to read service action stream for `{}` in stack `{}`",
                    args.service, args.name
                )
            })? {
                match event.payload {
                    Some(runtime_v2::stack_service_action_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::stack_service_action_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            let response = completion
                .ok_or_else(|| anyhow!("daemon stack service stream ended without completion"))?
                .response
                .ok_or_else(|| {
                    anyhow!("daemon stack service completion missing response payload")
                })?;

            response
                .service
                .ok_or_else(|| anyhow!("daemon returned missing stack service payload"))?
        }
        ControlPlaneTransport::ApiHttp => {
            let action_name = match action {
                ControlAction::Stop => "stop",
                ControlAction::Start => "start",
                ControlAction::Restart => "restart",
            };
            let response = api_stack_service_action(&args.name, &args.service, action_name)
                .await
                .with_context(|| {
                    format!(
                        "failed to {action_name} service `{}` in stack `{}` via api",
                        args.service, args.name
                    )
                })?;
            stack_service_status_from_api(response.service)
        }
    };

    let phase = if service.phase.trim().is_empty() {
        "unknown"
    } else {
        service.phase.as_str()
    };
    println!(
        "Service `{}` in stack `{}` now reports phase `{}`.",
        service.service_name, args.name, phase
    );
    if phase.eq_ignore_ascii_case("failed") {
        if service.last_error.trim().is_empty() {
            bail!(
                "service `{}` in stack `{}` entered failed state",
                service.service_name,
                args.name
            );
        }
        bail!(
            "service `{}` in stack `{}` entered failed state: {}",
            service.service_name,
            args.name,
            service.last_error
        );
    }

    Ok(())
}

// ── down ───────────────────────────────────────────────────────────

pub(super) async fn cmd_down(args: DownArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let mut stream = client
                .teardown_stack_stream(runtime_v2::TeardownStackRequest {
                    metadata: None,
                    stack_name: args.name.clone(),
                    dry_run: args.dry_run,
                    remove_volumes: args.volumes,
                })
                .await
                .with_context(|| format!("failed to teardown stack `{}` via daemon", args.name))?;
            let mut completion = None;
            while let Some(event) = stream.message().await.with_context(|| {
                format!("failed to read teardown stack stream for `{}`", args.name)
            })? {
                match event.payload {
                    Some(runtime_v2::teardown_stack_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::teardown_stack_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            completion
                .ok_or_else(|| anyhow!("daemon teardown_stack stream ended without completion"))?
                .response
                .ok_or_else(|| {
                    anyhow!("daemon teardown_stack completion missing response payload")
                })?
        }
        ControlPlaneTransport::ApiHttp => {
            let response = api_teardown_stack(ApiTeardownStackRequest {
                stack_name: args.name.clone(),
                dry_run: args.dry_run,
                remove_volumes: args.volumes,
            })
            .await?;
            runtime_v2::TeardownStackResponse {
                request_id: String::new(),
                stack_name: response.stack_name,
                changed_actions: response.changed_actions,
                removed_volumes: response.removed_volumes,
            }
        }
    };

    if args.dry_run {
        println!(
            "Plan for stack `{}`: {} action(s) would change.",
            response.stack_name, response.changed_actions
        );
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    if response.changed_actions == 0 && response.removed_volumes == 0 {
        println!("No changes needed.");
        return Ok(());
    }

    println!(
        "Teardown complete for stack `{}` ({} changed action(s)).",
        response.stack_name, response.changed_actions
    );
    if args.volumes {
        if response.removed_volumes == 0 {
            println!("No volumes to remove.");
        } else {
            println!("Removed {} volume(s).", response.removed_volumes);
        }
    }

    Ok(())
}

// ── ps ─────────────────────────────────────────────────────────────

pub(super) async fn cmd_ps(args: PsArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            client
                .get_stack_status(runtime_v2::GetStackStatusRequest {
                    metadata: None,
                    stack_name: args.name.clone(),
                })
                .await
                .with_context(|| {
                    format!("failed to get stack status for `{}` via daemon", args.name)
                })?
        }
        ControlPlaneTransport::ApiHttp => {
            let response = api_get_stack_status(&args.name).await.with_context(|| {
                format!("failed to get stack status for `{}` via api", args.name)
            })?;
            runtime_v2::GetStackStatusResponse {
                request_id: String::new(),
                stack_name: response.stack_name,
                services: response
                    .services
                    .into_iter()
                    .map(stack_service_status_from_api)
                    .collect(),
            }
        }
    };

    let observed = observed_from_stack_statuses(&response.services);
    if args.json {
        let json = serde_json::to_string_pretty(&observed)
            .with_context(|| "failed to serialize observed state")?;
        println!("{json}");
    } else {
        print_ps_table(&observed, None);
    }

    Ok(())
}

// ── events ─────────────────────────────────────────────────────────

pub(super) async fn cmd_events(args: EventsArgs) -> anyhow::Result<()> {
    let mut cursor = args.since.max(0);
    let mut events = Vec::new();
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            loop {
                let response = client
                    .list_stack_events(runtime_v2::ListStackEventsRequest {
                        metadata: None,
                        stack_name: args.name.clone(),
                        after: cursor,
                        limit: 1000,
                    })
                    .await
                    .with_context(|| {
                        format!("failed to list stack events for `{}` via daemon", args.name)
                    })?;
                if response.events.is_empty() {
                    break;
                }
                events.extend(response.events);
                if response.next_cursor <= cursor {
                    break;
                }
                cursor = response.next_cursor;
            }
        }
        ControlPlaneTransport::ApiHttp => loop {
            let response = api_list_stack_events(&args.name, cursor, 1000)
                .await
                .with_context(|| {
                    format!("failed to list stack events for `{}` via api", args.name)
                })?;
            if response.events.is_empty() {
                break;
            }
            let mut page_events = Vec::with_capacity(response.events.len());
            for event in response.events {
                let event_json = serde_json::to_string(&event.event)
                    .context("failed to serialize api stack event payload")?;
                page_events.push(runtime_v2::RuntimeEvent {
                    id: event.id,
                    stack_name: event.stack_name,
                    created_at: event.created_at,
                    event_json,
                });
            }
            events.extend(page_events);
            if response.next_cursor <= cursor {
                break;
            }
            cursor = response.next_cursor;
        },
    }

    if args.json {
        for event in &events {
            println!("{}", event.event_json);
        }
        return Ok(());
    }

    let mut records = Vec::with_capacity(events.len());
    for event in events {
        let parsed: StackEvent = serde_json::from_str(&event.event_json)
            .with_context(|| format!("failed to parse stack event payload {}", event.id))?;
        records.push(EventRecord {
            id: event.id,
            stack_name: event.stack_name,
            created_at: event.created_at,
            event: parsed,
        });
    }
    print_events_table(&records);
    Ok(())
}

// ── logs ──────────────────────────────────────────────────────────

pub(super) async fn cmd_logs(args: LogsArgs) -> anyhow::Result<()> {
    let service_filter = args.service.unwrap_or_default();
    let tail_limit = u32::try_from(args.tail).unwrap_or(u32::MAX);

    let mut previous_outputs: HashMap<String, String> = HashMap::new();
    let mut first_iteration = true;

    loop {
        let logs = match control_plane_transport()? {
            ControlPlaneTransport::DaemonGrpc => {
                let state_db = stack_state_db_path(args.state_dir.as_deref());
                let mut client = connect_control_plane_for_state_db(&state_db).await?;
                let response = client
                    .get_stack_logs(runtime_v2::GetStackLogsRequest {
                        metadata: None,
                        stack_name: args.name.clone(),
                        service: service_filter.clone(),
                        tail: if first_iteration { tail_limit } else { 0 },
                    })
                    .await
                    .with_context(|| {
                        format!("failed to get stack logs for `{}` via daemon", args.name)
                    })?;
                response.logs
            }
            ControlPlaneTransport::ApiHttp => {
                let response = api_get_stack_logs(
                    &args.name,
                    &service_filter,
                    if first_iteration { tail_limit } else { 0 },
                )
                .await
                .with_context(|| format!("failed to get stack logs for `{}` via api", args.name))?;
                response
                    .logs
                    .into_iter()
                    .map(|log| runtime_v2::StackServiceLog {
                        service_name: log.service_name,
                        output: log.output,
                    })
                    .collect()
            }
        };

        if logs.is_empty() {
            bail!("no running services in stack `{}`", args.name);
        }

        let multi = logs.len() > 1 || service_filter.is_empty();
        for log in logs {
            let previous = previous_outputs
                .get(&log.service_name)
                .map(String::as_str)
                .unwrap_or_default();
            let delta = log
                .output
                .strip_prefix(previous)
                .map(str::to_string)
                .unwrap_or_else(|| log.output.clone());
            print_log_output(&delta, &log.service_name, multi);
            previous_outputs.insert(log.service_name, log.output);
        }

        if !args.follow {
            return Ok(());
        }

        first_iteration = false;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

/// Print log output, prefixing each line with `[service]` for multi-service stacks.
fn print_log_output(output: &str, service: &str, multi: bool) {
    if output.is_empty() {
        return;
    }
    if multi {
        for line in output.lines() {
            println!("[{service}] {line}");
        }
    } else {
        print!("{output}");
    }
}

/// Extract the service name from a stack event, if applicable.
#[cfg(test)]
pub(super) fn event_service_name(event: &StackEvent) -> Option<&str> {
    match event {
        StackEvent::ServiceCreating { service_name, .. }
        | StackEvent::ServiceReady { service_name, .. }
        | StackEvent::ServiceStopping { service_name, .. }
        | StackEvent::ServiceStopped { service_name, .. }
        | StackEvent::ServiceFailed { service_name, .. }
        | StackEvent::PortConflict { service_name, .. }
        | StackEvent::HealthCheckPassed { service_name, .. }
        | StackEvent::HealthCheckFailed { service_name, .. }
        | StackEvent::DependencyBlocked { service_name, .. }
        | StackEvent::MountTopologyRecreateRequired { service_name, .. } => Some(service_name),
        StackEvent::StackApplyStarted { .. }
        | StackEvent::StackApplyCompleted { .. }
        | StackEvent::StackApplyFailed { .. }
        | StackEvent::VolumeCreated { .. }
        | StackEvent::StackDestroyed { .. }
        | StackEvent::SandboxCreating { .. }
        | StackEvent::SandboxReady { .. }
        | StackEvent::SandboxDraining { .. }
        | StackEvent::SandboxTerminated { .. }
        | StackEvent::SandboxFailed { .. }
        | StackEvent::LeaseOpened { .. }
        | StackEvent::LeaseHeartbeat { .. }
        | StackEvent::LeaseExpired { .. }
        | StackEvent::LeaseClosed { .. }
        | StackEvent::LeaseFailed { .. }
        | StackEvent::ExecutionQueued { .. }
        | StackEvent::ExecutionRunning { .. }
        | StackEvent::ExecutionExited { .. }
        | StackEvent::ExecutionFailed { .. }
        | StackEvent::ExecutionCanceled { .. }
        | StackEvent::ExecutionResized { .. }
        | StackEvent::ExecutionSignaled { .. }
        | StackEvent::CheckpointCreating { .. }
        | StackEvent::CheckpointReady { .. }
        | StackEvent::CheckpointFailed { .. }
        | StackEvent::CheckpointRestored { .. }
        | StackEvent::CheckpointForked { .. }
        | StackEvent::CheckpointGcCompacted { .. }
        | StackEvent::BuildQueued { .. }
        | StackEvent::BuildRunning { .. }
        | StackEvent::BuildSucceeded { .. }
        | StackEvent::BuildFailed { .. }
        | StackEvent::BuildCanceled { .. }
        | StackEvent::ContainerCreated { .. }
        | StackEvent::ContainerStarting { .. }
        | StackEvent::ContainerRunning { .. }
        | StackEvent::ContainerStopping { .. }
        | StackEvent::ContainerExited { .. }
        | StackEvent::ContainerFailed { .. }
        | StackEvent::ContainerRemoved { .. }
        | StackEvent::DriftDetected { .. }
        | StackEvent::OrphanCleaned { .. } => None,
    }
}

// ── ls ─────────────────────────────────────────────────────────────

/// Stack entry for the `ls` listing.
#[derive(Debug, Serialize)]
struct StackListEntry {
    name: String,
    status: String,
    ready: usize,
    total: usize,
    error_summary: Option<String>,
}

pub(super) async fn cmd_ls(args: LsArgs) -> anyhow::Result<()> {
    if args.state_dir.is_some() {
        bail!(
            "`vz stack ls --state-dir` is not supported in daemon mode; use the daemon default state db"
        );
    }

    #[derive(Default)]
    struct StackAggregate {
        states: Vec<String>,
        ready: usize,
        total: usize,
    }

    let mut grouped: HashMap<String, StackAggregate> = HashMap::new();
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = default_state_db_path();
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let response = client
                .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
                .await
                .with_context(|| "failed to list sandboxes via daemon for stack listing")?;
            for sandbox in response.sandboxes {
                let stack_name = sandbox
                    .labels
                    .get("stack_name")
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
                    .unwrap_or_else(|| sandbox.sandbox_id.clone());
                let aggregate = grouped.entry(stack_name).or_default();
                aggregate.total += 1;
                if sandbox.state.eq_ignore_ascii_case("ready") {
                    aggregate.ready += 1;
                }
                aggregate.states.push(sandbox.state);
            }
        }
        ControlPlaneTransport::ApiHttp => {
            let sandboxes = api_list_sandboxes().await?;
            for sandbox in sandboxes {
                let stack_name = sandbox
                    .labels
                    .get("stack_name")
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
                    .unwrap_or_else(|| sandbox.sandbox_id.clone());
                let aggregate = grouped.entry(stack_name).or_default();
                aggregate.total += 1;
                if sandbox.state.eq_ignore_ascii_case("ready") {
                    aggregate.ready += 1;
                }
                aggregate.states.push(sandbox.state);
            }
        }
    }

    let mut entries: Vec<StackListEntry> = grouped
        .into_iter()
        .map(|(name, aggregate)| {
            let (status, error_summary) = stack_status_from_sandbox_states(
                &aggregate.states,
                aggregate.ready,
                aggregate.total,
            );
            StackListEntry {
                name,
                status,
                ready: aggregate.ready,
                total: aggregate.total,
                error_summary,
            }
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    if args.json {
        let json = serde_json::to_string_pretty(&entries)
            .with_context(|| "failed to serialize stack list")?;
        println!("{json}");
        return Ok(());
    }

    if entries.is_empty() {
        println!("No stacks found.");
        return Ok(());
    }

    let name_width = entries
        .iter()
        .map(|entry| entry.name.len())
        .max()
        .unwrap_or(10)
        .max(10);
    let status_width = 14;
    let ready_width = 11;

    println!(
        "{:<width$} {:<status_width$} {:<ready_width$}",
        "STACK NAME",
        "STATUS",
        "READY/TOTAL",
        width = name_width
    );
    println!(
        "{}",
        "-".repeat(name_width + status_width + ready_width + 2)
    );

    for entry in &entries {
        let ready_str = format!("{}/{}", entry.ready, entry.total);
        println!(
            "{:<width$} {:<status_width$} {:<ready_width$}",
            entry.name,
            entry.status,
            ready_str,
            width = name_width
        );
        if let Some(ref err) = entry.error_summary {
            let summary = if err.len() > 50 {
                format!("{}...", &err[..47])
            } else {
                err.clone()
            };
            println!("  └─ {}", summary);
        }
    }

    println!();
    let running = entries
        .iter()
        .filter(|entry| entry.status.contains("running"))
        .count();
    let starting = entries
        .iter()
        .filter(|entry| entry.status.contains("starting"))
        .count();
    let failed = entries
        .iter()
        .filter(|entry| entry.status.contains("failed"))
        .count();

    if failed > 0 {
        println!(
            "Showing {} stacks ({} running, {} starting, {} failed)",
            entries.len(),
            running,
            starting,
            failed
        );
    } else {
        println!("Showing {} stacks", entries.len());
    }

    Ok(())
}

// ── config ─────────────────────────────────────────────────────────

pub(super) async fn cmd_config(args: ConfigArgs) -> anyhow::Result<()> {
    let file = resolve_compose_file(args.file)?;
    let yaml = std::fs::read_to_string(&file)
        .with_context(|| format!("failed to read compose file: {}", file.display()))?;

    let compose_dir = file
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let stack_name = resolve_stack_name(args.name.as_deref(), &file)?;
    let spec = parse_compose_with_dir(&yaml, &stack_name, &compose_dir)
        .with_context(|| "failed to parse compose file")?;

    if args.quiet {
        println!("Valid.");
    } else {
        let json = serde_json::to_string_pretty(&spec)
            .with_context(|| "failed to serialize stack spec")?;
        println!("{json}");
    }

    Ok(())
}

// ── run ────────────────────────────────────────────────────────────

pub(super) async fn cmd_run(args: RunArgs) -> anyhow::Result<()> {
    let _ = &args.file;

    let (container_id, run_service_name) = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let run_container = client
                .create_stack_run_container(runtime_v2::StackRunContainerRequest {
                    metadata: None,
                    stack_name: args.name.clone(),
                    service_name: args.service.clone(),
                    run_service_name: String::new(),
                })
                .await
                .with_context(|| {
                    format!(
                        "failed to create StackExecutor-backed run container for stack `{}` service `{}`",
                        args.name, args.service
                    )
                })?;
            (run_container.container_id, run_container.run_service_name)
        }
        ControlPlaneTransport::ApiHttp => {
            let run_container = api_create_stack_run_container(ApiStackRunContainerRequest {
                stack_name: args.name.clone(),
                service_name: args.service.clone(),
                run_service_name: None,
            })
            .await
            .with_context(|| {
                format!(
                    "failed to create api-backed run container for stack `{}` service `{}`",
                    args.name, args.service
                )
            })?;
            (run_container.container_id, run_container.run_service_name)
        }
    };

    if container_id.trim().is_empty() {
        bail!(
            "runtime returned empty container id for one-off run on service `{}`",
            args.service
        );
    }
    if run_service_name.trim().is_empty() {
        bail!(
            "runtime returned empty run service name for stack `{}` service `{}`",
            args.name,
            args.service
        );
    }

    let command_result = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = stack_state_db_path(args.state_dir.as_deref());
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            execute_stack_container_command_daemon(&mut client, container_id.clone(), &args.command)
                .await
        }
        ControlPlaneTransport::ApiHttp => {
            execute_stack_container_command_api(container_id.clone(), &args.command).await
        }
    }
    .with_context(|| {
        format!(
            "failed to run one-off command for stack `{}` service `{}`",
            args.name, args.service
        )
    });

    let cleanup_result = if args.rm {
        match control_plane_transport()? {
            ControlPlaneTransport::DaemonGrpc => {
                let state_db = stack_state_db_path(args.state_dir.as_deref());
                let mut client = connect_control_plane_for_state_db(&state_db).await?;
                client
                    .remove_stack_run_container(runtime_v2::StackRunContainerRequest {
                        metadata: None,
                        stack_name: args.name.clone(),
                        service_name: args.service.clone(),
                        run_service_name: run_service_name.clone(),
                    })
                    .await
                    .with_context(|| {
                        format!(
                            "failed to remove one-off run service `{}` (container `{}`) for stack `{}` service `{}`",
                            run_service_name, container_id, args.name, args.service
                        )
                    })
                    .map(|_| ())
            }
            ControlPlaneTransport::ApiHttp => api_remove_stack_run_container(ApiStackRunContainerRequest {
                stack_name: args.name.clone(),
                service_name: args.service.clone(),
                run_service_name: Some(run_service_name.clone()),
            })
            .await
            .with_context(|| {
                format!(
                    "failed to remove one-off run service `{}` (container `{}`) for stack `{}` service `{}` via api",
                    run_service_name, container_id, args.name, args.service
                )
            }),
        }
    } else {
        Ok(())
    };

    match (command_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(command_error), Err(cleanup_error)) => Err(anyhow::anyhow!(
            "{command_error}; cleanup failed: {cleanup_error}"
        )),
    }
}

// ── dashboard ─────────────────────────────────────────────────────

/// Open the TUI dashboard for an existing (running or stopped) stack.
pub(super) async fn cmd_dashboard(args: DashboardArgs) -> anyhow::Result<()> {
    let _ = args;
    bail!(
        "`vz stack dashboard` is deprecated and removed in daemon mode. Use `vz stack ps`, `vz stack logs`, and `vz stack events` instead."
    )
}
