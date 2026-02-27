use super::*;

pub(super) fn stack_state_db_path(explicit_state_dir: Option<&Path>) -> PathBuf {
    explicit_state_dir
        .map(|state_dir| state_dir.join("state.db"))
        .unwrap_or_else(default_state_db_path)
}

fn service_phase_from_stack_status(phase: &str) -> ServicePhase {
    match phase.trim().to_ascii_lowercase().as_str() {
        "pending" => ServicePhase::Pending,
        "creating" => ServicePhase::Creating,
        "running" => ServicePhase::Running,
        "stopping" => ServicePhase::Stopping,
        "stopped" => ServicePhase::Stopped,
        "failed" => ServicePhase::Failed,
        _ => ServicePhase::Pending,
    }
}

pub(super) fn observed_from_stack_statuses(
    services: &[runtime_v2::StackServiceStatus],
) -> Vec<ServiceObservedState> {
    services
        .iter()
        .map(|service| ServiceObservedState {
            service_name: service.service_name.clone(),
            phase: service_phase_from_stack_status(&service.phase),
            container_id: if service.container_id.trim().is_empty() {
                None
            } else {
                Some(service.container_id.clone())
            },
            last_error: if service.last_error.trim().is_empty() {
                None
            } else {
                Some(service.last_error.clone())
            },
            ready: service.ready,
        })
        .collect()
}

pub(super) fn resolve_service_container_id(
    stack_name: &str,
    service_name: &str,
    services: &[runtime_v2::StackServiceStatus],
) -> anyhow::Result<String> {
    let service = services
        .iter()
        .find(|service| service.service_name == service_name)
        .ok_or_else(|| {
            anyhow::anyhow!("service `{service_name}` not found in stack `{stack_name}`")
        })?;
    let container_id = service.container_id.trim();
    if container_id.is_empty() {
        let phase = if service.phase.trim().is_empty() {
            "unknown"
        } else {
            service.phase.as_str()
        };
        bail!("service `{service_name}` in stack `{stack_name}` is not running (phase: {phase})");
    }
    Ok(container_id.to_string())
}

pub(super) fn split_exec_command(command: &[String]) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let Some((head, tail)) = command.split_first() else {
        bail!("command cannot be empty");
    };
    Ok((vec![head.clone()], tail.to_vec()))
}
pub(super) fn stack_status_from_sandbox_states(
    states: &[String],
    ready_count: usize,
    total_count: usize,
) -> (String, Option<String>) {
    if total_count == 0 {
        return ("\u{25cb} stopped".to_string(), None);
    }

    let failed = states
        .iter()
        .any(|state| state.eq_ignore_ascii_case("failed"));
    let ready = states
        .iter()
        .filter(|state| state.eq_ignore_ascii_case("ready"))
        .count();
    let creating_or_draining = states.iter().any(|state| {
        state.eq_ignore_ascii_case("creating") || state.eq_ignore_ascii_case("draining")
    });
    let terminated = states
        .iter()
        .filter(|state| state.eq_ignore_ascii_case("terminated"))
        .count();

    if failed {
        return (
            "\u{2717} failed".to_string(),
            Some("one or more sandboxes are failed".to_string()),
        );
    }

    if terminated == total_count {
        return ("\u{25cb} stopped".to_string(), None);
    }

    if ready == total_count && ready_count == total_count {
        return ("\u{2713} running".to_string(), None);
    }

    if creating_or_draining {
        return ("\u{25d0} starting".to_string(), None);
    }

    ("\u{25d0} partial".to_string(), None)
}
// ── up ─────────────────────────────────────────────────────────────

pub(super) fn resolve_stack_registry_auth(
    opts: &StackRegistryAuthOpts,
) -> anyhow::Result<Option<vz_image::Auth>> {
    if opts.username.is_some() && opts.password.is_none() {
        bail!("--username requires --password");
    }
    if opts.password.is_some() && opts.username.is_none() {
        bail!("--password requires --username");
    }

    let auth = match (&opts.docker_config, &opts.username, &opts.password) {
        (true, _, _) => Some(vz_image::Auth::DockerConfig),
        (false, Some(username), Some(password)) => Some(vz_image::Auth::Basic {
            username: username.clone(),
            password: password.clone(),
        }),
        _ => None,
    };

    Ok(auth)
}
/// Standard compose file names in Docker Compose discovery order.
const COMPOSE_FILE_CANDIDATES: &[&str] = &[
    "compose.yaml",
    "compose.yml",
    "docker-compose.yml",
    "docker-compose.yaml",
];

/// Resolve the compose file path from an explicit `-f` flag or auto-discovery.
///
/// When no explicit path is given, searches the current directory for the first
/// existing file from [`COMPOSE_FILE_CANDIDATES`] (matching Docker Compose's
/// discovery behaviour).
pub(super) fn resolve_compose_file(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }

    for candidate in COMPOSE_FILE_CANDIDATES {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Ok(p);
        }
    }

    bail!(
        "no compose file found. Searched for: {}.\n\
         Use -f to specify one explicitly.",
        COMPOSE_FILE_CANDIDATES.join(", ")
    );
}

/// Resolve the stack name from explicit flag or parent directory of compose file.
pub(super) fn resolve_stack_name(
    explicit: Option<&str>,
    compose_path: &std::path::Path,
) -> anyhow::Result<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }

    // Use the parent directory name of the compose file.
    let parent = compose_path
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));

    parent.ok_or_else(|| anyhow::anyhow!("cannot determine stack name; use --name"))
}
