use super::*;

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("config", &self.config)
            .field("data_dir", &self.config.data_dir)
            .finish()
    }
}

pub(super) fn resolve_container_lifecycle(
    oci_annotations: &[(String, String)],
    default_class: ContainerLifecycleClass,
    default_auto_remove: bool,
) -> Result<ActiveContainerLifecycle, OciError> {
    let mut class = None;
    let mut auto_remove = None;

    for (key, value) in oci_annotations {
        if key == OCI_ANNOTATION_CONTAINER_CLASS {
            class = Some(parse_container_lifecycle_class(value)?);
            continue;
        }

        if key == OCI_ANNOTATION_AUTO_REMOVE {
            auto_remove = Some(parse_auto_remove_flag(value)?);
        }
    }

    Ok(ActiveContainerLifecycle {
        class: class.unwrap_or(default_class),
        auto_remove: auto_remove.unwrap_or(default_auto_remove),
    })
}

pub(super) fn parse_container_lifecycle_class(
    raw: &str,
) -> Result<ContainerLifecycleClass, OciError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "workspace" => Ok(ContainerLifecycleClass::Workspace),
        "service" => Ok(ContainerLifecycleClass::Service),
        "ephemeral" => Ok(ContainerLifecycleClass::Ephemeral),
        other => Err(OciError::InvalidConfig(format!(
            "invalid OCI annotation '{OCI_ANNOTATION_CONTAINER_CLASS}={other}'; expected one of: workspace, service, ephemeral"
        ))),
    }
}

pub(super) fn parse_auto_remove_flag(raw: &str) -> Result<bool, OciError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(OciError::InvalidConfig(format!(
            "invalid OCI annotation '{OCI_ANNOTATION_AUTO_REMOVE}={other}'; expected true or false"
        ))),
    }
}

pub(super) fn parse_compose_log_rotation(
    oci_annotations: &[(String, String)],
) -> Result<Option<ComposeLogRotation>, OciError> {
    let mut logging_driver = None;
    let mut logging_options_raw = None;
    for (key, value) in oci_annotations {
        if key == OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER {
            logging_driver = Some(value.as_str());
            continue;
        }
        if key == OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS {
            logging_options_raw = Some(value.as_str());
        }
    }

    let Some(driver) = logging_driver else {
        return Ok(None);
    };
    let normalized_driver = driver.trim().to_ascii_lowercase();
    if normalized_driver == "none" {
        return Ok(None);
    }
    if normalized_driver != "json-file" && normalized_driver != "local" {
        return Ok(None);
    }

    let options = parse_compose_logging_options(logging_options_raw.unwrap_or_default())?;
    if options.contains_key("labels") {
        return Err(OciError::InvalidConfig(
            "compose logging option `labels` is not supported in runtime log capture".to_string(),
        ));
    }
    if options.contains_key("tag") {
        return Err(OciError::InvalidConfig(
            "compose logging option `tag` is not supported in runtime log capture".to_string(),
        ));
    }

    let Some(max_size_raw) = options.get("max-size") else {
        return Ok(None);
    };
    let max_size_bytes = parse_compose_log_size_bytes(max_size_raw).ok_or_else(|| {
        OciError::InvalidConfig(format!(
            "invalid OCI annotation `{OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS}` max-size `{max_size_raw}`"
        ))
    })?;

    let max_files = match options.get("max-file") {
        Some(raw) => {
            let parsed = raw.parse::<u32>().map_err(|_| {
                OciError::InvalidConfig(format!(
                    "invalid OCI annotation `{OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS}` max-file `{raw}`"
                ))
            })?;
            if parsed == 0 {
                return Err(OciError::InvalidConfig(
                    "compose logging option `max-file` must be at least 1".to_string(),
                ));
            }
            parsed
        }
        None => 1,
    };

    Ok(Some(ComposeLogRotation {
        max_size_bytes,
        max_files,
    }))
}

pub(super) fn parse_compose_logging_options(
    raw: &str,
) -> Result<HashMap<String, String>, OciError> {
    let mut parsed = HashMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (key, value) = trimmed.split_once('=').ok_or_else(|| {
            OciError::InvalidConfig(format!(
                "invalid OCI annotation `{OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS}` entry `{trimmed}`"
            ))
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(OciError::InvalidConfig(format!(
                "invalid OCI annotation `{OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS}` entry `{trimmed}`"
            )));
        }

        parsed.insert(key.to_string(), value.to_string());
    }

    Ok(parsed)
}

pub(super) fn parse_compose_log_size_bytes(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut boundary = trimmed.len();
    for (idx, ch) in trimmed.char_indices() {
        if !ch.is_ascii_digit() {
            boundary = idx;
            break;
        }
    }
    if boundary == 0 {
        return None;
    }

    let quantity = trimmed[..boundary].parse::<u64>().ok()?;
    if quantity == 0 {
        return None;
    }

    let unit = trimmed[boundary..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1_u64,
        "k" | "kb" | "ki" | "kib" => 1024_u64,
        "m" | "mb" | "mi" | "mib" => 1024_u64 * 1024_u64,
        "g" | "gb" | "gi" | "gib" => 1024_u64 * 1024_u64 * 1024_u64,
        _ => return None,
    };

    quantity.checked_mul(multiplier)
}

pub(super) fn resolve_run_config(
    image_config: ImageConfigSummary,
    run: RunConfig,
    container_id: &str,
) -> Result<RunConfig, OciError> {
    let RunConfig {
        cmd: run_cmd,
        init_process,
        working_dir: run_working_dir,
        env: run_env,
        user: run_user,
        ports,
        mounts,
        cpus,
        memory_mb,
        network_enabled,
        serial_log_file,
        timeout,
        execution_mode,
        container_id: _,
        oci_annotations,
        extra_hosts,
        network_namespace_path,
        cpu_quota: _,
        cpu_period: _,
        capture_logs,
        cap_add,
        cap_drop,
        privileged,
        read_only_rootfs,
        sysctls,
        ulimits,
        pids_limit,
        hostname,
        domainname,
        stop_signal: _,
        stop_grace_period_secs: _,
        share_host_network,
        mount_tag_offset: _,
    } = run;

    let resolved_cmd = image_config
        .resolve_cmd(&run_cmd)
        .ok_or_else(|| OciError::InvalidConfig("run command must not be empty".to_string()))?;

    let resolved_env = image_config.resolve_env(&run_env, container_id);
    let working_dir = image_config.resolve_working_dir(run_working_dir.as_deref());
    let user = image_config.resolve_user(run_user.as_deref());
    let _ = parse_compose_log_rotation(&oci_annotations)?;

    if init_process.as_ref().is_some_and(Vec::is_empty) {
        return Err(OciError::InvalidConfig(
            "init process must not be empty".to_string(),
        ));
    }

    Ok(RunConfig {
        cmd: resolved_cmd,
        working_dir,
        env: resolved_env,
        user,
        ports,
        mounts,
        cpus,
        memory_mb,
        network_enabled,
        serial_log_file,
        timeout,
        execution_mode,
        container_id: Some(container_id.to_string()),
        init_process,
        oci_annotations,
        extra_hosts,
        network_namespace_path,
        cpu_quota: None,
        cpu_period: None,
        capture_logs,
        cap_add,
        cap_drop,
        privileged,
        read_only_rootfs,
        sysctls,
        ulimits,
        pids_limit,
        hostname,
        domainname,
        stop_signal: None,
        stop_grace_period_secs: None,
        share_host_network,
        mount_tag_offset: 0,
    })
}

pub(super) fn new_container_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = process::id();

    format!("vz-oci-{pid}-{nanos}")
}

pub(super) fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}
