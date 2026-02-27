use vz_runtime_proto::runtime_v2;

pub(in crate::grpc) const SANDBOX_SHELL_SESSION_ENV_KEY: &str = "VZ_SANDBOX_SHELL_SESSION";

pub(in crate::grpc) fn normalize_optional_wire_field(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

pub(in crate::grpc) fn create_open_lease_request_hash(sandbox_id: &str, ttl_secs: u64) -> String {
    format!("sandbox_id={sandbox_id};ttl_secs={ttl_secs}")
}

pub(in crate::grpc) fn create_checkpoint_request_hash(
    sandbox_id: &str,
    checkpoint_class: &str,
    compatibility_fingerprint: &str,
) -> String {
    format!(
        "sandbox_id={};class={};fingerprint={}",
        sandbox_id,
        checkpoint_class.trim(),
        compatibility_fingerprint.trim()
    )
}

pub(in crate::grpc) fn create_fork_checkpoint_request_hash(
    parent_checkpoint_id: &str,
    new_sandbox_id: &str,
) -> String {
    format!(
        "parent_checkpoint_id={};new_sandbox_id={}",
        parent_checkpoint_id.trim(),
        new_sandbox_id.trim()
    )
}

pub(in crate::grpc) fn create_sandbox_request_hash(
    request: &runtime_v2::CreateSandboxRequest,
    cpus: Option<u8>,
) -> String {
    let mut labels: Vec<_> = request.labels.iter().collect();
    labels.sort_by(|left, right| left.0.cmp(right.0));
    let mut hash = format!(
        "stack_name={};cpus={};memory_mb={};",
        request.stack_name.trim(),
        cpus.map(u64::from).unwrap_or(0),
        request.memory_mb
    );
    for (key, value) in labels {
        hash.push_str(key);
        hash.push('=');
        hash.push_str(value);
        hash.push(';');
    }
    hash
}

pub(in crate::grpc) fn create_container_request_hash(
    request: &runtime_v2::CreateContainerRequest,
    sandbox_id: &str,
    image_digest: &str,
) -> String {
    let mut env_entries: Vec<_> = request.env.iter().collect();
    env_entries.sort_by(|left, right| left.0.cmp(right.0));

    let mut hash = format!(
        "sandbox_id={};image_digest={};cwd={};user={};",
        sandbox_id,
        image_digest,
        request.cwd.trim(),
        request.user.trim()
    );
    for cmd in &request.cmd {
        hash.push_str("cmd=");
        hash.push_str(cmd.trim());
        hash.push(';');
    }
    for (key, value) in env_entries {
        hash.push_str("env:");
        hash.push_str(key);
        hash.push('=');
        hash.push_str(value.trim());
        hash.push(';');
    }
    hash
}

pub(in crate::grpc) fn create_execution_request_hash(
    request: &runtime_v2::CreateExecutionRequest,
) -> String {
    let mut hash = format!(
        "container_id={};pty_mode={};timeout_secs={};",
        request.container_id.trim(),
        request.pty_mode,
        request.timeout_secs
    );

    for cmd in &request.cmd {
        hash.push_str("cmd=");
        hash.push_str(cmd.trim());
        hash.push(';');
    }
    for arg in &request.args {
        hash.push_str("arg=");
        hash.push_str(arg.trim());
        hash.push(';');
    }

    let mut env_entries: Vec<_> = request.env_override.iter().collect();
    env_entries.sort_by(|left, right| left.0.cmp(right.0));
    for (key, value) in env_entries {
        hash.push_str("env:");
        hash.push_str(key);
        hash.push('=');
        hash.push_str(value.trim());
        hash.push(';');
    }

    hash
}
