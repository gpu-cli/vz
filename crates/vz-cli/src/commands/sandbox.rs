//! `vz sandbox` — sandbox lifecycle management commands.
//!
//! Provides sandbox CRUD and the default `vz` instant-sandbox experience.
//! Sandbox state persistence is routed through `vz-runtimed`.

#![allow(clippy::print_stdout)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::Args;
use reqwest::StatusCode as HttpStatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tonic::Code;
use vz_runtime_contract::{
    SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER, SANDBOX_LABEL_PROJECT_DIR,
    SANDBOX_LABEL_SPACE_CONFIG_PATH, SANDBOX_LABEL_SPACE_LIFECYCLE, SANDBOX_LABEL_SPACE_MODE,
    SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX, SANDBOX_LABEL_SPACE_SERVICE_STATE_PREFIX,
    SANDBOX_LABEL_SPACE_WORKTREE_ID, SANDBOX_LABEL_SPACE_WORKTREE_NAMESPACE,
    SANDBOX_SPACE_LIFECYCLE_EPHEMERAL, SANDBOX_SPACE_LIFECYCLE_PERSISTENT,
    SANDBOX_SPACE_MODE_REQUIRED, Sandbox, SandboxBackend, SandboxSpec, SandboxState,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    default_state_db_path, runtime_api_base_url,
};
use super::space_cache_key::{
    SPACE_CACHE_KEY_SCHEMA_VERSION, SpaceCacheIndex, SpaceCacheKey, SpaceCacheKeyMaterial,
    SpaceCacheLookup, SpaceCacheRuntimeIdentity,
};
use super::space_cache_trust::{SpaceRemoteCacheTrustConfig, SpaceRemoteCacheVerificationOutcome};

const SPACE_CONFIG_FILE: &str = "vz.json";
const SPACE_CACHE_INDEX_FILE: &str = "space-cache-index.json";

#[derive(Debug, Clone)]
struct SpaceConfig {
    config_path: PathBuf,
    external_secret_env: BTreeMap<String, String>,
    cache_definitions: Vec<SpaceCacheDefinition>,
}

#[derive(Debug, Clone)]
struct SpaceCacheDefinition {
    name: String,
    key_inputs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpaceWorktreeIdentity {
    root_path: PathBuf,
    worktree_id: String,
    service_namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorktreeNamespaceCollision {
    sandbox_id: String,
    namespace: String,
    existing_worktree_id: String,
    existing_project_dir: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpaceLifecycleMode {
    Persistent,
    Ephemeral,
}

impl SpaceLifecycleMode {
    const fn from_ephemeral_flag(ephemeral: bool) -> Self {
        if ephemeral {
            Self::Ephemeral
        } else {
            Self::Persistent
        }
    }

    const fn as_label_value(self) -> &'static str {
        match self {
            Self::Persistent => SANDBOX_SPACE_LIFECYCLE_PERSISTENT,
            Self::Ephemeral => SANDBOX_SPACE_LIFECYCLE_EPHEMERAL,
        }
    }
}

fn sandbox_backend_from_wire(backend: &str) -> SandboxBackend {
    match backend.trim().to_ascii_lowercase().as_str() {
        "macos_vz" | "macos-vz" => SandboxBackend::MacosVz,
        "linux_firecracker" | "linux-firecracker" => SandboxBackend::LinuxFirecracker,
        other => SandboxBackend::Other(other.to_string()),
    }
}

fn sandbox_state_from_wire(state: &str) -> anyhow::Result<SandboxState> {
    match state.trim().to_ascii_lowercase().as_str() {
        "creating" => Ok(SandboxState::Creating),
        "ready" => Ok(SandboxState::Ready),
        "draining" => Ok(SandboxState::Draining),
        "terminated" => Ok(SandboxState::Terminated),
        "failed" => Ok(SandboxState::Failed),
        other => Err(anyhow!("unsupported sandbox state from daemon: {other}")),
    }
}

fn execution_state_is_terminal(state: &str) -> bool {
    matches!(
        state.trim().to_ascii_lowercase().as_str(),
        "exited" | "failed" | "canceled"
    )
}

fn normalize_optional_label(value: Option<&String>) -> Option<String> {
    let raw = value?.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn apply_startup_selection_labels(
    labels: &mut BTreeMap<String, String>,
    base_image_ref: Option<String>,
    main_container: Option<String>,
) {
    if let Some(base_image_ref) = base_image_ref.as_deref().map(str::trim)
        && !base_image_ref.is_empty()
    {
        labels.insert(
            SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
            base_image_ref.to_string(),
        );
    }

    if let Some(main_container) = main_container.as_deref().map(str::trim)
        && !main_container.is_empty()
    {
        labels.insert(
            SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
            main_container.to_string(),
        );
    }
}

fn load_space_config(cwd: &Path) -> anyhow::Result<SpaceConfig> {
    let config_path = cwd.join(SPACE_CONFIG_FILE);
    if !config_path.is_file() {
        bail!(
            "spaces mode requires `{}` in {}. add a `{}` and retry",
            SPACE_CONFIG_FILE,
            cwd.display(),
            SPACE_CONFIG_FILE
        );
    }

    let raw = std::fs::read(&config_path).with_context(|| {
        format!(
            "failed to read required space definition file {}",
            config_path.display()
        )
    })?;
    let parsed = serde_json::from_slice::<serde_json::Value>(&raw).with_context(|| {
        format!(
            "invalid `{}` at {}: must contain valid JSON",
            SPACE_CONFIG_FILE,
            config_path.display()
        )
    })?;
    validate_space_config_has_no_inline_secrets(&parsed)?;
    let external_secret_env = parse_space_external_secret_env_refs(&parsed)?;
    let cache_definitions = parse_space_cache_definitions(&parsed)?;

    Ok(SpaceConfig {
        config_path,
        external_secret_env,
        cache_definitions,
    })
}

fn key_looks_secret(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase();
    [
        "secret",
        "password",
        "passwd",
        "token",
        "api_key",
        "apikey",
        "private_key",
        "access_key",
        "client_secret",
        "credential",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn collect_inline_secret_like_paths(
    value: &serde_json::Value,
    path: &str,
    in_external_secret_definitions: bool,
    violations: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                let child_path = if path == "$" {
                    format!("$.{key}")
                } else {
                    format!("{path}.{key}")
                };
                let child_in_external_secret_definitions =
                    in_external_secret_definitions || (path == "$" && key == "secrets");
                if !child_in_external_secret_definitions && key_looks_secret(key) {
                    violations.push(child_path.clone());
                }
                collect_inline_secret_like_paths(
                    child,
                    &child_path,
                    child_in_external_secret_definitions,
                    violations,
                );
            }
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let child_path = format!("{path}[{index}]");
                collect_inline_secret_like_paths(
                    item,
                    &child_path,
                    in_external_secret_definitions,
                    violations,
                );
            }
        }
        _ => {}
    }
}

fn validate_space_config_has_no_inline_secrets(parsed: &serde_json::Value) -> anyhow::Result<()> {
    let mut violations = Vec::new();
    collect_inline_secret_like_paths(parsed, "$", false, &mut violations);
    if violations.is_empty() {
        return Ok(());
    }

    let first_path = violations
        .first()
        .cloned()
        .unwrap_or_else(|| "$".to_string());
    bail!(
        "spaces mode config `{SPACE_CONFIG_FILE}` must not include inline secrets (first violation: {first_path}). define external secret sources under `secrets.<name>.env` or `secrets.<name>.environment`"
    );
}

fn ensure_valid_secret_label_segment(segment: &str, context: &str) -> anyhow::Result<()> {
    if segment.is_empty() {
        bail!("spaces mode {context} cannot be empty");
    }
    if !segment
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        bail!(
            "spaces mode {context} `{segment}` must contain only ASCII letters, digits, `_`, or `-`"
        );
    }
    Ok(())
}

fn parse_space_external_secret_env_refs(
    parsed: &serde_json::Value,
) -> anyhow::Result<BTreeMap<String, String>> {
    let Some(secrets_value) = parsed.get("secrets") else {
        return Ok(BTreeMap::new());
    };
    let secrets_map = secrets_value.as_object().ok_or_else(|| {
        anyhow!(
            "spaces mode config `{SPACE_CONFIG_FILE}` field `secrets` must be an object mapping secret names to external references"
        )
    })?;

    let mut refs = BTreeMap::new();
    for (secret_name, secret_def_value) in secrets_map {
        ensure_valid_secret_label_segment(secret_name, "secret name")?;
        let secret_def = secret_def_value.as_object().ok_or_else(|| {
            anyhow!(
                "spaces mode config `{SPACE_CONFIG_FILE}` secret `{secret_name}` must be an object with `env` or `environment`"
            )
        })?;
        if secret_def
            .keys()
            .any(|key| matches!(key.as_str(), "value" | "inline" | "literal" | "file"))
        {
            bail!(
                "spaces mode config `{SPACE_CONFIG_FILE}` secret `{secret_name}` cannot embed secret material; only external env references are allowed"
            );
        }
        let env_var_name = secret_def
            .get("env")
            .or_else(|| secret_def.get("environment"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "spaces mode config `{SPACE_CONFIG_FILE}` secret `{secret_name}` must define non-empty `env` or `environment`"
                )
            })?;
        ensure_valid_secret_label_segment(env_var_name, "secret env var name")?;
        for key in secret_def.keys() {
            if !matches!(key.as_str(), "env" | "environment" | "description") {
                bail!(
                    "spaces mode config `{SPACE_CONFIG_FILE}` secret `{secret_name}` has unsupported key `{key}`; allowed keys: env, environment, description"
                );
            }
        }
        refs.insert(secret_name.to_string(), env_var_name.to_string());
    }
    Ok(refs)
}

fn parse_space_cache_definitions(
    parsed: &serde_json::Value,
) -> anyhow::Result<Vec<SpaceCacheDefinition>> {
    let Some(caches_value) = parsed.get("caches") else {
        return Ok(Vec::new());
    };
    let caches_array = caches_value.as_array().ok_or_else(|| {
        anyhow!(
            "spaces mode config `{SPACE_CONFIG_FILE}` field `caches` must be an array of cache definitions"
        )
    })?;

    let mut names = BTreeSet::new();
    let mut definitions = Vec::new();

    for (index, cache_value) in caches_array.iter().enumerate() {
        let cache = cache_value.as_object().ok_or_else(|| {
            anyhow!(
                "spaces mode config `{SPACE_CONFIG_FILE}` cache at index {index} must be an object"
            )
        })?;
        let name = cache
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "spaces mode config `{SPACE_CONFIG_FILE}` cache at index {index} must define non-empty `name`"
                )
            })?;
        ensure_valid_secret_label_segment(name, "cache name")?;
        if !names.insert(name.to_string()) {
            bail!("spaces mode config `{SPACE_CONFIG_FILE}` defines duplicate cache name `{name}`");
        }

        let key_value = cache.get("key").ok_or_else(|| {
            anyhow!(
                "spaces mode config `{SPACE_CONFIG_FILE}` cache `{name}` must define `key` as a path string or array of path strings"
            )
        })?;
        let raw_inputs: Vec<String> = match key_value {
            serde_json::Value::String(path) => vec![path.clone()],
            serde_json::Value::Array(items) => items
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        anyhow!(
                            "spaces mode config `{SPACE_CONFIG_FILE}` cache `{name}` key entries must be strings"
                        )
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            _ => {
                bail!(
                    "spaces mode config `{SPACE_CONFIG_FILE}` cache `{name}` has invalid `key`; expected string or array of strings"
                )
            }
        };
        if raw_inputs.is_empty() {
            bail!(
                "spaces mode config `{SPACE_CONFIG_FILE}` cache `{name}` must define at least one key input path"
            );
        }

        let mut normalized_inputs = BTreeSet::new();
        for raw in raw_inputs {
            let normalized = raw.trim();
            if normalized.is_empty() {
                bail!(
                    "spaces mode config `{SPACE_CONFIG_FILE}` cache `{name}` key paths cannot be empty"
                );
            }
            normalized_inputs.insert(normalized.to_string());
        }

        definitions.push(SpaceCacheDefinition {
            name: name.to_string(),
            key_inputs: normalized_inputs.into_iter().collect(),
        });
    }

    Ok(definitions)
}

fn apply_space_external_secret_labels(
    labels: &mut BTreeMap<String, String>,
    external_secret_env: &BTreeMap<String, String>,
) {
    for (secret_name, env_var_name) in external_secret_env {
        labels.insert(
            format!("{SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX}{secret_name}"),
            env_var_name.to_string(),
        );
    }
}

fn space_cache_index_path(state_db: &Path) -> PathBuf {
    if let Some(parent) = state_db.parent() {
        parent.join(SPACE_CACHE_INDEX_FILE)
    } else {
        PathBuf::from(SPACE_CACHE_INDEX_FILE)
    }
}

fn sha256_file_hex(path: &Path) -> anyhow::Result<String> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn build_space_cache_keys(
    cwd: &Path,
    space_config: &SpaceConfig,
    cpus: u8,
    memory_mb: u64,
    base_image_ref: Option<&str>,
    main_container: Option<&str>,
) -> anyhow::Result<Vec<SpaceCacheKey>> {
    if space_config.cache_definitions.is_empty() {
        return Ok(Vec::new());
    }

    let canonical_project_root = std::fs::canonicalize(cwd)
        .with_context(|| format!("failed to resolve workspace root {}", cwd.display()))?;
    let canonical_config_path =
        std::fs::canonicalize(&space_config.config_path).with_context(|| {
            format!(
                "failed to resolve space config path {}",
                space_config.config_path.display()
            )
        })?;

    let runtime_identity = SpaceCacheRuntimeIdentity {
        base_image_ref: base_image_ref
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        main_container: main_container
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        cpus,
        memory_mb,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
    };

    let mut keys = Vec::new();
    for cache in &space_config.cache_definitions {
        let mut input_hashes = BTreeMap::new();
        for key_input in &cache.key_inputs {
            let key_path = cwd.join(key_input);
            if !key_path.is_file() {
                bail!(
                    "spaces cache `{}` key input `{}` is missing or not a file",
                    cache.name,
                    key_path.display()
                );
            }
            let digest = sha256_file_hex(&key_path).with_context(|| {
                format!(
                    "failed to hash spaces cache key input `{}` for cache `{}`",
                    key_path.display(),
                    cache.name
                )
            })?;
            input_hashes.insert(key_input.to_string(), digest);
        }

        keys.push(SpaceCacheKey::from_material(SpaceCacheKeyMaterial {
            cache_name: cache.name.to_string(),
            project_root: canonical_project_root.to_string_lossy().to_string(),
            config_path: canonical_config_path.to_string_lossy().to_string(),
            input_hashes,
            runtime: runtime_identity.clone(),
        })?);
    }

    Ok(keys)
}

fn update_space_cache_index(state_db: &Path, cache_keys: &[SpaceCacheKey]) -> anyhow::Result<()> {
    if cache_keys.is_empty() {
        return Ok(());
    }

    let remote_cache_trust = SpaceRemoteCacheTrustConfig::from_env()?;
    let index_path = space_cache_index_path(state_db);
    let mut index = SpaceCacheIndex::load(&index_path)?;
    let invalidated = index.invalidate_for_schema(SPACE_CACHE_KEY_SCHEMA_VERSION);
    if invalidated > 0 {
        println!(
            "[cache] invalidated {invalidated} entries due to schema v{SPACE_CACHE_KEY_SCHEMA_VERSION}"
        );
    }

    for key in cache_keys {
        let lookup = index.lookup(key);
        match lookup {
            SpaceCacheLookup::Hit => {
                println!("[cache:{}] hit {}", key.cache_name, key.digest_hex);
            }
            SpaceCacheLookup::MissNotFound => {
                println!("[cache:{}] miss (cold) {}", key.cache_name, key.digest_hex);
            }
            SpaceCacheLookup::MissKeyMismatch => {
                println!(
                    "[cache:{}] miss (dimension change) {}",
                    key.cache_name, key.digest_hex
                );
            }
            SpaceCacheLookup::MissVersionMismatch { requested, stored } => {
                println!(
                    "[cache:{}] miss (schema mismatch stored=v{stored} requested=v{requested}) {}",
                    key.cache_name, key.digest_hex
                );
            }
        }
        if !matches!(lookup, SpaceCacheLookup::Hit)
            && let Some(remote_cache_trust) = remote_cache_trust.as_ref()
        {
            match remote_cache_trust.verify_key(key) {
                SpaceRemoteCacheVerificationOutcome::Verified { .. } => {
                    println!(
                        "[cache:{}] remote verified {}",
                        key.cache_name, key.digest_hex
                    );
                }
                SpaceRemoteCacheVerificationOutcome::Miss(reason) => {
                    println!(
                        "[cache:{}] remote miss ({}) {}",
                        key.cache_name,
                        reason.diagnostic(),
                        key.digest_hex
                    );
                }
            }
        }
        index.upsert(key.clone());
    }

    index.save(&index_path)
}

fn git_rev_parse_value(cwd: &Path, arg: &str) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .arg("rev-parse")
        .arg(arg)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8(output.stdout).ok()?;
    let normalized = raw.trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn sanitize_namespace_segment(raw: &str) -> String {
    let mut sanitized = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else {
            sanitized.push('_');
        }
    }
    let trimmed = sanitized.trim_matches('_');
    if trimmed.is_empty() {
        "space".to_string()
    } else {
        trimmed.to_string()
    }
}

fn derive_space_worktree_identity(cwd: &Path) -> anyhow::Result<SpaceWorktreeIdentity> {
    let root_hint = git_rev_parse_value(cwd, "--show-toplevel")
        .map(PathBuf::from)
        .unwrap_or_else(|| cwd.to_path_buf());
    let root_path = std::fs::canonicalize(&root_hint)
        .with_context(|| format!("failed to resolve worktree root {}", root_hint.display()))?;

    let mut hasher = Sha256::new();
    hasher.update(root_path.to_string_lossy().as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    let short = &digest[..12];
    let root_leaf = root_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("space");
    let root_segment = sanitize_namespace_segment(root_leaf);
    let worktree_id = format!("{root_segment}-{short}");
    let service_namespace = format!("wt_{short}");

    Ok(SpaceWorktreeIdentity {
        root_path,
        worktree_id,
        service_namespace,
    })
}

fn default_worktree_service_state_defaults(service_namespace: &str) -> BTreeMap<String, String> {
    let namespace = sanitize_namespace_segment(service_namespace);
    BTreeMap::from([
        ("postgres.schema".to_string(), namespace.clone()),
        ("mysql.database".to_string(), namespace.clone()),
        ("redis.key_prefix".to_string(), format!("{namespace}:")),
    ])
}

fn service_state_label_key(suffix: &str) -> String {
    format!("{SANDBOX_LABEL_SPACE_SERVICE_STATE_PREFIX}{suffix}")
}

fn apply_worktree_service_state_labels(
    labels: &mut BTreeMap<String, String>,
    service_state_defaults: &BTreeMap<String, String>,
) {
    for (suffix, value) in service_state_defaults {
        labels.insert(service_state_label_key(suffix), value.clone());
    }
}

fn find_worktree_namespace_collision(
    sandboxes: &[Sandbox],
    namespace: &str,
    worktree_id: &str,
) -> Option<WorktreeNamespaceCollision> {
    for sandbox in sandboxes {
        if sandbox.state.is_terminal() {
            continue;
        }
        let Some(existing_namespace) = sandbox.labels.get(SANDBOX_LABEL_SPACE_WORKTREE_NAMESPACE)
        else {
            continue;
        };
        if existing_namespace != namespace {
            continue;
        }
        let Some(existing_worktree_id) = sandbox.labels.get(SANDBOX_LABEL_SPACE_WORKTREE_ID) else {
            continue;
        };
        if existing_worktree_id == worktree_id {
            continue;
        }

        return Some(WorktreeNamespaceCollision {
            sandbox_id: sandbox.sandbox_id.clone(),
            namespace: existing_namespace.to_string(),
            existing_worktree_id: existing_worktree_id.to_string(),
            existing_project_dir: sandbox.labels.get(SANDBOX_LABEL_PROJECT_DIR).cloned(),
        });
    }
    None
}

async fn ensure_worktree_namespace_not_colliding(
    state_db: &Path,
    namespace: &str,
    worktree_id: &str,
) -> anyhow::Result<()> {
    let sandboxes = daemon_list_sandboxes(state_db).await?;
    let Some(collision) = find_worktree_namespace_collision(&sandboxes, namespace, worktree_id)
    else {
        return Ok(());
    };

    let project_dir = collision
        .existing_project_dir
        .as_deref()
        .unwrap_or("<unknown>");
    bail!(
        "worktree namespace collision detected for `{namespace}`. active sandbox `{}` already owns this namespace with worktree id `{}` (project `{project_dir}`). terminate that sandbox before retrying (`vz rm {}`) to avoid shared-service state bleed",
        collision.sandbox_id,
        collision.existing_worktree_id,
        collision.sandbox_id
    );
}

fn enforce_btrfs_workspace_preflight(workspace_root: &Path) -> anyhow::Result<()> {
    if !workspace_root.is_dir() {
        bail!(
            "workspace path is not a directory: {}",
            workspace_root.display()
        );
    }

    #[cfg(target_os = "linux")]
    {
        if path_is_on_btrfs(workspace_root)? {
            return Ok(());
        }
        bail!(
            "spaces mode requires btrfs workspace storage; `{}` is not on btrfs",
            workspace_root.display()
        );
    }

    #[cfg(not(target_os = "linux"))]
    {
        bail!(
            "spaces mode requires Linux btrfs workspace storage; current platform `{}` is unsupported",
            std::env::consts::OS
        );
    }
}

#[cfg(target_os = "linux")]
fn path_is_on_btrfs(path: &Path) -> anyhow::Result<bool> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    const BTRFS_SUPER_MAGIC: libc::c_long = 0x9123_683E;

    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("failed to resolve workspace path {}", path.display()))?;
    let path_cstr = CString::new(canonical.as_os_str().as_bytes()).with_context(|| {
        format!(
            "workspace path contains unsupported null byte: {}",
            canonical.display()
        )
    })?;

    #[allow(unsafe_code)]
    let f_type = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::statfs(path_cstr.as_ptr(), &mut stat) != 0 {
            let io_error = std::io::Error::last_os_error();
            return Err(anyhow!(
                "failed to inspect workspace filesystem for {}: {}",
                canonical.display(),
                io_error
            ));
        }
        stat.f_type as libc::c_long
    };

    Ok(f_type == BTRFS_SUPER_MAGIC)
}

fn sandbox_from_proto(payload: runtime_v2::SandboxPayload) -> anyhow::Result<Sandbox> {
    let labels: BTreeMap<String, String> = payload.labels.into_iter().collect();
    let base_image_ref = normalize_optional_label(labels.get(SANDBOX_LABEL_BASE_IMAGE_REF));
    let main_container = normalize_optional_label(labels.get(SANDBOX_LABEL_MAIN_CONTAINER));
    Ok(Sandbox {
        sandbox_id: payload.sandbox_id,
        backend: sandbox_backend_from_wire(&payload.backend),
        spec: SandboxSpec {
            cpus: if payload.cpus == 0 {
                None
            } else {
                Some(payload.cpus as u8)
            },
            memory_mb: if payload.memory_mb == 0 {
                None
            } else {
                Some(payload.memory_mb)
            },
            base_image_ref,
            main_container,
            network_profile: None,
            volume_mounts: Vec::new(),
        },
        state: sandbox_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
        labels,
    })
}

#[derive(Debug, Deserialize)]
struct ApiErrorPayload {
    code: String,
    message: String,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct ApiErrorEnvelope {
    error: ApiErrorPayload,
}

#[derive(Debug, Deserialize)]
struct ApiSandboxPayload {
    sandbox_id: String,
    backend: String,
    state: String,
    cpus: Option<u8>,
    memory_mb: Option<u64>,
    base_image_ref: Option<String>,
    main_container: Option<String>,
    created_at: u64,
    updated_at: u64,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ApiSandboxResponse {
    sandbox: ApiSandboxPayload,
}

#[derive(Debug, Deserialize)]
struct ApiSandboxListResponse {
    sandboxes: Vec<ApiSandboxPayload>,
}

#[derive(Debug, Serialize)]
struct ApiCreateSandboxRequest {
    stack_name: String,
    cpus: u8,
    memory_mb: u64,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ApiOpenSandboxShellPayload {
    sandbox_id: String,
    container_id: String,
    #[serde(default)]
    cmd: Vec<String>,
    #[serde(default)]
    args: Vec<String>,
    execution_id: String,
}

#[derive(Debug, Deserialize)]
struct ApiOpenSandboxShellResponse {
    shell: ApiOpenSandboxShellPayload,
}

#[derive(Debug, Serialize)]
struct ApiCloseSandboxShellRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiCloseSandboxShellPayload {
    sandbox_id: String,
    execution_id: String,
}

#[derive(Debug, Deserialize)]
struct ApiCloseSandboxShellResponse {
    shell: ApiCloseSandboxShellPayload,
}

#[derive(Debug, Deserialize)]
struct ApiExecutionPayload {
    state: String,
    exit_code: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct ApiExecutionResponse {
    execution: ApiExecutionPayload,
}

#[derive(Debug, Serialize)]
struct ApiWriteExecStdinRequest {
    data: String,
}

#[derive(Debug, Serialize)]
struct ApiResizeExecRequest {
    cols: u16,
    rows: u16,
}

#[derive(Debug, Deserialize)]
struct ApiExecutionOutputEvent {
    event: String,
    #[serde(default)]
    data_base64: Option<String>,
    #[serde(default)]
    exit_code: Option<i32>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiStreamErrorBody {
    code: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ApiExecutionOutputStreamError {
    request_id: String,
    error: ApiStreamErrorBody,
}

fn sandbox_from_api_payload(payload: ApiSandboxPayload) -> anyhow::Result<Sandbox> {
    let mut labels = payload.labels;

    if let Some(base_image_ref) = payload.base_image_ref.as_deref().map(str::trim)
        && !base_image_ref.is_empty()
    {
        labels
            .entry(SANDBOX_LABEL_BASE_IMAGE_REF.to_string())
            .or_insert_with(|| base_image_ref.to_string());
    }
    if let Some(main_container) = payload.main_container.as_deref().map(str::trim)
        && !main_container.is_empty()
    {
        labels
            .entry(SANDBOX_LABEL_MAIN_CONTAINER.to_string())
            .or_insert_with(|| main_container.to_string());
    }

    Ok(Sandbox {
        sandbox_id: payload.sandbox_id,
        backend: sandbox_backend_from_wire(&payload.backend),
        spec: SandboxSpec {
            cpus: payload.cpus,
            memory_mb: payload.memory_mb,
            base_image_ref: normalize_optional_label(labels.get(SANDBOX_LABEL_BASE_IMAGE_REF))
                .or(payload.base_image_ref),
            main_container: normalize_optional_label(labels.get(SANDBOX_LABEL_MAIN_CONTAINER))
                .or(payload.main_container),
            network_profile: None,
            volume_mounts: Vec::new(),
        },
        state: sandbox_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
        labels,
    })
}

fn runtime_api_url(path: &str) -> anyhow::Result<String> {
    let base = runtime_api_base_url()?;
    Ok(format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    ))
}

async fn api_error_response(response: reqwest::Response, context: &str) -> anyhow::Error {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    if let Ok(error) = serde_json::from_slice::<ApiErrorEnvelope>(&body) {
        return anyhow!(
            "{context}: api error {} {} (request_id={})",
            error.error.code,
            error.error.message,
            error.error.request_id
        );
    }

    let snippet = String::from_utf8_lossy(&body);
    anyhow!("{context}: api status {status} body={snippet}")
}

async fn api_list_sandboxes() -> anyhow::Result<Vec<Sandbox>> {
    let url = runtime_api_url("/v1/sandboxes")?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api list sandboxes")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list sandboxes via api").await);
    }

    let payload: ApiSandboxListResponse = response
        .json()
        .await
        .context("failed to decode api list sandboxes response")?;
    payload
        .sandboxes
        .into_iter()
        .map(sandbox_from_api_payload)
        .collect()
}

async fn api_get_sandbox(sandbox_id: &str) -> anyhow::Result<Option<Sandbox>> {
    let url = runtime_api_url(&format!("/v1/sandboxes/{sandbox_id}"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api get sandbox")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get sandbox via api").await);
    }

    let payload: ApiSandboxResponse = response
        .json()
        .await
        .context("failed to decode api get sandbox response")?;
    Ok(Some(sandbox_from_api_payload(payload.sandbox)?))
}

async fn api_create_sandbox(
    sandbox_id: &str,
    cpus: u8,
    memory: u64,
    labels: BTreeMap<String, String>,
) -> anyhow::Result<Sandbox> {
    let url = runtime_api_url("/v1/sandboxes")?;
    let request = ApiCreateSandboxRequest {
        stack_name: sandbox_id.to_string(),
        cpus,
        memory_mb: memory,
        labels,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api create sandbox")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to create sandbox via api").await);
    }

    let payload: ApiSandboxResponse = response
        .json()
        .await
        .context("failed to decode api create sandbox response")?;
    sandbox_from_api_payload(payload.sandbox)
}

async fn api_terminate_sandbox(sandbox_id: &str) -> anyhow::Result<Option<Sandbox>> {
    let url = runtime_api_url(&format!("/v1/sandboxes/{sandbox_id}"))?;
    let response = reqwest::Client::new()
        .delete(url)
        .send()
        .await
        .context("failed to call api terminate sandbox")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to terminate sandbox via api").await);
    }

    let payload: ApiSandboxResponse = response
        .json()
        .await
        .context("failed to decode api terminate sandbox response")?;
    Ok(Some(sandbox_from_api_payload(payload.sandbox)?))
}

async fn api_open_sandbox_shell(
    sandbox_id: &str,
) -> anyhow::Result<runtime_v2::OpenSandboxShellResponse> {
    let url = runtime_api_url(&format!("/v1/sandboxes/{sandbox_id}/shell/open"))?;
    let response = reqwest::Client::new()
        .post(url)
        .send()
        .await
        .context("failed to call api open sandbox shell")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to open sandbox shell via api").await);
    }

    let payload: ApiOpenSandboxShellResponse = response
        .json()
        .await
        .context("failed to decode api open sandbox shell response")?;
    Ok(runtime_v2::OpenSandboxShellResponse {
        request_id: String::new(),
        sandbox_id: payload.shell.sandbox_id,
        container_id: payload.shell.container_id,
        cmd: payload.shell.cmd,
        args: payload.shell.args,
        execution_id: payload.shell.execution_id,
    })
}

async fn api_close_sandbox_shell(
    sandbox_id: &str,
    execution_id: Option<&str>,
) -> anyhow::Result<runtime_v2::CloseSandboxShellResponse> {
    let url = runtime_api_url(&format!("/v1/sandboxes/{sandbox_id}/shell/close"))?;
    let request = ApiCloseSandboxShellRequest {
        execution_id: execution_id.map(ToOwned::to_owned),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to call api close sandbox shell")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to close sandbox shell via api").await);
    }

    let payload: ApiCloseSandboxShellResponse = response
        .json()
        .await
        .context("failed to decode api close sandbox shell response")?;
    Ok(runtime_v2::CloseSandboxShellResponse {
        request_id: String::new(),
        sandbox_id: payload.shell.sandbox_id,
        execution_id: payload.shell.execution_id,
    })
}

async fn api_get_execution(execution_id: &str) -> anyhow::Result<Option<ApiExecutionPayload>> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api get execution")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get execution via api").await);
    }

    let payload: ApiExecutionResponse = response
        .json()
        .await
        .context("failed to decode api execution response")?;
    Ok(Some(payload.execution))
}

async fn api_write_exec_stdin(execution_id: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}/stdin"))?;
    let body = ApiWriteExecStdinRequest {
        data: String::from_utf8_lossy(&bytes).to_string(),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api write execution stdin")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to write stdin via api").await);
    }
    Ok(())
}

async fn api_resize_exec_pty(execution_id: &str, cols: u16, rows: u16) -> anyhow::Result<()> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}/resize"))?;
    let body = ApiResizeExecRequest { cols, rows };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api resize execution pty")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to resize execution via api").await);
    }
    Ok(())
}

async fn api_stream_exec_output(execution_id: &str) -> anyhow::Result<reqwest::Response> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}/stream"))?;
    let response = reqwest::Client::new()
        .get(url)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .await
        .context("failed to call api execution stream")?;
    if !response.status().is_success() {
        return Err(
            api_error_response(response, "failed to stream execution output via api").await,
        );
    }
    Ok(response)
}

async fn daemon_grpc_list_sandboxes(state_db: &Path) -> anyhow::Result<Vec<Sandbox>> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let response = client
        .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
        .await
        .context("failed to list sandboxes via daemon")?;
    response
        .sandboxes
        .into_iter()
        .map(sandbox_from_proto)
        .collect()
}

async fn daemon_grpc_get_sandbox(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<Option<Sandbox>> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(response) => {
            let payload = response
                .sandbox
                .ok_or_else(|| anyhow!("daemon get_sandbox returned missing payload"))?;
            Ok(Some(sandbox_from_proto(payload)?))
        }
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => Ok(None),
        Err(error) => Err(anyhow!(error).context("failed to get sandbox via daemon")),
    }
}

async fn daemon_grpc_create_sandbox(
    state_db: &Path,
    sandbox_id: &str,
    cpus: u8,
    memory: u64,
    labels: BTreeMap<String, String>,
) -> anyhow::Result<Sandbox> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let mut stream = client
        .create_sandbox_stream(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: sandbox_id.to_string(),
            cpus: u32::from(cpus),
            memory_mb: memory,
            labels: labels.into_iter().collect(),
        })
        .await
        .context("failed to create sandbox via daemon")?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading create sandbox stream")?
    {
        match event.payload {
            Some(runtime_v2::create_sandbox_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }
    let completion = completion
        .ok_or_else(|| anyhow!("daemon create_sandbox stream ended without completion"))?;
    let response = completion
        .response
        .ok_or_else(|| anyhow!("daemon create_sandbox completion missing response payload"))?;
    let payload = response
        .sandbox
        .ok_or_else(|| anyhow!("daemon create_sandbox returned missing payload"))?;
    sandbox_from_proto(payload)
}

async fn daemon_grpc_terminate_sandbox(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<Option<Sandbox>> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    match client
        .terminate_sandbox_stream(runtime_v2::TerminateSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(mut stream) => {
            let mut completion = None;
            while let Some(event) = stream
                .message()
                .await
                .context("failed reading terminate sandbox stream")?
            {
                match event.payload {
                    Some(runtime_v2::terminate_sandbox_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::terminate_sandbox_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            let completion = completion.ok_or_else(|| {
                anyhow!("daemon terminate_sandbox stream ended without completion")
            })?;
            let response = completion.response.ok_or_else(|| {
                anyhow!("daemon terminate_sandbox completion missing response payload")
            })?;
            let payload = response
                .sandbox
                .ok_or_else(|| anyhow!("daemon terminate_sandbox returned missing payload"))?;
            Ok(Some(sandbox_from_proto(payload)?))
        }
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => Ok(None),
        Err(error) => Err(anyhow!(error).context("failed to terminate sandbox via daemon")),
    }
}

async fn daemon_list_sandboxes(state_db: &Path) -> anyhow::Result<Vec<Sandbox>> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => daemon_grpc_list_sandboxes(state_db).await,
        ControlPlaneTransport::ApiHttp => api_list_sandboxes().await,
    }
}

async fn daemon_get_sandbox(state_db: &Path, sandbox_id: &str) -> anyhow::Result<Option<Sandbox>> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => daemon_grpc_get_sandbox(state_db, sandbox_id).await,
        ControlPlaneTransport::ApiHttp => api_get_sandbox(sandbox_id).await,
    }
}

async fn daemon_create_sandbox(
    state_db: &Path,
    sandbox_id: &str,
    cpus: u8,
    memory: u64,
    labels: BTreeMap<String, String>,
) -> anyhow::Result<Sandbox> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            daemon_grpc_create_sandbox(state_db, sandbox_id, cpus, memory, labels).await
        }
        ControlPlaneTransport::ApiHttp => {
            api_create_sandbox(sandbox_id, cpus, memory, labels).await
        }
    }
}

async fn daemon_terminate_sandbox(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<Option<Sandbox>> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            daemon_grpc_terminate_sandbox(state_db, sandbox_id).await
        }
        ControlPlaneTransport::ApiHttp => api_terminate_sandbox(sandbox_id).await,
    }
}

async fn daemon_open_sandbox_shell(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<runtime_v2::OpenSandboxShellResponse> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(state_db).await?;
            let mut stream = client
                .open_sandbox_shell(runtime_v2::OpenSandboxShellRequest {
                    sandbox_id: sandbox_id.to_string(),
                    metadata: None,
                })
                .await
                .context("failed to open sandbox shell via daemon")?;
            let mut completion = None;
            while let Some(event) = stream
                .message()
                .await
                .context("failed reading open sandbox shell stream")?
            {
                match event.payload {
                    Some(runtime_v2::open_sandbox_shell_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            completion
                .ok_or_else(|| anyhow!("daemon open_sandbox_shell stream ended without completion"))
        }
        ControlPlaneTransport::ApiHttp => api_open_sandbox_shell(sandbox_id).await,
    }
}

async fn daemon_close_sandbox_shell(
    state_db: &Path,
    sandbox_id: &str,
    execution_id: Option<&str>,
) -> anyhow::Result<runtime_v2::CloseSandboxShellResponse> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(state_db).await?;
            let mut stream = client
                .close_sandbox_shell(runtime_v2::CloseSandboxShellRequest {
                    sandbox_id: sandbox_id.to_string(),
                    execution_id: execution_id.unwrap_or_default().to_string(),
                    metadata: None,
                })
                .await
                .context("failed to close sandbox shell via daemon")?;
            let mut completion = None;
            while let Some(event) = stream
                .message()
                .await
                .context("failed reading close sandbox shell stream")?
            {
                match event.payload {
                    Some(runtime_v2::close_sandbox_shell_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            completion.ok_or_else(|| {
                anyhow!("daemon close_sandbox_shell stream ended without completion")
            })
        }
        ControlPlaneTransport::ApiHttp => api_close_sandbox_shell(sandbox_id, execution_id).await,
    }
}

// ── Top-level argument types ────────────────────────────────────

/// Arguments for `vz ls`.
#[derive(Args, Debug)]
pub struct SandboxListArgs {
    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,

    /// Output as JSON.
    #[arg(long)]
    json: bool,
}

/// Arguments for `vz inspect`.
#[derive(Args, Debug)]
pub struct SandboxInspectArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

/// Arguments for `vz rm`.
#[derive(Args, Debug)]
pub struct SandboxTerminateArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

/// Arguments for `vz attach`.
#[derive(Args, Debug)]
pub struct SandboxAttachArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

/// Arguments for `vz close-shell`.
#[derive(Args, Debug)]
pub struct SandboxCloseShellArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Explicit execution identifier to close (defaults to active shell session).
    #[arg(long)]
    pub execution_id: Option<String>,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

// ── Default sandbox command (no subcommand) ─────────────────────

/// Handle the default `vz` command — create or resume a sandbox.
///
/// When invoked with no subcommand:
/// - `vz -c`: continue most recent sandbox for the current directory
/// - `vz -r <name>`: resume a specific sandbox by name or ID
/// - `vz`: create a new sandbox bound to the current directory
pub async fn cmd_default_sandbox(
    continue_last: bool,
    resume: Option<String>,
    name: Option<String>,
    ephemeral: bool,
    cpus: u8,
    memory: u64,
    base_image_ref: Option<String>,
    main_container: Option<String>,
) -> anyhow::Result<()> {
    let state_db = default_state_db_path();
    let cwd = std::env::current_dir().context("failed to get current directory")?;

    if (continue_last || resume.is_some()) && ephemeral {
        bail!("--ephemeral is only valid when creating a new sandbox");
    }
    if (continue_last || resume.is_some()) && (base_image_ref.is_some() || main_container.is_some())
    {
        bail!("--base-image and --main-container are only valid when creating a new sandbox");
    }

    if continue_last {
        return cmd_continue_sandbox(&state_db, &cwd).await;
    }

    if let Some(ref target) = resume {
        return cmd_resume_sandbox(&state_db, target).await;
    }

    // Create a new sandbox in spaces mode.
    let space_config = load_space_config(&cwd)?;
    enforce_btrfs_workspace_preflight(&cwd)?;
    cmd_create_sandbox(
        &state_db,
        &cwd,
        &space_config,
        name,
        SpaceLifecycleMode::from_ephemeral_flag(ephemeral),
        cpus,
        memory,
        base_image_ref,
        main_container,
    )
    .await
}

/// Continue the most recent sandbox for the current directory.
async fn cmd_continue_sandbox(state_db: &Path, cwd: &Path) -> anyhow::Result<()> {
    let sandboxes = daemon_list_sandboxes(state_db).await?;
    let cwd_str = cwd.to_string_lossy();

    // Find sandbox matching this directory.
    let matching: Vec<_> = sandboxes
        .iter()
        .filter(|s| {
            s.labels.get(SANDBOX_LABEL_PROJECT_DIR).map(|d| d.as_str()) == Some(&*cwd_str)
                && !s.state.is_terminal()
        })
        .collect();

    if let Some(sandbox) = matching.last() {
        println!("Resuming sandbox {}...", sandbox.sandbox_id);
        return attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id)
            .await
            .map(|_| ());
    }

    // Fall back to most recent non-terminal sandbox.
    let most_recent = sandboxes.iter().rev().find(|s| !s.state.is_terminal());

    match most_recent {
        Some(sandbox) => {
            println!("Resuming sandbox {}...", sandbox.sandbox_id);
            attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id)
                .await
                .map(|_| ())
        }
        None => bail!("no active sandboxes found; run `vz` to create one"),
    }
}

/// Resume a specific sandbox by name or ID.
async fn cmd_resume_sandbox(state_db: &Path, target: &str) -> anyhow::Result<()> {
    // Try exact ID match first.
    if let Some(sandbox) = daemon_get_sandbox(state_db, target).await? {
        if sandbox.state.is_terminal() {
            bail!("sandbox {target} is in terminal state");
        }
        println!("Resuming sandbox {target}...");
        return attach_to_sandbox_by_id(state_db, target).await.map(|_| ());
    }

    // Try name label match.
    let sandboxes = daemon_list_sandboxes(state_db).await?;
    let by_name: Vec<_> = sandboxes
        .iter()
        .filter(|s| {
            s.labels.get("name").map(|n| n.as_str()) == Some(target) && !s.state.is_terminal()
        })
        .collect();

    match by_name.last() {
        Some(sandbox) => {
            println!("Resuming sandbox {} ({target})...", sandbox.sandbox_id);
            attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id)
                .await
                .map(|_| ())
        }
        None => bail!("sandbox {target} not found"),
    }
}

/// Create a new sandbox and attach to it.
async fn cmd_create_sandbox(
    state_db: &Path,
    cwd: &Path,
    space_config: &SpaceConfig,
    name: Option<String>,
    lifecycle_mode: SpaceLifecycleMode,
    cpus: u8,
    memory: u64,
    base_image_ref: Option<String>,
    main_container: Option<String>,
) -> anyhow::Result<()> {
    let sandbox_id = generate_sandbox_id();
    let display_name = name.as_deref().unwrap_or(&sandbox_id);
    let worktree_identity = derive_space_worktree_identity(cwd)?;
    let worktree_service_defaults =
        default_worktree_service_state_defaults(worktree_identity.service_namespace.as_str());

    let mut labels = BTreeMap::new();
    labels.insert(
        SANDBOX_LABEL_PROJECT_DIR.to_string(),
        cwd.to_string_lossy().to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_SPACE_MODE.to_string(),
        SANDBOX_SPACE_MODE_REQUIRED.to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_SPACE_CONFIG_PATH.to_string(),
        space_config.config_path.to_string_lossy().to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_SPACE_WORKTREE_ID.to_string(),
        worktree_identity.worktree_id.to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_SPACE_WORKTREE_NAMESPACE.to_string(),
        worktree_identity.service_namespace.to_string(),
    );
    apply_worktree_service_state_labels(&mut labels, &worktree_service_defaults);
    labels.insert(
        SANDBOX_LABEL_SPACE_LIFECYCLE.to_string(),
        lifecycle_mode.as_label_value().to_string(),
    );
    apply_space_external_secret_labels(&mut labels, &space_config.external_secret_env);
    labels.insert("source".to_string(), "standalone".to_string());
    if let Some(ref n) = name {
        labels.insert("name".to_string(), n.clone());
    }
    apply_startup_selection_labels(&mut labels, base_image_ref, main_container);
    let cache_keys = build_space_cache_keys(
        cwd,
        space_config,
        cpus,
        memory,
        labels.get(SANDBOX_LABEL_BASE_IMAGE_REF).map(String::as_str),
        labels.get(SANDBOX_LABEL_MAIN_CONTAINER).map(String::as_str),
    )?;
    ensure_worktree_namespace_not_colliding(
        state_db,
        worktree_identity.service_namespace.as_str(),
        worktree_identity.worktree_id.as_str(),
    )
    .await?;
    update_space_cache_index(state_db, &cache_keys)?;

    // Ensure state directory exists.
    if let Some(parent) = state_db.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    println!("Booting sandbox {display_name}...");
    println!("Mounting {} → /workspace", cwd.display());
    println!(
        "Resolved worktree root {}",
        worktree_identity.root_path.display()
    );
    println!("Worktree namespace {}", worktree_identity.service_namespace);
    println!(
        "Shared service defaults: postgres.schema={}, mysql.database={}, redis.key_prefix={}",
        worktree_service_defaults
            .get("postgres.schema")
            .map(String::as_str)
            .unwrap_or_default(),
        worktree_service_defaults
            .get("mysql.database")
            .map(String::as_str)
            .unwrap_or_default(),
        worktree_service_defaults
            .get("redis.key_prefix")
            .map(String::as_str)
            .unwrap_or_default(),
    );
    println!(
        "Using space definition {}",
        space_config.config_path.display()
    );

    let sandbox =
        daemon_create_sandbox(state_db, &sandbox_id, cpus, memory, labels.clone()).await?;
    let attach_result = attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id).await;

    match lifecycle_mode {
        SpaceLifecycleMode::Persistent => {
            if let Err(error) = attach_result {
                print_space_recovery_guidance(
                    &sandbox.sandbox_id,
                    "space preserved after attach failure",
                );
                return Err(error);
            }
            Ok(())
        }
        SpaceLifecycleMode::Ephemeral => {
            let session_completion = match &attach_result {
                Ok(SandboxAttachOutcome::ExitedClean) => EphemeralSessionCompletion::CleanExit,
                Ok(SandboxAttachOutcome::Detached) => EphemeralSessionCompletion::Detached,
                Err(_) => EphemeralSessionCompletion::Failed,
            };
            let sandbox_snapshot = if session_completion == EphemeralSessionCompletion::CleanExit {
                daemon_get_sandbox(state_db, &sandbox.sandbox_id).await?
            } else {
                None
            };
            let decision =
                evaluate_ephemeral_cleanup_decision(session_completion, sandbox_snapshot.as_ref());
            match decision {
                EphemeralCleanupDecision::AutoCleanup => {
                    let _ = daemon_terminate_sandbox(state_db, &sandbox.sandbox_id).await?;
                    println!("Ephemeral sandbox {} cleaned up.", sandbox.sandbox_id);
                }
                EphemeralCleanupDecision::Preserve { reason } => {
                    eprintln!("Ephemeral cleanup skipped: {reason}");
                    print_space_recovery_guidance(&sandbox.sandbox_id, "space preserved");
                }
            }

            match attach_result {
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxAttachOutcome {
    ExitedClean,
    Detached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EphemeralSessionCompletion {
    CleanExit,
    Detached,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EphemeralCleanupDecision {
    AutoCleanup,
    Preserve { reason: String },
}

fn evaluate_ephemeral_cleanup_decision(
    session_completion: EphemeralSessionCompletion,
    sandbox: Option<&Sandbox>,
) -> EphemeralCleanupDecision {
    match session_completion {
        EphemeralSessionCompletion::Detached => EphemeralCleanupDecision::Preserve {
            reason: "session detached and remains active".to_string(),
        },
        EphemeralSessionCompletion::Failed => EphemeralCleanupDecision::Preserve {
            reason: "session ended with an error".to_string(),
        },
        EphemeralSessionCompletion::CleanExit => match sandbox {
            Some(sandbox) if !sandbox.state.is_terminal() => EphemeralCleanupDecision::AutoCleanup,
            Some(_) => EphemeralCleanupDecision::Preserve {
                reason: "sandbox is already terminal".to_string(),
            },
            None => EphemeralCleanupDecision::Preserve {
                reason: "sandbox no longer exists".to_string(),
            },
        },
    }
}

fn sandbox_recovery_commands(sandbox_id: &str) -> [String; 3] {
    [
        format!("vz attach {sandbox_id}"),
        format!("vz inspect {sandbox_id}"),
        format!("vz rm {sandbox_id}"),
    ]
}

fn print_space_recovery_guidance(sandbox_id: &str, context: &str) {
    eprintln!("Recovery ({context}):");
    for command in sandbox_recovery_commands(sandbox_id) {
        eprintln!("  {command}");
    }
}

enum AttachInputEvent {
    Bytes(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    Detach,
}

async fn attach_to_execution_interactive(
    state_db: &Path,
    execution_id: &str,
) -> anyhow::Result<SandboxAttachOutcome> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            attach_to_execution_interactive_daemon(state_db, execution_id).await
        }
        ControlPlaneTransport::ApiHttp => attach_to_execution_interactive_api(execution_id).await,
    }
}

async fn attach_to_execution_interactive_daemon(
    state_db: &Path,
    execution_id: &str,
) -> anyhow::Result<SandboxAttachOutcome> {
    use crossterm::event::{self, Event};
    use crossterm::terminal;
    use std::io::Write;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let execution_id = execution_id.to_string();
    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await
        .with_context(|| {
            format!("failed to stream sandbox execution output for `{execution_id}`")
        })?;

    terminal::enable_raw_mode().context("failed to enable raw mode")?;
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel::<AttachInputEvent>();
    let stop_input = Arc::new(AtomicBool::new(false));
    let stop_input_worker = Arc::clone(&stop_input);
    let input_handle = tokio::task::spawn_blocking(move || {
        let mut detach_prefix_pending = false;
        loop {
            if stop_input_worker.load(Ordering::Relaxed) {
                break;
            }

            match event::poll(std::time::Duration::from_millis(100)) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => break,
            }

            match event::read() {
                Ok(Event::Key(key_event)) => {
                    let bytes = key_event_to_bytes(&key_event);
                    if detach_prefix_pending {
                        detach_prefix_pending = false;
                        if is_detach_confirm(bytes.as_slice()) {
                            if input_tx.send(AttachInputEvent::Detach).is_err() {
                                break;
                            }
                            continue;
                        }
                        if input_tx.send(AttachInputEvent::Bytes(vec![0x10])).is_err() {
                            break;
                        }
                    } else if is_detach_prefix(bytes.as_slice()) {
                        detach_prefix_pending = true;
                        continue;
                    }

                    if bytes.is_empty() {
                        continue;
                    }
                    if input_tx.send(AttachInputEvent::Bytes(bytes)).is_err() {
                        break;
                    }
                }
                Ok(Event::Resize(new_cols, new_rows)) => {
                    if input_tx
                        .send(AttachInputEvent::Resize {
                            cols: new_cols,
                            rows: new_rows,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let interaction_result = async {
        let mut stdout = std::io::stdout();
        let mut stderr = std::io::stderr();
        let mut detached = false;
        let mut terminal_exit_code: Option<i32> = None;
        loop {
            tokio::select! {
                maybe_input = input_rx.recv() => {
                    let Some(input) = maybe_input else {
                        continue;
                    };
                    match input {
                        AttachInputEvent::Bytes(bytes) => {
                            let write_result = client
                                .write_exec_stdin(runtime_v2::WriteExecStdinRequest {
                                    execution_id: execution_id.clone(),
                                    data: bytes,
                                    metadata: None,
                                })
                                .await;
                            if let Err(status) = write_result {
                                if matches!(
                                    status,
                                    DaemonClientError::Grpc(ref grpc_status)
                                        if matches!(
                                            grpc_status.code(),
                                            Code::FailedPrecondition | Code::NotFound
                                        )
                                ) {
                                    if let Ok(response) = client
                                        .get_execution(runtime_v2::GetExecutionRequest {
                                            execution_id: execution_id.clone(),
                                            metadata: None,
                                        })
                                        .await
                                        && let Some(execution) = response.execution
                                        && execution_state_is_terminal(execution.state.as_str())
                                    {
                                        terminal_exit_code = Some(execution.exit_code);
                                        break;
                                    }
                                }
                                return Err(status)
                                    .with_context(|| format!("failed to write stdin to `{execution_id}`"));
                            }
                        }
                        AttachInputEvent::Resize { cols, rows } => {
                            let resize_result = client
                                .resize_exec_pty(runtime_v2::ResizeExecPtyRequest {
                                    execution_id: execution_id.clone(),
                                    cols: u32::from(cols),
                                    rows: u32::from(rows),
                                    metadata: None,
                                })
                                .await;
                            if let Err(status) = resize_result {
                                if matches!(
                                    status,
                                    DaemonClientError::Grpc(ref grpc_status)
                                        if matches!(
                                            grpc_status.code(),
                                            Code::FailedPrecondition | Code::NotFound
                                        )
                                ) {
                                    if let Ok(response) = client
                                        .get_execution(runtime_v2::GetExecutionRequest {
                                            execution_id: execution_id.clone(),
                                            metadata: None,
                                        })
                                        .await
                                        && let Some(execution) = response.execution
                                        && execution_state_is_terminal(execution.state.as_str())
                                    {
                                        terminal_exit_code = Some(execution.exit_code);
                                        break;
                                    }
                                }
                                return Err(status)
                                    .with_context(|| format!("failed to resize PTY for `{execution_id}`"));
                            }
                        }
                        AttachInputEvent::Detach => {
                            detached = true;
                            break;
                        }
                    }
                }
                maybe_event = stream.message() => {
                    let maybe_event = maybe_event
                        .with_context(|| format!("failed reading stream for `{execution_id}`"))?;
                    let Some(event) = maybe_event else {
                        break;
                    };
                    match event.payload {
                        Some(runtime_v2::exec_output_event::Payload::Stdout(chunk)) => {
                            if !chunk.is_empty() {
                                stdout
                                    .write_all(&chunk)
                                    .context("failed writing sandbox stdout")?;
                                stdout.flush().context("failed flushing sandbox stdout")?;
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::Stderr(chunk)) => {
                            if !chunk.is_empty() {
                                stderr
                                    .write_all(&chunk)
                                    .context("failed writing sandbox stderr")?;
                                stderr.flush().context("failed flushing sandbox stderr")?;
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                            terminal_exit_code = Some(code);
                            break;
                        }
                        Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
                            bail!("sandbox execution `{execution_id}` reported error: {message}");
                        }
                        None => {}
                    }
                }
            }
        }

        if terminal_exit_code.is_none() && !detached {
            if let Ok(response) = client
                .get_execution(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                })
                .await
                && let Some(execution) = response.execution
            {
                terminal_exit_code = Some(execution.exit_code);
            }
        }

        Ok::<_, anyhow::Error>((detached, terminal_exit_code))
    }
    .await;

    terminal::disable_raw_mode().ok();
    stop_input.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), input_handle).await;
    let (detached, terminal_exit_code) = interaction_result?;

    if detached {
        eprintln!("\nDetached (Ctrl-P Ctrl-Q). Session remains active.");
        return Ok(SandboxAttachOutcome::Detached);
    }

    if let Some(exit_code) = terminal_exit_code
        && exit_code != 0
    {
        bail!("sandbox shell exited with status {exit_code}");
    }

    println!();
    Ok(SandboxAttachOutcome::ExitedClean)
}

fn handle_api_execution_stream_event(
    execution_id: &str,
    payload_json: &str,
    stdout: &mut std::io::Stdout,
    stderr: &mut std::io::Stderr,
    terminal_exit_code: &mut Option<i32>,
) -> anyhow::Result<bool> {
    if let Ok(event) = serde_json::from_str::<ApiExecutionOutputEvent>(payload_json) {
        match event.event.as_str() {
            "stdout" => {
                if let Some(encoded) = event.data_base64 {
                    let chunk = BASE64_STANDARD.decode(encoded).with_context(|| {
                        format!(
                            "failed to decode stdout chunk from api stream for `{execution_id}`"
                        )
                    })?;
                    if !chunk.is_empty() {
                        use std::io::Write;
                        stdout
                            .write_all(&chunk)
                            .context("failed writing sandbox stdout")?;
                        stdout.flush().context("failed flushing sandbox stdout")?;
                    }
                }
                return Ok(false);
            }
            "stderr" => {
                if let Some(encoded) = event.data_base64 {
                    let chunk = BASE64_STANDARD.decode(encoded).with_context(|| {
                        format!(
                            "failed to decode stderr chunk from api stream for `{execution_id}`"
                        )
                    })?;
                    if !chunk.is_empty() {
                        use std::io::Write;
                        stderr
                            .write_all(&chunk)
                            .context("failed writing sandbox stderr")?;
                        stderr.flush().context("failed flushing sandbox stderr")?;
                    }
                }
                return Ok(false);
            }
            "exit_code" => {
                *terminal_exit_code = event.exit_code;
                return Ok(true);
            }
            "error" => {
                let message = event
                    .error
                    .unwrap_or_else(|| "unknown execution stream error".to_string());
                bail!("sandbox execution `{execution_id}` reported error: {message}");
            }
            _ => return Ok(false),
        }
    }

    if let Ok(error) = serde_json::from_str::<ApiExecutionOutputStreamError>(payload_json) {
        bail!(
            "sandbox execution stream failed: {} {} (request_id={})",
            error.error.code,
            error.error.message,
            error.request_id
        );
    }

    bail!("received unrecognized execution stream payload from api: {payload_json}");
}

async fn attach_to_execution_interactive_api(
    execution_id: &str,
) -> anyhow::Result<SandboxAttachOutcome> {
    use crossterm::event::{self, Event};
    use crossterm::terminal;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let execution_id = execution_id.to_string();
    let mut stream = api_stream_exec_output(&execution_id)
        .await
        .with_context(|| {
            format!("failed to stream sandbox execution output for `{execution_id}`")
        })?;

    terminal::enable_raw_mode().context("failed to enable raw mode")?;
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel::<AttachInputEvent>();
    let stop_input = Arc::new(AtomicBool::new(false));
    let stop_input_worker = Arc::clone(&stop_input);
    let input_handle = tokio::task::spawn_blocking(move || {
        let mut detach_prefix_pending = false;
        loop {
            if stop_input_worker.load(Ordering::Relaxed) {
                break;
            }

            match event::poll(std::time::Duration::from_millis(100)) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => break,
            }

            match event::read() {
                Ok(Event::Key(key_event)) => {
                    let bytes = key_event_to_bytes(&key_event);
                    if detach_prefix_pending {
                        detach_prefix_pending = false;
                        if is_detach_confirm(bytes.as_slice()) {
                            if input_tx.send(AttachInputEvent::Detach).is_err() {
                                break;
                            }
                            continue;
                        }
                        if input_tx.send(AttachInputEvent::Bytes(vec![0x10])).is_err() {
                            break;
                        }
                    } else if is_detach_prefix(bytes.as_slice()) {
                        detach_prefix_pending = true;
                        continue;
                    }

                    if bytes.is_empty() {
                        continue;
                    }
                    if input_tx.send(AttachInputEvent::Bytes(bytes)).is_err() {
                        break;
                    }
                }
                Ok(Event::Resize(new_cols, new_rows)) => {
                    if input_tx
                        .send(AttachInputEvent::Resize {
                            cols: new_cols,
                            rows: new_rows,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let interaction_result = async {
        let mut stdout = std::io::stdout();
        let mut stderr = std::io::stderr();
        let mut detached = false;
        let mut terminal_exit_code: Option<i32> = None;
        let mut pending = Vec::<u8>::new();
        let mut event_data = String::new();

        loop {
            tokio::select! {
                maybe_input = input_rx.recv() => {
                    let Some(input) = maybe_input else {
                        continue;
                    };
                    match input {
                        AttachInputEvent::Bytes(bytes) => {
                            let write_result = api_write_exec_stdin(&execution_id, bytes).await;
                            if let Err(error) = write_result {
                                if let Ok(Some(execution)) = api_get_execution(&execution_id).await
                                    && execution_state_is_terminal(execution.state.as_str())
                                {
                                    terminal_exit_code = execution.exit_code;
                                    break;
                                }
                                return Err(error).with_context(|| format!("failed to write stdin to `{execution_id}`"));
                            }
                        }
                        AttachInputEvent::Resize { cols, rows } => {
                            let resize_result = api_resize_exec_pty(&execution_id, cols, rows).await;
                            if let Err(error) = resize_result {
                                if let Ok(Some(execution)) = api_get_execution(&execution_id).await
                                    && execution_state_is_terminal(execution.state.as_str())
                                {
                                    terminal_exit_code = execution.exit_code;
                                    break;
                                }
                                return Err(error).with_context(|| format!("failed to resize PTY for `{execution_id}`"));
                            }
                        }
                        AttachInputEvent::Detach => {
                            detached = true;
                            break;
                        }
                    }
                }
                maybe_chunk = stream.chunk() => {
                    let maybe_chunk = maybe_chunk
                        .with_context(|| format!("failed reading stream for `{execution_id}`"))?;
                    let Some(chunk) = maybe_chunk else {
                        break;
                    };

                    pending.extend_from_slice(&chunk);
                    while let Some(line_end) = pending.iter().position(|byte| *byte == b'\n') {
                        let mut line = pending.drain(..=line_end).collect::<Vec<u8>>();
                        if line.last() == Some(&b'\n') {
                            let _ = line.pop();
                        }
                        if line.last() == Some(&b'\r') {
                            let _ = line.pop();
                        }

                        let line = String::from_utf8(line).with_context(|| {
                            format!("received non UTF-8 stream line for `{execution_id}`")
                        })?;

                        if line.is_empty() {
                            if !event_data.is_empty() {
                                let done = handle_api_execution_stream_event(
                                    &execution_id,
                                    &event_data,
                                    &mut stdout,
                                    &mut stderr,
                                    &mut terminal_exit_code,
                                )?;
                                event_data.clear();
                                if done {
                                    break;
                                }
                            }
                            continue;
                        }

                        if line.starts_with(':') {
                            continue;
                        }
                        if let Some(data_line) = line.strip_prefix("data:") {
                            if !event_data.is_empty() {
                                event_data.push('\n');
                            }
                            event_data.push_str(data_line.trim_start());
                        }
                    }

                    if terminal_exit_code.is_some() {
                        break;
                    }
                }
            }
        }

        if terminal_exit_code.is_none() && !detached {
            if !event_data.is_empty() {
                handle_api_execution_stream_event(
                    &execution_id,
                    &event_data,
                    &mut stdout,
                    &mut stderr,
                    &mut terminal_exit_code,
                )?;
            }

            if let Ok(Some(execution)) = api_get_execution(&execution_id).await {
                terminal_exit_code = execution.exit_code;
            }
        }

        Ok::<_, anyhow::Error>((detached, terminal_exit_code))
    }
    .await;

    terminal::disable_raw_mode().ok();
    stop_input.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), input_handle).await;
    let (detached, terminal_exit_code) = interaction_result?;

    if detached {
        eprintln!("\nDetached (Ctrl-P Ctrl-Q). Session remains active.");
        return Ok(SandboxAttachOutcome::Detached);
    }

    if let Some(exit_code) = terminal_exit_code
        && exit_code != 0
    {
        bail!("sandbox shell exited with status {exit_code}");
    }

    println!();
    Ok(SandboxAttachOutcome::ExitedClean)
}

/// Convert a crossterm key event to the byte sequence the terminal expects.
fn key_event_to_bytes(key: &crossterm::event::KeyEvent) -> Vec<u8> {
    use crossterm::event::{KeyCode, KeyModifiers};

    match key.code {
        KeyCode::Char(c) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                // Ctrl+A = 0x01, Ctrl+B = 0x02, etc.
                let ctrl_byte = (c as u8).wrapping_sub(b'a').wrapping_add(1);
                if ctrl_byte <= 26 {
                    return vec![ctrl_byte];
                }
            }
            if key.modifiers.contains(KeyModifiers::ALT) {
                // Alt+key sends ESC prefix followed by the key byte.
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                let mut out = vec![0x1b];
                out.extend_from_slice(s.as_bytes());
                return out;
            }
            let mut buf = [0u8; 4];
            let s = c.encode_utf8(&mut buf);
            s.as_bytes().to_vec()
        }
        KeyCode::Enter => vec![b'\r'],
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Tab => vec![b'\t'],
        KeyCode::Esc => vec![0x1b],
        KeyCode::Up => b"\x1b[A".to_vec(),
        KeyCode::Down => b"\x1b[B".to_vec(),
        KeyCode::Right => b"\x1b[C".to_vec(),
        KeyCode::Left => b"\x1b[D".to_vec(),
        KeyCode::Home => b"\x1b[H".to_vec(),
        KeyCode::End => b"\x1b[F".to_vec(),
        KeyCode::PageUp => b"\x1b[5~".to_vec(),
        KeyCode::PageDown => b"\x1b[6~".to_vec(),
        KeyCode::Delete => b"\x1b[3~".to_vec(),
        KeyCode::Insert => b"\x1b[2~".to_vec(),
        KeyCode::F(n) => match n {
            1 => b"\x1bOP".to_vec(),
            2 => b"\x1bOQ".to_vec(),
            3 => b"\x1bOR".to_vec(),
            4 => b"\x1bOS".to_vec(),
            5 => b"\x1b[15~".to_vec(),
            6 => b"\x1b[17~".to_vec(),
            7 => b"\x1b[18~".to_vec(),
            8 => b"\x1b[19~".to_vec(),
            9 => b"\x1b[20~".to_vec(),
            10 => b"\x1b[21~".to_vec(),
            11 => b"\x1b[23~".to_vec(),
            12 => b"\x1b[24~".to_vec(),
            _ => vec![],
        },
        _ => vec![],
    }
}

fn is_detach_prefix(bytes: &[u8]) -> bool {
    bytes == [0x10]
}

fn is_detach_confirm(bytes: &[u8]) -> bool {
    matches!(bytes, [0x11] | [b'q'] | [b'Q'])
}

// ── Top-level sandbox commands ──────────────────────────────────

/// List all sandboxes (`vz ls`).
pub async fn cmd_list(args: SandboxListArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let sandboxes = daemon_list_sandboxes(&state_db).await?;

    if args.json {
        let json =
            serde_json::to_string_pretty(&sandboxes).context("failed to serialize sandboxes")?;
        println!("{json}");
        return Ok(());
    }

    if sandboxes.is_empty() {
        println!("No sandboxes found.");
        return Ok(());
    }

    println!(
        "{:<16} {:<12} {:<6} {:<10} {:<30} {:<12}",
        "SANDBOX", "STATE", "CPUS", "MEMORY MB", "DIR", "SOURCE"
    );
    for sandbox in &sandboxes {
        let cpus = sandbox
            .spec
            .cpus
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let memory = sandbox
            .spec
            .memory_mb
            .map(|m| m.to_string())
            .unwrap_or_else(|| "-".to_string());
        let state = serde_json::to_string(&sandbox.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let dir = sandbox
            .labels
            .get(SANDBOX_LABEL_PROJECT_DIR)
            .map(|d| {
                // Shorten home dir.
                if let Ok(home) = std::env::var("HOME") {
                    if let Some(rest) = d.strip_prefix(&home) {
                        return format!("~{rest}");
                    }
                }
                d.clone()
            })
            .unwrap_or_else(|| "-".to_string());
        let source = sandbox
            .labels
            .get("source")
            .cloned()
            .unwrap_or_else(|| "-".to_string());

        // Use name label if available, otherwise truncate sandbox_id.
        let display_id = sandbox.labels.get("name").cloned().unwrap_or_else(|| {
            if sandbox.sandbox_id.len() > 14 {
                format!("{}…", &sandbox.sandbox_id[..13])
            } else {
                sandbox.sandbox_id.clone()
            }
        });

        println!(
            "{:<16} {:<12} {:<6} {:<10} {:<30} {:<12}",
            display_id, state, cpus, memory, dir, source
        );
    }

    Ok(())
}

/// Show detailed sandbox information (`vz inspect`).
pub async fn cmd_inspect(args: SandboxInspectArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let sandbox = daemon_get_sandbox(&state_db, &args.sandbox_id).await?;

    match sandbox {
        Some(s) => {
            let json = serde_json::to_string_pretty(&s).context("failed to serialize sandbox")?;
            println!("{json}");
        }
        None => bail!("sandbox {} not found", args.sandbox_id),
    }

    Ok(())
}

/// Terminate (remove) a sandbox (`vz rm`).
pub async fn cmd_terminate(args: SandboxTerminateArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let existing = daemon_get_sandbox(&state_db, &args.sandbox_id)
        .await?
        .ok_or_else(|| anyhow!("sandbox {} not found", args.sandbox_id))?;

    if existing.state.is_terminal() {
        println!("Sandbox {} is already in terminal state.", args.sandbox_id);
        return Ok(());
    }

    let sandbox = daemon_terminate_sandbox(&state_db, &args.sandbox_id)
        .await?
        .ok_or_else(|| anyhow!("sandbox {} not found", args.sandbox_id))?;

    let state = serde_json::to_string(&sandbox.state)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();

    println!("Sandbox {} terminated (state: {state}).", args.sandbox_id);

    Ok(())
}

/// Attach to an existing sandbox (`vz attach`).
pub async fn cmd_attach(args: SandboxAttachArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    attach_to_sandbox_by_id(&state_db, &args.sandbox_id)
        .await
        .map(|_| ())
}

/// Close an active sandbox shell session (`vz close-shell`).
pub async fn cmd_close_shell(args: SandboxCloseShellArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let response =
        daemon_close_sandbox_shell(&state_db, &args.sandbox_id, args.execution_id.as_deref())
            .await?;
    println!(
        "Closed sandbox shell session {} for sandbox {}.",
        response.execution_id, response.sandbox_id
    );
    Ok(())
}

/// Attach to a sandbox by its ID (shared helper).
async fn attach_to_sandbox_by_id(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<SandboxAttachOutcome> {
    let opened = daemon_open_sandbox_shell(state_db, sandbox_id).await?;
    let execution_id = opened.execution_id.trim();
    if execution_id.is_empty() {
        bail!("daemon open_sandbox_shell returned empty execution_id");
    }
    attach_to_execution_interactive(state_db, execution_id).await
}

/// Generate a short sandbox ID.
fn generate_sandbox_id() -> String {
    let id = uuid::Uuid::new_v4();
    let hex = id.as_simple().to_string();
    format!("vz-{}", &hex[..4])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn sandbox_with_labels(
        sandbox_id: &str,
        state: SandboxState,
        labels: BTreeMap<String, String>,
    ) -> Sandbox {
        Sandbox {
            sandbox_id: sandbox_id.to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state,
            created_at: 1,
            updated_at: 1,
            labels,
        }
    }

    #[test]
    fn detach_prefix_matches_ctrl_p_byte() {
        assert!(is_detach_prefix(&[0x10]));
        assert!(!is_detach_prefix(&[0x11]));
        assert!(!is_detach_prefix(b"p"));
    }

    #[test]
    fn detach_confirm_accepts_ctrl_q_and_q_fallback() {
        assert!(is_detach_confirm(&[0x11]));
        assert!(is_detach_confirm(b"q"));
        assert!(is_detach_confirm(b"Q"));
        assert!(!is_detach_confirm(&[0x10]));
        assert!(!is_detach_confirm(b"x"));
    }

    #[test]
    fn space_lifecycle_mode_defaults_to_persistent() {
        let lifecycle = SpaceLifecycleMode::from_ephemeral_flag(false);
        assert_eq!(lifecycle, SpaceLifecycleMode::Persistent);
        assert_eq!(
            lifecycle.as_label_value(),
            SANDBOX_SPACE_LIFECYCLE_PERSISTENT
        );
    }

    #[test]
    fn space_lifecycle_mode_maps_ephemeral_flag() {
        let lifecycle = SpaceLifecycleMode::from_ephemeral_flag(true);
        assert_eq!(lifecycle, SpaceLifecycleMode::Ephemeral);
        assert_eq!(
            lifecycle.as_label_value(),
            SANDBOX_SPACE_LIFECYCLE_EPHEMERAL
        );
    }

    #[test]
    fn sanitize_namespace_segment_normalizes_to_safe_ascii() {
        assert_eq!(sanitize_namespace_segment("Feature/Auth"), "feature_auth");
        assert_eq!(sanitize_namespace_segment("___"), "space");
        assert_eq!(sanitize_namespace_segment("Main_Worktree"), "main_worktree");
    }

    #[test]
    fn derive_space_worktree_identity_is_stable_for_same_path() {
        let dir = tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace dir");

        let first = derive_space_worktree_identity(&workspace).expect("first identity");
        let second = derive_space_worktree_identity(&workspace).expect("second identity");
        assert_eq!(first.worktree_id, second.worktree_id);
        assert_eq!(first.service_namespace, second.service_namespace);
        assert_eq!(first.root_path, second.root_path);
    }

    #[test]
    fn default_worktree_service_state_defaults_cover_common_services() {
        let defaults = default_worktree_service_state_defaults("wt_abc123");
        assert_eq!(
            defaults.get("postgres.schema").map(String::as_str),
            Some("wt_abc123")
        );
        assert_eq!(
            defaults.get("mysql.database").map(String::as_str),
            Some("wt_abc123")
        );
        assert_eq!(
            defaults.get("redis.key_prefix").map(String::as_str),
            Some("wt_abc123:")
        );
    }

    #[test]
    fn apply_worktree_service_state_labels_projects_defaults_to_labels() {
        let defaults = default_worktree_service_state_defaults("wt_1a2b3c");
        let mut labels = BTreeMap::new();
        apply_worktree_service_state_labels(&mut labels, &defaults);

        assert_eq!(
            labels
                .get("vz.space.service_state.postgres.schema")
                .map(String::as_str),
            Some("wt_1a2b3c")
        );
        assert_eq!(
            labels
                .get("vz.space.service_state.mysql.database")
                .map(String::as_str),
            Some("wt_1a2b3c")
        );
        assert_eq!(
            labels
                .get("vz.space.service_state.redis.key_prefix")
                .map(String::as_str),
            Some("wt_1a2b3c:")
        );
    }

    #[test]
    fn find_worktree_namespace_collision_detects_active_conflict() {
        let mut labels = BTreeMap::new();
        labels.insert(
            SANDBOX_LABEL_SPACE_WORKTREE_NAMESPACE.to_string(),
            "wt_deadbeef0001".to_string(),
        );
        labels.insert(
            SANDBOX_LABEL_SPACE_WORKTREE_ID.to_string(),
            "main-deadbeef0001".to_string(),
        );
        labels.insert(
            SANDBOX_LABEL_PROJECT_DIR.to_string(),
            "/workspace/project-a".to_string(),
        );
        let sandboxes = vec![sandbox_with_labels("vz-a1", SandboxState::Ready, labels)];

        let collision =
            find_worktree_namespace_collision(&sandboxes, "wt_deadbeef0001", "main-bbbbbbbbbbbb")
                .expect("collision should be detected");
        assert_eq!(collision.sandbox_id, "vz-a1");
        assert_eq!(collision.namespace, "wt_deadbeef0001");
        assert_eq!(collision.existing_worktree_id, "main-deadbeef0001");
        assert_eq!(
            collision.existing_project_dir.as_deref(),
            Some("/workspace/project-a")
        );
    }

    #[test]
    fn find_worktree_namespace_collision_allows_same_worktree_identity() {
        let mut labels = BTreeMap::new();
        labels.insert(
            SANDBOX_LABEL_SPACE_WORKTREE_NAMESPACE.to_string(),
            "wt_deadbeef0002".to_string(),
        );
        labels.insert(
            SANDBOX_LABEL_SPACE_WORKTREE_ID.to_string(),
            "main-deadbeef0002".to_string(),
        );
        let sandboxes = vec![sandbox_with_labels("vz-a2", SandboxState::Ready, labels)];

        let collision =
            find_worktree_namespace_collision(&sandboxes, "wt_deadbeef0002", "main-deadbeef0002");
        assert!(collision.is_none());
    }

    #[test]
    fn find_worktree_namespace_collision_ignores_terminal_sandbox() {
        let mut labels = BTreeMap::new();
        labels.insert(
            SANDBOX_LABEL_SPACE_WORKTREE_NAMESPACE.to_string(),
            "wt_deadbeef0003".to_string(),
        );
        labels.insert(
            SANDBOX_LABEL_SPACE_WORKTREE_ID.to_string(),
            "main-deadbeef0003".to_string(),
        );
        let sandboxes = vec![sandbox_with_labels(
            "vz-a3",
            SandboxState::Terminated,
            labels,
        )];

        let collision =
            find_worktree_namespace_collision(&sandboxes, "wt_deadbeef0003", "main-ffffffffffff");
        assert!(collision.is_none());
    }

    #[test]
    fn worktree_service_defaults_do_not_bleed_between_worktrees() {
        let first = default_worktree_service_state_defaults("wt_aaaa1111bbbb");
        let second = default_worktree_service_state_defaults("wt_cccc2222dddd");
        assert_ne!(
            first.get("postgres.schema").map(String::as_str),
            second.get("postgres.schema").map(String::as_str)
        );
        assert_ne!(
            first.get("mysql.database").map(String::as_str),
            second.get("mysql.database").map(String::as_str)
        );
        assert_ne!(
            first.get("redis.key_prefix").map(String::as_str),
            second.get("redis.key_prefix").map(String::as_str)
        );
    }

    #[test]
    fn ephemeral_cleanup_decision_allows_clean_exit_for_non_terminal_space() {
        let sandbox = Sandbox {
            sandbox_id: "sandbox-ephemeral-clean".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state: SandboxState::Ready,
            created_at: 1,
            updated_at: 1,
            labels: BTreeMap::new(),
        };
        let decision = evaluate_ephemeral_cleanup_decision(
            EphemeralSessionCompletion::CleanExit,
            Some(&sandbox),
        );
        assert_eq!(decision, EphemeralCleanupDecision::AutoCleanup);
    }

    #[test]
    fn ephemeral_cleanup_decision_preserves_dirty_paths() {
        let detached = evaluate_ephemeral_cleanup_decision(
            EphemeralSessionCompletion::Detached,
            Some(&Sandbox {
                sandbox_id: "sandbox-ephemeral-detached".to_string(),
                backend: SandboxBackend::MacosVz,
                spec: SandboxSpec::default(),
                state: SandboxState::Ready,
                created_at: 1,
                updated_at: 1,
                labels: BTreeMap::new(),
            }),
        );
        assert!(matches!(
            detached,
            EphemeralCleanupDecision::Preserve { reason } if reason.contains("detached")
        ));

        let failed = evaluate_ephemeral_cleanup_decision(EphemeralSessionCompletion::Failed, None);
        assert!(matches!(
            failed,
            EphemeralCleanupDecision::Preserve { reason } if reason.contains("error")
        ));
    }

    #[test]
    fn sandbox_recovery_commands_are_deterministic() {
        let commands = sandbox_recovery_commands("space-123");
        assert_eq!(
            commands,
            [
                "vz attach space-123".to_string(),
                "vz inspect space-123".to_string(),
                "vz rm space-123".to_string(),
            ]
        );
    }

    #[test]
    fn startup_selection_labels_do_not_inject_base_image_when_unset() {
        let mut labels = BTreeMap::new();
        apply_startup_selection_labels(&mut labels, None, None);
        assert!(!labels.contains_key(SANDBOX_LABEL_BASE_IMAGE_REF));
        assert!(!labels.contains_key(SANDBOX_LABEL_MAIN_CONTAINER));
    }

    #[test]
    fn startup_selection_labels_include_explicit_base_image_and_main_container() {
        let mut labels = BTreeMap::new();
        apply_startup_selection_labels(
            &mut labels,
            Some("debian:bookworm".to_string()),
            Some("workspace-main".to_string()),
        );
        assert_eq!(
            labels.get(SANDBOX_LABEL_BASE_IMAGE_REF).map(String::as_str),
            Some("debian:bookworm")
        );
        assert_eq!(
            labels.get(SANDBOX_LABEL_MAIN_CONTAINER).map(String::as_str),
            Some("workspace-main")
        );
    }

    #[test]
    fn load_space_config_rejects_missing_file() {
        let dir = tempdir().expect("tempdir");
        let error = load_space_config(dir.path()).expect_err("missing vz.json should fail");
        assert!(
            error.to_string().contains("requires `vz.json`"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_space_config_rejects_invalid_json() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(&path, "{ invalid json").expect("write invalid config");
        let error = load_space_config(dir.path()).expect_err("invalid JSON should fail");
        assert!(
            error.to_string().contains("invalid `vz.json`"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_space_config_accepts_valid_json() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(&path, r#"{"image":"ubuntu:24.04"}"#).expect("write config");
        let resolved = load_space_config(dir.path()).expect("valid config should pass");
        assert_eq!(resolved.config_path, path);
        assert!(resolved.external_secret_env.is_empty());
        assert!(resolved.cache_definitions.is_empty());
    }

    #[test]
    fn load_space_config_rejects_inline_secret_field() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(
            &path,
            r#"{
                "env": {
                    "DB_PASSWORD": "super-secret"
                }
            }"#,
        )
        .expect("write config");
        let error = load_space_config(dir.path()).expect_err("inline secret field should fail");
        assert!(
            error
                .to_string()
                .contains("must not include inline secrets"),
            "unexpected error: {error:#}"
        );
        assert!(
            !error.to_string().contains("super-secret"),
            "error should not leak raw secret values: {error:#}"
        );
    }

    #[test]
    fn load_space_config_rejects_inline_secret_definition_values() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(
            &path,
            r#"{
                "secrets": {
                    "db_password": {
                        "value": "super-secret"
                    }
                }
            }"#,
        )
        .expect("write config");
        let error = load_space_config(dir.path()).expect_err("inline secret value should fail");
        assert!(
            error.to_string().contains("cannot embed secret material"),
            "unexpected error: {error:#}"
        );
        assert!(
            !error.to_string().contains("super-secret"),
            "error should not leak raw secret values: {error:#}"
        );
    }

    #[test]
    fn load_space_config_accepts_external_secret_env_references() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(
            &path,
            r#"{
                "secrets": {
                    "db_password": {
                        "env": "DB_PASSWORD"
                    },
                    "api_token": {
                        "environment": "API_TOKEN"
                    }
                }
            }"#,
        )
        .expect("write config");
        let loaded = load_space_config(dir.path()).expect("external refs should be accepted");
        assert_eq!(loaded.config_path, path);
        assert_eq!(
            loaded
                .external_secret_env
                .get("db_password")
                .map(String::as_str),
            Some("DB_PASSWORD")
        );
        assert_eq!(
            loaded
                .external_secret_env
                .get("api_token")
                .map(String::as_str),
            Some("API_TOKEN")
        );
        assert!(loaded.cache_definitions.is_empty());
    }

    #[test]
    fn load_space_config_parses_cache_definitions() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(
            &path,
            r#"{
                "caches": [
                    {
                        "name": "deps",
                        "key": "package-lock.json"
                    },
                    {
                        "name": "build",
                        "key": ["Cargo.lock", "rust-toolchain.toml", "Cargo.lock"]
                    }
                ]
            }"#,
        )
        .expect("write config");

        let loaded = load_space_config(dir.path()).expect("cache definitions should parse");
        assert_eq!(loaded.cache_definitions.len(), 2);
        assert_eq!(loaded.cache_definitions[0].name, "deps");
        assert_eq!(
            loaded.cache_definitions[0].key_inputs,
            vec!["package-lock.json".to_string()]
        );
        assert_eq!(loaded.cache_definitions[1].name, "build");
        assert_eq!(
            loaded.cache_definitions[1].key_inputs,
            vec!["Cargo.lock".to_string(), "rust-toolchain.toml".to_string(),]
        );
    }

    #[test]
    fn load_space_config_rejects_duplicate_cache_names() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vz.json");
        fs::write(
            &path,
            r#"{
                "caches": [
                    {"name": "deps", "key": "package-lock.json"},
                    {"name": "deps", "key": "Cargo.lock"}
                ]
            }"#,
        )
        .expect("write config");
        let error = load_space_config(dir.path()).expect_err("duplicate cache names should fail");
        assert!(
            error.to_string().contains("duplicate cache name"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn apply_space_external_secret_labels_projects_only_env_refs() {
        let mut labels = BTreeMap::new();
        let refs = BTreeMap::from([
            ("db_password".to_string(), "DB_PASSWORD".to_string()),
            ("api_token".to_string(), "API_TOKEN".to_string()),
        ]);
        apply_space_external_secret_labels(&mut labels, &refs);
        assert_eq!(
            labels
                .get("vz.space.secret.env.db_password")
                .map(String::as_str),
            Some("DB_PASSWORD")
        );
        assert_eq!(
            labels
                .get("vz.space.secret.env.api_token")
                .map(String::as_str),
            Some("API_TOKEN")
        );
    }

    #[test]
    fn btrfs_preflight_rejects_non_directory_paths() {
        let error = enforce_btrfs_workspace_preflight(Path::new("/definitely/not/a/real/path"))
            .expect_err("non-directory path should fail");
        assert!(
            error
                .to_string()
                .contains("workspace path is not a directory"),
            "unexpected error: {error:#}"
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn btrfs_preflight_rejects_non_linux_platforms() {
        let dir = tempdir().expect("tempdir");
        let error =
            enforce_btrfs_workspace_preflight(dir.path()).expect_err("non-linux should fail");
        assert!(
            error
                .to_string()
                .contains("requires Linux btrfs workspace storage"),
            "unexpected error: {error:#}"
        );
    }
}
