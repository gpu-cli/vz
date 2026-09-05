//! Process and Git-worktree inputs for Developer Environment selection.
//!
//! This module deliberately does not implement a CLI command. It discovers the
//! stable inputs that the five-verb Developer Environment CLI will pass to the
//! topology resolver.

use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use tempfile::NamedTempFile;
use vz_runtime_contract::{
    EnvironmentId, EnvironmentSelectionContext, EnvironmentSelector, MachineId, WorkspaceBindingId,
};

pub const VZ_ENVIRONMENT_ID: &str = "VZ_ENVIRONMENT_ID";
pub const VZ_MACHINE_ID: &str = "VZ_MACHINE_ID";

const WORKSPACE_METADATA_DIRECTORY: &str = "vz";
const WORKSPACE_ID_FILE: &str = "workspace-id";

/// Stable worktree identity plus non-authorizing diagnostic paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitWorkspace {
    /// Random opaque token persisted in this worktree's private Git metadata.
    pub workspace_key: String,
    /// Absolute per-worktree Git directory used to store the token.
    pub git_dir: PathBuf,
    /// Absolute checkout root for diagnostics only; never an identity input.
    pub path_hint: PathBuf,
}

/// Strictly typed process-scoped topology selectors.
///
/// Reading these values does not perform Environment or Machine selection. In
/// particular, Machine ownership and default-Machine behavior remain the
/// responsibility of the topology resolver.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessTopologySelectors {
    pub environment_id: Option<EnvironmentId>,
    pub machine_id: Option<MachineId>,
}

impl ProcessTopologySelectors {
    /// Read process-scoped selectors. A present invalid value is an error and is
    /// never treated as though the selector were absent.
    pub fn from_current_process() -> Result<Self> {
        Self::from_lookup(|variable| std::env::var_os(variable))
    }

    /// Compose the Environment resolver inputs without interpreting the
    /// Machine selector.
    ///
    /// Keeping the process ID and workspace key in their distinct fields lets
    /// the canonical resolver enforce explicit, then process, then workspace
    /// precedence without this adapter duplicating selection logic.
    pub fn environment_selection_context(
        &self,
        explicit: Option<EnvironmentSelector>,
        workspace: &GitWorkspace,
    ) -> EnvironmentSelectionContext {
        EnvironmentSelectionContext {
            explicit,
            process_environment_id: self.environment_id.clone(),
            workspace_key: Some(workspace.workspace_key.clone()),
        }
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<OsString>) -> Result<Self> {
        let environment_id = read_process_id(VZ_ENVIRONMENT_ID, &mut lookup, EnvironmentId::new)?;
        let machine_id = read_process_id(VZ_MACHINE_ID, &mut lookup, MachineId::new)?;
        Ok(Self {
            environment_id,
            machine_id,
        })
    }
}

/// Discover or create the opaque identity for the Git worktree containing
/// `cwd`.
///
/// Git itself resolves both the checkout root and its private per-worktree Git
/// directory. There is intentionally no path-derived or non-Git fallback.
pub fn discover_git_workspace(cwd: &Path) -> Result<GitWorkspace> {
    let git_dir = git_path(cwd, "--git-dir")?;
    let path_hint = git_path(cwd, "--show-toplevel")?;
    let workspace_key = load_or_create_workspace_key(&git_dir)?;

    Ok(GitWorkspace {
        workspace_key,
        git_dir,
        path_hint,
    })
}

/// Read an existing worktree binding token without creating or syncing files.
///
/// Read-only commands must not call `discover_git_workspace`: an unbound
/// checkout is a selection input, not permission to create workspace metadata.
/// A corrupt/unreadable token fails closed, rather than looking unbound.
pub fn discover_existing_git_workspace(cwd: &Path) -> Result<Option<GitWorkspace>> {
    let git_dir = git_path(cwd, "--git-dir")?;
    let path_hint = git_path(cwd, "--show-toplevel")?;
    let token_path = git_dir
        .join(WORKSPACE_METADATA_DIRECTORY)
        .join(WORKSPACE_ID_FILE);
    let workspace_key = match read_workspace_key(&token_path) {
        Ok(token) => token,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::symlink_metadata(&token_path) {
                Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                _ => {
                    return Err(error).with_context(|| {
                        format!("failed to read workspace token {}", token_path.display())
                    });
                }
            }
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read workspace token {}", token_path.display())
            });
        }
    };
    Ok(Some(GitWorkspace {
        workspace_key,
        git_dir,
        path_hint,
    }))
}

fn git_path(cwd: &Path, selector: &str) -> Result<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(["rev-parse", "--path-format=absolute", selector])
        .output()
        .with_context(|| format!("failed to run git while inspecting {}", cwd.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "git could not resolve {selector} from {}: {}",
            cwd.display(),
            stderr.trim()
        );
    }

    let raw = String::from_utf8(output.stdout)
        .context("git returned a non-UTF-8 path while resolving worktree metadata")?;
    let value = raw.trim_end_matches(['\r', '\n']);
    if value.is_empty() || value.contains(['\r', '\n']) {
        bail!("git returned an invalid {selector} value");
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("git returned a non-absolute {selector} path: {value}");
    }
    Ok(path)
}

fn load_or_create_workspace_key(git_dir: &Path) -> Result<String> {
    load_or_create_workspace_key_with_hook(git_dir, || {})
}

fn load_or_create_workspace_key_with_hook(
    git_dir: &Path,
    before_publish: impl FnOnce(),
) -> Result<String> {
    let metadata_dir = git_dir.join(WORKSPACE_METADATA_DIRECTORY);
    let token_path = metadata_dir.join(WORKSPACE_ID_FILE);
    let token = match read_workspace_key(&token_path) {
        Ok(token) => token,
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(error).with_context(|| {
                format!("failed to read workspace token {}", token_path.display())
            });
        }
        Err(_) => {
            fs::create_dir_all(&metadata_dir).with_context(|| {
                format!(
                    "failed to create workspace metadata directory {}",
                    metadata_dir.display()
                )
            })?;

            // Publish only a fully-written token. `persist_noclobber` is not
            // universally atomic, but tempfile uses an atomic no-replace operation
            // on the supported macOS/Linux paths. Its no-clobber contract also
            // ensures a contender never overwrites the token chosen by the winner.
            let generated = WorkspaceBindingId::generate().to_string();
            let mut temporary = NamedTempFile::new_in(&metadata_dir).with_context(|| {
                format!(
                    "failed to create temporary workspace token in {}",
                    metadata_dir.display()
                )
            })?;
            temporary
                .write_all(generated.as_bytes())
                .and_then(|()| temporary.as_file().sync_all())
                .with_context(|| {
                    format!("failed to write workspace token {}", token_path.display())
                })?;

            before_publish();
            match temporary.persist_noclobber(&token_path) {
                Ok(_) => generated,
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    read_workspace_key(&token_path).with_context(|| {
                        format!(
                            "failed to read concurrently-created workspace token {}",
                            token_path.display()
                        )
                    })?
                }
                Err(error) => {
                    return Err(error.error).with_context(|| {
                        format!("failed to publish workspace token {}", token_path.display())
                    });
                }
            }
        }
    };

    // Sync both entries before any successful discovery returns. This also
    // covers a contender that observes the token after its publisher wins but
    // before that publisher reaches its own directory sync.
    sync_workspace_metadata(git_dir, &metadata_dir)?;
    Ok(token)
}

#[cfg(unix)]
fn sync_workspace_metadata(git_dir: &Path, metadata_dir: &Path) -> Result<()> {
    sync_directory(metadata_dir, "workspace token")?;
    sync_directory(git_dir, "workspace metadata directory")
}

#[cfg(not(unix))]
fn sync_workspace_metadata(_git_dir: &Path, _metadata_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path, entry: &str) -> Result<()> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to make {entry} entry durable in {}", path.display()))
}

fn read_workspace_key(path: &Path) -> std::io::Result<String> {
    let token = fs::read_to_string(path)?;
    WorkspaceBindingId::new(token.clone())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    let opaque_suffix = token.strip_prefix("wsp_").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "workspace token must use the generated `wsp_` form",
        )
    })?;
    if opaque_suffix.len() != 32
        || !opaque_suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "workspace token must contain exactly 32 lowercase hexadecimal characters",
        ));
    }
    Ok(token)
}

fn read_process_id<T, E>(
    variable: &'static str,
    lookup: &mut impl FnMut(&str) -> Option<OsString>,
    parse: impl FnOnce(String) -> Result<T, E>,
) -> Result<Option<T>>
where
    E: std::fmt::Display,
{
    let Some(raw) = lookup(variable) else {
        return Ok(None);
    };
    let value = raw
        .into_string()
        .map_err(|_| anyhow!("{variable} is present but is not valid UTF-8"))?;
    let parsed = parse(value.clone())
        .map_err(|error| anyhow!("invalid present {variable} value `{value}`: {error}"))?;
    Ok(Some(parsed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier};

    #[test]
    fn lookup_rejects_present_invalid_values_without_treating_them_as_absent() {
        let result = ProcessTopologySelectors::from_lookup(|variable| match variable {
            VZ_ENVIRONMENT_ID => Some(OsString::from("not valid")),
            _ => None,
        });
        assert!(result.is_err());
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn forced_no_clobber_contenders_converge_through_the_sync_path() {
        let temporary = tempfile::tempdir().unwrap();
        let git_dir = temporary.path().join("git-dir");
        fs::create_dir(&git_dir).unwrap();
        let barrier = Arc::new(Barrier::new(9));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let git_dir = git_dir.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    load_or_create_workspace_key_with_hook(&git_dir, || {
                        barrier.wait();
                    })
                    .unwrap()
                })
            })
            .collect();
        barrier.wait();

        let tokens: BTreeSet<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(tokens.len(), 1);
        assert_eq!(
            fs::read_to_string(git_dir.join("vz/workspace-id")).unwrap(),
            tokens.into_iter().next().unwrap()
        );
    }
}
