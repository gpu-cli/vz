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

/// A worktree identity chosen without writing anything into the worktree.
///
/// Minting the token and persisting it are deliberately two steps. A refused
/// `vz up` must leave the worktree byte-identical — the workspace key is
/// persistent identity, so a token published behind a refusal is a binding
/// artifact a later Up would find and adopt — and the runtime needs the key in
/// the request that it is about to refuse. So the key is chosen in memory here,
/// sent, and only published by [`PendingGitWorkspace::commit`] once the
/// operation has been admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGitWorkspace {
    workspace: GitWorkspace,
    bound: bool,
}

impl PendingGitWorkspace {
    /// The identity to send, whether it is already on disk or newly minted.
    pub fn workspace(&self) -> &GitWorkspace {
        &self.workspace
    }

    /// Whether the worktree already carries this token, so `commit` writes nothing.
    pub fn is_bound(&self) -> bool {
        self.bound
    }

    /// Publish a newly minted token into the worktree's private Git metadata.
    ///
    /// This is the first and only write this module makes to the worktree, and
    /// callers must not reach it until the operation carrying the key has been
    /// admitted. It is a no-op for a worktree that was already bound.
    pub fn commit(&self) -> Result<()> {
        if self.bound {
            return Ok(());
        }
        publish_workspace_key(
            &self.workspace.git_dir,
            &self.workspace.workspace_key,
            || {},
        )
    }
}

/// Resolve the opaque identity for the Git worktree containing `cwd`, minting
/// one in memory when the worktree carries none.
///
/// Git itself resolves both the checkout root and its private per-worktree Git
/// directory. There is intentionally no path-derived or non-Git fallback. A
/// corrupt or unreadable token fails closed rather than looking unbound, so a
/// damaged worktree is never silently rebound to a fresh identity.
pub fn resolve_git_workspace(cwd: &Path) -> Result<PendingGitWorkspace> {
    let git_dir = git_path(cwd, "--git-dir")?;
    let path_hint = git_path(cwd, "--show-toplevel")?;
    let existing = read_persisted_workspace_key(&git_dir)?;
    let bound = existing.is_some();
    let workspace_key = existing.unwrap_or_else(|| WorkspaceBindingId::generate().to_string());

    Ok(PendingGitWorkspace {
        workspace: GitWorkspace {
            workspace_key,
            git_dir,
            path_hint,
        },
        bound,
    })
}

/// Read an existing worktree binding token without creating or syncing files.
///
/// Read-only commands must not call [`resolve_git_workspace`]: an unbound
/// checkout is a selection input, and only Up may ever publish a token.
/// A corrupt/unreadable token fails closed, rather than looking unbound.
pub fn discover_existing_git_workspace(cwd: &Path) -> Result<Option<GitWorkspace>> {
    let git_dir = git_path(cwd, "--git-dir")?;
    let path_hint = git_path(cwd, "--show-toplevel")?;
    let Some(workspace_key) = read_persisted_workspace_key(&git_dir)? else {
        return Ok(None);
    };
    Ok(Some(GitWorkspace {
        workspace_key,
        git_dir,
        path_hint,
    }))
}

/// The checked-out branch of the worktree containing `cwd`, if it has one.
///
/// This is where a fork's default label comes from. A detached HEAD legitimately
/// has no branch, and that is `Ok(None)` rather than an error: the caller then
/// requires an explicit `--as`, which is the honest outcome, because there is no
/// name for an agent to have predicted.
pub fn discover_checked_out_branch(cwd: &Path) -> Result<Option<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .output()
        .with_context(|| format!("failed to run git while inspecting {}", cwd.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let branch = String::from_utf8(output.stdout)
        .context("git reported a non-UTF-8 branch name")?
        .trim()
        .to_string();
    Ok((!branch.is_empty()).then_some(branch))
}

/// Read the persisted token, distinguishing "no token" from "unreadable token".
///
/// `Ok(None)` requires the token path to be genuinely absent. A dangling
/// symlink, a corrupt value or any other read failure is an error, because a
/// damaged binding that looked unbound would be rebound to a fresh identity.
fn read_persisted_workspace_key(git_dir: &Path) -> Result<Option<String>> {
    let token_path = git_dir
        .join(WORKSPACE_METADATA_DIRECTORY)
        .join(WORKSPACE_ID_FILE);
    match read_workspace_key(&token_path) {
        Ok(token) => Ok(Some(token)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::symlink_metadata(&token_path) {
                Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => Ok(None),
                _ => Err(error).with_context(|| {
                    format!("failed to read workspace token {}", token_path.display())
                }),
            }
        }
        Err(error) => Err(error)
            .with_context(|| format!("failed to read workspace token {}", token_path.display())),
    }
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

/// Publish an already-minted token into this worktree's private Git metadata.
///
/// The publish never clobbers. If a contender bound this worktree first, its
/// token stands and this call fails: the caller's operation was admitted under
/// a key the worktree does not carry, and silently adopting the winner's token
/// would leave that operation's Environment unreachable from here without
/// saying so.
fn publish_workspace_key(git_dir: &Path, token: &str, before_publish: impl FnOnce()) -> Result<()> {
    let metadata_dir = git_dir.join(WORKSPACE_METADATA_DIRECTORY);
    let token_path = metadata_dir.join(WORKSPACE_ID_FILE);
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
    let mut temporary = NamedTempFile::new_in(&metadata_dir).with_context(|| {
        format!(
            "failed to create temporary workspace token in {}",
            metadata_dir.display()
        )
    })?;
    temporary
        .write_all(token.as_bytes())
        .and_then(|()| temporary.as_file().sync_all())
        .with_context(|| format!("failed to write workspace token {}", token_path.display()))?;

    before_publish();
    match temporary.persist_noclobber(&token_path) {
        Ok(_) => {}
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let winner = read_workspace_key(&token_path).with_context(|| {
                format!(
                    "failed to read concurrently-created workspace token {}",
                    token_path.display()
                )
            })?;
            if winner != token {
                bail!(
                    "another process bound {} to workspace identity {winner} while this operation was running; this operation used {token} and its Environment is not reachable from this worktree",
                    git_dir.display()
                );
            }
        }
        Err(error) => {
            return Err(error.error).with_context(|| {
                format!("failed to publish workspace token {}", token_path.display())
            });
        }
    }

    // Sync both entries before the publish is reported durable. This also
    // covers a contender that observes the token after its publisher wins but
    // before that publisher reaches its own directory sync.
    sync_workspace_metadata(git_dir, &metadata_dir)
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

    /// Exactly one contender may bind a worktree, and the losers say so.
    ///
    /// Each caller now mints its own key before the runtime is asked to admit
    /// anything, so contenders no longer converge on one token by construction:
    /// the no-clobber publish picks a winner and every other caller learns that
    /// its own operation is not the one this worktree names. Nothing clobbers
    /// the winner's token, which is the property the sync path must preserve.
    #[test]
    #[allow(clippy::unwrap_used)]
    fn forced_no_clobber_contenders_leave_one_winner_and_refuse_the_rest() {
        let temporary = tempfile::tempdir().unwrap();
        let git_dir = temporary.path().join("git-dir");
        fs::create_dir(&git_dir).unwrap();
        let barrier = Arc::new(Barrier::new(9));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let git_dir = git_dir.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let token = WorkspaceBindingId::generate().to_string();
                    let published = publish_workspace_key(&git_dir, &token, || {
                        barrier.wait();
                    })
                    .is_ok();
                    (token, published)
                })
            })
            .collect();
        barrier.wait();

        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let winners: BTreeSet<_> = outcomes
            .iter()
            .filter(|(_, published)| *published)
            .map(|(token, _)| token.clone())
            .collect();
        assert_eq!(winners.len(), 1);
        assert_eq!(
            fs::read_to_string(git_dir.join("vz/workspace-id")).unwrap(),
            winners.into_iter().next().unwrap()
        );
    }

    /// A worktree that already carries a token is never written to again.
    #[test]
    #[allow(clippy::unwrap_used)]
    fn committing_an_already_bound_worktree_writes_nothing() {
        let temporary = tempfile::tempdir().unwrap();
        let git_dir = temporary.path().join("git-dir");
        fs::create_dir(&git_dir).unwrap();
        let token = WorkspaceBindingId::generate().to_string();
        publish_workspace_key(&git_dir, &token, || {}).unwrap();
        let token_path = git_dir.join("vz/workspace-id");
        let before = fs::metadata(&token_path).unwrap().modified().unwrap();

        let pending = PendingGitWorkspace {
            workspace: GitWorkspace {
                workspace_key: token.clone(),
                git_dir: git_dir.clone(),
                path_hint: temporary.path().to_path_buf(),
            },
            bound: true,
        };
        pending.commit().unwrap();

        assert_eq!(fs::read_to_string(&token_path).unwrap(), token);
        assert_eq!(
            fs::metadata(&token_path).unwrap().modified().unwrap(),
            before
        );
    }
}
