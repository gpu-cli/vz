//! Read-only discovery of the portable Developer Environment definition.

use std::fmt;
use std::path::{Path, PathBuf};

use vz_runtime_contract::ProjectDefinition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredProjectDefinition {
    pub path: PathBuf,
    pub definition: ProjectDefinition,
}

#[derive(Debug)]
pub enum DefinitionDiscoveryError {
    DefinitionNotFound {
        directory: PathBuf,
    },
    InvalidDefinition {
        path: PathBuf,
        reason: String,
    },
    ReadFailed {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl DefinitionDiscoveryError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::DefinitionNotFound { .. } => "definition_not_found",
            Self::InvalidDefinition { .. } => "invalid_definition",
            Self::ReadFailed { .. } => "definition_read_failed",
        }
    }
}

impl fmt::Display for DefinitionDiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefinitionNotFound { directory } => write!(
                formatter,
                "no vz.json project definition found at or above {}",
                directory.display()
            ),
            Self::InvalidDefinition { path, reason } => write!(
                formatter,
                "invalid project definition {}: {reason}",
                path.display()
            ),
            Self::ReadFailed { path, source } => write!(
                formatter,
                "cannot read project definition {}: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for DefinitionDiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ReadFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Find the nearest `vz.json`, decode and validate its versioned topology.
///
/// An invalid nearest definition, unreadable file, directory, or dangling
/// symlink is an error. None permits fallback to an ancestor definition.
/// This function never creates a definition, workspace token, or runtime state.
pub fn discover_project_definition(
    cwd: &Path,
) -> Result<DiscoveredProjectDefinition, DefinitionDiscoveryError> {
    let start = cwd
        .canonicalize()
        .map_err(|source| DefinitionDiscoveryError::ReadFailed {
            path: cwd.to_path_buf(),
            source,
        })?;
    for directory in start.ancestors() {
        let path = directory.join("vz.json");
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(DefinitionDiscoveryError::ReadFailed { path, source }),
        }
        let metadata =
            std::fs::metadata(&path).map_err(|source| DefinitionDiscoveryError::ReadFailed {
                path: path.clone(),
                source,
            })?;
        if !metadata.is_file() {
            return Err(DefinitionDiscoveryError::InvalidDefinition {
                path,
                reason: "vz.json must be a regular file".to_string(),
            });
        }
        let bytes =
            std::fs::read(&path).map_err(|source| DefinitionDiscoveryError::ReadFailed {
                path: path.clone(),
                source,
            })?;
        let definition: ProjectDefinition = serde_json::from_slice(&bytes).map_err(|error| {
            DefinitionDiscoveryError::InvalidDefinition {
                path: path.clone(),
                reason: error.to_string(),
            }
        })?;
        definition
            .validate()
            .map_err(|error| DefinitionDiscoveryError::InvalidDefinition {
                path: path.clone(),
                reason: error.to_string(),
            })?;
        return Ok(DiscoveredProjectDefinition { path, definition });
    }
    Err(DefinitionDiscoveryError::DefinitionNotFound { directory: start })
}
