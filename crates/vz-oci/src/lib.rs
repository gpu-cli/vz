//! OCI runtime layer for `vz`.

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

use std::path::PathBuf;

/// Runtime backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBackend {
    /// Linux OCI image backend (`vz-linux`).
    Linux,
    /// macOS sandbox backend (`vz-sandbox`).
    MacOS,
}

/// Top-level runtime configuration.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Base data directory for runtime metadata and caches.
    pub data_dir: PathBuf,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("~/.vz"),
        }
    }
}

/// Unified runtime entrypoint.
#[derive(Debug, Clone)]
pub struct Runtime {
    config: RuntimeConfig,
}

impl Runtime {
    /// Create a runtime instance.
    pub fn new(config: RuntimeConfig) -> Self {
        Self { config }
    }

    /// Return configured data directory.
    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
    }

    /// Pick backend from image reference and optional override.
    pub fn select_backend(image_ref: &str, force_macos: bool) -> RuntimeBackend {
        if force_macos || image_ref.starts_with("macos:") {
            RuntimeBackend::MacOS
        } else {
            RuntimeBackend::Linux
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_selection_defaults_to_linux() {
        let backend = Runtime::select_backend("ubuntu:24.04", false);
        assert_eq!(backend, RuntimeBackend::Linux);
    }

    #[test]
    fn backend_selection_supports_macos_override() {
        let backend = Runtime::select_backend("ubuntu:24.04", true);
        assert_eq!(backend, RuntimeBackend::MacOS);
    }

    #[test]
    fn backend_selection_supports_macos_prefix() {
        let backend = Runtime::select_backend("macos:sonoma", false);
        assert_eq!(backend, RuntimeBackend::MacOS);
    }
}
