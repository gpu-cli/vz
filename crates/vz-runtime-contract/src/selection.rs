//! Backend selection and auto-detection.
//!
//! Provides the [`HostBackend`] enum for choosing between macOS VM
//! and Linux-native container execution backends.

/// Available host backend implementations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum HostBackend {
    /// Automatically detect based on host OS.
    ///
    /// - macOS → `MacosVz`
    /// - Linux → `LinuxNative`
    #[default]
    Auto,
    /// macOS Virtualization.framework backend (runs containers in a Linux VM).
    MacosVz,
    /// Linux-native backend (runs containers directly via OCI runtime).
    LinuxNative,
}

impl HostBackend {
    /// Resolve `Auto` to a concrete backend based on the host OS.
    ///
    /// Also checks the `VZ_BACKEND` environment variable for overrides:
    /// - `VZ_BACKEND=macos` → `MacosVz`
    /// - `VZ_BACKEND=linux` → `LinuxNative`
    pub fn resolve(self) -> ResolvedBackend {
        // Check env override first.
        if let Ok(env_val) = std::env::var("VZ_BACKEND") {
            match env_val.to_lowercase().as_str() {
                "macos" | "macos-vz" | "vm" => return ResolvedBackend::MacosVz,
                "linux" | "linux-native" | "native" => return ResolvedBackend::LinuxNative,
                _ => {} // Fall through to auto-detection.
            }
        }

        match self {
            Self::MacosVz => ResolvedBackend::MacosVz,
            Self::LinuxNative => ResolvedBackend::LinuxNative,
            Self::Auto => detect_host_backend(),
        }
    }
}

/// Resolved (non-Auto) backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedBackend {
    /// macOS Virtualization.framework backend.
    MacosVz,
    /// Linux-native backend.
    LinuxNative,
}

impl ResolvedBackend {
    /// Human-readable backend name.
    pub fn name(self) -> &'static str {
        match self {
            Self::MacosVz => "macos-vz",
            Self::LinuxNative => "linux-native",
        }
    }

    /// Whether this is the macOS VM backend.
    pub fn is_macos(self) -> bool {
        matches!(self, Self::MacosVz)
    }

    /// Whether this is the Linux-native backend.
    pub fn is_linux(self) -> bool {
        matches!(self, Self::LinuxNative)
    }
}

/// Detect the appropriate backend based on the host operating system.
fn detect_host_backend() -> ResolvedBackend {
    if cfg!(target_os = "macos") {
        ResolvedBackend::MacosVz
    } else {
        ResolvedBackend::LinuxNative
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_platform_appropriate_backend() {
        let detected = detect_host_backend();
        if cfg!(target_os = "macos") {
            assert_eq!(detected, ResolvedBackend::MacosVz);
        } else {
            assert_eq!(detected, ResolvedBackend::LinuxNative);
        }
    }

    #[test]
    fn explicit_backend_resolves_directly() {
        // Explicit selections always resolve to themselves, regardless of env.
        assert_eq!(HostBackend::MacosVz.resolve(), ResolvedBackend::MacosVz);
        assert_eq!(
            HostBackend::LinuxNative.resolve(),
            ResolvedBackend::LinuxNative
        );
    }

    #[test]
    fn resolved_backend_names() {
        assert_eq!(ResolvedBackend::MacosVz.name(), "macos-vz");
        assert_eq!(ResolvedBackend::LinuxNative.name(), "linux-native");
    }

    #[test]
    fn resolved_backend_predicates() {
        assert!(ResolvedBackend::MacosVz.is_macos());
        assert!(!ResolvedBackend::MacosVz.is_linux());
        assert!(ResolvedBackend::LinuxNative.is_linux());
        assert!(!ResolvedBackend::LinuxNative.is_macos());
    }

    #[test]
    fn default_is_auto() {
        assert_eq!(HostBackend::default(), HostBackend::Auto);
    }
}
