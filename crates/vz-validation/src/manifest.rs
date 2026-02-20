//! Versioned cohort manifest for validation runs.
//!
//! Provides a [`CohortManifest`] that bundles image cohorts with
//! per-image behavior profiles, scenario applicability filters,
//! and manifest-level versioning metadata.
//!
//! The manifest is the single source of truth consumed by the
//! validation harness. It can be serialized to JSON for CI
//! artifact retention and loaded back for regression comparison.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::cohort::{ImageCohort, Tier, tier1_smoke, tier2_nightly};
use crate::scenario::{
    Scenario, ScenarioKind, s1_entrypoint_scenarios, s1_env_cwd_scenarios, s2_user_scenarios,
    s4_signal_scenarios, s5_service_scenarios,
};

/// Current manifest format version.
pub const MANIFEST_VERSION: &str = "1.0.0";

/// A versioned manifest containing all cohorts and their behavior profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CohortManifest {
    /// Manifest format version (semver).
    pub version: String,
    /// ISO 8601 timestamp when the manifest was last updated.
    pub updated_at: String,
    /// Image cohorts keyed by tier.
    pub cohorts: Vec<ImageCohort>,
    /// Per-image behavior profiles keyed by image reference string.
    pub profiles: HashMap<String, ImageProfile>,
    /// Scenario applicability: which scenario kinds apply to which images.
    /// Keyed by image reference string, value is the list of applicable scenario kinds.
    pub applicability: HashMap<String, Vec<ScenarioKind>>,
}

/// Behavior profile for a specific image.
///
/// Captures image-specific expectations like the default user,
/// whether the image runs a long-lived service, and expected
/// default command behavior.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImageProfile {
    /// Image reference string (e.g., "alpine:3.20").
    pub reference: String,
    /// Human-readable label.
    pub label: String,
    /// Expected default user (e.g., "root", "nobody").
    pub default_user: String,
    /// Whether the image runs a long-lived service (nginx, redis, etc.).
    pub is_service: bool,
    /// Expected default entrypoint behavior.
    pub default_behavior: DefaultBehavior,
    /// Service-specific metadata (port, readiness probe, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_info: Option<ServiceInfo>,
}

/// How the image behaves when run with no command override.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DefaultBehavior {
    /// Exits immediately with the given code (e.g., alpine with no cmd).
    ExitsWithCode(i32),
    /// Runs a shell and exits with code 0.
    ShellExit,
    /// Starts a long-lived process (services like nginx, redis).
    LongRunning,
}

/// Metadata for service images (nginx, redis, postgres).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceInfo {
    /// Default exposed port.
    pub default_port: u16,
    /// Protocol for readiness probe.
    pub probe_protocol: ProbeProtocol,
}

/// Protocol used to probe service readiness.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProbeProtocol {
    /// HTTP GET on a path.
    Http { path: String },
    /// TCP connection check.
    Tcp,
    /// Redis PING command.
    RedisPing,
    /// PostgreSQL pg_isready.
    PgReady,
}

impl CohortManifest {
    /// Load a manifest from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize the manifest to pretty-printed JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Get the cohort for a specific tier.
    pub fn cohort_for_tier(&self, tier: Tier) -> Option<&ImageCohort> {
        self.cohorts.iter().find(|c| c.tier == tier)
    }

    /// Get the behavior profile for an image reference.
    pub fn profile_for(&self, reference: &str) -> Option<&ImageProfile> {
        self.profiles.get(reference)
    }

    /// Get applicable scenario kinds for an image.
    pub fn applicable_scenarios(&self, reference: &str) -> &[ScenarioKind] {
        self.applicability
            .get(reference)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Resolve scenarios for a given image from the built-in scenario builders.
    ///
    /// Filters scenarios to only those whose kind is applicable to this image.
    pub fn scenarios_for_image(&self, reference: &str) -> Vec<Scenario> {
        let applicable = self.applicable_scenarios(reference);
        let mut scenarios = Vec::new();

        for kind in applicable {
            match kind {
                ScenarioKind::EntrypointCmd => {
                    scenarios.extend(s1_entrypoint_scenarios());
                    scenarios.extend(s1_env_cwd_scenarios());
                }
                ScenarioKind::UserPermissions => {
                    scenarios.extend(s2_user_scenarios());
                }
                ScenarioKind::SignalHandling => {
                    scenarios.extend(s4_signal_scenarios());
                }
                ScenarioKind::ServiceBehavior => {
                    scenarios.extend(s5_service_scenarios());
                }
                // S3, S6 builders will be added in future beads.
                _ => {}
            }
        }

        scenarios
    }

    /// Total number of unique images across all cohorts.
    pub fn total_images(&self) -> usize {
        self.profiles.len()
    }

    /// Total number of image+scenario combinations for a tier.
    pub fn test_matrix_size(&self, tier: Tier) -> usize {
        let Some(cohort) = self.cohort_for_tier(tier) else {
            return 0;
        };
        cohort
            .images
            .iter()
            .map(|img| self.scenarios_for_image(&img.reference).len())
            .sum()
    }
}

/// Build the default manifest with all standard cohorts and profiles.
pub fn default_manifest(updated_at: &str) -> CohortManifest {
    let cohorts = vec![tier1_smoke(), tier2_nightly()];

    let profiles = build_profiles();
    let applicability = build_applicability();

    CohortManifest {
        version: MANIFEST_VERSION.to_string(),
        updated_at: updated_at.to_string(),
        cohorts,
        profiles,
        applicability,
    }
}

fn build_profiles() -> HashMap<String, ImageProfile> {
    let mut profiles = HashMap::new();

    profiles.insert(
        "alpine:3.20".to_string(),
        ImageProfile {
            reference: "alpine:3.20".to_string(),
            label: "Alpine 3.20".to_string(),
            default_user: "root".to_string(),
            is_service: false,
            default_behavior: DefaultBehavior::ShellExit,
            service_info: None,
        },
    );

    profiles.insert(
        "python:3.12-slim".to_string(),
        ImageProfile {
            reference: "python:3.12-slim".to_string(),
            label: "Python 3.12 Slim".to_string(),
            default_user: "root".to_string(),
            is_service: false,
            default_behavior: DefaultBehavior::ShellExit,
            service_info: None,
        },
    );

    profiles.insert(
        "nginx:1.27-alpine".to_string(),
        ImageProfile {
            reference: "nginx:1.27-alpine".to_string(),
            label: "Nginx 1.27 Alpine".to_string(),
            default_user: "root".to_string(),
            is_service: true,
            default_behavior: DefaultBehavior::LongRunning,
            service_info: Some(ServiceInfo {
                default_port: 80,
                probe_protocol: ProbeProtocol::Http {
                    path: "/".to_string(),
                },
            }),
        },
    );

    profiles.insert(
        "redis:7-alpine".to_string(),
        ImageProfile {
            reference: "redis:7-alpine".to_string(),
            label: "Redis 7 Alpine".to_string(),
            default_user: "redis".to_string(),
            is_service: true,
            default_behavior: DefaultBehavior::LongRunning,
            service_info: Some(ServiceInfo {
                default_port: 6379,
                probe_protocol: ProbeProtocol::RedisPing,
            }),
        },
    );

    profiles.insert(
        "postgres:16-alpine".to_string(),
        ImageProfile {
            reference: "postgres:16-alpine".to_string(),
            label: "Postgres 16 Alpine".to_string(),
            default_user: "postgres".to_string(),
            is_service: true,
            default_behavior: DefaultBehavior::LongRunning,
            service_info: Some(ServiceInfo {
                default_port: 5432,
                probe_protocol: ProbeProtocol::PgReady,
            }),
        },
    );

    profiles.insert(
        "node:20-slim".to_string(),
        ImageProfile {
            reference: "node:20-slim".to_string(),
            label: "Node 20 Slim".to_string(),
            default_user: "root".to_string(),
            is_service: false,
            default_behavior: DefaultBehavior::ShellExit,
            service_info: None,
        },
    );

    profiles
}

fn build_applicability() -> HashMap<String, Vec<ScenarioKind>> {
    let mut app = HashMap::new();

    // All images get entrypoint/cmd and user/permissions scenarios.
    let base_scenarios = vec![ScenarioKind::EntrypointCmd, ScenarioKind::UserPermissions];

    // Non-service images: base scenarios + signal handling.
    let utility_scenarios = vec![
        ScenarioKind::EntrypointCmd,
        ScenarioKind::UserPermissions,
        ScenarioKind::SignalHandling,
    ];

    // Service images: all scenario kinds.
    let service_scenarios = vec![
        ScenarioKind::EntrypointCmd,
        ScenarioKind::UserPermissions,
        ScenarioKind::MountSemantics,
        ScenarioKind::SignalHandling,
        ScenarioKind::ServiceBehavior,
    ];

    app.insert("alpine:3.20".to_string(), utility_scenarios.clone());
    app.insert("python:3.12-slim".to_string(), utility_scenarios.clone());
    app.insert("node:20-slim".to_string(), utility_scenarios);

    app.insert("nginx:1.27-alpine".to_string(), service_scenarios.clone());
    app.insert("redis:7-alpine".to_string(), service_scenarios.clone());
    app.insert("postgres:16-alpine".to_string(), service_scenarios);

    // Tier 1 images only get base scenarios (entrypoint + user).
    // The applicability map defines the maximum; the cohort's scenario list
    // further restricts what actually runs. This is handled by the harness
    // intersecting cohort.scenarios with applicability.
    let _ = base_scenarios; // used conceptually, intersection done at runtime

    app
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn test_manifest() -> CohortManifest {
        default_manifest("2026-02-20T00:00:00Z")
    }

    #[test]
    fn manifest_has_correct_version() {
        let m = test_manifest();
        assert_eq!(m.version, MANIFEST_VERSION);
    }

    #[test]
    fn manifest_has_both_tiers() {
        let m = test_manifest();
        assert!(m.cohort_for_tier(Tier::Tier1).is_some());
        assert!(m.cohort_for_tier(Tier::Tier2).is_some());
        assert!(m.cohort_for_tier(Tier::Tier3).is_none());
    }

    #[test]
    fn manifest_has_all_profiles() {
        let m = test_manifest();
        assert_eq!(m.total_images(), 6);
        assert!(m.profile_for("alpine:3.20").is_some());
        assert!(m.profile_for("nginx:1.27-alpine").is_some());
        assert!(m.profile_for("redis:7-alpine").is_some());
        assert!(m.profile_for("postgres:16-alpine").is_some());
        assert!(m.profile_for("python:3.12-slim").is_some());
        assert!(m.profile_for("node:20-slim").is_some());
    }

    #[test]
    fn service_images_have_service_info() {
        let m = test_manifest();
        let nginx = m.profile_for("nginx:1.27-alpine").unwrap();
        assert!(nginx.is_service);
        assert!(nginx.service_info.is_some());
        assert_eq!(nginx.service_info.as_ref().unwrap().default_port, 80);

        let redis = m.profile_for("redis:7-alpine").unwrap();
        assert!(redis.is_service);
        assert_eq!(redis.default_user, "redis");
    }

    #[test]
    fn utility_images_are_not_services() {
        let m = test_manifest();
        let alpine = m.profile_for("alpine:3.20").unwrap();
        assert!(!alpine.is_service);
        assert!(alpine.service_info.is_none());
    }

    #[test]
    fn applicability_gives_more_scenarios_to_service_images() {
        let m = test_manifest();
        let alpine = m.applicable_scenarios("alpine:3.20");
        let nginx = m.applicable_scenarios("nginx:1.27-alpine");
        assert!(nginx.len() > alpine.len());
        assert!(nginx.contains(&ScenarioKind::ServiceBehavior));
        assert!(!alpine.contains(&ScenarioKind::ServiceBehavior));
    }

    #[test]
    fn scenarios_for_image_filters_correctly() {
        let m = test_manifest();
        let alpine_scenarios = m.scenarios_for_image("alpine:3.20");
        // Alpine gets S1 (3 base + 2 env/cwd) + S2 (2) + S4 (1) = 8
        assert_eq!(alpine_scenarios.len(), 8);

        // All scenarios should be from applicable kinds.
        let applicable = m.applicable_scenarios("alpine:3.20");
        for s in &alpine_scenarios {
            assert!(applicable.contains(&s.kind));
        }

        // Service images get S5 scenarios too.
        let nginx_scenarios = m.scenarios_for_image("nginx:1.27-alpine");
        // nginx: S1 (5) + S2 (2) + S4 (1) + S5 (2) + S3 (no builder) = 10
        assert_eq!(nginx_scenarios.len(), 10);
        assert!(nginx_scenarios.iter().any(|s| s.kind == ScenarioKind::ServiceBehavior));
    }

    #[test]
    fn unknown_image_returns_empty() {
        let m = test_manifest();
        assert!(m.applicable_scenarios("nonexistent:latest").is_empty());
        assert!(m.scenarios_for_image("nonexistent:latest").is_empty());
        assert!(m.profile_for("nonexistent:latest").is_none());
    }

    #[test]
    fn manifest_json_round_trip() {
        let m = test_manifest();
        let json = m.to_json().unwrap();
        let deserialized = CohortManifest::from_json(&json).unwrap();
        assert_eq!(deserialized.version, m.version);
        assert_eq!(deserialized.updated_at, m.updated_at);
        assert_eq!(deserialized.cohorts.len(), m.cohorts.len());
        assert_eq!(deserialized.profiles.len(), m.profiles.len());
        assert_eq!(deserialized.applicability.len(), m.applicability.len());
    }

    #[test]
    fn manifest_json_preserves_profiles() {
        let m = test_manifest();
        let json = m.to_json().unwrap();
        let deserialized = CohortManifest::from_json(&json).unwrap();
        let nginx = deserialized.profile_for("nginx:1.27-alpine").unwrap();
        assert_eq!(
            nginx,
            m.profile_for("nginx:1.27-alpine").unwrap()
        );
    }

    #[test]
    fn test_matrix_size_tier1() {
        let m = test_manifest();
        // Tier 1 has 3 images:
        // alpine: S1(5) + S2(2) + S4(1) = 8
        // python: S1(5) + S2(2) + S4(1) = 8
        // nginx:  S1(5) + S2(2) + S4(1) + S5(2) = 10 (S3 has no builder)
        let size = m.test_matrix_size(Tier::Tier1);
        assert!(size > 0);
        assert_eq!(size, 26);
    }

    #[test]
    fn test_matrix_size_nonexistent_tier() {
        let m = test_manifest();
        assert_eq!(m.test_matrix_size(Tier::Tier3), 0);
    }

    #[test]
    fn default_behavior_variants_round_trip() {
        let behaviors = vec![
            DefaultBehavior::ExitsWithCode(0),
            DefaultBehavior::ShellExit,
            DefaultBehavior::LongRunning,
        ];
        for b in &behaviors {
            let json = serde_json::to_string(b).unwrap();
            let d: DefaultBehavior = serde_json::from_str(&json).unwrap();
            assert_eq!(&d, b);
        }
    }

    #[test]
    fn probe_protocol_variants_round_trip() {
        let probes = vec![
            ProbeProtocol::Http {
                path: "/health".to_string(),
            },
            ProbeProtocol::Tcp,
            ProbeProtocol::RedisPing,
            ProbeProtocol::PgReady,
        ];
        for p in &probes {
            let json = serde_json::to_string(p).unwrap();
            let d: ProbeProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(&d, p);
        }
    }
}
