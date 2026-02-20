//! Image cohort definitions for validation testing.
//!
//! Defines the tiered image cohorts used by CI gates.

use serde::{Deserialize, Serialize};

use crate::scenario::ScenarioKind;

/// CI gate tier for test scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    /// PR smoke gate: must pass for merge.
    Tier1,
    /// Nightly: full cohort matrix.
    Tier2,
    /// Weekly: stress and recovery tests.
    Tier3,
}

impl Tier {
    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Tier1 => "tier-1-smoke",
            Self::Tier2 => "tier-2-nightly",
            Self::Tier3 => "tier-3-weekly",
        }
    }
}

/// Reference to an OCI image with an optional pinned digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImageRef {
    /// Image reference (e.g., "alpine:3.20").
    pub reference: String,
    /// Pinned digest for reproducibility (e.g., "sha256:abc...").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Human-readable label (e.g., "Alpine 3.20").
    pub label: String,
}

/// An image cohort: a set of images with their applicable scenarios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageCohort {
    /// Cohort name.
    pub name: String,
    /// CI tier this cohort belongs to.
    pub tier: Tier,
    /// Images in this cohort.
    pub images: Vec<ImageRef>,
    /// Scenario kinds applicable to this cohort.
    pub scenarios: Vec<ScenarioKind>,
}

/// Build the Tier 1 PR smoke cohort.
pub fn tier1_smoke() -> ImageCohort {
    ImageCohort {
        name: "tier-1-smoke".to_string(),
        tier: Tier::Tier1,
        images: vec![
            ImageRef {
                reference: "alpine:3.20".to_string(),
                digest: None,
                label: "Alpine 3.20".to_string(),
            },
            ImageRef {
                reference: "python:3.12-slim".to_string(),
                digest: None,
                label: "Python 3.12 Slim".to_string(),
            },
            ImageRef {
                reference: "nginx:1.27-alpine".to_string(),
                digest: None,
                label: "Nginx 1.27 Alpine".to_string(),
            },
        ],
        scenarios: vec![
            ScenarioKind::EntrypointCmd,
            ScenarioKind::UserPermissions,
            ScenarioKind::SignalHandling,
        ],
    }
}

/// Build the Tier 2 nightly cohort.
pub fn tier2_nightly() -> ImageCohort {
    ImageCohort {
        name: "tier-2-nightly".to_string(),
        tier: Tier::Tier2,
        images: vec![
            ImageRef {
                reference: "alpine:3.20".to_string(),
                digest: None,
                label: "Alpine 3.20".to_string(),
            },
            ImageRef {
                reference: "python:3.12-slim".to_string(),
                digest: None,
                label: "Python 3.12 Slim".to_string(),
            },
            ImageRef {
                reference: "nginx:1.27-alpine".to_string(),
                digest: None,
                label: "Nginx 1.27 Alpine".to_string(),
            },
            ImageRef {
                reference: "redis:7-alpine".to_string(),
                digest: None,
                label: "Redis 7 Alpine".to_string(),
            },
            ImageRef {
                reference: "postgres:16-alpine".to_string(),
                digest: None,
                label: "Postgres 16 Alpine".to_string(),
            },
            ImageRef {
                reference: "node:20-slim".to_string(),
                digest: None,
                label: "Node 20 Slim".to_string(),
            },
        ],
        scenarios: vec![
            ScenarioKind::EntrypointCmd,
            ScenarioKind::UserPermissions,
            ScenarioKind::MountSemantics,
            ScenarioKind::SignalHandling,
            ScenarioKind::ServiceBehavior,
            ScenarioKind::ComposeFixture,
        ],
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn tier_labels() {
        assert_eq!(Tier::Tier1.label(), "tier-1-smoke");
        assert_eq!(Tier::Tier2.label(), "tier-2-nightly");
        assert_eq!(Tier::Tier3.label(), "tier-3-weekly");
    }

    #[test]
    fn image_ref_round_trip() {
        let img = ImageRef {
            reference: "alpine:3.20".to_string(),
            digest: Some("sha256:abc123".to_string()),
            label: "Alpine".to_string(),
        };
        let json = serde_json::to_string(&img).unwrap();
        let deserialized: ImageRef = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, img);
    }

    #[test]
    fn image_ref_without_digest() {
        let json = r#"{"reference":"alpine:3.20","label":"Alpine"}"#;
        let img: ImageRef = serde_json::from_str(json).unwrap();
        assert!(img.digest.is_none());
    }

    #[test]
    fn tier1_smoke_has_expected_images() {
        let cohort = tier1_smoke();
        assert_eq!(cohort.tier, Tier::Tier1);
        assert_eq!(cohort.images.len(), 3);
        let refs: Vec<&str> = cohort.images.iter().map(|i| i.reference.as_str()).collect();
        assert!(refs.contains(&"alpine:3.20"));
        assert!(refs.contains(&"python:3.12-slim"));
        assert!(refs.contains(&"nginx:1.27-alpine"));
    }

    #[test]
    fn tier2_nightly_is_superset_of_tier1() {
        let t1 = tier1_smoke();
        let t2 = tier2_nightly();
        let t2_refs: Vec<&str> = t2.images.iter().map(|i| i.reference.as_str()).collect();
        for img in &t1.images {
            assert!(
                t2_refs.contains(&img.reference.as_str()),
                "tier2 should include tier1 image: {}",
                img.reference
            );
        }
    }

    #[test]
    fn cohort_round_trip() {
        let cohort = tier1_smoke();
        let json = serde_json::to_string_pretty(&cohort).unwrap();
        let deserialized: ImageCohort = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, cohort.name);
        assert_eq!(deserialized.tier, cohort.tier);
        assert_eq!(deserialized.images.len(), cohort.images.len());
    }
}
