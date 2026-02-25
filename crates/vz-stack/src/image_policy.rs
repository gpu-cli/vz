//! OCI image reference policy enforcement.
//!
//! Provides [`ImagePolicy`] for controlling which image reference
//! forms are accepted during stack reconciliation. The key insight is
//! that only digest-pinned references (`image@sha256:...`) are truly
//! immutable — all tag-based references (including semver tags) can
//! be repointed at any time.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Policy controlling which OCI image reference forms are accepted.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ImagePolicy {
    /// Accept any image reference form (tags, digests, implicit latest).
    /// This is the default for backwards compatibility.
    #[default]
    AllowAll,
    /// Require all image references to be digest-pinned (`@sha256:...`).
    /// Mutable tag references are rejected with an error.
    RequireDigest,
    /// Accept all forms but emit a warning for mutable tag references.
    WarnOnMutableTag,
}

/// A violation of the configured image reference policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyViolation {
    /// The image reference that violated the policy.
    pub image: String,
    /// The kind of violation detected.
    pub kind: ViolationKind,
}

/// The specific type of policy violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViolationKind {
    /// The reference uses an implicit `:latest` tag (no tag or digest specified).
    ImplicitLatest,
    /// The reference uses an explicit mutable tag (e.g., `:v1.2.3`, `:latest`).
    MutableTag {
        /// The tag that was used.
        tag: String,
    },
}

impl fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ViolationKind::ImplicitLatest => {
                write!(
                    f,
                    "image `{}` uses implicit :latest tag; pin to a digest with @sha256:...",
                    self.image
                )
            }
            ViolationKind::MutableTag { tag } => {
                write!(
                    f,
                    "image `{}` uses mutable tag `:{}`; pin to a digest with @sha256:...",
                    self.image, tag
                )
            }
        }
    }
}

impl std::error::Error for PolicyViolation {}

/// Classification of an image reference's mutability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageRefKind {
    /// Reference is digest-pinned and immutable (contains `@sha256:`).
    DigestPinned,
    /// Reference uses an explicit tag (mutable).
    Tagged {
        /// The tag portion of the reference.
        tag: String,
    },
    /// Reference has no tag or digest, implying `:latest`.
    ImplicitLatest,
}

/// Classify an OCI image reference by its mutability.
///
/// An image reference has the general form:
/// `[registry/][repository/]name[:tag][@digest]`
///
/// If a digest (`@sha256:...`) is present, the reference is immutable
/// regardless of whether a tag is also present. Otherwise, if a tag
/// is present it's mutable (tags can be repointed). If neither is
/// present, the implicit `:latest` tag applies.
pub fn classify_image_ref(image: &str) -> ImageRefKind {
    // Check for digest — the `@sha256:` marker makes it immutable.
    if let Some(at_pos) = image.rfind('@') {
        let digest_part = &image[at_pos + 1..];
        if digest_part.starts_with("sha256:") || digest_part.starts_with("sha512:") {
            return ImageRefKind::DigestPinned;
        }
    }

    // No digest — check for an explicit tag.
    // Tags come after the last `:` that is NOT part of a port number in
    // the registry prefix. We find the "name" portion by looking after
    // the last `/`.
    let name_part = image.rsplit('/').next().unwrap_or(image);
    if let Some(colon_pos) = name_part.rfind(':') {
        let tag = &name_part[colon_pos + 1..];
        if !tag.is_empty() {
            return ImageRefKind::Tagged {
                tag: tag.to_string(),
            };
        }
    }

    ImageRefKind::ImplicitLatest
}

/// Validate an image reference against the given policy.
///
/// Returns `Ok(())` if the reference is acceptable, or a
/// [`PolicyViolation`] describing why it was rejected.
///
/// Under [`ImagePolicy::AllowAll`], all references pass.
/// Under [`ImagePolicy::WarnOnMutableTag`], all references pass
/// (warnings are logged via `tracing`).
/// Under [`ImagePolicy::RequireDigest`], only digest-pinned
/// references pass.
pub fn validate_image_reference(image: &str, policy: &ImagePolicy) -> Result<(), PolicyViolation> {
    match policy {
        ImagePolicy::AllowAll => Ok(()),
        ImagePolicy::RequireDigest => match classify_image_ref(image) {
            ImageRefKind::DigestPinned => Ok(()),
            ImageRefKind::ImplicitLatest => Err(PolicyViolation {
                image: image.to_string(),
                kind: ViolationKind::ImplicitLatest,
            }),
            ImageRefKind::Tagged { tag } => Err(PolicyViolation {
                image: image.to_string(),
                kind: ViolationKind::MutableTag { tag },
            }),
        },
        ImagePolicy::WarnOnMutableTag => {
            match classify_image_ref(image) {
                ImageRefKind::DigestPinned => {}
                ImageRefKind::ImplicitLatest => {
                    tracing::warn!(
                        image = image,
                        "image uses implicit :latest tag; consider pinning to a digest"
                    );
                }
                ImageRefKind::Tagged { ref tag } => {
                    tracing::warn!(
                        image = image,
                        tag = tag.as_str(),
                        "image uses mutable tag; consider pinning to a digest"
                    );
                }
            }
            Ok(())
        }
    }
}

/// Validate all service image references in a stack spec.
///
/// Returns the first violation found, or `Ok(())` if all pass.
pub fn validate_stack_images(
    services: &[crate::spec::ServiceSpec],
    policy: &ImagePolicy,
) -> Result<(), PolicyViolation> {
    for svc in services {
        validate_image_reference(&svc.image, policy)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    // ── classify_image_ref ────────────────────────────────────────

    #[test]
    fn digest_pinned_sha256() {
        let result = classify_image_ref(
            "docker.io/library/nginx@sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        );
        assert_eq!(result, ImageRefKind::DigestPinned);
    }

    #[test]
    fn digest_pinned_sha512() {
        let result = classify_image_ref("myregistry.io/repo@sha512:abcdef");
        assert_eq!(result, ImageRefKind::DigestPinned);
    }

    #[test]
    fn digest_pinned_with_tag_and_digest() {
        // When both tag and digest are present, digest wins (immutable).
        let result = classify_image_ref("nginx:1.25@sha256:abc123");
        assert_eq!(result, ImageRefKind::DigestPinned);
    }

    #[test]
    fn explicit_latest_tag() {
        let result = classify_image_ref("nginx:latest");
        assert_eq!(
            result,
            ImageRefKind::Tagged {
                tag: "latest".to_string()
            }
        );
    }

    #[test]
    fn semver_tag() {
        let result = classify_image_ref("postgres:16.2");
        assert_eq!(
            result,
            ImageRefKind::Tagged {
                tag: "16.2".to_string()
            }
        );
    }

    #[test]
    fn complex_semver_tag() {
        let result = classify_image_ref("ghcr.io/myorg/myapp:v1.2.3-rc1");
        assert_eq!(
            result,
            ImageRefKind::Tagged {
                tag: "v1.2.3-rc1".to_string()
            }
        );
    }

    #[test]
    fn implicit_latest_bare_name() {
        let result = classify_image_ref("nginx");
        assert_eq!(result, ImageRefKind::ImplicitLatest);
    }

    #[test]
    fn implicit_latest_with_registry() {
        let result = classify_image_ref("docker.io/library/nginx");
        assert_eq!(result, ImageRefKind::ImplicitLatest);
    }

    #[test]
    fn registry_with_port_and_tag() {
        // The registry has a port `:5000`, but the tag is `v1`.
        let result = classify_image_ref("localhost:5000/myapp:v1");
        assert_eq!(
            result,
            ImageRefKind::Tagged {
                tag: "v1".to_string()
            }
        );
    }

    #[test]
    fn registry_with_port_no_tag() {
        // `localhost:5000/myapp` — the `:5000` is part of the registry,
        // the image name `myapp` has no tag.
        let result = classify_image_ref("localhost:5000/myapp");
        assert_eq!(result, ImageRefKind::ImplicitLatest);
    }

    #[test]
    fn registry_with_port_and_digest() {
        let result = classify_image_ref("localhost:5000/myapp@sha256:abc");
        assert_eq!(result, ImageRefKind::DigestPinned);
    }

    // ── validate_image_reference ─────────────────────────────────

    #[test]
    fn allow_all_accepts_everything() {
        let policy = ImagePolicy::AllowAll;
        assert!(validate_image_reference("nginx", &policy).is_ok());
        assert!(validate_image_reference("nginx:latest", &policy).is_ok());
        assert!(validate_image_reference("nginx:1.25", &policy).is_ok());
        assert!(validate_image_reference("nginx@sha256:abc", &policy).is_ok());
    }

    #[test]
    fn require_digest_accepts_pinned() {
        let policy = ImagePolicy::RequireDigest;
        assert!(validate_image_reference("nginx@sha256:abc", &policy).is_ok());
        assert!(validate_image_reference("nginx:1.25@sha256:abc", &policy).is_ok());
    }

    #[test]
    fn require_digest_rejects_implicit_latest() {
        let policy = ImagePolicy::RequireDigest;
        let err = validate_image_reference("nginx", &policy).unwrap_err();
        assert_eq!(err.kind, ViolationKind::ImplicitLatest);
        assert!(err.to_string().contains("implicit :latest"));
    }

    #[test]
    fn require_digest_rejects_explicit_latest() {
        let policy = ImagePolicy::RequireDigest;
        let err = validate_image_reference("nginx:latest", &policy).unwrap_err();
        assert_eq!(
            err.kind,
            ViolationKind::MutableTag {
                tag: "latest".to_string()
            }
        );
        assert!(err.to_string().contains("mutable tag"));
    }

    #[test]
    fn require_digest_rejects_semver_tag() {
        let policy = ImagePolicy::RequireDigest;
        let err = validate_image_reference("postgres:16.2", &policy).unwrap_err();
        assert_eq!(
            err.kind,
            ViolationKind::MutableTag {
                tag: "16.2".to_string()
            }
        );
    }

    #[test]
    fn warn_on_mutable_tag_accepts_all() {
        let policy = ImagePolicy::WarnOnMutableTag;
        assert!(validate_image_reference("nginx", &policy).is_ok());
        assert!(validate_image_reference("nginx:latest", &policy).is_ok());
        assert!(validate_image_reference("nginx:1.25", &policy).is_ok());
        assert!(validate_image_reference("nginx@sha256:abc", &policy).is_ok());
    }

    // ── validate_stack_images ────────────────────────────────────

    #[test]
    fn stack_validation_all_pinned() {
        let services = vec![
            crate::spec::ServiceSpec {
                name: "web".to_string(),
                image: "nginx@sha256:abc".to_string(),
                ..make_minimal_service("web")
            },
            crate::spec::ServiceSpec {
                name: "db".to_string(),
                image: "postgres@sha256:def".to_string(),
                ..make_minimal_service("db")
            },
        ];
        assert!(validate_stack_images(&services, &ImagePolicy::RequireDigest).is_ok());
    }

    #[test]
    fn stack_validation_mixed_fails_on_first() {
        let services = vec![
            crate::spec::ServiceSpec {
                name: "web".to_string(),
                image: "nginx@sha256:abc".to_string(),
                ..make_minimal_service("web")
            },
            crate::spec::ServiceSpec {
                name: "db".to_string(),
                image: "postgres:16".to_string(),
                ..make_minimal_service("db")
            },
        ];
        let err = validate_stack_images(&services, &ImagePolicy::RequireDigest).unwrap_err();
        assert_eq!(err.image, "postgres:16");
    }

    // ── ImagePolicy serde round-trip ────────────────────────────

    #[test]
    fn image_policy_serde_round_trip() {
        let policies = [
            ImagePolicy::AllowAll,
            ImagePolicy::RequireDigest,
            ImagePolicy::WarnOnMutableTag,
        ];
        for policy in policies {
            let json = serde_json::to_string(&policy).unwrap();
            let decoded: ImagePolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, policy);
        }
    }

    #[test]
    fn image_policy_default_is_allow_all() {
        assert_eq!(ImagePolicy::default(), ImagePolicy::AllowAll);
    }

    // ── PolicyViolation display ─────────────────────────────────

    #[test]
    fn violation_display_implicit_latest() {
        let v = PolicyViolation {
            image: "nginx".to_string(),
            kind: ViolationKind::ImplicitLatest,
        };
        let msg = v.to_string();
        assert!(msg.contains("nginx"));
        assert!(msg.contains("implicit :latest"));
        assert!(msg.contains("@sha256:"));
    }

    #[test]
    fn violation_display_mutable_tag() {
        let v = PolicyViolation {
            image: "postgres:16".to_string(),
            kind: ViolationKind::MutableTag {
                tag: "16".to_string(),
            },
        };
        let msg = v.to_string();
        assert!(msg.contains("postgres:16"));
        assert!(msg.contains("mutable tag"));
        assert!(msg.contains(":16"));
    }

    // ── Helper ──────────────────────────────────────────────────

    fn make_minimal_service(name: &str) -> crate::spec::ServiceSpec {
        crate::spec::ServiceSpec {
            name: name.to_string(),
            kind: crate::spec::ServiceKind::Service,
            image: String::new(),
            command: None,
            entrypoint: None,
            environment: std::collections::HashMap::new(),
            working_dir: None,
            user: None,
            mounts: vec![],
            ports: vec![],
            depends_on: vec![],
            healthcheck: None,
            restart_policy: None,
            resources: crate::spec::ResourcesSpec::default(),
            extra_hosts: vec![],
            secrets: vec![],
            networks: vec![],
            cap_add: vec![],
            cap_drop: vec![],
            privileged: false,
            read_only: false,
            sysctls: std::collections::HashMap::new(),
            ulimits: vec![],
            container_name: None,
            hostname: None,
            domainname: None,
            labels: std::collections::HashMap::new(),
            stop_signal: None,
            stop_grace_period_secs: None,
            expose: vec![],
            stdin_open: false,
            tty: false,
            logging: None,
        }
    }
}
