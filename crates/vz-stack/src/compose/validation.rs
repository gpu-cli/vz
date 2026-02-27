use super::*;

pub(super) fn validate_top_level_keys(root: &serde_yml::Mapping) -> Result<(), StackError> {
    for key in root.keys() {
        let key_str = key.as_str().unwrap_or("");

        // Check rejected keys first (stable error codes).
        for &(rejected, reason) in REJECTED_TOP_LEVEL {
            if key_str == rejected {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: rejected.to_string(),
                    reason: reason.to_string(),
                });
            }
        }

        // Check accepted keys.
        if !ACCEPTED_TOP_LEVEL.contains(&key_str) {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: key_str.to_string(),
                reason: format!(
                    "unknown top-level key; accepted keys are: {}",
                    ACCEPTED_TOP_LEVEL.join(", ")
                ),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_service_keys(
    svc_name: &str,
    svc_map: &serde_yml::Mapping,
) -> Result<(), StackError> {
    for key in svc_map.keys() {
        let key_str = key.as_str().unwrap_or("");

        // Check rejected keys first (stable error codes).
        for &(rejected, reason) in REJECTED_SERVICE {
            if key_str == rejected {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: format!("services.{svc_name}.{rejected}"),
                    reason: reason.to_string(),
                });
            }
        }

        // Check accepted keys.
        if !ACCEPTED_SERVICE.contains(&key_str) {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.{key_str}"),
                reason: format!(
                    "unknown service key; accepted keys are: {}",
                    ACCEPTED_SERVICE.join(", ")
                ),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_volume_keys(
    vol_name: &str,
    vol_map: &serde_yml::Mapping,
) -> Result<(), StackError> {
    for key in vol_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if !ACCEPTED_VOLUME.contains(&key_str) {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("volumes.{vol_name}.{key_str}"),
                reason: format!(
                    "unknown volume key; accepted keys are: {}",
                    ACCEPTED_VOLUME.join(", ")
                ),
            });
        }
    }
    Ok(())
}

// ── Service parsing ────────────────────────────────────────────────
