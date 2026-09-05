//! Canonical request binding shared by Delete admission and observation.
use sha2::{Digest, Sha256};

use crate::{EnvironmentId, EnvironmentSelectionContext, ProjectId, TopologyOperation};

/// Bind the exact normalized request to its resolved immutable Environment.
///
/// Callers preserve the original selector variant and workspace token; a
/// `NameOrId` string is never reinterpreted as an ID based on its spelling.
/// The tuple encoding intentionally preserves the existing durable Delete
/// request hash, including its numeric millisecond timeout.
pub fn environment_delete_request_hash(
    project: &ProjectId,
    environment: &EnvironmentId,
    selection: &EnvironmentSelectionContext,
    timeout_millis: u128,
) -> Result<String, serde_json::Error> {
    let raw = serde_json::to_vec(&(
        TopologyOperation::Delete,
        project,
        environment,
        selection,
        timeout_millis,
    ))?;
    Ok(format!("sha256:{:x}", Sha256::digest(raw)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::EnvironmentSelector;

    #[test]
    fn delete_hash_preserves_original_tuple_bytes_and_id_like_human_selector() {
        let project = ProjectId::new("prj_request_hash").unwrap();
        let environment = EnvironmentId::new("env_resolved").unwrap();
        let selection = EnvironmentSelectionContext {
            explicit: Some(EnvironmentSelector::NameOrId("env_human_name".into())),
            ..Default::default()
        };
        let original = br#"["delete","prj_request_hash","env_resolved",{"explicit":{"kind":"name_or_id","value":"env_human_name"}},120000]"#;
        assert_eq!(
            environment_delete_request_hash(&project, &environment, &selection, 120_000).unwrap(),
            format!("sha256:{:x}", Sha256::digest(original)),
        );
        let old_u64 = serde_json::to_vec(&(
            TopologyOperation::Delete,
            &project,
            &environment,
            &selection,
            120_000_u64,
        ))
        .unwrap();
        assert_eq!(old_u64, original);
        let typed = EnvironmentSelectionContext {
            explicit: Some(EnvironmentSelector::Id(
                EnvironmentId::new("env_human_name").unwrap(),
            )),
            ..Default::default()
        };
        assert_ne!(
            environment_delete_request_hash(&project, &environment, &selection, 120_000).unwrap(),
            environment_delete_request_hash(&project, &environment, &typed, 120_000).unwrap()
        );
    }

    #[test]
    fn delete_hash_binds_all_selection_inputs_resolved_scope_and_timeout() {
        let project = ProjectId::new("prj_request_hash").unwrap();
        let environment = EnvironmentId::new("env_resolved").unwrap();
        let selection = EnvironmentSelectionContext::default();
        let expected =
            environment_delete_request_hash(&project, &environment, &selection, 60_000).unwrap();
        for changed in [
            EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::NameOrId("other".into())),
                ..Default::default()
            },
            EnvironmentSelectionContext {
                process_environment_id: Some(environment.clone()),
                ..Default::default()
            },
            EnvironmentSelectionContext {
                workspace_key: Some("exact-worktree-token".into()),
                ..Default::default()
            },
        ] {
            assert_ne!(
                environment_delete_request_hash(&project, &environment, &changed, 60_000).unwrap(),
                expected
            );
        }
        assert_ne!(
            environment_delete_request_hash(&project, &environment, &selection, 60_001).unwrap(),
            expected
        );
        assert_ne!(
            environment_delete_request_hash(
                &ProjectId::generate(),
                &environment,
                &selection,
                60_000
            )
            .unwrap(),
            expected
        );
        assert_ne!(
            environment_delete_request_hash(
                &project,
                &EnvironmentId::generate(),
                &selection,
                60_000
            )
            .unwrap(),
            expected
        );
    }
}
