//! Binds `config/vz-0.4-migration-barriers.json` to the state store it describes.
//!
//! The barrier inventory is a frozen input of the 0.4 release gate: the
//! migration lane replays each recorded mutation boundary, injects its failure
//! and checks its rollback assertion. A boundary the file omits is therefore a
//! boundary the release never proves, and the omission is silent — which is how
//! the v8-to-v9 teardown-runtime-identity step went unrecorded until the v10
//! network step was added beside it.
//!
//! So the binding is mechanical here rather than editorial: every state-store
//! schema version up to `StateStore::CURRENT_SCHEMA_VERSION` must be the target
//! of a declared step, each step must be a single version, and each barrier on
//! a step must carry the injected failure and rollback text the lane replays.
//! Adding a migration without its barrier fails this test.
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use vz_stack::StateStore;

const BARRIERS: &str = include_str!("../../../config/vz-0.4-migration-barriers.json");

/// The first version a store can be migrated from. v1 is the legacy v0.3.20
/// sandbox schema, which `migrate_legacy_v1_to_v2` is the only path out of.
const LEGACY_SCHEMA_VERSION: u32 = 1;

#[derive(Deserialize)]
struct Inventory {
    legacy_release_tag: String,
    barriers: Vec<Barrier>,
}

#[derive(Deserialize)]
struct Barrier {
    id: String,
    domain: String,
    component: String,
    source_reference: String,
    pre_state: String,
    post_state: String,
    failure_injection: String,
    expected_failure_state: String,
    rollback_assertion: String,
    receipt_field: String,
    #[serde(default)]
    state_schema_step: Option<Step>,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Step {
    from: u32,
    to: u32,
}

/// The repository root, so a barrier's cited source path can be checked.
fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("vz-stack lives two directories below the repository root")
        .to_path_buf()
}

fn inventory() -> Inventory {
    serde_json::from_str(BARRIERS).expect("migration barrier inventory parses")
}

#[test]
fn every_state_schema_version_has_a_declared_migration_barrier() {
    let inventory = inventory();
    let mut covered: BTreeMap<u32, Vec<&str>> = BTreeMap::new();
    for barrier in &inventory.barriers {
        if let Some(step) = barrier.state_schema_step {
            covered.entry(step.to).or_default().push(&barrier.id);
        }
    }

    let missing: Vec<u32> = (LEGACY_SCHEMA_VERSION + 1..=StateStore::CURRENT_SCHEMA_VERSION)
        .filter(|version| !covered.contains_key(version))
        .collect();
    assert!(
        missing.is_empty(),
        "state schema versions {missing:?} have no migration barrier in \
         config/vz-0.4-migration-barriers.json; the release gate's migration lane would \
         never replay their mutation boundary. Add a barrier per stage of the migration \
         that produces each version, with its injected failure and rollback assertion."
    );

    // A barrier may not claim a version this store cannot produce, which would
    // make the lane replay a boundary that does not exist.
    let unreachable: Vec<(u32, &Vec<&str>)> = covered
        .iter()
        .filter(|(version, _)| **version > StateStore::CURRENT_SCHEMA_VERSION)
        .map(|(version, ids)| (*version, ids))
        .collect();
    assert!(
        unreachable.is_empty(),
        "barriers {unreachable:?} declare a schema version above \
         StateStore::CURRENT_SCHEMA_VERSION ({})",
        StateStore::CURRENT_SCHEMA_VERSION
    );
}

#[test]
fn each_declared_step_advances_exactly_one_version() {
    for barrier in &inventory().barriers {
        let Some(step) = barrier.state_schema_step else {
            continue;
        };
        assert_eq!(
            step.to,
            step.from + 1,
            "barrier {} declares step {step:?}; a state-store migration advances \
             user_version by exactly one",
            barrier.id
        );
        assert!(
            step.from >= LEGACY_SCHEMA_VERSION,
            "barrier {} migrates from below the legacy schema version",
            barrier.id
        );
    }
}

#[test]
fn barriers_outside_the_state_store_declare_no_schema_step() {
    // These record mutation boundaries the state store does not own: the backup
    // file, the legacy checkpoint archive, Docker context ownership and the
    // credential scope. Giving one a version step would claim the schema
    // migration proves it.
    let outside: BTreeSet<&str> = ["filesystem", "context", "credential"]
        .into_iter()
        .collect();
    for barrier in &inventory().barriers {
        if outside.contains(barrier.domain.as_str()) {
            assert!(
                barrier.state_schema_step.is_none(),
                "barrier {} is domain {} but declares a state schema step",
                barrier.id,
                barrier.domain
            );
        }
    }
}

#[test]
fn every_barrier_carries_the_evidence_the_lane_replays() {
    let inventory = inventory();
    assert_eq!(
        inventory.legacy_release_tag, "v0.3.20",
        "the migration fixture release is pinned by GOAL-0.4.0.md"
    );
    let mut ids = BTreeSet::new();
    let mut receipts = BTreeSet::new();
    for barrier in &inventory.barriers {
        assert!(
            ids.insert(&barrier.id),
            "duplicate barrier id {}",
            barrier.id
        );
        assert!(
            receipts.insert(&barrier.receipt_field),
            "duplicate receipt field {} on barrier {}",
            barrier.receipt_field,
            barrier.id
        );
        // The lane cannot replay a boundary whose failure or rollback is unstated,
        // and a placeholder would pass a mere non-empty check, so require prose.
        // `component` names a thing and is legitimately short; the rest are the
        // narrative the lane replays and a placeholder there must not pass.
        assert!(
            !barrier.component.trim().is_empty(),
            "barrier {} names no component",
            barrier.id
        );
        // A source reference names the file the boundary lives in, then its
        // symbols. Checking the path exists catches a reference left behind by a
        // move, which would send the lane author to a file that is not there.
        let cited = barrier
            .source_reference
            .split_whitespace()
            .next()
            .unwrap_or_default();
        let path = repo_root().join(cited.split('#').next().unwrap_or_default());
        assert!(
            path.is_file(),
            "barrier {} cites {cited}, which is not a file in the repository",
            barrier.id
        );
        for (field, value) in [
            ("pre_state", &barrier.pre_state),
            ("post_state", &barrier.post_state),
            ("failure_injection", &barrier.failure_injection),
            ("expected_failure_state", &barrier.expected_failure_state),
            ("rollback_assertion", &barrier.rollback_assertion),
        ] {
            assert!(
                value.split_whitespace().count() >= 5,
                "barrier {} field {field} is too terse to replay: {value:?}",
                barrier.id
            );
        }
        assert!(
            barrier.receipt_field.starts_with("receipts."),
            "barrier {} receipt field {} is not addressed under receipts.",
            barrier.id,
            barrier.receipt_field
        );
    }
}
