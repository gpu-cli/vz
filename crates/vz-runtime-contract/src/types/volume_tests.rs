#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

fn attachment(machine: &str, mode: VolumeAccessMode) -> VolumeAttachment {
    VolumeAttachment {
        machine: machine.to_string(),
        target_path: "/cache".to_string(),
        mode,
    }
}

fn block(size_bytes: Option<u64>, attachments: Vec<VolumeAttachment>) -> VolumeSpec {
    VolumeSpec {
        schema_version: 1,
        name: "data".to_string(),
        kind: VolumeKind::Block,
        size_bytes,
        consistency: None,
        attachments,
    }
}

fn shared(
    consistency: Option<SharedCacheConsistency>,
    attachments: Vec<VolumeAttachment>,
) -> VolumeSpec {
    VolumeSpec {
        schema_version: 1,
        name: "cache".to_string(),
        kind: VolumeKind::SharedCache,
        size_bytes: None,
        consistency,
        attachments,
    }
}

fn bounded(millis: u32) -> SharedCacheConsistency {
    SharedCacheConsistency {
        model: SharedCacheConsistencyModel::BoundedStaleness,
        staleness_bound_millis: millis,
    }
}

#[test]
fn a_sized_single_writer_block_volume_validates() {
    let spec = block(
        Some(64 * 1024 * 1024),
        vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
    );
    validate_volume(&spec).expect("a sized, single-writer block volume is valid");
    assert_eq!(spec.writers().count(), 1);
}

#[test]
fn a_block_volume_without_a_size_is_refused() {
    // Vacuity guard for the case above: the happy path is only evidence if the
    // same declaration minus its size actually fails.
    let error = validate_volume(&block(
        None,
        vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("requires `size_bytes`"),
        "{error}"
    );
}

#[test]
fn a_block_volume_outside_the_size_domain_is_refused() {
    for size in [MIN_BLOCK_VOLUME_BYTES - 1, MAX_BLOCK_VOLUME_BYTES + 1] {
        let error = validate_volume(&block(
            Some(size),
            vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
        ))
        .unwrap_err();
        assert!(error.to_string().contains("1 MiB..=1 TiB"), "{size}");
    }
}

#[test]
fn a_block_volume_declaring_shared_cache_consistency_is_refused() {
    let mut spec = block(
        Some(MIN_BLOCK_VOLUME_BYTES),
        vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
    );
    spec.consistency = Some(bounded(1000));
    let error = validate_volume(&spec).unwrap_err();
    assert!(
        error.to_string().contains("must not declare `consistency`"),
        "{error}"
    );
}

#[test]
fn a_shared_cache_validates_only_with_an_explicit_consistency_declaration() {
    let attachments = vec![
        attachment("machine-0", VolumeAccessMode::ReadWrite),
        attachment("machine-1", VolumeAccessMode::ReadWrite),
    ];
    validate_volume(&shared(Some(bounded(2000)), attachments.clone()))
        .expect("a declared shared cache is valid");
    let error = validate_volume(&shared(None, attachments)).unwrap_err();
    assert!(
        error.to_string().contains("requires an explicit `consistency`"),
        "{error}"
    );
}

#[test]
fn a_shared_cache_staleness_bound_outside_the_domain_is_refused() {
    // Zero is the value the carrier certainly cannot honour, so the declaration
    // must not be able to state it.
    for millis in [0, MAX_STALENESS_BOUND_MILLIS + 1] {
        let error = validate_volume(&shared(
            Some(bounded(millis)),
            vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
        ))
        .unwrap_err();
        assert!(
            error.to_string().contains("staleness_bound_millis"),
            "{millis}: {error}"
        );
    }
}

#[test]
fn a_shared_cache_declaring_a_size_is_refused() {
    let mut spec = shared(
        Some(bounded(1000)),
        vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
    );
    spec.size_bytes = Some(MIN_BLOCK_VOLUME_BYTES);
    let error = validate_volume(&spec).unwrap_err();
    assert!(
        error.to_string().contains("must not declare `size_bytes`"),
        "{error}"
    );
}

#[test]
fn a_volume_with_no_attachment_is_refused() {
    let error = validate_volume(&block(Some(MIN_BLOCK_VOLUME_BYTES), Vec::new())).unwrap_err();
    assert!(error.to_string().contains("1..=128 attachments"), "{error}");
}

#[test]
fn one_machine_attaching_one_volume_twice_is_refused() {
    let error = validate_volume(&shared(
        Some(bounded(1000)),
        vec![
            attachment("machine-0", VolumeAccessMode::ReadWrite),
            VolumeAttachment {
                machine: "machine-0".to_string(),
                target_path: "/other".to_string(),
                mode: VolumeAccessMode::ReadOnly,
            },
        ],
    ))
    .unwrap_err();
    assert!(error.to_string().contains("duplicate"), "{error}");
}

#[test]
fn an_attachment_target_path_must_be_a_bounded_absolute_traversal_free_machine_path() {
    for target in ["relative/path", "/escapes/../..", "/nul\u{0}", ""] {
        let spec = shared(
            Some(bounded(1000)),
            vec![VolumeAttachment {
                machine: "machine-0".to_string(),
                target_path: target.to_string(),
                mode: VolumeAccessMode::ReadWrite,
            }],
        );
        let error = validate_volume(&spec).unwrap_err();
        assert!(
            error.to_string().contains("Machine path"),
            "{target:?}: {error}"
        );
    }
}

#[test]
fn writers_counts_only_read_write_attachments() {
    let spec = shared(
        Some(bounded(1000)),
        vec![
            attachment("machine-0", VolumeAccessMode::ReadWrite),
            attachment("machine-1", VolumeAccessMode::ReadOnly),
            attachment("machine-2", VolumeAccessMode::ReadWrite),
        ],
    );
    let writers: Vec<&str> = spec
        .writers()
        .map(|attachment| attachment.machine.as_str())
        .collect();
    assert_eq!(writers, ["machine-0", "machine-2"]);
    assert!(VolumeAccessMode::ReadWrite.writes());
    assert!(!VolumeAccessMode::ReadOnly.writes());
}

#[test]
fn an_unsupported_schema_version_is_refused_before_anything_else() {
    let mut spec = block(
        Some(MIN_BLOCK_VOLUME_BYTES),
        vec![attachment("machine-0", VolumeAccessMode::ReadWrite)],
    );
    spec.schema_version = 2;
    let error = validate_volume(&spec).unwrap_err();
    assert!(
        matches!(
            error,
            TopologyValidationError::UnsupportedSchemaVersion {
                found: 2,
                supported: 1
            }
        ),
        "{error}"
    );
}

#[test]
fn the_wire_form_of_every_enum_is_snake_case_and_round_trips() {
    // The JSON authoring schema restates these spellings, so a rename here that
    // the schema did not learn about would make a valid vz.json undeserializable.
    for (kind, wire) in [
        (VolumeKind::Block, "\"block\""),
        (VolumeKind::SharedCache, "\"shared_cache\""),
    ] {
        assert_eq!(serde_json::to_string(&kind).unwrap(), wire);
        assert_eq!(
            serde_json::from_str::<VolumeKind>(wire).unwrap(),
            kind,
            "{wire}"
        );
    }
    for (mode, wire) in [
        (VolumeAccessMode::ReadWrite, "\"read_write\""),
        (VolumeAccessMode::ReadOnly, "\"read_only\""),
    ] {
        assert_eq!(serde_json::to_string(&mode).unwrap(), wire);
        assert_eq!(
            serde_json::from_str::<VolumeAccessMode>(wire).unwrap(),
            mode,
            "{wire}"
        );
    }
    assert_eq!(
        serde_json::to_string(&SharedCacheConsistencyModel::BoundedStaleness).unwrap(),
        "\"bounded_staleness\""
    );
}

#[test]
fn an_unknown_field_is_refused_rather_than_dropped() {
    // `deny_unknown_fields` is what makes the authoring schema's
    // `additionalProperties: false` and this parser agree; a misspelled
    // `size_byte` must fail rather than silently leave the volume unsized.
    let error = serde_json::from_str::<VolumeSpec>(
        r#"{"schema_version":1,"name":"data","kind":"block","size_byte":1048576,"attachments":[]}"#,
    )
    .unwrap_err();
    assert!(error.to_string().contains("size_byte"), "{error}");
}
