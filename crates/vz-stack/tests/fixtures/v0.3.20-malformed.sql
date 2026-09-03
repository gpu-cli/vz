-- Apply after v0.3.20-state.sql. The positive Developer marker makes this row
-- migration-eligible, but malformed spec JSON must abort the whole migration.
INSERT INTO sandbox_state (
    sandbox_id, stack_name, state, backend, spec_json, labels_json,
    created_at, updated_at
) VALUES (
    'vz-run-malformed-deadbeef0002',
    'vz-run-malformed-deadbeef0002',
    '"ready"',
    '"macos_vz"',
    '{not-valid-json',
    '{"vz.run.workspace":"/workspace"}',
    1779100900,
    1779101000
);
