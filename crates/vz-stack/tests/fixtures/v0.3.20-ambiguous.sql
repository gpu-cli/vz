-- Apply after v0.3.20-state.sql. This record has both legacy Developer and
-- Spaces/Hardened markers; migration must reject it before any mutation.
INSERT INTO sandbox_state (
    sandbox_id, stack_name, state, backend, spec_json, labels_json,
    created_at, updated_at
) VALUES (
    'vz-run-ambiguous-deadbeef0001',
    'vz-run-ambiguous-deadbeef0001',
    '"ready"',
    '"macos_vz"',
    '{"cpus":2,"memory_mb":2048,"base_image_ref":"ubuntu:24.04","main_container":"workspace-main","network_profile":null,"volume_mounts":[]}',
    '{"project_dir":"/legacy/ambiguous","vz.run.workspace":"/workspace","vz.space.mode":"required"}',
    1779100700,
    1779100800
);
