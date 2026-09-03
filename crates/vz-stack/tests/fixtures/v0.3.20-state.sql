-- Immutable legacy state fixture for vz v0.3.20.
-- Source tag: v0.3.20
-- Source commit: f6c43b08db22ef5fcbb5f2fa2d2248edbc886930
-- The DDL below is copied from StateStore::init_schema at that tag. The rows
-- exercise legacy `vz run`, Spaces/Hardened, and unclassified sandbox records.

CREATE TABLE IF NOT EXISTS desired_state (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL UNIQUE,
    spec_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS observed_state (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(stack_name, service_name)
);

CREATE TABLE IF NOT EXISTS service_mount_digests (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    mount_digest TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(stack_name, service_name)
);

CREATE TABLE IF NOT EXISTS reconcile_progress (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    next_action_index INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS health_poller_state (
    stack_name TEXT NOT NULL UNIQUE,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stack_name TEXT NOT NULL,
    event_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sandbox_state (
    sandbox_id TEXT PRIMARY KEY,
    stack_name TEXT NOT NULL,
    state TEXT NOT NULL,
    backend TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    labels_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(stack_name)
);
CREATE INDEX IF NOT EXISTS idx_sandbox_stack ON sandbox_state(stack_name);

CREATE TABLE IF NOT EXISTS allocator_state (
    stack_name TEXT PRIMARY KEY,
    ports_json TEXT NOT NULL DEFAULT '{}',
    service_ips_json TEXT NOT NULL DEFAULT '{}',
    mount_tag_offsets_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS reconcile_sessions (
    session_id TEXT PRIMARY KEY,
    stack_name TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    actions_json TEXT NOT NULL,
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL DEFAULT 0,
    total_actions INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_reconcile_session_stack ON reconcile_sessions(stack_name);
CREATE INDEX IF NOT EXISTS idx_reconcile_session_status ON reconcile_sessions(status);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys(expires_at);

CREATE TABLE IF NOT EXISTS lease_state (
    lease_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    ttl_secs INTEGER NOT NULL,
    last_heartbeat_at INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lease_sandbox ON lease_state(sandbox_id);

CREATE TABLE IF NOT EXISTS execution_state (
    execution_id TEXT PRIMARY KEY,
    container_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    state TEXT NOT NULL,
    exit_code INTEGER,
    started_at INTEGER,
    ended_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_execution_container ON execution_state(container_id);

CREATE TABLE IF NOT EXISTS checkpoint_state (
    checkpoint_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    parent_checkpoint_id TEXT,
    class TEXT NOT NULL,
    state TEXT NOT NULL,
    compatibility_fingerprint TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_checkpoint_sandbox ON checkpoint_state(sandbox_id);
CREATE INDEX IF NOT EXISTS idx_checkpoint_parent ON checkpoint_state(parent_checkpoint_id);
CREATE INDEX IF NOT EXISTS idx_checkpoint_created_at ON checkpoint_state(created_at);

CREATE TABLE IF NOT EXISTS checkpoint_retention_tags (
    checkpoint_id TEXT PRIMARY KEY,
    tag TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_checkpoint_retention_tag ON checkpoint_retention_tags(tag);

CREATE TABLE IF NOT EXISTS checkpoint_file_entries (
    checkpoint_id TEXT NOT NULL,
    path TEXT NOT NULL,
    digest_sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (checkpoint_id, path)
);
CREATE INDEX IF NOT EXISTS idx_checkpoint_file_entries_checkpoint
    ON checkpoint_file_entries(checkpoint_id);

CREATE TABLE IF NOT EXISTS container_state (
    container_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    image_digest TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    ended_at INTEGER,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_container_sandbox ON container_state(sandbox_id);

CREATE TABLE IF NOT EXISTS image_state (
    image_ref TEXT PRIMARY KEY,
    resolved_digest TEXT NOT NULL,
    platform TEXT NOT NULL,
    source_registry TEXT NOT NULL,
    pulled_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS receipt_state (
    receipt_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    request_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_receipt_entity ON receipt_state(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_receipt_request ON receipt_state(request_id);
CREATE INDEX IF NOT EXISTS idx_receipt_created_at ON receipt_state(created_at);

CREATE TABLE IF NOT EXISTS build_state (
    build_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    state TEXT NOT NULL,
    result_digest TEXT,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_build_sandbox ON build_state(sandbox_id);

CREATE TABLE IF NOT EXISTS reconcile_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    stack_name TEXT NOT NULL,
    action_index INTEGER NOT NULL,
    action_kind TEXT NOT NULL,
    service_name TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_session ON reconcile_audit_log(session_id);
CREATE INDEX IF NOT EXISTS idx_audit_stack ON reconcile_audit_log(stack_name);

CREATE TABLE IF NOT EXISTS control_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO control_metadata (key, value) VALUES
    ('schema_version', '1'),
    ('created_at', '1779100000');

-- Positively classified legacy `vz run` record. v0.3.20 persisted only guest
-- mount targets and the workspace path, not the source mount or disk path.
INSERT INTO sandbox_state (
    sandbox_id, stack_name, state, backend, spec_json, labels_json,
    created_at, updated_at
) VALUES (
    'vz-run-shop-a1b2c3d4e5f6',
    'vz-run-shop-a1b2c3d4e5f6',
    '"ready"',
    '"macos_vz"',
    '{"cpus":4,"memory_mb":4096,"base_image_ref":"ubuntu:24.04","main_container":"workspace-main","network_profile":null,"volume_mounts":[]}',
    '{"vz.run.mount.vz-mount-0":"/workspace","vz.run.workspace":"/workspace","vz.sandbox.base_image_ref":"ubuntu:24.04","vz.sandbox.main_container":"workspace-main"}',
    1779100100,
    1779100200
);

INSERT INTO container_state (
    container_id, sandbox_id, image_digest, spec_json, state,
    created_at, started_at, ended_at, updated_at
) VALUES (
    'ctr-legacy-workspace',
    'vz-run-shop-a1b2c3d4e5f6',
    'sha256:legacy-workspace',
    '{"cmd":["sleep","infinity"],"env":{},"cwd":"/workspace","user":null,"mounts":[],"resources":{"cpus":null,"memory_mb":null},"network_attachments":[]}',
    '"running"',
    1779100101,
    1779100102,
    NULL,
    1779100200
);

INSERT INTO checkpoint_state (
    checkpoint_id, sandbox_id, parent_checkpoint_id, class, state,
    compatibility_fingerprint, created_at, updated_at
) VALUES (
    'ckpt-legacy-workspace',
    'vz-run-shop-a1b2c3d4e5f6',
    NULL,
    'fs_quick',
    'ready',
    'legacy-kernel-arm64',
    1779100150,
    1779100200
);

INSERT INTO events (stack_name, event_json, created_at) VALUES (
    'vz-run-shop-a1b2c3d4e5f6',
    '{"type":"sandbox_ready","stack_name":"vz-run-shop-a1b2c3d4e5f6","sandbox_id":"vz-run-shop-a1b2c3d4e5f6"}',
    '2026-05-18 22:01:40'
);

INSERT INTO receipt_state (
    receipt_id, operation, entity_id, entity_type, request_id, status,
    created_at, metadata_json
) VALUES (
    'rcp-legacy-create',
    'create_sandbox',
    'vz-run-shop-a1b2c3d4e5f6',
    'sandbox',
    'req-legacy-create',
    'success',
    1779100100,
    '{}'
);

-- Spaces/Hardened record. It must remain a Sandbox and must not become a
-- Developer Machine.
INSERT INTO sandbox_state (
    sandbox_id, stack_name, state, backend, spec_json, labels_json,
    created_at, updated_at
) VALUES (
    'sbx-hardened-001',
    'hardened-space',
    '"ready"',
    '"macos_vz"',
    '{"cpus":2,"memory_mb":2048,"base_image_ref":"debian:bookworm","main_container":"workspace-main","network_profile":null,"volume_mounts":[]}',
    '{"project_dir":"/legacy/path/that-must-not-be-identity","vz.space.mode":"required","vz.space.lifecycle":"persistent","vz.space.worktree.id":"legacy-worktree"}',
    1779100300,
    1779100400
);

-- Generic low-level Sandbox with no Developer marker. It also stays legacy.
INSERT INTO sandbox_state (
    sandbox_id, stack_name, state, backend, spec_json, labels_json,
    created_at, updated_at
) VALUES (
    'sbx-generic-001',
    'generic-stack',
    '"terminated"',
    '"macos_vz"',
    '{"cpus":1,"memory_mb":1024,"base_image_ref":"alpine:3.20","main_container":null,"network_profile":null,"volume_mounts":[]}',
    '{}',
    1779100500,
    1779100600
);
