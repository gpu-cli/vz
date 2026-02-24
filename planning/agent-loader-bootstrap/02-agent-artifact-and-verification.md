# 02: Agent Artifact and Verification Model

## Artifact structure

Each agent release artifact contains:

1. `manifest.json`
2. `vz-guest-agent` binary
3. detached signature (`manifest.sig`)

Optional packaging: `tar.zst` with the three files.

## Manifest schema (v1)

Required fields:

- `schema_version`
- `agent_version` (semver)
- `channel` (`stable`, `canary`, etc.)
- `target_os` (`darwin`)
- `target_arch` (`arm64`)
- `binary_sha256`
- `binary_size`
- `created_at`
- `min_loader_version`
- `signing_key_id`

## Trust model

- Loader trusts a pinned set of public keys shipped in bootstrap.
- Manifest signature must validate against trusted key set.
- Binary digest and size must match manifest.
- Any verification failure aborts install.

## Anti-rollback policy

Default rule:

- Reject install if `agent_version` is lower than `state.json` highest known-good for channel.

Override path:

- Explicit `--allow-downgrade` flag for controlled rollback.

## Atomic install algorithm

1. Verify artifact signature and digest in temp staging dir.
2. Write binary to `staging/<txn-id>/vz-guest-agent`.
3. Set mode/owner.
4. Move staging dir to `versions/<version>` (atomic rename).
5. Swap `current` symlink atomically.
6. Update `state.json` with `current`, `previous`, and timestamps.

## Recovery model

If install fails before symlink swap, existing `current` remains untouched.

If post-swap startup healthcheck fails, rollback command points `current` to `previous`.
