# Architecture: Pinned Base + File-Level Patch Bundles

## Base descriptor model

Each supported base is identified by a stable descriptor.

Required fields:

- `base_id` (e.g. `macos-15.3.1-24D70-arm64-64g`)
- `macos_version`
- `macos_build`
- `ipsw_url`
- `ipsw_sha256`
- `disk_size_gb`
- `base_fingerprint`:
  - `img_sha256`
  - `aux_sha256`
  - `hwmodel_sha256`
  - `machineid_sha256`

`base_fingerprint` is the canonical match key for patch compatibility.

## Patch bundle model

Each patch bundle includes:

- `bundle_id`
- `target_base_id`
- `target_base_fingerprint`
- `patch_version`
- `operations` (ordered)
- `post_state_hashes`
- signature metadata

Operation types:

- `write_file` (path, content hash, mode)
- `delete_file`
- `mkdir`
- `symlink`
- `set_owner`
- `set_mode`

## Apply algorithm

1. Resolve and read base descriptor.
2. Compute local fingerprint for `.img/.aux/.hwmodel/.machineid`.
3. Compare with bundle target fingerprint (exact match required).
4. Mount target image.
5. Apply operations transactionally in order.
6. Validate post-state hashes.
7. Unmount and record apply receipt (`~/.vz/patch-state.json`).

If any step fails, abort and return structured error; never continue with partial success.

## Security model

- Signature verification required before apply.
- Path validation blocks traversal and absolute-path escape outside mounted root.
- Owner/mode operations are explicitly listed and audited.
- `system` operations are allowed only in policy-approved contexts.

## Why not block-level deltas

Raw image deltas are tightly coupled to exact APFS block layout and are fragile across minor changes. File-level bundles provide:

- smaller, semantically meaningful diffs
- deterministic verification
- easier debugging and auditability
