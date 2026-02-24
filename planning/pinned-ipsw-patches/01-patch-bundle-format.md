# Phase 1: Patch Bundle Format + Verifier

## Bundle layout

```
patch-<id>.vzpatch/
  manifest.json
  payload.tar.zst
  signature.sig
```

## Manifest fields

- `bundle_id`
- `patch_version`
- `target_base_id`
- `target_base_fingerprint`
- `operations_digest`
- `payload_digest`
- `post_state_hashes`
- `created_at`
- `signing_identity`

## Operation semantics

Operations are replayed in order and must be deterministic.

- `mkdir`
- `write_file`
- `delete_file`
- `symlink`
- `set_owner`
- `set_mode`

Each operation includes explicit absolute path under mounted root and required metadata.

## Verifier behavior

Preflight:

1. Verify manifest signature.
2. Verify payload digest and operation digest.
3. Verify base fingerprint exact match.

Apply:

1. Mount image.
2. Apply operations.
3. Verify `post_state_hashes`.
4. Write apply receipt.

Failure behavior:

- Abort on first failed operation.
- Return structured error with operation index and path.
- Never mark bundle as applied when verification fails.

## Idempotency

Patch apply is idempotent when:

- `write_file` is content-addressed
- owner/mode operations enforce desired state
- apply receipt records bundle ID + target base fingerprint
