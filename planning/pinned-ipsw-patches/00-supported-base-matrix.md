# Phase 0: Supported Base Matrix

## Deliverable

Introduce a versioned matrix at `config/base-images.json`.

Example schema:

```json
{
  "version": 1,
  "default_base": "macos-15.3.1-24D70-arm64-64g",
  "bases": [
    {
      "base_id": "macos-15.3.1-24D70-arm64-64g",
      "macos_version": "15.3.1",
      "macos_build": "24D70",
      "ipsw_url": "https://updates.cdn-apple.com/.../UniversalMac_15.3.1_24D70_Restore.ipsw",
      "ipsw_sha256": "<sha256>",
      "disk_size_gb": 64,
      "fingerprint": {
        "img_sha256": "<sha256>",
        "aux_sha256": "<sha256>",
        "hwmodel_sha256": "<sha256>",
        "machineid_sha256": "<sha256>"
      },
      "supported_patches": ["patch-system-v1", "patch-user-v1"]
    }
  ]
}
```

## CLI additions

- `vz vm base list`
- `vz vm base verify --image <path> --base-id <id>`
- `vz vm init --base <id>`
- Optional escape hatch: `--allow-unpinned` (explicit warning)

## Rules

- Any automated patch apply requires a base matrix match.
- `--allow-unpinned` is disabled in CI unless explicitly enabled.
- Base IDs are immutable once published.

## Validation

- Unit test: matrix schema parse and validation.
- Integration test: verify command fails on hash mismatch.
- Integration test: verify command passes on exact match.
