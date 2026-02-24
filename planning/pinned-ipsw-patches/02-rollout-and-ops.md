# Phase 2+: Release, Rollout, and Operations

## Release pipeline

For each new pinned base:

1. CI downloads pinned IPSW and builds base image.
2. CI computes and records base fingerprint.
3. CI applies provisioning (system and user variants).
4. CI generates patch bundles and signatures.
5. CI publishes artifacts and updates matrix metadata.

## User-facing channels

- `stable`: latest validated pinned base + patches
- `previous`: last stable pinned base (short overlap window)

## Update cadence

- Refresh on Apple macOS security/point releases.
- Keep at least one previous pinned base active for rollback.
- Mark retired bases explicitly with end-of-support date.

## Runtime policy

- Default runtime policy remains `system` mode for reliability.
- `user` mode is opt-in for local rootless development.

## Recommended UX flows

### No local sudo path

1. `vz vm init --base stable`
2. consume CI-published pre-provisioned artifact or compatible signed bundle
3. `vz vm run ...`

### Local privileged path

1. `vz vm init --base stable`
2. `sudo vz vm provision --agent-mode system ...`
3. `vz vm run ...`

## Failure and fallback

- Base mismatch: stop and print expected vs actual fingerprint.
- Unsupported base: suggest `vz vm init --base stable`.
- Patch verify failure: no partial apply state; require re-run.

## Tests

- Verify matrix pinning behavior for known/unknown base.
- Verify patch preflight mismatch errors.
- Verify full apply on matching base.
- Verify startup behavior for both `system` and `user` modes.
