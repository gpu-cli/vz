# Pinned IPSW + Patch Distribution Plan

## Why this plan exists

We need a path that preserves reliable guest-agent startup while improving open-source and agent UX.

- `system` provisioning mode is the most reliable (pre-login launch semantics), but local apply needs root ownership semantics.
- `user` provisioning mode can run without root, but startup depends on login/session state and is less deterministic for unattended automation.
- Raw `.img` binary deltas are brittle; file-level provisioning deltas are more stable and auditable.

This plan keeps reliability first while reducing local privilege requirements for users.

## Goals

1. Keep `system` mode as default for reliability.
2. Make no-sudo user workflows possible by moving privileged work to release/CI artifacts.
3. Pin all provisioning/patch artifacts to exact base image identity.
4. Fail closed on base/patch mismatch.

## Non-goals

- Supporting arbitrary unpinned base images in production automation.
- Best-effort patch application on unknown macOS builds.
- Block-level image diff/patch as the primary distribution format.

## Key decisions

- Default mode remains `system`.
- `user` mode remains available for local rootless workflows.
- Distribution uses file-level patch bundles, not raw block deltas.
- All bundles are pinned to a base matrix entry (IPSW + base fingerprint).

## Phase plan

### Phase 0: Pin supported base matrix

- Add a versioned `config/base-images.json` with explicit base descriptors.
- Each descriptor includes macOS build, IPSW URL/hash, and derived base fingerprints.
- Add CLI commands to list and verify supported bases.

### Phase 1: Define patch bundle format and verifier

- Add signed bundle format for file-level operations and metadata.
- Add strict preflight verification against base fingerprint.
- Add post-apply verification and idempotency checks.

### Phase 2: CI release pipeline

- Build pinned bases in CI.
- Apply provisioning in CI (privileged environment).
- Publish either pre-provisioned images or patch bundles + signatures.

### Phase 3: Client workflows

- `vz vm init --base <id>` creates a known base.
- `vz vm patch apply --bundle <...>` applies pinned patch if base matches.
- For default users: prefer downloaded pre-provisioned artifacts to avoid local sudo.

### Phase 4: Rollout and deprecation policy

- Keep N recent base entries active (for example, latest 2).
- Retire older base/patch pairs with explicit compatibility errors.

## Dependency graph

```
Phase 0 (base matrix)
   -> Phase 1 (patch format/verifier)
   -> Phase 2 (CI publishing)
   -> Phase 3 (client apply UX)
   -> Phase 4 (rollout policy)
```

## Acceptance criteria

- A patch bundle only applies when base fingerprint matches exactly.
- Applying a valid bundle is deterministic and idempotent.
- Mismatch errors are explicit and actionable.
- Default install path can be run without local sudo by consuming CI-produced artifacts.
- `system` mode remains the default runtime policy.
