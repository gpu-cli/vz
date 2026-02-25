# Agent Loader Bootstrap Plan

## Why this plan exists

Current VM bootstrap bundles include the full `vz-guest-agent` binary. That forces frequent patch/delta regeneration whenever the agent changes.

We want a stable bootstrap layer that changes rarely, and a small swappable agent payload that can be updated independently.

## Goals

1. Install a tiny stage-0 loader once per base image line.
2. Decouple guest-agent updates from image delta distribution.
3. Make agent updates small, signed, atomic, and rollback-safe.
4. Preserve unattended startup reliability (`launchd` + pre-login behavior).
5. Keep `vz vm` UX simple for default users.

## Non-goals

1. Replace all patch infrastructure immediately.
2. Support unsigned or best-effort agent updates.
3. Ship a background privileged host daemon in v1.

## Design summary

- Bootstrap image contains:
  - `vz-agent-loader` (small static-ish binary, stable interface)
  - launchd plist pointing to loader path, not direct guest-agent path
  - trust root material for artifact signature verification
- Loader resolves and executes the current agent from a versioned store.
- New agent versions are delivered as signed artifacts and installed atomically.
- No new `.img` delta is required for normal guest-agent releases.

## Document map

- `01-stage0-loader.md` — loader contract, file layout, startup lifecycle.
- `02-agent-artifact-and-verification.md` — artifact format, signature and rollback rules.
- `03-update-and-cli-ux.md` — one-command UX and command surface.
- `04-rollout-risks.md` — rollout waves, validation, risks, and open questions.

## Phase dependency graph

```
Phase 1: stage-0 loader contract + filesystem layout
   -> Phase 2: signed agent artifact format + verifier
   -> Phase 3: update/install commands (offline + online)
   -> Phase 4: rollout and deprecate frequent image-delta agent updates
```

## Acceptance criteria

1. New guest-agent release does not require a new image delta in normal path.
2. Loader starts agent at boot/login according to policy without host-side manual steps.
3. Agent update is atomic (`current` pointer swap) and rollback-capable.
4. Invalid signatures or hash mismatches fail closed.
5. CLI exposes one primary bootstrap command and one primary update command.
