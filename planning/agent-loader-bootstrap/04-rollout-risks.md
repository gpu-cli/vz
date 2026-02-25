# 04: Rollout, Validation, and Risks

## Rollout waves

1. Wave 0 (internal):
   - ship loader and artifact verifier behind feature flag
   - keep legacy direct-agent bootstrap path
2. Wave 1 (opt-in):
   - expose `bootstrap-agent` and `agent install`
   - document as preferred for fast agent iteration
3. Wave 2 (default):
   - default provisioning path installs loader
   - legacy direct binary patch remains fallback
4. Wave 3 (cleanup):
   - reduce direct-agent-in-image updates to exceptional cases only

## Validation matrix

1. Fresh base image bootstrap and first boot.
2. Online update while VM running.
3. Offline update on stopped image.
4. Corrupted artifact (signature fail).
5. Downgrade reject + explicit rollback allow path.
6. Startup recovery from broken `current` symlink.

## Risks

1. Loader bug can block agent startup across fleet.
2. Key rotation mistakes can brick update path.
3. Version-state corruption can cause bad rollback behavior.
4. Divergence between offline and online install paths.

## Mitigations

1. Keep loader minimal and heavily tested.
2. Support multiple trusted keys and overlap rotation windows.
3. Write `state.json` atomically with checksum.
4. Reuse a single install engine for offline and online modes.

## Open questions

1. Should `bootstrap-agent` always seed an initial agent artifact?
2. Should channel selection live in loader config or artifact manifest only?
3. Do we require healthcheck ack before finalizing `current` switch?
4. How strict should anti-rollback be for local dev workflows?
