# 01: Stage-0 Loader Contract

## Responsibilities

`vz-agent-loader` is a minimal bootstrap executable with a stable contract:

1. Discover the active guest-agent version.
2. Verify local install metadata before execution.
3. Execute the selected agent binary with expected args/env.
4. Emit clear diagnostics and fallback behavior when no valid agent is available.

The loader should avoid feature creep. Business logic belongs in the main guest agent.

## Proposed guest filesystem layout

- Loader binary: `/usr/local/libexec/vz-agent-loader`
- Launchd plist target:
  - system mode: `/Library/LaunchDaemons/com.vz.agent.loader.plist`
  - user mode: `/Library/LaunchAgents/com.vz.agent.loader.plist` or per-user location
- Agent store root: `/var/lib/vz/agent`
- Versioned installs: `/var/lib/vz/agent/versions/<version>/vz-guest-agent`
- Active pointer: `/var/lib/vz/agent/current` (symlink to `versions/<version>`)
- Update staging: `/var/lib/vz/agent/staging/<txn-id>`
- State file: `/var/lib/vz/agent/state.json`

## Loader startup sequence

1. Read `state.json` and resolve `current` symlink.
2. Validate agent binary exists and matches recorded digest.
3. `execve()` into the resolved agent binary.
4. If validation fails:
   - fallback to previous known-good version if available,
   - otherwise exit with explicit error code and structured log.

## Failure behavior

- Never run an unverified binary.
- Never mutate installed versions during boot path.
- Keep startup deterministic: success path is `resolve -> verify -> exec`.

## Compatibility contract

The loader and agent communicate through CLI/env contract, not private ABI.

Required env examples:

- `VZ_AGENT_HOME=/var/lib/vz/agent`
- `VZ_AGENT_CHANNEL=<channel>`
- `VZ_AGENT_LOADER_VERSION=<semver>`

## Implementation constraints

1. Keep loader dependency surface minimal.
2. Keep binary size small enough that bootstrap patch churn is rare.
3. Add integration test that simulates broken `current` symlink and validates fallback.
