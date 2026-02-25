# 03: Update Flow and CLI UX

## Primary commands

### One-time bootstrap (per base line)

`vz vm bootstrap-agent --image <base.img> --loader <loader-bin> --trust-key <pubkey>`

What it does:

1. Installs loader + launchd plist.
2. Seeds trust roots and empty agent store layout.
3. Optionally seeds an initial agent version.

### Routine agent update (no image delta)

`vz vm agent install --artifact <agent.tar.zst> [--image <img> | --name <running-vm>]`

What it does:

1. Verifies signature and manifest.
2. Installs version atomically.
3. Flips `current` pointer.
4. Optionally restarts loader/agent service.

## Update modes

1. Offline image mode:
   - mounts image root
   - updates `/var/lib/vz/agent/*`
   - used for baking new base variants
2. Online VM mode:
   - sends artifact to running VM via existing control channel
   - installs in guest without rebuilding image

## Desired UX simplification

User-facing default should be:

1. Bootstrap once for base image family.
2. Ship frequent small agent artifacts.
3. Run a single install command for updates.

No manual bundle JSON, payload dirs, or frequent image deltas for agent-only changes.

## Observability

Add `vz vm agent status`:

- current version
- previous version
- last update time
- channel
- loader version

Add structured logs/events for install, verify, rollback.
