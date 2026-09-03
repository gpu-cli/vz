# macOS migration, documentation, and launch

Depends on: Linux-on-macOS Docker validation; Native macOS-on-macOS validation;
release-built aggregate topology validation

## Purpose

Land vz 0.4's one topology product on macOS, migrate durable data safely, align
every public surface with verified reality, and launch only after Linux,
native-macOS, and aggregate topology gates pass.

## Step 1: Migrate CLI and configuration

- Introduce `vz up/exec/status/stop/delete`; remove legacy execution paths and
  return actionable migration errors rather than public or hidden aliases.
- Migrate legacy single-Machine records and configuration into explicit Project,
  Environment, Machine, and topology records without data loss.
- Rename public Container profile references to Hardened with a time-bounded compatibility alias.
- Distinguish any deprecated Docker translation shim from the context-selecting host Docker passthrough.

## Step 2: Update all communication surfaces

Update README, CLI help, generated docs, architecture diagrams, examples, skills,
agent guidance, site, release notes, troubleshooting, and issue descriptions.
Remove singular Environment TargetSpec, per-Environment Docker, `vz dev`, global
socket, optional-facade, Container-profile-Docker, and sandbox-as-peer-product
claims.

## Step 3: Publish the support contract

Document supported Docker Engine/API/CLI/Compose/buildx versions, macOS/architecture requirements, persistence and recovery, bind mounts, networking, security boundaries, resource behavior, status labels, and every intentional incompatibility.

Document each host/Machine-target pair separately. Linux-on-macOS,
macOS-on-macOS, and mixed Linux/macOS topology become ACTIVE only after all Mac
gates; later pairs remain DEV/PLANNED. Do not infer aggregate support from one
target gate.

## Step 4: Release and observe

Run migration/release rehearsals, upgrade/rollback tests, artifact verification, and the full release gate. Collect environment/Docker startup, failure, and recovery telemetry without exposing project data or credentials.

## Validation

- Repository-wide terminology/link/search audit.
- Clean install and upgrade from the previous supported release, including
  deterministic single-Machine-to-topology migration.
- Existing scripts receive clear compatibility behavior.
- Site claims match evidence and ACTIVE/DEV/PLANNED status.
- Beads graph has no stale duplicates or contradictory profile/socket descriptions.
- All P0/P1 0.4 work is closed with linked evidence; missing aggregate evidence
  blocks GA.
