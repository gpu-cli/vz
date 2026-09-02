# macOS migration, documentation, and launch

Depends on: Linux-on-macOS Docker validation; Native macOS-on-macOS validation

## Purpose

Remove the dual-product story on macOS without breaking existing automation, ensure every public surface reflects verified reality, and launch only after both Linux-on-macOS and macOS-on-macOS gates pass.

## Step 1: Migrate CLI and configuration

- Introduce canonical `vz dev` workflows with aliases and actionable deprecation notices for old top-level flows.
- Migrate legacy environment records, project disks, labels, and configuration without data loss.
- Rename public Container profile references to Hardened with a time-bounded compatibility alias.
- Distinguish any deprecated Docker translation shim from the context-selecting host Docker passthrough.

## Step 2: Update all communication surfaces

Update README, CLI help, generated docs, architecture diagrams, examples, skills, AGENTS guidance, site, release notes, troubleshooting, and issue descriptions. Remove global socket, optional-facade, Container-profile-Docker, and sandbox-as-peer-product claims.

## Step 3: Publish the support contract

Document supported Docker Engine/API/CLI/Compose/buildx versions, macOS/architecture requirements, persistence and recovery, bind mounts, networking, security boundaries, resource behavior, status labels, and every intentional incompatibility.

Document each host/target pair separately: Linux-on-macOS and macOS-on-macOS ACTIVE after their gates, Linux-on-Linux NEXT, Linux-on-Windows PLANNED, and Windows-on-Windows PLANNED. Do not imply that target-specific capabilities are universal.

## Step 4: Release and observe

Run migration/release rehearsals, upgrade/rollback tests, artifact verification, and the full release gate. Collect environment/Docker startup, failure, and recovery telemetry without exposing project data or credentials.

## Validation

- Repository-wide terminology/link/search audit.
- Clean install and upgrade from the previous supported release.
- Existing scripts receive clear compatibility behavior.
- Site claims match evidence and ACTIVE/DEV/PLANNED status.
- Beads graph has no stale duplicates or contradictory profile/socket descriptions.
