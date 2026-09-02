# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd dolt push          # Push issue DB to the beads remote
```

**There is no `bd sync` command in this bd build** — `bd dolt push` is the sync path for issue data; git handles the code.

## Product Mission

- The primary product object is a reproducible, parallel **Developer Environment**. Sandboxes, containers, VMs, and processes are implementation mechanisms or advanced capabilities.
- Host OS and target OS are separate dimensions. Linux is the universal target: Linux-on-macOS now, Linux-on-Linux next, and Linux-on-Windows later. Native macOS-on-macOS is also current; native Windows-on-Windows follows Linux-on-Windows.
- Docker is implicit, private, and scoped to each Linux-target Developer Environment. There is no global vz Docker socket or fallback daemon. Native macOS and native Windows targets do not implicitly provide Docker.
- Linux-target OCI execution is youki-only. The public locked-down profile is **Hardened** and remains a secondary, restricted mode.
- Capability and release claims are qualified by host×target pair and must distinguish ACTIVE, DEV, and PLANNED behavior.

## Remote-backed DB coordination (READ BEFORE ANY bd WRITE)

This beads DB is remote-backed (Dolt). When bd refuses writes with a schema-migration warning:

- bd will NOT auto-apply pending migrations when the DB is remote-backed (migrating clones independently forks the schema, bd#4259).
- **Only one clone may migrate at a time.** If another clone already migrated, run `bd bootstrap` instead.
- To become the single designated migrator: `BD_ALLOW_REMOTE_MIGRATE=1 bd migrate && bd dolt push`
- After the migrator pushes, every other clone runs `bd bootstrap`.
- Keep `.beads/` at permissions `0700` (bd warns at `0750`).

## gRPC UX Policy

- Prefer streaming gRPC responses for interactive or long-running operations.
- Unary request/response APIs are for short, bounded operations only.
- New control-plane surfaces should default to stream-first UX unless there is a clear reason not to.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items; push the issue DB with `bd dolt push`
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Verification Standard for Beads/Tasks

- Do NOT consider any bead/task complete based only on unit tests.
- Completion requires real end-to-end verification on the relevant host×target backend.
- On macOS hosts, Linux-target verification should run inside a local `vz`-managed Linux VM first (via the `vz vm ...` flows), not an arbitrary external SSH host.
- Do not use ad-hoc external SSH Linux hosts for release-gate evidence collection.
- If the applicable host×target end-to-end gate has not run and passed, keep the bead/task open.
- For container/stack runtime work, run `scripts/run-sandbox-vm-e2e.sh --suite all` and attach the artifact logs as evidence.
- For Linux-target Docker work (including beads `vz-5in`/`vz-yr9`/`vz-k3v`/`vz-7ez`), evidence must show youki as the only OCI runtime binary present in the target; ship and run `scripts/run-linux-docker-e2e.sh --suite all` from the host when the wave lands.
- Native macOS work requires the native macOS-on-macOS gate; Docker evidence from a neighboring Linux environment does not certify the macOS target.
- Future Linux-on-Linux, Linux-on-Windows, and Windows-on-Windows claims require their own release-built host×target conformance evidence.
- For btrfs portability changes, run `scripts/run-linux-btrfs-e2e.sh` and attach `.artifacts/linux-btrfs-e2e/<timestamp>/summary.txt` + logs as evidence.
