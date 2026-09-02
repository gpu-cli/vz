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
- Completion requires real verification in a Linux VM with end-to-end testing of the implemented behavior.
- On macOS hosts, Linux verification should run inside a local `vz`-managed Linux VM first (via the `vz vm ...` flows), not an arbitrary external SSH host.
- Do not use ad-hoc external SSH Linux hosts for release-gate evidence collection.
- If Linux VM end-to-end verification has not been run and passed, keep the bead/task open.
- For container/stack runtime work, run `scripts/run-sandbox-vm-e2e.sh --suite all` and attach the artifact logs as evidence.
- For Docker-in-guest work (beads `vz-5in`/`vz-yr9`/`vz-k3v`/`vz-7ez`), evidence must show youki as the only OCI runtime binary present in the guest; ship a dedicated `scripts/run-linux-docker-e2e.sh` lane when the wave lands.
- For btrfs portability changes, run `scripts/run-linux-btrfs-e2e.sh` and attach `.artifacts/linux-btrfs-e2e/<timestamp>/summary.txt` + logs as evidence.
