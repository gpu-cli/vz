# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## gRPC UX Policy

- Prefer streaming gRPC responses for interactive or long-running operations.
- Unary request/response APIs are for short, bounded operations only.
- New control-plane surfaces should default to stream-first UX unless there is a clear reason not to.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
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
- On macOS hosts, Linux verification should run inside a local `vz`-managed Linux VM first (via `vz debug vm ...` flows), not an arbitrary external SSH host.
- External SSH Linux hosts are fallback-only when explicitly requested or when local `vz` VM execution is unavailable.
- If Linux VM end-to-end verification has not been run and passed, keep the bead/task open.
- For btrfs portability changes, run `scripts/run-linux-btrfs-e2e.sh` and attach `.artifacts/linux-btrfs-e2e/<timestamp>/summary.txt` + logs as evidence.
