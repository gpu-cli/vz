# Working on vz

This is the shared repository guidance for coding agents. `CLAUDE.md` imports it.
Keep durable project constraints here and task-specific procedures in skills or
linked docs. Prefer decision criteria over fixed scripts for ordinary work.

## Product contract

vz provides reproducible, local Developer Environment topologies. Read
[the product contract](docs/developer-environments.md) when changing architecture
or public behavior; [the 0.4 gate](planning/developer-environments/GOAL-0.4.0.md)
defines release completion.

- A project can have multiple named Environments. A worktree selects/binds an
  Environment; it is not its identity. Each Environment is an isolation and
  lifecycle boundary containing one or more target-native Machines.
- Host OS and Machine target OS are separate. Linux and native macOS Machines
  can coexist in an Environment. Linux is the universal target: Linux-on-macOS
  now, Linux-on-Linux next, Linux-on-Windows later, then native Windows-on-Windows.
- Docker is implicit and private to each Developer-profile Linux Machine, with
  no Environment-wide/global socket or fallback daemon. Native macOS/Windows
  Machines do not implicitly provide Docker. Linux OCI execution is youki-only.
  Hardened is the secondary, restricted public profile.
- Machine paths are declared private or Environment-local public-like paths.
  Separate Environments are default-deny; cross-Environment access requires an
  explicit directional service grant.
- Host imports require exact authenticated Environment/Machine grants to a
  declared host-loopback service, independently of external egress. NAT aliases
  and wildcard/LAN listeners are not authorization.
- The 0.4 public lifecycle is `vz up`, `vz exec`, `vz status`, `vz stop`, and
  `vz delete`; richer topology operations belong to typed APIs. Check actual
  CLI help/source before assuming planned commands have shipped.
- Qualify capability claims by host×Machine-target pair and distinguish ACTIVE,
  DEV, and PLANNED behavior. Backend optimizations must preserve Machine identity
  and state ownership.
- Prefer streaming gRPC for interactive or long-running operations; unary RPCs
  suit short, bounded work.

## Development

The Rust workspace is `crates/`; its `Cargo.toml` defines members, toolchain
requirements, and shared lints. Follow the affected crate's existing patterns.
Use `tracing` for diagnostics, typed serialization for stable contracts, and
contextual errors for fallible operations. Keep public Rust APIs safe and
documented; contain necessary FFI/unsafe at implementation boundaries with safety
justifications. Preserve target-specific compilation gates.

Typical checks, run from `crates/`:

```bash
cargo build -p <crate>
cargo fmt --check -p <crate>
cargo clippy -p <crate> -- -D warnings
cargo nextest run -p <crate>
```

Choose checks for the changed behavior and affected consumers. Broaden to
workspace checks for shared contracts, dependencies, or integration risk.
Investigate failures before labeling them pre-existing; an unchanged consumer
can expose a regression. Documentation-only work needs reference/command checks,
not a runtime rebuild. For runtime/backend changes, unit tests alone cannot close
the task: follow [backend verification requirements](docs/agent-verification.md).

For macOS-hosted Linux kernel builds, use the Docker builder described in
[linux/README.md](linux/README.md); build inputs and cleanup targets live in
`linux/Makefile`. Avoid hardcoded kernel versions or routine source deletion.

## Issue tracking and delivery

Use **bd (Beads)** for tracked work. `bd onboard` provides setup guidance;
command-specific `--help` describes this installed build.

```bash
bd ready
bd show <id>
bd update <id> --status in_progress
bd close <id> --reason "Changes and verification evidence"
bd dolt push
```

`bd dolt push` syncs issue data; git syncs code. There is no `bd sync` command.
Record meaningful progress, evidence, blockers, and concrete follow-up work.
Close issues only when their acceptance criteria and applicable gates pass.

The DB is remote-backed by Dolt. On a schema-migration warning, coordinate one
designated migrator; independently migrating clones forks the schema. If another
clone already migrated, run `bd bootstrap`. Only the designated migrator runs
`BD_ALLOW_REMOTE_MIGRATE=1 bd migrate && bd dolt push`; other clones then bootstrap.
Keep `.beads/` permissions at `0700`.

Runtime/backend changes must pass the relevant installed user-level end-to-end
flow before landing on main. Unit tests, synthetic image exercises, and backend
helpers alone do not authorize a main merge. Keep fixes on an isolated branch
until that evidence exists, and record any unvalidated paths explicitly.

For implementation sessions, review and commit the task's changes, integrate
remote changes (normally `git pull --rebase`), push issue data and code, and verify
the branch is up to date with origin. Preserve unrelated edits and stashes; use an
isolated checkout if needed. Resolve ordinary push conflicts, but report access
or infrastructure blockers instead of retrying indefinitely or claiming success.
The handoff should state the result, checks, and any remaining work.

## Task-specific skills

Load a skill only when its workflow helps the requested task:

- [implement-bd](.claude/skills/implement-bd/SKILL.md): implement tracked issues.
- [plan-to-beads](.claude/skills/plan-to-beads/SKILL.md): convert plans to issues.
- [tmux-cli-test](.claude/skills/tmux-cli-test/SKILL.md): test terminal interactions.
- [tui-review](.claude/skills/tui-review/SKILL.md): review terminal UX.
- [vz](skills/vz/SKILL.md): execute work inside a vz Linux environment.
