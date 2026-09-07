---
name: vz
description: Run commands in a vz Developer Environment through the five-verb CLI (up, exec, status, stop, delete). Use when the user requests vz Linux or native macOS Machine execution or the task needs the project's vz.json Environment; the presence of vz.json alone does not require moving host tools into a Machine.
---

# Execute work in vz

Choose execution based on the requested target and project configuration. The
nearest checked-in `vz.json` is the ProjectDefinition; inspect it (and parent
directories) when present. Keep host operations such as git or native host
checks on the host when appropriate.

## Command surface

The public CLI has exactly five lifecycle verbs. Check `vz --help` and each
subcommand's help for the installed binary; a published release can differ from
this checkout. Bare `vz` prints static help and touches no state.

```bash
vz up [--environment <name-or-id>]                       # create/reconcile the selected Environment
vz status [--environment <name-or-id> | --all] [--machine <name-or-id>] --json
vz exec [--environment <name-or-id>] [--machine <name-or-id>] -- <command> [args...]
vz stop [--environment <name-or-id>]                     # preserves identity and declared state
vz delete [--environment <name-or-id>]                   # removes only the selected ownership graph
```

Selection precedence is an explicit `--environment` name/ID, then the
process-scoped `VZ_ENVIRONMENT_ID`, then the sole Environment bound to this
worktree. Ambiguity fails closed and lists candidates; there is no mutable
global current Environment. Within the Environment, `--machine` or
`VZ_MACHINE_ID` selects a Machine; otherwise the declared default or sole
Machine is used. `vz up` creates `default` only when the project has no
Environment; `vz up --environment <name>` creates or reconciles that named
instance and binds the current worktree to it.

`vz exec` passes the executable and arguments after `--` directly, without an
implicit shell; wrap shell syntax in `sh -c '...'`. `--json` is a global flag
for scripted status and error output. `-t`/`--tty` requests an interactive
terminal and is negotiated only by native macOS Machines today; Linux Machines
run `exec` without a PTY. Host `docker`, `docker compose`, and `docker buildx`
must select the exact Developer Linux Machine context reported by `vz status`;
never assume a global socket or Docker Desktop fallback.

Retired command families (`run`, `init`, `logs`, `create`, `ls`, `rm`,
`inspect`, `attach`, `stack`, `image`, `checkpoint`, `vm`, `debug`, and the rest
of `config/cli-removal-v0.4.json`) exit 2 with a `legacy_command_removed` JSON
error before touching state. Do not fall back to them, invent an init command,
or point `VZ_BIN` at an older binary. Author `vz.json` from
`schemas/vz-project-definition-v1.schema.json` or
`examples/developer-environment/vz.json`; definition authoring never creates a
Machine.

## Implementation status

Labels follow `config/host-target-capabilities-v0.4.json`. Linux and native
macOS Developer Machines on Apple-silicon macOS are
<!-- capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair -->**DEV**: all five verbs work against installed local-Mac builds,
but no 0.4 release is published and nothing is release certified. Private
Docker, Compose and buildx for Linux Developer Machines are
<!-- capability-matrix: macos-arm64/linux/developer docker_engine,compose,buildx -->**DEV**. Linux Machine PTY, declared
networks, endpoints and workspace projections are
<!-- capability-matrix: macos-arm64/linux/* posix_pty,network_private,endpoint,workspace_read_write -->**PLANNED**;
Up rejects definitions that declare them. Linux and Windows hosts are
<!-- capability-matrix: linux-*/linux/*,windows-*/linux/* pair -->**PLANNED**. Treat installed
evidence as a development slice, not certification, and record which
host×Machine-target pair a result covers.

## Recovery and state

Commands need an already-running runtime daemon (`vz-runtimed`) and a valid
nearest `vz.json`; they do not autostart a daemon. Diagnose failures from the
structured JSON error on stderr, `vz status --json`, the definition, and daemon
diagnostics. A timeout does not by itself prove a stale lock, and warnings are
not universally harmless. Fix the identified cause and verify the requested
command's result.

`vz stop` stops the Environment's Machines while preserving identities and
declared state; re-running `vz up` reconciles the existing Environment rather
than resetting it. `vz delete` removes the selected Environment's Machines,
managed Docker contexts and private stores. Use deletion only when discarding
that state is within the user's authorized scope; otherwise describe the impact
and ask before doing it.

Report the command's outcome, the selected Environment/Machine, and the
host×Machine-target pair. Follow the repository's backend verification rules
(`docs/agent-verification.md`) for backend/release work; Linux execution alone
does not certify a native macOS target.
