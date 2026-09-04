---
name: vz
description: Run commands in a vz-managed Linux development environment. Use when the user requests vz/Linux execution or the task needs the project's configured vz environment; the presence of vz.json alone does not require moving host tools into Linux.
---

# Execute work in vz

Choose execution based on the requested target and project configuration.
Inspect `vz.json` in the project or parents when present. Keep host operations
such as git or native macOS checks on the host when appropriate.

## Command surface

Check `vz --help` and relevant subcommand help for the installed version. The
repository currently implements the Linux-on-macOS `vz run` surface; the planned
0.4 `up/exec/status/stop/delete` lifecycle must not be assumed available.

For installations supporting `vz run`:

```bash
vz run cargo build       # execute a Linux command with project configuration
vz run -i bash           # interactive Linux shell
vz status                # inspect current project/VM state
vz logs                  # inspect daemon logs
```

Use `vz init` when project initialization is part of the request. Inspect an
existing configuration before changing it. Environment and toolchain setup
belongs in the configuration's `setup` array; one-off builds and tests belong in
runtime commands. Check actual setup/runtime behavior for the installed version.

## Recovery and state

Diagnose failures from command output, logs, configuration, and process state.
A timeout does not by itself prove a stale lock, and warnings are not universally
harmless. Fix the identified cause and verify the requested command's result.

`vz stop` interrupts the current Machine. `vz run --fresh` removes its persistent
disk and re-provisions it; it is not a routine diagnostic or package-install step.
Use recreation only when discarding that state is within the user's authorized
scope; otherwise describe the impact and ask before doing it.

Report the command's outcome and relevant environment details. Follow the
repository's host×Machine-target verification rules for backend/release work;
Linux execution alone does not certify a native macOS target.
