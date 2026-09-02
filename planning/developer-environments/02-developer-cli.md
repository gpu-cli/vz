# Converged Developer Environment CLI

Depends on: First-class Developer Environment identity and lifecycle

## Purpose

Make the normal workflow project-first and obvious while keeping automation stable.

## Step 1: Introduce the canonical namespace

Provide `vz dev up --target <linux|macos|windows>`, `run`, `shell`, `exec`, `status`, `logs`, `stop`, `restart`, `delete`, and `list`. Target is immutable after creation. Current-project resolution uses the nearest config plus canonical project/worktree identity; explicit name/ID overrides are supported.

## Step 2: Converge legacy entry points

- Route top-level `vz init/run/stop/status/logs` through the same Developer Environment service as aliases during a documented deprecation window.
- Reconcile the separate `vz create`/space flow without maintaining a second lifecycle implementation.
- Outside a project or when selection is ambiguous, fail with a list of candidates rather than guessing.

## Step 3: Make Docker selection implicit

- Linux-target `up` and first `run` create/repair the managed per-environment Docker context automatically.
- `vz dev docker -- ...` invokes the host's installed Docker CLI with that context; it does not translate Engine API calls.
- `context` prints the stable context name.
- `env` emits shell-safe session variables, preferring `DOCKER_CONTEXT` over raw `DOCKER_HOST`.
- Never change Docker's global default context.
- Docker/context/env commands on targets without `capabilities.docker` fail with an actionable structured error.

## Step 4: Provide machine-readable status

`vz dev status --json` includes environment identity/class/state, host/target/architecture, backend diagnostics, target image/build, negotiated capabilities, native runtime state, persistent storage, ports, and last actionable failure. Docker fields appear only when negotiated.

## Validation

- CLI parsing/snapshot tests and stable JSON schema tests.
- Current-project, worktree, explicit-name, ambiguity, stopped, and missing cases.
- Verify context helpers preserve the user's default Docker context.
- Real Mac smoke: create environment, immediately run host `docker info`, stop, restart, and delete.
