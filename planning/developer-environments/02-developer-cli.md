# Minimal five-verb Developer Environment CLI

Depends on: First-class Environment topology, Machine identity, and durable lifecycle

## Purpose

Expose the complete normal Developer Environment workflow without leaking
backend or infrastructure nouns into the public CLI. Project configuration and
typed APIs define topology; five top-level verbs operate it.

## Step 1: Ship exactly five lifecycle verbs

```text
vz up [--environment <name-or-id>]
vz exec [--environment <name-or-id>] [--machine <name-or-id>] -- <command>
vz status [--environment <name-or-id> | --all] [--machine <name-or-id>] [--json]
vz stop [--environment <name-or-id>]
vz delete [--environment <name-or-id>]
```

- `up` creates or reconciles the selected complete Environment topology.
- `exec` selects the declared default/only Machine or fails with candidates; it
  auto-reconciles that Machine and its dependency closure.
- `status` is read-only and reports Projects, Environments, Machines, targets,
  topology, health, endpoints, capability gaps, and per-Linux-Machine Docker
  contexts. `--all` lists instances for the resolved project.
- `stop` preserves identity and declared state. `delete` removes the selected
  Environment ownership graph.
- Bare `vz` shows help/read-only status and never creates or mutates resources.

## Step 2: Resolve identity without global state

Selection order is explicit immutable ID/name, process-scoped Environment and
Machine selectors, unambiguous project/worktree binding, then a declared default
or sole Machine. A worktree may resolve more than one named Environment;
ambiguity fails with a bounded candidate list. There is no mutable global current
Environment, Machine, socket, or Docker context.

## Step 3: Remove infrastructure command families

0.4 does not retain public or hidden `vz dev`, `init`, `create`, `run`, `shell`,
`list`, `logs`, `restart`, `docker`, `stack`, `build`, `image`, `network`,
`machine`, `sandbox`, or `vm` execution paths. Old spellings return actionable
migration errors. They are not aliases and do not maintain a second lifecycle.

Files, logs, topology mutation, snapshots, faults, peering, diagnostics, and
individual Machine administration remain typed API operations. Docker work is
performed with the unmodified Docker CLI/API using a context returned by
`vz status`; vz never mutates the user's default context.

## Step 4: Provide stable automation behavior

All commands have stable exit classes, versioned structured errors, `--json`
where meaningful, request correlation, explicit timeouts/cancellation, and no
interactive prompts in non-interactive mode. Interactive/long-running work uses
streaming gRPC progress and a terminal result. Status JSON and events identify
the exact project, Environment, Machine, incarnation, and topology digest.

## Validation

- CLI parser/snapshot tests prove exactly five public lifecycle verbs; hidden
  help exposes no legacy product command family.
- Project, worktree, explicit instance, multiple instances per worktree,
  default/ambiguous Machine, stopped, missing, and unsupported target cases.
- Bare `vz` is read-only; old verbs reject with migration guidance.
- Two simultaneous Linux Machines return distinct Docker contexts and preserve
  the user's default context.
- Real local-Mac black-box tests cover all five verbs against multi-instance,
  multi-Machine, and mixed Linux/macOS topologies.
