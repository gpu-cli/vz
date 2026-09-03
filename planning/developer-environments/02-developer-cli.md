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
  topology, health, endpoints, capability gaps, and per-Developer-Linux-Machine
  Docker contexts. Hardened Machines omit Docker contexts. `--all` lists
  instances for the resolved project.
- `stop` preserves identity and declared state. `delete` removes the selected
  Environment ownership graph.
- Bare `vz` prints the top-level help snapshot to stdout, exits zero, and never
  inspects, creates, or mutates resources.

## Step 2: Resolve identity without global state

Selection order is explicit immutable ID/name, process-scoped Environment and
Machine selectors, unambiguous project/worktree binding, then a declared default
or sole Machine. A worktree may resolve more than one named Environment;
ambiguity fails with a bounded candidate list. There is no mutable global current
Environment, Machine, socket, or Docker context.

## Step 3: Discover definitions and create instances deterministically

The checked-in portable project definition remains `vz.json` for 0.4 and is
validated against `schemas/vz-project-definition-v1.schema.json`; the minimal
bootstrap fixture is `examples/developer-environment/vz.json`. Commands search from the
working directory toward the filesystem root and select the nearest `vz.json`;
multiple candidates at the same selection level or an invalid definition fail
before mutation. The file carries a stable `project_id`, so moving or cloning a
worktree does not change project identity. Host paths may appear only in
workspace bindings, never as derived persistent IDs.

`vz up --environment <name>` creates that project-unique named Environment
instance when absent and otherwise reconciles it to the discovered definition
digest. A successful explicit selection always creates or refreshes the current
worktree binding after ownership validation. With no selector, `vz up` selects the sole
Environment bound to the current worktree or fails when several are bound. It
creates and binds project-wide `default` only when the Project has no existing
Environment. When this worktree has no binding but the Project already has any
Environment, it fails with candidates and requires an explicit existing or new
project-unique name. It never guesses, adopts, or silently creates a sibling.

`project_id` is a required opaque stable identifier stored in the file; the
typed ProjectDefinition authoring API generates it, while manual authors must
supply a schema-valid unique value once and retain it across moves/clones. A
missing definition returns `definition_not_found` plus the installed schema and
example paths; no resource is created. There is no mutating `vz init` path.
Authoring and richer definition mutation use the file/schema or typed API.

## Step 4: Remove infrastructure command families

The exhaustive pre-0.4 removal and retained-spelling migration inventory is
[`legacy-cli-removal.md`](legacy-cli-removal.md). Old spellings return
actionable migration errors. They are not aliases and do not maintain a second
lifecycle.

Files, logs, topology mutation, snapshots, faults, peering, diagnostics, and
individual Machine administration remain typed API operations. Docker work is
performed with the unmodified Docker CLI/API using a context returned by
`vz status`; vz never mutates the user's default context.

## Step 5: Provide stable automation behavior

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
- Nearest-definition discovery, missing/invalid/ambiguous definitions, first
  `default` creation, explicit named instance creation, sole selection, and
  multi-instance ambiguity all pass without path-derived identity.
- Bare `vz` exactly matches the zero-exit help snapshot and performs no reads
  beyond static help generation; old verbs reject with migration guidance.
- A clean directory proves missing-definition failure and zero mutation, then
  bootstraps only through the published example/schema or typed authoring API
  before `vz up` creates the first `default` instance.
- Two simultaneous Linux Machines return distinct Docker contexts and preserve
  the user's default context.
- Real local-Mac black-box tests cover all five verbs against multi-instance,
  multi-Machine, and mixed Linux/macOS topologies.
