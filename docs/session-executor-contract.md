# vz Developer Environment Executor Contract

Date: 2026-09-02
Status: draft contract for tools and external orchestrators driving work in vz Developer Environments.
Boundary rules apply (`docs/vz-innovation-planning.md`): **no orchestrator-specific nouns in vz APIs, types, events, or environment variables.** vz exports generic primitives; any orchestrator composes them.

## Product boundary

- **The caller** decides what work should happen: tasks, attempts, models, retry policy, evidence selection, and node placement.
- **vz** realizes a declared Project topology: Environment and Machine identity,
  target-native provisioning, workspace projections, resources, networks,
  services, egress, credentials, checkpoints, transport, and lifecycle.

The Developer Environment is the externally meaningful aggregate. It contains
one or more target-native Machines. A sandbox, VM, container, harness process,
or worktree may implement, inhabit, or bind to a Machine/Environment but is not
its portable identity.

## Surfaces

All surfaces provide structured output, stable exit codes, versioned errors, streaming responses for interactive or long-running operations, and target-qualified capability reporting.

1. **Environment topology lifecycle.** Create/reconcile, resolve, start, inspect,
   watch, stop, recover, and delete an Environment containing Machines,
   networks, endpoints, volumes, workspace bindings, and policies. Results
   include stable aggregate identity, topology digest, state, and evidence.
2. **Machine lifecycle and execution.** Select a Machine explicitly or by an
   unambiguous default; inspect/watch/start/stop/rebuild it and run commands or a
   resident harness with streaming stdin/stdout/stderr, PTY, signals,
   cancellation, exit status, and telemetry.
3. **Files, workspaces, and endpoints.** Scope file operations and projections
   to Environment/Machine identity; expose ports/services through declared
   private or simulated-public paths without backend transport leakage.
4. **Target services.** Resolve capabilities by Machine. Every Linux Machine has
   its own Docker endpoint/context; it is never Environment-global and never
   falls back to another Machine or daemon.
5. **Brokered credentials.** Assign per-environment broker identities and provider capabilities with an audit feed of every use. Credentials are usable according to policy but are not copied into the target as ambient secrets.
6. **Checkpoints and forks.** Expose idempotent primitives where the backend supports them. The caller composes those primitives into retry or rewind semantics; vz does not know what an “attempt” is.
7. **Networks and faults.** Declare segments, service paths, DNS, ingress, TLS,
   NAT/firewall, egress, and explicit Environment peer grants; inject bounded,
   deterministic faults with receipts.
8. **Events and capability reports.** Emit Environment- and Machine-scoped
   lifecycle/service/topology events and versioned capabilities. Callers query,
   never assume.

## Host×Machine-target matrix and order

| Order | Host | Target | Backend direction | Contract status |
|---:|---|---|---|---|
| 1 | macOS | Linux | Virtualization.framework Linux VM | Immediate |
| 2 | macOS | macOS | Virtualization.framework macOS VM | Immediate |
| 3 | Linux | Linux | Native Linux isolation / optional VM-grade backend | Next |
| 4 | Windows | Linux | Windows-hosted Linux virtualization | Then |
| 5 | Windows | Windows | Native Windows virtualization/isolation | Final |

Linux is the universal Machine target across supported hosts. Native macOS and
native Windows Machines are target-specific additions. Capability sets and
evidence remain host×Machine-target qualified.

## Residency modes

Execution location is a capability rather than a separate product tier:

| Mode | Meaning |
|---|---|
| Direct execution | Commands run on a selected Machine through its guest/native agent |
| Resident harness | A harness lives on a selected Machine and communicates over a declared transport |
| Nested exec zone | Harness-launched code receives a stricter environment-local sandbox |
| Host-driven service | A normal host client drives a selected Machine service, such as one Linux Machine's Docker Engine |

An orchestrator may bind an Environment to an existing worktree without
transferring Git ownership. A worktree may bind to several named Environments.
Environment identity—not the worktree or harness—scopes aggregate services,
events, credentials, and cleanup; Machine identity scopes target execution.

## What vz will not grow

Attempt graphs, task references, model selection, fleet admission/transport, global evidence stores, or orchestrator-specific scheduling semantics belong outside vz. Features should be expressed as generic environment lifecycle, execution, files, ports, service, policy, broker, checkpoint, event, or capability primitives.

## Historical note

The original version of this contract centered a “sandbox session,” described VZ VM harness residency as the 1.0 flagship, and listed Linux containers and KVM microVMs as alternate backends. Those backend and residency patterns remain valid. The current contract lifts them under the Developer Environment object so it can also represent native macOS now, universal Linux across all hosts, and native Windows last.

## Consumer note

prtl is the first expected orchestrator consumer: a node can drive this contract locally while prtl retains its own remote overlay and work-attempt semantics. Solo users, IDEs, host CLIs, and orchestrators share the same primitives and capability truth.
