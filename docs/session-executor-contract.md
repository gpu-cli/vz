# vz Developer Environment Executor Contract

Date: 2026-09-02
Status: draft contract for tools and external orchestrators driving work in vz Developer Environments.
Boundary rules apply (`docs/vz-innovation-planning.md`): **no orchestrator-specific nouns in vz APIs, types, events, or environment variables.** vz exports generic primitives; any orchestrator composes them.

## Product boundary

- **The caller** decides what work should happen: tasks, attempts, models, retry policy, evidence selection, and node placement.
- **vz** decides where and how local execution happens: Developer Environment identity, target OS, provisioning, mounts, resource caps, services, egress, credential brokering, checkpoints, transport, and lifecycle.

The Developer Environment is the externally meaningful execution object. A sandbox, VM, container, harness process, or worktree session may implement or inhabit an environment, but none replaces it in the portable contract.

## Surfaces

All surfaces provide structured output, stable exit codes, versioned errors, streaming responses for interactive or long-running operations, and target-qualified capability reporting.

1. **Environment lifecycle.** Create, resolve, start, inspect, stop, recover, clone, checkpoint where supported, and delete a Developer Environment. Inputs include a reproducible definition, explicit target OS, project/worktree attachment, profile, resources, and policy. Results include stable environment identity, host×target pair, backend, state, capabilities, and endpoints.
2. **Execution.** Start commands or an optional resident harness with streaming stdin, stdout, stderr, PTY resize, signals, cancellation, exit status, and resource telemetry.
3. **Files, mounts, and ports.** Provide explicit, policy-checked project attachment, file operations, shared paths, and port publication without exposing backend-specific transport to ordinary callers.
4. **Target services.** Resolve services advertised by the selected environment. For a Developer-profile Linux target this includes that environment's Docker endpoint/context; it is never a global endpoint and never silently falls back to another daemon.
5. **Brokered credentials.** Assign per-environment broker identities and provider capabilities with an audit feed of every use. Credentials are usable according to policy but are not copied into the target as ambient secrets.
6. **Checkpoints and forks.** Expose idempotent primitives where the backend supports them. The caller composes those primitives into retry or rewind semantics; vz does not know what an “attempt” is.
7. **Events and capability reports.** Emit environment-scoped lifecycle/service events and a versioned capability report. Callers query, never assume.

## Host×target matrix and order

| Order | Host | Target | Backend direction | Contract status |
|---:|---|---|---|---|
| 1 | macOS | Linux | Virtualization.framework Linux VM | Immediate |
| 2 | macOS | macOS | Virtualization.framework macOS VM | Immediate |
| 3 | Linux | Linux | Native Linux isolation / optional VM-grade backend | Next |
| 4 | Windows | Linux | Windows-hosted Linux virtualization | Then |
| 5 | Windows | Windows | Native Windows virtualization/isolation | Final |

Linux is the universal target across supported hosts. Native macOS and native Windows are target-specific additions. The contract is shared; capability sets and evidence remain host×target-qualified.

## Residency modes

Execution location is a capability rather than a separate product tier:

| Mode | Meaning |
|---|---|
| Direct execution | Commands run in the environment through the guest/native agent |
| Resident harness | A harness lives inside the environment and communicates over ACP or another declared transport |
| Nested exec zone | Harness-launched code receives a stricter environment-local sandbox |
| Host-driven service | A normal host client drives a selected target service, such as Docker in one Linux environment |

An orchestrator may attach an environment to an existing worktree without transferring ownership of Git objects. Solo workflows may ask vz to create a worktree. In both cases the environment identity, not the worktree or harness, scopes services, events, credentials, and cleanup.

## What vz will not grow

Attempt graphs, task references, model selection, fleet admission/transport, global evidence stores, or orchestrator-specific scheduling semantics belong outside vz. Features should be expressed as generic environment lifecycle, execution, files, ports, service, policy, broker, checkpoint, event, or capability primitives.

## Historical note

The original version of this contract centered a “sandbox session,” described VZ VM harness residency as the 1.0 flagship, and listed Linux containers and KVM microVMs as alternate backends. Those backend and residency patterns remain valid. The current contract lifts them under the Developer Environment object so it can also represent native macOS now, universal Linux across all hosts, and native Windows last.

## Consumer note

prtl is the first expected orchestrator consumer: a node can drive this contract locally while prtl retains its own remote overlay and work-attempt semantics. Solo users, IDEs, host CLIs, and orchestrators share the same primitives and capability truth.
