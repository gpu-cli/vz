# vz Session Executor Contract

Date: 2026-09-02
Status: draft contract for external orchestrators driving agent sessions in vz sandboxes.
Boundary rules apply (docs/vz-innovation-planning.md): **no orchestrator-specific nouns in vz APIs, types, events, or env vars.** vz exports generic primitives; any orchestrator (prtl today) composes them. This doc names orchestrators only as consumers, never as API surface.

## Roles

- **Orchestrator** decides *what should happen*: attempts, task references, models, policy, evidence, node selection.
- **vz** decides *where execution happens and what it can touch*: the sandbox contract — mounts, resource caps, egress, credential brokering, checkpoints, transport.

## Surfaces (all `--json`, stable exit codes — vz-d0s; non-interactive by design)

1. **Lifecycle.** `spawn / status / terminate` on a sandbox, taking:
   - **agent definition artifact** — harness pin + toolbox + broker needs + policy (format from vz-9qf; the definition IS the integration currency);
   - **attach mode** — `spawn --worktree <existing-path>` binds a sandbox to a worktree the orchestrator already created (vz never creates/owns git objects in this mode; the solo-user create mode remains for direct UX);
   - returns: session id, **capability report**, transport endpoints.
2. **Session transport bridge.** Bridges the Agent Client Protocol (ACP — open standard) between the orchestrator (host) and the resident harness: over **vsock** on VM backends, **direct socket** on container backends. Same bridge pattern as buildkitd/dockerd/broker shims.
3. **Brokered credentials.** Per-session broker identities, pluggable providers (ssh-agent signer, gh token, Keychain, env), audit feed of every use (vz-6er). Credentials usable, never readable.
4. **Checkpoint primitives.** Idempotent checkpoint/restore/fork verbs (fs_quick; btrfs-backed on linux-native). The orchestrator composes these into rewind/retry semantics; vz does not know what an "attempt" is.
5. **Capability report.** Per-backend profile: `vm-resident | container-resident | ...`, isolation grade, available brokers, limits (vz-bgd). Orchestrators **query, never assume** — this is how one contract spans heterogeneous nodes.

## Backend matrix

| Backend | Host | Isolation grade | Residency |
|---|---|---|---|
| VZ VM | macOS | hardware VM | harness-in-VM (flagship, 1.0 capstone) |
| youki container | Linux | container (namespaces/cgroups) | harness-in-container |
| microVM (KVM) | Linux | hardware VM | future backend, same contract |

The contract is the product; backends are swappable behind it. A node reports what it offers; an orchestrator selects by capability.

## What vz will not grow (the boundary, restated)

Attempt graphs, task references, evidence stores, node admission/transport, model selection, fleet semantics. Any feature request containing those nouns is an orchestrator feature and belongs outside vz.

## Consumer note

prtl (the agent session control plane) is the first expected consumer: its nodes would gain an execution backend that drives this contract locally (prtl's remote overlay stays prtl's). The solo-user surface (`claude` in a worktree) and the orchestrator surface are the *same* primitives — one contract, two audiences.