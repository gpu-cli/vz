# Product contract and terminology

Depends on: none

## Purpose

Make the new mission explicit and remove contradictory product language before implementation spreads more legacy assumptions.

## Step 1: Establish canonical language

- Define Developer Environment as the primary product object.
- Define a ProjectDefinition as a reusable topology blueprint, an
  EnvironmentInstance as its isolated realization, and a MachineInstance as one
  target-native compute member. A one-Machine Environment is only the default
  simple case.
- Make a worktree a workspace binding/default selector rather than persistent
  identity or a one-Environment limit.
- Reserve workspace container and Docker container for their precise meanings.
- Describe Sandbox as the runtime isolation primitive and advanced API concept.
- Rename the public locked-down Container profile to Hardened, preserving compatibility aliases internally and on disk.
- Define host OS and Machine target OS as separate explicit dimensions; an
  Environment may contain heterogeneous Machine targets.
- State that Docker is implicit and private for each Linux Machine, while macOS
  and Windows Machines expose native target capabilities.
- Define the ordered matrix: Linux-on-macOS and macOS-on-macOS now; Linux-on-Linux next; Linux-on-Windows then; Windows-on-Windows finally.

## Step 2: Establish product invariants

- One Environment identity owns a topology of Machines plus all aggregate state,
  networks, endpoints, credentials, policies, faults, and evidence. Machine
  identity owns target-native state and capabilities; every Linux Machine owns
  an independent Docker stack and context.
- Project membership grants no connectivity. Machines use declared paths;
  separate Environments are default-deny and can interact only through explicit,
  directional, expiring service peer grants.
- No global capability endpoint, mutable current-environment symlink, or fallback routing.
- Developer is capability-rich; Hardened is not weakened.
- youki-only execution is non-negotiable for Linux targets and inapplicable to native macOS/Windows targets.
- Stop and delete are different operations.
- Public IDs, configuration, APIs, status, and core scenarios never encode a host-specific transport, path syntax, process model, or hypervisor.

## Step 3: Align canonical decisions

Update `README.md`, `PRODUCT.md`, `docs/positioning.md`, scope/vision documents,
agent instructions, the site, and status language so contributors implement one
contract. Update stale issues that encode a singular Environment TargetSpec,
per-Environment Docker, `vz dev`, Container Docker, or `~/.vz/docker.sock`.

## Validation

- Repository search finds no normative global vz Docker socket.
- Repository search finds no normative one-target-per-Environment,
  per-Environment Docker-daemon, or canonical `vz dev` claim.
- No current document tells users to choose between Developer and Sandbox as peer product modes.
- Canonical surfaces use Project -> Environment -> Machine identity and the
  five-verb public CLI.
- Every future-state claim is status-labelled until release evidence exists.
