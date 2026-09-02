# Product contract and terminology

Depends on: none

## Purpose

Make the new mission explicit and remove contradictory product language before implementation spreads more legacy assumptions.

## Step 1: Establish canonical language

- Define Developer Environment as the primary product object.
- Reserve workspace container and Docker container for their precise meanings.
- Describe Sandbox as the runtime isolation primitive and advanced API concept.
- Rename the public locked-down Container profile to Hardened, preserving compatibility aliases internally and on disk.
- Define host and target OS as separate explicit dimensions.
- State that Docker is implicit and private for Linux targets, while macOS and Windows targets expose native target capabilities.
- Define the ordered matrix: Linux-on-macOS and macOS-on-macOS now; Linux-on-Linux next; Linux-on-Windows then; Windows-on-Windows finally.

## Step 2: Establish product invariants

- One environment identity owns all compute, state, network, endpoint, credential, and target-capability resources; Linux environments additionally own all Docker state and contexts.
- No global capability endpoint, mutable current-environment symlink, or fallback routing.
- Developer is capability-rich; Hardened is not weakened.
- youki-only execution is non-negotiable for Linux targets and inapplicable to native macOS/Windows targets.
- Stop and delete are different operations.
- Public IDs, configuration, APIs, status, and core scenarios never encode a host-specific transport, path syntax, process model, or hypervisor.

## Step 3: Align canonical decisions

Update `README.md`, `docs/positioning.md`, `docs/1.0-scope.md`, `docs/2.0-vision.md`, `AGENTS.md`, and status language so future contributors implement one contract. Update stale Docker issue descriptions that still target Container or `~/.vz/docker.sock`.

## Validation

- Repository search finds no normative global vz Docker socket.
- No current document tells users to choose between Developer and Sandbox as peer product modes.
- Every future-state claim is status-labelled until release evidence exists.
