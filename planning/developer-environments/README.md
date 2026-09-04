# Developer Environments: the primary vz product

Status: canonical 0.4 product plan
Release goal: [`GOAL-0.4.0.md`](GOAL-0.4.0.md)

## Mission

vz makes reproducible, parallel Developer Environments on developer
workstations. A project defines a topology; any number of isolated Environment
instances may realize it for worktrees, agents, tests, comparisons, and releases.
Each Environment contains one or more target-native Machines and owns their
combined lifecycle, storage, network fabric, DNS, endpoints, credentials,
policies, faults, and evidence.

A worktree normally resolves an Environment but is not its identity or a
one-instance limit. Target OS belongs to each Machine, so one Environment may
contain Linux and native macOS Machines on macOS now, and future supported
Machines on Linux and Windows. Linux remains the universal Machine target:
Linux-on-macOS now, Linux-on-Linux next, and Linux-on-Windows after that. Native
macOS-on-macOS is immediate; Windows-on-Windows follows last.

The locked-down sandbox/container capability remains an implementation primitive
and restricted profile, not the product center. Users and agents operate the
Environment topology rather than assembling VMs, containers, Docker daemons,
sockets, and networks themselves.

## Product contract

- `ProjectDefinition` is the versioned topology blueprint.
- `EnvironmentInstance` is the isolation, ownership, lifecycle, and evidence
  boundary. A project and a worktree may each have multiple named instances.
- `MachineInstance` is a target-native compute member. Its target specification,
  backend, lifecycle, filesystem, and capabilities are explicit.
- Each Developer-profile Linux Machine implicitly owns a private Docker Engine, containerd,
  BuildKit, images, volumes, networks, endpoint, and managed context. Docker is
  never Environment-global.
- Native macOS and Windows Machines expose native capabilities and never acquire
  a hidden Linux Docker sidecar.
- Machines communicate only through declared private or simulated-public paths.
  Each Environment has isolated routes, DNS, ingress, NAT, firewall, ports,
  egress policy, and deterministic faults.
- Separate Environments are default-deny even when they share a project or
  worktree. Explicit cross-Environment access is directional, service-scoped,
  least-privilege, expiring, and audited.
- The public lifecycle CLI is exactly `up`, `exec`, `status`, `stop`, and
  `delete`; topology resources and advanced operations live in the typed API.
- Pinned youki is the only OCI runtime present or executed in Linux Machines.
- Hardened remains restricted and does not acquire Developer/Docker defaults.

## Delivery phases and dependencies

```text
Topology product contract and 0.4 definition of done
                         |
                         v
Project / Environment / Machine identity and aggregate lifecycle
       /                 |                   |                 \
      v                  v                   v                  v
five-verb CLI    per-Developer-Linux Docker native macOS   topology network
      |                  |                   |          and isolation
      |                  v                   |                 |
      +-------- Machine Engine Adapter ------+-----------------+
                         |
                         v
         Linux-on-Mac + macOS-on-Mac target gates
                         |
                         v
      aggregate multi-instance/multi-Machine topology gate
                         |
                         v
                    vz 0.4 Mac GA
                         |
              Linux-on-Linux parity
                         |
              Linux-on-Windows parity
                         |
             Windows-on-Windows parity
```

The identity/lifecycle phase introduces the aggregate and child resource model;
all implementation tracks depend on it. CLI, Linux Docker, native macOS, and
network topology can then proceed in parallel. Target-specific Mac gates prove
their Machine contracts. The aggregate topology gate proves the product: several
Environment instances, multiple Machines, mixed targets, public-like networking,
faults, peering, isolation, recovery, and deletion. Mac GA depends on all three
gates. Later host work reuses the exact Machine and topology scenario IDs.

## 0.4 success criteria

- A project definition can create multiple concurrent named Environments, and a
  single worktree can create more than one without identity aliasing.
- An Environment can contain multiple Linux Machines and mixed Linux/native
  macOS Machines with independent target-qualified capabilities.
- Every Developer-profile Linux Machine exposes a distinct managed Docker context to the Mac's
  unmodified Docker/Compose/buildx clients and cannot observe another Machine's
  Docker API or state.
- Declared private paths work; undeclared paths fail. A simulated-public path
  exercises split DNS, `.test` TLS, routed ingress, firewall/NAT, egress policy,
  and seeded network faults without public or LAN exposure.
- Separate Environments cannot communicate until an explicit service peer grant
  is created, and revocation restores denial without network merging.
- Stop/up preserves identity and declared state. Failure recovery is idempotent.
  Deleting one Environment removes only its ownership graph while peers remain
  byte-for-byte and operationally unchanged.
- The public binary exposes the five lifecycle verbs and no hidden legacy
  product command families. CLI, gRPC streams, Rust API, status JSON, events,
  errors, and receipts agree.
- Release-built topology, Linux Docker, native macOS, and existing VM regression
  gates pass from the current local Mac exactly as specified in
  [`GOAL-0.4.0.md`](GOAL-0.4.0.md). Missing evidence or a skipped required
  scenario leaves the work open.

## Existing work to absorb

The epic must re-scope or link the existing Docker and Developer-environment
work rather than duplicate it: `vz-5in`, `vz-yr9`, `vz-k3v`, `vz-7ez`, `vz-0ml`,
`vz-avq`, `vz-kna`, `vz-767`, `vz-219`, `vz-305`, `vz-0yt`, `vz-dko`, `vz-ehz`,
`vz-wtz`, `vz-xym`, `vz-1jw`, `vz-bgd`, and completed youki-only BuildKit work
in `vz-356`.

## Plan files

- `GOAL-0.4.0.md` — exact scope, acceptance matrix, release gates, evidence, and
  terminal definition of done
- `architecture.md` — aggregate object model, identities, APIs, and invariants
- `reconcile-generation-fencing.md` — exact planned-generation preconditions,
  durable claims, mutation ordering, migration, and race/crash gates
- `reconcile-effective-inputs.md` — immutable desired activation inputs,
  canonical per-service digests, atomic capture, replay, and tamper gates
- `00-product-contract.md` — language, profiles, compatibility, and audit
- `01-environment-lifecycle.md` — aggregate and Machine identity/lifecycle
- `02-developer-cli.md` — five-verb CLI and deterministic selectors
- `legacy-cli-removal.md` — exhaustive 0.3 surface retirement and migration map
- `03-implicit-docker.md` — private Docker per Linux Machine
- `03-native-macos.md` — native macOS Machines in heterogeneous topologies
- `04-isolation-storage-network.md` — storage, network fabric, simulated public
  edge, faults, peering, and isolation
- `05-host-docker-bridge.md` — per-Developer-Linux-Machine endpoints and contexts
- `06-local-mac-validation.md` — release-built Linux/Docker Mac gate
- `06-macos-target-validation.md` — release-built native macOS Machine gate
- `07-migration-launch.md` — schema/CLI migration, public surfaces, and Mac GA
- `08-linux-parity.md` — Linux-host Machine/topology parity
- `09-windows-parity.md` — Linux-Machine parity on Windows
- `10-windows-native.md` — native Windows Machine parity
