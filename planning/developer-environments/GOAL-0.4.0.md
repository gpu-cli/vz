# vz 0.4.0 goal and definition of done

Status: canonical release goal
Target release: 0.4.0
Parent roadmap: `vz-mzs`

## Goal

Deliver vz 0.4.0 as a local-first system for reproducible, parallel Developer
Environments. A project definition can be instantiated as any number of isolated,
named Environment instances, and each Environment is a declarative topology of
one or more target-native Machines plus its owned storage, networks, DNS,
endpoints, credentials, policies, faults, lifecycle, and evidence.

The first complete product runs on Apple-silicon macOS and supports Linux and
native macOS Machines in the same Environment. A native macOS Machine is a
separately virtualized macOS guest with its own kernel, disk, process/service
namespace, credentials, and lifecycle; executing a process directly on the host
cannot satisfy the target. Every Developer-profile Linux Machine implicitly
provides its own private Docker Engine, containerd, BuildKit state, image and
volume stores, networks, endpoint, and managed Docker context. Host `docker`,
Compose, and buildx drive that exact Machine without Docker Desktop, a global vz
daemon, or another Environment. Linux remains the universal Machine target for
later Linux and Windows hosts; native Windows Machines follow on Windows.

## Product boundary

The portable hierarchy is:

```text
ProjectDefinition
└── EnvironmentInstance (isolation, ownership, and lifecycle boundary)
    ├── MachineInstance[] (profile, target OS, and capabilities)
    ├── Network[] and declared service paths
    ├── Endpoint[], HostImport[], HostExport[], and EgressPolicy[]
    │   └── environment-local public-like ingress
    └── Volume[], SecretBinding[], Fault[], Execution[], and Receipt[]
```

- A worktree is a workspace binding and default selector, not an identity or a
  one-environment limit. One project and one worktree may each have multiple
  Environment instances.
- Machines that form one system belong to one Environment. Separate
  Environments are mutually isolated, even when they share a project, worktree,
  definition, names, CIDRs, ports, or host.
- Target OS belongs to a Machine, not to an Environment. A single Environment
  may therefore contain Linux and native macOS Machines now and Windows Machines
  later.
- Cross-Environment communication is forbidden by default. It requires an
  explicit, directional, service-scoped, expiring, audited peer grant; it never
  merges route domains or exposes storage, credentials, Docker, or control-plane
  access.
- The public Developer Environment CLI has exactly five lifecycle verbs:
  `vz up`, `vz exec`, `vz status`, `vz stop`, and `vz delete`. Environment and
  Machine selectors make them topology-aware. Infrastructure-oriented command
  families such as `dev`, `run`, `shell`, `list`, `logs`, `restart`, `docker`,
  `stack`, `network`, `machine`, and `vm` are not retained as public or hidden
  compatibility paths in 0.4. Richer operations are typed, versioned APIs;
  Docker workloads use the native Docker API and clients.

## Required implementation

0.4.0 includes all of the following:

1. Versioned ProjectDefinition, WorkspaceBinding, EnvironmentSpec/Instance,
   MachineSpec and MachineInstance with a required Developer/Hardened profile,
   replaceable MachineIncarnation,
   Network, Endpoint, HostImport, HostExport, EgressPolicy, Volume,
   SecretBinding, PeerGrant, Fault, Execution, Receipt, lifecycle, and ownership schemas
   with migration from legacy records. Immutable IDs, human selectors, and
   machine incarnations are distinct. Persistent identity never depends on raw
   host path spelling.
2. Idempotent reconcile, start, stop, inspect, exec, and delete behavior for a
   complete Environment topology. Stop preserves identity and declared state;
   delete traverses only the selected Environment's ownership graph.
3. Stream-first gRPC and equivalent Rust/API contracts for Environment watch and
   reconciliation, Machine lifecycle and exec, events/logs, files, endpoints,
   fault injection, snapshots, receipts, and capability discovery. Short,
   bounded reads may remain unary. Local endpoints authorize every handle and
   stream against exact Environment/Machine ownership. Streams specify and test
   ordering, bounded buffering/backpressure, cancellation, terminal status, and
   reconnect/resume behavior without leaking another owner's events or data.
4. Deterministic selection by explicit immutable ID/name, process-scoped
   selector, or unambiguous project/worktree binding. There is no mutable global
   current Environment. Ambiguity fails closed and reports candidates.
   The nearest checked-in `vz.json` is the 0.4 ProjectDefinition. Missing,
   invalid, or ambiguous definitions fail before mutation.
   `vz up --environment <name>` creates or reconciles that project-unique named
   instance and, on success, always creates or refreshes the current worktree
   binding after ownership validation. Without a selector, the
   sole instance bound to this worktree is selected; multiple bound instances
   are ambiguous. `default` is created only when the Project has no Environment.
   An unbound worktree never silently adopts or creates beside existing
   instances. There is no mutating `vz init` fallback.
5. Explicit workspace projections per Machine (`read-write`, `read-only`, or
   `snapshot`) and explicit ownership/consistency rules for volumes and shared
   caches. A writable block device is never silently attached to two Machines.
6. An Environment-owned network fabric with isolated routing and DNS views,
   declared private links, environment-local public-like ingress, firewall/NAT,
   TLS, controlled egress, explicit host imports/exports, collision-safe ports,
   and deterministic latency/loss/bandwidth/partition/DNS fault controls. Host
   imports use authenticated Environment/Machine-owned relays to exact stored
   loopback services and are independent from external egress. No shared NAT
   gateway address, wildcard host listener, or guest-selected host destination
   is an authorization boundary.
7. A private Docker capability on every Developer-profile Linux Machine. Each
   such Machine has a unique daemon, state, BuildKit cache, network namespace,
   endpoint, and Docker context.
   Hardened Linux Machines and native macOS Machines do not acquire Docker;
   native Machines never receive a hidden Linux sidecar. New 0.4 definitions
   require an explicit Machine profile; legacy migration assigns it
   deterministically and never upgrades Hardened to Developer.
8. Pinned and verified artifacts, with youki as the only OCI runtime binary
   present or executable in every Linux Machine. Runtime overrides and
   runc/crun fallback fail closed.
9. Release packaging, migration, help, examples, skills, website, status output,
   and documentation that expose this one model and accurately label ACTIVE,
   DEV, and PLANNED host×Machine-target capabilities. A checked-in
   machine-readable capability matrix is the source of truth; generated or
   validated help/docs/site/status claims must match it exactly.

## Observable acceptance criteria

The release is acceptable only when every assertion below is demonstrated by a
required E2E scenario and retained evidence:

1. **Multiple instances:** two worktrees each create an Environment, and one of
   those worktrees creates a second named Environment. All three run
   concurrently with repeated Machine names, service names, internal ports, DNS
   aliases, and overlapping guest CIDRs without collision or cross-observation.
2. **Multiple Machines:** one Environment runs at least two Developer-profile
   Linux Machines, one Hardened Linux Machine, and one native macOS Machine.
   `vz status --json` reports stable Environment and Machine IDs,
   target-qualified profiles/capabilities, topology, health, endpoints, and
   Docker contexts. Ambiguous `vz exec` without a default Machine fails closed.
   The Hardened Machine omits all Docker capabilities/contexts and cannot use a
   sibling Developer Machine endpoint.
3. **Machine-scoped Docker:** both Developer-profile Linux Machines are
   independently driven by the Mac's unmodified `docker`, Compose, and buildx
   clients. Containers,
   images, volumes, networks, events, caches, and lifecycle operations in one
   context are absent from the other and from every other Environment.
4. **Target-native execution:** Linux commands execute on Linux. The checked-in
   native fixture `tests/fixtures/vz-0.4/native-macos-swift/` builds and tests
   with the exact Xcode/Swift toolchain digest pinned by the release contract,
   then its release executable prints the expected fixture protocol/version.
   VM identity/process evidence proves it ran inside the selected separately
   virtualized macOS Machine and not on the host. Unsupported target/capability
   requests fail without substitution.
5. **Private topology:** a client, API, and database communicate only over their
   declared paths. The client cannot directly reach the database, and an
   undeclared Machine-to-Machine path is denied. At least one required service
   path crosses between a Linux Machine and a native macOS Machine in both
   directions permitted by its declarations; undeclared reverse/side paths fail.
6. **Public-like topology:** a client reaches an API through environment-local
   split DNS, a `.test` hostname, TLS, routed ingress, firewall, and NAT. Evidence
   proves the path crossed the virtual edge rather than localhost or a private
   shortcut. Nothing listens on the host LAN or public Internet.
7. **Host boundaries:** host imports and exports are absent by default. A host
   service bound only to `127.0.0.1` works through one declared import from its
   authorized Machine. The undeclared port, wrong protocol, wrong Machine,
   sibling Environment, arbitrary host destination, and LAN access are denied.
   Offline egress does not break the declared import; enabled egress does not
   create one. Loopback exports work without collisions, and listener evidence
   proves no wildcard or LAN host listener exists.
8. **Cross-Environment isolation and peering:** the three Environments cannot
   resolve, route to, read, control, or receive events from one another. One
   directional endpoint peer grant permits only its declared protocol/port,
   denies reverse and transitive access, then passes both explicit revocation and
   independent TTL expiry scenarios with clean denial restoration.
9. **Network faults:** seeded latency, loss, bandwidth restriction, DNS failure,
   reset, and timed partition affect only the declared path. The checked-in gate
   manifest fixes numeric tolerances and activation/removal deadlines. TTL
   cleanup restores baseline connectivity and emits a receipt.
10. **Lifecycle and recovery:** stop/up preserves identity and declared disks,
    volumes, Docker data, and endpoints. Daemon/adapter/guest crashes and Mac
    sleep/wake reconstruct authoritative routes, sockets, DNS, and port state
    within manifest deadlines without cross-routing or stale resources.
11. **Deletion safety:** deleting one Environment removes only its Machines,
    disks, sockets, contexts, routes, DNS, ports, credentials, and faults. The
    other Environments continue serving traffic and retain byte-identical
    sentinel data.
12. **Agentic workload:** a checked-in deterministic agent driver runs at least
    three isolated workers against separate Environments and two cooperating
    workers against a Linux Machine and a native macOS Machine in one Environment
    using a fixed schedule, a declared cross-target service path, and
    synchronization barriers. Workspace writer policy is enforced and every
    output, cancellation, PTY, exit status, event, and receipt maps to the exact
    Environment, Machine, worker, and request.
13. **Resource pressure:** the supported Mac runs the manifest's declared
    Machine resources, 20 concurrent ready Linux containers for at least 60
    seconds, four parallel pulls, four parallel builds, and eight parallel execs.
    A 1 GiB-limited Machine is deliberately OOM-killed and a 1 GiB disposable
    test volume is filled; unrelated services must pass one-second health probes
    for 60 seconds with zero failures and unchanged sentinel checksums.
14. **Runtime provenance:** recursive inventory and execution-derived evidence
    prove youki is the only OCI runtime binary present or invoked. Docker Desktop,
    the host system daemon, runc, and crun are proven unused.
15. **CLI/API agreement:** the five CLI verbs, Rust API, gRPC streams, status
    JSON, events, errors, and receipts agree on identities, state transitions,
    topology, capabilities, and failures. Old public command families are absent
    from normal and hidden CLI help and are rejected with migration guidance.
16. **Reproducibility:** after final deletion, the pinned ProjectDefinition is
    recreated from fresh state. It resolves the same topology/configuration and
    artifact digests with new runtime identities and none of the deleted mutable
    sentinel data.
17. **Workspace and storage policy:** read-write, read-only, and snapshot
    projections pass target-qualified file semantics; forbidden writes fail;
    a writable block volume attached to two Machines is rejected before
    mutation; declared shared-cache consistency passes its concurrent fixture.
18. **Secrets and snapshots:** SecretBindings are scoped to the selected
    Environment/Machine, redacted from status/logs/evidence, audited on use, and
    denied cross-boundary. Snapshot/restore behavior passes where advertised and
    returns an explicit unsupported capability otherwise.
19. **Migration and installation:** a clean 0.4 installation and an upgrade from
    the pinned v0.3.20 release fixture migrate a legacy record to one Project,
    one Environment, and one Machine without data loss. Injected migration
    failure restores the backup and leaves v0.3.20 rollback usable. Legacy
    Hardened/generic records remain semantically Hardened/generic and do not
    acquire Developer, Docker, host-import, or egress defaults. Uninstall removes
    only vz-owned software/runtime resources and preserves project data and
    unrelated Docker state.
20. **Exhaustive network denial:** a machine-readable
    source×destination×protocol×port matrix records expected and observed results
    for private, public-like, declared and undeclared host imports/exports,
    offline/allowed/CIDR/domain Internet policy, LAN/control-plane destinations,
    and peered paths. Two Machines in one Environment use different egress
    attachments without policy or host-import cross-talk. Any unexpected success
    fails the gate.
21. **CLI removal:** a checked-in help snapshot lists the five lifecycle verbs.
    Every entry in
    [`legacy-cli-removal.md`](legacy-cli-removal.md) is invoked; each removed
    command/flag returns the specified nonzero exit class and structured
    migration message, while retained `status`/`stop` spellings prove only the
    new semantics are reachable. No entry is an executable hidden alias. Bare
    `vz` exactly matches static top-level help, exits zero, and performs no state
    discovery or mutation. In a clean directory `vz up` fails with
    `definition_not_found` and zero mutation; after schema/API-only bootstrap it
    creates `default`, and new-worktree/multi-instance binding rules match the
    contract without guessing or adoption.
22. **Definition reconciliation:** changing a mutable ProjectDefinition field
    produces a deterministic plan and reconciles each selected Environment
    without changing its stable identity. Immutable/unsafe changes fail before
    mutation with a structured explanation. Concurrent updates, interrupted
    reconciliation, and stale clients converge or fail without mixed-version
    topology, cross-owner adoption, or orphaned resources. Every scoped create,
    recreate, and remove additionally satisfies the exact-generation
    precondition, durable-claim, fail-closed migration, and race/crash contract
    in [`reconcile-generation-fencing.md`](reconcile-generation-fencing.md).
    Desired planning and activation additionally consume the same immutable,
    operation-owned effective-input snapshot, canonical service digests, and
    fail-closed replay/tamper contract in
    [`reconcile-effective-inputs.md`](reconcile-effective-inputs.md).

## Strict E2E release gate

Unit, component, integration, serialization, migration, and CLI snapshot tests
are mandatory prerequisites, but they never satisfy this goal by themselves.
Release acceptance uses one installed, signed, release-built artifact set and
black-box public interfaces from the user's local Apple-silicon Mac.

### Versioned gate inputs

The implementation must add and freeze these checked-in, schema-validated inputs
before its acceptance gate can close:

- `config/vz-0.4-e2e-contract.json`: stable scenario IDs, exact fixtures,
  Machine resources, operation deadlines, health windows, fault parameters and
  numeric tolerances, listener checks, cleanup rules, and required evidence;
- `config/docker-compatibility-v0.4.json`: supported Docker Engine API, Docker
  CLI, Compose, and buildx version ranges; every required command/API behavior,
  expected result, scenario ID, and intentional exclusion;
- `config/vz-0.4-migration-barriers.json`: every durable schema, filesystem,
  context, credential, and ownership mutation boundary plus its required
  pre/post/failure state and rollback assertion; an empty inventory is invalid;
- `config/vz-0.4-decisions.json`: schema-validated scope/exclusion decisions,
  rationale, approval identity, source reference, and effective commit, all
  committed before the release-candidate commit they affect;
- `config/vz-0.4-decision-authorities.json`: project-owner-approved public keys
  allowed to authorize scope/exclusion decisions; every decision carries a
  detached signature verified against this pre-candidate authority set;
- `config/host-target-capabilities-v0.4.json`: the authoritative
  host×Machine-target/profile capability and ACTIVE/DEV/PLANNED matrix consumed
  by capability discovery and checked against public help, docs, and site copy;
- `tests/fixtures/vz-0.4/`: pinned project/worktree, Linux service, native
  macOS Swift, registry, workspace-mode, shared-cache, secrets, migration,
  pressure, network, and deterministic agent-driver fixtures; and
- JSON schemas for the aggregate manifest, lane summaries, connectivity matrix,
  runtime provenance, resource inventories, and receipts.

The contract initially requires the following measurable network/recovery
checks; changing them requires a recorded product decision and updated manifest:

- injected one-way latency: 100 ms with observed median added latency within
  ±30 ms over 100 requests;
- bandwidth cap: 10 MiB/s with measured payload throughput within ±20%;
- loss/partition: zero successful new connections across 100 attempts during a
  30-second active window while unrelated one-second health probes have zero
  failures;
- fault activation, removal, and peer revocation: effective within 5 seconds;
- warm Machine/service recovery: healthy within 60 seconds; aggregate cold
  topology creation: healthy within 10 minutes; and
- every readiness loop polls a documented condition within a deadline; it is not
  counted as a test retry.

Docker compatibility means every required entry in the versioned Docker
manifest passes. “Full Docker parity” must never mean unbounded equivalence to
all historical Docker releases or plugins; public claims name the tested
versions and intentional exclusions. Adding an exclusion after a failing gate
requires an explicit recorded product decision, not a test waiver.

At minimum the manifest covers, from the Mac's supported unmodified clients:

- Engine/version/info/context negotiation; registry login, pull, push, tag,
  inspect, save/load, and removal;
- create/start/stop/restart/kill/wait/remove, health checks, logs, events,
  attach, exec, stdin/TTY/signals, and exact exit results;
- bind mounts, named volumes, tmpfs/read-only mounts, ownership, persistence,
  user-defined networks, DNS, published ports, and cleanup;
- BuildKit multi-stage and parallel builds, build arguments, secrets, cache
  reuse/export/isolation, SSH mounts where advertised, and output export;
- Compose create/up/down, dependency and health ordering, networks, volumes,
  logs, exec, scaling where advertised, and failure propagation; and
- resource limits/OOM behavior, daemon restart recovery, concurrent clients,
  and isolation between two Machines in one Environment and Machines in sibling
  Environments.

Every behavior has a stable scenario ID and expected result. A capability is
either tested or explicitly excluded by a product decision recorded before the
release candidate; silence is not compatibility.

### Release candidate and entry point

The repository must ship `scripts/build-vz-0.4-release-candidate.sh`, which builds
with the lockfile, assembles all host/guest/kernel/runtime artifacts, records
their SHA-256 digests, applies the required local test signature/entitlements,
and verifies them with `codesign --verify --strict`. GA additionally requires
the Developer ID-signed and notarized distribution built by the release workflow
to have the same source commit and normalized pre-signing component content
digests. The manifest separately records each locally signed and distribution-
signed file digest because signing changes Mach-O bytes. Signing metadata,
entitlements, hardened-runtime/options, embedded provisioning, and notarization
are compared against the release contract. The terminal GA run uses the actual
Developer ID-signed and notarized installed distribution; a locally test-signed
run is development evidence only. Harnesses invoke the
installed release directory supplied by `--release-dir`; `cargo run`, debug
binaries, and PATH fallbacks are rejected.

The checked-in gate contract declares the minimum supported Apple-silicon Mac,
macOS version (never below the repository's macOS 14 baseline), free memory and
disk, and required host Docker/Compose/buildx client ranges. The normative gate
must pass under that minimum resource envelope; a larger Mac may additionally
certify concurrency/performance but cannot silently raise the supported floor.
The release builder operates from a fresh checkout with clean tracked and
submodule state. It rejects tracked changes, untracked source inputs, unpinned
submodules, and generated artifacts not reproduced by the build, and records a
canonical source-tree digest in addition to the Git commit.

The single release-gate entry point is:

```bash
scripts/run-vz-0.4-release-gate.sh \
  --suite all \
  --release-dir <verified-release-directory> \
  --run-id <unique-run-id>
```

It runs these required prerequisites and lanes, forwarding one run ID, isolated
state root, release digest, fixture digest, and evidence root:

```bash
cargo fmt --manifest-path crates/Cargo.toml --all -- --check
cargo clippy --manifest-path crates/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo nextest run --manifest-path crates/Cargo.toml --workspace --all-features
scripts/run-sandbox-vm-e2e.sh --suite all --profile release
scripts/run-developer-environment-e2e.sh --suite all
scripts/run-linux-docker-e2e.sh --suite all
scripts/run-macos-developer-environment-e2e.sh --suite all
scripts/validate-vz-0.4-evidence.sh <aggregate-manifest>
```

The aggregate harness orchestrates rather than replaces target lanes. Linux
Docker tests run in local vz-managed Linux Machines and use the Mac's installed
unmodified Docker CLI/plugins. Native tests use local vz-managed macOS Machines.
Arbitrary SSH hosts, Docker Desktop/system daemons, guest-side substitute
clients, internal Rust shortcuts, mocks, manually preconfigured resources, and
unrecorded network services are forbidden as release evidence.

### Staged run and safe state handling

One invocation performs three ordered phases under a unique temporary vz state
root. It never deletes, adopts, or inspects unrelated user resources:

1. `clean-provision`: assert no resources bearing the run ID exist, create the
   topology, run clean-state scenarios, write persistence sentinels, and emit a
   schema-validated, content-addressed state-handoff manifest without cleanup;
2. `persisted-recovery`: consume that exact handoff, exercise stop/up,
   daemon/adapter/guest termination at manifest-defined injection points,
   interrupted operations, faults, and recovery, then run warm-state scenarios;
3. `final-cleanup`: delete only resources carrying the run ID, verify surviving
   fixture Environments stayed healthy, inventory host/guest resources, and fail
   unless every owned socket, route, DNS record, listener, context, process,
   disk, port, fault, and credential materialization is gone.

No cleanup assertion is made between phases one and two. Each scenario process
starts once. Test-case retries are forbidden; a failed assertion fails the run.
Bounded readiness polling is permitted only where declared in the gate manifest
and its samples are recorded. Candidate identity is a digest tuple over the
source commit/tree, normalized release contents, signed distribution,
gate/Docker/capability/migration/decision manifests, schemas, fixtures, and
harness scripts. A fresh top-level invocation may start a new run ID only for a
new candidate tuple. It cannot replace or hide a failed result, and the
aggregate index retains failed candidates alongside the eventual passing
candidate.

Host sleep/wake is a mandatory hardware scenario, not “where possible.” The
harness writes a pre-sleep checkpoint, instructs the operator/runner to perform
one real sleep/wake cycle, and resumes only with the matching run ID and recorded
sleep/wake timestamps. It also captures OS power-management sleep/wake events,
boot/session identity, monotonic-clock discontinuity, and checkpoint nonce; the
validator proves a real hardware sleep interval occurred between the bound
events. Automated CI incapable of host sleep cannot certify the release; the
local-Mac evidence must contain the hardware phase.

### Evidence and mechanical verdict

All lanes write beneath one aggregate root:

```text
.artifacts/vz-0.4-e2e/<run-id>/
├── manifest.json
├── summary.json
├── summary.txt
├── checksums.sha256
├── sandbox-vm/
├── topology/
├── linux-docker/
└── native-macos/
```

`manifest.json` binds schema version, Git commit/dirty state, run ID, each phase,
host and toolchain facts, fixture/config digests, release component/signature
digests, lane paths, every required scenario ID, and final verdict. Raw evidence
includes command/stdout/stderr/exit/timing logs; Project/Environment/Machine maps;
resolved topology; status/event streams; Docker contexts and state inventories;
runtime inventory/invocation proof; persistence checksums; source×destination×
protocol×port expected/observed matrices; DNS/routes/NAT/firewall/TLS/ingress;
fault/peering receipts; before/during/after host listeners and interfaces;
recovery timelines; and pre/post resource inventories.

Listener evidence probes every applicable local interface and proves host exports
bind only to their declared loopback address. Crash cases identify the exact
process, signal, injection barrier, expected intermediate state, recovery
deadline, and post-recovery state/leak assertions. Secrets are redacted and the
validator scans for fixture canaries that must never appear.

`scripts/validate-vz-0.4-evidence.sh` validates the JSON schemas, checks every
required scenario appears exactly once as PASS, verifies `checksums.sha256`,
recomputes release/fixture/config digests, compares summary claims with raw
results, and fails on any missing field/ID, unexpected network success,
prohibited runtime/daemon, secret canary, test retry, digest mismatch, stale
listener/resource, or nonzero lane result. Every lane, including the existing
sandbox lane, must emit machine-readable results consumable by this validator.

### Installation and migration fixture

The upgrade gate starts from the immutable released v0.3.20 macOS artifact and a
checked-in legacy-state fixture whose expected content hashes are versioned. It
installs 0.4 through the normal installer, verifies deterministic
single-Machine-to-topology migration and data/context ownership, injects failure
at every boundary frozen in `config/vz-0.4-migration-barriers.json`, proves
instrumented migration receipts report every observed durable write and match
that inventory exactly with no unlisted or unexercised boundary, proves backup
restoration, and proves v0.3.20 rollback can still read its restored state. The
evidence records the
v0.3.20 source URL/tag and SHA-256, 0.4 installer/release digests, migrations,
backups, failure points, and post-upgrade/rollback hashes.

The same lane installs into a disposable prefix, creates both vz-owned runtime
state and decoy project/unrelated Docker state, runs the supported uninstaller,
and inventories before/after results. It must remove only the installed vz
software and explicitly disposable vz-owned runtime resources while preserving
the project definition/workspace, persistent data not explicitly selected for
deletion, unrelated Docker contexts/images/volumes, and every decoy byte.

All external inputs—release components, Linux kernel/initramfs/youki, macOS base,
Docker clients/plugins, registry images, toolchains, fixture repos, and allowed
network responses—are pinned by immutable digest or retained in a
content-addressed replayable fixture. Merely recording a mutable URL/response in
the manifest is insufficient.
A fresh checkout at the recorded commit plus the verified release/fixture inputs
must reproduce the gate without relying on mutable latest tags.

## Terminal definition of done

vz 0.4.0 is done only when:

- every required implementation item and observable acceptance criterion above
  is complete;
- all prerequisite and E2E commands pass against one immutable candidate digest
  tuple;
- one staged clean-provision/persisted-recovery/final-cleanup run contains every
  required scenario exactly once with zero skips, test retries, leaks,
  fallbacks, digest mismatches, or unresolved failures;
- the mechanical validator passes and its content digest, aggregate evidence
  digest, and artifact/run IDs are attached to the release/Beads gates;
- all P0/P1 0.4.0 Beads children are closed or explicitly removed from the
  release contract by a pre-candidate decision in
  `config/vz-0.4-decisions.json`;
- documentation and support labels match the verified evidence;
- capability discovery, generated/validated CLI help, docs, site, and support
  labels exactly match `config/host-target-capabilities-v0.4.json` and its
  observed evidence links;
- clean installation, v0.3.20 upgrade, injected-failure restoration, and
  rollback succeed; and
- the release commit, Beads metadata, tags, and artifacts are pushed and a fresh
  checkout can reproduce the gate.

Anything less—including passing unit tests, a single-Machine demo, guest-local
Docker commands, Docker behavior absent from the versioned compatibility
manifest, an unmeasured network claim, an optional sleep/wake result, or an E2E
run with missing evidence—means the goal remains open.
