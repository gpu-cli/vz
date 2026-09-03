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
native macOS Machines in the same Environment. Every Linux Machine implicitly
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
    ├── MachineInstance[] (each has its own target OS and capabilities)
    ├── Network[] and declared service paths
    ├── Endpoint[] including environment-local public-like ingress
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

1. Versioned ProjectDefinition, WorkspaceBinding, EnvironmentInstance,
   MachineInstance, Network, Endpoint, and ownership schemas with migration from
   legacy records. Immutable IDs, human selectors, and machine incarnations are
   distinct. Persistent identity never depends on raw host path spelling.
2. Idempotent reconcile, start, stop, inspect, exec, and delete behavior for a
   complete Environment topology. Stop preserves identity and declared state;
   delete traverses only the selected Environment's ownership graph.
3. Stream-first gRPC and equivalent Rust/API contracts for Environment watch and
   reconciliation, Machine lifecycle and exec, events/logs, files, endpoints,
   fault injection, snapshots, receipts, and capability discovery. Short,
   bounded reads may remain unary.
4. Deterministic selection by explicit immutable ID/name, process-scoped
   selector, or unambiguous project/worktree binding. There is no mutable global
   current Environment. Ambiguity fails closed and reports candidates.
5. Explicit workspace projections per Machine (`read-write`, `read-only`, or
   `snapshot`) and explicit ownership/consistency rules for volumes and shared
   caches. A writable block device is never silently attached to two Machines.
6. An Environment-owned network fabric with isolated routing and DNS views,
   declared private links, environment-local public-like ingress, firewall/NAT,
   TLS, controlled egress, explicit host imports/exports, collision-safe ports,
   and deterministic latency/loss/bandwidth/partition/DNS fault controls.
7. A private Docker capability on every Linux Machine. Each Machine has a unique
   daemon, state, BuildKit cache, network namespace, endpoint, and Docker context.
   Native macOS Machines do not acquire Docker or a hidden Linux sidecar.
8. Pinned and verified artifacts, with youki as the only OCI runtime binary
   present or executable in every Linux Machine. Runtime overrides and
   runc/crun fallback fail closed.
9. Release packaging, migration, help, examples, skills, website, status output,
   and documentation that expose this one model and accurately label ACTIVE,
   DEV, and PLANNED host×Machine-target capabilities.

## Observable acceptance criteria

The release is acceptable only when every assertion below is demonstrated by a
required E2E scenario and retained evidence:

1. **Multiple instances:** two worktrees each create an Environment, and one of
   those worktrees creates a second named Environment. All three run
   concurrently with repeated Machine names, service names, internal ports, DNS
   aliases, and overlapping guest CIDRs without collision or cross-observation.
2. **Multiple Machines:** one Environment runs at least two Linux Machines and
   one native macOS Machine. `vz status --json` reports stable Environment and
   Machine IDs, target-qualified capabilities, topology, health, endpoints, and
   Docker contexts. Ambiguous `vz exec` without a default Machine fails closed.
3. **Machine-scoped Docker:** both Linux Machines are independently driven by
   the Mac's unmodified `docker`, Compose, and buildx clients. Containers,
   images, volumes, networks, events, caches, and lifecycle operations in one
   context are absent from the other and from every other Environment.
4. **Target-native execution:** Linux commands execute on Linux. The checked-in
   native fixture `tests/fixtures/vz-0.4/native-macos-swift/` builds and tests
   with the exact Xcode/Swift toolchain digest pinned by the release contract,
   then its release executable prints the expected fixture protocol/version.
   Unsupported target/capability requests fail without substitution.
5. **Private topology:** a client, API, and database communicate only over their
   declared paths. The client cannot directly reach the database, and an
   undeclared Machine-to-Machine path is denied.
6. **Public-like topology:** a client reaches an API through environment-local
   split DNS, a `.test` hostname, TLS, routed ingress, firewall, and NAT. Evidence
   proves the path crossed the virtual edge rather than localhost or a private
   shortcut. Nothing listens on the host LAN or public Internet.
7. **Host boundaries:** host imports and exports are absent by default. Explicit
   loopback exports work without collisions across Environment instances; an
   undeclared guest-to-host service and LAN access are denied.
8. **Cross-Environment isolation and peering:** the three Environments cannot
   resolve, route to, read, control, or receive events from one another. One
   directional endpoint peer grant permits only its declared protocol/port,
   denies reverse and transitive access, then expires or revokes cleanly.
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
    workers against different Machines in one Environment using a fixed schedule
    and synchronization barriers. Workspace writer policy is enforced and every
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
    failure restores the backup and leaves v0.3.20 rollback usable.
20. **Exhaustive network denial:** a machine-readable
    source×destination×protocol×port matrix records expected and observed results
    for private, public-like, host, Internet, and peered paths. Any unexpected
    success fails the gate.
21. **CLI removal:** a checked-in help snapshot lists the five lifecycle verbs.
    Every removed command name is invoked; each returns the specified nonzero
    exit class and structured migration message, proving it is not an executable
    hidden alias.

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

### Release candidate and entry point

The repository must ship `scripts/build-vz-0.4-release-candidate.sh`, which builds
with the lockfile, assembles all host/guest/kernel/runtime artifacts, records
their SHA-256 digests, applies the required local test signature/entitlements,
and verifies them with `codesign --verify --strict`. GA additionally requires
the Developer ID-signed and notarized distribution built by the release workflow
to have the same source commit and component digests. Harnesses invoke the
installed release directory supplied by `--release-dir`; `cargo run`, debug
binaries, and PATH fallbacks are rejected.

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
   signed state-handoff manifest without cleanup;
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
and its samples are recorded. A fresh top-level invocation may start a new run
ID, but cannot replace or hide a failed release result.

Host sleep/wake is a mandatory hardware scenario, not “where possible.” The
harness writes a pre-sleep checkpoint, instructs the operator/runner to perform
one real sleep/wake cycle, and resumes only with the matching run ID and recorded
sleep/wake timestamps. Automated CI incapable of host sleep cannot certify the
release; the local-Mac evidence must contain the hardware phase.

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
at each declared migration commit barrier, proves backup restoration, and proves
v0.3.20 rollback can still read its restored state. The evidence records the
v0.3.20 source URL/tag and SHA-256, 0.4 installer/release digests, migrations,
backups, failure points, and post-upgrade/rollback hashes.

All external inputs—release components, Linux kernel/initramfs/youki, macOS base,
Docker clients/plugins, registry images, toolchains, fixture repos, and allowed
network responses—are pinned by immutable digest or captured in the manifest.
A fresh checkout at the recorded commit plus the verified release/fixture inputs
must reproduce the gate without relying on mutable latest tags.

## Terminal definition of done

vz 0.4.0 is done only when:

- every required implementation item and observable acceptance criterion above
  is complete;
- all prerequisite and E2E commands pass against one release-candidate digest;
- one staged clean-provision/persisted-recovery/final-cleanup run contains every
  required scenario exactly once with zero skips, test retries, leaks,
  fallbacks, digest mismatches, or unresolved failures;
- the evidence is reviewed and attached to the release/Beads gates;
- all P0/P1 0.4.0 Beads children are closed or explicitly removed from the
  release contract by a recorded product decision;
- documentation and support labels match the verified evidence;
- clean installation, v0.3.20 upgrade, injected-failure restoration, and
  rollback succeed; and
- the release commit, Beads metadata, tags, and artifacts are pushed and a fresh
  checkout can reproduce the gate.

Anything less—including passing unit tests, a single-Machine demo, guest-local
Docker commands, Docker behavior absent from the versioned compatibility
manifest, an unmeasured network claim, an optional sleep/wake result, or an E2E
run with missing evidence—means the goal remains open.
