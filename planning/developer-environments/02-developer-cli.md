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

Environment selection uses strict, non-fallback precedence: an explicit
`--environment` immutable ID/name, then `VZ_ENVIRONMENT_ID`, then the current
workspace binding. `VZ_ENVIRONMENT_ID` is process-scoped and accepts an
immutable Environment ID only. If an explicit or process selector is present
but invalid or stale, the command fails at that level instead of consulting a
workspace binding. A worktree may bind more than one named Environment;
ambiguity fails with a bounded candidate list.

Within the selected Environment, Machine selection uses explicit `--machine`
ID/name, then the process-scoped `VZ_MACHINE_ID`, then the declared default or
sole Machine. `VZ_MACHINE_ID` accepts an immutable Machine ID only and is
ownership-checked against the selected Environment. A present invalid, stale,
or foreign value fails at the process tier without falling through. There is no
mutable global current Environment, Machine, socket, or Docker context.

The optional `environment.default_machine` field names an exact declared Machine
in `vz.json`; it is not an immutable runtime ID. It participates in the definition
digest and survives typed API round-trips. Missing or null means no declared
default. A present unknown name is invalid, and a stale explicit/process selector
never falls through to it. With no default, the sole Machine may be selected;
several Machines require an explicit selection.

## Step 3: Discover definitions and create instances deterministically

The checked-in portable project definition remains `vz.json` for 0.4 and is
validated against `schemas/vz-project-definition-v1.schema.json`; the minimal
bootstrap fixture is `examples/developer-environment/vz.json`. Commands search from the
working directory toward the filesystem root and select the nearest `vz.json`;
multiple candidates at the same selection level or an invalid definition fail
before mutation. The file carries a stable `project_id`, so moving or cloning a
worktree does not change project identity.

Each checkout/worktree gets a random opaque workspace token, persisted at
`<resolved-per-worktree-git-dir>/vz/workspace-id`. Moving that worktree
preserves its token and bindings; creating another worktree or clone creates a
new token. The token, not the native path, is the workspace binding key. A raw
path or stored `path_hint` is optional, refreshable diagnostic context only: it
cannot authorize lookup, establish identity, or cause selection or adoption of an
Environment.

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
- Explicit, process, and workspace precedence tests prove that a present stale
  selector fails without falling through; `VZ_ENVIRONMENT_ID` accepts IDs but
  not names. `VZ_MACHINE_ID` accepts only an ID owned by the selected
  Environment and likewise fails without fallback when present but invalid,
  stale, or foreign.
- Nearest-definition discovery, missing/invalid/ambiguous definitions, first
  `default` creation, explicit named instance creation, sole selection, and
  multi-instance ambiguity all pass without path-derived identity.
- Worktree relocation preserves the opaque workspace token and binding; a new
  clone/worktree gets a different token, and matching `path_hint` values never
  select or adopt an Environment.
- Bare `vz` exactly matches the zero-exit help snapshot and performs no reads
  beyond static help generation; old verbs reject with migration guidance.
- A clean directory proves missing-definition failure and zero mutation, then
  bootstraps only through the published example/schema or typed authoring API
  before `vz up` creates the first `default` instance.
- Two simultaneous Linux Machines return distinct Docker contexts and preserve
  the user's default context.
- Real local-Mac black-box tests cover all five verbs against multi-instance,
  multi-Machine, and mixed Linux/macOS topologies.

## Implementation evidence (DEV, not release acceptance)

The authoring schema and example now exist at the paths above. The production
definition loader rejects unknown fields at every desired-object boundary;
schema and semantic validation remain distinct. The example deliberately does
not invent a published appliance digest. Installed authoring-resource packaging
and real `up` bootstrap are still required.

`stop` now dispatches to a selected-Environment streamed RPC, not the old direct
VM command. The daemon authorizes exact topology ownership, retains the Stop
task independently of client observation, and preserves request/idempotency
identity for replay. Its current effect adapter is limited to registered
Linux/ARM64 sessions; unknown state after restart and unsupported native or
additional topology resources fail closed.

The staged, ad-hoc-signed release CLI and external `macos-vz` daemon passed
14 local-Mac black-box checks at
`.artifacts/topology-stop-installed-L0YbgC/`: Stop 3, status 4, bare help 2,
and existing legacy-removal regressions 5. `evidence.sha256` binds executables,
test drivers, and raw logs. Stop tests use seeded stopped/failed topology and
prove control-plane selection, replay, sibling preservation, and unknown-state
refusal; they do **not** prove physical Machine shutdown. This does not replace
the daily installed binary or certify all five verbs.

`exec` now has a real bidirectional CLI/client/daemon path for an already Ready,
exactly owned Linux Machine. Durable admission prevents duplicate dispatch after
restart; stream events and controls bind request, Environment, Machine, and
incarnation. A retained supervisor handles stdin EOF, output, signals, PTY resize,
deadlines, and cancellation. Stop closes execution admission and requires
positive reaping before releasing the original Machine. Uncertain work retains
ownership; a no-live-work result without an exit status is labelled `Quiesced`,
not falsely reported as never started. This is a DEV implementation slice, not
physical CLI acceptance or permission to promote Developer readiness from Engine
startup alone.

The subsequent staged, ad-hoc-signed CLI/daemon run at
`.artifacts/topology-cli-installed-r93vAW/` passed 16 checks: the preceding 14
plus two Exec checks over a real local socket. Exec coverage proves exact
terminal-receipt replay (including a nonzero command exit) and unknown-runtime
refusal, not live command execution. Its checksums bind the copied executables,
test drivers, and raw logs. The daily installation remains unchanged.

`delete` now has a streamed CLI/client/daemon DEV adapter for exactly owned
Linux-on-macOS Machines. It preflights the complete supported ownership graph,
requires positive quiescence, removes exact managed contexts and private stores,
and binds successful replay to the original Environment tombstone even after
name reuse. Native targets and other topology resource cleanup adapters fail
closed. The Delete bead remains open pending physical and aggregate evidence.
Exec's automatic dependency reconciliation,
physical declared-default Machine coverage, and native adapter remain required, as do
full live status/contexts and the complete installed mixed-target lifecycle gate.

All 16 retired roots, including hidden `debug`, have now been removed from the
actual parser and dispatch. Legacy command modules are no longer compiled into
the CLI. `config/cli-removal-v0.4.json` records a signed DEV baseline inventory
and the still-missing immutable v0.3.20 release traversal. Tests cover every
inventoried nested path, static help, structured rejection, and zero contacts
to real HTTP/Unix listeners. The old API-mode CLI-success tests were replaced
with retirement assertions; this does not certify their typed-API replacements.
Remaining API coverage and stale harness consumers are explicitly inventoried
in `crates/vz-cli/tests/fixtures/retired-api-cli-coverage.md`.

The Up prerequisite now reserves exact prospective ownership and a non-expiring
idempotency receipt in one transaction, after authorization. Seven tests cover
denied-creation rollback, retry/reopen identity, sibling preservation, and
durable workspace-binding truth. A subsequent DEV `vz up` now connects the CLI,
typed client, streaming RPC and retained original-Machine startup supervisor.
Exact non-dispatch proofs let Stop account for failed siblings that never booted;
consumption precedes boot and a crash after consumption remains uncertain.
Developer boot/private Engine endpoint is not full readiness: missing host
Docker/Compose/buildx conformance and managed-context evidence yields failure,
with original live ownership retained for Stop. Hardened readiness measures its
exact guest POSIX execution. A configured verified catalog is required; automatic
installed-catalog discovery, full reconciliation, native/topology adapters and
physical public lifecycle acceptance remain open. Three CLI/private-socket tests
at `.artifacts/up-host-St0qWY/up-cli-uds.log` prove admission/replay/refusal, not VM
boot or Developer readiness.

The post-retirement signed CLI/daemon run at
`.artifacts/topology-cli-installed-9JFtJP/` passed 19 checks: Exec 2, Stop 3,
status 4, bare help 2, retirement 6, and no-contact rejection over both transport
modes 2. Its checksum manifest binds executables, test drivers, and raw logs.
These are installed-artifact control-plane and retirement checks, including
seeded topology and terminal receipts, not live execution or the complete
five-verb lifecycle. The daily installation remains unchanged.

The repeatable DEV installed-artifact control-plane runner is
`bash scripts/run-installed-topology-cli-tests.sh`. It stages signed release
CLI/daemon files, selects exact Cargo-built test drivers, rejects empty,
ignored or filtered test runs, and retains checksummed build/test/daemon logs.
It is not the physical Developer Environment or aggregate release-gate entry
point and does not replace the daily installation.

The runner's current signed CLI/external-daemon run at
`.artifacts/topology-cli-installed-YCnECh/` passed 23 checks: Up 3, Exec 3,
Stop 3, status 4, bare help 2, retirement 6, and transport rejection 2.
The checksum manifest was reverified. This adds streamed Up admission and
declared-default Machine selection evidence, still at the control-plane
boundary; it does not prove physical Developer readiness or Docker parity.

The next signed candidate at `.artifacts/topology-cli-installed-7zd8qA/` also
passed all 23 checks, including a regression for raw Exec output: fresh commands
no longer prepend receipt guidance to stderr. JSON mode retains structured
identity reporting; replay still discloses unavailable historical output.

The checked-in [physical runner](../../docs/topology-up-machine-e2e.md) passed
on the local Mac at `.artifacts/topology-up-physical-backing-store/`, using those
exact signed CLI/daemon files. Public Up booted two Developer Linux Machines;
the Mac's existing Docker client reached distinct Engines at their private
endpoints. Public Hardened Up/Exec/Stop/re-Up proved Linux execution, exact
stdout/stderr/exit 23, stable Environment/Machine IDs and a sentinel persisted
on the positively identified `/vz-rootfs` VirtioFS backing share. Six host
Engine probes proved the same two Engines remained usable across the neighboring
Hardened Stop/re-Up. Final Stop removed their sockets, both Environments were
stopped, and the daemon exited zero. The nested checksum manifest was verified.

Two preceding failures remain retained: `topology-up-physical-d8zdOX` exposed
the raw stderr banner; `topology-up-physical-raw-streams` exposed the fixture's
incorrect pre-switch-root sentinel path. Both performed successful exact Stop
cleanup. The passing candidate includes the corresponding source/harness fixes;
it is not a test-case retry or a complete release-gate run.

Developer Ready remains unsupported in this candidate. The next implementation
must package/discover a trusted installed catalog, manage exact Machine-owned
Docker contexts, and measure bounded Engine/Compose/buildx operation through
those contexts with digest-bound offline probe artifacts. Full release-wide
compatibility certification is separate: users must not run the entire release
suite on each Up. Native topology, full reconciliation, declared workspace/volume
semantics, interrupted partial startup, and Delete remain required; the backing
share sentinel does not certify those features.

The subsequent required backend regression run
`.artifacts/sandbox-vm-e2e/20260905T144809Z/summary.txt` passed every selected
release-profile runtime, recovery, runtimed, Machine registry, stack and BuildKit
stage. Its pinned runtime inventory and logs remain backend evidence, not the
missing host-Docker compatibility or mixed-target aggregate release gate.

### Installed Developer operational startup checkpoint

The subsequent candidate in `.artifacts/installed-developer-startup-candidate-2/`
passed normal installed `vz up` autostart/catalog discovery on the local Mac.
It created two named Environments in one worktree, each with two Developer Linux
Machines and distinct managed Docker contexts. Host Engine, Compose and uncached
buildx execution measured readiness; status returned exact persisted descriptors.
Stop/Up preserved context/Machine/Engine identity and advanced incarnations while
the neighboring Environment remained usable. A sequential Hardened Machine
provided POSIX execution without Docker. Final positive Stop and graceful daemon
cleanup preserved both Docker defaults. All 1,124 evidence hashes were verified.

This supersedes the earlier Engine-only/missing-context readiness boundary for
this DEV path. It does not complete Delete, native macOS lifecycle, declared
workspace/network/volume/secret behavior, mutating-failure recovery or the full
Docker/mixed-target release gate. See
[operational readiness and retained failures](../../docs/developer-startup-readiness.md).

The subsequent full backend regression gate passed at
`.artifacts/sandbox-vm-e2e/20260905T183551Z/summary.txt`, including runtime,
generation recovery, daemon teardown, Machine registry, stack and BuildKit.
Crash-fixture socket ownership and digest-declared probe propagation corrections
are verified; their preceding failed candidates remain retained. Public daemon
crash recovery (`vz-ehz`) is still open and is not certified by fixture cleanup.

### Streamed Delete checkpoint (2026-09-05, DEV)

All five lifecycle verbs now have parser and typed daemon paths. The current
Delete adapter preflights every supported ownership edge before admission,
quiesces the original Linux/ARM64 Machines, retires exact managed contexts and
private stores, and atomically records the Environment tombstone. It retains
supervision across observer disconnects and checks the original selector,
timeout, request, generation and ownership on replay. Reusing a human name
cannot retarget an old completed request. Unknown live ownership, native targets
and unsupported resource graphs fail closed; there is no force-delete fallback.

The signed installed control-plane gate passed 31 checks at
`.artifacts/topology-cli-installed-IiMGsJ/`, including eight Delete checks and
the complete retired-parser inventory. Its checksummed CLI and daemon are
`c3843d815eee0259c273bda4ee2a594add6109af80f14afa995b7e59b2d49c01`
and `ec8539501786aaa5be6d03444e45872f3bc3bed701d1c1ca5d0756745561f23d`.
The actual Mac Docker client also passed isolated owned-context deletion,
foreign-context/default preservation and zero Engine contact at
`.artifacts/topology-delete-n6euqZ/actual-host-context-delete-2.log`.
Neither test claims physical VM deletion. Preceding fixture/inventory failures
remain recorded under the same wave's artifact directory.

The separate [installed Delete driver](../../scripts/helpers/installed_delete_e2e.md)
requires fresh installed public Up/Ready Delete, name-reuse replay, Stopped
Delete, neighboring Machine liveness, exact cleanup receipts and graceful daemon
shutdown. Broader crash/recovery and mixed-target aggregate conformance remain
required before the Delete task or 0.4 release can close.

That driver's first physical candidate passed on the local Mac at
`.artifacts/installed-delete-candidate-1/`, using the signed files above and
normal public Up autostart. Two named Environments each ran two Developer Linux
Machines. Ready Delete removed the original primary; recreating its name
allocated new identities, and replaying the original request returned only its
original tombstone. Public Stop/Delete removed the replacement and then the
neighbor. All six distinct Machine stores, managed contexts and endpoint sockets
were removed, with three exact tombstones and original-runtime quiescence
receipts. There were zero test-case retries or cleanup errors. Both Docker
defaults and host project/worktree sentinel bytes survived; the exact daemon
shut down gracefully. The 128 background neighbor observations and explicit
bracketing probes establish sampled liveness/non-restart, not uninterrupted
packet-level availability.

The result SHA256 is
`814e47f71bfcc20699a98b46ba42d9f0c6c2a0e95003e107355a4a40b66aca62`;
the evidence manifest SHA256 is
`259d3f78ec39702f2770e46cd1f8887662ccb94dbf6e4ebaa7074d4a57f01888`.
Independent replay verified all 3,360 evidence hashes, 687 bounded command
receipts, six outside-store deletion proofs and all three persisted tombstones.
The isolated host fixture, database and outside-store journals remain at
`/private/tmp/vzdev-vjoll2ts`; the daily installation was not replaced.

The same wave's full release-profile backend gate passed all seven lanes (50
selected tests, zero failed or ignored) at
`.artifacts/sandbox-vm-e2e/20260905T224239Z/summary.txt`, including the youki-only
inventory and 18 positive exact-child/socket teardown receipts. Its complete
`raw-evidence.sha256` manifest is
`7d66b7feb89bfd6c472569b62dcae559590a6246c203113539724584c4d58bc1`.
All 4,062 entries verified; raw logs, summary and socket receipts are included.
Workspace check, formatting, strict production Clippy, protocol tests and the
offline driver validators passed. Strict all-target Clippy remains red on
pre-existing contract-test lints tracked in `vz-1ff`.

These are scoped DEV passes, not full Docker compatibility or 0.4 certification.
`vz-mzs.3.1.3` stays open: physical crash/ack-loss tests, foreign path/context
replacement scenarios, other ownership adapters and the applicable aggregate
Environment gate still require evidence. Observer disconnect is covered by the
subsequent checkpoint below. Native macOS and future host/target combinations
need their own conformance runs.

### Delete observer-disconnect checkpoint (2026-09-05, DEV)

The [separate installed disconnect driver](../../scripts/helpers/installed_delete_disconnect_e2e.md)
passed its first local-Mac candidate at
`.artifacts/installed-delete-disconnect-candidate-1/`. It used the same signed
CLI/daemon and guest artifacts as the preceding Delete wave; this checkpoint
adds harness/evidence coverage, not a runtime binary change. Forty offline
Delete/reader/quiescence tests passed before the physical run.

Normal public Up created two named Environments with two Developer Linux
Machines each. The harness observed the primary's admitted Delete, sent SIGTERM
only to its exact unreaped CLI child (PID 41905), and observed exit -15. A fresh
read-only live-WAL transaction started after reap still found that exact
operation Running with its Environment Deleting and its original active
generation. Five subsequent read-only samples reached the exact completed
tombstone; its positive record was fsynced before any replay. The sole replay
returned that already-completed operation, not newly initiated cleanup.

Both Environments were positively deleted, all four private Machine stores,
contexts and endpoint sockets were removed, and exact original-runtime
quiescence receipts were validated. Four background neighbor samples (two per
Machine), plus explicit bracketing probes, passed; this remains sampled
liveness/non-restart, not continuous availability. Host project/worktree bytes
and Docker defaults survived, the exact daemon shut down gracefully, and there
were zero test-case retries, unresolved requests or cleanup errors.

Result SHA256:
`9a9b7fb303ff1f92fe4b792792a40d781ae33cc8645cf7028447484036985bb3`.
Evidence manifest SHA256:
`a72adc3514e2d25332066be47e09c1331c09f4b520cc0151b9f49542028d41df`.
Independent audit verified all 901 evidence hashes, 119 ordinary command
receipts, the separately captured interrupted observer, four deletion proofs
and both persisted tombstones. Autonomous success was observed 436 ms after
observer reap and 80 ms before replay dispatch. Each neighbor had a successful
background observation overlapping that interval.
Host fixture, database and outside-store journals remain at
`/private/tmp/vzdev-w8krvyvo`; the daily installation remains unchanged.

This proves observer-disconnect continuation only. Daemon/adapter crash recovery
(`vz-ehz`), physical acknowledgement-loss and adversarial replacement cases,
mixed-target topology, full Docker compatibility and aggregate release
certification remain open. In particular, a stale control socket or missing
live-session handle is still not authoritative permission to recover/delete a
Machine.

### Completed-owner daemon recovery implementation (2026-09-05, DEV)

The macOS daemon now admits exact persistent control ownership before database
migration and diagnostic writes. Persistent database and socket-path locks,
native process birth observations, pinned file/directory identities and durable
owner records replace the previous memory-only socket guard on this host.
Public Up may recover that owner; Delete may start a replacement only for an
existing database with prior owner discovery. Neither client removes sockets
or signals an existing daemon. The five public verbs remain unchanged.

See [control recovery and its limits](../../docs/daemon-control-recovery.md).
Control recovery does not establish Machine quiescence: positive prior Stop
authority and exact lifecycle ownership remain required. Incomplete preparation,
interrupted closed-record publication, active/uncertain Machine recovery and
the full `vz-ehz` acceptance matrix remain open. The installed physical candidate
must pass independently; unit tests do not certify this capability.

The fresh release-profile backend gate passed at
`.artifacts/sandbox-vm-e2e/20260906T001521Z/summary.txt`: seven lanes and 50
selected tests, zero selected failures or ignored tests. It includes all 17
teardown interruption boundaries and 18 exact-child fixture socket cleanup
receipts. That fixture cleanup is not production socket-recovery evidence.
The installed daemon has no test features; the runtime inventory contains only
youki 0.7.0. Unit/regression checks additionally passed 378 daemon tests (three
existing backend-specific ignores), 57 client tests, strict production Clippy,
workspace compilation and all 31 installed CLI checks.

The backend raw manifest binds all 4,062 retained files, independently verified:
`1f2cbcf60f8490836f979a76d82e60559b88760f69e6e00b53f590bf2a15a01b`.

The separate [installed recovery candidates](../../scripts/helpers/installed_daemon_recovery_e2e.md)
both failed and remain retained. Candidate 1 exposed a fixed historical
activation-validator bug. Candidate 2 recovered the exact dead daemon, deleted
the stopped primary and restarted the neighbor with preserved metadata, but
Docker could not start its original container. Read-only disk inspection found
an unjournaled filesystem with corrupted inode references. The existing Stop
path powers off the VM without Docker/containerd/filesystem drain; `vz-u0u`
tracks the required durability fix. Both candidates positively Stopped their
remaining Machines and gracefully closed the replacement daemon, without
repairing or deleting the retained neighbor data. These partial observations
do not certify workload persistence, complete crash recovery or 0.4.

### Journaled Docker Stop correction (2026-09-06, DEV)

`vz-u0u` now implements pinned static journal-capable formatting, clean ext4
admission, streaming guest daemon/filesystem closure, and exact private durable
Stop receipts. Forwarding drains before daemon termination; positive closure
precedes VM power-off and lifecycle acknowledgement. Unclean/legacy disks and
incomplete bootstrap ownership remain preserved and refused, not silently
reformatted or repaired. Automatic recovery of those cases is still unfinished.

Two backend candidates exposed and drove integration fixes. The first,
`20260906T010254Z`, lacked formatter tools inside the guest-agent chroot. The
second, `20260906T013530Z`, passed runtime and generation/teardown lanes but
failed Machine reopen because receipt publication changed the immutable
artifact pin inventory. Tools and provenance now cross the rootfs boundary
with integrity checks; lifecycle receipts live separately under
`data/linux-lifecycle/stops`. Strict artifact inventory validation is unchanged.
Both failed runs and their exact guest bundles are retained under
`.artifacts/sandbox-vm-e2e/`; their raw manifests bind 48 and 86 files,
respectively. Neither is a passing backend or installed persistence gate.

Candidate `20260906T015039Z` then passed the physical registry test, but its
evidence checker rejected the added Docker shutdown field. Its 90-file raw
manifest and exact bundles are retained as another failed full run. The
checker now requires correlated daemon and clean journaled-filesystem closure,
distinct Developer filesystem identities, and no Docker proof for Hardened;
75 registry/recovery/Delete validator tests pass. Read-only validation of saved
evidence does not relabel the failed run or supply its unrun downstream lanes.

The final source checks passed 382 daemon tests (three existing platform-specific
ignores), 141 guest tests, 88 Linux client tests, workspace compilation and
strict production host/Linux Clippy. The signed installed CLI candidate
`.artifacts/topology-cli-installed-ijivI3/` passes all 31 control-plane and
retired-command checks, with independently verified signatures and evidence.
These checks do not replace fresh physical workload-persistence evidence, the
complete host Docker/Compose/buildx matrix, or the aggregate release gate.

Installed recovery candidate 3 now passes that scoped physical scenario:
`.artifacts/installed-daemon-recovery-candidate-3/`. All four Machines completed
positive Stop; the neighbor's two original containers restarted after exact
daemon recovery with their original writable-layer and named-volume bytes.
Six correlated filesystem closures and two exact Deletes completed with no
test retries or cleanup errors. The primary was deleted without restart;
active-Machine crash adoption and automatic unclean-disk recovery are not
certified. See the [candidate evidence and limits](../../scripts/helpers/installed_daemon_recovery_e2e.md#passing-scoped-candidate-2026-09-06-dev).

The fresh full local-Mac backend candidate
`.artifacts/sandbox-vm-e2e/20260906T020915Z/summary.txt` also passes: seven
lanes, 50 selected tests, zero selected failures or ignored tests. It includes
the stricter registry evidence validation, stack lifecycle and BuildKit lanes.
Its signed production daemon matches the installed recovery candidate's bytes.
This closes neither the full 63-case host Docker matrix nor the aggregate 0.4
gate; `vz-u0u` and `vz-ehz` retain their unfinished recovery requirements.
