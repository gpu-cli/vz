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

`delete` remains absent. Exec's automatic dependency reconciliation,
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
