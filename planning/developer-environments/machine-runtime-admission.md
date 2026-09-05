# Private Machine runtime admission

Status: DEV implementation; production topology Up and the aggregate release
gate remain incomplete. Tracking: `vz-mzs.2.5.8`.

## Boundary

The legacy daemon runtime is not a topology Machine runtime. Its writable image,
rootfs and setup-commit stores must never be shared implicitly by separately
owned Machines, including siblings in one Environment. The daemon owns a
separate registry of private runtime stores keyed by the complete
Project/Environment/Machine owner tuple.

The store is not a live VM. Its reservation uses
`Other("machine_runtime_store")`, a Machine owner, and logical name `runtime`.
The physical directory name is the bounded owner-derived resource ID. Paths,
project names, worktree paths and matching directory names do not authorize
adoption.

The physical VM has a separate Machine-owned `Other("runtime_vm")` reservation
with logical name `vm`. The activation API checks that supplied record against
the exact entry owner. The supervisor must validate both records in the same
current lifecycle snapshot and include both in ownership-safe deletion; neither
record is interchangeable with the logical `Machine` ownership edge.

## Admission and recovery

1. Resolve the requested host/target/profile and artifact identities before
   Machine effects. Persist the exact ownership reservation before creating its
   runtime store, and create that store before beginning its lifecycle operation.
2. Open the runtime root and children without following symlinks. The root must
   be owned by the daemon user and not writable by other users; private child
   directories and files have exact `0700` and `0600` permissions respectively.
3. Publish a complete private directory containing an owner manifest and data
   directory: write and fsync a unique staging directory, rename without
   replacement, then fsync the parent. Never adopt an incomplete or foreign
   directory. Unknown staging directories are not runtime candidates and are not
   recursively cleaned up.
4. Compare the complete owner, reservation and resolved-configuration digest.
   Acquire an exclusive lifetime lock on the final directory inode before
   constructing a Runtime: construction can reconcile persisted state.
5. Retain the backend and directory handles together. A cached admission must
   revalidate the current directory and data inode identities, not merely its
   cached manifest. No second constructor may run while the original entry is
   retained.
6. Recovery uses `ExistingOnly`. Revalidate the persisted ownership edge with
   `StateStore::require_owned_resource`; do not use a new reservation to bypass
   active-operation or Stopped-state restrictions. This read-side assertion is
   not effect authorization: the controller must independently fence its
   lifecycle operation and generation.

The backend still consumes filesystem paths. The registry now checks every
ancestor without following symlinks: ancestors must be root- or daemon-owned and
not group/world writable, except for trusted sticky directories with trusted
children. Unsafe ancestry fails before namespace creation or backend construction.
The final root retains its stricter ownership and permission checks. This is a
POSIX ownership/mode boundary, not an ACL auditor: operators must ensure ACLs or
equivalent grants do not permit another user to replace the hierarchy. The
local-Mac fixture uses canonical, trusted temporary-directory ancestry. Tracking
and aggregate verification remain under `vz-mzs.2.5.8.1`.

Even with trusted ancestry, path-based backend I/O is not safe against a hostile
process with the daemon's own host UID. Such a process must not concurrently
rename the runtime hierarchy. Guest writable shares must not expose the registry
envelope or its parent.

## Atomic boot evidence

`Runtime::boot_or_inspect_shared_vm` requires an explicit Linux profile and
verifies the selected boot artifact metadata. Reusing a boot requires its exact
ports and resource request to match. Its write lifecycle guard is downgraded
without unlocking into a non-cloneable lease containing the full runtime
identity and verified profile. Exact shutdown/replacement waits for this lease.
Docker readiness can run through that same lease.

Topology code retains a `MachineRuntimeActivation`, which owns both the VM
lease and an `Arc` to its private runtime entry. A detached backend lease alone
would leave the VM alive without retaining the registry's filesystem lock after
the registry was dropped. The composite also executes guest probes without
recursively acquiring a read lock behind a pending shutdown writer.

Each Runtime uses its own writable data and artifact-install directories.
Explicit, verified source bundles may be shared read-only. A successful
guest-local Docker health check does not prove a host endpoint, managed Docker
context, Docker/Compose/buildx conformance, or negotiated Machine capabilities.
Hardened and native macOS targets do not inherit Developer Linux Docker claims.

The focused registry lane uses a checked-in, test-only Unix HTTP probe to create
and inspect same-named real Docker volumes with distinct owner labels and
persistent contents in two Developer Machines. The probe is copied into each
private test share; it is not added to the shipped daemon binary allowlist.
This remains guest-local infrastructure evidence, not a replacement for the
unmodified host Docker CLI/Compose/buildx acceptance lane. Untracked rootfs
fixture directories remain subject to normal orphan cleanup on Runtime reopen;
the test must not disable cleanup to retain synthetic rootfs sentinels.

The lane deliberately does not publish Developer Machines as Ready: their
implicit Docker Engine, Compose and buildx capability contract needs the full
host-client conformance evidence. It proves that a POSIX-only activation is
rejected without state changes, records the unsuccessful Developer Up steps,
and verifies exact Stop and storage recovery from the failed aggregate. The
Hardened Machine may publish its actually demonstrated POSIX activation. This
negative activation check must not be bypassed by adding unproven capabilities
or weakening the production Ready guard.

Stopping after a pre-activation failure must remain possible. A stopped Machine
with no backend, incarnation, runtime identity or negotiated capabilities is
not a successfully activated Machine and advertises no capability evidence.
Ready always requires complete negotiation; a stopped Machine with any prior
activation evidence retains the normal capability and incarnation checks.
The physical lane preserves separate, checksummed serial logs for both boot
generations rather than overwriting the first boot's diagnostics.

## Explicit target resolution (DEV)

`MachineTargetResolver::resolve_project` consumes every Machine's host/target,
profile, image, version, channel, digest and requested resources before returning
an artifact plan. All sibling selections must succeed before artifact reads;
all artifact verification must succeed before a caller may reserve state. This
read-only API has no StateStore, installer, cache discovery or boot callback.

The macOS daemon accepts an explicit absolute `--machine-target-catalog` JSON
file, validated before tracing, runtime directories, database or socket creation.
Omission means an empty catalog, not discovery from environment variables or a
workspace. Schema version 1 contains a `linux` list whose entries declare
`image`, `version`, `profile`, `bundle_dir`, `digest`, and optional `channels`.
The only currently implemented image is `vz-linux-appliance`, on Apple-silicon
macOS with Linux/aarch64 targets. It is not Ubuntu or another requested distro.
Native macOS returns a typed unimplemented-backend error; future host/target
pairs are unsupported rather than mapped to this Linux adapter.

Every target requires a canonical pinned SHA-256 bundle digest. A supplied
version or channel must match an explicit catalog entry; omitted selectors are
accepted only when the remaining exact selection is unambiguous. Developer and
Hardened select the actual `developer` and `container` bundles respectively.
The read-only verifier requires exact profile, security profile, agent version,
protocol revision, required kernel capabilities and all artifact checksums. Its
aggregate identity binds kernel, initramfs, youki and raw version metadata with
the `vz.linux.kernel-bundle.v1` domain. The path-independent Machine configuration
digest additionally binds the full requested Machine specification, actual host,
backend, release, profile, artifacts and normalized resources with the
`vz.machine-configuration.v1` domain. Explicit Machine disk sizing is rejected
until the backend can honor it; it is not silently ignored.

Catalog paths and their ancestry are trusted operator inputs. Read-only
verification does not pin the source bytes against subsequent replacement.
Before production activation, the controller must materialize and retain the
verified bytes in immutable private storage, then bind the actual installed
bytes to the resolved identity. Rechecking mutable paths is not a substitute.
Kernel metadata is not negotiated Docker/Compose/buildx capability evidence,
and artifact resolution is not authorization for workspace or network effects.

## Private artifact pins (DEV)

The registry now separates `acquire_store` from `attach_runtime`.
`MachineRuntimeStoreLease` retains the exact owner manifest, trusted ancestry,
directory identities and exclusive store lock without constructing a Runtime.
The registry retains pending leases, and runtime attachment revalidates the
lease and serializes the factory so an exact store has at most one Runtime.
An `ExistingOnly` acquisition can read its persisted configuration digest rather
than needing a catalog to reconstruct it.

`pin_machine_artifacts` copies the four resolved Linux appliance artifacts into
a private pending directory and checks every copied hash. It persists canonical
resolved configuration, syncs files and directories, independently verifies the
completed bundle, and publishes `data/linux-target` with no-replace rename and
parent sync. The outer pin directory is private `0700` (Darwin requires a
writable directory for rename), its bundle directory and executable youki are
`0500`, and other files are `0400`, with exact owner, single-link regular-file
and inventory checks. Docker and its embedded BuildKit execute youki directly
from a read-only VirtioFS share, so its owner execute bit must survive pinning;
neither execute removal nor added write permission is accepted on recovery. Pins
are immutable through the API; hostile same-UID host processes remain outside
the isolation guarantee. Another successful publisher is
accepted only after its complete pin verifies against the same store identity.
Heavy copying runs separately from asynchronous verification to avoid starving
the filesystem worker pool under concurrent admissions.

`load_machine_artifacts` is the recovery path: missing or incomplete pins are
errors, not requests to install, repair or rediscover artifacts. It validates the
canonical configuration against the current exact Machine request, host,
backend, resource normalization and owner digest, then verifies pinned bytes.
It performs no channel lookup. Unknown pending directories left by a crash are
not candidates and are not automatically deleted or adopted.

The explicit `RuntimeConfig.pinned_linux_bundle` mode verifies this expected
bundle read-only and rejects legacy install/bundle paths and a separate youki
override. It never invokes the ambient kernel installer. This pins the Linux
appliance bundle; it does not yet bind the separate Developer Docker tool bundle
and BuildKit provisioning to a complete production admission plan. Nor does it
certify host Docker clients or publish negotiated capabilities.

Callers must pin every sibling before attaching any Runtime, and keep the exact
store lease alive through boot. Owner-validated deletion must account for the
read-only pin directories; changing their permissions is a deletion effect and
must occur only under the controller's lifecycle/resource fence, after the VM is
stopped. The infrastructure alone does not authorize deletion or establish the
full crash-resumable admission state machine.

## Serialized runtime admission (DEV)

`EnvironmentRuntimeController` is now daemon-owned and keys its retained async
locks by immutable Environment ID, not by a worktree, name or caller-supplied
Project/Environment pair. A different claimed Project cannot obtain a second
lock for the same Environment. The daemon rejects leases from another controller.
Different Environments can still prepare concurrently. The non-cloneable public
lease is retained by `PreparedEnvironmentMachines` through lifecycle start,
runtime attachment and the caller's subsequent effects and acknowledgements.

Preparation reloads and checks the exact persisted aggregate under the lease.
Fresh admission requires `require_environment_admission_fence`: Creating,
generation zero, no active operation, no Machine activation/legacy evidence,
and no lifecycle journal history, checked together in a read-only snapshot.
Resetting visible state cannot erase the historical prohibition on creating
missing resources. All fresh sibling targets resolve before ownership writes;
all ownership rows precede store acquisition; all pins precede any runtime
construction. Reservations remain individually transactional and resumable, not
an all-or-nothing Environment plan transaction.

Post-begin preparation validates existing ownership and opens stores/pins
read-only, using persisted Machine specifications and the explicit host tuple
without consulting a catalog. Attachment requires the prepared definition,
Project/Environment identity and next (or exact resumed) lifecycle generation,
the current Up step and both store/VM ownership records. It also rechecks every
sibling pin directory before constructing a runtime.

Cancellation retains the Environment fence inside the blocking artifact-copy
worker and its pending pin until staging cleanup completes. A cancelled async
waiter cannot let another controller overlap detached staging writes. This does
not implement request-disconnect-independent lifecycle supervision: after
admission, boot/effect tasks still need a daemon-owned supervisor and retained
activation handles.

The validated read-only lifecycle lookup by idempotency key is now public and
survives Environment deletion. The future request controller must use it before
any selection/reservation effects, verify the complete request identity and
return or resume the original journal rather than recreate a deleted name.
These runtime APIs remain trusted-library infrastructure, not an authorized
public Up API or a Developer Ready certificate.

## Still required before production Up

Wire the serialized runtime admission path into the production request/lifecycle
supervisor with the immutable artifact boundary above. The legacy shared-VM path remains separate;
its ambient artifact selection must never be used as a topology fallback.
The pinning/recovery implementation is tracked by `vz-mzs.2.5.8.2`: acquire
runtime-free owner store leases, pin every sibling, then attach runtimes.
`Runtime::new` itself performs reconciliation and cleanup, so it is not a pure
constructor that can safely precede this preflight. Recovery after lifecycle
begin must validate persisted pins without requiring the original catalog.
The per-Environment controller lock must cover admission as well as lifecycle
execution; a generation check without that serialization is insufficient.

The durable never-started fence permits completing missing resources before
first lifecycle begin, but the supervisor still needs a complete immutable
admission plan binding the request, policy receipt, workspace inputs, every
selected sibling configuration and independent Docker/BuildKit tool artifacts.
Partial pre-begin pins cannot substitute for that complete plan. A missing
directory after lifecycle begin remains an error, never permission to recreate
or silently select a replacement. Full admission/startup/response-loss recovery
must be verified before crash-resumable production Up is claimed.

The registry and boot lease also do not replace the Environment supervisor,
streaming lifecycle API, native macOS adapter, endpoint manager or five-verb CLI.
Their focused local-Mac evidence is infrastructure evidence only. Completion
still requires the complete installed-artifact aggregate gate in
[`GOAL-0.4.0.md`](GOAL-0.4.0.md), including exact cleanup of owned resources and
all crash/recovery scenarios.

The next product vertical is a server-streaming topology Up RPC with a thin
`vz up` client, not a legacy `run` alias. Its daemon-owned supervisor must hold
the selected Environment lock across admission and lifecycle, retain Machine
activations, and reconstruct response-loss results from the durable journal.
Fresh preflight resolves all siblings before reservation; existing recovery
loads exact persisted specifications and pins without a catalog. The controller
must fence both store and VM records before every effect, and may only use
CreateOrOpen in a proven never-started admission phase. Workspace binding for an
existing Stopped Environment, mutable definition reconciliation, topology policy
authorization, startup replay, and Developer endpoint/context capability proof
remain missing integration work. A Hardened-only successful Up or guest Docker
ping must not be presented as the requested Developer lifecycle.

## Verification evidence

The focused signed release-built local-Mac lane passed at
`.artifacts/sandbox-vm-e2e/20260905T033454Z/summary.txt`. Its
`machine-runtime-registry.json` and checksum bind the test, bundle and probe
identities, exact owners, separate persistent disk identities, six Docker API
observations and six retained serial logs. Earlier failed development attempts
remain at `20260905T031600Z`, `20260905T032421Z` and `20260905T032844Z`; they exposed
the probe path mismatch, the intended Developer capability refusal and the
pre-activation Stop persistence defect respectively. These are not a zero-retry
aggregate release run.

The subsequent full `scripts/run-sandbox-vm-e2e.sh --suite all --profile release`
backend regression passed at
`.artifacts/sandbox-vm-e2e/20260905T033814Z/summary.txt`: runtime, runtime
crash/reopen, StateStore crash atomicity, daemon teardown/recovery, the three-Machine
registry lane, stack and BuildKit. The registry evidence and all six serial logs
were validated again in that run. This certifies the scoped backend regression,
not production resolver/controller integration, host Docker clients, native
macOS adapter, five-verb CLI or aggregate 0.4 release gate.

The resolver-backed signed release lane passed at
`.artifacts/sandbox-vm-e2e/20260905T041000Z/summary.txt`. It resolves all three
Machines before opening StateStore, refuses four invalid sibling selectors
without state creation, and binds the actual resolver's configuration and
artifact digests to the installed private artifacts and two boot generations.
The independent validator recomputes both domain-separated digests and validates
all six raw serial logs. This run also exercises the trusted-ancestry admission
checks with real local Machines. A preceding compiler-cache wrapper failure
occurred before test execution; the successful run disabled that wrapper.

The installed-daemon catalog rejection check passed at
`.artifacts/machine-target-catalog/20260905T041411Z/installed-invalid-catalog.log`,
against the exact signed release daemon staged by that physical run. Invalid
schema, relative path, final symlink and writable-by-others catalog cases all
returned failure without creating database, runtime, socket, log, PID or lock
paths, and preserved catalog bytes and inode metadata. The raw rejection
diagnostics and executable digest are retained. This is startup rejection
evidence, not a production lifecycle command or aggregate release certification.

The subsequent full release backend regression passed at
`.artifacts/sandbox-vm-e2e/20260905T041159Z/summary.txt`: runtime 19, runtime
crash/reopen, StateStore crash atomicity, daemon teardown/recovery, registry,
stack 24 and BuildKit 3. The independent registry audit additionally matched
current source bundle bytes to first/reopened installed artifacts and verified
all six raw logs. Unit verification passed for Linux 76, daemon 195 with one
existing ignored test, and the expanded resolver suite 9; Python evidence tests
6, strict affected-target Clippy and workspace formatting passed. These scoped
results do not satisfy the missing five-verb or aggregate 0.4 release gates.

The source-independent pinning lane passed at
`.artifacts/sandbox-vm-e2e/20260905T050448Z/summary.txt`. All three Machine stores
and pins are acquired before any Runtime construction. The fixture removes only
its private source bundles and drops the resolver before first boot; recovery
opens ExistingOnly stores and validates persisted specifications and pins.
Before/after replay/recovery inode, mode, mtime, ctime and byte identities match.
The independent validator also checked exact file modes, both Developer Docker
volume owners and contents, and all six retained serial logs. This is
catalog/source-independent backend recovery, not a new daemon-process or full
production-controller recovery test. Guest mount-table text is not used as proof
of host-side write denial; host pin identities and read-only replay are what this
lane proves.

Earlier attempts at `20260905T045455Z` and diagnostic run `20260905T045959Z`
failed Docker readiness. The diagnostic log proves pinned youki mode `0400`
caused `fork/exec /mnt/linux-bin/youki: permission denied`, then embedded BuildKit
initialization failed. The correction preserves owner execute permission with
exact `0500` mode, without adding write access or choosing another runtime.
The diagnostics remain in the test and are bounded and failure-only; readiness
failures still fail the lane. These development attempts are not a zero-retry
aggregate release run.

The final release-built artifact-store crash companion passed at
`.artifacts/machine-artifact-pins/run-3Pig4o/crash-reopen.log`: ten actual SIGKILL
checkpoints spanning pending creation, each artifact/configuration sync,
directory sync, publication and parent sync. The parent requires signal 9 and
the exact checkpoint prefix, rejects panic/unwind and timeout, and validates
read-only refusal or exact published recovery without Runtime construction.
The injector parks after successful signal delivery rather than unwinding while
Darwin schedules termination. This is a filesystem-component test with synthetic
bundle contents, not a VM or installed-daemon crash gate. The copied release
driver, build output and raw log are bound by `evidence.sha256` in that directory.
Its companion library log records OCI 235 passed with four opt-in tests ignored,
and daemon 214 passed with two opt-in tests ignored; the crash companion was
explicitly run separately. A sandboxed library attempt in `run-8RlWpP` could not
create daemon test sockets; the successful run had local socket permission.
Focused pin tests passed 8/8, pinned OCI configuration 6/6, resolver 11/11,
registry 25/25 and Python evidence tests 7/7. Strict affected production/fixture
Clippy and workspace formatting passed. None of these scoped results closes the
production Developer Up or aggregate release requirements.

The first full regression attempt for this pinning change, `20260905T050642Z`,
passed runtime 19 and both runtime/StateStore crash companions but exposed a
teardown-test handoff race: its helper created `replacement-identity.json`
before writing the JSON, while its reader waited only for path existence.
`vz-mzs.2.5.9` tracks the fix: complete synced, no-replace JSON publication plus
a separately published checksum-bound ready marker. Once ready is observed,
checksum or JSON errors are terminal; readers do not retry malformed committed
data. This changes the test's process handoff, not production teardown semantics.
The failed attempt's owned helper processes were confirmed exited.

The subsequent focused teardown scenario at `20260905T051848Z` passed its real
VM/process assertions but was correctly rejected by the harness's exact test
count: the new helper unit test had been colocated in the physical target. The
shared handoff implementation now lives in `tests/support/committed_json.rs`,
with its deterministic publication/no-overwrite/malformed-commit regression in
the separate `teardown_handoff` target. Each target lists exactly one test;
the separate helper passed with zero ignored or filtered tests. The physical
suite-count gate was not weakened.

The final fresh full release backend regression passed at
`.artifacts/sandbox-vm-e2e/20260905T052420Z/summary.txt`: runtime 19, runtime
crash/reopen, StateStore crash atomicity, daemon teardown/recovery, Machine
registry, stack 24 and BuildKit 3. Every required evidence validator reported
success. The physical teardown and three-Machine targets each ran exactly one
test with zero failed, ignored or filtered tests; the separate deterministic
handoff regression remains in
`.artifacts/machine-artifact-pins/run-3Pig4o/teardown-handoff-unit.log`.
After the run, a process inventory found no remaining teardown, registry,
runtime, stack or BuildKit test processes, or `vz-runtimed` daemon. The crash
companion's six checksummed artifacts were reverified, and workspace formatting
and diff checks passed. These are scoped backend and test-protocol results;
they do not certify the installed five-command lifecycle, host Docker clients,
native macOS Machines or the aggregate 0.4 release gate.

The controller-integrated focused three-Machine gate passed at
`.artifacts/sandbox-vm-e2e/20260905T055345Z/summary.txt` (one physical test,
zero failed/ignored/filtered). Its strict evidence proves same-Environment lock
contention and different-Environment independence, exact owner attachment,
stale-generation refusal, and source-free recovery through an empty catalog.
Recovery aggregate equality and pin inode/mode/time/byte snapshots are checked;
the aggregate comparison is not a SQLite write-counter measurement. An
independent audit verified the evidence checksum, all six distinct single-link
serial logs, actual test/probe digests and removal of the fixture root.

Component regressions are retained at
`.artifacts/environment-controller/run-zKtVEb/`: the copied release driver
passed eight controller tests, the deterministic cancelled-copy exclusion and
cleanup test, and all ten filesystem SIGKILL checkpoints. These are synthetic
filesystem/controller tests, not VM/daemon crash certification. Its seven-file
`evidence.sha256` also binds build output and the complete library log: OCI 235
passed/four opt-in ignored, daemon 223 passed/two opt-in ignored, stack 941
passed/two opt-in ignored. Python evidence tests passed 7/7, strict affected
production libraries/binaries and Machine fixture Clippy passed, and workspace
formatting/diff checks passed. The full release backend regression for this
controller change is recorded below.

The final full release backend run passed at
`.artifacts/sandbox-vm-e2e/20260905T055617Z/summary.txt`: runtime 19, runtime
crash/reopen, StateStore crash atomicity, daemon teardown/recovery (102.94s),
Machine controller (18.80s), stack 24 and BuildKit 3. Every required validator
reported success. The teardown and Machine lanes each ran one physical test
with zero failed/ignored/filtered tests. The independent completed-lane audit
verified all 17 teardown boundaries, exact test and installed default-feature
daemon digests/signature, Machine controller assertions and six serial hashes,
and both actual initramfs identities. Post-run process inspection found no
matching runtime/stack/BuildKit/teardown/Machine or crash-test helpers and no
`vz-runtimed`. The component evidence manifest was reverified after completion.
These results do not close production request/lifecycle supervision, host
Docker compatibility, native macOS or the aggregate 0.4 release requirements.
