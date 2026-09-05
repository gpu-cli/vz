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

## Still required before production Up

Wire the explicit resolver into a production Environment controller with the
immutable artifact boundary above. The legacy shared-VM path remains separate;
its ambient artifact selection must never be used as a topology fallback.
The pinning/recovery implementation is tracked by `vz-mzs.2.5.8.2`: acquire
runtime-free owner store leases, pin every sibling, then attach runtimes.
`Runtime::new` itself performs reconciliation and cleanup, so it is not a pure
constructor that can safely precede this preflight. Recovery after lifecycle
begin must validate persisted pins without requiring the original catalog.
The per-Environment controller lock must cover admission as well as lifecycle
execution; a generation check without that serialization is insufficient.

The supervisor also needs a proven durable admission phase for a crash between
the ownership reservation and first directory publication. This may use a phase
marker or an audited equivalent that proves no lifecycle has ever begun, rather
than introducing new state unnecessarily. A missing directory must not be
treated as an ordinary `ExistingOnly` recovery or silently recreated after
lifecycle effects have begun. This policy and its exact operation/generation
checks must land before crash-resumable production Up is claimed.

The registry and boot lease also do not replace the Environment supervisor,
streaming lifecycle API, native macOS adapter, endpoint manager or five-verb CLI.
Their focused local-Mac evidence is infrastructure evidence only. Completion
still requires the complete installed-artifact aggregate gate in
[`GOAL-0.4.0.md`](GOAL-0.4.0.md), including exact cleanup of owned resources and
all crash/recovery scenarios.

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
