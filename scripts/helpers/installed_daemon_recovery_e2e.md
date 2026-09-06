# Installed dead-daemon recovery — scoped DEV physical gate

This harness exercises the production dead-owner socket recovery path. Both
initial physical candidates failed; neither certifies complete recovery,
live-Machine adoption, full Docker parity, or the 0.4 aggregate lifecycle.

The intended scenario uses only a fresh isolated installation and the public
five-verb CLI. Normal Up provisions two named Environments with two Developer
Linux Machines each. Exact independently labeled containers, writable-layer
markers and volume sentinels establish their Engine and persistent data identities.

Both Environments must then positively Stop. Before the deliberate crash, the
driver retains the complete correlated Stop operations and exact per-Machine
physical receipts from `data/linux-lifecycle/stops/<operation_id>.json`. Each
receipt must bind the original owner, runtime incarnation and public Stop
generation, an accounted and joined endpoint shutdown, and the exact guest
Docker shutdown request. These started Developer Machines require positively
reaped Docker/containerd processes, filesystem sync and normal unmount, plus a
clean filesystem UUID and `has_journal`/`extent` feature proof. An AlreadyAbsent
result, never-started branch or control-daemon closure cannot substitute for
these six physical Stop closures across the scenario. The driver also verifies
all four Machine endpoints are absent and rejects Docker access through those contexts.
Only after that positive boundary may it SIGKILL the exact autospawned fixture
daemon, with two matching executable/PID/start/invocation fingerprints. It never
signals a process group or an arbitrary daemon and never kills a running VM.

The dead daemon's original socket inode and ownership receipt must remain
unchanged until a normal public Delete invokes production managed autostart.
Only production code may reconcile that dead-owner socket. The driver does not
unlink sockets, modify ownership journals, write the database, retry operations,
or use legacy commands or a manual daemon launch.

Delete removes the stopped primary using exact prior positive Stop authority,
not an inferred absence or a fabricated live-runtime teardown receipt. The
neighbor must remain exactly Stopped. Its subsequent public Up must preserve
Environment/Machine IDs, context routing, Engine IDs, owned volume identities
and both writable-layer and volume sentinel bytes; a changed runtime incarnation
is expected. Its second Stop must retain the original filesystem UUID and
features. Containers may
be restarted explicitly after their exact persisted ownership is verified, so
this is not a container uptime or uninterrupted-availability claim.

Final public Stop/Delete cleans the neighbor and the exact replacement daemon
must shut down gracefully. Host project/worktree files and both daily and
isolated Docker default bytes must survive. Raw signed artifact identities,
commands, process observations, socket/recovery authority, Stop/Delete receipts
and persistent-data checks are retained and checksummed. Failure retains
uncertain operations, fixture state and ownership evidence; there is no recovery
retry or force-delete fallback.

The entry point is `scripts/run-installed-daemon-recovery-e2e.sh` and uses the
same eight explicit artifact/client arguments as
the [installed startup driver](installed_developer_startup.md). Its `--help`
path performs no daemon or Docker operations. A daemon crash with live or
uncertain Machines and a daemon crash before any Machine exists are separate
acceptance cases, not silently covered by this stopped-Machine scenario.

## Retained development candidates

`.artifacts/installed-daemon-recovery-candidate-1` failed because the shared
activation validator compared an earlier same-Environment Up with current
status. The corrected validator selects the exact lifecycle generation without
discarding history; offline and actual saved-record replay regressions pass.
The failed candidate and its 1,209 independently verified evidence files remain
at `/private/tmp/vzdev-nfs7pz1w` with the remaining neighbor positively Stopped.

`.artifacts/installed-daemon-recovery-candidate-2` passed control recovery,
primary Delete and neighbor re-Up/metadata identity checks, but the original
persisted container failed to start: Docker reported ENOTDIR at its rootfs
directory. Its 1,604 evidence files were independently verified. Read-only
inspection of the stopped retained disk found filesystem corruption and no
journal. That candidate's production formatter used BusyBox mke2fs and Stop
directly powered off the VM without Docker/filesystem drain. Priority-zero `vz-u0u`
tracks the required journaled formatting and durable Stop fix.

Both candidates have one completed Delete, not the required two. Their
remaining Machines positively Stopped and replacement daemons gracefully
closed; host files and Docker defaults survived. Candidate 2 remains at
`/private/tmp/vzdev-a0wbja1r`; it was not repaired, reformatted or reused.
Neither failed candidate supplies full persisted-workload and final-cleanup
evidence.

The strengthened P0 harness rejects the old receipt-less Stop behavior. Its
offline adversarial checks exercise missing or forged closure evidence,
non-journaled/corrupt filesystem claims, changed filesystem identities, and
missing writable-layer bytes. These checks are not physical persistence
certification. Fresh candidate 3 uses release-built artifacts; the two failed
candidates cannot be repaired, replayed or relabeled as a pass.

## Passing scoped candidate (2026-09-06, DEV)

`.artifacts/installed-daemon-recovery-candidate-3/` passes using the signed
`topology-cli-installed-ijivI3` binaries. Normal public Up created two named
Environments with two Developer Linux Machines each. Three positive Stops
retained six exact daemon/filesystem closure receipts. After exact dead-daemon
recovery, the neighbor's two original containers restarted with their original
identities, writable-layer markers, named-volume bytes, and filesystem UUIDs.
The primary is stopped then deleted, not restarted; this is not an all-four
workload-restart claim.

Both exact Deletes and graceful replacement-daemon closure completed. There
were 130 ordinary recorded commands, zero test retries, unresolved lifecycle
requests or cleanup errors, and unchanged host project/worktree bytes and
Docker defaults. The removed Machine stores and control cleanup are distinct
from the intentionally retained evidence fixture `/private/tmp/vzdev-ll9zvfwg`.

Result SHA256: `f1b181033af8741fd4df1c6403883950b19ce1823421af8cf9074bf28ee63c24`.
The 1,339-file evidence manifest SHA256 is
`7c98d1c78d60f916652eba9c4a47066c07410e0a1769b89a1a70248261addca6`.

This proves persisted recovery after positive Stop, not adoption of active
Machines after a crash, automatic repair of unclean filesystems, full Docker
compatibility, or aggregate 0.4 certification. `vz-u0u` and `vz-ehz` remain open.
