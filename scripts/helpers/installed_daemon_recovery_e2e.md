# Installed dead-daemon recovery — scoped DEV physical gate

This harness exercises the production dead-owner socket recovery path. Both
initial physical candidates failed; neither certifies complete recovery,
live-Machine adoption, full Docker parity, or the 0.4 aggregate lifecycle.

The intended scenario uses only a fresh isolated installation and the public
five-verb CLI. Normal Up provisions two named Environments with two Developer
Linux Machines each. Exact independently labeled containers and volume sentinels
establish their Engine and persistent data identities.

Both Environments must then positively Stop. Before the deliberate crash, the
driver retains the complete correlated Stop operations, verifies all four
Machine endpoints are absent and rejects Docker access through those contexts.
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
and sentinel bytes; a changed runtime incarnation is expected. Containers may
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
journal. The production formatter uses BusyBox mke2fs and Stop directly
powers off the VM without Docker/filesystem drain. Priority-zero `vz-u0u`
tracks the required journaled formatting and durable Stop fix.

Both candidates have one completed Delete, not the required two. Their
remaining Machines positively Stopped and replacement daemons gracefully
closed; host files and Docker defaults survived. Candidate 2 remains at
`/private/tmp/vzdev-a0wbja1r`; it was not repaired, reformatted or reused.
Full persisted workload and final-cleanup evidence remains missing.
