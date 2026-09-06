# Daemon control recovery (macOS, DEV)

This documents the implemented **completed-owner** control-socket recovery
path on macOS. It is not physical recovery certification or completion of
Beads issue `vz-ehz`. Other hosts are not covered by this implementation claim.
The [Developer Environment contract](developer-environments.md) and
[0.4 release gate](../planning/developer-environments/GOAL-0.4.0.md) remain
the product and acceptance boundaries.

## Public entry points

Normal `vz up` permits managed startup using the installed sibling daemon and
verified installed Machine catalog, unless trusted configuration explicitly
selects another catalog. An existing compatible daemon is used unchanged.

`vz delete` permits managed recovery only when the configured state database
and a prior control-owner record exist. It does not bootstrap a fresh database
or infer Environment ownership from a socket or PID filename. The client checks
discovery prerequisites; the daemon independently admits the exact ownership.
Status, Stop and Exec remain existing-daemon connections, not recovery aliases.

A failed connection is not permission to remove a pathname. Clients never
unlink stale sockets, kill a version/protocol-mismatched daemon, or redirect to
another daemon. There is no extra public recovery/debug verb or legacy fallback.

## Admission and lifetime

The daemon retains two independent exclusive advisory locks: one for the exact
state database and one for the exact socket name. Lock files are persistent,
single-link, owned mode-0600 regular files. Releasing a lock never unlinks its
pathname; old openers therefore cannot acquire a detached inode while a new
daemon locks another file at the same name.

Admission pins directory and file identities, rejects redirected or replaced
authority, and checks the database/socket/configuration mapping before opening
the state store for migration or writing daemon diagnostics. Legacy database
read permissions may remain; ownership credentials retain stricter permissions.
The control owner holds both fences through runtime/session destruction and
exact socket/PID cleanup. These checks trust the daemon's effective UID; they
do not claim an atomic namespace CAS against a malicious same-UID renamer.

Each owner records native process birth: PID, UID, start seconds/microseconds
and boot-session UUID. Without an exact graceful-close receipt, recovery must
observe that the original birth is absent, replaced, or a zombie. A live exact
birth or failed/partial native lookup rejects recovery. No recovery path signals
the observed process.

## Durable control records

For socket path `<socket>`, the current owner is `<socket>.owner.json`.
Immutable history is retained in the private directory `<socket>.owners/`:

- `<daemon_id>.owner.json`: byte-identical original owner record;
- `<new_daemon_id>.recovery.json`: previous owner SHA, new identity and native
  process observation or exact graceful-close authority;
- `<daemon_id>.closed.json`: owner SHA and positive socket/PID removal result.

The owner binds configuration, database and lock identities, socket/diagnostic
inodes, history and staging directories, and the preparation record's inode
and SHA. `<socket>.preparing.json` records startup intent. A compact private
staging socket is published to the final name using no-replace rename only
after complete owner records are durable. Reclamation and graceful cleanup
remove only the validated original socket/PID inodes; replacement paths are
preserved. Logs, locks and ownership history remain for inspection and replay.

These receipts explicitly have scope `control_socket_only_not_VM_quiescence`.
They do not certify absence of Machines, execution descendants, disks or Docker
state. Machine lifecycle authority remains in its typed operations and exact
runtime/session ownership. A restarted daemon may use an exact prior positive
Stop to Delete a stopped Machine; missing live-session ownership is not a Stop
or Delete success condition.

## Remaining acceptance work

Incomplete preparation without a matching complete owner fails closed and is
retained. This first implementation does not certify every startup crash
boundary. Closed receipts currently write directly to their final filename;
an interrupted write can leave malformed evidence that blocks subsequent
recovery. Atomic closed-record publication remains a follow-up.

The [installed recovery harness](../scripts/helpers/installed_daemon_recovery_e2e.md)
and [entry point](../scripts/run-installed-daemon-recovery-e2e.sh) exercise a
fresh two-Environment/four-Linux-Machine scenario: positive Stop of all Machines,
exact daemon crash, public Delete recovery, neighboring persistent data, and
graceful replacement shutdown. Both initial physical candidates **failed**;
the complete lane is not certified. Candidate 1 exposed a corrected historical
activation-validator bug. Candidate 2 proved control recovery and preserved
Engine/container/volume metadata, but restarting the original workload failed
with filesystem corruption. The unjournaled Docker disk and power-cut Stop
path require the priority-zero durability fix tracked by `vz-u0u`. No retries,
harness socket unlinking, database writes or disk repair are allowed.

Real concurrent-start races, observer/process crash boundaries, foreign path
replacement, live or uncertain Machine recovery, and the aggregate Environment
gate still require applicable evidence. Unit tests and a stopped-Machine DEV
pass cannot close `vz-ehz`, certify other host/target pairs, establish full Docker
parity, or certify release 0.4.
