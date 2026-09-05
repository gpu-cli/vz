# Installed public Delete — scoped DEV physical driver

This is a fresh-install, happy-path Delete proof. It does not certify the full
0.4 lifecycle, crash recovery, other topology adapters, or Docker parity.
It never attaches to an existing daemon or previous candidate's Machines.

Use the same eight explicit artifact/client flags as
[the installed startup driver](installed_developer_startup.md), with this entry point:

```bash
scripts/run-installed-delete-e2e.sh \
  --release-dir /absolute/staged-signed-release \
  --release-version 0.4.0-dev \
  --developer-bundle /absolute/vz/linux/out \
  --hardened-bundle /absolute/vz/linux/out/container \
  --docker /usr/local/bin/docker \
  --compose-plugin /absolute/docker-compose \
  --buildx-plugin /absolute/docker-buildx \
  --evidence-dir /absolute/new-delete-evidence
```

The driver verifies installed help exposes exactly Up/Exec/Status/Stop/Delete,
and the retired `vm` root rejects without starting a daemon. Normal public Up
then autospawns the installed sibling daemon and provisions two named Developer
Environments, each with two Linux Machines. Each Machine gets an independently
labeled offline imported BusyBox container and volume sentinel.

Ready Delete removes the primary's exact private stores, contexts and sockets.
Recreating its name must allocate new immutable Environment/Machine identities.
Replaying the old Delete request with that reused name must return the original
tombstone and leave the replacement operational. The replacement is then stopped
and deleted, followed by the neighbor. All public Delete streams, cleanup graphs,
tombstones and outside-tree inode-bound receipts are checked against original
public activation and store ownership evidence.

Independent bounded neighbor observations run throughout the primary lifecycle.
Every Delete is bracketed by positive Engine/container/volume-byte checks;
background sample overlap and uncovered intervals are reported explicitly.
This is sampled liveness and non-restart evidence, **not** packet-level zero
downtime certification. Host project definition, `.git` pointer, HEAD, worktree
token, user sentinel and both daily/isolated Docker default bytes must survive.

Before deletion, raw Machine startup receipts are copied into the evidence tree.
Every host command retains intent, bounded raw streams, exit/timing and hashes.
Success requires three distinct positively deleted Environments and verified
graceful termination of only the exact autospawned daemon. Host fixture/evidence
directories, database/tombstones and outside-store Delete journals remain; no
recursive host cleanup or Docker prune is performed.

Any failed/uncertain Delete is not retried and withholds automatic Stop and daemon
termination, retaining the original operation and runtime for reconciliation.
Other certain failures may use the existing authenticated Stop cleanup, but
never automatically Delete remaining Environments. Partial failures are not
presented as passing evidence.

Offline checks (no Docker, daemon, VM or build):

```bash
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -m unittest discover \
  -s scripts/helpers -p test_installed_delete_e2e.py -v
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -m unittest discover \
  -s scripts/helpers -p test_installed_delete_quiescence.py -v
bash scripts/run-installed-delete-e2e.sh --help
```

The first local-Mac run passed at `.artifacts/installed-delete-candidate-1/`:
three positive Deletes, six distinct Linux Machines, 128 background neighbor
observations, zero test-case retries and zero cleanup errors. Its result and
checksum pins, backend regression evidence and remaining acceptance scope are
recorded in the [Delete checkpoint](../../planning/developer-environments/02-developer-cli.md#streamed-delete-checkpoint-2026-09-05-dev).
