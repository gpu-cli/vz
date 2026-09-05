# Installed Delete observer-disconnect proof (DEV)

This driver tests an admitted public Delete continuing after its CLI observer
disconnects. It is not daemon-crash recovery, Docker compatibility certification,
or the aggregate 0.4 release gate.

Use the eight explicit artifact/client flags from the
[happy-path Delete driver](installed_delete_e2e.md) with:

```bash
scripts/run-installed-delete-disconnect-e2e.sh \
  --release-dir /absolute/staged-signed-release \
  --release-version 0.4.0-dev \
  --developer-bundle /absolute/vz/linux/out \
  --hardened-bundle /absolute/vz/linux/out/container \
  --docker /usr/local/bin/docker \
  --compose-plugin /absolute/docker-compose \
  --buildx-plugin /absolute/docker-buildx \
  --evidence-dir /absolute/new-disconnect-evidence
```

Normal installed public Up creates two fresh named Environments, each containing
two Developer Linux Machines with distinct managed contexts and Engines. The
Mac's Docker client creates owned container/volume sentinels. No existing daemon
or previous candidate's Machines are adopted.

The driver starts public JSON Delete exactly once. After a correlated admitted
nonterminal frame and a fresh durable Running observation, it sends SIGTERM only
to its exact unreaped CLI child—not to the daemon, a process group, or a Machine.
The child must exit from that signal. A new read-only live-WAL transaction,
started after the child was reaped, must still observe the same Running operation
without a tombstone. Missing this window fails the candidate without retry; an
old buffered frame or stale immutable SQLite view cannot satisfy the assertion.
Read-only means no logical database/lifecycle writes; normal SQLite WAL/SHM
reader coordination is not a claim of byte-immutable filesystem access.

Only read-only observations then poll the exact durable operation to Succeeded
and its tombstone. The positive result is recorded durably before any replay
request. Outside-store original-runtime quiescence and deletion receipts are
verified, then one exact request replay must return that already completed
operation and tombstone. No DB writes, direct lifecycle calls, artificial pause
hooks, or replay-triggered recovery are used to manufacture completion.

Neighbor Engine/container/volume-byte probes bracket the operation and run in
the background; these establish sampled liveness/non-restart, not continuous
uptime. Host project/worktree bytes and daily/isolated Docker defaults must stay
unchanged. Public Delete then cleans the neighbor, and the exact test daemon
must shut down gracefully. Four private Machine stores are intentionally
removed; host fixtures, DB/tombstones, outside-store journals, and raw evidence
remain. Any uncertain operation withholds automatic lifecycle cleanup and
daemon termination, preserving its original authority.

Offline prerequisites (no Docker/VM/daemon):

```bash
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -m unittest discover \
  -s scripts/helpers -p 'test_installed_delete*.py' -v
bash -n scripts/run-installed-delete-disconnect-e2e.sh
bash scripts/run-installed-delete-disconnect-e2e.sh --help
```

The first physical local-Mac candidate passed at
`.artifacts/installed-delete-disconnect-candidate-1/`. See the
[checkpoint and evidence pins](../../planning/developer-environments/02-developer-cli.md#delete-observer-disconnect-checkpoint-2026-09-05-dev)
for its result and remaining acceptance scope. The previous full backend gate
remains applicable to the unchanged signed runtime files; this harness-only
checkpoint does not claim a newly rebuilt backend or aggregate gate.
