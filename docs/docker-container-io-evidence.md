# Container lifecycle and interactive I/O evidence (DEV)

Tracked by `vz-mzs.7.1.6`. This is test infrastructure for the 16
`docker.container.*` scenarios, not a completed Docker compatibility lane.
The installed lifecycle dispatcher is available as the explicit DEV
`run-linux-docker-e2e.sh --suite lifecycle` slice; its physical Docker acceptance
remains unfinished. `--suite all` still rejects before provisioning.

## Implemented capture boundary

`docker_host_driver.Recorder.run` and `Driver.command` accept an explicit
`interaction_plan`. Ordinary commands retain their existing DEVNULL stdin and
separate bounded output pipes. Interactive commands use either separate pipes
or a newly allocated, disposable PTY; they never use the user's terminal or
inherit stdin, Docker environment variables, or an SSH agent.

A plan specifies schema 1, `pipes` or `pty`, a deadline of at most 120 seconds,
input/output bounds of at most 4 MiB, and at most 32 ordered actions:

- write exact public bytes;
- close pipe stdin once, while continuing to drain both output streams;
- resize the PTY and send SIGWINCH to the exact owned CLI process group;
- send SIGINT or SIGTERM to that owned group.

Actions can wait for exact bytes on a specified output stream. Receipts bind
marker offsets, observed prefixes, input lengths/hashes, EOF counts, resize
dimensions, signal scope, times, raw outputs, and the positively reaped child.
The public input/marker bytes are retained in a digest-bound plan, fsynced before
dispatch intent. Private canaries in those inputs are rejected before dispatch.
Timeout, overflow, missing actions, failed reap, or failed terminal restoration
retain uncertainty and cannot authorize workload cleanup.

PTY output is a merged terminal transcript, never proof of demultiplexed
stdout/stderr. The capture checks post-child terminal attributes before any
harness repair. Repair cannot manufacture a passing client-restoration result.
The harness does not claim its PTY is a controlling terminal; explicit resize
notification is recorded. Raw negative host signal exits are retained, but
cannot satisfy expected normal Docker exit-code assertions.

`linux_docker_interactive_evidence.validate_recorded` independently replays the
durable intent, exact invocation, public plan, action timeline, and raw files.
The caller must supply its source-selected `expected_plan`; resealing changed
plan bytes with new self-reported hashes cannot replace that input. Wall-clock
observations must remain ordered and agree with the monotonic clock within an
explicit one-second tolerance; the capture must fit inside the recorder's
monotonic interval. This is clock-coherence validation, not a latency guarantee.
This proves host capture only. It does not authenticate a Machine or prove that
a CLI signal reached the intended guest process. The surrounding lane must bind
current Machine/context/Engine/incarnation guards, exact container identity,
guest records, sibling observations, and ownership-safe cleanup.

## Frozen public probe

The sibling fixture `tests/fixtures/vz-0.4/docker-container-io` leaves previously
verified fixture trees unchanged. Its five-file tree digest is
`7c964069b26ff1dac16fd1ef3a951c11c7758da97c0bf6b46a1d37d12b48e4da`.
It uses the existing pinned Linux/arm64 Python base and no package downloads.

The probe supplies a 65,792-byte binary round trip with output delayed until
after EOF; a 24×80 → 40×120 terminal exchange; actual observed signal records;
root/nonroot exec identity and namespace observations; and explicit starting,
healthy, and unhealthy service states. The host validators require exact public
tokens, canonical JSON, expected stream separation and exact exits.

Nonroot permission denial reading PID 1 namespace links is an observation, not
proof of namespace equality. A separate root exec must establish PID 1's full
namespace inventory and match the nonroot process within the same independently
authenticated container incarnation.

## Remaining acceptance

The lifecycle dispatcher now connects these primitives to the installed Mac
topology harness: lifecycle identity/process generations, health polling,
followed logs/events, binary attach and exec stdin, TTY resize/restoration,
signals, and exits 0/37/130/137/143/126/127. It independently reads retained
command, state and interaction evidence in source-selected order before and
after owned cleanup. The concurrent logs follower has its own command ledger
and must observe the exact ready prefixes before the source-selected TERM
command begins. Attach uses a public kickoff byte to prevent output racing ahead
of attachment, and explicitly requests stdin attachment for Docker's
`StdinOnce` EOF behavior.

The physical run must still establish these assertions. Numeric fixture exits
alone cannot prove signal handling. Require three tested Machines, four continuous sentinels,
independent raw replay, owned cleanup, clean public Stops, unchanged defaults,
and checksummed evidence. Full runtime/process provenance and actual Docker tmux
acceptance also remain; Engine configuration and stopped container inspection
alone do not certify all youki invocations or process absence. Local Python or
tmux tests do not close this issue or the full 63-scenario Docker/release gate.

## Verified local checkpoint, 2026-09-06

The installed harness interpreter, `/usr/bin/python3`, passed 680 affected host
harness/driver/startup tests; the separate frozen guest probe passed 17 tests.
Those include real finite local Python pipes and PTYs, exact binary bytes,
half-close, delayed output, signals, restoration, pending-child retention, and
adversarial replay. They do not run the new Docker lifecycle scenarios.

`linux_docker_interactive_tmux.py` then exercised the public probe directly on
this Mac in an isolated tmux 3.6 server. Candidate 3 proved three exact terminal
records (ready 24×80, resized 40×120, done), actual pane exit 37, normal server
exit 0 and positive reap, and exact post-reap socket/directory removal.
Its 117 payloads total 72,842 bytes, retained under
`.artifacts/container-io-tmux-candidate-3`; the manifest SHA-256 is
`cba516a1b717c1b2bd8f7b34325cba731353e62e5ded59513cc3944bc4b1c3d5`.
This tmux result explicitly does not certify Docker or host Docker terminal
restoration. The separate byte-level capture tests cover client restoration.

Earlier candidates remain failed and retained. Candidate 1 exposed tmux 3.6's
null-window manual-sizing path: configure manual sizing only after window
creation ([upstream spawn](https://raw.githubusercontent.com/tmux/tmux/3.6/spawn.c),
[upstream sizing](https://raw.githubusercontent.com/tmux/tmux/3.6/resize.c)).
Candidate 2 proved the terminal exchange but rejected an incorrect expectation
that tmux automatically removes its socket. Both servers were positively
reaped; separately recorded exact socket disposition removed only their stale
owned socket and empty private directory. Neither failure was reclassified.

An unrelated scanner-depth test also fails on Homebrew Python 3.14.6 and was
reproduced on unchanged HEAD. The installed interpreter passes that test;
`vz-mzs.7.1.7` tracks making the JSON-depth bound independent of parser recursion.
