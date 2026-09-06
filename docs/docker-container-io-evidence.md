# Container lifecycle and interactive I/O evidence (DEV)

Tracked by `vz-mzs.7.1.6`. This is test infrastructure for the 16
`docker.container.*` scenarios, not a completed Docker compatibility lane.
The installed lifecycle dispatcher is available as the explicit DEV
`run-linux-docker-e2e.sh --suite lifecycle --tmux /absolute/canonical/tmux` slice
(alongside its required installed-client and guest-bundle options); its physical Docker acceptance
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

The Docker-backed tmux adapter now launches the exact Machine-context Docker
`exec --interactive --tty` invocation inside an owned foreground tmux server.
It reuses the local smoke's terminal interactions, but launches the frozen probe
inside the authenticated service container, not host Python. Original admission
pins bind tmux, Python, Docker and the helper sources. Independent raw replay
requires the ready/resize/done records, pane exit 37, normal server exit/reap,
socket retirement and no fallback. Ten source-selected Machine/service guard
commands bracket the separate terminal ledger; unresolved children prevent
cleanup. Its first-Machine physical results are recorded below; the full
three-Machine workload remains unverified.

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

## Installed Mac candidate 1, 2026-09-06: failed, retained

Source commit `351e452b` passed 747 affected regression tests before the final
health-timing patch; all 79 container-focused tests passed with that patch.
The installed signed `0.4.0-dev` build then provisioned two Environments with
four Linux Machines. This is a local DEV build, not a notarized release claim.

On the first Machine, the workload reached `docker run -it` after health
transitions, all five exec phases (including TTY resize, Ctrl-C, and exact host
terminal restoration), binary attach/stdin, and run exits 0 and 37. Command 110
failed: the frozen guest probe returned 70 with
`VZ_CONTAINER_IO_CONTRACT_REJECTED` before its ready record or Ctrl-C input.
The capture correctly retained uncertainty instead of acknowledging success.
All eight interactive host clients were positively reaped. The four sentinels
produced 96 samples / 384 raw commands with no liveness errors. Neither the first
Machine workload nor the three-Machine slice passed independent final replay.

Retained evidence is `.artifacts/linux-docker-lifecycle-candidate-1`:
2,753 payloads, 3,325,530 bytes; manifest SHA-256
`a19eea2a635c9005e9c3c21477571744a9687fb159096af8e260781f04924894`.
A separate exact-owner disposition performed two public Environment Stops,
verified four distinct clean-journal shutdowns, observed graceful daemon
shutdown, and confirmed unchanged daily and isolated Docker defaults. It
retained all objects and disks and did not reclassify the failed candidate.
Its 500 payloads / 575,289 bytes are in
`.artifacts/linux-docker-lifecycle-candidate-1-disposition`; manifest SHA-256
`7214542bd16aac4ec4912328cee7b9ef73f50cb320abbd2bdf7546b476c5b432`.

The separately retained failed-container inspect shows `ConsoleSize: [24, 80]`,
TTY enabled, and stopped exit 70 / PID 0. Although Docker starts its resize
monitor after container start, it also sends the initial console size at create
time ([run source](https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/command/container/run.go),
[create source](https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/command/container/create.go)).
The pinned youki source provides a concrete defect to investigate:
[`setup_console`](https://github.com/youki-dev/youki/blob/94ba653efbb180ce04650f6ae01a8e6bc8f96d92/crates/libcontainer/src/tty.rs#L286)
calls `openpty(None, None)` without the OCI console dimensions. Both init and
exec use this path; none of our seven local patches changes it. Moby forwards
the configured size into the OCI process specification. The authenticated
youki source archive SHA-256 is
`bbf134e568c6cc2672c687c93176a1f0a67df332d416feb5f3a078fb39762b42`.

Ignoring the requested initial size is a source-confirmed runtime defect; its
causal link to this generic probe failure remains unproven. Next, observe actual
guest dimensions, terminal flags, and `isatty` values at entry, then implement
and verify the runtime correction with rebuilt guest artifacts and the backend
gate. Do not add a wait or relax the frozen probe merely to obtain a passing
candidate.

## Diagnostic candidate 2 and runtime correction

Source `4eec3f3e` adds a failure-only observer around the unchanged probe. It
records the rejected predicate's function/line and public terminal observations,
then preserves the original rejection and exit code. It introduces no waits,
terminal repair, accepted fallback, or additional successful output. All 80
container-focused tests passed before the fresh installed-Mac run.

Candidate 2, using the same old guest bundles, failed at command 110 with an
explicit `check: size`, line 82, `rows: 0`, `cols: 0`, and
`isatty: [true, true, true]` record. This confirms the probe actually encountered
zero initial kernel PTY dimensions, rather than merely suggesting it from the
runtime source. The original probe still returned 70. Evidence under
`.artifacts/linux-docker-lifecycle-candidate-2` contains 2,953 payloads /
3,510,053 bytes; manifest SHA-256
`893c9ba637f674b0ac2ea354fcee13e2a8efae0b90eb745878b4682c3f8a93c4`.

Its separately verified exact-owner disposition again stopped all four Machines
with clean-journal receipts, observed graceful daemon shutdown, and preserved
Docker defaults, objects, disks, and the failed outcome. The 500 payloads /
580,109 bytes in `.artifacts/linux-docker-lifecycle-candidate-2-disposition`
have manifest SHA-256
`a3cba437a063f93a270760beb52564bf990345deea7c78f35588f89e8ae746a8`.

Runtime correction `1f3c01ea` introduces the pinned, locally authored
[`console-size.patch`](../linux/youki/console-size.patch). It passes the selected
OCI process dimensions into PTY allocation before descriptor handoff for both
init and exec. Checked conversion rejects overflow; omitted and explicit zero
values retain their semantics. Four required native tests cover real PTY size
readback, edge cases, and spec/call-site routing. The build validator requires
their complete output and exact patch/runtime identity. All 29 offline validator
tests and 751 affected host-harness tests passed. The native ARM64-musl build
passed its required regression groups, including all four console-size tests
with real PTY readback and no ignored tests in that group. Independent artifact
validation returned runtime SHA-256
`b811a418031fbe2c8d84acc92a2d9c3e1191701478efe471ddfb0dbbc4421d20`.
The candidate is retained under
`linux/.cache/youki-source/builds/89b46f93c7751cfe866be4272302e01a523dd48b81b9d8de0c5542365255039d`;
its evidence manifest SHA-256 is
`ef9e12147095ea08e91e3ba78a3a8188ed64c0090bc72d90225a1b75348664e6`.
The rebuilt backend gate passed all 50 parent tests in seven selected suites in
`.artifacts/sandbox-vm-e2e/20260906T180936Z`; the console log is
`.artifacts/console-size-backend-1.console.log`. Nine copied guest inputs are
preserved in its `retained-guest-bundles` directory with manifest SHA-256
`1676ee80650b38b5415e82da5eba46c42e7bf91c629eeffd2ff35c8a0ea13289`.
A fresh installed Docker lifecycle pass remains required. Neither failed
candidate is retried or reclassified as acceptance.

The subsequent Docker tmux integration passed 92 focused container tests.
Broader host discovery completed 815 tests successfully in 198.239 seconds;
two additional schema modules failed import because system Python lacks
`jsonschema`, so that invocation exited unsuccessfully. Its complete output is
`.artifacts/container-tmux-host-regression-1.log`.
Both schema modules then passed their documented offline
`uv run --offline --with jsonschema==4.23.0` runner: 15 compatibility-contract
tests and seven project-schema tests. This is 837 passing host tests across the
two interpreter/dependency setups, not a green result for the earlier command.

## Corrected-runtime candidate 3

Source `0d06f8ab` ran with the newly retained guest bundles. Command 120 now
passes the previously failing `docker run --interactive --tty` phase: the first
guest record has 24×80 dimensions and three TTY streams, an actual Ctrl-C byte
follows readiness, and the guest reports SIGINT/130. The owned client is reaped;
its terminal attributes were already restored before any harness repair.

The first Machine's actual Docker tmux exchange also independently replays:
24 controls, canonical ready 24×80 → size 40×120 → done/37 records, pane exit 37,
normal server exit/reap, and exact socket retirement without fallback. The raw
tmux manifest is
`e2ea872ca888d861e1040a6032b36e373da1f8342892ecd1da5e24b3411ece52`.

The overall candidate remains **failed** at command 128: Python PID 1 sent
itself SIGKILL but exited 0, while the harness expected 137. Pinned Linux
6.12.85 source explains this behavior: `kernel/fork.c` marks namespace init
`SIGNAL_UNKILLABLE`; `kernel/signal.c` ignores its default-handler signals unless
the forced kernel/ancestor-namespace exception applies. Self-SIGKILL therefore
does not establish the intended test. No runtime change is justified by this
failure. The revised test must keep the Docker run client attached to the normal
service while a separately guarded host command kills its exact container ID,
then verify both client and container exit 137. A numeric fixture exit or killing
the host client cannot substitute for that signal propagation.

The original 3,099 payloads / 3,668,543 bytes are retained in
`.artifacts/linux-docker-lifecycle-candidate-3`; manifest SHA-256
`0a4783401999cd4b73ee4d1d0a4b4844d74d758f02b7282a4d5f2280ef801e8f`.
Separate disposition confirms the exact failed container exited 0, four
owner/incarnation-bound clean-journal Stops, graceful daemon retirement and
unchanged defaults. Its 500 payloads / 579,091 bytes are under
`.artifacts/linux-docker-lifecycle-candidate-3-disposition`; manifest SHA-256
`95f9d9cd8b32ff1840591bd62f74639ca136675f4160fbebf40197193c19dec6`.
Objects/disks and the failed result are preserved. First-Machine phase results
do not establish the three-Machine workload, full process/runtime provenance,
or the aggregate release gate.

The revised observer now owns a separate five-command guard/run/guard ledger.
It observes both canonical ready streams before the parent authenticates the
running container generation and issues exact-CID KILL. Independent replay
requires attached-client 137, the external KILL timestamp after live readiness,
wait/inspect 137 from the same started generation, positive child/thread
closure, and a source-bound negative-exit acknowledgement. The invalid self-kill
fixture is removed from the workload; the frozen probe itself is unchanged.
All 105 container-focused tests and 690 affected Docker/host/startup regressions
pass; the latter output is `.artifacts/container-kill-host-regression-1.log`.
These unit results alone do not establish physical acceptance; the next section
records the first installed attempt with this revised observer.

## Candidate 4: signal propagation proved, error classification still failing

Source `c36c4bbc` passed the revised attached-run phase on the first Machine.
Independent replay binds the five-command observer ledger to original main
command 139's exact-CID KILL and timestamp, with both ready streams observed
before that command. The attached client, wait result and same-generation
container inspection all report 137; its observer is reaped/joined and the
negative result is acknowledged only after semantic validation.

The candidate then failed at command 145: `/fixture/not-executable` returned
host CLI 125 instead of required 126. The actual host client is Docker 29.4.0
build `9d7ad9f`; the guest Engine is 29.7.2. These versions must not be conflated.
Raw stderr preserves youki's `does not have correct permissions` diagnostic.
The pinned runtime's structural permission rejection lacks the text recognized
by Docker's [error classifier](https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/command/container/run.go),
which maps `permission denied` to 126 and missing-command diagnostics to 127.
Youki's missing-path diagnostic has the analogous source-confirmed risk;
candidate 4 did not reach that physical case.

Next: preserve the rejection predicates, correct those diagnostic contracts,
add native executor/classification regressions, rebuild both guest profiles,
pass the full signed backend gate, and require a fresh installed run to prove
126 and 127. Do not accept 125 or change file permissions to bypass the failure.
Full workload/provenance/aggregate acceptance remains open.

Original evidence: `.artifacts/linux-docker-lifecycle-candidate-4`; manifest
SHA-256 `b4b285c997e59e5289bc2d1a6e82228321da0d43a81362a8187e6574ad92fdc6`.
Its exact inventory is 3,380 payloads / 3,970,606 bytes. Separate diagnostic
inspection confirms the failed object remains `created`, PID 0, with Engine
`ExitCode: 128` and the original permission error; that Engine state is distinct
from the host client's 125.

The separate public-Stop disposition passed: four original-owner/incarnation
clean-journal receipts, graceful daemon retirement, socket absence and unchanged
defaults. Its 500 payloads / 584,759 bytes are under
`.artifacts/linux-docker-lifecycle-candidate-4-disposition`; manifest SHA-256
`1ae859df6ef3a73bf464931f247f658a570fbdf699aa3ff641693818357bff84`.
Both complete inventories/hashes were independently verified. Objects and disks
remain retained; no failed result was retried or reclassified.

The next source candidate adds the pinned `vz-executable-errors-v1` patch
(SHA-256 `83b34be8acf1b730e62c0e0a05cd541320ab1af7ae6d4b286a4322f974cd07f5`).
Only the two structural validation diagnostics change. Its four mandatory native
regressions exercise the actual validator and narrowly mirrored pinned consumer
classifiers, not Docker itself. Physical 126/127 acceptance remains pending.
Independent source review also identified a distinct unresolved path:
`ExecutorError::Execution` hides its boxed execvp cause when displayed, and the
intermediate process transports that display string. Kernel authorization and
interpreter/format failures therefore need additional transport tests and actual
Docker coverage; this prefix correction does not establish those cases.

Source `00effbb3` passed all 33 offline candidate-validator tests and the pinned
native ARM64 build. All four executable-error regressions passed with no failed
or ignored tests. The candidate runtime SHA-256 is
`bc624392bf1733f3eb0339c397d86d8aa422ab34783fdc6da7eae7df5fde0970`;
its 31-payload evidence manifest SHA-256 is
`c6f3f1ccc5512ac7bb7482afc4ddf39bd2220b6cdd99739ec3cfa70c34442869`.
The candidate is retained under
`linux/.cache/youki-source/builds/6bb9da17c79065cd1fef4cd75650be782b55cc2c3f4c4839b89314678d23f666`;
the complete native log is `.artifacts/youki-executable-errors-build-1.console.log`
(SHA-256 `d7e54781595067211cfddb2f03b573d86613121fba04ff3737c9eacea61ea236`).
These native results do not replace the rebuilt backend or installed-Mac gates.

Independent native audit verifies the exact 31 payloads (6,870,832 bytes) and
81 distinct selected passing tests: the prior 77 plus the four new regressions.
The lifecycle harness now requires the exact lowercase, entrypoint-specific
canonical diagnostic once, with the original required 126/127 exit and ownership
checks. It no longer requires `no such file or directory` for a runtime whose
missing-command contract is `executable file not found`; unrelated Docker wrapper
and help formatting are not pinned. All 692 affected Docker/host/startup tests
pass in `.artifacts/container-executable-errors-host-regression-1.log` (SHA-256
`72c9e76d52269b07ecd1bfd8e6a5993b2d8f88573e81e7da67ae1c9ba260107c`).

The rebuilt release-profile backend gate passed all 50 selected tests across
seven lanes under `.artifacts/sandbox-vm-e2e/20260906T192121Z`; none failed or
were ignored among the selected tests. Summary SHA-256:
`435e09017d69d6ebf7242fb449975cede8d543654090d9eec605e6861bf70032`.
Console `.artifacts/executable-errors-backend-1.console.log` SHA-256:
`81921bbbc5e4a1ab3e9c003c5186bab4556636aa503a41f8335a3cac95e01eb1`.
Its nine retained guest inputs total 85,560,956 bytes, with manifest SHA-256
`f5220ec93ee0eed8c6612d1bcda1588737ba928e4bf1bf19345d68e106f2c335`.
Both profiles contain the new runtime; these backend results do not establish
installed lifecycle or aggregate release acceptance.

## Candidate 5: installed three-Machine lifecycle workload passed

Source `d432b3f1`, using the new runtime and retained backend bundles above,
completed the installed local-Mac lifecycle slice with no test retries or cleanup
errors. Each of three Machines completed its 222-command main ledger, separate
stream observers and owned tmux session. Required exits 0, 37, 130, 143, 137,
126 and 127 all passed. On each Machine, commands 145 and 148 are the real host
Docker non-executable and missing-command results; the new exact canonical
diagnostics, original rejection predicates and frozen fixture are preserved.

The workload also covers health transitions, binary stdin/stdout/stderr and EOF,
exec user/cwd/root/namespace checks, initial/resized TTY dimensions, attach,
stop/restart/kill/wait, followed logs and create/start/die/destroy events.
Each tmux pane reports 37, its server is reaped, and its owned socket is removed.
Successful workload cleanup removes the owned containers and image tags while
preserving unrelated IDs. The run reports four positive public Stops, graceful
daemon shutdown and unchanged daily/isolated Docker defaults. Stopped Machine
disks and contexts remain retained; this is not Delete certification.

Evidence is `.artifacts/linux-docker-lifecycle-candidate-5`; manifest SHA-256
`f79520c8ee3af436526bc4fdd24ca9ea7de798533b8b13ad7dc15f7ff19beb2f`.
Independent integrity verification checks the exact 12,214 payloads totaling
14,170,691 bytes. Four raw clean-journal Stop receipts match the original
Machine owners and incarnations; host daemon PID 84369 is confirmed absent.
Result SHA-256:
`05abb34de632b397fda767d78aca53211f9423cf6f6a2dffd796ffd0f28999fc`.
Console `.artifacts/linux-docker-lifecycle-candidate-5.console.log` SHA-256:
`aebcf936f22ed5ffd2417e28f95809378cb6e9c75b8dda29a96d2d35104da1cc`.

An independent no-dispatch replay matches all three retained workload/cleanup
results exactly: 222 main commands and 135 source-selected steps per Machine
(666 main commands total),
including raw stream hashes, negative acknowledgements, tmux, external KILL and
followed logs. The replay disables process dispatch and evidence writes; it does
not reuse a live Machine or rerun a test.

This is a passing DEV lifecycle slice, not full Docker or 0.4 certification.
`full_process_absence_certified` remains false: guest process birth/namespace/
cgroup absence and actual Engine-youki invocation receipts still need complete
evidence. The separate kernel-exec error transport gap above and aggregate
release integration also remain open. Earlier failed candidates remain failed.

## Process observation contract

The lifecycle source now adds read-only, exact-Machine public Exec observations
around five long-lived container generations: the service before and after its
restart, the waiting attach process, the externally killed process, and the
followed-log TERM process. Each running snapshot binds guest boot ID, PID birth
ticks, namespace PID mapping, namespace memberships and owned cgroup identity.
Stopped observations reject the original birth (including a zombie), remaining
private PID/mount namespace members, or any task in the owned cgroup subtree.
Every owned container also gets a cgroup check after removal. Fast-exit cases
without a prior running snapshot explicitly do not gain historical PID evidence.

The probe admits only the observed pinned Engine's unified cgroupfs/default-parent
configuration and checks actual membership under `/docker/<full-container-ID>`.
Directory/process/stream bounds fail closed, including trailing-newline overflow
and excessive directory depth. Shared user/cgroup namespace memberships are not
private absence targets. Observations cover an interval, not an atomic kernel
transaction or namespace-object/file-descriptor teardown.

A separate ledger pins the public CLI, observer/parser sources, environment,
project definition and Machine incarnation. Six source-selected Docker commands
bracket each probe; replay checks its original timestamps between the two
generation/absence checks. Raw frames remain checksummed while returned proofs
use compact process-inventory hashes. The observer is registered before capture,
and incomplete observations prevent normal cleanup. Unit/replay tests alone do
not establish physical acceptance; candidate 6 below provides installed evidence.

Historical OCI invocation evidence still needs an internal youki audit mechanism,
not a substitute runtime wrapper or faster process polling. Containerd invokes
youki for container create/start/kill/delete and detached exec admission, but it
kills exec processes directly and removes their IO/pidfile without extra youki
kill/delete invocations. Audit expectations must reflect those actual paths.

## Candidate 6: installed lifecycle with process observations passed

Source `9dc6768b` completed a fresh installed local-Mac run against the unchanged
release-built CLI, daemon and retained guest bundles used by candidate 5. The
runtime bytes are unchanged, so the earlier native 81-test and backend 50-test
results remain applicable. The changed host harness passed 729 regressions in
32.485 seconds; log `.artifacts/container-process-host-regression-2.log` SHA-256:
`03f3e448df5bb1f5cc091aa7ab337e9b0396f170f05d47765a4a2b8ceca3ee3f`.

All three Machines passed the original lifecycle and interactive workload,
including exits 0, 37, 130, 143, 137, 126 and 127. Each completed 354 main Docker
commands, 179 source-selected steps and 22 separate process observations.
Independent no-dispatch, no-write replay reconstructed every retained workload
and cleanup result exactly: 1,062 main commands and 66 process observations
across the three Machines. Each owned tmux pane returned 37, its server was
reaped and its socket removed. There were no test retries or cleanup errors.

Independent raw sampler replay confirms 15 running, 15 stopped and 36 removed
observations. All 51 nonrunning samples found the owned cgroup absent; 27 also
verified the previously recorded birth and private PID/mount namespace members
absent. The other 24 explicitly provide cgroup-only evidence without historical
birth claims. Captures took 0.349–0.537 seconds and retained 12,162,443 raw bytes.
Each sampled 93–97 stable processes, including 84 kernel threads. No zombie or
unavailable-namespace observations occurred, so the narrow zombie allowance is
source/unit verified but not physically exercised by this candidate.

Four raw public Stop receipts independently match the original Machine owners,
incarnations and operations, with clean journals, synced/unmounted storage and
reaped guest daemons. Host daemon PID 8295, its PID file and all five owned daemon
and Docker sockets are absent. Daily and isolated Docker defaults are unchanged.
All 2,848 raw sentinel commands independently validate as complete, certain and
successful: 712 ordered samples, 178 per Machine across four Machines. Original
Engine/context endpoint, container/image identity, start time, zero restart count
and host-written marker remain stable, with no sentinel errors or retries. This
establishes sampled continuity, not network-service conformance.

Evidence is `.artifacts/linux-docker-lifecycle-candidate-6`. Independent full-byte
integrity verification checks the exact 18,221 payloads totaling 34,841,321 bytes.
Manifest SHA-256:
`4ec6d7450f2f7e9c1aebbe447b7c066819beeb154dc36d9b4ab9022663e284c2`.
Result SHA-256:
`de331990f7c027420b28b903fb135b8cd061c30a29b984bbad623c80933ef1d6`.
Console `.artifacts/linux-docker-lifecycle-candidate-6.console.log` SHA-256:
`c2f96c914e92e1e6e264a1fb66c6396817c3655d68bf67054a0a54f8940f5a17`.

This remains a DEV lifecycle slice, not full process absence, Docker parity or
0.4 release certification. The observations establish bounded kernel-state
checks, not historical births of unsampled fast processes, surviving namespace
file-descriptor references or actual Engine-to-youki invocation history. The
kernel-exec error transport gap and full aggregate release gate remain open.
Stopped Machine disks and contexts are retained; this run does not certify Delete.

## Runtime invocation journal: native verification passed, Machine integration pending

Source `29886b79` adds an opt-in internal youki journal, a strict independent host
parser and source-pinned native CLI probes; see
[`../linux/youki/README.md`](../linux/youki/README.md). The final host Docker-helper
regression passes 718 tests in 34.912 seconds, and the offline candidate validator
passes 41 in 3.541 seconds. Their retained logs are
`.artifacts/runtime-audit-host-regression-1.log` (SHA-256
`8cbe6444b8edc4523939af54adb7684370053823498bb6dc85e56e4971ad861f`) and
`.artifacts/runtime-audit-candidate-validator-1.log` (SHA-256
`a7ddc898e7c0843764a641b7ab02810213f27c4f215d8e11960b3637037dee5f`).

Native build attempt 1 compiled the release binary, but failed the typed-command
audit test: pinned upstream `CommonCmd::Checkpointt` exposes `checkpointt`, not
`checkpoint`. Five audit groups passed; this attempt is not a passing candidate.
The complete failure log remains `.artifacts/youki-runtime-audit-build-1.console.log`
(SHA-256 `3b9e14f3ba5cc2ef664ffe6b62a078dab9d5d6ff0db800d59234b9f22c15f79f`).
The corrected fixture exercises the actual pinned spelling while retaining the
journal's typed `checkpoint` operation, without claiming that the upstream public
spelling was fixed. The verifier also handles the exact static audit warning
interleaved with native test-status lines; original raw logs remain unchanged,
and the first failed attempt remains rejected. The revised offline validator
passes 43 tests in 3.429 seconds; log
`.artifacts/runtime-audit-candidate-validator-2.log` SHA-256:
`eaca3fd3127d2f9f087db79f7fe66e5d2ab9dbfcd33c8964056ac73e862bf328`.

Source `809123a0` passed native build attempt 2. Independent audit verifies all
87 distinct selected tests (81 prior plus six audit groups), zero failed/ignored,
and the exact 57 evidence payloads totaling 6,999,384 bytes. Candidate:
`linux/.cache/youki-source/builds/84dacbcdaff11736597e024c6e4efe7dbc74f871afbbe0eda8d79a75868cd59c`.
The static ARM64 runtime SHA-256 is
`dcf891fa177f5b3e4326ba89ebe3455506a577b6707e70a8be63a75e65a20a84`;
manifest SHA-256 is
`cfcc5f5cc274fe5b9d74cdceb62816ff4f21a19a215d58ee05011a68f3d97f15`.
Console `.artifacts/youki-runtime-audit-build-2.console.log` SHA-256:
`a404d8686f3063cc62139780a592df41dae83bccab4aca24470f89c254fccd60`.
The authenticated ten-patch chain leaves Cargo.lock unchanged and applies with
zero fuzz; GNU patch reports the existing tenant-root +2 and console-size +22
line offsets, not a zero-offset chain.

Independent replay with dispatch/writes forbidden validates eight journal records
from four actual compiled CLI calls: version 0, invalid-bundle create 1,
missing-container exec 255 and invalid-bundle run 255. Four process births,
exact arguments, boot, timestamps, source pins, unchanged protected file metadata
and canary exclusion match; status remains complete. The journal has 3,569 bytes,
SHA-256 `cbd58f6df0e4c877eca3fc60872f605acf883d486b7f3a1fc5bfbf7641c7d51e`.
These are early-return and error-routing probes, not successful OCI payloads.

No installed runtime was replaced by these native builds. Exact-Machine
enrollment/retrieval, installed Docker invocation
mapping and the aggregate release gate remain open. Fresh workload enrollment
must precede all fixture/sentinel Docker mutations; persisted recovery additionally
needs enrollment before daemon restore can invoke the runtime, explicit retained
old-session disposition and a new-boot transition. Post-Ready enrollment alone
cannot establish that recovery coverage.

### Rebuilt Mac backend gate

The new runtime passed the release-built Mac backend gate from source `296f6bcb`:
`.artifacts/sandbox-vm-e2e/20260906T210423Z`. The original process exited zero;
all seven selected lanes passed, with 50 selected tests: runtime 19, runtime
crash/reopen 1, StateStore crash/reopen 1, runtimed 1, Machine registry 1, stack
24 and BuildKit 3. There were no failed or ignored selected tests.

Console `.artifacts/runtime-audit-backend-1.console.log` is 717,286 bytes,
SHA-256 `c0f99658acb98a566ee6d59f51e9751d28e4d0cf1e755457694a281d49ebb96e`.
Summary SHA-256:
`b8749d920add876e989c08b9e362b11ac3e1d95aa297220b0864fe923dc49bf3`.
The exact nine guest inputs were copied into the run's
`retained-guest-bundles/` directory, totaling 85,692,154 bytes, with manifest
SHA-256 `ee34ae422ae71b2e264d07cf5ae105a5f0e83ecfda48af8da89c8b12d47141ef`.
Both profiles contain the native-verified `dcf891fa…5a20a84` runtime.
Independent full-byte audit verifies all 11 JSON checksum sidecars and 4,072
regular files totaling 69,584,261,153 logical bytes, including every byte of the
64 GiB sparse cache. The in-memory inventory digest is
`fdc7862ba791d0824a32275508c1ceff182e76cbd27f1463c2ee69251fb4700b`:
sort regular-file relative POSIX paths, concatenate each file's SHA-256, two
spaces, its relative path and LF, then hash those UTF-8 bytes. This includes the
retained guest manifest and excludes directories and OCI symlinks; it is not
an on-disk aggregate release manifest.

This backend pass exercises the new runtime without diagnostic enrollment. It
does not establish Machine-bound journal capture or installed host-Docker
invocation mapping, and does not replace the required aggregate release gate.

### Machine-bound capture implementation (physical verification pending)

The DEV lifecycle harness now requires four fresh diagnostic enrollments before
any sentinel or fixture Docker mutation. Sessions are registered before dispatch;
even a normally returned nonzero enrollment remains uncertain and fences automatic
cleanup. After the monitor is joined and owned Docker objects are removed, each
Machine's journal is captured and independently replayed before public Stop.
Capture failure also fences automatic Stop; it never resets or retries a journal.

`scripts/helpers/linux_docker_runtime_audit_capture.py` uses fixed guest paths,
fresh-directory/no-clobber enrollment with the enrollment marker written last,
checked file synchronization, actual boot/runtime observations and protected-file
metadata. Capture transfers at most 1 MiB of journal bytes per command, below the
recorder's 4 MiB stream limit. Before/after snapshots and full content hashes must
remain identical through all chunks; the existing parser validates the entire
nonempty journal, up to 16 MiB, with no unmatched invocations.

`scripts/helpers/linux_docker_runtime_audit_evidence.py` binds these source-selected
commands to the original project, Environment, Machine, incarnation, staged CLI,
runtime artifacts and authenticated startup selection. Independent replay reads
original receipts only, checks the exact evidence inventory and proof readback,
and cannot dispatch or write. The focused audit family passes 41 tests, including
a 3,200-record, 1,600-invocation multi-chunk journal. The harness integration passes
54 tests. These are host tests, not installed guest execution.
The finalized broader host regression passes 749 tests in 91.794 seconds:
`.artifacts/runtime-audit-capture-host-regression-2.log`, SHA-256
`f7580543f45b7dfc70f5a5e6502f43fe57bd90ccc943e8f40a9802da92d75741`.
The earlier sandboxed run remains failed: 746 tests with 12 errors during
disposable SSH-agent startup; the final run used the local socket permissions
required by those tests and includes three subsequently completed bridge tests.

The BusyBox shell acquisition is diagnostic, not hostile-root containment:
metadata/content checks detect ordinary concurrent change, but do not establish
fd-relative protection against a malicious root path-swap/ABA attack. Workload
quiescence is provided by the harness's owned cleanup boundary, not a journal
lock. Up/startup, public Stop and persisted-recovery invocations remain outside
this capture window. Docker-operation mapping and full historical process or
namespace-reference absence remain uncertified.

Native portability attempts are retained separately. Attempt 1 failed before
container creation because the OrbStack multicall executable received the wrong
argv0; attempt 2 found the exact builder image absent from the local cache.
Attempt 3 fetched and verified the pinned image but timed out after 40 seconds
during container creation, before enrollment or any youki invocation. Its host
client was reaped, and separate exact-name inspection found no container at that
observation; the original Engine mutation remains uncertain, not a verified
rollback. No further create was dispatched.

Attempt 3 evidence is `.artifacts/runtime-audit-capture-native-3`, 19 payloads,
24,540 bytes, manifest SHA-256
`71ed4671995e63dc75327a5ce0c83a2d4e75b01bbd32e48ee196fdb0a85f9f76`.
Separate disposition evidence is `.artifacts/runtime-audit-capture-native-3-disposition`,
six payloads, 4,306 bytes, manifest SHA-256
`5ff4a92e6958920aaa1661195490b1035db0dcbfabe2e5a574c3db08a4b54046`.
At `2026-09-06T21:35:25.071328Z`, a later bounded read-only observation found
delayed creation had completed for the exact owned name
`vz-audit-capture-736956287e494c92b38cd9847fdaff70`, CID
`d9fb10044346ef5a70291a07c8da15eea17f73b47c5605070f39f70fac19a406`.
It was `created`, PID 0, never started, with the pinned image and ownership label.
That observation is separately retained in
`.artifacts/runtime-audit-capture-native-3-observation-2`, ten payloads,
16,118 bytes, manifest SHA-256
`f0bcff431f080822f1bc623f2563b01a4c4708182ef4723370396484893616e2`.
The original timeout remains failed. A separate diagnostic continuation may use
this now-resolved exact container; it must not dispatch another create or
reinterpret the original timeout as a passing command.
None of these initial diagnostic attempts certifies runtime capture or Docker
parity. The separate continuation below subsequently verifies native portability.

Source `2350aa83` starts fresh installed-Mac lifecycle candidate 7 with the nine
retained guest inputs above. Evidence is
`.artifacts/linux-docker-lifecycle-candidate-7`, console
`.artifacts/linux-docker-lifecycle-candidate-7.console.log`, retained root
`/private/tmp/vzdev-t0mvxp68`. Both Environments reached Ready, but the original
process terminated with exit 1 at the first enrollment, before any sentinel or
Docker workload fixture. No test case was retried. Original failed manifest:
`fc09ac1bac6b453cd70f09ae169b6b2ee4bbbe649a1d065fc20fa4b4894ff0e9`;
result SHA-256:
`a2228dad6f8765d96c322a0b5efdc660402559e04e9242f26e19865d86be8dbd`.

### Native portability passed; installed read-only-directory mismatch diagnosed

`.artifacts/runtime-audit-capture-native-3-continuation-2` uses the resolved exact
container above, without another create. An earlier continuation's guard rejected
only a reordered Docker mount array before Start; that guard failure is retained.
The final continuation compares the exact mount inventory by destination, rejecting
duplicates, and passes with actual guest BusyBox and the pinned runtime. One fresh
enrollment and one real `youki --version` call produce a complete two-record pair;
snapshot/chunk/final replay and denied reenrollment with unchanged history pass.
Normal Stop returns zero, and the exact owned container is removed. Independent
verification checks 69 payloads totaling 119,813 bytes, manifest SHA-256
`d216e14b9b45ffc4ef62c4ac4ce472e9e022217cea82770feb48dc2e6b33cc65`.
Journal SHA-256:
`a1e4ccf1b6ea9d8bb6233fd9366f7b55c77f74ec65e1ff014a9f006334851207`.
This is native acquisition portability, not an OCI payload or vz Machine pass.

Candidate 7's separate authenticated public-Exec diagnostic identifies the actual
Machine difference: `/mnt/linux-bin` is a root-owned `0500` virtiofs directory,
but the capture helper's directory-mode list omitted `0500`. All audit paths
are absent, so the rejection preceded enrollment creation. Protected journal
ancestors are root-owned and correct; there is no evidence justifying a UID
exception or changing initramfs ownership. The observed guest mount flags are
`rw`; host-side `VZSharedDirectory(read_only=true)` enforces the artifact share's
read-only setting, rather than a guest `ro` mount flag.

Disposition `.artifacts/linux-docker-lifecycle-candidate-7-disposition` verifies
all four exact-owner public Stops, four clean distinct filesystem journals,
graceful daemon exit, absent PID file/five sockets and unchanged Docker defaults.
Original failure evidence and stopped disks are preserved; no enrollment repair,
Docker-object removal or Delete was performed. Independent inventory: 513 payloads,
557,439 bytes, manifest SHA-256
`aca4500ccc18bd18bb0586e37c7afa3dd9b84a865ba4abad62b71568e7de8767`.
Raw filesystem diagnostic SHA-256:
`81a30adfce61de84912d7ea019f02ef15007e172fabe2761058eb1e06454c84e`.

The capture-only correction admits root-owned `0500` ancestors while preserving
the audit sink's exact `0700` requirement and every file's `0600` requirement.
The expanded focused audit family passes 42 tests in 3.297 seconds, including
rejection of a `0500` journal root. Log
`.artifacts/runtime-audit-capture-mode500-1.log`, SHA-256
`988bfe7e98f3e411c35ca7947731244798c5f6027a04b2e07024d1d628da36dd`.
The final broad host regression passes 750 tests in 193.446 seconds:
`.artifacts/runtime-audit-capture-host-regression-3.log`, SHA-256
`8badb487fa739a7b589d34eb0777294b0f126bfb4068485d8e4d4ae6d59f3682`.
Runtime/guest artifacts are unchanged; fresh installed-Mac verification remains
required before this corrected acquisition path can pass.

### Candidate 8: enrollment passes; asynchronous resize ordering fails

Installed source `2c14c55f`, unchanged runtime `dcf891fa…20a84`, completed all
four fresh Machine-bound enrollments. Independent inert replay verified exact
owners, incarnations, boots, sessions, protected metadata and empty initial
journals. This establishes enrollment, not final runtime capture.

The same run failed at first-Machine command 70, `docker exec --interactive
--tty`, after the preceding root/nonroot/binary-stream checks. The guest emitted
initial `24×80` readiness. The host resized its local PTY to `40×120` and wrote
the single `size` query 11.42 ms later; the guest answered `24×80`. No matching
`40×120` record arrived, so the dependent `exit` write was never dispatched.
At the original 30-second deadline, the owned client PID 4621 was killed and
positively reaped. Its terminal restoration checks failed; no successful
restoration or workload completion is claimed.

Local PTY resize completion is not acknowledgement of the asynchronous remote
Docker resize. The fixture only reports dimensions when queried, so these bytes
do not establish that the guest never resized later. The correction requires a
distinct guest `SIGWINCH`/actual-dimensions acknowledgement before that one size
query, while preserving the initial size, queried final size and exit assertions.
It must not add sleeps, repeated size queries or a fallback transport.

Original evidence `.artifacts/linux-docker-lifecycle-candidate-8` independently
verifies 3,439 payloads / 3,827,651 bytes, manifest SHA-256
`34805a462ec6d5339e3f8633f5cceefd650bfc3e59dbb7700583ba3aa2b7c7c6`.
No final journal capture ran; the original attempt remains failed.

Separate disposition `.artifacts/linux-docker-lifecycle-candidate-8-disposition`
retains a bounded, nonquiescent runtime-journal diagnostic and completes four
exact-owner/incarnation public Stops, distinct clean filesystem journals,
graceful daemon retirement, five absent sockets/PID file and unchanged defaults.
Its 513 payloads / 939,606 bytes independently verify against manifest SHA-256
`8752ae3f605717e9c1b47b957ab49030b4ab38d46ee03409ab7486b73ed90b46`.
Diagnostic SHA-256:
`b2f745948b7e303629bd55347c46f0be4883b71f0eb0dfdcb7d579017aaeb065`.
No Docker objects or disks were deleted, and no enrollment or workload was retried.

The standalone runtime-correlation helper validates raw journals and finds exact
CID/operation candidates in externally supplied Engine-clock windows. It keeps
ambiguous healthcheck/exec candidates and unmatched background invocations
explicit, never equates runtime admission with payload exit, and does not yet
authenticate those request windows or integrate them into lifecycle acceptance.
Its 12 offline adversarial tests pass; it is not a physical Docker proof.

The resize correction uses a private nonblocking self-pipe: the `SIGWINCH`
handler never writes buffered output, and normal control flow samples the real
guest dimensions before emitting `tty_resized`. Both direct host input and tmux
wait for this exact record before sending `size`; replay requires the same
ordering. Missing/wrong acknowledgements remain failures. Both signal handlers,
the exact pipe descriptors and guest termios are restored on exit.

Updated fixture tree SHA-256:
`60a9c9b1f27c489c28ee272f04fd6916e34544a93482c82de8ee0a7f3d39701a`;
probe SHA-256:
`1d240ff2e5964cdc7d95ab1b015c419618b3f6495a2ca5aa84447015737362f1`.
All 145 container-family tests and 21 fixture tests pass, including real owned
PTY success, wrong dimensions, missing signal, deadline and restoration checks.
The broader affected host suite passes all 800 tests in 69.500 seconds, log
`.artifacts/container-resize-handshake-host-1.log`, SHA-256
`d2e8fa70ccd1c6732863f03a75243a84b1a330669bdfa51b31e71768c722144f`.

Actual local tmux evidence `.artifacts/container-resize-handshake-tmux-1` passes
the four-record handshake, pane exit 37 and normal owned-server retirement.
Manifest SHA-256:
`7bfbccc53a92926a1fe36cef2f199060ae01caeb374a5e6aace60e372d9e178e`.
This local terminal test is not an installed-Docker pass. Runtime/guest bundles
are unchanged; fresh installed-Mac lifecycle acceptance remains required.

### Candidate 9: missing resize acknowledgement; queued-signal launch

Installed source `688a83cf` again passes all four fresh Machine enrollments,
but first-Machine command 76 emits only initial `24×80` readiness. The single
host resize to `40×120` completes; no guest acknowledgement arrives, and neither
the size query nor exit query is sent. The original 30-second deadline expires;
owned client PID 4463 is killed and positively reaped. Terminal restoration
fails. This attempt remains failed, with no final runtime-journal capture.

Original evidence `.artifacts/linux-docker-lifecycle-candidate-9` independently
verifies 3,185 payloads / 3,603,106 bytes, manifest SHA-256
`53d57441fc643f485581b708779968132f625d326505cc4b76de6b9938c20546`.
Separate disposition `.artifacts/linux-docker-lifecycle-candidate-9-disposition`
verifies 513 payloads / 944,413 bytes, manifest SHA-256
`b3f25a5675ddb1c66b999b8f1ad6c93d7d980c9abffc98f5eb1fb3f455e15b29`.
All four exact-owner public Stops have distinct clean filesystem journals;
the daemon, five sockets and PID file are gone, and defaults are unchanged.
The diagnostic retains a nonquiescent journal prefix, not a final capture.
Failed evidence, Docker objects and stopped disks are preserved.

Pinned upstream Docker CLI 29.4.0 starts the exec I/O goroutine before
`MonitorTtySize`. That function sends its initial resize HTTP request before
subscribing to `SIGWINCH`. An early host resize can therefore be lost while
initial dimensions are still being sent. Guest output readiness does not prove
that the client has subscribed. This source ordering also remains in upstream
29.8.0; no fixed-version upgrade is claimed. The installed OrbStack multicall
wrapper's private startup code has not been audited, so upstream source alone
does not prove its entire signal-subscription history.

The narrowly selected `tty-exit` launch now inherits a blocked `SIGWINCH` mask.
The parent thread adds only that signal around `Popen` and immediately restores
its exact original mask; the child handle is retained even if restoration fails.
Go preserves this inherited mask until a subscriber enables the signal, allowing
the one early resize to remain pending. This is not a resend or timeout increase.
The guest acknowledgement, dimensions, single query and exit remain unchanged.
Other plans, including SIGINT and `docker run`, do not select this behavior.
Durable plans select the opt-in before dispatch; replay verifies exact mask
union/restoration and launch ordering. The receipt explicitly describes parent
thread inheritance, not observation of every child runtime thread.

The bounded native experiment `.artifacts/queued-sigwinch-probe/evidence` uses
Go 1.25.4 with cgo on this Mac: one owned signal becomes pending before Notify
and one notification is observed afterward; the child exits zero and is reaped,
and the parent mask is unchanged. This does not exhaustively observe subsequent
duplicate delivery. Its five payloads / 7,131 bytes verify against manifest
`4c8835470d7badfa4283a2af300724a55a569af10c0337eb539c359b2737565d`.
The installed Docker wrapper was built with
Go 1.25.5, so this experiment is not an exact-binary Docker proof. Actual owned
Python PTY tests additionally prove queueing, `40×120`, termios restoration,
spawn-failure restoration and bounded child cleanup on restoration failure.
Fresh installed-Mac acceptance is still required.

The reviewed launch change passes all 840 affected host tests in 60.802 seconds,
including the image-helper foundation tests then present. Log:
`.artifacts/container-queued-winch-host-1.log`, SHA-256
`27fd0bca8612707810ea862e97d0c1166c845ddf8faae08bf654cf71bdbcbf39`.
These host tests do not certify installed Docker behavior.
