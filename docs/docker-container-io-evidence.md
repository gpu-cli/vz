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

## Process observations: next installed candidate

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
and incomplete observations prevent normal cleanup. Unit/replay tests do not
establish physical acceptance of this new probe; a fresh installed run is required.

Historical OCI invocation evidence still needs an internal youki audit mechanism,
not a substitute runtime wrapper or faster process polling. Containerd invokes
youki for container create/start/kill/delete and detached exec admission, but it
kills exec processes directly and removes their IO/pidfile without extra youki
kill/delete invocations. Audit expectations must reflect those actual paths.
