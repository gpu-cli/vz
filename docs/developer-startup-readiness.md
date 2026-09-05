# Developer startup readiness (DEV)

Normal Linux-on-macOS `vz up` measures operational readiness through the exact
Machine's retained VM and private Docker endpoint. It does not run or require
the complete Docker compatibility release suite on each startup. Release-wide
compatibility and the 0.4 aggregate remain separate acceptance gates.

The installed catalog supplies verified profile-qualified artifact pins (see
[installed catalog](installed-machine-catalog.md)). The Developer bundle now
contains a deterministic offline BusyBox rootfs with versioned public marker;
its archive SHA and build provenance are bound by the bundle's version metadata.
Legacy bundles without that input cannot silently substitute an image or daemon.

Startup measures POSIX execution and inventories executable paths and pinned
daemon mounts in the original VM. It verifies the actual youki binary against
the Machine's artifact digest before and after Docker operations. Moby's inert
built-in runtime-name metadata does not count as an installed runtime: the
default and every inspected probe container must use youki. This startup check
does not replace the release gate's recursive cached-artifact/execution audit.

The host adapter discovers the installed Docker executable, or uses the exact
absolute `VZ_DOCKER_CLIENT` override. It uses `VZ_DOCKER_CONFIG`, then
`DOCKER_CONFIG`, then the normal user Docker config directory. Compose and buildx
must be installed for that client. The executable is canonicalized and hashed;
each bounded command verifies those bytes again. On macOS, the native
administrator-managed `/Applications` directory is an explicit executable trust
boundary; Machine-store and Docker-config ownership checks are not relaxed.

One stable context is create-only, backed by a private durable claim containing
the exact Project, Environment, Machine, endpoint, config directory and nonce.
An existing foreign, edited or malformed context is not adopted or overwritten.
No operation selects a global/default Engine or changes the user's default
context. Status returns an opaque context descriptor with the persisted
incarnation and Engine identity; its availability label is explicitly persisted
state, not a live health measurement.

Bounded offline operations import the verified rootfs, create/start/exec a
container, bring up/exec/down a Compose service, and build/load/run a scratch
image through that context's embedded `docker` buildx driver. The build includes
an uncached guest `RUN`, whose output is checked in the loaded image. No registry base,
frontend download, alternate runtime, `docker-container` builder bootstrap or
fallback daemon is used. Exact disposable probe containers, Compose objects and
images must be removed before a successful immutable receipt is emitted. Normal
Machine-owned BuildKit cache is explicitly retained; no global prune is allowed.

The private per-Machine journal admits each command before dispatch and retains
bounded raw stdout/stderr and checksums. An uncertain mutation is not retried or
declared absent. Such failures retain original Machine ownership for `vz stop`
and require exact journal-aware recovery before another probe can run. That
mutating-failure recovery, ownership-safe Delete/context removal, host bind-path
translation and the full compatibility matrix remain unfinished.

A repaired host-client prerequisite may be checked by a subsequent Up only when
the retained, exact-owner journal positively proves that every admitted command
was a recognized read-only query and no resource mutation was admitted. The
failed attempt is archived before supersession. This does not replay a failed
mutating command or provide mutating-failure recovery.

Stop retains the logical context descriptor while removing its live socket.
Subsequent Up must preserve the context's owner, name, endpoint and config
directory while binding new activation evidence to the new incarnation. Native
macOS and Hardened Machines do not acquire implicit Docker capabilities.

This Stop/Up behavior assumes the original daemon remains alive. Supported
daemon-crash recovery is still missing: a stale control socket blocks autostart,
and a new daemon cannot adopt previously active Machines without authoritative
backend reconciliation. See the [catalog recovery limitation](installed-machine-catalog.md).

The separate [installed startup harness](../scripts/helpers/installed_developer_startup.md)
tests this path using signed staged product binaries and real local vz-managed
Machines; passing it is DEV operational evidence, not 0.4 release certification.

## Local-Mac evidence: 2026-09-05

`.artifacts/installed-developer-startup-candidate-2/` passed the installed startup
harness with four Developer Machines across two named Environments in one
worktree, plus a sequential Hardened Machine. Six distinct activation attempts
(four initial, two after Stop/Up) each completed 45 startup commands, including
uncached Buildx execution. All six immutable receipts have matching, separately
retained before/after runtime inventories. Three additional independent host
Docker/Compose/buildx workloads passed through the managed contexts.

Environment/Machine/context identities and Engine IDs persisted across Stop/Up;
the neighboring Environment remained usable. Hardened acquired no Docker
capabilities or descriptor. Final public Stop receipts, socket removal, observed
graceful daemon termination and unchanged daily/isolated Docker defaults passed.
All 1,124 evidence-file checksums were independently verified. The exact signed
CLI/daemon are retained in
`.artifacts/developer-startup-release-SlDuQ1/signed-inventory-root/`.

The preceding `installed-developer-startup-candidate-1` failed before Docker
workloads because its new inventory expected the outer-init runtime mount inside
the overlay chroot. That candidate and successful Stop/daemon cleanup remain
retained; the corrected candidate verifies the actual guest runtime copies.
These are distinct development candidates, not retries within a release gate.

Remaining findings are tracked: `vz-7ez.1` covers mutating-probe recovery;
`vz-7ez.2` covers macOS full-close shutdown warnings without masking truncation.
`vz-ehz` covers exact daemon/control-socket recovery after a crash.
The passing workload results do not certify every stream-close condition,
named-volume/workspace semantics, native macOS, the 63-scenario Docker contract,
or the mixed-target 0.4 aggregate. Nothing was installed over the daily binaries.

## Installed Compose gate development

The [host Compose entry point](../scripts/helpers/linux_docker_e2e.md) now binds
normal public Up activation identities, installed configuration/artifact pins,
startup runtime inventories and exact host contexts before fixture mutations.
It implements an eight-recipe DEV slice with independent raw-evidence replay;
`--suite all` rejects before provisioning because the full63 dispatcher is not
implemented. The standalone Python image input pins actual ARM64 registry
metadata; physical suitability still requires pull/inspection/execution in each
selected Machine. Offline assertions alone do not establish compatibility.

The first physical attempt, `.artifacts/linux-docker-compose-candidate-1/`,
failed during initial Machine boot, before any Compose fixture ran. Guest exec
timed out after30s. The fresh private64GiB disk retained its format-intent and
substantial formatting writes, strongly implicating first-format work, but the
old error lacked a phase label and does not conclusively identify the command.
The implementation now separates a bounded30s filesystem probe from180s
first-format work and reports phase/device/purpose/budget/elapsed diagnostics.
The second candidate booted all four Machines with this change; that does not
conclusively identify the earlier timeout or prove formatting under pressure.
It does not turn the failed run into a pass or authorize reuse/reformatting of
its retained disk.

Public Stop was not admitted: the failed Up retained its fence and Running
lifecycle. Early boot also discards VM-stop results before registering the VM,
so the retained Runtime does not establish a recoverable exact early-boot VM
handle. This separate recovery gap remains in `vz-ehz`. The twice-fingerprinted
test daemon received SIGTERM and exited with socket/PID removal and a graceful
shutdown log. `.artifacts/linux-docker-compose-candidate-1-reconciliation/`
records only that host-process disposition: **not** public Stop, guest
quiescence, journal reconciliation or Delete. All failed state/storage remains.

The second candidate, `.artifacts/linux-docker-compose-candidate-2/`, passed
normal Up for four Developer Machines, authenticated startup/activation proofs
and four independent sentinel containers. The first immutable Python image pull
then failed TLS verification (`x509: certificate signed by unknown authority`).
The Linux builder had public roots but neither guest initramfs nor overlay root
included them. No Compose recipe ran. The new pinned public
[CA payload](../linux/ca-trust/README.md) is verified during assembly and boot;
the third physical candidate below verifies actual HTTPS behavior.

Separate `.artifacts/linux-docker-compose-candidate-2-reconciliation/` evidence
records positive public Stop for all four Machines and graceful shutdown of the
exact test daemon, with both Docker defaults unchanged. Failed-pull uncertainty,
all Docker objects and stopped Machine storage remain retained. This is runtime
quiescence, not successful Docker cleanup, Delete, or a retried passing test.

The third candidate, `.artifacts/linux-docker-compose-candidate-3/`, passed
normal Up and exact public CA-hash checks on all four Machines. The first
Machine's unmodified host Docker client successfully pulled the immutable
Python ARM64 image over verified HTTPS, inspected its manifest and platform,
executed Python and a POSIX shell, and built the Compose fixture through its
private embedded builder. All eight first-Machine recipe assertions completed.
The run nevertheless **failed** cleanup: never-started dependency-blocked
containers have configured network names but empty Engine network/endpoint IDs;
the ownership check rejected those unmaterialized attachments. The second and
third Machine slices did not run, and no successful independent slice replay or
Docker parity is claimed. All 9,319 original evidence checksums were independently
verified. The correction accepts only exact owned network names on proven
never-started containers with no endpoint state or network membership; attached
networks still require matching names and IDs. Driver and independent-validator
adversarial regressions passed before the next candidate.

Separate `.artifacts/linux-docker-compose-candidate-3-reconciliation/` receipts
positively stopped the four exact Machines and gracefully shut down their
fingerprinted daemon. Host and private Docker defaults were unchanged. Failed
cleanup evidence, Docker resources and stopped Machine storage remain retained;
this separate disposition does not turn the candidate into a passing run.

### Passing installed Compose DEV checkpoint

The fresh `.artifacts/linux-docker-compose-candidate-4/` run passed with zero
test-case retries. Normal installed public Up booted four Developer Linux
Machines across two named Environments in one worktree. Eight Compose recipes
ran on each of three Machines, with 307 raw commands per slice independently
replayed: create, health/dependency ordering, exact Exec streams/exit37, network
paths and denials, host-written volume persistence, scaling, blocked unhealthy
dependencies, and failure propagation. Each selected Machine independently
pulled/inspected/executed the pinned HTTPS Python image and built its fixture
through its own private embedded builder.

Four continuously observed sentinels (350 observations each) retained Engine/container identities,
zero restart counts and host-written marker bytes. Their observations cover
each slice's sibling/neighbor interval; they are not network-service conformance.
Owned Compose fixtures, sentinels and input images were removed, both public
Environment Stops succeeded, all four endpoints became unavailable, and the
fingerprinted daemon shut down gracefully. Daily and isolated Docker defaults
remained unchanged. Stopped Machine disks, contexts and BuildKit cache remain
for inspection: this is not Delete or a complete leak audit.

All 27,190 evidence checksums and the three raw slice replays were independently
verified after completion. Evidence anchors:

- `result.json`: `de760ba1b73668540723cb10d5f8c803822114123d10b9031c1fc8848e23fef5`.
- `checksums.sha256`: `2a972283f58f4337e13b73b17a267e9d8d3975083abbaec32568f02fd18b8ad2`.

The signed artifacts are local DEV candidates, not a published/notarized 0.4
release. The full 63-scenario Docker dispatcher, additional Buildx/interactive/
image-roundtrip coverage, native macOS, public Delete and the three-phase mixed
Environment release gate remain unfinished. No release-scenario PASS is emitted.

## Backend regression checkpoint

The subsequent fresh run `20260905T201331Z` failed its runtime lane (7 passed,
12 failed). Alpine smoke-test serial output directly identified a CA-placement
regression: the initial installer compared its pin to Alpine's existing distro
bundle, rejected the difference, and exited init before agent startup. Minimal
root, Hardened, and normal Developer shared-VM readiness paths passed. The
correction uses a separate `/etc/vz/ca-certificates.crt` control-plane trust path
and preserves distro certificates. Neither longer boot timeouts nor reuse of
the failed run can certify the correction.
Exact failed guest bundles are retained under that run's `failed-guest-bundles/`.

The corrected fresh run `20260905T205453Z` passed all seven backend lanes:
runtime19, runtime-generation crash/reopen1, StateStore crash/reopen1, daemon
teardown1 (18 exact child/socket dispositions), Machine registry1, stack24 and
BuildKit3. All selected tests passed with none ignored; all required evidence
flags were validated. `raw-evidence.sha256` additionally binds 107 raw logs,
summaries and supporting files, independently verified after completion.
This is the current backend regression checkpoint, not Docker63 or aggregate
0.4 certification. The passing installed Compose DEV candidate above supplies
separate host-client workload evidence, not the remaining full release gate.

The required `scripts/run-sandbox-vm-e2e.sh --suite all --profile release` run
passed at `.artifacts/sandbox-vm-e2e/20260905T183551Z/summary.txt`, with raw logs
and pinned youki/BuildKit runtime inventory retained beside it. Selected checks:
runtime 19, two generation-recovery checks, daemon teardown 1, Machine registry 1,
stack 24 and BuildKit 3. The harness's defined test selection remains unchanged;
this is backend regression evidence, not the missing 63-scenario Docker gate.

Three preceding full-run failures remain retained: `20260905T172026Z` exposed
the crash fixture's unsafe stale-socket assumption; `20260905T175255Z` exposed
host tests incorrectly included in its exact physical-test inventory;
`20260905T181407Z` exposed a four-file registry fixture copy that omitted the
newly declared probe. The corrected crash fixture removes only its captured
socket after positively reaping its exact child. Its host regressions run in a
separate target, and all 18 physical cleanup receipts passed strict validation.
This fixture authority does not implement user-facing daemon-crash recovery.

Registry fixture copies now pass the production verifier before and after copy,
and evidence binds the exact optional probe digest/file identity to retained
version bytes. The focused physical run `20260905T183319Z` and the subsequent
full run passed. The installer legacy alias also carries only the declared,
verified probe; replacing or removing an old probe requires its previous
declaration and matching bytes. Eleven installer regressions passed.
