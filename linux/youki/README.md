# Pinned source-built youki

Both Linux profiles use the same ARM64-musl youki binary, built from the exact
upstream 0.7.0 commit in `inputs.env`, with the explicitly identified local
corrections described below. This recipe selects
`v2,cgroupsv2_devices,seccomp` explicitly. It does not enable systemd, v1, WASM,
or another OCI runtime. Linux init does not use systemd.

Upstream 0.7.0 includes the [time namespace implementation (#3550)](https://github.com/youki-dev/youki/commit/59fbf00fa2fb6ccc33b5252b9ab2b2936efa3ffb),
[BPF options-size initialization fix (#3340)](https://github.com/youki-dev/youki/commit/0fb71e3ea77da9119bcf040c43eea813f0127244),
and [exact cgroup filesystem path fix (#3355)](https://github.com/youki-dev/youki/commit/6668787d87f2fac3d763f2aef1e36c144fa56881).
Docker 29.7.2 requests a new time namespace when the kernel supports it; the
runtime implements that isolation and enforces cgroup-v2 device filtering.
No namespace stripping, device-policy bypass, or fallback runtime participates
in this build. The previous 0.5.7 backport and its evidence
remain in retained candidate/build directories, not in the current source recipe.

`seccomp-exec.patch` is a **locally authored vz patch**, not an upstream-reviewed
backport. Stock 0.7.0 drops `linux.seccomp` while adapting the original spec for
`exec`; namespace joining does not inherit the init task's filter. The patch
copies the original profile without changing rules, flags, or absent-profile
semantics. It also chooses filter installation by the value of
`no_new_privileges`: omitted/false before dropping capabilities, true afterward.
The adapter regressions retain upstream's ordinary-CLI default of enabled NNP
when omitted; a process.json with omitted NNP retains omission. The installation
phase test independently covers all three actual process values.
The patch SHA256 and identifier are pinned in `inputs.env`, applied offline with
zero fuzz, and included in candidate evidence. The runtime's commit string has
the `+vz-seccomp-exec-v2+vz-tenant-root-v1+vz-runtime-log-v1+vz-executable-permissions-v1+vz-tenant-cgroup-v1+vz-run-keep-v1+vz-foreground-wait-v1+vz-console-size-v1`
suffix so it cannot be mistaken for vanilla upstream.
The original upstream source archive and Cargo.lock pins are unchanged.

Version 2 also corrects the parent-side seccomp listener setup: ordinary filters
do not require lifecycle Container state. Notify profiles receive a separate
read-only snapshot of the original container ID/bundle/annotations and the new
payload PID, with OCI status `running` for exec. The tenant's mutable lifecycle
Container remains absent, so exec cannot overwrite init's persisted PID/status.
Source regressions require no-profile/default/custom non-notify operation without
Container state and exact, unchanged original metadata for init/exec notify.

`tenant-root.patch` is a separate **locally authored vz patch**, applied after
the seccomp patch. Mount namespace entry alone can restore a broader root than
the init task's chroot, as observed with the guest's initramfs boot. The patch
pins one init proc directory, acquires root and every selected namespace through
that directory, and uses those handles without later numeric-PID path lookups.
It checks the pinned task's root is still live and unchanged at acquisition and
after namespace entry, then restores root/cwd before privilege or seccomp drops.
Owned, close-on-exec handles are above stdio/preserved descriptors; the child
consumes and closes them on both success and error. No mounts are remade and no
broader-root fallback is permitted. Upstream state has no persisted PID birth
identity, so this does **not** claim to authenticate stale state before the
initial proc-directory acquisition. Root-entry ordering/failure/handle-lifetime
regressions are required in artifact evidence; actual root isolation and custom
seccomp syscall denial still require the physical host-Docker gate.

`runtime-log.patch` is a third **locally authored vz patch**. It preserves
containerd-compatible JSON `level` (lowercase), `msg`, and RFC3339 `time`, while
retaining escaped structured fields without allowing them to override metadata.
JSON file logging appends complete records across invocations; text formatting
is unchanged. Five formatter/file regressions and a real compiled-runtime
invalid-create failure are required candidate evidence. The independent
[pinned-containerd decoder replay](runtime-log-decoder.md) checks the actual
failure output on the host. This diagnostic fix does not certify successful
BuildKit startup or Docker parity.

`executable-permissions.patch` is a fourth **locally authored vz patch**.
Upstream's executable preflight incorrectly checks only the other-execute bit;
vz's private, root-owned `0700` Docker init binary therefore fails before exec.
The correction accepts any execute bit on a regular file. The unchanged kernel
exec call still enforces credentials, DAC, ACLs, and mount execution policy;
host artifact modes, isolation, and `--init` are not changed. Four regressions
cover exact mode matrices, real default-executor validation, directory/missing
rejection, and actual owner-only `0700` execution paired with `0600` kernel
denial. The last requires the pinned Alpine builder's real BusyBox and root
ownership, observes exact output/exit 37, and retains both child streams/status
as JSON proof lines in `executable-permissions-tests.txt`; it never skips.
The candidate validator independently checks those proofs. These build-time
checks do not replace the fresh installed Mac Buildx gate.

`tenant-cgroup.patch` is a fifth **locally authored vz patch**. BuildKit's pinned
image enables `BUILDKIT_SETUP_CGROUPV2_ROOT=1`: it moves its processes into an
`init` child before enabling domain controllers at its private cgroup root.
The original container cgroup then cannot accept ordinary processes. For an
implicit filesystem-v2 exec only, the patch permits an `EBUSY` fallback into
the live init process's strict descendant cgroup. Explicit sub-cgroup choices,
including the root override, and v1/systemd behavior remain unchanged.
The proc and cgroup handles are pinned, descendant traversal rejects symlinks,
and membership/identity checks fail closed. This path creates no cgroups,
enables no controllers, and never converts a domain to threaded mode.
Six native regressions cover routing, error discrimination, ownership and
descriptor lifetime; they do not replace actual nested BuildKit and Docker exec
verification on the Mac. The installed builder gate separately requires a
private cgroup namespace, exact setup environment, and external read-only
cgroup observations. This integration remains DEV until its physical gate passes.

`run-keep.patch` is a sixth **locally authored vz patch**. Upstream parses
foreground `run --keep` but unconditionally deletes state and cgroups after the
payload exits. BuildKit 0.19 explicitly requests `--keep` and later performs a
normal delete; its asynchronous cgroup cleanup can discard that deletion error.
Successful RUN/export alone therefore did not establish correct cleanup.
The correction persists stopped state without the reaped init PID and retains
the cgroup/configuration for explicit deletion. A failed wait retains uncertain
ownership and returns the error; a failed state save is also an error. The
default foreground deletion/error ordering, detached behavior, and build/start
failures are unchanged. Seven required native regressions cover state/config
retention, exit codes, default cleanup, normal/forced explicit deletion, wait/save
errors, invalid bundles, and real BusyBox children reaped by the foreground
waiter. State-only deletion fixtures do not prove kernel cgroup removal.
Actual retained cgroup ownership and the absence of the subsequent BuildKit
missing-container cleanup error still require the rebuilt full Mac backend and
fresh installed Docker gate; old failed candidates remain failed evidence.

`foreground-wait.patch` is a seventh **locally authored vz patch**. The default
probe in installed candidate 7 left an exit-37 zombie while its exact parent
runtime slept in ARM64 `rt_sigtimedwait` (syscall 137). Stock foreground run
starts the payload before blocking SIGCHLD, then waits for a notification before
checking waitable children. An early notification can already have been discarded.
The correction drains nonblocking child statuses after the existing signal block
and before every signal wait. An already-exited init is reaped immediately; a
later exit queues its notification under the blocked mask. Signal forwarding,
existing exit-code mapping, wait errors, detached execution, and keep/default
cleanup selection are unchanged. No child-inherited signal mask is changed.
Four native regressions use actual BusyBox children: already-exited 0 and 37,
already-signaled SIGTERM, and a live-child SIGTERM forwarding control. WNOWAIT
observes exited children without reaping, and missing pending SIGCHLD is required
before entering the real waiter. An unrelated child is also drained. Isolated
test processes retain four JSON proof records, enforce owned timeouts, and use
parent-death signals to avoid leaving a live payload after test failure.
The existing seven keep tests alone preblock SIGCHLD before spawning and did not
cover this production ordering race. Source/native evidence does not replace
the unchanged fast-payload installed probe and full backend verification.

`console-size.patch` is an eighth **locally authored vz patch**. The runtime
must honor OCI `process.consoleSize` when allocating a terminal, before handing
the console to its caller. A later Docker resize does not replace that initial
size contract. Four dedicated native regressions require real PTY dimension
readback, omitted/zero semantics, overflow rejection, and spec/callsite routing.
The complete `console-size-tests.txt` output, exact patch bytes and patch pin
are mandatory candidate evidence; missing, ignored, duplicate or failed tests
are rejected. These are bounded unit and PTY/ioctl checks, not a claim that a
container payload or Docker workflow ran. The fresh installed Mac lifecycle
gate and rebuilt backend still must pass; the old failed candidate is retained.

The Rust 1.96.0 native ARM64 Alpine 3.22 builder is pinned by its platform manifest
digest. All additional APKs, including transitive native-library dependencies,
are individually SHA256-pinned in `apk.sha256`. APK installation is offline;
Cargo fetch uses the pinned upstream lockfile and compilation is frozen/offline.
No floating package index, host Homebrew library, upstream `cross` image, or
official featureless runtime fallback participates in compilation.

```sh
# Network/disk preparation only, safe to separate from kernel compilation:
YOUKI_DOCKER_CONTEXT=orbstack bash linux/youki/build.sh --fetch-only

# Compile and verify a candidate without changing either installed profile:
YOUKI_DOCKER_CONTEXT=orbstack bash linux/youki/build.sh --build-only
```

Docker builds and pulls require an explicit `YOUKI_DOCKER_CONTEXT` (for example
`orbstack` on macOS or `default` on native Linux). Validated cached installation
needs no Docker context or daemon. Builds use two Cargo jobs.
The normal `make docker-build` and `make docker-build-all` targets prebuild youki
on the host before entering the existing kernel/guest-agent builder; their
inner make only revalidates the shared candidate, with no nested Docker daemon.
Candidates, verified input downloads, and exact build contexts are retained in
`linux/.cache/youki-source/`; `YOUKI_CACHE_DIR` selects another build cache.
Missing immutable remote inputs fail closed; mirror/cache the exact checked
bytes if Alpine removes an old package. Do not substitute newer package bytes.

Installation (`--install /absolute/path/youki`) occurs only after independent
validation and atomically replaces a regular output file. A cached candidate is
revalidated before every reuse. Existing output remains untouched after fetch,
compilation, or candidate-validation failure. The advisory build lock is released
by the kernel after its holder exits; the inert lock file is never auto-deleted.
The child retains the lock descriptor if its original caller exits.

Evidence includes the binary, executed `youki features` output, ELF inspection,
version, source-lock checksum, exact APK/input manifests, and Cargo feature tree,
all covered by `evidence.sha256`. The host validator independently parses the
binary's ELF headers and rejects an interpreter or dynamic NEEDED entry.
`features.linux.cgroup.v2` must be true. Upstream 0.7.0 does **not** report seccomp
support in `features` even when enabled; the locked build dependency evidence
is not a substitute for real seccomp/device/resource-control behavioral tests.
Candidate evidence also includes passing upstream namespace, offset-channel,
invalid-spec, BPF load/attach/query/error, and device-program allow/deny unit tests.
Separate local regression evidence checks actual tenant adaptation with default,
custom, and absent profiles through both argument and process-file entry paths,
and omitted/false/true NNP values. These adaptation tests do not create containers
or prove custom syscall denial; the physical gate must test init and exec against
both Docker's default policy and an explicit custom deny rule.
The upstream lock selects libbpf-sys 1.7.0 and libseccomp 0.4.0; checks require
those exact dependency versions. Mocked BPF unit tests do not prove kernel policy
enforcement, and upstream contest source presence does not mean that suite ran.
The local Machine Docker gate must still exercise create, exec isolation, actual
device denial, and seccomp behavior against the resulting runtime.

Offline validator tests:

```sh
python3 -m unittest discover -s linux/youki -p 'test_*.py'
```

This build alone does not certify Docker compatibility, Hardened isolation, or
the 0.4 gate. Run the required local vz-managed Linux Machine tests against each
updated profile and retain their artifacts before claiming those behaviors.
