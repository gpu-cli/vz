# Linux Machine Docker artifacts and lazy supervision

Status: **DEV**, macOS host × Developer Linux Machine — private Machine runtime
admission, artifact pinning, persistent disks and guest supervision have focused
local-VM evidence. The host endpoint transport is implemented; actual host
container execution and full Docker compatibility are not yet certified.

Docker compatibility is an implicit capability of each Linux Developer Machine.
Reconciliation provisions or verifies its requirements; the Machine's private
Engine may still start lazily on first API traffic. The
host endpoint adapter (`vz-7ez`) calls the guest agent's server-streamed
`EnsureDocker` RPC for the selected Environment/Machine. Native macOS and native
Windows Machines do not use this contract.

## Host artifact and Machine-state layout

`ensure_docker_artifacts()` owns the checksum-pinned, immutable binary cache.
Mutable state must be owned by stable Environment and Machine identities rather than the
shared artifact directory:

```text
~/.vz/docker/
├── bin/
│   ├── containerd
│   ├── containerd-shim-runc-v2
│   ├── docker-init
│   ├── docker-proxy
│   └── dockerd
└── version.json

<daemon-owned-environment-root>/<environment-id>/machines/<machine-id>/docker/
└── data.img
```

Physical state and endpoint paths are backend-owned details. Users address the
Environment/Machine by name/ID or managed context and do not construct paths.

Docker Engine 29.7.2 for `linux/arm64` is pinned from Docker's official stable
static-download index (the current stable ARM64 release when checked on
2026-09-02). The archive SHA-256 and an independent SHA-256 for every installed binary are pinned. Cached files
are re-hashed on every reuse. The installer rejects symlinks, non-executable or
non-regular files, and any inventory other than the five files above. In
particular, it never extracts `runc`, `docker`, or `ctr` from the upstream
archive. Installation uses a private OS advisory lock (released by the kernel
if the installer exits), stages a complete generation, and
retains the prior generation until the metadata commit succeeds.

Each Linux Machine's `data.img` is a private sparse persistent disk. Docker state
is never placed in the initramfs or shared with another Machine. Its backend
must attach the owned image as `/dev/vda`, expose
the verified artifact binaries through the read-only `vz-docker-bin` VirtioFS
tag, and expose the operation-owned pinned kernel artifact directory (containing
youki) through the read-only `linux-bin` tag. Runtime admission captures the
effective artifact/configuration identities before constructing any sibling
Machine; recovery loads existing pins without consulting a mutable catalog.

## Guest supervision contract

`EnsureDocker` streams validation, startup, wait, and ready events for one
Environment/Machine. On first call, the guest agent mounts the two read-only
binary shares and requires that Machine's existing ext4 `/dev/vda` mount at
`/var/lib/docker`. It then supervises external containerd and dockerd, keeping
all state and logs on that persistent mount and all sockets under
`/run/vz-docker`. Readiness requires an HTTP `GET /_ping`
against the Engine Unix socket to return status 200 with body `OK`.

The guest refuses startup unless:

- the Docker binary share contains exactly the five allowlisted files;
- `/mnt/linux-bin/youki` exists;
- the containerd daemon plumbing config and Docker daemon JSON config are
  regular, non-symlink files;
- the Docker daemon JSON config does not add or override runtimes; and
- `/var/lib/docker` is an ext4 virtio-block mount, not initramfs storage.

The shim name `containerd-shim-runc-v2` describes containerd's protocol and is
not an installed OCI runtime. Runtime selection is not inferred from
containerd's CRI-only TOML runtime tables. Instead the supervisor starts
dockerd with immutable `--add-runtime youki=/mnt/linux-bin/youki` and
`--default-runtime youki` arguments. Moby translates that registered runc
drop-in into the runc-v2 shim's `BinaryName` task option. Dockerd is also
pointed explicitly at the persistent `/var/lib/docker/config/daemon.json`, so
an initramfs configuration cannot silently supply another runtime.

## Exact-Machine host transport

The daemon's trusted-library `MachineDockerEndpoint` accepts an exact retained
Machine activation lease and a private, effective-user-owned directory. It
refuses Hardened profiles and pre-existing endpoint paths before starting the
Engine. A staged mode-0600 socket is atomically published without replacement.
Its bounded client supervisor retains the activation until every relay has
joined, then removes only the recorded socket inode. An unverified staging path
is preserved and reported, never silently deleted or counted as clean teardown.

`DockerForward`, introduced in protocol revision 7 and retained in revision 8,
is a dedicated bidirectional byte stream
to the already provisioned `/run/vz-docker/docker.sock`. Callers cannot supply
guest paths, hosts or ports. Bounded data frames, directional EOF and a guest
write-completion acknowledgement preserve both orders of half-close without
discarding queued input. This is not the older generic TCP forwarding path.

The transport does not yet publish managed contexts, translate host bind paths,
recover endpoint ownership after a daemon crash, or integrate with production
`vz up`. Same-UID host processes and directory ACL configuration remain trusted;
there is no new cross-user authorization claim. The public interface remains
the planned per-Machine context returned by status, not a user-constructed path
or global `DOCKER_HOST`.

## Pending integration and evidence

Runtime admission now initializes only proven-new private ext4 disks and
materializes the containerd and Docker configurations. Existing disks are never
reformatted. Legacy state migration, production lifecycle/context integration,
and the complete host Docker/Compose/buildx matrix remain unfinished.

The first focused host run at
`.artifacts/sandbox-vm-e2e/20260905T062616Z/` is a retained failure: the Mac's
Docker CLI reached distinct Engines and imported an offline fixture image, but
actual `docker run` failed during youki creation. Both Engines also reported
missing memory-limit support. Neither successful API traffic nor previous
guest-local readiness evidence proves container execution or full compatibility.

The Developer kernel now explicitly enables memory cgroups and bridge netfilter.
The rebuilt kernel and effective configuration are recorded in
`.artifacts/developer-kernel/run-j5o19E/summary.json`. The subsequent physical
run `.artifacts/sandbox-vm-e2e/20260905T064139Z/` reports memory-limit support
on both Engines, but still fails container creation with the old runtime; its
guest diagnostics are retained. That binary reports cgroup-v2 support disabled.
The replacement source-build recipe in `linux/youki/` pins the upstream commit,
Cargo lockfile, native static dependencies and builder image, and explicitly
enables cgroup v2, device filtering and seccomp. Runtime replacement is not
certified until actual host container execution and resource checks pass.

The feature-enabled static runtime was built and verified, but candidate
`.artifacts/sandbox-vm-e2e/20260905T070256Z/` still failed its first host
container creation. A bounded, read-only observer in the diagnostic fixture
retained the disappearing shim log at
`.artifacts/docker-create-diagnostic-YnGp82/test.log`: youki rejects the OCI
specification's `time` namespace. The Developer kernel already has
`CONFIG_TIME_NS=y`; this is a runtime compatibility failure, not missing kernel
support. The pinned containerd error reader expects lowercase `level: error`
and `msg`, whereas youki emits `level: ERROR` and `message`, explaining why the
ordinary Docker response loses that detail. Diagnostic runs are not release
conformance evidence and do not replace the required unmodified host tests.

The reviewed time-namespace backport was source-built as runtime
`8c764ebc546947d13ab2233f68346c8b8fef6e94769677ff34a361e89a2610ba`.
Both fresh profiles in `.artifacts/linux-source-candidate-wEZ458/` passed
case-preserving kernel/BusyBox builds and guest-agent compilation. The first
staged diagnostic (`docker-create-diagnostic-rk4NCo`) stopped before container
creation because the fixture selected an old host BusyBox; that fixture now
uses the selected Developer bundle and still verifies its bytes against the
guest. The next run (`docker-create-diagnostic-fXWrLv`) reaches cgroup setup
but fails device-policy BPF loading with `EINVAL`. Its raw shim log retains the
exact error. This advances the diagnosis beyond the old namespace rejection;
it does not prove completed container creation, exec, endpoint teardown, or
Docker compatibility. Device filtering remains required, not disabled to pass.

The coherent youki 0.7.0 source candidate (`c8888254617a9db6d0871248b7d7354b9be2f8ffe28a67dfd1ee58cabe6b76c7`)
includes the upstream time-namespace and device-BPF corrections. With the
audited bundles in `.artifacts/linux-source-renewed-if4WKo/output`, actual Mac
Docker create/run and default time-namespace exec checks succeeded on both
Developer Linux Machines. The stronger security checks still fail: this is
**DEV diagnostic progress, not a passing Docker gate**.

`.artifacts/docker-create-diagnostic-3cHJRf/test.log` establishes two independent
exec defects. Exact Docker-inspected init PIDs have seccomp mode 2 and isolated
PID/mount/time namespaces. Exec joins those namespaces but has seccomp mode 0.
Its root device/inode is `2:1`, while the actual container init roots are
`35:917575` and `35:1572936`. Mount observations show exec sees the outer guest
initramfs root with the container root below `/mnt/merged`. Namespace equality
alone therefore cannot establish container filesystem isolation. This is a
container-to-Machine filesystem boundary failure; it is not evidence of escape
from the VM to the Mac.

Source review identifies dropped tenant seccomp configuration, incorrect
optional-no-new-privileges filter ordering, and missing restoration of the
container process root after entering its mount namespace. Auditable local
runtime corrections and physical root/PID/mount/seccomp regressions are in
progress. They must pass through the same unmodified host Docker client without
runtime substitution, privilege escalation, or relaxed security assertions.

Do not close the Docker wave beads until `scripts/run-linux-docker-e2e.sh`
passes in a local `vz`-managed Linux VM and its evidence proves that youki is
the only OCI runtime binary present in the guest.

The locally patched candidate
`92c75a943cf2ef1e0f31059b11e98ec9ac3b133341c25ea366ba89c47c17af16`
with protocol-8 bundles in `.artifacts/linux-security-candidate-QpUYnT/output`
passes the bracketed init/exec root and namespace observations in the retained
Mac diagnostic `.artifacts/docker-create-diagnostic-bb6ROZ/test.log`. Init and
exec also both report seccomp mode 2. This run remains **FAILED**, before custom
seccomp and the later lifecycle checks: its TUN-denial assertion was incorrect.

Pinned youki 0.7 `common::default_allow_devices` explicitly grants TUN
`c 10:200 rwm`, as does runc 1.3's compatibility default. The BusyBox probe did
open the device, but this was not a default-denied target. It does not demonstrate
absent cgroup enforcement. The supplied OCI rule list and runtime-added defaults
must not be conflated; exact explicit-deny precedence remains a separate policy
review concern, not something this differential test certifies.

Device proof schema 2 instead uses loop-control `c 10:237`: it is absent from
the pinned runtime's allowed defaults, present in both `CONFIG_BLK_DEV_LOOP=y`
kernels, and uses `nonseekable_open` in Linux 6.12 `loop_ctl_fops`. BusyBox 1.37
`dd count=0` opens the descriptor but issues no reads or device ioctl. The
default case must return exact `EPERM`; the positive control changes only the
explicit device-cgroup rule. Old TUN receipts cannot pass the version-2
validator. No runtime policy was weakened or modified to fix this test, and a
new physical run is required before claiming device enforcement.

That corrected focused run passed on the local Mac:
`.artifacts/docker-create-diagnostic-xFIDk5/` retains the signed release-profile
test driver, input checksums, raw host Docker commands, serial logs, and
`machine-runtime-registry.json`. It proves both private Developer Machine
endpoints, bracketed exec root/namespace isolation, the loop-control denial/grant
pair, default/custom seccomp syscall behavior for init and exec, per-container
memory limits, isolated volumes, stdin half-close/trailing output, and events.
Original-runtime Stop receipts also prove endpoint shutdown and the sibling
Engine remains usable. The independent focused endpoint and live-session
validators passed against the retained raw receipts.

This is still **DEV focused backend evidence**. It uses a signed test driver,
not the installed five-verb public CLI. Compose, buildx, managed contexts, and
the complete Docker matrix are not covered. The diagnostic omitted the Go
probe's source/toolchain provenance fields and uses a diagnostic serial-log
layout, so it cannot pass the full registry evidence preflight or substitute for
`scripts/run-sandbox-vm-e2e.sh --suite all --profile release`. That normal gate
must collect fresh canonical evidence before task completion.

The subsequent normal release-profile local-Mac sandbox gate passed at
`.artifacts/sandbox-vm-e2e/20260905T100246Z/`: runtime, both runtime-generation
recovery lanes, runtimed, Machine registry, stack, and BuildKit; `failed=none`.
The canonical registry evidence passed the full preflight, including provenance
and serial-log layout. The run used the renewed case-sensitive-source kernel
bundles and patched youki candidate above. This supersedes the diagnostic's
provenance limitation for this backend gate, without altering its retained raw
evidence. It still does not certify host Compose/buildx compatibility, managed
contexts, the five-verb lifecycle, or the complete 0.4 aggregate release gate.
