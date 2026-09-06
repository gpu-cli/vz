# macOS bootstrap integration handoff (DEV)

Tracks `vz-mzs.11.4`. This is the artifact preparation foundation. The native
macOS and aggregate release gates remain open; a prepared template is not a
booted Machine or a supported published release.

## Integration API

`vz-macos-provision::bootstrap` exports `ReleaseManifest`, `Platform`,
`ImageIdentity`, `BootstrapCache`, `PreparedTemplate`, and structured `Progress`.
It is compiled on Unix; disk clone creation requires macOS and clone-capable
storage. There is no new public CLI verb.

1. The catalog adapter resolves the latest **verified, host-compatible** release
   or an explicit version to an authenticated `artifact_cache::Artifact` pin for
   the exact manifest JSON. Persist this pin in Environment state before calling
   preparation. Existing Environments keep their pins when the channel advances.
   Signing-authority verification and channel selection belong to the adapter;
   HTTPS and a hash received from an untrusted source are not authentication.
2. Open `BootstrapCache::new(absolute_private_root)`. Its parent must exist; the
   root and managed subdirectories are caller-owned and private with no symlink
   ancestry. Canonicalize an owned parent where macOS `/tmp` or `/var` aliases
   occur. Do not place mutable Machine state inside this cache.
3. Call `cache.prepare(&manifest_pin, callback).await`. Forward `Progress` over
   the Up operation stream. Events distinguish manifest/base/patch/platform
   acquisition, lock waiting, image/platform preparation, publication, and
   `TemplateReady { reused }`. Nested progress includes units: patch application
   counts chunks; other delta phases and downloads count bytes. Returning an
   error from the callback cancels and drains the worker. Dropping the future
   closes its event channel, cancelling the worker at its next checkpoint.
4. Check `prepared.manifest().platform` against the actual framework/host. Call
   `prepared.clone_disk(new_absolute_disk_path)` in a blocking task, in a new
   private Machine directory on the same clone-capable filesystem. This uses
   `clonefile`, creates a distinct writable inode, refuses existing destinations,
   and does not fall back to a full copy. It copies no identity sidecars.
5. The native adapter allocates Machine identity, MAC, credentials and private
   auxiliary state, binds the workspace, boots the VM and verifies the agent.
   `hardware_model_path()` and `auxiliary_storage_seed_path()` expose immutable
   inputs. Never attach the shared auxiliary seed directly to a VM. Proving the
   seed's compatibility with fresh Machine identity is still native integration
   work; this layer does not claim that arbitrary copied platform state boots.
6. Stop/Up reuses the Machine's private disk and state. Delete removes exactly
   that Machine's state, preserving templates and other Machines.

## Manifest contract

Version 1 uses strict serde structures, rejecting unknown fields and unsupported
schema versions. See
[`bootstrap/manifest.rs`](../../crates/vz-macos-provision/src/bootstrap/manifest.rs)
for the authoritative field names and documentation. It binds:

- Exact macOS 26+ version/build and the uncompressed base/patch HTTPS artifact
  URL, SHA-256 and byte length.
- Expected uncompressed prepared disk SHA-256 and logical length. Both base and
  output must match the delta header before disk preparation begins.
- Architecture, minimum host version, CPU/RAM requirements, and exact hardware
  model and auxiliary-storage seed artifact pins.
- Guest agent and toolchain digests for later native verification.

The manifest is capped at 64 KiB. The whole manifest's authenticated SHA-256 is
its cache/release identity, including platform and toolchain inputs. The IPSW pin
in `config/macos-26.6.2-25G83-ipsw.json` is a **maintainer source input**, not a
consumer release manifest. No real consumer manifest or loader patch has yet been
published. Do not promote synthetic test manifests to the release catalog.

## Cache and failure behavior

`downloads/<sha256>` holds verified blobs; `templates/<manifest-sha256>/` holds
`disk.img`, `hardware-model`, `auxiliary-storage-seed`, `manifest.json` and
`receipt.json`. Persistent locks serialize downloads by artifact digest and
preparation by manifest digest. Waiter cancellation leaves the owner running.

Preparation verifies all artifacts, validates the delta's base/output binding,
applies it in a blocking worker, verifies platform copies, sets template files
read-only, syncs a completion receipt, and atomically publishes the directory
without replacement. Worker events are acknowledged before the next step, so
callback cancellation before publication leaves no ready entry. A future dropped
after publication may leave a complete valid template. Interrupted deterministic
staging directories are reclaimed by the next owner under the same lock.

A warm hit verifies the small manifest and receipt and checks each file's device,
inode, size, timestamps, owner, mode and link count. It does not read large disk
bytes or revisit base/patch/platform downloads. This trusts the owning account
and filesystem; stamps detect ordinary edits/replacement, not malicious same-UID
receipt forgery or storage corruption that leaves metadata unchanged. Cache
repair/eviction and full integrity scrubbing are not implemented. Changed or
incomplete completed entries fail closed and remain for diagnosis. Do not use
this cache as a boundary against processes with the same account privileges.

## Maintainer exercise and remaining work

From `crates/`, after publishing/reviewing the real manifest pin and creating the
private parent directories:

```sh
cargo run -p vz-macos-provision --release --example prepare_bootstrap -- \
  /absolute/trusted-manifest-pin.json /absolute/private/bootstrap-cache \
  /absolute/private/machine/disk.img
```

The example emits throttled JSON progress to stderr and preparation/clone timings
to stdout. Invoke again with a different new disk path to measure cached creation.
It does not boot a VM and reports `native_machine_ready: false`. Existing
`image_delta` and `fetch_artifact` examples remain available for artifact work.

The next agent should prepare the real 26.6.2 / 25G83 base and loader/toolchain
patch, retain compatible platform resources, publish/sign the complete manifest
and artifact set, and wire this API into the native target adapter and Up stream.
Add persisted catalog resolution and test advancing the latest pointer without
changing existing Environment pins. Then prove a fresh installed download,
no-sudo bootstrap, guest agent/Swift execution, private platform identity,
Stop/Up/Delete, mixed-target isolation and the required native/aggregate gates.
Record cold preparation, cached clone creation, and warm VM start independently.

## Verification checkpoint

The foundation passes 47 crate tests on the local macOS host, including download
verification, concurrent preparation, acknowledged cancellation, abandoned staging
recovery, changed-template rejection, manifest pin independence and real APFS
clone mutation/deletion isolation. Release examples build, and a separate 16 MiB
synthetic disk exercise passes cold preparation plus cached reuse after deleting
large download blobs. This is file preparation evidence, not guest startup speed.

Formatting and strict library/example Clippy pass. All-target strict Clippy still
reports 39 existing unwrap/expect diagnostics in the original provisioning tests;
those test bodies match main exactly. Evidence is retained at
`.artifacts/macos26-bootstrap/foundation-verification/` in the bootstrap worktree.
The full native and aggregate gates remain required before closing `vz-mzs.11.4`.

## Installed-user validation findings (in progress)

The foundation was landed before a successful installed native happy path; that
was premature. Further runtime/backend changes must stay off main until the
relevant installed user flow passes. The new source guidance records this as a
merge gate, separately from keeping the release issue open.

The first real attempts on the macOS 26.3.1(a) host found:

- A fresh signed installed `vz up` requesting a native macOS Machine returns
  `unsupported_operation`: the current Up adapter supports only Linux/ARM64.
  `vz status` then reports `project_not_found`, confirming no topology admission.
  The probe used an empty catalog to expose the missing native adapter; it does
  not claim that a published macOS catalog entry exists.
- The actual `VZMacOSInstaller` call for the downloaded 26.6.2 / 25G83 IPSW
  fails with VZErrorDomain 10006, requiring a software update. The earlier
  read-only hardware-model compatibility check was insufficient to qualify it.
- The alternate 26.3.1 / 25D2128 restore input downloaded completely and matched
  its Apple CDN digest and length (19,330,833,456 bytes). Actual installation
  succeeded in 281 seconds on this host. This qualifies an installation input,
  not a booted or certified release.

The new `vz` example `prepare_macos_base` is a maintainer-only installer into a
new, task-owned directory; it cannot replace an existing image. Sign it with the
repository virtualization entitlement before running it on a macOS host. Failed
candidate directories remain isolated evidence and must not become published
bases. End users continue to require the exact prepared base/patch download path.

The required Swift fixture now exists at
`tests/fixtures/vz-0.4/native-macos-swift/`. Its executable rejects a physical Mac
hardware model; host syntax/unit checks are not guest execution evidence.

Retained attempt logs, signed-binary identities, CLI output, and restore-install
results live under `.artifacts/macos26-bootstrap/native-e2e/` in this worktree.
The aggregate native release gate remains **incomplete** until published artifacts,
Swift execution and aggregate topology behavior are proven. The DEV installed
lifecycle result below supersedes this initial adapter failure.

The clone gate must verify fresh platform identity as well as private disk bytes.
Apple specifies that concurrent VMs use distinct
[machine identifiers](https://developer.apple.com/documentation/virtualization/vzmacmachineidentifier)
and that clones receive their own
[platform state](https://developer.apple.com/documentation/virtualization/vzmacplatformconfiguration).
A copied auxiliary seed still needs a boot proof with the newly allocated
identifier; metadata equality or an APFS clone syscall alone does not prove it.

Maintainer continuation uses `provision_bootstrap` on a new APFS copy of the
stopped installed base, with the release loader and guest agent. Its root-only
mount/ownership step belongs to artifact production, never consumer setup.
The first authenticated run failed during Data-volume discovery: hdiutil returned
a container before the whole disk. The helper now selects the unique GUID whole
disk explicitly, with an ordering regression test. The authenticated retry
successfully mounted, provisioned and detached the owned clone. The original base
is preserved. The mount helper allocates a separate private directory per
attachment and removes only the empty directory after a successful detach.

Two independently cloned macOS 26.3.1 / 25D2128 VMs booted with the copied
auxiliary seed and fresh identifiers. Both loader pings and guest-agent execution
succeeded; boot to the agent probe took about 12 and 14 seconds. The guest reports
`VirtualMac2,1`; both VM identifiers differ from the installer and each other.
The second guest's loader and agent SHA-256 values match the prepared binaries.
No Xcode/Command Line Tools developer directory is installed, so the Swift gate
remains open. The second probe records a forced stop after a 30-second power-button
shutdown deadline; this is not graceful installed Stop evidence.

To repeat the prerequisite test, sign and run the `vz-cli` example `native_bootstrap_probe`
with the patched disk, installed hardware model, auxiliary seed and a new private
Machine directory. It creates a disk clone and fresh VM identifier, checks the
loader and guest agent over vsock, reads the guest version/hardware model, and
stops its owned VM, then repeats boot and verifies a persisted guest marker with
the same identity. Shutdown receipts distinguish forced fallback from graceful
shutdown. Its output explicitly marks this as prerequisite evidence,
not installed consumer E2E. Do not publish a base/patch until this succeeds and
the exact patch has been applied and booted independently.

Current branch checks: provisioning nextest 48 passed, zero skipped; strict
library/example Clippy and Rust formatting passed. The Swift fixture passes its
host syntax/unit check and its executable correctly rejects this physical
`Mac16,5` host. These checks do not close the native lifecycle gate.

### Real local patch round trip

The 26.3.1 / 25D2128 candidate now passes a local base → delta → reconstructed
disk → native boot → stop/start prerequisite test. The unprivileged patcher
(UID 501) verified the entire pristine base and reconstructed output. The
reconstructed image's private clone booted its loader and agent, executed guest
commands, then retained a guest-written marker and the same VM identifier across
stop/start. A third fresh identity differs from both earlier probes and the
installer identity. Both stops used the recorded force-stop fallback; this does
not qualify graceful installed Stop.

Local artifact pins, relative to `.artifacts/macos26-bootstrap/native-e2e/`:

| Artifact | SHA-256 |
| --- | --- |
| `base-candidate-2/base.img` (85,899,345,920 bytes) | `f2fe7a840f6251fb7e7e2603a4e3b5d99c769b0886b3a46288f92c22b9767858` |
| `bootstrap-26.3.1.vzdelta` (24,963,357 bytes; 18 changed chunks) | `60112dc7bba4ecf354d24e592498fe02302ce1e8c31e90f08ba13dca2845a148` |
| `reconstructed.img` (85,899,345,920 bytes) | `ce1335ae436f7bd7435ad8ecf05a4457f3a356450f8e87631e2d6135966d7ad0` |
| Guest loader | `f454fba42b94072f825e03b804633b6d52fc1af83b226a8e0d4281b86df7aee6` |
| Guest agent | `e7be3f4196dd03d34d834cd34c998efedc42ea2124589dac53321e7deaf20d97` |

Patch creation took approximately 362 seconds. Cold application took 930 seconds
with the original progress logger; two process samples showed substantial time
in token-by-token stderr writes. The example now serializes each JSON event into
one write. Debug/release format checks and matching patch bytes pass; the cold
timing has not been remeasured with this logging change. APFS cloning of the real
80 GiB reconstructed disk took 0.0039 seconds, and its boot/agent/exec probes took
17.23 seconds initially and 12.18 seconds after restart. These are local backend
measurements, not installed download or full Up timings.

`summary.json`, `patch-receipt.json`, `delta-apply-timing.json`, per-boot probes
and shutdown receipts retain the results; `summarize-prerequisites.py` checks the
receipts. The original base and reconstructed template remain separate from all
booted writable copies. Nothing is published or certified as a consumer release.
Remaining gates are authenticated publication/catalog selection, native installed
five-verb integration, pinned guest Swift/toolchain execution, graceful shutdown,
and mixed-target conformance. All further changes remain on the feature branch.

### Installed native adapter in development

The feature branch now connects native macOS to the shared Up/Exec/Status/Stop/
Delete controller, runtime registry, exact boot leases, execution supervision and
owner-scoped cleanup. The DEV adapter currently admits macOS/ARM64 Developer
Machines with POSIX exec/PTY on Apple-silicon macOS. Network attachments, workspace
projection, Docker, toolchain qualification and public artifact publication remain
outside this adapter's measured scope.

A trusted installation catalog can select `vz-macos` by exact version or a
`latest` channel. Selection persists the immutable manifest pin in the Machine's
configuration; recovery loads that pin without resolving the channel again.
Each Machine gets a private APFS disk clone, auxiliary state and new VZ machine
identifier. Native Stop requests guest shutdown and requires a positive framework
Stopped state before shared lifecycle cleanup may release ownership.

For local DEV delivery, a maintainer/operator supplies a directory of artifacts
whose filenames are their SHA-256 digests, including the exact manifest JSON.
The manifest declares `development: true`; an empty toolchain hash is accepted
only for this explicitly unqualified input. `bundle:<sha256>` locators never
resolve host paths: only the trusted installed catalog supplies the bundle root.
The installer verifies the manifest pin and bounded source metadata; first Up
verifies all actual artifact bytes, applies the exact matching patch, verifies
output, publishes the immutable template and boots a private clone. A warm hit
uses validated template receipts without downloading or rehashing the full disk.

The installer accepts `VZ_NATIVE_BUNDLE` and `VZ_NATIVE_MANIFEST_SHA256`, together
with its existing Linux profile selection. Its internal offline catalog writer
also accepts `--installed-native-bundle` and
`--installed-native-manifest-sha256`; these are installation inputs, not new
public `vz` lifecycle commands. Public HTTPS publication can use the same native
catalog entry and preparation pipeline without an installed bundle.

An ordinary project selects the installed pointer:

```json
{
  "schema_version": 1,
  "project_id": "prj_native_example",
  "name": "native-example",
  "environment": {
    "schema_version": 1,
    "machines": [{
      "schema_version": 1,
      "name": "mac",
      "profile": "developer",
      "target": {"os": "macos", "arch": "aarch64", "image": "vz-macos", "channel": "latest"}
    }]
  }
}
```

`vz up --environment dev` performs preparation automatically. Native Up defaults
to a one-hour first-use deadline; cached boot does not repeat preparation.
Preparation is a bounded structured field on the existing Up stream, separate
from its lifecycle phase, rendered as terminal progress. `vz exec -- /usr/bin/sw_vers`, `vz status`, `vz stop` and `vz delete` use the same Environment selection.

Installed physical verification is in progress under
`.artifacts/macos26-bootstrap/native-e2e/installed-candidate-*`. Early attempts
caught and fixed the preparation-progress protocol mismatch and first-use
10-minute deadline. The original guest agent rejected supervised Machine exec
on Darwin, so the final candidate includes a native supervisor with authenticated
execve readiness, PTY support and cancellation of the retained command group.
Deliberately detached sessions remain owned by the Machine; this DEV adapter does
not claim Linux subreaper/escaped-descendant conformance or advertise Signals.

The updated 26.3.1 / 25D2128 local bundle manifest pin is
`3ecc5ad65c762da36170f81d18cff3a61b3f7e2658f2394715fe740743d709cf`.
Its 165,932,984-byte patch has SHA-256
`d4579623cc7169d921322d2df1719a42787aecd0b1b9d764246f97d94cd1cea3`;
the reconstructed 80 GiB image must hash to
`a462c28a1d3bdffaa7b062492056bf553a3e1a88e54e92b178399dee737da163`.
The agent is pinned to
`76a5c8d93762f126e79e19d968a702fca0825fffe991602f21e858c76fdee3a3`.
The stopped maintainer clone passed pipe/PTY execution, cancellation/reconcile
and graceful shutdown before patch creation (`agent-update-candidate-3`).

The reusable installed gate is `scripts/run-installed-native-macos-e2e.py`:
provide signed binaries via `--release-dir`, the exact content-addressed
`--bundle`, trusted `--manifest` SHA-256 and a new `--evidence` directory.
It uses a fresh private installation/project, the public five verbs, tmux
input/resize/interrupt checks, bounded cancellation, restart persistence and
two-Environment clone/Delete isolation. No consumer VM is manually provisioned.

The updated host passes 1,491 guest/runtime/state tests, strict affected Clippy,
11 installer checks, installed corrupt-input rejection/never-started Delete
(`installed-corrupt-input-2`), and installed Linux Hardened lifecycle/persistence
regression (`linux-regression-3`). The native cold gate is `installed-candidate-6`.
The installed DEV gate subsequently **passed**, with receipts summarized in
[macos-native-dev-evidence.json](macos-native-dev-evidence.json). Fresh first-use
Up imported the exact base and matching patch, verified the full output and
reached Ready in **717.05 seconds**. Cached Stop/Up reached Ready in **8.90 seconds**;
a second Environment cloned the cached template and reached Ready in **13.67
seconds**. Graceful Stop took **5.86 seconds** and running Delete **5.89 seconds**.
These are local measurements on Mac16,5 / macOS 26.3.1(a), not network-download
timings or a performance guarantee.

The cold receipt is `installed-candidate-6/cold-up.json`. Terminal/lifecycle
checks completed in `installed-candidate-6-continuation-2`, against the same
unchanged installed binaries, original daemon and patched image. Two harness
assertions were corrected: wait for the CLI's existing 250 ms resize poll, and
expect the positively reaped cancellation exit code (137) with its deadline
diagnostic. No consumer guest provisioning or binary replacement occurred.
The final gate proves stdin/output/exit status, user/env/cwd, tmux input/resize/
Ctrl-C, cancellation with command/child reaping, persisted restart state,
distinct private platform identities, clone isolation and both public Deletes.

This is a usable local-bundle **DEV** path. Authenticated public publication,
guest Swift/toolchain execution, networking/workspace integration, and mixed-target
aggregate conformance remain open; `vz-mzs.11.4` is not release-complete.
An additional negative test found the shared pre-existing cleanup gap for an Up
rejected before any runtime reservation (such as a nonexistent catalog pin).
That distinct case is tracked in `vz-1fv`; the installed corrupt-bytes case after
reservation passes Delete in this change.
