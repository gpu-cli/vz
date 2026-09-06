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
Native E2E remains **failing/incomplete** until real artifacts, installation/boot,
agent readiness, Swift execution, and the installed lifecycle are proven.

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
disk explicitly, with an ordering regression test. Its retry is waiting for macOS
administrator authentication; the original base is preserved. The mount helper now allocates a separate private directory
per attachment and removes only the empty directory after a successful detach.
The existing unit suite passes with this change, which awaits real mounting
verification before main integration.

After provisioning, sign and run the `vz-cli` example `native_bootstrap_probe`
with the patched disk, installed hardware model, auxiliary seed and a new private
Machine directory. It creates a disk clone and fresh VM identifier, checks the
loader and guest agent over vsock, reads the guest version/hardware model, and
stops its owned VM. Its output explicitly marks this as prerequisite evidence,
not installed consumer E2E. Do not publish a base/patch until this succeeds and
the exact patch has been applied and booted independently.

Current branch checks: provisioning nextest 48 passed, zero skipped; strict
library/example Clippy and Rust formatting passed. The Swift fixture passes its
host syntax/unit check and its executable correctly rejects this physical
`Mac16,5` host. These checks do not close the native lifecycle gate.
