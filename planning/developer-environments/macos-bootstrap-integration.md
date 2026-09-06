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
