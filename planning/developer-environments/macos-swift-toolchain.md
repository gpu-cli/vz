# Pinned native Swift toolchain (DEV)

Tracks `vz-mzs.11.4.1`, under `vz-mzs.11.4`. This work qualifies a local
Apple-silicon macOS host → native macOS Machine development path. Public
artifact publication and the aggregate 0.4 gate remain separate requirements.

## Recipe and trust boundary

The maintainer supplies an explicit local Apple Command Line Tools directory.
`scripts/prepare-native-swift-toolchain.py` packages its `usr` and `Library`
trees and the selected macOS SDK. It normalizes archive order, ownership and
timestamps, rejects absolute or escaping symlinks, and omits the obsolete
`usr/bin/crashlog` alias that points outside the CLT installation. Other installed
SDK versions are excluded. This is Swift build/test tooling, not full Xcode or
debugger qualification.

The archive and a bounded JSON receipt have separate SHA-256 pins. The receipt
binds the archive size/digest, Swift version, SDK version, and the compiler,
driver, package manager, Clang, linker and selected SDK settings hashes. The
release manifest's `toolchain_sha256` is the SHA-256 of the **exact receipt
bytes**, installed at `/usr/local/share/vz/toolchain.json`.

`prepare_native_swift` verifies the archive, makes a private disk clone with a
fresh VM identity, boots it without networking or host shares, and transfers the
archive over ticketed guest-agent stdin. It verifies the transferred archive,
installs CLT and the receipt, selects the developer directory, removes temporary
inputs, verifies the installed toolchain, and requires a graceful VM shutdown.
Only a stopped successful candidate may be used to generate a release patch.
The original base and source installation stay intact.

On every native Up, a nonempty release toolchain pin requires an exact receipt
match and an in-guest hash/version verification before Ready. The base/patch
authentication covers the complete initial installation; boot anchor checks
do not attest every mutable SDK file. Existing local DEV releases with empty
toolchain pins retain their lifecycle support but do not qualify Swift builds.

These tools consume local maintainer inputs. They do not discover, download,
license, sign or publish a public Apple toolchain distribution. Consumers receive
the complete toolchain through their exact base image and matching block patch;
they run no provisioning commands.

## Maintainer commands

From the repository root, with new absolute output directories:

```sh
python3 scripts/prepare-native-swift-toolchain.py \
  --source /Library/Developer/CommandLineTools \
  --output /absolute/private/toolchain-inputs
```

Build `vz-cli`'s release example `prepare_native_swift` from `crates/`, and sign
the resulting executable with `entitlements/vz-cli.entitlements.plist`. Invoke
it with a stopped image that already contains the native guest agent:

```sh
crates/target/release/examples/prepare_native_swift \
  --disk /absolute/private/agent-image/disk.img \
  --hardware /absolute/private/agent-image/hardware-model \
  --auxiliary /absolute/private/agent-image/auxiliary-storage \
  --payload /absolute/private/toolchain-inputs \
  --toolchain-sha256 RECEIPT_SHA256 \
  --output /absolute/private/swift-image
```

Require `shutdown.json` to report `provisioned: true`, `stopped: true` and
`forced: false`. Generate the delta from the pristine pinned base to this stopped
disk using the `vz-macos-provision` `image_delta` example. Update the complete
release manifest together: patch digest/length, prepared-image digest/length,
auxiliary seed, and toolchain receipt pin. The existing guest-agent and hardware
pins must still match. Assemble a content-addressed installed DEV bundle using
the [native integration contract](macos-bootstrap-integration.md).

Validate a **fresh** installed bundle with signed release binaries:

```sh
python3 scripts/run-installed-native-macos-e2e.py \
  --release-dir /absolute/private/signed-release \
  --bundle /absolute/private/installed-bundle \
  --manifest MANIFEST_SHA256 \
  --evidence /absolute/private/fresh-native-swift-evidence \
  --require-swift
```

The gate imports and applies the exact pair, then transfers only the checked-in
Swift project source through ordinary `vz exec` stdin. It builds release output,
tests and executes it as `dev` inside `VirtualMac2,1`, verifies persisted output
and toolchain identity after Stop/Up, and checks independent Machine state and
Delete. It also alters the disposable second Machine's SDK settings and requires
the next Up to reject that pin mismatch before deleting it. No host Swift build
or hidden toolchain installation can satisfy this gate.

## Candidate pins

The current local maintainer candidate uses guest **macOS 26.3.1 / 25D2128**,
Apple Swift **6.2.1** (`swiftlang-6.2.1.4.8`, driver `1.127.14.1`) and macOS SDK
**26.1**. Its receipt SHA-256 is
`4e28af6ec0c68a46f2dc2d17655a2ad0350119e3161396029aefe3cc6b989c61`.
The archive is 1,433,323,751 bytes, SHA-256
`71bd783d5c4ec40646159f5125a3fc59623a0404301a097d67c92a72fb10731c`.

Raw maintainer evidence is retained in the macOS worktree under
`.artifacts/macos26-bootstrap/native-e2e/swift-toolchain-3/` and
`swift-image-candidate-2/`. The first attempted VirtioFS transfer failed with
`Operation not permitted`; its VM stopped gracefully. The successful candidate
uses the guest-agent transfer described above.
