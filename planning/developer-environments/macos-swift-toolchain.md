# Pinned native Swift toolchain (DEV)

Tracks `vz-mzs.11.4.1`, under `vz-mzs.11.4`. Work remains on
`feat/macos26-bootstrap`; the main worktree is reserved for the other agent.
The operator authorized Xcode license acceptance in the maintainer VM, and
maintainer Swift build/test/run passed. The first fresh installed run also passed
Swift and persistence, but its negative restart exposed a lifecycle acknowledgement
bug. The runtime fix is under fresh installed validation; this task remains open.

## Current candidate and evidence

The target remains **macOS 26.3.1 / 25D2128** on this Apple-silicon macOS
26.3.1(a) host. The explicit local input is **Xcode 26.3 / 17C529**, containing
Apple Swift **6.2.4** (`swiftlang-6.2.4.1.4`, driver `1.127.15`) and SDK **26.2**.
Only Swift build/test/run qualification is intended; packaging the application
bundle does not certify its GUI, debugger, simulators or other targets.

| Input | SHA-256 |
| --- | --- |
| Exact toolchain receipt | `4cd4a2882de582db89715646939aeef6504ff53f639ebf753536c51af189d67c` |
| Complete archive, 4,442,355,108 bytes | `c8a3d1a14f255462e0d0f75f3dee305e7785650980456661f663a28d89c45619` |
| Pristine 80 GiB base | `f2fe7a840f6251fb7e7e2603a4e3b5d99c769b0886b3a46288f92c22b9767858` |
| Block patch, 3,940,620,548 bytes | `3226f357ce4eac9d4f5bc89be0fd51de9821d7b01c8a4fb007374b4ab9f1da40` |
| Prepared 80 GiB image | `21818773a98bd4287120ac4a80934a0469464806666ead459017685872a49f7e` |
| Installed DEV manifest | `5595d334981bfc7a1de6c9d974ca05ba6dbe778f0765846170943e56de130c56` |

Raw artifacts live under `.artifacts/macos26-bootstrap/native-e2e/`:
`swift-toolchain-xcode-1/` contains the archive and pinned JSON;
`swift-image-xcode-1/` records the initial license-blocked installation.
`swift-image-xcode-2/` records the authorized continuation: exact toolchain identity,
ordinary release build (3.06 seconds), Swift Testing (5.75-second build and one
passing test), and execution reporting `VirtualMac2,1`, macOS 26.3.1 / 25D2128.
The fixture directory was removed and shutdown reports `provisioned: true`,
`stopped: true`, `forced: false`. These are maintainer timings, not consumer
first-use or cached-boot measurements.

The previous CLT-only candidate (Swift 6.2.1, SDK 26.1) reached installed Ready
in **716.159 seconds**, but failed ordinary user execution because `/Users/dev`
was owned by root. Account creation now sets configured guest ownership and mode
0700; the maintainer also repairs older source images and proves `dev` can write
its home. Ownership changes refuse paths resolving outside the mounted image.

A diagnostic-only home repair allowed a release Swift build (31.95 seconds), but
plain `swift test` could not discover Testing.framework. Explicit framework flags
passed; a compatibility layout still failed linking. Those repaired diagnostics
are **not** acceptance evidence. SwiftPM's
[6.2 toolchain search logic](https://raw.githubusercontent.com/swiftlang/swift-package-manager/release/6.2/Sources/PackageModel/UserToolchain.swift)
provides context for the observed framework/directory mismatch. The candidate
was deleted through its original installed daemon, that daemon was stopped, and
its disposable cache was reclaimed. Logs remain in `installed-swift-candidate-2/`.
The final gate must use ordinary commands with the complete Xcode candidate.

The first Xcode consumer (`installed-xcode-candidate-1`) reached Ready in 728.553
seconds, built in 10.675 seconds, tested in 7.052 seconds and restarted in 27.499
seconds. Its initial SDK fault injection was denied by macOS; the harness now
uses an authenticated receipt mutation with readback and immediate shell failure.
A continuation also needed explicit Environment selection for status once both
Environments existed.

The corrected corruption check then exposed a runtime failure: a failed restart
acknowledgement incorrectly supplied the previous incarnation as its result.
The state contract rejected that acknowledgement and retained the operation
fence, masking the toolchain mismatch and blocking Delete of that Environment.
Up now supplies a resulting incarnation only with successful activation evidence;
the previous incarnation remains the expected fence. The unaffected Environment
was positively deleted; the failed daemon was terminated and its disposable
images retired. That run is not qualification evidence.

The next fresh run (`installed-xcode-candidate-2`) passed Swift and persistence
and returned the exact mismatch diagnostic. The harness was corrected to expect
the established `backend_unavailable` exit code 5. Delete then exposed a second
failure: the durable identity described the previous successful boot, while the
daemon held the new, unready boot. That run also required diagnostic retirement
and is not qualification evidence.

Cleanup now retains a sealed record of the failed Up journal, prior persisted
incarnation/runtime identity, and exact registered VM identity. Stop/Delete
validate that binding and the terminal failed journal before draining the
original session. Positive Delete quiescence retains the same record for retries.
This does not publish Ready or adopt a replacement Runtime. A regression test
checks the real state-journal sequence and rejects changed operation, owner,
incarnation, runtime and quiescence evidence.

The complete fix passes 1,606 runtime/contract/state tests (31 existing skipped
tests) and strict production Clippy. An initial suite attempt hit a legacy
concurrent sandbox-create failure; the complete rerun passed. The final
`installed-release-xcode-3` is under fresh native validation in
`installed-xcode-candidate-3`, with Linux regression in `linux-swift-regression-2`.
The native negative gate now requires both Stop after failed readiness and direct
Delete after a second rejected restart.

## Receipt and installation contract

`scripts/prepare-native-swift-toolchain.py` accepts explicit local developer tools.
`--layout xcode` archives the complete application; `--layout clt` archives its
`usr`, `Library` and selected SDK trees. It normalizes ownership, ordering and
mtime, rejects absolute or escaping symlinks, and leaves source files untouched.
The CLT recipe omits its obsolete external `usr/bin/crashlog` alias.

`ToolchainManifest` selects one of two fixed layouts: CLT under
`/Library/Developer/CommandLineTools`, or Xcode under `/Applications/Xcode.app`.
Arbitrary installation paths are not accepted. The receipt binds the archive,
Swift and SDK versions, compiler, driver, package manager, Clang, linker and SDK
settings hashes. `toolchain_sha256` in the release manifest is the hash of the
**exact receipt bytes** installed at `/usr/local/share/vz/toolchain.json`.

Every native Up with a nonempty toolchain pin verifies that receipt and its
in-guest tool/SDK anchors before Ready. The base/patch pins authenticate the
complete initial installation; anchor checks do not attest every mutable file.
Missing legacy `layout` fields select CLT. Empty legacy DEV toolchain pins retain
lifecycle compatibility and do not qualify Swift execution.

The maintainer helper verifies the archive, makes a new private disk clone and
VM identity, and uses ticketed guest-agent stdin for transfer. It uses neither
networking nor host shares. It selects the developer directory, removes temporary
inputs, checks identity and requires a graceful VM shutdown. With `--fixture`,
it builds, tests and runs the checked-in project as `dev` and removes its fixture
directory before shutdown. This catches toolchain failures before an expensive
patch/installed-consumer cycle; it does not replace that final cycle.

## Maintainer continuation

From the repository root, package an explicit input into a new private directory:

```sh
python3 scripts/prepare-native-swift-toolchain.py --layout xcode \
  --source /Applications/Xcode.app --output /absolute/private/toolchain-inputs
```

Build the release `vz-cli` example `prepare_native_swift` from `crates/`, then
sign it with `entitlements/vz-cli.entitlements.plist`. Supply a stopped native
agent image and the exact receipt pin:

```sh
crates/target/release/examples/prepare_native_swift \
  --disk /absolute/private/source/disk.img \
  --hardware /absolute/private/source/hardware-model \
  --auxiliary /absolute/private/source/auxiliary-storage \
  --payload /absolute/private/toolchain-inputs \
  --toolchain-sha256 RECEIPT_SHA256 \
  --fixture tests/fixtures/vz-0.4/native-macos-swift \
  --output /absolute/private/new-swift-image
```

Use `--reuse-installed-toolchain` to continue from the stopped Xcode candidate
above in another new clone. It requires the installed receipt to match exactly.
The separate `--accept-xcode-license` switch runs `xcodebuild -license accept`
**inside the maintainer VM** and must only be supplied after the operator has
reviewed and explicitly approved that agreement. It is off by default; no
automatic license acceptance occurs without that switch. The operator explicitly
authorized its use for `swift-image-xcode-2`; the guest command returned zero
and its receipt is retained. The exact local terms
are `/Applications/Xcode.app/Contents/Resources/en.lproj/License.pdf`.

After identity, fixture and graceful-shutdown receipts pass, generate a delta
from the pristine base to that stopped image. Update the complete release
manifest together: patch/output identities, auxiliary seed and toolchain pin.
Use private single-link files (APFS copies, not hard links) in the installed DEV
bundle. Retain guest-agent and hardware pins from the matching source image.

Run a fresh installed gate using signed release binaries:

```sh
python3 scripts/run-installed-native-macos-e2e.py \
  --release-dir /absolute/private/signed-release \
  --bundle /absolute/private/installed-bundle --manifest MANIFEST_SHA256 \
  --evidence /absolute/private/fresh-native-swift-evidence --require-swift
```

This imports/applies the exact pair, transfers only project source through public
`vz exec`, builds/tests/runs inside VirtualMac, verifies persistence after Stop/Up,
and checks separate Machines and Delete. It deliberately changes the second
Machine's toolchain receipt, reads back a different digest, and requires the next
Up to reject it before Delete. SDK anchors are measured on successful Up; the
negative test does not claim an SDK-file mutation. macOS refused the initial
SDK write inside Xcode, and a trailing `sync` masked that shell error. The harness
now uses `set -eu` plus readback for the receipt mutation. This correction changes
only host test logic; the consumer image and installed binaries stay unchanged.
Public artifact publication, authenticated channel delivery, native networking,
workspace integration and aggregate 0.4 conformance remain parent-issue work.
