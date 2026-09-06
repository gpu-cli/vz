# Local macOS setup

Status: DEV. Clean macOS setup, warm reuse, and the installed native lifecycle
have passed, including private clones, terminal behavior, restart persistence,
and public cleanup after a rejected start at the host VM limit. The earlier
Xcode setup passed signature/license/Swift validation and warm reuse. Fresh
optional-Xcode setup preserving the clean selection is in progress.
See the [current evidence](macos-local-setup-evidence.json).
Tracked by `vz-mzs.11.4.2`, on `feat/macos26-bootstrap`. Main remains reserved
for other work; no runtime merge before installed-user verification.

The user selected local preparation on 2026-09-06, replacing the public Apple
base/patch delivery dependency. The reusable delta implementation remains in
`vz-macos-provision::image_delta`. It is not invoked by local setup.

## User operation

After installing the release binaries, prepare macOS without developer tools:

```sh
vz-macos-setup
```

Xcode is optional. To prepare a separate template with a locally installed Xcode:

```sh
vz-macos-setup --xcode /Applications/Xcode.app --accept-xcode-license
```

The default template contains the vz agent and `dev` account, with neither Xcode
nor Command Line Tools. It does not require Xcode on the host or license
acceptance. Each variant has its own recipe and cached image. In a Machine's
macOS target, set `channel` to `clean` or `xcode` to choose explicitly; `latest`
selects the most recently registered template. Preparing one variant preserves
the other and never changes an existing Environment's persisted image pin.

Review that Xcode installation's license before passing the acceptance flag.
The flag accepts it inside the new guest, not on behalf of other users.
`--json` provides structured progress. `--ipsw /absolute/restore.ipsw` uses an
existing copy only after verification against the built-in exact Apple pin.
The default downloads directly from Apple. `--prefix` selects an explicit
installed vz prefix; it must contain the corresponding daemon and setup tools.

The initial recipe pins macOS 26.3.1 / 25D2128, minimum host 26.3.1 on Apple
silicon, an 80 GiB sparse guest disk, and, when selected, the local Xcode application.
It installs the loader and agent into the newly installed disk during one
short-lived sudo operation. The setup command itself runs as the normal user.
There is no persistent root service or global sudo configuration.

Setup then boots a private candidate with no network or host shares and verifies
the exact guest version, native VM identity, and usable `dev` home. Clean setup
checks that Xcode, CLT, and the toolchain receipt are absent. Xcode setup transfers
the application through the guest agent, verifies its Apple signature and pinned
compiler/SDK identity, and runs the embedded native Swift fixture as `dev`.
Application symlink targets are preserved literally because Apple's signature
seals those bytes. Setup requires a
graceful stop before publishing the image. Failures never register partial
images. Cancellation waits for an active installation/provisioning operation to
reach a safe boundary; ordinary downloads can cancel at their progress checks.

## Cache and registration

A private `macos-local` directory under the selected installation prefix owns
setup locks, verified IPSW downloads, complete local images, and recipe receipts.
Each image manifest binds the entire prepared image, hardware model, stopped
auxiliary-storage seed, guest agent, and toolchain. It contains no base or patch.
The bootstrap cache APFS-clones the local image, verifies its full hash, then
publishes an immutable receipt. Warm hits verify file stamps without rereading
80 GiB of image data. Machines always get separate writable disk inodes and
fresh platform state; they never boot the shared template.

Catalog registration preserves installed Linux profiles and prior native pins.
New Environment resolution reloads the explicit trusted installed catalog;
existing Environments use their persisted resolution and do not move when the
local `latest` selection changes. The catalog remains operator-owned input;
project definitions cannot name arbitrary host images or execute setup as root.

## Required verification

Use signed installed binaries with a fresh setup directory and a real pinned
IPSW. Retain phase logs, privilege result, hashes, guest version, build/test/run,
graceful stop and setup receipt. Then exercise installed Up/exec/status/Stop/Up/
Delete, two independent Machines, repeat setup without sudo or install, and
cancellation/corruption rejection. Separately exercise actual Apple HTTPS
acquisition; using `--ipsw` alone does not prove the default download path.
Signing/notarization and binary packaging are added to the ordinary release
workflow. A dedicated Mac can run this same setup/E2E recipe when available;
no hosted runner or public Apple-image publication is assumed to exist.

## Current physical run

The first run fetched and hashed the entire pinned IPSW from Apple, then found
that Virtualization.framework requires a `.ipsw` filename. Setup now makes a
private APFS clone named `restore.ipsw` from the verified digest-named cache blob.
Installed retries retain only the verified Apple download, always creating a
fresh guest. They exposed a pre-Xcode Perl archive-hash timeout and signed Xcode
resource corruption: the Rust tar link helper normalized doubled slashes in
symlink targets. Native OpenSSL hashing and literal symlink headers fix those
paths. A BSD tar extraction regression fails before the link fix and passes
afterwards, including long link targets and long archive entry names.

The corrected installed run passed guest Xcode signature validation, authorized
license acceptance, exact toolchain identity, Swift build/test/run, and graceful
shutdown, image publication, and warm reuse. The clean variant additionally
passed its installed five-verb lifecycle, PTY/resize/Ctrl-C, cancellation reaping,
restart persistence, and independent platform identities and private disks.
Clean repeat setup took 0.124 seconds; warm Up took 9.3 seconds. A fresh runtime
cache still performs full image verification on first Up (229 seconds in this
run). These timings were measured on the shared development host.

A real third-VM rejection exposed two cleanup defects: a lost activation lease
held the Up fence indefinitely, and graceful Stop waited for a terminal Error
state to become Stopped. Failed starts now retain their exact activation in live
sessions. Stop queries the original framework VM under its lifecycle fence and
retires stopped or irrecoverably errored objects; unknown states remain uncertain.
The regression passed rejected-start Stop/Delete while both running VMs remained
usable. Fresh optional-Xcode coexistence testing remains in progress. No main
merge or aggregate 0.4 conformance is claimed.
