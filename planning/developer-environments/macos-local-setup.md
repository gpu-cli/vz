# Local macOS setup

Status: DEV, implementation and fresh installed E2E in progress.
Tracked by `vz-mzs.11.4.2`, on `feat/macos26-bootstrap`. Main remains reserved
for other work; no runtime merge before installed-user verification.

The user selected local preparation on 2026-09-06, replacing the public Apple
base/patch delivery dependency. The reusable delta implementation remains in
`vz-macos-provision::image_delta`. It is not invoked by local setup.

## User operation

After installing the release binaries, select a locally installed Xcode:

```sh
vz-macos-setup --xcode /Applications/Xcode.app --accept-xcode-license
```

Review that Xcode installation's license before passing the acceptance flag.
The flag accepts it inside the new guest, not on behalf of other users.
`--json` provides structured progress. `--ipsw /absolute/restore.ipsw` uses an
existing copy only after verification against the built-in exact Apple pin.
The default downloads directly from Apple. `--prefix` selects an explicit
installed vz prefix; it must contain the corresponding daemon and setup tools.

The initial recipe pins macOS 26.3.1 / 25D2128, minimum host 26.3.1 on Apple
silicon, an 80 GiB sparse guest disk, and the explicit local Xcode application.
It installs the loader and agent into the newly installed disk during one
short-lived sudo operation. The setup command itself runs as the normal user.
There is no persistent root service or global sudo configuration.

Setup then boots a private candidate with no network or host shares, transfers
Xcode through the guest agent, verifies the exact guest version/toolchain, and
runs the embedded native Swift build/test/run fixture as `dev`. It requires a
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
