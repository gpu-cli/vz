# Native macOS Swift gate fixture

Build and test this package **inside the selected macOS Machine**, using the
release-pinned Swift toolchain. Then run its release executable through installed
`vz exec`. It emits `vz-native-macos-swift` protocol version 1 with guest
`sw_vers`, hardware model and process ID. The executable rejects physical Mac
hardware models; host execution cannot satisfy its normal success path.

Retain the exact guest version/build and toolchain identity, matched to the
resolved authenticated release manifest. Also retain daemon VM/process ownership
and execution receipts: the probe alone is not proof of selected-Machine identity.
Native Up/exec/Stop/Up/Delete and mixed-target gates are still required.

There are no package dependencies. Do not check in `.build/` or use a host build
as native guest execution evidence.
