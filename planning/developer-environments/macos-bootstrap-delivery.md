# Automated macOS bootstrap delivery (DEV)

Tracks: `vz-mzs.11.4`. Native integration and release acceptance remain open.

## Workflow

Maintainers prepare the first candidate manually: choose and pin an Apple restore
IPSW, create a quiescent base, provision the loader/agent and native toolchain in
an owned image, produce the matching patch, and verify a fresh consumer run.
Once that candidate works, a release workflow reproduces the same preparation,
verification, signing, and artifact publication. End users do none of this work.

The user selects a supported native macOS release and runs `vz up`. The product
resolves and persists its exact artifact pins automatically. Its stream presents
plain phases with byte progress where measurable: download macOS, verify,
prepare image, apply patch, start Machine. Non-TTY consumers receive equivalent
structured progress. Cancellation must not leave a partial artifact marked ready.

Preparation is once per pinned artifact set, with concurrent requests sharing the
same preparation. A validated immutable prepared template is cached independently
of mutable Machines. Subsequent Machine creation uses APFS copy-on-write clones
and allocates Machine-owned platform identity, auxiliary storage, credentials,
and workspace bindings. Stop/Up reuses the Machine's own state. Deleting a Machine
must not delete another Machine or its reusable immutable template. Measure cold
preparation, cached creation and warm start separately; do not label speed from
synthetic file tests as VM startup performance.

## Initial restore candidate

`config/macos-26.6.2-25G83-ipsw.json` pins the exact Apple download URL, size
19,772,231,540 bytes, and SHA-256
`885503b7f4b06609e9a512f2befd40f59730640a3f1233e3892d60affdd51c95`.
The version/build and URL were read from Apple's
[restore catalog](https://mesu.apple.com/assets/macos/com_apple_macOSIPSW/com_apple_macOSIPSW.xml).
The digest/length came from the matching Apple CDN response headers; downloaded
bytes must independently match them before use. This is a candidate input, not
an ACTIVE capability or a certified macOS version. The complete download matched
that SHA-256, and its Restore.plist identifies 26.6.2 / 25G83 and VirtualMac2,1.
A signed read-only Virtualization.framework probe on the local Apple-silicon
macOS 26.3.1(a) host accepted the restore hardware model and reported minimums of
2 CPUs and 4 GiB RAM. Installation, loader bootstrap, Xcode/Swift execution and
the installed physical gate still need proof.

The framework requires a `.ipsw` filename even for valid archive bytes. Native
preparation must stage a verified cache blob under that suffix in its owned
workspace. The probe used a task-owned hard link; it did not copy the 19.8 GB
archive, mount a disk, install or boot a VM. Evidence is retained under
`.artifacts/macos26-bootstrap/verification/` in this worktree.

## Selected base delivery contract

Users download an exact published base image and its matching VZDELTA1 block
patch. This is the selected delivery path. Apple IPSW acquisition and installation
belong to the maintainer artifact recipe; users do not install an IPSW locally or
prepare their own base. The entire base must match the patch's SHA-256 precondition.
A fresh consumer run must download the published pair and prove compatibility.

A `latest` pointer selects the latest verified release supported by the host,
resolving to one authenticated immutable manifest. That manifest binds the exact
macOS version/build, base and patch URLs, sizes and digests, expected patched-image
digest, platform requirements/resources, guest agent and toolchain pins. Base and
patch are selected together; separate moving URLs cannot choose incompatible
versions. Apple's latest available OS is a maintainer discovery input, not an
unverified consumer release.

Persist the resolved manifest identity and pins before preparation. Existing
Machines and Environments retain their resolved release when the pointer advances;
upgrades require explicit selection. A cached pinned release remains usable
without resolving `latest` again. Keep prior published artifact sets obtainable
for pinned recreation. A supported release with no matching artifacts cannot be
advertised as ready.

The first milestone is the manually prepared macOS 26.6.2 / 25G83 base plus loader
patch, followed by a fresh download/apply/boot proof. Artifact publication and
native integration remain unimplemented; this decision selects their contract.

## Implemented prerequisite APIs

`vz-macos-provision::artifact_cache` validates trusted HTTPS URL/length/SHA-256
pins, streams downloads with cancellation, hashes bytes before publication, and
serializes same-digest requests with persistent locks. Completed downloads are
verified and reused without a network request. The cache is private to the caller.
Partial staging files are discarded; corrupt completed entries fail closed.
It currently rehashes a cached blob and does not resume interrupted downloads.
It is not the prepared-template cache or a Machine readiness assertion.

`vz-macos-provision::image_delta` exposes maintainer creation and unprivileged
application of VZDELTA1 as Rust APIs, independently of retired CLI command code.
It streams phase progress, bounds record/decompression sizes, verifies base and
output digests, and publishes without replacing an existing output. It does not
mount images or copy Machine identity sidecars. The caller must authenticate the
patch with trusted release inputs; embedded content hashes are not signatures.

Maintainer examples, run from `crates/`:

```sh
cargo run -p vz-macos-provision --release --example fetch_artifact -- \
  ../config/macos-26.6.2-25G83-ipsw.json /absolute/private/artifact-cache
cargo run -p vz-macos-provision --release --example image_delta -- \
  create /absolute/base.img /absolute/prepared.img /absolute/bootstrap.vzdelta
cargo run -p vz-macos-provision --release --example image_delta -- \
  apply /absolute/base.img /absolute/bootstrap.vzdelta /absolute/output.img
```

These are development artifact tools, not public lifecycle verbs. They do not
boot a Machine, install a loader, authenticate/sign a release manifest, publish
a patch, or satisfy the native release gate.

## Next integration and acceptance

Prepare the exact candidate base on this host and create its real loader-bearing
patch. Publish the authenticated pair and immutable release manifest. Bind all
artifacts and platform resources to the native target catalog and Machine ownership. Add the operation-owned
prepared-template cache, streamed Up progress, clone-based creation, and native
exec/lifecycle adapters. Automate the proven maintainer recipe in a release
workflow. Finish with a clean consumer download/bootstrap and the pinned Swift
fixture, mixed-target paths, Stop/Up, Delete, and the canonical installed native
and aggregate gates. Keep `vz-mzs.11.4` open until that evidence exists.
