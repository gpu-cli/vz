# Docker SSH test inputs

The SSH forwarding fixture uses an authenticated, offline Debian ARM64 runtime
overlay. Its exact inputs and finite byte limits are recorded in
[the package pin](../config/docker-ssh-packages-bookworm-arm64.json). This is test
input admission, not evidence that SSH forwarding or the full Docker gate passed.
The checked-in pin is 14,332 bytes with SHA256
`aa751b309dfb8a0c7c0c3ab61c30bc9efd9ea2ec67fe15b7bc252a321e6cb4ca`.
It additionally authenticates the base's GNU tar, ELF loader, and merged-`/usr`
aliases. The original candidate and its evidence remain unchanged.

## Immutable inputs and trust

The base is the existing admitted Python ARM64 manifest
`docker.io/library/python@sha256:d04f49f5882f49a3b91f874e75e19f0c265f7222da8659741a9d7eab148f22a9`,
not a mutable Python tag. Its config digest, ordered rootfs diffIDs, and the
compressed layer containing the Debian archive keyring are bound in the pin.
Reading those verified layers establishes Debian 12 Bookworm, the installed
package status, and the actual ARM64 `/usr/bin/dpkg-deb` bytes.

The trust anchor is `usr/share/keyrings/debian-archive-keyring.gpg` from that
already-admitted image: 55,918 bytes, SHA256
`506b815cbb32d9b6066b4a2aa524071e071761e7e7f68c3ac74f3061ba852017`.
Downloading a new key from a website is not the trust decision. Debian's
[archive key documentation](https://ftp-master.debian.org/keys.html) provides
an independent fingerprint reference.

All eight DEBs come from the fixed
[Debian snapshot](https://snapshot.debian.org/archive/debian/20260901T000000Z/):
Bookworm `InRelease` authenticates `main/binary-arm64/Packages.xz`, whose exact
entries authenticate package version, architecture, path, size, and SHA256.
The selected OpenSSH version is `1:9.2p1-2+deb12u10`; the overlay includes
`openssh-client`, `openssh-server`, `openssh-sftp-server`, `libbsd0`, `libcbor0.8`,
`libedit2`, `libfido2-1`, and `libwrap0`. The security snapshot was examined only
as a comparison; no selected package comes from it.

The retained Bookworm signature verification completed with `gpgv` exit 0.
The pin requires the Bookworm automatic, Trixie automatic, and Bookworm stable
primary fingerprints observed on this exact Release. Admission must verify the
signature itself and reject signature errors, rather than trusting a previously
captured success transcript. A fixed historical snapshot does not imply current
security-update freshness.

## Reproduction and evidence boundary

Reproduction uses the pin's exact descriptors and bounds:

1. Verify the admitted base manifest/config, compressed layers, and uncompressed
   diffIDs; recover the keyring and confirm its metadata and digest.
2. Fetch the pinned snapshot Release, Packages index, and eight DEBs into a fresh
   input directory. Verify the Release with the recovered keyring, then verify
   the signed index descriptor and every selected package descriptor.
3. Check each DEB's `debian-binary`, `control.tar.xz`, and `data.tar.xz` structure
   and control identity. Keep size/decompression limits finite; do not install
   or execute downloaded code during host-side admission.
4. Stage regular DEB files plus the exact guest manifest. Inside the owned test
   image, verify all staged bytes, tools, loader, and directory aliases before
   any extraction. Capture `dpkg-deb --fsys-tarfile` with a 16 MiB limit into a
   private spool, then invoke pinned GNU tar with `--keep-directory-symlink`.
   Recheck identities and bytes around each package. Extraction is offline and
   runs no maintainer scripts. There is no `apt`, `dpkg -i`, or mutable package
   resolution.

The pin's `source_proofs` is a finite list of filename/size/SHA256 descriptors:
base metadata and installed status, signature transcripts, the closure summary,
ELF inspection, signed source-index bytes, the selected source stanza, and the
OpenSSH source archives. Original investigation files are retained under
`.artifacts/docker-ssh-debian-inputs-kV8vSp/`; those paths are evidence locations,
not a substitute for verifying their descriptors. The source archives are
authenticated through the same Release's `main/source/Sources.xz` entry and its
OpenSSH stanza. The recorded Debian source Dgit commit is
`e406a8b019e8128644ad0f230b60710abd746298`.

This overlay is **not a full dpkg installation**: package-manager integration
dependencies and maintainer scripts are deliberately not installed or run.
The retained ELF inspection establishes 46-path SONAME reachability against the
exact base plus overlay, not complete symbol/ABI compatibility or successful
kernel execution. Actual client/server execution and authenticated host-driven
positive/negative forwarding tests remain separate required evidence.

## Account and diagnostic policy

The first installed-Mac SSH candidate failed before reaching an SSH operation:
the original direct `dpkg-deb --extract` path did not preserve the base's
merged-`/usr` directory aliases. A separate public-input diagnostic passed
admission and seven extractions, then the next executable failed with ENOENT.
The OpenSSH server archive contains a `./lib/` directory, while the admitted
base has `/lib -> usr/lib` and uses `/lib/ld-linux-aarch64.so.1` as its ELF
interpreter. GNU tar's default directory-symlink replacement explains this
failure. All four Machines were subsequently stopped with clean-journal
receipts; this attempt remains failed, not an SSH compatibility pass.

Setting `TAR_OPTIONS` is not a fix: dpkg 1.21.23's `extracthalf` explicitly
removes it before invoking tar. The replacement extractor uses a bounded
`dpkg-deb --fsys-tarfile` path followed by independently pinned GNU tar with
explicit `--keep-directory-symlink`, preserving and verifying the known
directory aliases and loader/tool identities before and after extraction.
All eight admitted archives fit the 16 MiB per-package limit (9,830,400 bytes
total uncompressed). Source review and fixture tests cover the replacement;
fresh installed-Mac verification remains required. See
[GNU tar's option semantics](https://www.gnu.org/software/tar/manual/html_node/Option-Summary.html)
and the [matching dpkg source archive](https://deb.debian.org/debian/pool/main/d/dpkg/dpkg_1.21.23.tar.xz).

The authenticated OpenSSH 9.2p1 source defines Linux's locked-password prefix as
`!` (`configure.ac`); `platform.c` checks that prefix, and `auth.c` applies the
check when PAM is disabled. Therefore a dedicated fixture account with shadow
password `*` and no expiry is password-disabled without matching that OpenSSH
lock check. No valid password hash or password unlock is needed. The fixture
must reject pre-existing account/UID/GID collisions, preserve existing accounts,
provide an executable login shell, and explicitly create its privilege-separation
account and root-owned `/run/sshd` because package maintainer scripts do not run.
Debian's `rules` and `openssh-server.postinst` establish the `/run/sshd` path.

The test server keeps `UsePAM no`, password and keyboard-interactive
authentication disabled, public-key authentication required, and root login
disabled. These are fixture constraints, not a recommendation to replace a
general-purpose host's account policy.

Negative diagnostic matching follows the authenticated Debian source, not just
upstream text. `mention-ssh-keygen-on-keychange.patch` adds the `remove with:`
and `ssh-keygen -f ... -R ...` lines to changed-host-key output. `log.c` writes
CRLF per log record while the fingerprint message contains an embedded LF.
Retain and validate those exact bytes; do not broaden a negative test to accept
unrelated connection or authentication failures.

## Installed-Mac SSH lane

The explicit `scripts/run-linux-docker-e2e.sh --suite ssh` DEV lane takes the
normal installed-release, Developer/Hardened bundle, host Docker/plugin, and
fresh evidence-directory arguments, plus `--buildkit-archive` and
`--ssh-packages`. The latter names the retained, descriptor-verified input
directory described above, not an arbitrary package directory. Optional
`--ssh-gpgv` selects the verifier executable; its bytes are frozen before use.

Three Linux Machines each run four uncached builds against their own builder
and disposable, unexposed SSH server: supplied agent without a Dockerfile
mount, required mount without a supplied provider, wrong pinned host key, and
successful declared forwarding. The Mac's normal SSH agent, keys, Keychain,
and Docker default context are not selected. This is build-time agent
forwarding, not a new SSH login or public lifecycle command.

Exact negative diagnostics and raw Buildx progress are independently replayed
before a nonzero build can be acknowledged as an expected denial. The positive
OCI output and exported cache are inspected. After normal builder shutdown,
its complete cache archive is streamed into a private quarantine and scanned
before publication or builder removal. The scanner checks literal private-key
canaries through supported nested archive/compression formats; unsupported
formats and exceeded limits fail closed. It does not claim secure erasure,
arbitrary encoding detection, deleted-sector inspection, or live-memory review.

Failures retain uncertain guest objects and private quarantine bytes; there is
no automatic retry, builder restart, or global prune. Successful workload
cleanup still requires public Machine Stops and unchanged host defaults.
Stopped Machine disks remain retained: this lane does not certify Delete.
Implementation and helper tests alone do not pass this lane, the full
63-scenario Docker contract, or the aggregate 0.4 release gate.
