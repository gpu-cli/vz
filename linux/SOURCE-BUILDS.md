# Case-preserving Linux source builds

Kernel and BusyBox source extraction **and compilation** require case-sensitive
storage. Running `make` in Docker while bind-mounting a case-insensitive Mac
source directory does not satisfy this requirement. Linux 6.12.85 contains 13
case-distinct path pairs, including `xt_TCPMSS.h` and `xt_tcpmss.h`, with different
contents. A previously booted kernel is not evidence that these sources survived
extraction intact.

## Supported local-Mac path

Select the local build context explicitly; these commands never change Docker's
global current context. Only a local Unix-socket context is accepted.
Full builds use the default checkout-local youki cache. Unset `YOUKI_CACHE_DIR`:
the wrapper rejects nonempty overrides before invoking Docker because external
youki caches are not mounted into the Linux builder. Kernel-only and source-check
targets do not consume youki and are unaffected by that override.

```bash
export LINUX_DOCKER_CONTEXT=orbstack
export YOUKI_DOCKER_CONTEXT=orbstack
make -C linux docker-build-all DOCKER_BUILD_JOBS=4
```

`docker-build-all` builds Developer then Hardened/Container, sequentially. Its
default output root is `linux/out`, with the Container profile in
`linux/out/container`. To build candidates without replacing existing artifacts,
choose a new absolute output root:

```bash
make -C linux docker-build-all \
  ALL_OUT_DIR=/absolute/path/to/new-linux-candidates \
  DOCKER_BUILD_JOBS=4
```

For one profile use `docker-build KERNEL_PROFILE=developer` (or `container`),
with an optional absolute `OUT_DIR`. Without `LINUX_DOCKER_BUILDER`, the wrapper
builds `linux/Dockerfile`; set that variable to an already-prepared local
Linux/arm64 image when intentionally reusing a builder. The resolved image ID,
not its mutable tag, identifies the build and cache. The Dockerfile pins the
official ARM64 Rust 1.89.0 Bookworm base by manifest digest, and the wrapper checks
the selected builder's Rust version before starting. This is a pinned toolchain
choice; it does not assert that the Linux guest requires a 1.89 minimum version.

For a focused builder check, `LINUX_DOCKER_TARGET=source-check` extracts and
verifies both pinned archives without compiling or changing kernel artifacts;
`LINUX_DOCKER_TARGET=kernel` builds only the selected kernel. Neither focused
target substitutes for rebuilding both complete profiles and running their
backend gate.

The wrapper mounts the checkout read-only and gives the container writable
access only to the selected artifact output, archive cache, and an owned Docker
build volume. Source extraction, out-of-tree compiler output, and Cargo work all
live in that Linux volume. The volume name and ownership labels bind the
profile, recipes, config fragment, and resolved builder ID. An existing volume
with mismatched labels is refused. Concurrent users of an identical build volume
serialize on its profile lock. Developer and Container use separate volumes;
native Linux builds also have separate per-profile object directories.

These volumes intentionally persist for incremental builds and diagnostics.
The wrapper prints their exact names. It does not prune volumes or delete old
host source trees. Before any manual cleanup, inspect the exact volume's labels
and make sure no build owns it; never remove a wildcard set of Docker volumes.

For capacity planning, allow roughly 10–20 GiB of Linux Docker storage per
uncached Developer build volume (kernel source, objects, and Rust dependencies),
plus the builder image and archive cache. Start with four build jobs and at least
8 GiB available to the Linux builder. These are planning estimates, not measured
performance or resource guarantees. Do not overlap fresh kernel builds with the
local-vz physical verification gate on a memory-constrained host.

## Pinned source and artifact evidence

`source-build.py` verifies the publisher-pinned SHA-256 before reading an
archive. Kernel version/hash live in `kernel-version.mk`; BusyBox version/hash
live in `Makefile`. Version changes require updating their checksum pins too.
The checksum sources are:

- [kernel.org signed checksum listing](https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc)
- [BusyBox 1.37.0 checksum](https://busybox.net/downloads/busybox-1.37.0.tar.bz2.sha256)

Before extraction, the helper probes both source and object storage for genuine
case sensitivity. It inventories every archive entry, rejecting duplicate,
escaping, and unsupported paths. After extraction it verifies exact inventory,
file bytes, modes, symlink targets, and distinct inodes for every case-distinct
path. Existing content is verified, never silently repaired or replaced.

Sources use archive-hash-qualified directories. Kernel and BusyBox build with
`O=` in separate object directories, and their source inventories are checked
again after compilation. Build timestamps derive from the pinned archive;
kernel user, host, and build sequence are fixed rather than taken from a random
container hostname. Compiler/Kbuild/parent-make override variables are discarded;
only ordinary path/home/temp locations and fixed locale/time/build identity reach
the compiler build process. Native cache checks also compare the current compiler
identification, not merely its executable name.

Candidate provenance is written and validated before an existing binary is
replaced. Existing binaries and sidecars are retained in an exactly named
`.previous-<artifact>-*` recovery directory before publication; its path is
printed. A partial filesystem publication failure is an error, never a valid
cache hit, and does not make the previous pair unrecoverable. FIFO, symlink,
device, directory, and hardlinked proof/artifact inputs are refused before reads.

Each binary has a `vmlinux.build.json` or `busybox.build.json` sidecar containing
its digest/mode/size, profile, complete source identity and case-pair evidence,
config digests and the complete effective config, recipe digests, build parameters, compiler identification, and
builder image ID. These sidecars bind source/build provenance to the exact
binary; they do not certify runtime behavior. Release CI retains all four
sidecars as `linux-source-build-provenance` and caches kernel sidecars with their
images. Cached artifacts are accepted only after checking this evidence against
the current inputs. `TRUST_EXISTING_KERNEL_IMAGE=1` now requires valid matching
provenance; it cannot bless an old image by timestamp or existence alone.

Direct `make all`/`make kernel` remain available on a case-sensitive native Linux
build filesystem. They deliberately refuse a case-insensitive Mac checkout.
Use the Docker wrappers on the Mac; do not delete `linux/src` or touch output
timestamps to bypass the check.

## Verification status

The offline tests run with:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 linux/test-source-build.py
bash -n linux/docker-build.sh
```

They test checksum/path refusal, preservation of existing sources, case-pair
inventory, and cache-proof drift. Small extraction tests mock only the storage
probe so they can run on the case-insensitive Mac; this is not evidence that a
Linux build or case-preserving extraction passed.

Issue `vz-5in.1` remains open until fresh builds of **both** profiles produce the
case-preservation/provenance evidence and pass the complete local-vz Linux
backend gate. Old kernel boot results, source inventory alone, and offline tests
cannot close it or certify a 0.4 release.
