# Four concurrent BuildKit workers

This isolated DEV fixture does not change the existing Docker build fixture.
Build four distinct slots (0 through 3) on one fresh, explicitly owned builder,
with the same unique `FIXTURE_RUN` and digest-pinned Python `FIXTURE_BASE`.
Each solve uses the `output` target and exports a gzip OCI directory with
attestations disabled. `contract.json` fixes timing and public evidence.
The global base argument defaults to the exact selected ARM64 manifest verified
by `../docker/python-image-input.json`, not its multi-platform index or a tag.
BuildKit 0.19 validates `FROM` using argument defaults before applying supplied
build arguments; this pinned default avoids `InvalidDefaultArgInFrom` without
suppressing warnings. The host still supplies and verifies the explicit base
argument. No other graph, mount, or command behavior changes.

Each network-disabled RUN claims one exclusive directory in the explicitly
shared BuildKit cache. Atomic, bounded ready records bind the run and slot.
Every worker must observe all four immutable records, then dwell at least one
second before writing its slot-specific public payload with mode 0644. All
four transcripts must agree on ordered participants and their canonical SHA256.
Per-slot guest wall/monotonic timestamps and bounded readiness samples prove a
common interval inside four distinct uncached RUNs; a cached or sequential
substitute cannot prove the barrier. Buildx translates each solve's progress
timestamps independently, but preserves durations and log payloads. Replay keeps
those raw timestamps separate from guest clocks. For RUN duration `D` and its
authenticated guest script interval `[S, C]`, `[C-D, S+D]` conservatively encloses
the entire RUN. The one-second HTTP observer must bracket all four of these
guest-clock envelopes; overlapping envelopes alone never prove concurrency.
Shared speculative COPY steps may merge only when the canceled local edge and
adopted successful edge are bound to an unaliased winner in this same four-slot
group. Warnings, arbitrary cancellations, cached RUNs, and unresolved foreign
origins still fail. No host networking or implicit host service is involved.

Failure leaves claims and other artifacts intact: no retry, repair, or worker
cleanup occurs. Reusing a successful or failed barrier cache is an error; the
parent owns authenticated builder disposition. The timeout includes the dwell.
Do not pass `--no-cache`: pinned BuildKit 0.19 prunes cache-mount indexes and
clears active shared-cache references for that option, which could split the
barrier across concurrent solves. A fresh builder, unique run argument, distinct
slot arguments, and four independently verified uncached RUNs prove cold work.
The independent health observer is separate from build execution, and is excluded
from the Docker context along with tests and contract documentation.

Run local process tests with `python3 -B -m unittest discover -s
tests/fixtures/vz-0.4/docker-parallel -p test_parallel.py`. These test the barrier,
not installed Docker capability or the aggregate 63-scenario release gate.
