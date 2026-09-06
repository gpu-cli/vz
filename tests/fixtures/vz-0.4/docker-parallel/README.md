# Four concurrent BuildKit workers

This isolated DEV fixture does not change the existing Docker build fixture.
Build four distinct slots (0 through 3) on one fresh, explicitly owned builder,
with the same unique `FIXTURE_RUN` and digest-pinned Python `FIXTURE_BASE`.
Each solve uses the `output` target and exports a gzip OCI directory with
attestations disabled. `contract.json` fixes timing and public evidence.

Each network-disabled RUN claims one exclusive directory in the explicitly
shared BuildKit cache. Atomic, bounded ready records bind the run and slot.
Every worker must observe all four immutable records, then dwell at least one
second before writing its slot-specific public payload with mode 0644. All
four transcripts must agree on ordered participants and their canonical SHA256.
Per-slot wall/monotonic timestamps and bounded readiness samples permit replay
against raw BuildKit RUN intervals; a cached or sequential substitute cannot
prove the barrier. No host networking or implicit host service is involved.

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
