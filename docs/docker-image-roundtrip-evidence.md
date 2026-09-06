# Docker image round-trip evidence

Status: DEV implementation foundation, not an installed Docker pass or release
certification. Tracked by `vz-mzs.7.1.8`; the complete Docker catalog and the
three-phase 0.4 aggregate remain separate requirements.

The host recipe covers four image expectations: tag, inspect, save/load and
owned removal. It creates tiny deterministic ARM64/Linux subject and decoy
images, each with one public data file and no executable. The fixture does not
require registry access, a base-image build or a fallback Docker daemon.

Each fixture supplies an OCI index, manifest, config, uncompressed layer and
Docker compatibility manifest. Subject and decoy have distinct content hashes;
changing their source-derived per-Machine tags does not change their content.
The pure archive verifier checks all members, JSON records, descriptors, content
hashes, layer payload bytes and padding under a 1 MiB bound. It does not extract
files and does not accept arbitrary OCI archives.

The recipe records the complete bounded baseline image inventory, loads both
fixtures, tags the subject, and verifies exact image config/platform/references.
It saves the alias, removes every subject reference and verifies the subject ID
is absent, then loads those exact saved bytes. A second save must independently
validate the same manifest/config/layer/payload; TAR padding need not be identical.
Subject removal must preserve the decoy and unrelated baseline. Decoy cleanup is
an explicit later step, allowed only after independent workload replay succeeds.

Every mutation has authenticated Machine-context/Engine guards. Host stdin,
stdout, stderr, EOF, bounded timing and owned process completion are retained.
Independent replay reconstructs the command program from source rather than
trusting recorded command choices. Failed commands and assertions retain their
registered ownership and evidence; no automatic retry, force removal or prune is
part of the recipe.

Pinned Moby 29.7.2's containerd image backend reports the manifest digest as
image ID, independently of the config digest. Inspect uses familiar repository
tags and synthesizes/deduplicates repository digests. Load acknowledgements also
use familiar names. The archive naming and compatibility records follow its
vendored containerd exporter. These source-derived expectations still require
verification against the installed guest Engine.

The currently unused Machine adapter accepts the harness's authenticated
descriptor/scope/runtime proof, pins its execution and replay helpers, and
registers its Driver before any image mutation. It rechecks source/runtime/input
identity and earlier cleanup certainty at boundaries; only exact raw replay and
retained-document readback permit its cleanup flag to become true. Independent
review found no concrete blocker. The focused image family passes 56 host tests;
the broader affected host suite passes 854 tests in 139.634 seconds, log
`.artifacts/docker-image-foundation-host-1.log`, SHA-256
`6cec1103d58961a1157e87a6f1ea617abdb9c8355e44e4b706030d69fd5870ed`.

Remaining acceptance: wire the adapter into the installed harness with pinned
inputs, then run it across three fresh vz Linux Machines while observing four
isolation sentinels. Require independent raw replay, owned object cleanup, four
clean-journal public Stops and unchanged host Docker defaults. Registry
login/pull/push are not covered by these four cases. Stopped disks are retained;
this focused slice does not certify Delete or the complete 63-case gate.
