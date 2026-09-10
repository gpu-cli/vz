# vz 0.4.0: where it stands

Status: living summary, rewritten when a gate run changes it
Last measured: 2026-09-10, release candidate `0.4.0-rc3`, Apple-silicon macOS 26.3.1

This is the short answer to "what's left". The gate itself is
[GOAL-0.4.0.md](GOAL-0.4.0.md); this file says which of its claims are proved,
which are disproved, and what is in the way.

## The one-line version

The **harness is finished** -- all eighteen topology-lane scenarios have real
checks, and nothing in that lane reports "needs provisioned Machines" any more.
What remains is **product work**, and the gate now names it precisely instead of
failing in a heap.

## What the last hardware run proved

Topology lane, clean-provision phase, against a locally signed release candidate
with a registered native macOS target and guest bundles built from source.
**Thirteen of sixteen sub-checks pass.**

| Passing on hardware | |
|---|---|
| criterion 1 | three concurrent Environments, no collision |
| criterion 2 | mixed Linux + native macOS topology and status |
| criterion 15 | CLI/API agreement, including live gRPC and the status field set |
| criterion 19 | clean install, upgrade from the pinned v0.3.20 fixture, injected migration failure and rollback, uninstall |
| criterion 21 | legacy CLI removal, bare help, bootstrap rules |

Criterion 2 is the notable one: a native macOS Machine boots, is supervised, and
reports correctly beside Linux Machines in one Environment. Criterion 19 is the
most substantive: it opens the restored store with the real v0.3.20 daemon.

Zero daemons leak. Earlier runs left eight to ten alive, which on this host
exhausts the macOS virtual-machine cap and makes every later Environment fail
with `VZErrorDomain:6` -- three criteria failed that way before it was found,
with evidence blaming the Environment rather than the daemons nobody stopped.

## What is in the way, in the order it blocks things

Each of these is a filed issue, and each is the reason a specific criterion
cannot pass. None of them is a harness gap.

### P0

1. **There is no reconciliation.** `vz up` refuses *every* ProjectDefinition
   change before admission -- `project definition drift`, `StackError::InvalidSpec`.
   No plan is derived, no durable claim is taken, and a mutable field change gets
   the same code as an immutable one. Neither of criterion 22's two normative
   sub-documents has an implementation subject at all: `admit_reconcile_round`,
   `ReconcileInputSnapshot` and `effective_digest` exist nowhere in `crates/`.
   *Blocks criterion 22 entirely.*

2. **No machine-scoped lifecycle operation.** Every runtime teardown primitive is
   fenced on a persisted Environment-wide `EnvironmentLifecycleOperation` whose
   structure check requires one machine step per Machine, so `vz delete --machine`
   resolves a fork and then refuses. The refusal is deliberate: a partial teardown
   leaks a host Docker context and a runtime store *while the ownership rows claim
   reclamation*. *Blocks criterion 23's delete clause.*

### P1

3. **Native macOS Machines take no fabric address.** The declaration is accepted
   at every layer now and the Environment comes up; the guest just never gets an
   address on the subnet. A Linux guest reads its address off the kernel cmdline;
   a macOS guest is given it over the agent channel during readiness, and that
   half does not work. *Blocks criterion 5's crossing and criterion 12's
   cooperating Linux-to-macOS pair.*

4. *Closed.* **Guests resolved through public DNS.** `resolv.conf` came up as
   `[1.1.1.1, 8.8.8.8]` instead of the Environment's declared gateway. The
   per-Environment edge and its `vz.dns.N` kernel argument closed it; re-measured
   against `0.4.0-rc3` on 2026-09-10, every Machine on the public-like network
   came up with its Environment's resolver and nothing else, the declared `.test`
   name resolved to the edge, an undeclared name did not resolve, and neither
   Environment's name resolved in the other. See "What criterion 6 now proves"
   below. Criterion 8's resolve clause still reports `not_implemented`, but for
   a different reason, recorded there.

5. **A refused `vz up` mutates the worktree**, minting `.git/vz/workspace-id`.
   Fail-before-mutation is stated for definition changes and for bootstrap, and
   this is the same contract. It leaves a binding artifact a later Up will adopt.

6. **Capability negotiation is a rubber stamp.** `negotiated_capabilities =
   requested_capabilities.clone()`, so a Machine asking for `snapshot` is granted
   it while the capability matrix says PLANNED. Every consumer that trusts
   capability discovery -- help, docs, site copy, status -- inherits the false
   claim. *Blocks criterion 18's snapshot clause.*

7. **No `SecretBinding` exists** -- not in the project schema, not in
   `vz-runtime-contract`. The gate check for it is written and waiting: it plants
   a high-entropy sentinel through the CLI environment, reads it back only as a
   digest, and sweeps seven artifact groups for the literal bytes.
   *Blocks criterion 18's secrets clause.*

8. **A host import declared for a stream protocol also passes UDP** to the same
   guest port. A grant that widens from what was declared is the same class of
   defect as treating a NAT alias as authorization: the boundary is not where
   the declaration says it is. Newly visible -- until the guest bundle was
   rebuilt, criterion 7 failed at `up` and never reached its own claims.

9. **A guest RPC was added without bumping `AGENT_PROTOCOL_REVISION`.** The
   handshake that exists to refuse a stale guest passed, and the guest answered
   `Unimplemented` deep inside `up` instead. The revision is a hand-maintained
   constant with nothing tying it to the surface it describes. Note the
   release-grade build path already prevents this by rebuilding both bundles
   every time; only the `--reuse-guest-bundles` DEV shortcut exposes it.

### P2

10. **A host export port collision is refused without a structured error.** The
    port is not silently shared, which is the half that matters, but stderr is
    empty -- no envelope, no machine-readable code, so an agent cannot tell a
    collision from any other refusal.

## What criterion 6 now proves

Measured 2026-09-10 by running the sub-check alone
(`developer_environment_e2e.py --only public_like_ingress`) against `0.4.0-rc3`,
whose guest bundles were built from source rather than reused. Every clause the
criterion names ran from inside real Machines and passed:

| Clause | Observed |
|---|---|
| environment-local split DNS | `resolv.conf ['10.150.7.1']` on both Machines, equal to the `vz.dns.0` the host derived and wrote to each kernel cmdline, and equal to each Machine's declared route |
| the resolver is the edge, not the Machine | resolver `10.150.7.1`; Machine addresses `10.150.7.87` and `10.150.7.58` |
| a `.test` hostname | `api.one.test` resolved to `10.150.7.1`, the edge, and never to the origin Machine behind it |
| the resolver was genuinely asked | no `/etc/hosts` entry for the published name on either Machine |
| the view is split, not shared | an undeclared name did not resolve; `api.one.test` did not resolve in the second Environment and `api.two.test` did not resolve in the first |
| TLS | `TLSv1_3`, status 200, verified against the Environment's own published authority and refused (exit 7, `UnknownIssuer`) against both the image's public CA bundle and the other Environment's authority |
| routed ingress | the response carried the token the declared origin Machine wrote, on its declared port |
| NAT | the origin's own `REMOTE_ADDR` was the edge; spoken to directly by its sibling the same origin reported the caller |
| nothing on the host LAN | no attributable host LAN or wildcard listener; the edge address bound nowhere on the host |

The sub-check still grades `not_implemented`, and names why: it did not exercise
controlled egress (`EgressPolicy` admits only `Offline`), host import/export, or
network faults. Those are separate criteria; none of them is a DNS gap.

The wildcard `*:53` listener that appears while Machines run is macOS's own DNS
proxy for the shared vmnet NAT segment, not a vz socket. The listener sweep now
records it as unattributable rather than charging it to vz.

Criterion 8's resolve clause is still `not_implemented`, and its own words say
why: the Environments its phase establishes declare no networks and no
endpoints, so none of them publishes a name and none is given a resolver to ask.
That is now a harness gap, not a product one -- criterion 6 proves the product
answers exactly this question for two Environments that *do* declare. Closing it
means giving `establish_recovery_environments` a declared network and endpoint.
Note the one product constraint on doing so: a native macOS Machine on a
`simulated_public` network is refused at plan time (`UnresolvedPublicMachine`),
because the native addressing channel installs an address and a route but has no
resolver step, so those Environments must stay all-Linux until it does.

## What the gate will not tell you yet

- **The fork check has now run against real VMs and does not yet get far enough
  to measure.** It brings the parent Environment up, records its identities and
  round-trips a sentinel, then fails importing a warm image into the parent's
  own engine: `docker --context <name>` cannot resolve a context whose name
  `vz status` itself just reported. Two bounds a first *complete* run must still
  settle: `FORK_SPEEDUP_MIN = 2.0` and `FORK_FREE_SPACE_FRACTION = 0.25`.
  The free-space window spans the whole `up`, so it carries the fork's own boot
  writes. The check records the cold Up's wall time and free-space delta beside
  the fork's so those bounds can be judged from evidence; revising them is a
  product decision, not a repair to the measurement.
- **No aggregate run has completed.** The four-phase gate needs a real Mac sleep
  between pre-sleep and post-wake, so it is an attended run.
- **The pinned macOS template lives in `/private/tmp`.** Rebuilding it costs an
  IPSW download and an administrator authorisation. It is one purge away from
  gone and should be moved somewhere durable.

## Measured facts worth keeping

| | |
|---|---|
| `clonefile(2)` of an 80 GiB template holding 32.9 GiB | **0.029 s, 28 KB of volume free space** |
| the same file, held open by a writer mid-`fsync` | clone exit 0 in **0.077 s** |
| a Machine's Docker `data.img` | 64 GiB logical, **29 MB of data in 78 extents** |
| APFS `st_blocks` of a clone versus its parent | **exactly equal** -- which is why the criterion measures free space |

The last row is the one that keeps being rediscovered. A per-file allocated-size
comparison reads a *correct* copy-on-write clone as a deep copy. Criterion 23 was
rewritten around it once and the fork check asserts the per-file equality
alongside the free-space bound so it cannot be undone by accident.
