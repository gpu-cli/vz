"""The cross-Machine half of `docker.network.dns`: a foreign Environment's alias.

One Machine slice can show that a name produced no address. It cannot show that
the Environment owning that name was live at the instant it asked -- and a
denial of a name nobody was serving is the same shape of empty proof as
disjointness from an Environment that owns nothing. So each slice records, from
its own receipts, the window in which it proved *its own* Compose alias
resolving, and `verify_dns_boundary` requires every denial to sit inside the
window of the slice that owns the denied name.

`Rendezvous` is what makes that window reliable rather than lucky: every
concurrently running compose slice stops at the same four points, so all the
local proofs close before any denial is asked and none reopens until every
denial has been. It is an ordering device only. A run given no rendezvous
records the same timestamps and is decided by the same containment; it simply
has nothing arranging them.

Aliases are recomputed here from each Machine's own admitted run id and scope,
so a slice cannot nominate whichever foreign name suited it.
"""
from __future__ import annotations

import threading

import docker_host_driver as driver

require = driver.require
SCOPE = "DEV_installed_cross_Environment_Compose_DNS_boundary_not_release_certification"
# Long enough that a slow Machine's Compose `up --wait` is never the reason a
# peer gives up, short enough that a slice which died before the barrier fails
# the run rather than hanging it. `abort` is the normal exit for that case.
RENDEZVOUS_TIMEOUT = 300
POINTS = ("compose-dns-live", "compose-dns-probe", "compose-dns-denied", "compose-dns-bracket")


class Rendezvous:
    """The four points every compose slice of one run passes together."""

    def __init__(self, parties, timeout=RENDEZVOUS_TIMEOUT):
        require(type(parties) is int and parties >= 2, "a rendezvous needs at least two Machine slices")
        self.parties, self.timeout = parties, timeout
        self.barrier = threading.Barrier(parties)
        self.reached = []

    def wait(self, label):
        require(label in POINTS, "unknown compose DNS rendezvous point: " + str(label))
        try:
            self.barrier.wait(timeout=self.timeout)
        except threading.BrokenBarrierError as error:
            raise driver.Rejected(
                "compose DNS rendezvous " + label + " was not reached by every Machine slice") from error
        self.reached.append(label)

    def abort(self):
        """Release everyone waiting; a slice that failed will never arrive."""
        self.barrier.abort()


def machine_alias(run_id, scope):
    """The Compose container name a Machine's own admitted inputs fix in advance."""
    return driver.owner_token(run_id, scope) + driver.DNS_SUFFIX


def foreign_aliases(run_id, scopes, scope):
    """The live aliases of every *other* Environment in this run, for one Machine."""
    rows = []
    for peer in scopes:
        if peer["environment_id"] == scope["environment_id"]:
            continue
        rows.append({"environment_id": peer["environment_id"], "alias": machine_alias(run_id, peer)})
    return rows


def _slice(observation, run_id):
    scope = observation["scope"]
    dns = observation["independent_validation"]["dns_boundary"]
    require(isinstance(dns, dict) and dns.get("schema_version") == 1, "malformed slice DNS evidence")
    require(dns["own_alias"] == machine_alias(run_id, scope),
            "a slice's Compose alias is not the one its own admitted inputs fix")
    for key in ("resolved_from_unix_ns", "resolved_until_unix_ns"):
        require(type(dns[key]) is int and dns[key] > 0, "invalid local proof window")
    require(dns["resolved_from_unix_ns"] <= dns["resolved_until_unix_ns"], "local proof window closed before it opened")
    require(type(dns["local_resolutions"]) is int and dns["local_resolutions"] >= 2,
            "a slice must bracket its denials with its own resolutions on both sides")
    stale = dns["stale"]
    require(isinstance(stale, dict) and stale["removed_container"] != stale["replacement_container"] and
            stale["names_unresolved_after_remove"] == [driver.DNS_ROLE, dns["own_alias"]] and
            stale["control_name_resolved"] == "worker" and stale["names_restored_at"],
            "a slice's stale-alias evidence is incomplete")
    return {"environment_id": scope["environment_id"], "machine_id": scope["machine_id"], **dns}


def verify_dns_boundary(observations, run_id):
    """Every denial held against the live window of the Environment that owns the name.

    Rejects, in this order: fewer than two slices; one Environment; a slice that
    denied a name no other Environment declared, or missed one that did; a
    denial whose named owner is not the Environment that actually owns it; and a
    denial outside that owner's own proof window, which is the case a peer that
    was never running would produce.
    """
    require(type(observations) is list and len(observations) >= 2,
            "the foreign-Environment alias claim needs at least two Machine slices, observed " +
            str(len(observations) if isinstance(observations, list) else 0))
    rows = [_slice(observation, run_id) for observation in observations]
    by_alias = {}
    for row in rows:
        require(row["own_alias"] not in by_alias, "two Machine slices declared the same Compose alias")
        by_alias[row["own_alias"]] = row
    environments = sorted({row["environment_id"] for row in rows})
    require(len(environments) >= 2,
            "the foreign-Environment alias claim needs two Environments, observed " + str(len(environments)))
    denials, pairs = 0, set()
    for row in rows:
        expected = {other["own_alias"] for other in rows if other["environment_id"] != row["environment_id"]}
        require(expected, "a slice had no foreign Environment whose alias it could be denied")
        observed = [entry["alias"] for entry in row["foreign_denials"]]
        require(len(observed) == len(set(observed)) and set(observed) == expected,
                "a slice did not deny exactly the live foreign Environment aliases of this run: " +
                str(sorted(set(observed) ^ expected)))
        for entry in row["foreign_denials"]:
            owner = by_alias[entry["alias"]]
            require(entry["environment_id"] == owner["environment_id"] != row["environment_id"],
                    "a denied alias was attributed to an Environment that does not own it")
            require(type(entry["at_unix_ns"]) is int and
                    owner["resolved_from_unix_ns"] <= entry["at_unix_ns"] <= owner["resolved_until_unix_ns"],
                    "a foreign alias was denied outside the window its own Environment proved it resolving: " +
                    entry["alias"])
            denials += 1
            pairs.add((row["environment_id"], owner["environment_id"]))
    return {"schema_version": 1, "scope": SCOPE, "machine_slices": len(rows), "environments": len(environments),
            "foreign_environment_denials": denials,
            "ordered_environment_pairs_covered": sorted(list(pair) for pair in pairs),
            "aliases": {row["machine_id"]: row["own_alias"] for row in rows},
            "own_alias_resolutions": {row["machine_id"]: row["local_resolutions"] for row in rows},
            "stale_alias_removals": len(rows),
            "full_dns_certified": False}
