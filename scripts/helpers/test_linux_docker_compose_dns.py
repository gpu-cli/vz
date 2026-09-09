"""The cross-Environment DNS decision, over synthetic slices; never Docker."""
import copy
import threading
import time
import unittest

import docker_host_driver as driver
import linux_docker_compose_dns as subject

RUN = "synthetic-dns-run-1"


def scope(environment, machine):
    return {"project_id": "project", "environment_id": environment, "machine_id": machine,
            "machine_incarnation": machine + "-inc", "runtime_identity": machine + "-rt",
            "docker_context": machine + "-ctx", "docker_endpoint": "unix:///tmp/" + machine + ".sock",
            "engine_id": machine + "-engine"}


PRIMARY_A, PRIMARY_B, NEIGHBOUR = scope("env-primary", "m0"), scope("env-primary", "m1"), scope("env-neighbour", "m2")
ALL = [PRIMARY_A, PRIMARY_B, NEIGHBOUR]


def slice_for(own, start, denials):
    alias = subject.machine_alias(RUN, own)
    return {"scope": copy.deepcopy(own), "started_unix_ns": start, "ended_unix_ns": start + 1000,
            "independent_validation": {"dns_boundary": {
                "schema_version": 1, "own_alias": alias, "own_address": "172.18.0.30",
                "resolved_from_unix_ns": start, "resolved_until_unix_ns": start + 900,
                "local_resolutions": 6, "foreign_denials": denials,
                "stale": {"removed_container": "a" * 64, "replacement_container": "b" * 64,
                          "names_unresolved_after_remove": ["api", alias],
                          "control_name_resolved": "worker", "names_restored_at": "172.18.0.31"}}}}


def observations(denial_at=500):
    """Three slices over two Environments, each denying every foreign alias."""
    rows = []
    for own in ALL:
        denials = [{"alias": subject.machine_alias(RUN, peer), "environment_id": peer["environment_id"],
                    "at_unix_ns": denial_at}
                   for peer in ALL if peer["environment_id"] != own["environment_id"]]
        rows.append(slice_for(own, 100, denials))
    return rows


def dns(row):
    return row["independent_validation"]["dns_boundary"]


class BoundaryTests(unittest.TestCase):
    def test_a_complete_run_reports_its_observed_counts(self):
        proof = subject.verify_dns_boundary(observations(), RUN)
        self.assertEqual(proof["machine_slices"], 3)
        self.assertEqual(proof["environments"], 2)
        # Two primary Machines deny the neighbour's alias, and the neighbour
        # denies both primaries': four denials, both ordered pairs covered.
        self.assertEqual(proof["foreign_environment_denials"], 4)
        self.assertEqual(proof["ordered_environment_pairs_covered"],
                         [["env-neighbour", "env-primary"], ["env-primary", "env-neighbour"]])
        self.assertEqual(proof["stale_alias_removals"], 3)
        self.assertEqual(set(proof["own_alias_resolutions"].values()), {6})
        self.assertIs(proof["full_dns_certified"], False)

    def test_one_environment_can_never_prove_the_foreign_claim(self):
        rows = [slice_for(PRIMARY_A, 100, []), slice_for(PRIMARY_B, 100, [])]
        with self.assertRaisesRegex(driver.Rejected, "needs two Environments"):
            subject.verify_dns_boundary(rows, RUN)

    def test_one_slice_is_not_a_cross_environment_observation(self):
        with self.assertRaisesRegex(driver.Rejected, "at least two Machine slices"):
            subject.verify_dns_boundary(observations()[:1], RUN)

    def test_a_denial_outside_the_owners_live_window_is_rejected(self):
        # The whole point: a name nobody was serving is denied just as readily
        # as a name behind an enforced boundary.
        for offset in (-1, 10_000):
            with self.subTest(offset=offset):
                rows = observations()
                owner = dns(rows[2])
                for row in rows[:2]:
                    for entry in dns(row)["foreign_denials"]:
                        if entry["alias"] == owner["own_alias"]:
                            entry["at_unix_ns"] = owner["resolved_from_unix_ns"] + offset \
                                if offset < 0 else owner["resolved_until_unix_ns"] + offset
                with self.assertRaisesRegex(driver.Rejected, "outside the window"):
                    subject.verify_dns_boundary(rows, RUN)

    def test_a_slice_that_skipped_a_foreign_environment_is_rejected(self):
        rows = observations()
        dns(rows[0])["foreign_denials"] = []
        with self.assertRaisesRegex(driver.Rejected, "did not deny exactly"):
            subject.verify_dns_boundary(rows, RUN)

    def test_a_denial_of_an_invented_name_is_rejected(self):
        rows = observations()
        dns(rows[0])["foreign_denials"][0]["alias"] = "vz04-ffffffffffffffffffffffff-compose-api-1"
        with self.assertRaisesRegex(driver.Rejected, "did not deny exactly"):
            subject.verify_dns_boundary(rows, RUN)

    def test_an_alias_not_fixed_by_the_machines_own_inputs_is_rejected(self):
        rows = observations()
        dns(rows[2])["own_alias"] = "vz04-ffffffffffffffffffffffff-compose-api-1"
        with self.assertRaisesRegex(driver.Rejected, "not the one its own admitted inputs fix"):
            subject.verify_dns_boundary(rows, RUN)

    def test_a_denial_attributed_to_the_wrong_environment_is_rejected(self):
        rows = observations()
        dns(rows[0])["foreign_denials"][0]["environment_id"] = "env-primary"
        with self.assertRaisesRegex(driver.Rejected, "Environment that does not own it"):
            subject.verify_dns_boundary(rows, RUN)

    def test_incomplete_stale_alias_evidence_is_rejected(self):
        for field, value in (("control_name_resolved", "api"),
                             ("names_restored_at", ""),
                             ("names_unresolved_after_remove", ["api"])):
            with self.subTest(field=field):
                rows = observations()
                dns(rows[0])["stale"][field] = value
                with self.assertRaisesRegex(driver.Rejected, "stale-alias evidence is incomplete"):
                    subject.verify_dns_boundary(rows, RUN)
        rows = observations()
        dns(rows[0])["stale"]["replacement_container"] = dns(rows[0])["stale"]["removed_container"]
        with self.assertRaisesRegex(driver.Rejected, "stale-alias evidence is incomplete"):
            subject.verify_dns_boundary(rows, RUN)

    def test_a_slice_without_its_own_bracketing_resolutions_is_rejected(self):
        rows = observations()
        dns(rows[0])["local_resolutions"] = 1
        with self.assertRaisesRegex(driver.Rejected, "bracket its denials"):
            subject.verify_dns_boundary(rows, RUN)

    def test_a_slice_cannot_present_another_machines_alias_as_its_own(self):
        rows = observations()
        rows[1] = copy.deepcopy(rows[0])
        rows[1]["scope"] = copy.deepcopy(PRIMARY_B)
        with self.assertRaisesRegex(driver.Rejected, "not the one its own admitted inputs fix"):
            subject.verify_dns_boundary(rows, RUN)


class AliasDerivationTests(unittest.TestCase):
    def test_every_machine_gets_a_distinct_alias_fixed_before_any_slice_runs(self):
        aliases = [subject.machine_alias(RUN, item) for item in ALL]
        self.assertEqual(len(set(aliases)), 3)
        for alias in aliases:
            self.assertTrue(alias.endswith(driver.DNS_SUFFIX))
        self.assertEqual(subject.machine_alias(RUN, PRIMARY_A),
                         driver.owner_token(RUN, PRIMARY_A) + driver.DNS_SUFFIX)

    def test_only_other_environments_are_offered_to_a_machine(self):
        rows = subject.foreign_aliases(RUN, ALL, PRIMARY_A)
        self.assertEqual(rows, [{"environment_id": "env-neighbour",
                                 "alias": subject.machine_alias(RUN, NEIGHBOUR)}])
        # A single-Environment selection can offer a slice nothing, which the
        # Driver refuses rather than proving less than the claim says.
        self.assertEqual(subject.foreign_aliases(RUN, [PRIMARY_A, PRIMARY_B], PRIMARY_A), [])

    def test_the_offered_aliases_are_admissible_driver_inputs(self):
        rows = subject.foreign_aliases(RUN, ALL, PRIMARY_A)
        for row in rows:
            self.assertEqual(set(row), {"environment_id", "alias"})
            driver.checked_text(row["alias"], r"[a-z0-9][a-z0-9-]{0,126}", "alias")


class RendezvousTests(unittest.TestCase):
    def test_every_party_leaves_a_point_only_once_all_of_them_arrived(self):
        point = subject.Rendezvous(3, timeout=5)
        seen, lock = [], threading.Lock()

        def participant(index):
            with lock:
                seen.append(("before", index))
            point.wait("compose-dns-probe")
            with lock:
                seen.append(("after", index))

        threads = [threading.Thread(target=participant, args=(index,)) for index in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual([stage for stage, _index in seen], ["before"] * 3 + ["after"] * 3)

    def test_an_abort_releases_the_peers_instead_of_hanging_the_run(self):
        point = subject.Rendezvous(2, timeout=30)
        errors = []

        def waiting():
            try:
                point.wait("compose-dns-live")
            except driver.Rejected as error:
                errors.append(str(error))

        thread = threading.Thread(target=waiting)
        thread.start()
        # The peer that never arrives is the slice that failed before the point.
        deadline = time.monotonic() + 10
        while point.barrier.n_waiting < 1 and time.monotonic() < deadline:
            time.sleep(0.001)
        self.assertEqual(point.barrier.n_waiting, 1)
        point.abort()
        thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIn("compose-dns-live", errors[0])

    def test_a_rendezvous_needs_at_least_two_slices_and_a_known_point(self):
        with self.assertRaises(driver.Rejected):
            subject.Rendezvous(1)
        point = subject.Rendezvous(2, timeout=1)
        with self.assertRaisesRegex(driver.Rejected, "unknown compose DNS rendezvous point"):
            point.wait("compose-dns-elsewhere")


if __name__ == "__main__":
    unittest.main()
