"""Offline same-Environment isolation adversaries; no Docker dispatch."""
import copy
import json
import unittest

import linux_docker_isolation_machine as subject


def slice_for(machine, environment='env-a', *, owned=None, inventory=None, rechecks=(), started='start'):
    identity = {'container': machine + '-c' * 63, 'image': machine + ':owned',
                'volume': machine + '-state', 'network': machine + '-n' * 63}
    identity.update(owned or {})
    seen = {kind: [value] for kind, value in identity.items()}
    seen.update({'cache': [], 'event': [machine + '-c' * 63]})
    for kind, extra in (inventory or {}).items():
        seen[kind] = sorted(set(seen.get(kind, [])) | set(extra))
    return {'owner': {'project_id': 'p', 'environment_id': environment, 'machine_id': machine},
            'owned': identity, 'inventory': seen,
            'generation': {'id': identity['container'], 'started_at': started, 'pid': 7},
            'rechecks': list(rechecks)}


class VerifyTests(unittest.TestCase):
    def pair(self, **changes):
        first = slice_for('m1')
        second = slice_for('m2', rechecks=[{'machine_id': 'm1', 'generation': copy.deepcopy(first['generation'])}])
        rows = [first, second]
        for index, change in changes.items():
            rows[int(index[-1])].update(change)
        return rows

    def test_two_machines_of_one_environment_sharing_nothing(self):
        proof = subject.verify_machines(self.pair())
        self.assertEqual(proof['cross_visible_containers_images_volumes_networks_events_caches'], 0)
        self.assertEqual(proof['cross_machine_lifecycle_effects'], 0)
        self.assertEqual(proof['developer_linux_machines'], 2)
        self.assertEqual(proof['environment_count'], 1)
        self.assertEqual(proof['rechecks'], 1)
        self.assertFalse(proof['full_isolation_certified'])
        self.assertEqual(proof['machines'], ['m1', 'm2'])
        # The counts are reported so a reader can tell a real disjointness from
        # a vacuous one.
        self.assertEqual(proof['observed_cache_records'], {'m1': 0, 'm2': 0})
        self.assertEqual(proof['observed_event_identities'], {'m1': 1, 'm2': 1})

    def test_any_cross_visible_identity_is_rejected(self):
        for kind in subject.KINDS:
            rows = self.pair()
            rows[1]['inventory'][kind] = sorted(set(rows[1]['inventory'][kind]) | {rows[0]['owned'][kind]})
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, 'cross-visible'):
                subject.verify_machines(rows)
        for kind, value in (('cache', 'sha256:shared'), ('event', 'm1' + 'c' * 63)):
            rows = self.pair()
            rows[0]['inventory'][kind] = sorted(set(rows[0]['inventory'][kind]) | {value})
            rows[1]['inventory'][kind] = sorted(set(rows[1]['inventory'][kind]) | {value})
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, 'cross-visible'):
                subject.verify_machines(rows)

    def test_a_machine_that_cannot_see_its_own_resource_is_rejected(self):
        rows = self.pair()
        rows[0]['inventory']['volume'] = []
        with self.assertRaisesRegex(ValueError, 'cannot see its own'):
            subject.verify_machines(rows)

    def test_a_changed_generation_on_the_far_side_is_a_lifecycle_effect(self):
        rows = self.pair()
        rows[1]['rechecks'] = [{'machine_id': 'm1', 'generation': {'id': rows[0]['owned']['container'],
                                                                   'started_at': 'later', 'pid': 7}}]
        with self.assertRaisesRegex(ValueError, 'changed generation'):
            subject.verify_machines(rows)
        rows = self.pair()
        rows[1]['rechecks'] = [{'machine_id': 'unknown', 'generation': rows[0]['generation']}]
        with self.assertRaisesRegex(ValueError, 'unknown Machine'):
            subject.verify_machines(rows)

    def test_a_silent_event_stream_is_refused_as_vacuous(self):
        # Every category's disjointness is trivially true over empty sets, so a
        # Machine that recorded no event of its own cannot support the claim.
        rows = self.pair()
        rows[1]['inventory']['event'] = []
        with self.assertRaisesRegex(ValueError, 'no event of its own'):
            subject.verify_machines(rows)

    def test_the_manifest_precondition_is_checked_not_inferred(self):
        for rows, reason in (
                ([slice_for('m1')], 'one Machine'),
                ([slice_for('m1'), slice_for('m1')], 'same Machine twice'),
                ([slice_for('m1', 'env-a'), slice_for('m2', 'env-b')], 'two Environments'),
                ([slice_for('m1'), slice_for('m2')], 'no recheck')):
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                subject.verify_machines(rows)

    def test_a_third_machine_in_another_environment_does_not_break_the_pair(self):
        rows = self.pair() + [slice_for('m3', 'env-b')]
        proof = subject.verify_machines(rows)
        self.assertEqual(proof['machines'], ['m1', 'm2'])
        self.assertEqual(proof['environment_id'], 'env-a')
        self.assertEqual(sorted(proof['observed_cache_records']), ['m1', 'm2', 'm3'])


class CoverageTests(unittest.TestCase):
    def test_the_reachable_isolation_id_is_claimed_and_the_sibling_stays_uncovered(self):
        import linux_docker_scenarios as scenarios
        self.assertEqual([claim.id for claim in scenarios.claims('isolation')],
                         ['docker.operation.same_environment_isolation'])
        uncovered = {identifier for identifier, _ in scenarios.UNCOVERED}
        # The sibling row needs a third Environment the harness does not build.
        self.assertIn('docker.operation.sibling_environment_isolation', uncovered)
        self.assertNotIn('docker.operation.same_environment_isolation', uncovered)


if __name__ == '__main__':
    unittest.main()
