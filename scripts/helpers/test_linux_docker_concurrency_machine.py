"""Offline concurrency-suite adversaries; no Docker dispatch and no Machine.

Every test here models the property the suite claims and then breaks it: a
serialized set of registry service windows, an exec that answered another
container's work, a container that started but never became healthy, a pull that
returned another slot's image. None of them replays a canned answer that the
checked function only has to echo back.
"""
import copy
import datetime
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import linux_docker_concurrency_machine as subject

TOKEN = 'vzconc-' + 'a' * 24
INSTANCE = '11111111-2222-3333-4444-555555555555'
SECOND = 10 ** 9


def container_id(index):
    return format(index, 'x').rjust(64, 'b')


def image_id(index):
    return 'sha256:' + format(index, 'x').rjust(64, 'c')


def digest(index):
    return 'sha256:' + format(index, 'x').rjust(64, 'd')


def state_table(rows):
    return ''.join(identity + ' ' + state + ' ' + health + '\n' for identity, state, health in rows).encode('ascii')


def ready_table(count=subject.READY_CONTAINERS, overrides=None):
    overrides = overrides or {}
    rows = []
    for index in range(count):
        identity = container_id(index)
        state, health = overrides.get(index, ('running', 'healthy'))
        rows.append((identity, state, health))
    return state_table(rows)


def exec_report(marker, slot, arrived=subject.PARALLEL_EXECS, waited=1, ready_marker=None):
    head = subject.READY_TEMPLATE.format(marker=ready_marker or marker)
    tail = subject.EXEC_TEMPLATE.format(marker=marker, slot=slot, arrived=arrived, waited=waited)
    return (head + '\n' + tail + '\n').encode('ascii')


def rfc3339(unix_ns):
    moment = datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=unix_ns // 1000)
    return moment.strftime('%Y-%m-%dT%H:%M:%S') + '.' + str(unix_ns % SECOND).zfill(9) + 'Z'


def log_line(*, uri, started_ns, duration, instance=INSTANCE, method='GET', status=200, level='info',
             msg='response completed', extra=None):
    row = {'time': rfc3339(started_ns + duration), 'level': level, 'msg': msg,
           'go.version': subject.registry.GO_VERSION, 'instance.id': instance,
           'version': subject.REGISTRY_LOG_VERSION,
           'http.request.id': INSTANCE, 'http.request.method': method,
           'http.request.host': '127.0.0.1:5000', 'http.request.uri': uri,
           'http.response.status': status, 'http.response.duration': str(duration) + 'ns'}
    if extra:
        row.update(extra)
    return json.dumps(row, sort_keys=True) + '\n'


def startup_log(instance=INSTANCE):
    rows = [{'time': rfc3339(SECOND), 'level': 'info', 'msg': 'listening on [::]:5000',
             'go.version': subject.registry.GO_VERSION, 'instance.id': instance,
             'version': subject.REGISTRY_LOG_VERSION}]
    return ''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows).encode('ascii')


def repositories():
    return tuple(TOKEN + '/slot' + str(slot) for slot in range(subject.PARALLEL_PULLS))


def pull_log(schedule):
    """Render one registry log from {repository: [(start_ns, duration_ns), ...]}."""
    lines = []
    for repository, requests in schedule.items():
        for position, (started, duration) in enumerate(requests):
            kind = 'manifests/v1' if position == 0 else 'blobs/' + digest(position)
            lines.append((started + duration,
                          log_line(uri='/v2/' + repository + '/' + kind, started_ns=started, duration=duration)))
    lines.sort(key=lambda item: item[0])
    return ''.join(line for _, line in lines).encode('ascii')


def concurrent_schedule():
    """Four pulls dispatched together: two requests each, all inside one window."""
    return {repository: [(SECOND + slot, 40 * 10 ** 6), (SECOND + 50 * 10 ** 6 + slot, 40 * 10 ** 6)]
            for slot, repository in enumerate(repositories())}


def serial_schedule():
    """The same work, one repository fully served before the next begins."""
    return {repository: [(SECOND + slot * 10 ** 8, 10 ** 7),
                         (SECOND + slot * 10 ** 8 + 2 * 10 ** 7, 10 ** 7)]
            for slot, repository in enumerate(repositories())}


class FixtureTests(unittest.TestCase):
    def test_pinned_probe_and_contract_agree_with_every_module_constant(self):
        contract = subject.fixture_contract()
        self.assertEqual(contract['script_sha256'], subject.PROBE_SHA256)
        self.assertEqual(contract['contract']['probe']['cases'], sorted(subject.CASES))
        self.assertIn('VZREADY', contract['script'])
        self.assertIn(subject.RENDEZVOUS_PATH, contract['script'])
        self.assertEqual(contract['contract']['concurrent_clients']['ready_containers'], subject.READY_CONTAINERS)

    def test_the_fixture_repeats_the_frozen_manifest_expected_block(self):
        expected = subject.manifest_expectations()
        self.assertEqual(expected, subject.fixture_contract()['contract']['concurrent_clients'])

    def test_changed_probe_or_contract_bytes_are_refused(self):
        root = Path(subject.FIXTURE)
        for name in ('probe.sh', 'fixture.json'):
            with tempfile.TemporaryDirectory() as temporary:
                copy_root = Path(temporary) / 'fixture'
                shutil.copytree(root, copy_root)
                (copy_root / name).write_bytes((copy_root / name).read_bytes() + b'\n# drift\n')
                with self.subTest(name=name), self.assertRaises(Exception):
                    subject.fixture_contract(copy_root)

    def test_a_manifest_whose_expected_block_moved_is_refused(self):
        source = Path(subject.REPO / 'config/docker-compatibility-v0.4.json')
        value = json.loads(source.read_bytes())
        for scenario in value['scenarios']:
            if scenario['id'] == 'docker.operation.concurrent_clients':
                scenario['expected']['parallel_execs'] = 7
        with tempfile.TemporaryDirectory() as temporary:
            drifted = Path(temporary) / 'manifest.json'
            drifted.write_text(json.dumps(value))
            with self.assertRaises(Exception):
                subject.manifest_expectations(drifted)


class ReadyTests(unittest.TestCase):
    def identities(self):
        return [container_id(index) for index in range(subject.READY_CONTAINERS)]

    def test_twenty_healthy_containers_inside_the_window_are_ready(self):
        rows = subject.health_rows(ready_table(), self.identities())
        self.assertTrue(subject.all_ready(rows))
        proof = subject.verify_ready(rows, self.identities(),
                                     dispatched_unix_ns=SECOND, ready_unix_ns=SECOND + 5 * SECOND)
        self.assertEqual(proof['ready_containers'], subject.READY_CONTAINERS)
        self.assertEqual(proof['observed_window_ns'], 5 * SECOND)
        self.assertEqual(proof['readiness_source'], 'engine_state_health_status')

    def test_a_started_but_unready_container_is_never_counted(self):
        """`running` is not readiness: the healthcheck greps the marker the
        container's own entrypoint wrote, so `starting` means it never got there."""
        for state, health in (('running', 'starting'), ('running', 'unhealthy'), ('running', 'none'),
                              ('exited', 'healthy'), ('created', 'none')):
            rows = subject.health_rows(ready_table(overrides={7: (state, health)}), self.identities())
            with self.subTest(state=state, health=health):
                self.assertFalse(subject.all_ready(rows))
                with self.assertRaises(Exception):
                    subject.verify_ready(rows, self.identities(),
                                         dispatched_unix_ns=SECOND, ready_unix_ns=SECOND + SECOND)

    def test_the_state_table_must_be_exactly_the_owned_containers(self):
        identities = self.identities()
        foreign = state_table([(container_id(index), 'running', 'healthy')
                               for index in range(1, subject.READY_CONTAINERS + 1)])
        short = ready_table(count=subject.READY_CONTAINERS - 1)
        duplicated = ready_table() + (container_id(0) + ' running healthy\n').encode('ascii')
        for raw, reason in ((foreign, 'foreign container'), (short, 'missing container'),
                            (duplicated, 'duplicate container'), (b'', 'empty'),
                            (b'deadbeef running healthy\n', 'short id'),
                            ((container_id(0) + ' running healthy').encode('ascii'), 'unterminated'),
                            ((container_id(0) + ' running\n').encode('ascii'), 'missing column')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.health_rows(raw, identities)

    def test_an_unknown_state_or_health_word_is_refused(self):
        raw = (container_id(0) + ' spinning healthy\n').encode('ascii')
        with self.assertRaises(Exception):
            subject.health_rows(raw, [container_id(0)])
        raw = (container_id(0) + ' running ready\n').encode('ascii')
        with self.assertRaises(Exception):
            subject.health_rows(raw, [container_id(0)])

    def test_readiness_outside_the_sixty_second_window_is_refused(self):
        rows = subject.health_rows(ready_table(), self.identities())
        subject.verify_ready(rows, self.identities(), dispatched_unix_ns=SECOND,
                             ready_unix_ns=SECOND + subject.READY_WINDOW_SECONDS * SECOND)
        for dispatched, ready, reason in (
                (SECOND, SECOND + subject.READY_WINDOW_SECONDS * SECOND + 1, 'one nanosecond late'),
                (SECOND, SECOND, 'no elapsed window'),
                (SECOND + SECOND, SECOND, 'ready before dispatch')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.verify_ready(rows, self.identities(), dispatched_unix_ns=dispatched, ready_unix_ns=ready)

    def test_fewer_or_more_than_twenty_containers_cannot_satisfy_the_claim(self):
        for count in (subject.READY_CONTAINERS - 1, subject.READY_CONTAINERS + 1):
            identities = [container_id(index) for index in range(count)]
            rows = subject.health_rows(ready_table(count=count), identities)
            with self.subTest(count=count), self.assertRaises(Exception):
                subject.verify_ready(rows, identities, dispatched_unix_ns=SECOND, ready_unix_ns=SECOND + SECOND)


class ExecReportTests(unittest.TestCase):
    def test_a_full_rendezvous_report_for_its_own_container_is_accepted(self):
        proof = subject.parse_exec_report(exec_report(TOKEN + '-c03', 3), b'', TOKEN + '-c03', 3)
        self.assertEqual(proof, {'marker': TOKEN + '-c03', 'slot': 3,
                                 'arrived': subject.PARALLEL_EXECS, 'waited_seconds': 1})

    def test_an_exec_that_answered_another_container_or_slot_is_refused(self):
        marker = TOKEN + '-c03'
        for raw, reason in (
                (exec_report(TOKEN + '-c04', 3), 'another container'),
                (exec_report(marker, 4), 'another slot'),
                (exec_report(marker, 3, ready_marker=TOKEN + '-c09'), 'another ready marker'),
                (exec_report(marker, 3, arrived=subject.PARALLEL_EXECS - 1), 'partial rendezvous'),
                (exec_report(marker, 3, arrived=subject.PARALLEL_EXECS + 1), 'extra participant'),
                (exec_report(marker, 3, waited=subject.RENDEZVOUS_POLL_SECONDS + 1), 'waited past its bound'),
                (exec_report(marker, 3) + exec_report(marker, 3), 'two reports'),
                (exec_report(marker, 3)[:-1], 'unterminated'),
                (b'', 'empty')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.parse_exec_report(raw, b'', marker, 3)

    def test_diagnostics_on_the_exec_stream_are_refused(self):
        with self.assertRaises(Exception):
            subject.parse_exec_report(exec_report(TOKEN + '-c00', 0), b'warning\n', TOKEN + '-c00', 0)


class OverlapTests(unittest.TestCase):
    def test_intervals_sharing_one_instant_overlap_and_serialized_ones_do_not(self):
        proof = subject.mutual_overlap([[10, 40], [20, 50], [5, 45], [25, 41]], 'unused')
        self.assertEqual(proof['overlap_started_unix_ns'], 25)
        self.assertEqual(proof['overlap_ended_unix_ns'], 40)
        self.assertEqual(proof['participants'], 4)
        for intervals, reason in (
                ([[10, 20], [20, 30]], 'adjacent but never simultaneous'),
                ([[10, 20], [30, 40], [50, 60], [70, 80]], 'strictly serialized'),
                ([[10, 40], [20, 50], [5, 45], [45, 60]], 'one straggler outside the window'),
                ([[10, 40]], 'a single client cannot be concurrent'),
                ([[40, 10], [20, 50]], 'reversed interval'),
                ([[10, 10], [5, 20]], 'zero-length interval')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.mutual_overlap(intervals, 'refused')


class ClockTests(unittest.TestCase):
    def test_rfc3339_nano_round_trips_and_bad_syntax_is_refused(self):
        for value in (1234567890 * SECOND, 1234567890 * SECOND + 123456789, 0):
            self.assertEqual(subject.timestamp_ns(rfc3339(value)), value)
        for value in ('2026-09-08T01:02:03', '2026-09-08T01:02:03+02:00', '2026-13-08T01:02:03Z',
                      '2026-09-08T01:02:03.1234567890Z', '', 'now'):
            with self.subTest(value=value), self.assertRaises(Exception):
                subject.timestamp_ns(value)

    def test_go_durations_convert_and_unknown_units_are_refused(self):
        self.assertEqual(subject.duration_ns('0s'), 0)
        self.assertEqual(subject.duration_ns('1.000000001s'), SECOND + 1)
        self.assertEqual(subject.duration_ns('2ms'), 2 * 10 ** 6)
        self.assertEqual(subject.duration_ns('1.5µs'), 1500)
        self.assertEqual(subject.duration_ns('1.5us'), 1500)
        for value in ('1m0s', '2h', '5', 'ms', '-1s', '1.5', '999999999999999s', ''):
            with self.subTest(value=value), self.assertRaises(Exception):
                subject.duration_ns(value)


class RegistryLogTests(unittest.TestCase):
    def test_only_this_instance_writes_admissible_records(self):
        raw = pull_log(concurrent_schedule())
        rows = subject.registry_records(raw, instance_id=INSTANCE)
        self.assertEqual(len(rows), 2 * subject.PARALLEL_PULLS)
        foreign = log_line(uri='/v2/', started_ns=SECOND, duration=1000,
                           instance='99999999-2222-3333-4444-555555555555').encode('ascii')
        unknown = json.dumps({'time': rfc3339(SECOND), 'level': 'info', 'msg': 'x',
                              'go.version': subject.registry.GO_VERSION, 'instance.id': INSTANCE,
                              'version': subject.REGISTRY_LOG_VERSION, 'surprise': 1}, sort_keys=True).encode() + b'\n'
        wrong_version = log_line(uri='/v2/', started_ns=SECOND, duration=1000).replace(
            '"' + subject.REGISTRY_LOG_VERSION + '"', '"9.9.9"').encode('ascii')
        for candidate, reason in ((foreign, 'another instance'), (unknown, 'unknown field'),
                                  (wrong_version, 'another registry version'),
                                  (b'plain text line\n', 'not a Distribution record'),
                                  (raw[:-1], 'unterminated')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.registry_records(candidate, instance_id=INSTANCE)

    def test_startup_needs_one_instance_and_one_announced_listener(self):
        proof = subject.registry_startup(startup_log())
        self.assertEqual(proof['instance_id'], INSTANCE)
        doubled = startup_log() + startup_log()
        other = startup_log() + startup_log('99999999-2222-3333-4444-555555555555')
        silent = json.dumps({'time': rfc3339(SECOND), 'level': 'info', 'msg': 'starting',
                             'go.version': subject.registry.GO_VERSION, 'instance.id': INSTANCE,
                             'version': subject.REGISTRY_LOG_VERSION}, sort_keys=True).encode() + b'\n'
        for candidate, reason in ((doubled, 'two listeners'), (other, 'two instances'),
                                  (silent, 'no listener'), (b'', 'empty')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.registry_startup(candidate)


class PullConcurrencyTests(unittest.TestCase):
    """The registry's own service windows decide this, not client lifetime."""

    def windows(self, schedule):
        rows = subject.registry_records(pull_log(schedule), instance_id=INSTANCE)
        service = subject.served_requests(rows, repositories())
        return subject.pull_windows(service['served'], repositories())

    def test_four_pulls_served_inside_one_window_are_concurrent(self):
        proof = self.windows(concurrent_schedule())
        self.assertEqual(proof['served_requests'], 2 * subject.PARALLEL_PULLS)
        self.assertEqual(proof['overlap']['participants'], subject.PARALLEL_PULLS)
        self.assertGreater(proof['overlap']['overlap_ns'], 0)

    def test_the_same_work_served_one_repository_at_a_time_is_refused(self):
        with self.assertRaises(Exception):
            self.windows(serial_schedule())

    def test_three_concurrent_and_one_late_repository_is_refused(self):
        schedule = concurrent_schedule()
        late = repositories()[3]
        schedule[late] = [(SECOND + 10 * SECOND, 10 ** 6), (SECOND + 11 * SECOND, 10 ** 6)]
        with self.assertRaises(Exception):
            self.windows(schedule)

    def test_a_repository_the_registry_never_served_is_refused(self):
        schedule = concurrent_schedule()
        del schedule[repositories()[2]]
        with self.assertRaises(Exception):
            self.windows(schedule)

    def test_a_repository_served_only_blobs_never_proves_its_manifest(self):
        rows = subject.registry_records(pull_log(concurrent_schedule()), instance_id=INSTANCE)
        served = subject.served_requests(rows, repositories())['served']
        stripped = [row for row in served
                    if not (row['repository'] == repositories()[1] and row['kind'] == 'manifests')]
        with self.assertRaises(Exception):
            subject.pull_windows(stripped, repositories())

    def test_a_zero_duration_window_cannot_witness_an_overlap(self):
        schedule = {repository: [(SECOND, 0), (SECOND, 0)] for repository in repositories()}
        with self.assertRaises(Exception):
            self.windows(schedule)

    def test_pings_are_counted_and_unowned_or_written_repositories_are_refused(self):
        rows = subject.registry_records(
            (log_line(uri='/v2/', started_ns=SECOND, duration=1000)).encode('ascii') +
            pull_log(concurrent_schedule()), instance_id=INSTANCE)
        service = subject.served_requests(rows, repositories())
        self.assertEqual(service['pings'], 1)
        cases = {
            'unowned repository': log_line(uri='/v2/someone-else/other/manifests/v1',
                                           started_ns=SECOND, duration=1000),
            'a write, not a pull': log_line(uri='/v2/' + repositories()[0] + '/blobs/' + digest(1),
                                            started_ns=SECOND, duration=1000, method='PUT'),
            'not found': log_line(uri='/v2/' + repositories()[0] + '/manifests/v1',
                                  started_ns=SECOND, duration=1000, status=404),
            'error record': log_line(uri='/v2/' + repositories()[0] + '/manifests/v1',
                                     started_ns=SECOND, duration=1000, level='error',
                                     extra={'err.code': 'manifest unknown'}),
            'unknown route': log_line(uri='/v2/_catalog', started_ns=SECOND, duration=1000),
        }
        for reason, line in cases.items():
            rows = subject.registry_records(line.encode('ascii'), instance_id=INSTANCE)
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.served_requests(rows, repositories())

    def test_a_record_without_its_own_handling_duration_is_refused(self):
        line = json.loads(log_line(uri='/v2/' + repositories()[0] + '/manifests/v1',
                                   started_ns=SECOND, duration=1000))
        del line['http.response.duration']
        rows = subject.registry_records((json.dumps(line, sort_keys=True) + '\n').encode('ascii'),
                                        instance_id=INSTANCE)
        with self.assertRaises(Exception):
            subject.served_requests(rows, repositories())

    def test_a_record_whose_vars_name_contradicts_its_own_uri_is_refused(self):
        line = log_line(uri='/v2/' + repositories()[0] + '/manifests/v1', started_ns=SECOND, duration=1000,
                        extra={'vars.name': repositories()[1]})
        rows = subject.registry_records(line.encode('ascii'), instance_id=INSTANCE)
        with self.assertRaises(Exception):
            subject.served_requests(rows, repositories())


class PullResultTests(unittest.TestCase):
    def rows(self, **changes):
        rows = []
        for slot in range(subject.PARALLEL_PULLS):
            repository = TOKEN + '/slot' + str(slot)
            rows.append({'slot': slot, 'repository': repository,
                         'reference': '127.0.0.1:5000/' + repository + ':' + subject.PULL_TAG,
                         'pushed_image_id': image_id(slot), 'pulled_image_id': image_id(slot),
                         'repo_digest': digest(slot), 'pulled_repo_digest': digest(slot)})
        for slot, change in changes.items():
            rows[slot].update(change)
        return rows

    def test_each_pull_must_return_exactly_the_image_its_own_slot_pushed(self):
        proof = subject.verify_pull_results(self.rows())
        self.assertEqual(proof['parallel_pulls'], subject.PARALLEL_PULLS)
        self.assertEqual(len(proof['slots']), subject.PARALLEL_PULLS)

    def test_a_pull_that_returned_another_slots_work_is_refused(self):
        for changes, reason in (
                ({1: {'pulled_image_id': image_id(2)}}, 'another slot image'),
                ({1: {'pulled_repo_digest': digest(2)}}, 'another slot manifest'),
                ({1: {'pushed_image_id': image_id(0), 'pulled_image_id': image_id(0)}}, 'shared image'),
                ({1: {'repo_digest': digest(0), 'pulled_repo_digest': digest(0)}}, 'shared manifest'),
                ({2: {'reference': '127.0.0.1:5000/' + TOKEN + '/slot3:v1'}}, 'reference names another repository'),
                ({0: {'pulled_image_id': 'sha256:not-a-digest'}}, 'malformed image id')):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.verify_pull_results(self.rows(**changes))

    def test_a_short_or_reordered_pull_set_is_refused(self):
        rows = self.rows()
        with self.assertRaises(Exception):
            subject.verify_pull_results(rows[:3])
        shuffled = [rows[1], rows[0], rows[2], rows[3]]
        with self.assertRaises(Exception):
            subject.verify_pull_results(shuffled)


class LayerTests(unittest.TestCase):
    def test_every_slot_gets_its_own_layer_and_the_same_marker_repeats(self):
        markers = [TOKEN + '-l' + str(slot) for slot in range(subject.PARALLEL_PULLS)]
        archives = [subject.layer_tar(marker, 4096) for marker in markers]
        self.assertEqual(len(set(archives)), subject.PARALLEL_PULLS)
        self.assertEqual(subject.layer_tar(markers[0], 4096), archives[0])
        self.assertEqual(len(subject.payload_bytes(markers[0], 4096)), 4096)
        self.assertNotEqual(subject.payload_bytes(markers[0], 64), subject.payload_bytes(markers[1], 64))

    def test_the_payload_is_bounded(self):
        for size in (0, -1, 64 * 1024 * 1024):
            with self.subTest(size=size), self.assertRaises(Exception):
                subject.payload_bytes('x', size)


class CorrelationTests(unittest.TestCase):
    """A bijection between what was asked for and what came back."""

    def model(self):
        markers = {container_id(index): TOKEN + '-c' + str(index).zfill(2)
                   for index in range(subject.READY_CONTAINERS)}
        return {
            'token': TOKEN,
            'container_markers': dict(markers),
            'ready_containers': {'containers': sorted(markers)},
            'parallel_execs': {'slots': [
                {'slot': slot, 'container_id': container_id(slot),
                 'report': {'marker': markers[container_id(slot)], 'slot': slot,
                            'arrived': subject.PARALLEL_EXECS, 'waited_seconds': 1}}
                for slot in range(subject.PARALLEL_EXECS)]},
            'parallel_builds': {'slots': [
                {'slot': slot, 'payload_sha256': format(slot, 'x').rjust(64, 'e'),
                 'run_digest': format(slot, 'x').rjust(64, 'f')}
                for slot in range(subject.PARALLEL_BUILDS)]},
            'parallel_pulls': {'slots': [
                {'slot': slot, 'repository': TOKEN + '/slot' + str(slot),
                 'image_id': image_id(slot), 'repo_digest': digest(slot)}
                for slot in range(subject.PARALLEL_PULLS)]},
        }

    def test_a_fully_owner_correlated_run_is_accepted(self):
        proof = subject.correlate(self.model())
        self.assertTrue(proof['all_results_exact_owner_correlated'])
        self.assertEqual(proof['correlated_results'],
                         subject.READY_CONTAINERS + subject.PARALLEL_EXECS +
                         subject.PARALLEL_BUILDS + subject.PARALLEL_PULLS)
        self.assertEqual(proof['owner_token'], TOKEN)

    def mutate(self, change):
        model = self.model()
        change(model)
        return model

    def test_cross_talk_between_concurrent_clients_is_refused(self):
        def swapped_markers(model):
            first, second = model['parallel_execs']['slots'][0], model['parallel_execs']['slots'][1]
            first['report']['marker'], second['report']['marker'] = (
                second['report']['marker'], first['report']['marker'])

        def same_container(model):
            slots = model['parallel_execs']['slots']
            slots[1]['container_id'] = slots[0]['container_id']
            slots[1]['report']['marker'] = slots[0]['report']['marker']

        def foreign_container(model):
            model['parallel_execs']['slots'][0]['container_id'] = container_id(90)

        def duplicated_slot(model):
            model['parallel_execs']['slots'][1]['slot'] = 0

        def slot_disagrees(model):
            model['parallel_execs']['slots'][1]['report']['slot'] = 5

        def shared_build_payload(model):
            slots = model['parallel_builds']['slots']
            slots[1]['payload_sha256'] = slots[0]['payload_sha256']

        def reused_run_identity(model):
            slots = model['parallel_builds']['slots']
            slots[2]['run_digest'] = slots[0]['run_digest']

        def shared_pulled_image(model):
            slots = model['parallel_pulls']['slots']
            slots[3]['image_id'] = slots[0]['image_id']

        def unowned_repository(model):
            model['parallel_pulls']['slots'][0]['repository'] = 'someone-else/slot0'

        def shared_marker(model):
            model['container_markers'][container_id(1)] = model['container_markers'][container_id(0)]

        def unowned_marker(model):
            identity = container_id(0)
            model['container_markers'][identity] = 'vzconc-' + 'f' * 24 + '-c00'
            model['parallel_execs']['slots'][0]['report']['marker'] = model['container_markers'][identity]

        def marker_set_drift(model):
            model['ready_containers']['containers'] = sorted(model['container_markers'])[:-1]

        def short_exec_set(model):
            model['parallel_execs']['slots'] = model['parallel_execs']['slots'][:-1]

        def foreign_token(model):
            model['token'] = 'not-a-token'

        for reason, change in (
                ('two execs answered each other', swapped_markers),
                ('two execs entered one container', same_container),
                ('an exec entered an unowned container', foreign_container),
                ('a duplicated exec slot', duplicated_slot),
                ('an exec reported another slot', slot_disagrees),
                ('two builds exported one payload', shared_build_payload),
                ('two builds reused one RUN identity', reused_run_identity),
                ('two pulls returned one image', shared_pulled_image),
                ('an unowned pull repository', unowned_repository),
                ('two containers share a marker', shared_marker),
                ('a marker owned by another run', unowned_marker),
                ('markers that are not the ready set', marker_set_drift),
                ('seven execs, not eight', short_exec_set),
                ('a token this suite never issued', foreign_token)):
            with self.subTest(reason=reason), self.assertRaises(Exception):
                subject.correlate(self.mutate(change))

    def test_the_accepted_model_is_not_accidentally_permissive(self):
        """A run whose every leg is right except one number must still fail."""
        for key, size in (('parallel_execs', subject.PARALLEL_EXECS),
                          ('parallel_builds', subject.PARALLEL_BUILDS),
                          ('parallel_pulls', subject.PARALLEL_PULLS)):
            model = self.model()
            extra = copy.deepcopy(model[key]['slots'][-1])
            extra['slot'] = size
            model[key]['slots'].append(extra)
            with self.subTest(key=key), self.assertRaises(Exception):
                subject.correlate(model)


class SourceTests(unittest.TestCase):
    def test_source_pins_cover_the_module_the_fixture_and_the_reused_suites(self):
        paths = subject.required_source_paths()
        self.assertEqual(len(paths), len(set(paths)))
        for name in ('linux_docker_concurrency_machine.py', 'linux_docker_build_parallel.py',
                     'linux_docker_registry_machine.py', 'probe.sh', 'fixture.json'):
            self.assertTrue(any(path.endswith(name) for path in paths), name)
        pins = {path: subject.sha256(subject.driver.regular(Path(path), subject.LIMIT)) for path in paths}
        subject.verify_sources(pins)
        for path in paths:
            drifted = dict(pins, **{path: 'f' * 64})
            with self.subTest(path=path), self.assertRaises(Exception):
                subject.verify_sources(drifted)
        with self.assertRaises(Exception):
            subject.verify_sources({path: pins[path] for path in paths[:-1]})


class WiringTests(unittest.TestCase):
    def test_the_suite_is_wired_into_the_lane_and_claims_its_scenario(self):
        import linux_docker_e2e as gate
        import linux_docker_scenarios as scenarios
        self.assertIn('concurrency', gate.SUITES)
        self.assertIn('concurrency', gate.SUITE_ORDER)
        self.assertEqual(set(gate.SUITES), set(gate.SUITE_ORDER))
        claimed = [claim.id for claim in scenarios.claims('concurrency')]
        self.assertEqual(claimed, ['docker.operation.concurrent_clients'])
        uncovered = {identifier for identifier, _ in scenarios.UNCOVERED}
        self.assertFalse(uncovered & set(claimed))
        self.assertNotIn('concurrency', scenarios.GAP_SUITES)
        self.assertEqual(scenarios.machine_prefix('concurrency'), 'concurrency-machine-')

    def test_the_suite_requires_the_registry_and_buildkit_inputs_it_actually_uses(self):
        import linux_docker_e2e as gate
        common = []
        for name in gate.startup.OPTIONS:
            common.extend(['--' + name, '/absolute/input'])
        base = common + ['--suite', 'concurrency']
        with self.assertRaisesRegex(Exception, '--registry-archive is required'):
            gate.arguments(base + ['--buildkit-archive', '/absolute/buildkit'])
        with self.assertRaisesRegex(Exception, '--buildkit-archive is required'):
            gate.arguments(base + ['--registry-archive', '/a', '--registry-layout', '/b'])
        args = gate.arguments(base + ['--registry-archive', '/a', '--registry-layout', '/b',
                                      '--buildkit-archive', '/c'])
        self.assertEqual((args.suite, args.registry_archive, args.buildkit_archive),
                         ('concurrency', '/a', '/c'))
        # A suite that never pulls must still refuse those inputs.
        with self.assertRaisesRegex(Exception, '--registry-archive is required'):
            gate.arguments(common + ['--suite', 'mounts', '--registry-archive', '/a', '--registry-layout', '/b'])


if __name__ == '__main__':
    unittest.main()
