"""Offline limits/OOM-adapter tests; no Docker, vz, VM or network dispatch."""
import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_limits_machine as subject

ENGINE = 'engine-1'
IMAGE = 'sha256:' + 'a' * 64
HEALTH_IMAGE = 'sha256:' + 'c' * 64
IDS = {'limited': '1' * 64, 'control': '2' * 64, 'oom': '3' * 64}
LIMITED_CGROUP = b'1073741824\n100000 100000\n64\n'
CONTROL_CGROUP = b'max\nmax 100000\nmax\n'
PROGRESS = b''.join(b'VZ_ALLOC step=%d bytes=%d\n' % (step, 1048576 * 2 ** step) for step in range(10))


def inspect_item(role, token, *, running=True, oom=False, exit_code=0, memory=0, pid=0):
    return {'Id': IDS[role], 'Name': '/' + token + '-' + role, 'Image': IMAGE, 'RestartCount': 0,
            'Config': {'Labels': {subject.LABEL: token}},
            'HostConfig': {'Runtime': 'youki', 'NetworkMode': 'none', 'CgroupnsMode': 'private', 'Memory': memory,
                           'NanoCpus': 1_000_000_000 if role == 'limited' else 0,
                           'PidsLimit': 64 if role == 'limited' else None},
            'State': {'Running': running, 'Status': 'running' if running else 'exited', 'OOMKilled': oom,
                      'ExitCode': exit_code, 'Pid': pid, 'StartedAt': '2026-01-01T00:00:0' + str(len(role)) + 'Z'}}


def events_stream(actions=('create', 'start', 'oom', 'die'), exit_code='137', container=None):
    container = container or IDS['oom']
    rows = []
    for action in actions:
        row = {'status': action, 'id': container, 'Type': 'container', 'Action': action,
               'Actor': {'ID': container, 'Attributes': {'name': 'x'}}, 'scope': 'local', 'time': 1, 'timeNano': 1}
        if action == 'die':
            row['Actor']['Attributes']['exitCode'] = exit_code
        rows.append(row)
    return b''.join(json.dumps(row).encode() + b'\n' for row in rows)


class Helpers(unittest.TestCase):
    def test_fixture_contract_pins_checked_in_bytes(self):
        fixture = subject.fixture_contract()
        self.assertEqual(fixture['script_sha256'], subject.ALLOCATOR_SHA256)
        self.assertIn('VZ_ALLOC step=', fixture['script'])
        self.assertIn('exit 61', fixture['script'])
        self.assertEqual(fixture['contract']['oom']['container_exit_code'], 137)
        self.assertEqual(fixture['contract']['resource_limits']['limited'], subject.LIMITED)
        with tempfile.TemporaryDirectory() as temporary:
            copied = Path(temporary) / 'docker-limits'
            shutil.copytree(subject.FIXTURE, copied)
            for name, change in (('allocate.sh', b'x'), ('fixture.json', b'x')):
                with (copied / name).open('ab') as handle:
                    handle.write(change)
                with self.subTest(name=name), self.assertRaises(ValueError):
                    subject.fixture_contract(copied)
                shutil.copyfile(subject.FIXTURE / name, copied / name)
            contract = json.loads((copied / 'fixture.json').read_bytes())
            contract['resource_limits']['limited']['pids.max'] = '65'
            (copied / 'fixture.json').write_bytes(json.dumps(contract).encode())
            with self.assertRaisesRegex(ValueError, 'resource limit contract'):
                subject.fixture_contract(copied)

    def test_cgroup_wait_progress_event_and_timestamp_parsers(self):
        self.assertEqual(subject.parse_cgroup(LIMITED_CGROUP, b'', subject.LIMITED), subject.LIMITED)
        self.assertEqual(subject.parse_cgroup(CONTROL_CGROUP, b'', subject.CONTROL), subject.CONTROL)
        for raw, stderr in ((CONTROL_CGROUP, b''), (LIMITED_CGROUP[:-1], b''), (LIMITED_CGROUP, b'x'),
                            (b'1073741824\nmax\n100000 100000\n64\n', b''), (b'1073741824\n100000 100000\nmax\n', b'')):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_cgroup(raw, stderr, subject.LIMITED)
        self.assertEqual(subject.parse_wait(b'137\n', b''), 137)
        for raw, stderr in ((b'137', b''), (b'', b''), (b'137\n', b'e'), (b'abc\n', b'')):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_wait(raw, stderr)
        self.assertEqual(subject.parse_progress(PROGRESS), {'reported_steps': 9, 'last_reported_bytes': 1048576 * 2 ** 9})
        survivor = b''.join(b'VZ_ALLOC step=%d bytes=%d\n' % (step, 1048576 * 2 ** step) for step in range(12))
        for raw in (b'', b'VZ_ALLOC step=1 bytes=2097152\n', PROGRESS[:-1], PROGRESS + b'garbage\n', survivor,
                    b'VZ_ALLOC step=0 bytes=1048577\n'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_progress(raw)
        proof = subject.parse_events(events_stream(), b'', IDS['oom'])
        self.assertEqual(proof['actions'], ['create', 'start', 'oom', 'die'])
        self.assertEqual(proof['die_exit_code'], '137')
        for raw, stderr in ((events_stream(('create', 'start', 'die')), b''), (events_stream(exit_code='1'), b''),
                            (events_stream(('create', 'start', 'die', 'oom')), b''), (events_stream(container='f' * 64), b''),
                            (events_stream(), b'warn'), (events_stream()[:-1], b''), (events_stream(('create', 'start', 'oom', 'die', 'die')), b'')):
            with self.subTest(raw=raw[:40]), self.assertRaises(ValueError):
                subject.parse_events(raw, stderr, IDS['oom'])
        self.assertEqual(subject.timestamp(1_700_000_000_000_000_042), '1700000000.000000042')
        with self.assertRaises(ValueError):
            subject.timestamp(0)


class Machine(unittest.TestCase):
    @contextmanager
    def case(self, *, outputs=None, wait=b'137\n', oom_state=None, health_fail=None, during_change=None):
        with tempfile.TemporaryDirectory(prefix='vz-limits-unit-') as temporary:
            root = Path(temporary).resolve()
            evidence = root / 'evidence'
            evidence.mkdir(mode=0o700)
            events = []
            scope = {'project_id': 'project-1', 'environment_id': 'environment-1', 'machine_id': 'machine-1',
                     'machine_incarnation': 'incarnation-1', 'runtime_identity': 'runtime-1',
                     'docker_context': 'owned-machine', 'docker_endpoint': 'unix:///private/owned/docker.sock',
                     'engine_id': ENGINE}
            descriptor = {'owner': {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
                          'name': scope['docker_context'], 'endpoint': scope['docker_endpoint'], 'config_dir': str(root / 'cfg'),
                          'engine_id': scope['engine_id'], 'incarnation_id': scope['machine_incarnation'],
                          'incarnation_generation': 1}
            proof = {'receipt_path': '/original/runtime-proof.json', 'receipt_sha256': 'a' * 64}
            images = {'base': {'id': 'sha256:' + 'b' * 64}, 'compose': {'id': HEALTH_IMAGE}}
            sentinel = {'descriptor': copy.deepcopy(descriptor), 'token': 'vzlive-sentinel', 'tag': 'vzlive-sentinel:sentinel',
                        'kind': 'sentinel', 'container_id': 'f' * 64, 'image_id': IMAGE}
            holder = SimpleNamespace(token=None, health=None, removed=[])
            harness = SimpleNamespace(evidence=evidence, descriptors=[copy.deepcopy(descriptor)], effects_uncertain=False,
                env={'LC_ALL': 'C'}, root=root, owned=[sentinel],
                info={'clients': {'docker': {'canonical': '/owned/docker'}}, 'inputs': {name: subject.driver.sha256(
                    subject.driver.regular(Path(name), subject.LIMIT)) for name in subject.required_source_paths()}},
                monitor=SimpleNamespace(check=Mock(side_effect=lambda: events.append('monitor'))))
            outputs = dict(outputs or {})
            baseline = (IMAGE + '\n' + HEALTH_IMAGE + '\n').encode()
            def state(role, label):
                if role == 'oom':
                    item = inspect_item('oom', holder.token, running=False, oom=True, exit_code=137, memory=subject.MEMORY_BYTES)
                    if oom_state:
                        item['State'].update(oom_state)
                    return item
                pid = 100 if role == 'limited' else 200
                if during_change and label.startswith('limits-during'):
                    pid += 1
                return inspect_item(role, holder.token, memory=subject.MEMORY_BYTES if role == 'limited' else 0, pid=pid)
            def docker(label, given, args, **kwargs):
                self.assertEqual(given, descriptor)
                events.append(label)
                self.assertNotIn(label, ('limits-remove-limited', 'limits-remove-control', 'limits-remove-oom'))
                if label in outputs:
                    return outputs[label]
                if args[:2] == ['image', 'ls']:
                    return baseline, b'', 0
                if args[0] == 'wait':
                    self.assertEqual(args[1], IDS['oom'])
                    self.assertEqual(kwargs.get('timeout'), 60)
                    return wait, b'', 0
                if args[:2] == ['container', 'inspect']:
                    role = next(r for r, i in IDS.items() if i == args[2])
                    return json.dumps([state(role, label)]).encode(), b'', 0
                if args[0] == 'exec':
                    self.assertEqual(args[2:], ['/bin/busybox', 'cat', *subject.CGROUP_FILES])
                    return (LIMITED_CGROUP if args[1] == IDS['limited'] else CONTROL_CGROUP), b'', 0
                if args[0] == 'logs':
                    return PROGRESS, b'', 0
                if args[0] == 'events':
                    self.assertEqual(args[1:3], ['--filter', 'container=' + IDS['oom']])
                    self.assertRegex(args[args.index('--since') + 1], r'[0-9]+\.[0-9]{9}')
                    self.assertRegex(args[args.index('--until') + 1], r'[0-9]+\.[0-9]{9}')
                    return events_stream(), b'', 0
                raise AssertionError('unexpected read command ' + repr((label, args)))
            def mutate(label, given, args, **kwargs):
                self.assertEqual(given, descriptor)
                self.assertFalse(harness.effects_uncertain)
                events.append(label)
                if label.startswith('limits-run-'):
                    role = label[len('limits-run-'):]
                    self.assertEqual(args[:4], ['run', '--detach', '--network', 'none'])
                    self.assertEqual(args[args.index('--name') + 1], holder.token + '-' + role)
                    self.assertEqual(args[args.index('--label') + 1], subject.LABEL + '=' + holder.token)
                    index = args.index(IMAGE)
                    if role == 'limited':
                        self.assertEqual(args[8:index], ['--memory', '1073741824', '--cpus', '1', '--pids-limit', '64'])
                        self.assertEqual(args[index + 1:], ['/bin/busybox', 'sleep', '600'])
                    elif role == 'control':
                        self.assertEqual(args[8:index], [])
                    else:
                        self.assertEqual(args[8:index], ['--memory', '1073741824'])
                        self.assertEqual(args[index + 1:4 + index], ['/bin/busybox', 'sh', '-c'])
                        self.assertEqual(subject.sha256(args[index + 4].encode()), subject.ALLOCATOR_SHA256)
                        self.assertIsNotNone(holder.health.started_at)
                    return (IDS[role] + '\n').encode(), (subject.SWAP_WARNING if '--memory' in args else b''), 0
                if label.startswith('limits-remove-'):
                    role = label[len('limits-remove-'):]
                    self.assertEqual(args, ['container', 'rm', *(['--force'] if role != 'oom' else []), IDS[role]])
                    self.assertTrue(holder.health.finished)
                    holder.removed.append(role)
                    return b'', b'', 0
                raise AssertionError('unexpected mutation ' + repr((label, args)))
            def exact_absent(given, kind, name):
                self.assertEqual((given, kind), (descriptor, 'container'))
                events.append('absent:' + name.rsplit('-', 1)[1])
            harness.docker, harness.mutate, harness.exact_absent = Mock(side_effect=docker), Mock(side_effect=mutate), Mock(side_effect=exact_absent)
            self_case = self
            class Health:
                def __init__(self, given_harness, given_descriptor, given_images, index):
                    self_case.assertIs(given_harness, harness)
                    self_case.assertEqual(given_descriptor, descriptor)
                    self_case.assertEqual(given_images, images)
                    self_case.assertEqual(index, 0)
                    self.prepared, self.started_at, self.finished, self.container_id = False, None, False, 'e' * 64
                    holder.health = self
                    events.append('health-construct')
                def prepare(self):
                    events.append('health-prepare')
                    self_case.assertNotIn('limits-run-limited', events)
                    self.prepared = True
                def start(self):
                    events.append('health-start')
                    self_case.assertIn('limits-run-control', events)
                    self_case.assertNotIn('limits-run-oom', events)
                    self.started_at = subject.time.time_ns()
                def finish(self, intervals):
                    events.append('health-finish')
                    self.finished = True
                    self_case.assertEqual(len(intervals), 4)
                    self_case.assertTrue(all(self.started_at < b < e for b, e in intervals))
                    if health_fail:
                        raise ValueError(health_fail)
                    return {'samples': 60, 'sample_errors': 0, 'missed_deadlines': 0, 'timing': dict(subject.parallel_health.TIMING),
                            'guest_run_envelopes': intervals, 'container_id': self.container_id}
            generated = 'vzlimits-' + 'd' * 24
            holder.token = generated
            with ExitStack() as stack:
                stack.enter_context(patch.object(subject.parallel_health, 'Health', Health))
                stack.enter_context(patch.object(subject.uuid, 'uuid4', Mock(return_value=SimpleNamespace(hex='d' * 32))))
                yield SimpleNamespace(harness=harness, scope=scope, proof=proof, images=images, descriptor=descriptor,
                    events=events, holder=holder, evidence=evidence, token=generated,
                    invoke=lambda: subject.run_machine(harness, descriptor, scope, proof, images, 0))

    def test_limits_oom_health_then_exact_cleanup(self):
        with self.case() as case:
            result = case.invoke()
            self.assertEqual(result['scope'], subject.SCOPE)
            workload = result['workload']
            self.assertEqual(workload['oom_exit_code'], 137)
            self.assertTrue(workload['oom_killed'])
            self.assertEqual(workload['before']['limited']['cgroup'], subject.LIMITED)
            self.assertEqual(workload['before']['control']['cgroup'], subject.CONTROL)
            self.assertEqual(workload['before'], workload['during'])
            self.assertEqual(workload['before'], workload['after'])
            self.assertEqual(workload['events']['actions'], ['create', 'start', 'oom', 'die'])
            self.assertEqual(workload['sibling_health']['samples'], 60)
            self.assertEqual(workload['allocator_progress']['reported_steps'], 9)
            self.assertEqual(case.holder.removed, ['oom', 'limited', 'control'])
            self.assertEqual(result['cleanup']['containers_removed'],
                             sorted(case.token + '-' + role for role in ('control', 'limited', 'oom')))
            self.assertFalse(result['release_certified'])
            self.assertFalse(result['docker_parity_certified'])
            session = case.harness.limits_sessions[0]
            self.assertTrue(session.cleanup_complete)
            self.assertFalse(session.failed)
            ordered = [e for e in case.events if e in ('health-prepare', 'limits-run-limited', 'health-start', 'limits-run-oom',
                                                       'limits-oom-wait', 'limits-oom-events', 'health-finish',
                                                       'limits-remove-oom', 'limits-image-final')]
            self.assertEqual(ordered, ['health-prepare', 'limits-run-limited', 'health-start', 'limits-run-oom',
                                       'limits-oom-wait', 'limits-oom-events', 'health-finish', 'limits-remove-oom',
                                       'limits-image-final'])
            self.assertEqual(case.events.count('absent:limited'), 2)
            self.assertEqual(case.events.count('absent:oom'), 2)
            retained = subject.parse((case.evidence / 'limits-machine-0/machine-limits-validation.json').read_bytes())
            self.assertEqual(retained, result)
            self.assertTrue((case.evidence / 'limits-machine-0/workload.json').exists())

    def test_wrong_outputs_fail_before_any_removal_and_keep_session_registered(self):
        failures = {
            'survivor-exit': dict(wait=b'61\n'),
            'nonzero-other': dict(wait=b'1\n'),
            'not-oom-killed': dict(oom_state={'OOMKilled': False}),
            'still-running': dict(oom_state={'Running': True, 'Status': 'running'}),
            'exit-code': dict(oom_state={'ExitCode': 1}),
            'limited-unlimited': dict(outputs={'limits-before-limited-cgroup': (CONTROL_CGROUP, b'', 0)}),
            'control-limited': dict(outputs={'limits-before-control-cgroup': (LIMITED_CGROUP, b'', 0)}),
            'events-no-oom': dict(outputs={'limits-oom-events': (events_stream(('create', 'start', 'die')), b'', 0)}),
            'events-die-code': dict(outputs={'limits-oom-events': (events_stream(exit_code='1'), b'', 0)}),
            'allocator-stderr': dict(outputs={'limits-oom-logs': (PROGRESS, b'sh: out of memory\n', 0)}),
            'allocator-survived': dict(outputs={'limits-oom-logs': (
                b''.join(b'VZ_ALLOC step=%d bytes=%d\n' % (s, 1048576 * 2 ** s) for s in range(12)), b'', 0)}),
            'sibling-restarted': dict(during_change=True),
            'health-failed': dict(health_fail='health sample errors'),
            'image-baseline-drift': dict(outputs={'limits-image-final': (IMAGE.encode() + b'\n', b'', 0)}),
            'run-warning': dict(outputs=None)}
        for name, options in failures.items():
            with self.subTest(name=name), self.case(**{k: v for k, v in options.items() if v is not None}) as case:
                if name == 'run-warning':
                    original = case.harness.mutate.side_effect
                    def warned(label, *args, **kwargs):
                        raw, stderr, code = original(label, *args, **kwargs)
                        if label == 'limits-run-limited':
                            stderr = b'WARNING: Your kernel does not support pids limit capabilities\n'
                        return raw, stderr, code
                    case.harness.mutate.side_effect = warned
                with self.assertRaises(ValueError):
                    case.invoke()
                if name == 'image-baseline-drift':
                    self.assertEqual(case.holder.removed, ['oom', 'limited', 'control'])
                else:
                    self.assertEqual(case.holder.removed, [])
                session = case.harness.limits_sessions[0]
                self.assertTrue(session.failed)
                self.assertFalse(session.cleanup_complete)
                self.assertFalse((case.evidence / 'limits-machine-0/machine-limits-validation.json').exists())

    def test_health_finish_always_precedes_removal_and_sibling_snapshot_after(self):
        with self.case() as case:
            case.invoke()
            self.assertLess(case.events.index('health-finish'), case.events.index('limits-after-limited'))
            self.assertLess(case.events.index('limits-after-control-cgroup'), case.events.index('limits-remove-oom'))

    def test_foreign_scope_descriptor_proof_images_index_or_evidence_rejected_before_commands(self):
        for field in ('machine_id', 'machine_incarnation', 'docker_context', 'docker_endpoint', 'engine_id'):
            with self.subTest(field=field), self.case() as case:
                case.scope[field] = 'foreign'
                with self.assertRaises(ValueError):
                    case.invoke()
                self.assertEqual(case.events, [])
        for change in ('descriptors', 'proof', 'images', 'index', 'evidence', 'uncertain', 'sentinel', 'earlier'):
            with self.subTest(change=change), self.case() as case:
                index = 0
                if change == 'descriptors': case.harness.descriptors = []
                elif change == 'proof': case.proof.clear()
                elif change == 'images': case.images['compose']['id'] = 'latest'
                elif change == 'index': index = True
                elif change == 'evidence': (case.evidence / 'limits-machine-0').mkdir()
                elif change == 'uncertain': case.harness.effects_uncertain = True
                elif change == 'sentinel': case.harness.owned = []
                else: case.harness.limits_sessions = [SimpleNamespace(cleanup_complete=False)]
                with self.assertRaises(ValueError):
                    subject.run_machine(case.harness, case.descriptor, case.scope, case.proof, case.images, index)
                self.assertEqual([e for e in case.events if e != 'monitor'], [])
                case.harness.mutate.assert_not_called()

    def test_missing_or_changed_source_pin_prevents_dispatch(self):
        for missing in (False, True):
            with self.subTest(missing=missing), self.case() as case:
                path = subject.required_source_paths()[0]
                if missing:
                    del case.harness.info['inputs'][path]
                else:
                    case.harness.info['inputs'][path] = '0' * 64
                with self.assertRaises((ValueError, KeyError)):
                    case.invoke()
                self.assertEqual(case.events, [])
        pins = subject.required_source_paths()
        self.assertIn(str(subject.FIXTURE / 'allocate.sh'), pins)
        self.assertIn(str(subject.FIXTURE / 'fixture.json'), pins)

    def test_inputs_are_not_mutated_and_result_documents_are_resealed(self):
        with self.case() as case:
            snapshot = copy.deepcopy((case.descriptor, case.scope, case.proof, case.images))
            case.invoke()
            self.assertEqual((case.descriptor, case.scope, case.proof, case.images), snapshot)
        original = subject.startup.document
        def tamper(path, value):
            original(path, dict(value, forged=True) if path.name == 'machine-limits-validation.json' else value)
        with self.case() as case, patch.object(subject.startup, 'document', side_effect=tamper):
            with self.assertRaisesRegex(ValueError, 'retained limits result differs'):
                case.invoke()


if __name__ == '__main__':
    unittest.main()
