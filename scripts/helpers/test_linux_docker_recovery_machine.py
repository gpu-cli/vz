"""Offline persistence/recovery-adapter tests; no Docker, vz, VM or network dispatch."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_recovery_machine as subject

SENTINEL = b'vz-docker-recovery-persistence-sentinel-v1\n'
IMAGE = 'sha256:' + 'a' * 64
BASE = {'reference': 'python@sha256:' + 'b' * 64, 'id': 'sha256:' + 'b' * 64, 'platform': 'linux/arm64'}
MACHINES = [('env-p', 'm0'), ('env-p', 'm1'), ('env-n', 'm2'), ('env-n', 'm3')]
BOOT = {'env-p': ['0f8a3d2e-1111-4111-8111-111111111111', '0f8a3d2e-2222-4222-8222-222222222222'],
        'env-n': ['0f8a3d2e-3333-4333-8333-333333333333', '0f8a3d2e-4444-4444-8444-444444444444']}


def daemon_stream(boot_id, pid=212, argv=None):
    argv = subject.DOCKERD_ARGV if argv is None else argv
    lines = ['VZ_RECOVERY_DAEMON_V1', 'BOOT_ID=' + boot_id, 'PID=' + str(pid)] + ['ARG=' + a for a in argv] + \
            ['COUNT=1', 'VZ_RECOVERY_DAEMON_END', '']
    return '\n'.join(lines).encode()


def tar_bytes(name, data):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as archive:
        info = tarfile.TarInfo(name)
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class Helpers(unittest.TestCase):
    def test_fixture_contract_pins_checked_in_bytes(self):
        fixture = subject.fixture_contract()
        self.assertEqual(fixture['sentinel'].encode(), SENTINEL)
        self.assertEqual(hashlib.sha256(SENTINEL).hexdigest(), subject.SENTINEL_SHA256)
        self.assertEqual(fixture['contract']['readiness_poll'], subject.READINESS)
        self.assertFalse(fixture['contract']['recovery_path']['in_guest_dockerd_restart_supported'])
        with tempfile.TemporaryDirectory() as temporary:
            copied = Path(temporary) / 'docker-recovery'
            shutil.copytree(subject.FIXTURE, copied)
            for name in ('persistence-sentinel.txt', 'fixture.json'):
                with (copied / name).open('ab') as handle:
                    handle.write(b'x')
                with self.subTest(name=name), self.assertRaises(ValueError):
                    subject.fixture_contract(copied)
                shutil.copyfile(subject.FIXTURE / name, copied / name)
            contract = json.loads((copied / 'fixture.json').read_bytes())
            contract['readiness_poll']['deadline_seconds'] = 61
            (copied / 'fixture.json').write_bytes(json.dumps(contract).encode())
            with self.assertRaisesRegex(ValueError, 'readiness poll contract'):
                subject.fixture_contract(copied)

    def test_required_sources_pin_the_guest_supervisor_and_fixture(self):
        pins = subject.required_source_paths()
        self.assertIn(str(subject.SUPERVISION_SOURCE), pins)
        self.assertIn(str(subject.FIXTURE / 'persistence-sentinel.txt'), pins)
        self.assertIn(str(subject.HELPERS / 'linux_docker_e2e.py'), pins)
        self.assertTrue(all(Path(p).is_file() for p in pins))
        source = Path(subject.SUPERVISION_SOURCE).read_text()
        # The truth this module documents must still hold in the pinned source.
        self.assertIn('owned dockerd exited outside shutdown', source)
        self.assertIn('Docker supervisor terminated; Machine recovery is required', source)
        for flag in ('--default-runtime', '--add-runtime', '--data-root', '--exec-root', '--pidfile', '--config-file'):
            self.assertIn('"' + flag + '"', source)

    def test_daemon_ids_sha256sum_diff_and_tar_parsers(self):
        proof = subject.parse_daemon(daemon_stream(BOOT['env-p'][0]), b'')
        self.assertEqual(proof['boot_id'], BOOT['env-p'][0])
        self.assertEqual(proof['pid'], 212)
        self.assertEqual(proof['argv'], subject.DOCKERD_ARGV)
        good = daemon_stream(BOOT['env-p'][0])
        bad = [good[:-1], good.replace(b'COUNT=1', b'COUNT=2'), good.replace(b'COUNT=1', b'COUNT=0'),
               daemon_stream('not-a-uuid'), daemon_stream(BOOT['env-p'][0], argv=subject.DOCKERD_ARGV[:-2]),
               daemon_stream(BOOT['env-p'][0], argv=subject.DOCKERD_ARGV[:-1] + ['runc']),
               good.replace(b'PID=212', b'PID=0'), good + b'PID=3\n', b'', b'\xff' + good]
        for raw in bad:
            with self.subTest(raw=raw[:60]), self.assertRaises(ValueError):
                subject.parse_daemon(raw, b'')
        with self.assertRaises(ValueError):
            subject.parse_daemon(good, b'warn')
        self.assertEqual(subject.parse_ids(b'', b'', r'[0-9a-f]{64}', 'x'), [])
        self.assertEqual(subject.parse_ids(b'b\na\n', b'', r'[a-z]', 'x'), ['a', 'b'])
        for raw, stderr in ((b'a\nb', b''), (b'a\na\n', b''), (b'A\n', b''), (b'a\n', b'e')):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_ids(raw, stderr, r'[a-z]', 'x')
        digest = subject.SENTINEL_SHA256
        self.assertEqual(subject.parse_sha256sum((digest + '  /data/persistence-sentinel\n').encode(), b'', '/data/persistence-sentinel'), digest)
        for raw, stderr in (((digest + '  /other\n').encode(), b''), ((digest + '  /data/persistence-sentinel').encode(), b''),
                            ((digest + '  /data/persistence-sentinel\n').encode(), b'e'), (b'', b'')):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_sha256sum(raw, stderr, '/data/persistence-sentinel')
        self.assertEqual(subject.parse_diff(b'A /vz-recovery-writable-sentinel\n', b'', '/vz-recovery-writable-sentinel'),
                         ['A /vz-recovery-writable-sentinel'])
        for raw, stderr in ((b'C /etc\n', b''), (b'A /vz-recovery-writable-sentinel', b''), (b'', b'')):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.parse_diff(raw, stderr, '/vz-recovery-writable-sentinel')
        self.assertEqual(subject.tar_member(tar_bytes('vz-recovery-writable-sentinel', SENTINEL), 'vz-recovery-writable-sentinel'), SENTINEL)
        with self.assertRaises(ValueError):
            subject.tar_member(tar_bytes('other', SENTINEL), 'vz-recovery-writable-sentinel')


class FakeMonitor:
    def __init__(self, harness, rows, output, log):
        self.rows, self.output, self.log = rows, output, log
        self.alive, self.errors, self.stopped = False, [], False
        self.thread = SimpleNamespace(is_alive=lambda: self.alive)
        self.log.append(('monitor-new', tuple(sorted(r['descriptor']['owner']['machine_id'] for r in rows)), output.name))

    def start(self):
        self.alive = True
        self.log.append(('monitor-start', self.output.name))

    def check(self):
        if not self.alive:
            raise ValueError('monitor not alive')
        self.log.append(('monitor-check', self.output.name))

    def stop(self):
        self.alive, self.stopped = False, True
        self.log.append(('monitor-stop', self.output.name))

    def summary(self):
        return {'samples': [], 'errors': list(self.errors), 'output': self.output.name}


class FakeTopology:
    """Two Environments, four Machines, per-context object stores, Stop/Up semantics."""
    def __init__(self, case, root, evidence, knobs):
        self.case, self.root, self.evidence, self.knobs = case, root, evidence, knobs
        self.events, self.mutations, self.clock = [], [], 10
        self.generation = {'env-p': 1, 'env-n': 1}
        self.lifecycle = {'env-p': 1, 'env-n': 1}
        self.state = {'env-p': 'ready', 'env-n': 'ready'}
        self.stores = {mid: {'containers': {}, 'volumes': {}, 'images': {IMAGE}} for _, mid in MACHINES}
        self.project = root / 'developer'
        self.project.mkdir(mode=0o700)
        self.definition = {'schema_version': 1, 'project_id': 'prj_one', 'name': 'developer'}
        self._write(self.project / 'vz.json', json.dumps(self.definition).encode())
        self.descriptors = []
        self.owned = []
        self.engine_ids = {mid: 'engine-' + mid for _, mid in MACHINES}
        for env, mid in MACHINES:
            descriptor = self.descriptor(mid)
            self.descriptors.append(descriptor)
            cid = mid[-1] * 64
            self.stores[mid]['containers'][cid] = self.container(cid, 'vzlive-' + mid, 'vzlive-' + mid, running=True,
                                                                files={'/sentinel': ('vzlive-' + mid + '\n').encode()}, sentinel=True)
            self.owned.append({'descriptor': copy.deepcopy(descriptor), 'token': 'vzlive-' + mid, 'tag': 'vzlive-' + mid + ':sentinel',
                               'kind': 'sentinel', 'container_id': cid, 'image_id': IMAGE,
                               'started_at': self.stores[mid]['containers'][cid]['State']['StartedAt']})
        self._write(evidence / 'topology.json', json.dumps({'project': str(self.project), 'primary': self.status(None, 'env-p'),
                                                            'neighbor': self.status(None, 'env-n')}).encode())
        self.daemon = {'pid': 4242, 'process': 'vz-runtimed', 'executable_sha256': 'e' * 64}

    @staticmethod
    def _write(path, data):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)

    def tick(self):
        self.clock += 1
        return '2026-01-01T00:%02d:%02d.000000000Z' % (self.clock // 60, self.clock % 60)

    def env_of(self, mid):
        return next(env for env, m in MACHINES if m == mid)

    def descriptor(self, mid):
        env = self.env_of(mid)
        gen = self.generation[env]
        engine = self.engine_ids[mid]
        return {'schema_version': 1, 'owner': {'project_id': 'prj_one', 'environment_id': env, 'machine_id': mid},
                'name': 'ctx-' + mid, 'endpoint': 'unix:///owned/' + mid + '/docker.sock', 'config_dir': '/owned/' + mid + '/cfg',
                'engine_id': engine, 'incarnation_id': 'inc-' + mid + '-g' + str(gen), 'incarnation_generation': gen}

    def status(self, project, selector):
        if project is not None:
            self.case.assertEqual(project, self.project)
        env = selector
        return {'environment_id': env, 'project_id': 'prj_one', 'name': 'primary' if env == 'env-p' else 'neighbor',
                'state': self.state[env], 'definition_digest': 'sha256:' + 'd' * 64, 'lifecycle_generation': self.lifecycle[env],
                'machines': [{'machine_id': mid, 'name': 'machine-' + mid[-1], 'state': self.state[env],
                              'docker_context': self.descriptor(mid), 'incarnation_id': 'inc-' + mid + '-g' + str(self.generation[env]),
                              'incarnation_generation': self.generation[env]} for e, mid in MACHINES if e == env]}

    def inspect(self, environment):
        self.case.assertEqual(environment['state'], 'ready')
        result = []
        for machine in environment['machines']:
            descriptor = machine['docker_context']
            result.append(descriptor)
            if descriptor not in self.descriptors:
                self.descriptors.append(descriptor)
        return result

    def stop(self, project, selector):
        self.case.assertEqual(project, self.project)
        env = selector
        self.case.assertEqual(self.state[env], 'ready')
        self.events.append('stop:' + env)
        self.state[env] = 'stopped'
        self.lifecycle[env] += 1
        for e, mid in MACHINES:
            if e != env:
                continue
            for item in self.stores[mid]['containers'].values():
                if item['State']['Running']:
                    item['State'].update(Running=False, Status='exited', ExitCode=143, FinishedAt=self.tick())
        return self.status(project, env)

    def up(self, project, selector):
        self.case.assertEqual(project, self.project)
        env = selector
        self.case.assertEqual(self.state[env], 'stopped')
        self.events.append('up:' + env)
        self.state[env] = 'ready'
        self.generation[env] += 1
        self.lifecycle[env] += 1
        if self.knobs.get('engine_changed') and env == 'env-p':
            self.engine_ids['m0'] = 'engine-replaced'
        for e, mid in MACHINES:
            if e != env:
                continue
            for item in self.stores[mid]['containers'].values():
                policy_always = item['HostConfig']['RestartPolicy']['Name'] == 'always'
                if policy_always and not self.knobs.get('always_not_restarted'):
                    item['State'].update(Running=True, Status='running', ExitCode=0, StartedAt=self.tick())
                if self.knobs.get('stopped_started_by_up') and item['Name'].endswith('-stopped'):
                    item['State'].update(StartedAt=self.tick())
                if self.knobs.get('sentinel_running_after_up') and item.get('sentinel'):
                    item['State'].update(Running=True, Status='running')
            if self.knobs.get('volume_corrupt'):
                for volume in self.stores[mid]['volumes'].values():
                    volume['files'] = {k: b'corrupt\n' for k in volume['files']}
            if self.knobs.get('writable_corrupt'):
                for item in self.stores[mid]['containers'].values():
                    if item['Name'].endswith('-stopped'):
                        item['files'] = {k: b'corrupt\n' for k in item['files']}
        if self.knobs.get('inventory_drift'):
            other = 'm3' if env == 'env-p' else 'm1'
            self.stores[other]['volumes']['stray'] = {'Name': 'stray', 'Labels': {}, 'Driver': 'local', 'Scope': 'local',
                                                       'Mountpoint': '/x', 'CreatedAt': 'x', 'Options': None, 'files': {}}
        return self.status(project, env)

    def container(self, cid, name, token, *, running, files, always=False, volume=None, sentinel=False):
        created = self.tick()
        item = {'Id': cid, 'Name': '/' + name, 'Image': IMAGE, 'Created': created, 'RestartCount': 0,
                'Config': {'Labels': {subject.LABEL: token}},
                'HostConfig': {'Runtime': 'youki', 'NetworkMode': 'none',
                               'RestartPolicy': {'Name': 'always' if always else 'no', 'MaximumRetryCount': 0}},
                'Mounts': [{'Type': 'volume', 'Name': volume, 'Destination': '/data', 'Source': '/var/lib/docker/volumes/' + volume}] if volume else [],
                'State': {'Running': running, 'Status': 'running' if running else 'exited', 'ExitCode': 0,
                          'StartedAt': created, 'FinishedAt': '0001-01-01T00:00:00Z' if running else self.tick()},
                'files': dict(files), 'sentinel': sentinel}
        return item

    @staticmethod
    def public(item):
        return {k: v for k, v in item.items() if k not in ('files', 'sentinel')}

    def store_for(self, descriptor):
        mid = descriptor['owner']['machine_id']
        self.case.assertEqual(descriptor['name'], 'ctx-' + mid)
        return mid, self.stores[mid]

    def read_file(self, mid, item, path):
        if item['Mounts'] and path.startswith(item['Mounts'][0]['Destination'] + '/'):
            volume = self.stores[mid]['volumes'][item['Mounts'][0]['Name']]
            return volume['files'].get(path)
        return item['files'].get(path)

    def docker(self, label, descriptor, args, **kwargs):
        self.events.append(label)
        override = self.knobs.get('overrides', {}).get(label)
        if override is not None:
            return override
        mid, store = self.store_for(descriptor)
        if self.state[self.env_of(mid)] != 'ready':
            self.case.assertFalse(kwargs.get('success', True), 'read-only command against a stopped Machine must be tolerant')
            return b'', b'Cannot connect\n', 1
        if args[:2] == ['container', 'ls']:
            ids = sorted(store['containers'])
            if '--filter' in args:
                name = args[args.index('--filter') + 1][len('name=^'):-1]
                ids = [cid for cid in ids if store['containers'][cid]['Name'] == name]
            return ''.join(cid + '\n' for cid in ids).encode(), b'', 0
        if args[:2] == ['volume', 'ls']:
            return ''.join(name + '\n' for name in sorted(store['volumes'])).encode(), b'', 0
        if args[:2] == ['image', 'ls']:
            return ''.join(i + '\n' for i in sorted(store['images'])).encode(), b'', 0
        if args[:2] == ['container', 'inspect']:
            item = store['containers'].get(args[2])
            return (json.dumps([self.public(item)]).encode(), b'', 0) if item else (b'[]\n', b'Error: No such container\n', 1)
        if args[:2] == ['volume', 'inspect']:
            volume = store['volumes'][args[2]]
            return json.dumps([{k: v for k, v in volume.items() if k != 'files'}]).encode(), b'', 0
        if args[:2] == ['image', 'inspect']:
            self.case.assertIn(args[2], store['images'])
            return (args[2] + '\n').encode(), b'', 0
        if args[0] == 'info':
            return (self.engine_ids[mid] + '\n').encode(), b'', 0
        if args[0] == 'exec':
            item = store['containers'][args[1]]
            self.case.assertTrue(item['State']['Running'], 'exec into a stopped container')
            if args[2:4] == ['/bin/busybox', 'cat'] or args[2] == '/bin/cat':
                path = args[-1]
                data = self.read_file(mid, item, path)
                return (data, b'', 0) if data is not None else (b'', b'cat: no such file\n', 1)
            if args[2:4] == ['/bin/busybox', 'sha256sum']:
                data = self.read_file(mid, item, args[4])
                return (hashlib.sha256(data).hexdigest() + '  ' + args[4] + '\n').encode(), b'', 0
        if args[:2] == ['container', 'cp']:
            cid, path = args[2].split(':', 1)
            item = store['containers'][cid]
            return tar_bytes(path.lstrip('/'), item['files'][path]), b'', 0
        if args[:2] == ['container', 'diff']:
            item = store['containers'][args[2]]
            return ''.join('A ' + path + '\n' for path in sorted(item['files'])).encode(), b'', 0
        raise AssertionError('unexpected read command ' + repr((label, args)))

    def mutate(self, label, descriptor, args, **kwargs):
        self.case.assertFalse(self.harness.effects_uncertain)
        self.events.append(label)
        self.mutations.append((label, descriptor['owner']['machine_id'], list(args)))
        mid, store = self.store_for(descriptor)
        self.case.assertEqual(self.state[self.env_of(mid)], 'ready', 'mutation against a stopped Machine')
        self.case.assertNotIn('--force', args)
        self.case.assertNotIn('prune', args)
        if args[:2] == ['volume', 'create']:
            name = args[-1]
            store['volumes'][name] = {'Name': name, 'Labels': {args[3].split('=')[0]: args[3].split('=')[1]}, 'Driver': 'local',
                                      'Scope': 'local', 'Mountpoint': '/var/lib/docker/volumes/' + name, 'CreatedAt': self.tick(),
                                      'Options': None, 'files': {}}
            return (name + '\n').encode(), b'', 0
        if args[0] == 'run':
            name = args[args.index('--name') + 1]
            token = args[args.index('--label') + 1].split('=')[1]
            self.case.assertIn('--network', args)
            self.case.assertEqual(args[args.index('--network') + 1], 'none')
            image_index = args.index(IMAGE)
            command = args[image_index + 1:]
            cid = hashlib.sha256(name.encode()).hexdigest()
            if '--detach' in args:
                self.case.assertEqual(args[args.index('--restart') + 1], 'always')
                volume = args[args.index('--volume') + 1].split(':')[0]
                self.case.assertIn(volume, store['volumes'])
                self.case.assertEqual(command, subject.ALWAYS_COMMAND)
                store['containers'][cid] = self.container(cid, name, token, running=True, files={}, always=True, volume=volume)
                return (cid + '\n').encode(), b'', 0
            self.case.assertEqual(command[:3], ['/bin/busybox', 'sh', '-c'])
            self.case.assertEqual(command[4], 'sh')
            store['containers'][cid] = self.container(cid, name, token, running=False,
                                                       files={command[6]: (command[5] + '\n').encode()})
            return b'', b'', 0
        if args[0] == 'exec':
            item = store['containers'][args[1]]
            self.case.assertEqual(args[2:5], ['/bin/busybox', 'sh', '-c'])
            volume = store['volumes'][item['Mounts'][0]['Name']]
            volume['files'][args[8]] = (args[7] + '\n').encode()
            return b'', b'', 0
        if args[:2] == ['container', 'start']:
            item = store['containers'][args[2]]
            self.case.assertFalse(item['State']['Running'])
            item['State'].update(Running=True, Status='running', StartedAt=self.tick())
            return (args[2] + '\n').encode(), b'', 0
        if args[:2] == ['container', 'stop']:
            item = store['containers'][args[2]]
            item['State'].update(Running=False, Status='exited', ExitCode=143, FinishedAt=self.tick())
            return (args[2] + '\n').encode(), b'', 0
        if args[:2] == ['container', 'rm']:
            item = store['containers'].pop(args[2])
            self.case.assertFalse(item['State']['Running'])
            return (args[2] + '\n').encode(), b'', 0
        if args[:2] == ['volume', 'rm']:
            used = [c for c in store['containers'].values() if c['Mounts'] and c['Mounts'][0]['Name'] == args[2]]
            self.case.assertEqual(used, [], 'volume removed while referenced')
            del store['volumes'][args[2]]
            return (args[2] + '\n').encode(), b'', 0
        raise AssertionError('unexpected mutation ' + repr((label, args)))

    def exact_absent(self, descriptor, kind, name):
        self.events.append('absent:' + kind)
        mid, store = self.store_for(descriptor)
        self.case.assertEqual(kind, 'container')
        self.case.assertFalse(any(c['Name'] == '/' + name for c in store['containers'].values()), name)

    def command(self, label, argv, cwd=None, **kwargs):
        self.events.append(label)
        self.case.assertEqual(argv[:3], ['/owned/bin/vz', 'exec', '--environment'])
        env, mid = argv[3], argv[argv.index('--machine') + 1]
        self.case.assertEqual(self.env_of(mid), env)
        self.case.assertEqual(argv[argv.index('--')+1:], ['/bin/busybox', 'sh', '-c', subject.DAEMON_SCRIPT])
        self.case.assertEqual(cwd, self.project)
        self.case.assertFalse(kwargs['success'])
        gen = self.generation[env]
        boot = BOOT[env][0 if (gen == 1 or self.knobs.get('boot_id_same')) else 1]
        return daemon_stream(boot, pid=200 + gen), b'', 0

    def daemon_fingerprint(self):
        self.events.append('daemon-fingerprint')
        if self.knobs.get('daemon_replaced') and any(e.startswith('up:') for e in self.events):
            return dict(self.daemon, pid=9999)
        return dict(self.daemon)


class Machine(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='vz-recovery-unit-')
        self.root = Path(self.temporary.name).resolve()
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir(mode=0o700)
        self.monitor_log = []
        self.sleeps = []
        # Virtual clock: a patched sleep advances time_ns so declared-poll
        # deadlines expire without wall-clock waiting.
        self.offset_ns = 0
        real_time_ns = subject.time.time_ns
        def fake_sleep(seconds):
            self.sleeps.append(seconds)
            self.offset_ns += int(seconds * 10 ** 9)
        clock = SimpleNamespace(time_ns=lambda: real_time_ns() + self.offset_ns, sleep=fake_sleep)
        self.patches = [patch.object(subject, 'phase_monitor', lambda h, rows, out: FakeMonitor(h, rows, out, self.monitor_log)),
                        patch.object(subject, 'time', clock)]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in self.patches:
            item.stop()
        self.temporary.cleanup()

    def harness(self, **knobs):
        topology = FakeTopology(self, self.root, self.evidence, knobs)
        original = FakeMonitor(None, topology.owned, self.evidence / 'sibling-liveness', self.monitor_log)
        original.start()
        harness = SimpleNamespace(evidence=self.evidence, root=self.root, cli='/owned/bin/vz', env={'LC_ALL': 'C'},
                                  descriptors=topology.descriptors, owned=topology.owned, effects_uncertain=False,
                                  monitor=original, daemon_identity=dict(topology.daemon),
                                  info={'python_image': dict(BASE), 'clients': {'docker': {'canonical': '/owned/docker'}},
                                        'inputs': {name: subject.driver.sha256(subject.driver.regular(Path(name), subject.LIMIT))
                                                   for name in subject.required_source_paths()}},
                                  status=Mock(side_effect=topology.status), inspect=Mock(side_effect=topology.inspect),
                                  up=Mock(side_effect=topology.up), stop=Mock(side_effect=topology.stop),
                                  docker=Mock(side_effect=topology.docker), mutate=Mock(side_effect=topology.mutate),
                                  exact_absent=Mock(side_effect=topology.exact_absent), command=Mock(side_effect=topology.command),
                                  daemon_fingerprint=Mock(side_effect=topology.daemon_fingerprint))
        topology.harness = harness
        harness.topology = topology
        return harness

    @staticmethod
    def inputs(topology, mid):
        descriptor = copy.deepcopy(topology.descriptor(mid))
        scope = dict(descriptor['owner'], machine_incarnation=descriptor['incarnation_id'], runtime_identity='rt-' + mid,
                     docker_context=descriptor['name'], docker_endpoint=descriptor['endpoint'], engine_id=descriptor['engine_id'])
        proof = {'receipt_path': '/original/receipt.json', 'receipt_sha256': 'a' * 64}
        images = {'base': dict(BASE), 'compose': dict(BASE)}
        return descriptor, scope, proof, images

    def invoke(self, harness, index, mid=None):
        mid = mid or MACHINES[index][1]
        descriptor, scope, proof, images = self.inputs(harness.topology, mid)
        return subject.run_machine(harness, descriptor, scope, proof, images, index)

    def test_three_indexes_two_environment_cycles_and_exact_cleanup(self):
        harness = self.harness()
        topology = harness.topology
        first = self.invoke(harness, 0)
        self.assertEqual(topology.events.count('stop:env-p'), 1)
        self.assertEqual(topology.events.count('up:env-p'), 1)
        self.assertEqual(topology.events.count('stop:env-n'), 0)
        self.assertEqual(len(harness.recovery_sessions), 2)
        self.assertEqual([s.index for s in harness.recovery_sessions], [0, 1])
        self.assertTrue(harness.recovery_sessions[0].cleanup_complete)
        self.assertFalse(harness.recovery_sessions[1].cleanup_complete)
        self.assertFalse(harness.recovery_sessions[1].failed)
        # machine-1 objects survive index 0; the harness sentinel image remains everywhere.
        self.assertEqual(len(topology.stores['m1']['containers']), 3)
        self.assertEqual(len(topology.stores['m0']['containers']), 1)
        self.assertEqual(len(topology.stores['m0']['volumes']), 0)
        cycle = first['cycle']
        self.assertEqual(cycle['machine_ids'], ['m0', 'm1'])
        self.assertEqual(cycle['other_environment_id'], 'env-n')
        for mid in ('m0', 'm1'):
            self.assertNotEqual(cycle['daemons_before'][mid]['boot_id'], cycle['daemons_after'][mid]['boot_id'])
            self.assertEqual(cycle['daemons_after'][mid]['argv'], subject.DOCKERD_ARGV)
            poll = cycle['readiness_polls'][mid]
            self.assertEqual(len(poll['samples']), 1)
            self.assertTrue(poll['samples'][0]['healthy'])
            self.assertEqual(poll['declared'], subject.READINESS)
        self.assertEqual(self.sleeps, [])
        self.assertEqual(len(cycle['sentinel_restarts']), 2)
        for row in topology.owned:
            if row['descriptor']['owner']['environment_id'] == 'env-p':
                self.assertEqual(row['descriptor']['incarnation_generation'], 2)
                self.assertTrue(topology.stores[row['descriptor']['owner']['machine_id']]['containers'][row['container_id']]['State']['Running'])
            else:
                self.assertEqual(row['descriptor']['incarnation_generation'], 1)
        session = first['session']
        self.assertEqual(session['before']['volume'], session['after']['volume'])
        self.assertGreater(session['after']['always']['State']['StartedAt'], session['before']['always']['State']['StartedAt'])
        self.assertEqual(session['after']['stopped']['State']['StartedAt'], session['before']['stopped']['State']['StartedAt'])
        self.assertEqual(session['after']['volume_sentinel_sha256'], subject.SENTINEL_SHA256)
        self.assertEqual(session['after']['writable_layer']['sha256'], subject.SENTINEL_SHA256)
        self.assertEqual(session['after']['incarnation_generation'], 2)
        self.assertEqual(first['scenarios']['docker.storage.persistence'], 'dev_observed_via_public_stop_up_not_release_certified')
        self.assertFalse(first['recovery_path']['in_guest_dockerd_restart_supported'])
        self.assertFalse(first['release_certified'])
        self.assertEqual(first['cleanup']['force_used'], False)
        # Monitor handoff: original stopped, phase over env-n only, resumed over all four installed.
        names = [entry for entry in self.monitor_log if entry[0] in ('monitor-new', 'monitor-stop')]
        self.assertEqual(names[:5], [('monitor-new', ('m0', 'm1', 'm2', 'm3'), 'sibling-liveness'),
                                     ('monitor-stop', 'sibling-liveness'),
                                     ('monitor-new', ('m2', 'm3'), 'other-environment-liveness'),
                                     ('monitor-stop', 'other-environment-liveness'),
                                     ('monitor-new', ('m0', 'm1', 'm2', 'm3'), 'resumed-liveness')])
        self.assertEqual(len(harness.recovery_monitors), 2)
        self.assertTrue(harness.monitor.alive)
        self.assertEqual(harness.monitor.output.name, 'resumed-liveness')
        # Ordering: Stop happens after both Sessions were prepared, before any removal.
        prepared = [i for i, e in enumerate(topology.events) if e == 'recovery-run-stopped']
        self.assertEqual(len(prepared), 2)
        self.assertLess(prepared[1], topology.events.index('stop:env-p'))
        self.assertLess(topology.events.index('up:env-p'), topology.events.index('recovery-remove-always'))
        self.assertLess(topology.events.index('recovery-daemon-after-m0'), topology.events.index('recovery-stop-always'))
        self.assertTrue((self.evidence / 'recovery-machine-0/cycle-env-p.json').exists())
        self.assertTrue((self.evidence / 'recovery-machine-0/workload-before-m1.json').exists())
        retained = subject.parse((self.evidence / 'recovery-machine-0/machine-recovery-validation.json').read_bytes())
        self.assertEqual(retained, first)

        second = self.invoke(harness, 1)
        self.assertEqual(topology.events.count('stop:env-p'), 1, 'index 1 must not cycle again')
        self.assertTrue(harness.recovery_sessions[1].cleanup_complete)
        self.assertEqual(len(topology.stores['m1']['containers']), 1)
        self.assertEqual(second['session']['verified_incarnations'], ['inc-m1-g2', 'inc-m1-g2'])
        self.assertEqual(second['cycle_document'], first['cycle_document'])
        self.assertEqual(len(harness.recovery_monitors), 2)

        third = self.invoke(harness, 2)
        self.assertEqual(topology.events.count('stop:env-n'), 1)
        self.assertEqual(topology.events.count('up:env-n'), 1)
        self.assertEqual(len(harness.recovery_sessions), 3)
        self.assertTrue(all(s.cleanup_complete and not s.failed for s in harness.recovery_sessions))
        self.assertEqual(third['cycle']['machine_ids'], ['m2', 'm3'])
        self.assertEqual(third['cycle']['other_environment_id'], 'env-p')
        self.assertEqual(len(third['cycle']['readiness_polls']['m3']['samples']), 1)
        self.assertEqual(len(harness.recovery_monitors), 4)
        self.assertEqual(topology.generation, {'env-p': 2, 'env-n': 2})
        for mid in ('m0', 'm1', 'm2', 'm3'):
            self.assertEqual(len(topology.stores[mid]['containers']), 1)
            self.assertEqual(topology.stores[mid]['volumes'], {})
            self.assertEqual(topology.stores[mid]['images'], {IMAGE})
        for row in topology.owned:
            self.assertEqual(row['descriptor']['incarnation_generation'], 2)
        self.assertEqual(sorted(set(harness.recovery_cycles)), ['env-n', 'env-p'])

    def test_failures_withhold_cleanup_and_keep_sessions_registered(self):
        knobs = {
            'always-not-restarted': dict(always_not_restarted=True),
            'volume-corrupt': dict(volume_corrupt=True),
            'writable-corrupt': dict(writable_corrupt=True),
            'stopped-started': dict(stopped_started_by_up=True),
            'sentinel-running': dict(sentinel_running_after_up=True),
            'inventory-drift': dict(inventory_drift=True),
            'boot-id-same': dict(boot_id_same=True),
            'engine-changed': dict(engine_changed=True),
            'daemon-replaced': dict(daemon_replaced=True),
            'after-image': dict(overrides={'recovery-after-0-image': (b'sha256:' + b'f' * 64 + b'\n', b'', 0)}),
        }
        for name, options in knobs.items():
            with self.subTest(name=name):
                self.monitor_log.clear()
                harness = self.harness(**options)
                topology = harness.topology
                with self.assertRaises(ValueError):
                    self.invoke(harness, 0)
                labels = [m[0] for m in topology.mutations]
                self.assertFalse(any(l.startswith('recovery-remove') or l == 'recovery-stop-always' for l in labels), labels)
                self.assertEqual(len(harness.recovery_sessions), 2)
                for session in harness.recovery_sessions:
                    self.assertTrue(session.failed)
                    self.assertFalse(session.cleanup_complete)
                self.assertEqual(len(topology.stores['m0']['containers']), 3)
                self.assertEqual(len(topology.stores['m1']['volumes']), 1)
                self.assertFalse((self.evidence / 'recovery-machine-0/machine-recovery-validation.json').exists())
                if name == 'engine-changed':
                    self.assertEqual(topology.events.count('up:env-p'), 1)
                shutil.rmtree(self.evidence / 'recovery-machine-0')
                if (self.evidence / 'topology.json').exists():
                    (self.evidence / 'topology.json').unlink()
                shutil.rmtree(self.root / 'developer')

    def test_readiness_poll_records_samples_and_enforces_deadline(self):
        harness = self.harness()
        topology = harness.topology
        descriptor = topology.descriptor('m2')
        answers = iter([(b'', b'Cannot connect\n', 1), (b'engine-m2\n', b'', 0)])
        harness.docker = Mock(side_effect=lambda *a, **k: next(answers))
        started = subject.time.time_ns()
        proof = subject.readiness_poll(harness, descriptor, 'ready', started)
        self.assertEqual([s['healthy'] for s in proof['samples']], [False, True])
        self.assertEqual(self.sleeps, [1])
        self.assertEqual(proof['declared']['deadline_seconds'], 60)
        harness.docker = Mock(return_value=(b'', b'Cannot connect\n', 1))
        with self.assertRaisesRegex(ValueError, 'readiness deadline exceeded'):
            subject.readiness_poll(harness, descriptor, 'ready', subject.time.time_ns() - 60 * 10 ** 9)

    def test_foreign_inputs_order_and_evidence_rejected_before_commands(self):
        for field in ('machine_id', 'machine_incarnation', 'docker_context', 'docker_endpoint', 'engine_id'):
            with self.subTest(field=field):
                harness = self.harness()
                descriptor, scope, proof, images = self.inputs(harness.topology, 'm0')
                scope[field] = 'foreign'
                with self.assertRaises(ValueError):
                    subject.run_machine(harness, descriptor, scope, proof, images, 0)
                self.assertEqual(harness.topology.events, [])
                shutil.rmtree(self.root / 'developer'); (self.evidence / 'topology.json').unlink()
        for change in ('descriptors', 'proof', 'images', 'index', 'evidence', 'uncertain', 'pin', 'index-1-first', 'index-2-first'):
            with self.subTest(change=change):
                harness = self.harness()
                descriptor, scope, proof, images = self.inputs(harness.topology, 'm0')
                index = 0
                if change == 'descriptors': harness.descriptors.clear()
                elif change == 'proof': proof.clear()
                elif change == 'images': images['compose'] = {'reference': 'x', 'id': 'sha256:' + 'c' * 64, 'platform': 'linux/arm64'}
                elif change == 'index': index = 3
                elif change == 'evidence': (self.evidence / 'recovery-machine-0').mkdir()
                elif change == 'uncertain': harness.effects_uncertain = True
                elif change == 'pin': harness.info['inputs'][subject.required_source_paths()[0]] = '0' * 64
                elif change == 'index-1-first':
                    descriptor, scope, proof, images = self.inputs(harness.topology, 'm1'); index = 1
                else:
                    descriptor, scope, proof, images = self.inputs(harness.topology, 'm2'); index = 2
                with self.assertRaises(ValueError):
                    subject.run_machine(harness, descriptor, scope, proof, images, index)
                harness.mutate.assert_not_called()
                harness.stop.assert_not_called()
                self.assertEqual([e for e in harness.topology.events if e.startswith('recovery-')], [])
                shutil.rmtree(self.root / 'developer'); (self.evidence / 'topology.json').unlink()
                if (self.evidence / 'recovery-machine-0').exists():
                    shutil.rmtree(self.evidence / 'recovery-machine-0')

    def test_inputs_not_mutated_and_result_resealed(self):
        harness = self.harness()
        descriptor, scope, proof, images = self.inputs(harness.topology, 'm0')
        snapshot = copy.deepcopy((descriptor, scope, proof, images))
        subject.run_machine(harness, descriptor, scope, proof, images, 0)
        self.assertEqual((descriptor, scope, proof, images), snapshot)
        shutil.rmtree(self.evidence / 'recovery-machine-0'); (self.evidence / 'topology.json').unlink()
        shutil.rmtree(self.root / 'developer')
        self.monitor_log.clear()
        harness = self.harness()
        original = subject.startup.document
        def tamper(path, value):
            original(path, dict(value, forged=True) if path.name == 'machine-recovery-validation.json' else value)
        with patch.object(subject.startup, 'document', side_effect=tamper):
            with self.assertRaisesRegex(ValueError, 'retained recovery result differs'):
                self.invoke(harness, 0)

    def test_real_phase_monitor_is_a_sentinel_monitor_with_its_own_single_use_directory(self):
        import linux_docker_e2e as gate
        for item in self.patches:
            item.stop()
        try:
            harness = SimpleNamespace(env={'LC_ALL': 'C'}, evidence=self.evidence)
            rows = [{'descriptor': {'owner': {'machine_id': 'm2'}, 'name': 'ctx'}, 'container_id': '2' * 64}]
            monitor = subject.phase_monitor(harness, rows, self.evidence / 'phase-a')
            self.assertIsInstance(monitor, gate.SentinelMonitor)
            self.assertIs(monitor.rows, rows)
            self.assertFalse(monitor.thread.is_alive())
            self.assertEqual(oct((self.evidence / 'phase-a').stat().st_mode & 0o777), '0o700')
            self.assertEqual(monitor.record.root, self.evidence / 'phase-a')
            with self.assertRaises(FileExistsError):
                subject.phase_monitor(harness, rows, self.evidence / 'phase-a')
            with self.assertRaises(ValueError):
                subject.phase_monitor(harness, [], self.evidence / 'phase-b')
        finally:
            for item in self.patches:
                item.start()

    def test_project_binding_requires_private_owned_topology(self):
        harness = self.harness()
        owner = {'project_id': 'prj_one', 'environment_id': 'env-p', 'machine_id': 'm0'}
        self.assertEqual(subject.project_path(harness, owner), self.root / 'developer')
        with self.assertRaises(ValueError):
            subject.project_path(harness, dict(owner, project_id='prj_other'))
        with self.assertRaises(ValueError):
            subject.project_path(harness, dict(owner, environment_id='env-x'))
        (self.evidence / 'topology.json').chmod(0o644)
        with self.assertRaises(ValueError):
            subject.project_path(harness, owner)
        (self.evidence / 'topology.json').unlink()
        with self.assertRaises(ValueError):
            subject.project_path(harness, owner)


if __name__ == '__main__':
    unittest.main()
