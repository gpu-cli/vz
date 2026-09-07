"""Mocked exact-resource/session boundaries; no Docker or guest dispatch."""
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_registry_commands as commands
import linux_docker_registry_fixture as fixture
import linux_docker_registry_session as subject

OWNER = {'project_id': 'project', 'environment_id': 'environment', 'machine_id': 'machine'}
SPEC = fixture.resource_spec(OWNER, 'run')
INSTANCE = '11111111-1111-4111-8111-111111111111'
OTHER_INSTANCE = '22222222-2222-4222-8222-222222222222'
LISTENING = 'listening on ' + SPEC['authority'] + ', tls'


def startup_row(msg, *, instance=INSTANCE, level='info', version='3.1.1', go='go1.25.9',
                time='2026-09-06T20:00:00.000000001Z'):
    return {'go.version': go, 'instance.id': instance, 'level': level, 'msg': msg, 'time': time, 'version': version}


def lines(rows):
    return b''.join((row if type(row) is bytes else json.dumps(row, separators=(',', ':')).encode()) + b'\n'
                    for row in rows)


def startup_log(**changes):
    return lines([startup_row('redis not configured', **changes),
                  startup_row('restricting TLS version to tls1.2 or higher', **changes),
                  startup_row(LISTENING, **changes)])


def handshake(reason='bad certificate', peer=SPEC['gateway'], port='41234'):
    return ('2026/09/06 21:04:23 http: TLS handshake error from ' + peer + ':' + port +
            ': remote error: tls: ' + reason).encode()


def challenge_row(**changes):
    row = startup_row('error authorizing context: basic authentication challenge for realm '
                      '"vz-private-registry": invalid authorization credential', level='warning', **changes)
    row.update({'http.request.host': SPEC['authority'], 'http.request.id': '33333333-3333-4333-8333-333333333333',
                'http.request.method': 'GET', 'http.request.remoteaddr': SPEC['gateway'] + ':45678',
                'http.request.uri': '/v2/', 'http.request.useragent': 'docker/29.7.2 go/go1.25.7'})
    return row


def network():
    return {'Name': SPEC['network_name'], 'Id': 'a' * 64, 'Driver': 'bridge', 'Scope': 'local',
            'Internal': True, 'Labels': dict(SPEC['labels']), 'EnableIPv6': False,
            'IPAM': {'Driver': 'default', 'Config': [{'Subnet': SPEC['subnet'], 'Gateway': SPEC['gateway']}]},
            'Containers': {}}


def volume():
    return {'Name': SPEC['volume_name'], 'Driver': 'local', 'Scope': 'local',
            'Labels': dict(SPEC['labels']), 'Options': None}


def encoded(row):
    return json.dumps([row]).encode()


class IdentityTests(unittest.TestCase):
    def test_exact_internal_network(self):
        row = network()
        self.assertEqual(subject.network_identity(encoded(row), SPEC), row)

    def test_network_rejects_foreign_and_public_topology(self):
        for key, value in (('Name', 'foreign'), ('Id', 'short'), ('Driver', 'host'),
                           ('Scope', 'swarm'), ('Internal', False), ('Labels', {}),
                           ('EnableIPv6', True), ('IPAM', {'Driver': 'default', 'Config': []})):
            row = network(); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                subject.network_identity(encoded(row), SPEC)

    def test_network_rejects_wrong_gateway_and_extra_ranges(self):
        row = network(); row['IPAM']['Config'][0]['Gateway'] = '172.30.241.9'
        with self.assertRaises(ValueError):
            subject.network_identity(encoded(row), SPEC)
        row = network(); row['IPAM']['Config'].append({'Subnet': '10.0.0.0/8'})
        with self.assertRaises(ValueError):
            subject.network_identity(encoded(row), SPEC)

    def test_exact_volume(self):
        row = volume()
        self.assertEqual(subject.volume_identity(encoded(row), SPEC), row)

    def test_volume_rejects_foreign_or_host_backed_options(self):
        for key, value in (('Name', 'foreign'), ('Driver', 'foreign'), ('Scope', 'swarm'),
                           ('Labels', {}), ('Options', {'device': '/foreign', 'type': 'none'})):
            row = volume(); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                subject.volume_identity(encoded(row), SPEC)

    def test_single_resource_inventory_required(self):
        for validate, row in ((subject.network_identity, network()), (subject.volume_identity, volume())):
            for raw in (b'[]', json.dumps([row, row]).encode(), b'{}'):
                with self.assertRaises(ValueError):
                    validate(raw, SPEC)


class LogClassificationTests(unittest.TestCase):
    PROBES = {'repository_name': 'vz-registry-abc', 'username': 'vz-registry-user', 'host': '172.30.241.2:5443',
              'blob_digests': ('sha256:' + 'a' * 64, 'sha256:' + 'b' * 64)}

    def probe(self, kind, digest='sha256:' + 'a' * 64, **override):
        row = startup_row('response completed with error', level='error')
        row.update({'auth.user.name': 'vz-registry-user', 'vars.name': 'vz-registry-abc', 'http.request.id': 'r',
                    'http.request.method': 'HEAD', 'http.request.host': '172.30.241.2:5443',
                    'http.request.useragent': 'ua', 'http.request.remoteaddr': '172.30.241.1:1',
                    'http.response.status': 404, 'http.response.contenttype': 'application/json',
                    'http.response.written': 1, 'http.response.duration': '1ms'})
        if kind == 'blob':
            row.update({'http.request.uri': '/v2/vz-registry-abc/blobs/' + digest, 'vars.digest': digest,
                        'err.code': 'blob unknown', 'err.message': 'blob unknown to registry', 'err.detail': digest})
        else:
            row.update({'http.request.uri': '/v2/vz-registry-abc/manifests/subject', 'vars.reference': 'subject',
                        'err.code': 'manifest unknown', 'err.message': 'manifest unknown',
                        'err.detail': 'unknown tag=subject'})
        row.update(override)
        return (json.dumps(row) + '\n').encode()

    def test_push_probes_admitted_exactly_once_per_push(self):
        raw = (json.dumps(startup_row('redis not configured')) + '\n').encode() + self.probe('blob') + \
            self.probe('blob', digest='sha256:' + 'b' * 64) + self.probe('manifest')
        proof = subject.classify_log(raw, instance_id=INSTANCE, gateway='172.30.241.1', phase='final',
                                     push_probes=self.PROBES)
        self.assertEqual(proof['push_not_found_probes'], {'blob': 2, 'manifest': 1})
        with self.assertRaises(ValueError):
            subject.classify_log(raw, instance_id=INSTANCE, gateway='172.30.241.1', phase='final')
        for bad in (raw + self.probe('manifest'), raw[:-len(self.probe('manifest'))],
                    raw.replace(b'"http.request.method": "HEAD"', b'"http.request.method": "GET"', 1),
                    raw.replace(b'vz-registry-user', b'other-user', 1),
                    raw.replace(b'"http.response.status": 404', b'"http.response.status": 500', 1),
                    raw.replace(('sha256:' + 'b' * 64).encode(), ('sha256:' + 'c' * 64).encode())):
            with self.subTest(bad=bad[-120:]), self.assertRaises(ValueError):
                subject.classify_log(bad, instance_id=INSTANCE, gateway='172.30.241.1', phase='final',
                                     push_probes=self.PROBES)

    def test_failed_basic_auth_record_keys_admitted(self):
        row = startup_row('user failed to authenticate')
        row.update({'username': 'vz-registry-user', 'error': 'htpasswd: invalid credentials',
                    'http.request.id': 'r', 'http.request.method': 'GET', 'http.request.host': 'h',
                    'http.request.uri': '/v2/', 'http.request.useragent': 'ua', 'http.request.remoteaddr': 'p'})
        raw = (json.dumps(startup_row('redis not configured')) + '\n' + json.dumps(row) + '\n').encode()
        proof = subject.classify_log(raw, instance_id=INSTANCE, gateway='172.30.241.1', phase='invalid')
        self.assertEqual(proof['json_records'], 2)

    def test_startup_identity_from_consistent_json_only(self):
        proof = subject.startup_identity(startup_log(), authority=SPEC['authority'])
        self.assertEqual(proof['instance_id'], INSTANCE)
        self.assertEqual((proof['rows'], proof['listening'], proof['version'], proof['go_version']),
                         (3, LISTENING, '3.1.1', 'go1.25.9'))
        self.assertEqual(proof['raw_bytes'], len(startup_log()))
        self.assertNotIn('raw', proof)

    def test_startup_rejects_plaintext_drift_and_missing_tls_listener(self):
        rows = [startup_row('redis not configured'), startup_row(LISTENING)]
        for raw in (lines([rows[0], handshake(), rows[1]]), lines(rows + [handshake()]),
                    lines([rows[0], startup_row(LISTENING, instance=OTHER_INSTANCE)]),
                    startup_log(version='v3.1.0'), startup_log(go='go1.25.8'), startup_log(level='warning'),
                    lines([rows[0]]), lines([rows[0], startup_row('listening on ' + SPEC['authority'])]),
                    lines(rows + [startup_row(LISTENING)]), lines([dict(rows[1], service='registry')]),
                    lines([b'plain text\n']), startup_log()[:-1], b''):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                subject.startup_identity(raw, authority=SPEC['authority'])

    def classify(self, raw, alerts=(), phase='wrong-ca'):
        return subject.classify_log(raw, instance_id=INSTANCE, gateway=SPEC['gateway'], phase=phase,
                                    handshake_alerts=alerts)

    def test_wrong_ca_phase_admits_exactly_the_bad_certificate_alert(self):
        raw = startup_log() + lines([handshake(), challenge_row()])
        proof = self.classify(raw, ('bad certificate',))
        self.assertEqual((proof['lines'], proof['json_records'], proof['handshake_errors']), (5, 4, 1))
        self.assertEqual(proof['handshake_reasons'], {'bad certificate': 1})
        self.assertEqual(proof['record_levels'], {'info': 3, 'warning': 1})
        self.assertEqual(proof['filtered_lines'], 0)
        self.assertEqual(self.classify(startup_log() + lines([handshake(), handshake()]), ('bad certificate',))
                         ['handshake_errors'], 2)

    def test_handshake_lines_never_pass_without_phase_allowlist(self):
        with self.assertRaises(ValueError):
            self.classify(startup_log() + lines([handshake()]))
        with self.assertRaises(ValueError):
            subject.classify_log(startup_log(), instance_id=INSTANCE, gateway=SPEC['gateway'], phase='x',
                                 handshake_alerts=('unknown certificate authority',))

    def test_other_plaintext_and_foreign_handshakes_rejected(self):
        for line in (handshake('unknown certificate authority'), handshake('handshake failure'),
                     handshake('bad certificate', peer='172.30.241.3'), handshake('bad certificate', port='0'),
                     handshake('bad certificate', port='70000'), handshake().replace(b'remote error', b'local error'),
                     handshake() + b' trailing', b'2026/09/06 21:04:23 http: TLS handshake error from ' +
                     SPEC['gateway'].encode() + b':41234: EOF', b'panic: runtime error', b' ' + handshake(),
                     b'\xff\xfe', handshake('bad certificate\xc3\xa9'.encode() if False else 'bad certificat\u00e9')):
            with self.subTest(line=line), self.assertRaises(ValueError):
                self.classify(startup_log() + lines([line]), ('bad certificate',))

    def test_json_rows_must_match_instance_pins_fields_and_levels(self):
        for row in (startup_row('x', instance=OTHER_INSTANCE), startup_row('x', version='v3.1.1'),
                    startup_row('x', go='go1.25.8'), startup_row('x', level='error'),
                    startup_row('x', level='debug'), dict(startup_row('x'), Authorization='Basic xx'),
                    dict(startup_row('x'), unknown='y'), startup_row('x', time='yesterday'),
                    {'msg': 'x'}, {'instance.id': INSTANCE}):
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.classify(startup_log() + lines([row]))
        for raw in (b'[]\n', b'{"a":1}\n' + startup_log(), startup_log()[:-1], b'{}}\n'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.classify(raw)
        self.assertEqual(self.classify(b'')['lines'], 0)
        self.assertEqual(self.classify(startup_log() + lines([dict(challenge_row(), service='registry')]))
                         ['json_records'], 4)


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.harness = SimpleNamespace(info={'run_id': 'run', 'registry_archive': '/owned/registry.tar',
            'registry': {'manifest_digest': 'sha256:' + 'b' * 64},
            'clients': {'docker': {'canonical': '/owned/docker'}}},
            evidence=Path('/owned/evidence'), config=Path('/owned/config'), cli=Path('/owned/vz'),
            record=SimpleNamespace(canaries=[], receipts=[]), sensitive_canaries=[], registry_sessions=[],
            effects_uncertain=False, monitor=None,
            assert_certain=Mock(), docker=Mock(return_value=(b'', b'', 0)),
            mutate=Mock(return_value=(b'', b'', 0)), exact_absent=Mock(), command=Mock())
        self.descriptor = {'owner': dict(OWNER), 'config_dir': '/owned/machine-config',
                           'name': 'exact-context', 'incarnation_id': 'incarnation'}
        self.private = Mock()
        self.private.password.return_value = b'1' * 48
        self.private.canaries.return_value = (b'1' * 48, b'2' * 48)
        self.private.public.return_value = {'owner': dict(OWNER), 'run_id': 'run',
                                           'authority': SPEC['authority']}
        self.private.privatefiles.return_value = {name: b'fixture-' + name.encode()
            for name in subject.guest.FILES}
        self.private.ca_pem.return_value = b'public-fixture-ca'
        self.private.pins = {'ca_sha256': 'c' * 64}
        self.owned_commands = Mock(spec=commands.Commands)
        self.owned_commands.assert_certain = Mock()
        self.store = Mock()
        self.store.snapshot.return_value = object()
        self.store.check_transition.return_value = {'specific_auth_transition': True}
        self.store.check_unchanged.return_value = {'unchanged': True}
        patches = (patch.object(commands, 'Commands', return_value=self.owned_commands),
                   patch.object(subject.credentials, 'Store', return_value=self.store),
                   patch.object(subject.startup, 'private', side_effect=lambda path: path),
                   patch.object(subject.startup, 'document'),
                   patch.object(subject.fixture, 'validate_tls_public'))
        self.mocks = []
        for item in patches:
            self.mocks.append(item.start())
            self.addCleanup(item.stop)

    def session(self):
        return subject.Session(self.harness, self.descriptor, '/owned/project', 0, self.private)

    def test_construction_registers_without_dispatch(self):
        item = self.session()
        self.assertEqual(self.harness.registry_sessions, [item])
        self.assertFalse(item.prepared)
        self.assertFalse(item.workload_complete)
        self.assertFalse(item.cleanup_complete)
        self.harness.docker.assert_not_called()
        self.harness.mutate.assert_not_called()
        self.owned_commands.private.assert_not_called()
        self.store.snapshot.assert_called_once_with(expected='empty')
        self.private.validate_private.assert_called_once()
        self.mocks[4].assert_called_once()

    def test_native_or_owner_admission_failure_precedes_side_effects(self):
        for admission in (self.private.validate_private, self.mocks[4]):
            admission.side_effect = ValueError('fixture rejected')
            with self.assertRaisesRegex(ValueError, 'fixture rejected'):
                self.session()
            self.mocks[0].assert_not_called()
            self.mocks[2].assert_not_called()
            self.harness.mutate.assert_not_called()
            self.assertEqual(self.harness.registry_sessions, [])
            admission.side_effect = None

    def test_certain_checks_private_owner_and_harness(self):
        item = self.session(); item.certain()
        self.owned_commands.assert_certain.assert_called_once()
        self.harness.assert_certain.assert_not_called()
        item.failed = True
        with self.assertRaises(ValueError):
            item.certain()

    def test_uncertain_main_receipt_fences_session(self):
        item = self.session()
        self.harness.record.receipts.append({'effects_uncertain': True})
        with self.assertRaises(ValueError):
            item.docker('must-not-run', ['version'])
        self.harness.docker.assert_not_called()

    def test_live_monitor_failure_fences_session(self):
        item = self.session()
        self.harness.monitor = SimpleNamespace(thread=SimpleNamespace(is_alive=lambda: True),
            check=Mock(side_effect=ValueError('sentinel failed')))
        with self.assertRaisesRegex(ValueError, 'sentinel failed'):
            item.docker('must-not-run', ['version'])
        self.harness.docker.assert_not_called()

    def test_exact_public_exec_argv_and_private_stdin_selector(self):
        item = self.session()
        expected = ['/owned/vz', 'exec', '--environment', 'environment', '--machine', 'machine']
        public = item.exec_argv('source-fixed')
        private = item.exec_argv('source-fixed', stdin=True)
        self.assertEqual(public[:6], expected)
        self.assertEqual(private[:6], expected)
        self.assertIn('--no-stdin', public)
        self.assertNotIn('--no-stdin', private)
        self.assertEqual(private[6:], ['--timeout', '30', '--', '/bin/busybox', 'sh', '-c', 'source-fixed'])

    def test_public_exec_rejects_nonzero_and_stderr(self):
        item = self.session()
        for result in ((b'output', b'', 1), (b'output', b'error', 0)):
            self.harness.command.return_value = result
            with self.assertRaises(ValueError):
                item.public_exec('observation', 'source-fixed')

    def test_prepare_collision_is_sticky_before_any_mutation(self):
        item = self.session()
        self.harness.docker.return_value = (json.dumps({'Name': SPEC['network_name']}).encode() + b'\n', b'', 0)
        with self.assertRaises(ValueError):
            item.prepare()
        self.assertTrue(item.failed)
        self.assertFalse(item.prepared)
        self.harness.mutate.assert_not_called()
        self.owned_commands.private.assert_not_called()
        for canary in self.private.canaries():
            self.assertIn(canary, self.harness.record.canaries)
            self.assertIn(canary, self.harness.sensitive_canaries)
        with self.assertRaises(ValueError):
            item.cleanup()

    def test_prepare_real_guest_fixed_ack_seam(self):
        item = self.session()
        with patch.object(item, 'private_exec', side_effect=RuntimeError('stop at private setup')) as private:
            with self.assertRaisesRegex(RuntimeError, 'stop at private setup'):
                item.prepare()
        self.assertTrue(item.failed)
        private.assert_called_once()
        label, script, payload, acknowledgment = private.call_args.args
        self.assertEqual(label, 'setup')
        self.assertEqual(script, subject.guest.setup_script(item.plan))
        self.assertEqual(acknowledgment, subject.guest.fixed_ack(item.plan, action='SETUP'))
        self.assertTrue(payload.startswith(b'VZ_REGISTRY_PRIVATE_V1\n'))
        self.assertEqual(self.harness.mutate.call_count, 1)

    def test_document_canary_rejected_without_write(self):
        item = self.session()
        before = self.mocks[3].call_count
        with self.assertRaises(ValueError):
            item.document('public.json', {'secret': '1' * 48})
        self.assertEqual(self.mocks[3].call_count, before)

    def prepare_dispatch(self, item, logs):
        """Canned exact-resource answers keyed by label; the registry log is supplied."""
        cid = 'c' * 64
        running = [False]

        def inspect_row():
            return {'Id': cid, 'Name': '/' + SPEC['container_name'],
                    'Image': self.harness.info['registry']['manifest_digest'],
                    'Config': {'Labels': dict(SPEC['labels'])},
                    'State': {'Running': running[0], 'ExitCode': 0, 'OOMKilled': False}, 'RestartCount': 0,
                    'HostConfig': {'PortBindings': {}, 'RestartPolicy': {'Name': 'no'}},
                    'Mounts': [{'Destination': '/run/vz-registry', 'Type': 'bind', 'Source': item.plan['directory'],
                                'RW': False}, {'Destination': '/var/lib/registry', 'Type': 'volume',
                                               'Name': SPEC['volume_name'], 'RW': True}],
                    'NetworkSettings': {'Networks': {SPEC['network_name']: {
                        'NetworkID': 'a' * 64, 'IPAddress': SPEC['address'], 'Gateway': '',
                        'IPAMConfig': {'IPv4Address': SPEC['address']}, 'IPPrefixLen': 24,
                        'Links': None, 'Aliases': None}}}}
        answers = {'registry-network-inspect': encoded(network()), 'registry-volume-inspect': encoded(volume())}

        def docker(label, descriptor, args, **options):
            if label == 'registry-server-inspect':
                return encoded(inspect_row()), b'', 0
            if label == 'registry-server-logs':
                return b'', logs, 0
            return answers.get(label, b''), b'', 0

        def mutate(label, descriptor, args, **options):
            if label == 'registry-server-start':
                running[0] = True
            return (cid.encode() + b'\n' if label == 'registry-server-create' else b''), b'', 0
        self.harness.docker.side_effect = docker
        self.harness.mutate.side_effect = mutate
        patches = (patch.object(item, 'private_exec'), patch.object(item, 'public_exec', return_value=b'ack'),
                   patch.object(subject.guest, 'parse_ack', return_value={'owner': 'ids'}),
                   patch.object(subject.time, 'sleep', side_effect=AssertionError('no readiness wait expected')))
        for entry in patches:
            entry.start()
            self.addCleanup(entry.stop)

    def documents(self):
        return [call.args[0].name for call in self.mocks[3].call_args_list]

    def test_prepare_captures_instance_from_all_json_startup_rows(self):
        item = self.session()
        self.prepare_dispatch(item, startup_log())
        item.prepare()
        self.assertTrue(item.prepared)
        self.assertEqual(item.instance_id, INSTANCE)
        self.assertEqual(item.startup_log, startup_log())
        self.assertFalse(item.wrong_ca_probed)
        names = self.documents()
        self.assertIn('startup-log.json', names)
        self.assertLess(names.index('startup-log.json'), names.index('prepared.json'))
        startup_proof = next(c.args[1] for c in self.mocks[3].call_args_list if c.args[0].name == 'startup-log.json')
        self.assertEqual((startup_proof['instance_id'], startup_proof['rows'], startup_proof['listening']),
                         (INSTANCE, 3, LISTENING))
        prepared = next(c.args[1] for c in self.mocks[3].call_args_list if c.args[0].name == 'prepared.json')
        self.assertEqual(prepared['instance_id'], INSTANCE)

    def test_prepare_rejects_plaintext_or_inconsistent_startup_rows(self):
        for logs in (lines([startup_row('redis not configured'), handshake(), startup_row(LISTENING)]),
                     startup_log() + lines([handshake()]),
                     lines([startup_row('redis not configured', instance=OTHER_INSTANCE), startup_row(LISTENING)]),
                     lines([startup_row('listening on ' + SPEC['authority'])])):
            with self.subTest(logs=logs):
                self.setUp()
                item = self.session()
                self.prepare_dispatch(item, logs)
                with self.assertRaises(ValueError):
                    item.prepare()
                self.assertTrue(item.failed)
                self.assertFalse(item.prepared)
                self.assertIsNone(item.instance_id)
                self.assertNotIn('startup-log.json', self.documents())

    def prepared(self):
        item = self.session()
        item.prepared, item.container_id = True, 'c' * 64
        item.instance_id, item.startup_log = INSTANCE, startup_log()
        return item

    def test_classify_binds_prefix_and_exact_wrong_ca_handshake_count(self):
        item = self.prepared()
        self.assertEqual(item.classify(startup_log(), 'pre')['handshake_errors'], 0)
        with self.assertRaises(ValueError):
            item.classify(startup_log() + lines([handshake()]), 'pre')
        item.wrong_ca_probed = True
        proof = item.classify(startup_log() + lines([handshake(), challenge_row()]), 'wrong-ca')
        self.assertEqual((proof['handshake_errors'], proof['handshake_alerts_allowed']), (1, ['bad certificate']))
        for raw in (startup_log(), startup_log() + lines([handshake(), handshake()]),
                    startup_log() + lines([handshake('unknown certificate authority')]),
                    startup_log()[:-2] + b'\n' + lines([handshake()]), lines([handshake()]) + startup_log()):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                item.classify(raw, 'wrong-ca')
        item.instance_id = None
        with self.assertRaises(ValueError):
            item.classify(startup_log() + lines([handshake()]), 'wrong-ca')

    def test_retain_log_writes_raw_bytes_after_classification_and_canary_scan(self):
        item = self.prepared()
        item.wrong_ca_probed = True
        raw = startup_log() + lines([handshake(), challenge_row()])
        with patch.object(subject.startup, 'write') as write:
            proof = item.retain_log('final', raw, 'final')
        write.assert_called_once_with(Path('/owned/evidence/registry-machine-0/registry-stderr-final.log'), raw)
        self.assertEqual(proof['retained_file'], 'registry-stderr-final.log')
        self.assertEqual(proof['handshake_errors'], 1)
        self.assertEqual(self.documents()[-1], 'final-log.json')
        leaked = startup_log() + lines([handshake(), startup_row('1' * 48)])
        with patch.object(subject.startup, 'write') as write, self.assertRaises(ValueError):
            item.retain_log('leak', leaked, 'final')
        write.assert_not_called()
        with patch.object(subject.startup, 'write') as write, self.assertRaises(ValueError):
            item.retain_log('plain', startup_log() + lines([handshake(), b'stray text']), 'final')
        write.assert_not_called()

    def test_authenticate_uses_prepare_instance_and_retains_complete_log(self):
        item = self.prepared()
        item.identities = {'owner': 'ids'}
        self.private.ca_pem.side_effect = lambda wrong=False: b'wrong-ca' if wrong else b'public-fixture-ca'
        after_probe = startup_log() + lines([handshake()])
        before_login = after_probe + lines([challenge_row()])
        after_login = before_login + lines([dict(challenge_row(), msg='authorized request')])
        logs = iter([after_probe, after_probe, after_probe, before_login, after_login])
        self.harness.docker.return_value = (json.dumps({'Version': '29.7.2', 'GoVersion': 'go1.25.7',
            'GitCommit': '6a43e3d', 'KernelVersion': '6.12.85', 'Os': 'linux', 'Arch': 'arm64'}).encode(), b'', 0)
        with patch.object(item, 'login') as login, patch.object(item, 'private_exec'), \
                patch.object(item, 'public_exec', return_value=b'ack'), \
                patch.object(subject.guest, 'parse_ack'), patch.object(item, 'unauthenticated_push'), \
                patch.object(subject.guest, 'install_trust_script', return_value='trust'), \
                patch.object(subject.guest, 'inspect_script', return_value='inspect'), \
                patch.object(item, 'guest_seconds', side_effect=[1757188800, 1757188801]), \
                patch.object(item, 'logs', side_effect=lambda: next(logs)), \
                patch.object(subject.startup, 'write') as write, \
                patch.object(subject.route, 'validate', return_value={'records': 1}) as validate:
            proof = item.authenticate()
        self.assertTrue(item.wrong_ca_probed)
        registry = validate.call_args.kwargs['registry']
        self.assertEqual(registry['instance_id'], INSTANCE)
        self.assertEqual((registry['version'], registry['go_version']), ('3.1.1', 'go1.25.9'))
        self.assertEqual(validate.call_args.args[0], after_login[len(before_login):])
        self.assertEqual(validate.call_args.kwargs['window_ns'], (1757188800 * 10**9, 1757188802 * 10**9))
        write.assert_called_once_with(Path('/owned/evidence/registry-machine-0/registry-stderr-authenticate.log'),
                                      after_login)
        self.assertEqual(proof['complete_log_handshake_errors'], 1)
        self.assertEqual(proof['log_prefix_bytes'], len(before_login))
        names = self.documents()
        for name in ('login-wrong-ca-log.json', 'login-invalid-log.json', 'unauthorized-push-log.json',
                     'authenticate-log.json', 'login-route.json'):
            self.assertIn(name, names)
        self.assertEqual([call.kwargs['case'] for call in login.call_args_list], ['wrong-ca', 'invalid', 'valid'])

    def test_authenticate_rejects_unexpected_plaintext_after_wrong_ca(self):
        item = self.prepared()
        item.identities = {'owner': 'ids'}
        logs = iter([startup_log() + lines([handshake('unknown certificate authority')])])
        with patch.object(item, 'login'), patch.object(item, 'logs', side_effect=lambda: next(logs)), \
                patch.object(item, 'private_exec') as trust, self.assertRaises(ValueError):
            item.authenticate()
        trust.assert_not_called()

    def test_login_uses_exact_config_context_and_private_password(self):
        item = self.session(); item.prepared = True
        item.login(case='valid', role='valid', expected_stdout=b'Login Succeeded\n',
                   expected_stderr=b'', expected_exit=0)
        args, options = self.owned_commands.private.call_args
        self.assertEqual(args[0], 'login-valid')
        self.assertEqual(args[1], ['docker', '--config', '/owned/machine-config', '--context',
            'exact-context', 'login', '--username', 'vz-registry-user', '--password-stdin', SPEC['authority']])
        self.assertEqual(options['private_input'], b'1' * 48 + b'\n')
        self.assertNotIn('1' * 48, ' '.join(args[1]))
        self.assertEqual(options['executable'], '/owned/docker')
        self.store.check_transition.assert_called_once_with(item.initial_credentials, expected='login')


if __name__ == '__main__':
    unittest.main()
