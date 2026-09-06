"""Offline lifecycle adversaries only; no Docker, SSH daemon or VM launch."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_ssh_server as server

TOKEN = 'vzssh-' + 'a' * 24
CID, IMAGE = 'b' * 64, 'sha256:' + 'c' * 64
SINCE, UNTIL = '2026-09-06T12:00:01Z', '2026-09-06T12:00:02Z'


def item(status='running'):
    return {'Id': CID, 'Image': IMAGE, 'Name': '/' + TOKEN,
        'Config': {'Labels': {server.LABEL: TOKEN}, 'Entrypoint': server.ENTRYPOINT[:1], 'Cmd': server.ENTRYPOINT[1:] + [server.START]},
        'Path': server.ENTRYPOINT[0], 'Args': server.ENTRYPOINT[1:] + [server.START],
        'HostConfig': {'NetworkMode': 'bridge', 'Runtime': 'youki', 'Privileged': False,
            'PublishAllPorts': False, 'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': 0}},
        'Mounts': [], 'NetworkSettings': {'Networks': {'bridge': {'IPAddress': '172.17.0.2'}}},
        'RestartCount': 0, 'State': {'Running': status == 'running', 'Status': status,
            'Pid': 17 if status == 'running' else 0, 'Paused': False, 'Restarting': False,
            'Dead': False, 'OOMKilled': False, 'Error': '', 'ExitCode': 0,
            'StartedAt': '2026-09-06T12:00:00Z', 'FinishedAt': UNTIL}}


def events():
    rows = []
    for action, extra in [('kill', {'signal': '15'}), ('die', {'exitCode': '0', 'execDuration': '1'}), ('stop', {})]:
        rows.append({'type': 'container', 'action': action, 'id': CID, 'scope': 'local',
                     'time_nano': server.timestamp(SINCE),
                     'attributes': {server.LABEL: TOKEN, 'name': TOKEN, 'image': IMAGE, **extra}})
    return rows


def encoded(rows):
    return b''.join(json.dumps(row).encode() + b'\n' for row in rows)


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def validate(self, row):
        return server.container_identity(row, cid=CID, image=IMAGE, token=TOKEN, status=row['State']['Status'])

    def test_exact_private_bridge_and_all_three_lifecycle_states(self):
        self.assertEqual(self.validate(item()), '172.17.0.2')
        for status in ('created', 'exited'):
            self.assertIsNone(self.validate(item(status)))

    def test_foreign_identity_runtime_ports_mounts_and_restart_rejected(self):
        changes = [('Id', 'f' * 64), ('Image', 'sha256:' + 'f' * 64), ('Name', '/foreign'),
                   ('RestartCount', 1), ('Mounts', [{'Type': 'bind'}])]
        for key, value in changes:
            row = item(); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): self.validate(row)
        for key, value in [('NetworkMode', 'host'), ('Runtime', 'runc'), ('Privileged', True),
                           ('Binds', ['/host:/data']), ('Mounts', [{}]), ('PortBindings', {'2222/tcp': []}),
                           ('PublishAllPorts', True), ('AutoRemove', True),
                           ('RestartPolicy', {'Name': 'always', 'MaximumRetryCount': 0})]:
            row = item(); row['HostConfig'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): self.validate(row)

    def test_foreign_networks_addresses_and_process_flags_rejected(self):
        for address in ('127.0.0.1', '0.0.0.0', '169.254.1.2', '8.8.8.8', '224.0.0.1', '172.017.0.2'):
            row = item(); row['NetworkSettings']['Networks']['bridge']['IPAddress'] = address
            with self.subTest(address=address), self.assertRaises(ValueError): self.validate(row)
        for field in ('Paused', 'Restarting', 'Dead', 'OOMKilled'):
            row = item(); row['State'][field] = True
            with self.subTest(field=field), self.assertRaises(ValueError): self.validate(row)
        row = item(); row['NetworkSettings']['Networks']['foreign'] = {}
        with self.assertRaises(ValueError): self.validate(row)

    def test_one_sigterm_exit_zero_with_exact_engine_events(self):
        proof = server.stopped_proof(item(), item('exited'), encoded(events()), SINCE, UNTIL, TOKEN)
        self.assertEqual(proof['signal'], 'SIGTERM')
        self.assertFalse(proof['filesystem_closure_certified'])

    def test_stop_forced_signal_duplicates_foreign_and_stale_fail(self):
        for change in ('signal', 'duplicate', 'foreign', 'missing', 'time', 'exit'):
            rows = events()
            if change == 'signal': rows[0]['attributes']['signal'] = '9'
            elif change == 'duplicate': rows.append(rows[0])
            elif change == 'foreign': rows[0]['id'] = 'd' * 64
            elif change == 'missing': rows.pop()
            elif change == 'time': rows[0]['time_nano'] = 1
            elif change == 'exit': rows[1]['attributes']['exitCode'] = '137'
            with self.subTest(change=change), self.assertRaises(ValueError):
                server.stopped_proof(item(), item('exited'), encoded(rows), SINCE, UNTIL, TOKEN)
        after = item('exited'); after['State']['StartedAt'] = SINCE
        with self.assertRaises(ValueError):
            server.stopped_proof(item(), after, encoded(events()), SINCE, UNTIL, TOKEN)

    def test_build_has_only_public_exact_owned_inputs(self):
        args = server.build_arguments({'builder': {'name': 'owned'}, 'images': {'base': {'reference': 'python@sha256:' + 'a'*64}}}, self.root, TOKEN)
        self.assertIn('--network=none', args)
        self.assertIn('--load', args)
        self.assertEqual(args[args.index('--tag')+1], TOKEN + ':server')
        self.assertEqual(args.count('--build-arg'), 1)
        for forbidden in ('--ssh', '--secret', '--push', '--privileged', '--publish'):
            self.assertNotIn(forbidden, args)

    def fake(self):
        selected = server.Server.__new__(server.Server)
        selected.token, selected.tag = TOKEN, TOKEN + ':server'
        selected.context, selected.response_path = self.root, self.root / 'public-response.txt'
        selected.response_path.write_bytes(b'public\n')
        public = self.root / 'host.pub'; public.write_bytes(b'ssh-ed25519 AAAA vz-ssh:unit:host\n')
        selected.paths = {'host_private_key': self.root / 'private-host', 'auth_public_key': self.root / 'auth.pub', 'host_public_key': public}
        selected.agent = SimpleNamespace(paths=selected.paths, verify=lambda: selected.agent_proof)
        selected.agent_proof = {'fingerprints': {'host': 'SHA256:'+'a'*43, 'auth': 'SHA256:'+'b'*43}}
        selected.inputs_snapshot = {'builder': {'name': 'owned', 'container_id': 'd'*64, 'image_id': 'sha256:'+'f'*64},
                                    'images': {'base': {'reference': 'python@sha256:'+'a'*64}}}
        selected.scope_snapshot, selected.context_digest = {'machine_id': 'mch_owned'}, 'a'*64
        selected.driver = SimpleNamespace(record=SimpleNamespace(receipts=[]), _engine_system_time=SINCE)
        selected.container_id = selected.image_id = selected.host = selected.started_identity = None
        selected.container_configuration = selected.image_configuration = None
        selected.bridge_configuration = None
        selected.bridge = Mock()
        selected.attempted = selected.closed = selected.failed = selected.prepared = selected.cleanup_authorized = False
        selected.document, selected.guard, selected.absent, selected.image = Mock(), Mock(), Mock(), Mock()
        selected.removed_id = Mock()
        selected.object = Mock(return_value={'Id': IMAGE})
        def inspect(status):
            selected.host = '172.17.0.2'
            if status == 'exited': selected.driver._engine_system_time = UNTIL
            return item(status)
        selected.inspect = Mock(side_effect=inspect)
        def command(args, **kwargs):
            stdout = b''
            if args[:2] in (['container', 'create'], ['container', 'start'], ['container', 'stop'], ['container', 'rm']):
                stdout = (CID+'\n').encode()
            elif args[:1] == ['events']: stdout = encoded(events())
            elif args[:1] == ['exec']:
                stdout = public.read_bytes() if '/usr/bin/ssh-keygen' in args else b'VZ_SSH_SERVER_READY\n'
            return SimpleNamespace(stdout=stdout, stderr=b'', timed_out=False, returncode=0, index=1)
        selected.command = Mock(side_effect=command)
        return selected

    def test_prepare_copies_only_runtime_keys_before_start_then_public_key_check(self):
        selected = self.fake()
        request = selected.prepare()
        self.assertEqual(request['host'], '172.17.0.2')
        self.assertTrue(selected.prepared)
        calls = [call.args[0] for call in selected.command.call_args_list]
        copies = [args for args in calls if args[0] == 'cp']
        self.assertEqual(len(copies), 3)
        self.assertEqual(copies[0][1], str(selected.paths['host_private_key']))
        self.assertTrue(all(args[2].startswith(CID+':'+server.DIRECTORY) for args in copies))
        start = next(i for i, args in enumerate(calls) if args[:2] == ['container', 'start'])
        self.assertTrue(all(calls.index(args) < start for args in copies))
        self.assertEqual(sum(args[:2] == ['buildx', 'build'] for args in calls), 1)
        with self.assertRaises(ValueError): selected.prepare()

    def test_failed_private_copy_withholds_cleanup_and_never_starts(self):
        selected = self.fake(); original = selected.command.side_effect
        def fail(args, **kwargs):
            if args[0] == 'cp': raise driver.Rejected('bounded failed copy')
            return original(args, **kwargs)
        selected.command.side_effect = fail
        with self.assertRaises(ValueError): selected.prepare()
        self.assertTrue(selected.failed)
        self.assertFalse(any(call.args[0][:2] == ['container', 'start'] for call in selected.command.call_args_list))
        selected.cleanup_authorized = True
        with self.assertRaises(ValueError): selected.cleanup()

    def test_normal_cleanup_only_after_semantic_acceptance_and_no_uncertain_effects(self):
        selected = self.fake(); selected.prepare()
        with self.assertRaises(ValueError): selected.cleanup()
        selected.cleanup_authorized = True
        selected.driver.record.receipts = [{'effects_uncertain': True}]
        with self.assertRaises(ValueError): selected.cleanup()
        selected.driver.record.receipts = [{'effects_uncertain': False}]
        proof = selected.cleanup()
        self.assertTrue(selected.closed)
        self.assertEqual(proof['exit_code'], 0)
        calls = [call.args[0] for call in selected.command.call_args_list]
        self.assertEqual(sum(args[:2] == ['container', 'stop'] for args in calls), 1)
        self.assertIn(['container', 'rm', CID], calls)
        self.assertIn(['image', 'rm', TOKEN+':server'], calls)
        self.assertLess(calls.index(['logs', CID]), calls.index(['container', 'rm', CID]))
        self.assertEqual(selected.removed_id.call_args_list[0].args, ('container', CID))
        self.assertEqual(selected.removed_id.call_args_list[1].args, ('image', IMAGE))
        self.assertFalse(any('--force' in args or 'prune' in args for args in calls))

    def test_foreign_host_public_key_fails_after_bounded_start_without_retry(self):
        selected = self.fake(); original = selected.command.side_effect
        def wrong(args, **kwargs):
            result = original(args, **kwargs)
            if '/usr/bin/ssh-keygen' in args: result.stdout = b'foreign\n'
            return result
        selected.command.side_effect = wrong
        with self.assertRaises(ValueError): selected.prepare()
        self.assertTrue(selected.failed)

    def test_fixed_start_wrapper_exact_modes_no_shell_or_private_literals(self):
        compile(server.START, '<fixed-start>', 'exec')
        compile(server.READY, '<bounded-ready>', 'exec')
        self.assertIn("('host_key',0o600)", server.START)
        self.assertIn("('authorized_keys',0o444)", server.START)
        self.assertIn('os.O_NOFOLLOW', server.START)
        self.assertNotIn('shell=True', server.START)
        self.assertNotIn('PRIVATE KEY', server.START)

    def test_exact_removed_id_diagnostic_not_generic_error(self):
        selected = self.fake()
        selected.command = Mock(return_value=SimpleNamespace(stdout=b'[]\n', stderr=('Error: No such object: '+CID+'\n').encode()))
        server.Server.removed_id(selected, 'container', CID)
        for raw, error in ((b'[]\n', b'connection unavailable\n'), (b'[{}]\n', ('Error: No such object: '+CID).encode())):
            selected.command.return_value = SimpleNamespace(stdout=raw, stderr=error)
            with self.assertRaises(ValueError): server.Server.removed_id(selected, 'container', CID)

    def test_process_or_address_replacement_prevents_verify(self):
        selected = self.fake()
        selected.image_id, selected.container_id = IMAGE, CID
        selected.object = Mock(return_value=item())
        server.Server.inspect(selected, 'running')
        changed = item(); changed['State']['Pid'] += 1
        selected.object.return_value = changed
        with self.assertRaises(ValueError): server.Server.inspect(selected, 'running')

    def test_guard_rejects_context_key_canary_before_engine_call(self):
        selected = self.fake()
        selected.driver.inputs = SimpleNamespace(raw=selected.inputs_snapshot, scope=selected.scope_snapshot)
        selected.driver.record.canaries = [b'private-sentinel']
        selected.driver.builder_guard = Mock()
        (self.root/'leak').write_bytes(b'private-sentinel')
        selected.context_digest = driver.tree_digest(self.root)
        with self.assertRaises(ValueError): server.Server.guard(selected)
        selected.driver.builder_guard.assert_not_called()

    def test_unlisted_container_configuration_drift_is_not_ignored(self):
        selected = self.fake()
        selected.image_id, selected.container_id = IMAGE, CID
        selected.object = Mock(return_value=item('created'))
        server.Server.inspect(selected, 'created')
        changed = item(); changed['HostConfig']['CapAdd'] = ['SYS_ADMIN']
        selected.object.return_value = changed
        with self.assertRaises(ValueError): server.Server.inspect(selected, 'running')

    def test_default_bridge_identity_and_ipam_binding_reject_replacement(self):
        selected = self.fake()
        bridge = {'Id': 'e'*64, 'Name': 'bridge', 'Driver': 'bridge', 'Scope': 'local', 'Internal': False,
                  'EnableIPv6': False, 'IPAM': {'Config': [{'Subnet': '172.17.0.0/16', 'Gateway': '172.17.0.1'}]},
                  'Options': {}, 'Labels': {}}
        builder = {'Id': 'd'*64, 'Image': 'sha256:'+'f'*64, 'State': {'Running': True},
                   'HostConfig': {'NetworkMode': 'bridge'}, 'NetworkSettings': {'Networks': {'bridge': {
                       'NetworkID': 'e'*64, 'EndpointID': 'f'*64, 'IPAddress': '172.17.0.3',
                       'Gateway': '172.17.0.1', 'IPPrefixLen': 16}}}}
        selected.object.side_effect = lambda kind, value: bridge if kind == 'network' else builder
        row = item()
        row['NetworkSettings']['Networks']['bridge'].update(NetworkID='e'*64, Gateway='172.17.0.1', IPPrefixLen=16)
        server.Server.bridge(selected, row)
        for field, value in [('NetworkID', 'f'*64), ('Gateway', '172.17.0.9'), ('IPPrefixLen', 24)]:
            changed = copy.deepcopy(row); changed['NetworkSettings']['Networks']['bridge'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): server.Server.bridge(selected, changed)
        builder['NetworkSettings']['Networks']['bridge']['NetworkID'] = 'f'*64
        with self.assertRaises(ValueError): server.Server.bridge(selected)

    def test_log_canary_failure_prevents_object_removal(self):
        selected = self.fake(); selected.prepare(); selected.cleanup_authorized = True
        original = selected.command.side_effect
        def leaked(args, **kwargs):
            if args[0] == 'logs': raise driver.Rejected('secret canary appeared; bytes withheld')
            return original(args, **kwargs)
        selected.command.side_effect = leaked
        with self.assertRaises(ValueError): selected.cleanup()
        self.assertTrue(selected.failed)
        self.assertFalse(any(call.args[0][:2] == ['container', 'rm'] for call in selected.command.call_args_list))


if __name__ == '__main__':
    unittest.main()
