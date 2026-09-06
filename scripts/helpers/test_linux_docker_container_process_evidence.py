"""Offline bridge rejection tests, not guest kernel or installed-Mac evidence."""
import copy
from contextlib import ExitStack
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_container_process_evidence as bridge

CID = 'a' * 64
BOOT = '12345678-1234-1234-1234-123456789abc'


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.umask = os.umask(0o077)
        self.addCleanup(os.umask, self.umask)
        temporary = tempfile.TemporaryDirectory(prefix='vz-process-bridge-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        project, evidence, cli = root / 'project', root / 'evidence', root / 'vz'
        project.mkdir(mode=0o700); evidence.mkdir(mode=0o700)
        cli.write_bytes(b'not executed: pinned CLI test bytes\n'); cli.chmod(0o500)
        self.descriptor = {'owner': {'project_id': 'project-exact', 'environment_id': 'env-exact',
                                    'machine_id': 'machine-exact'},
                           'incarnation_id': 'incarnation-exact', 'incarnation_generation': 1}
        topology = {'project': str(project),
                    'primary': {'project_id': 'project-exact', 'environment_id': 'env-exact', 'state': 'ready',
                                'machines': [{'machine_id': 'machine-exact', 'state': 'ready',
                                              'incarnation_id': 'incarnation-exact', 'incarnation_generation': 1,
                                              'docker_context': copy.deepcopy(self.descriptor)}]},
                    'neighbor': {'project_id': 'project-exact', 'environment_id': 'env-other',
                                 'state': 'ready', 'machines': []}}
        bridge.startup.document(project / 'vz.json', {'schema_version': 1, 'project_id': 'project-exact'})
        bridge.startup.document(evidence / 'topology.json', topology)
        self.harness = SimpleNamespace(root=root, evidence=evidence, cli=cli,
            env={'PATH': '/usr/bin:/bin', 'VZ_RUNTIME_DAEMON_SOCKET': str(root / 'daemon.sock')},
            record=SimpleNamespace(canaries=[], receipts=[]))
        self.pins = {p: bridge.startup.digest(Path(p)) for p in bridge.required_source_paths() + [str(cli)]}
        self.output = evidence / 'process-observer'
        self.observer = bridge.Observer(self.harness, self.descriptor, self.output, self.pins)
        self.inspected = {'Id': CID, 'HostConfig': {'CgroupParent': ''},
                          'State': {'Running': True, 'Pid': 321, 'StartedAt': '2026-09-06T01:02:03Z'}}
        self.policy = {'CgroupDriver': 'cgroupfs', 'CgroupVersion': '2'}
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.executor = stack.enter_context(patch.object(bridge.startup, 'execute_observer',
            return_value=SimpleNamespace(returncode=0, stdout=b'raw guest frames\n', stderr=b'')))
        self.parser = stack.enter_context(patch.object(bridge.process, 'validate', side_effect=lambda raw, **kwargs:
            {'container_id': kwargs['inspected']['Id'], 'phase': kwargs['phase'], 'boot_id': BOOT,
             'raw_sha256': bridge.driver.sha256(raw), 'previous_seen': kwargs['previous'] is not None}))

    def capture(self, observer=None, *, phase='running', previous=None, label='service-running', expected_boot_id=None):
        return (observer or self.observer).capture(self.inspected, phase=phase, previous=previous,
            engine_policy=self.policy, label=label, expected_boot_id=expected_boot_id)

    def replace(self, name, change):
        path = self.output / name
        value = bridge.document(path)
        change(value)
        path.write_bytes(bridge.evidence.canonical(value))

    def test_exact_public_exec_and_separate_original_ledger(self):
        proof = self.capture()
        self.assertEqual(self.harness.record.receipts, [])
        self.assertEqual(len(self.observer.record.receipts), 1)
        args, kwargs = self.executor.call_args
        self.assertEqual(args[0][:11], [str(self.harness.cli), 'exec', '--environment', 'env-exact',
            '--machine', 'machine-exact', '--no-stdin', '--timeout', '30', '--', '/bin/busybox'])
        self.assertEqual(kwargs['env'], self.harness.env)
        self.assertEqual(kwargs['executable'], str(self.harness.cli))
        self.assertEqual(str(kwargs['cwd']), str(self.harness.root / 'project'))
        row = self.observer.record.receipts[0]
        self.assertEqual(proof['finished_unix_ns'], row['started_unix_ns'] + row['elapsed_ns'])
        self.assertEqual(proof['owner'], self.descriptor['owner'])
        self.assertEqual(proof['observation']['boot_id'], BOOT)

    def test_replay_does_not_dispatch_or_write(self):
        proof = self.capture()
        before = {p.name: p.read_bytes() for p in self.output.iterdir()}
        calls = self.executor.call_count
        replay = self.observer.replay()
        with patch.object(bridge.startup, 'write', side_effect=AssertionError('write during replay')):
            self.assertEqual(self.capture(replay), proof)
            self.assertEqual(replay.assert_complete()['command_count'], 1)
        self.assertEqual(self.executor.call_count, calls)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.output.iterdir()})

    def test_prefix_then_final_replay_and_previous_raw_binding(self):
        original = self.capture()
        first = self.observer.replay()
        self.capture(first)
        first.assert_complete()
        stopped = self.capture(phase='stopped', previous=original, label='service-stopped', expected_boot_id=BOOT)
        self.assertTrue(stopped['observation']['previous_seen'])
        last = self.observer.replay()
        replayed = self.capture(last)
        self.assertEqual(self.capture(last, phase='stopped', previous=replayed,
                                     label='service-stopped', expected_boot_id=BOOT), stopped)
        self.assertEqual(last.assert_complete()['command_count'], 2)

    def test_wrong_previous_machine_or_unrecorded_birth_rejected_before_dispatch(self):
        original = self.capture()
        for mutate in (lambda p: p['owner'].update(machine_id='foreign'),
                       lambda p: p.update(incarnation_generation=2),
                       lambda p: p.update(command_index=2),
                       lambda p: p.update(phase='stopped'),
                       lambda p: p['observation'].update(boot_id='foreign')):
            previous = copy.deepcopy(original); mutate(previous)
            with self.subTest(previous=previous), self.assertRaises(ValueError):
                self.capture(phase='stopped', previous=previous, label='service-stopped')
        self.assertEqual(self.executor.call_count, 1)

    def test_source_cli_environment_and_topology_drift_rejected(self):
        self.capture()
        self.harness.env['VZ_RUNTIME_DAEMON_SOCKET'] += '-foreign'
        with self.assertRaises(ValueError):
            self.observer.replay()
        self.harness.env['VZ_RUNTIME_DAEMON_SOCKET'] = self.observer.inputs['environment']['VZ_RUNTIME_DAEMON_SOCKET']
        self.harness.cli.chmod(0o700); self.harness.cli.write_bytes(b'changed CLI')
        with self.assertRaises(ValueError):
            self.observer.replay()

    def test_source_pin_inventory_must_include_every_verifier_and_cli(self):
        for selected in self.pins:
            pins = dict(self.pins); del pins[selected]
            with self.subTest(selected=selected), self.assertRaises(ValueError):
                bridge.Observer(self.harness, self.descriptor, self.output.parent / 'other', pins)

    def test_invalid_container_or_engine_policy_rejected_before_intent_and_dispatch(self):
        for policy, inspected in (({'CgroupDriver': 'systemd', 'CgroupVersion': '2'}, self.inspected),
                                  (self.policy, dict(self.inspected, Id='invalid')),
                                  (self.policy, dict(self.inspected, HostConfig={'CgroupParent': '/foreign'}))):
            with self.subTest(policy=policy, inspected=inspected), self.assertRaises(ValueError):
                self.observer.capture(inspected, phase='running', engine_policy=policy)
        self.assertEqual(self.executor.call_count, 0)
        self.assertEqual({p.name for p in self.output.iterdir()}, {'inputs.json'})

    def test_project_topology_and_descriptor_incarnation_are_original_inputs(self):
        self.capture()
        path = self.harness.evidence / 'topology.json'
        topology = bridge.document(path)
        topology['primary']['machines'][0]['incarnation_generation'] = 2
        path.write_bytes(bridge.evidence.canonical(topology))
        with self.assertRaises(ValueError):
            self.observer.replay()

    def test_receipt_stream_and_request_tampering_rejected(self):
        self.capture()
        cases = [('001-service-running.result.json', lambda p: p.update(executable='/bin/true')),
                 ('001-service-running.result.json', lambda p: p.update(effects_uncertain=True)),
                 ('001-service-running.result.json', lambda p: p.update(capture_complete=False)),
                 ('001-service-running.result.json', lambda p: p.update(elapsed_ns=bridge.MAX_ELAPSED_NS + 1)),
                 ('001-service-running.result.json', lambda p: p.update(stdout_sha256='0' * 64)),
                 ('001-service-running.result.json', lambda p: p.update(retained_stdout_bytes=True)),
                 ('001-service-running.result.json', lambda p: p.update(index=True)),
                 ('001-service-running.intent.json', lambda p: p.update(started_unix_ns=True)),
                 ('001-service-running.intent.json', lambda p: p.update(index=True)),
                 ('request-001.json', lambda p: p.update(expected_boot_id='foreign')),
                 ('proof-001.json', lambda p: p.update(incarnation_id='foreign'))]
        for name, change in cases:
            path = self.output / name; original = path.read_bytes()
            self.replace(name, change)
            try:
                with self.subTest(name=name, changed=bridge.document(path)), self.assertRaises(ValueError):
                    self.capture(self.observer.replay())
            finally:
                path.write_bytes(original)

    def test_replay_rejects_changed_selected_argv_and_order(self):
        self.capture()
        for kwargs in ({'label': 'other'}, {'phase': 'removed'}, {'expected_boot_id': BOOT}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.capture(self.observer.replay(), **kwargs)

    def test_missing_extra_symlink_and_duplicate_json_evidence_rejected(self):
        self.capture()
        replay = self.observer.replay()
        with self.assertRaises(ValueError):
            replay.assert_complete()
        self.capture(replay)
        extra = self.output / 'unselected.stdout'; extra.write_bytes(b'extra')
        with self.assertRaises(ValueError):
            replay.assert_complete()
        extra.unlink()
        original = self.output / '001-service-running.stdout'
        backup = self.output.parent / 'saved-stream'; original.rename(backup); original.symlink_to(backup)
        with self.assertRaises(OSError):
            self.capture(self.observer.replay())
        original.unlink(); backup.rename(original)
        request = self.output / 'request-001.json'
        request.write_bytes(b'{"phase":"running","phase":"running"}\n')
        with self.assertRaises(ValueError):
            self.capture(self.observer.replay())

    def test_failed_exec_retains_uncertainty_and_cannot_certify(self):
        self.executor.return_value = SimpleNamespace(returncode=-15, stdout=b'partial', stderr=b'failed')
        with self.assertRaises(ValueError):
            self.capture()
        self.assertTrue(self.observer.record.receipts[0]['effects_uncertain'])
        self.assertEqual((self.output / '001-service-running.stdout').read_bytes(), b'partial')
        with self.assertRaises(ValueError):
            self.capture(self.observer.replay())

    def test_guest_diagnostics_and_parser_failure_retained_without_proof(self):
        self.executor.return_value = SimpleNamespace(returncode=0, stdout=b'frames', stderr=b'diagnostic')
        with self.assertRaises(ValueError):
            self.capture()
        self.assertFalse((self.output / 'proof-001.json').exists())
        self.assertEqual((self.output / '001-service-running.stderr').read_bytes(), b'diagnostic')

    def test_parser_failure_retains_complete_raw_streams_without_positive_proof(self):
        self.parser.side_effect = ValueError('kernel semantics failed')
        with self.assertRaises(ValueError):
            self.capture()
        self.assertFalse((self.output / 'proof-001.json').exists())
        self.assertEqual((self.output / '001-service-running.stdout').read_bytes(), b'raw guest frames\n')


if __name__ == '__main__':
    unittest.main()
