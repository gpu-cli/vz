"""Offline real-parser bridge tests; no guest, CLI, or runtime is launched."""
import copy
from contextlib import ExitStack
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_runtime_audit_evidence as bridge
import test_linux_docker_runtime_audit_capture as frames


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.umask = os.umask(0o077)
        self.addCleanup(os.umask, self.umask)
        temporary = tempfile.TemporaryDirectory(prefix='vz-audit-bridge-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        project, evidence, cli = root / 'project', root / 'evidence', root / 'vz'
        original, staged = root / 'original', root / 'prefix/linux/developer'
        for path in (project, evidence, original, staged):
            path.mkdir(mode=0o700, parents=True)
        cli.write_bytes(b'not executed: original staged CLI\n')
        cli.chmod(0o500)
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
        for directory in (original, staged):
            (directory / 'youki').write_bytes(b'not executed: native-audited runtime bytes\n')
            (directory / 'youki').chmod(0o500)
            bridge.startup.document(directory / 'version.json', {
                'profile': 'developer', 'sha256_youki': bridge.startup.digest(directory / 'youki')})
        self.runtime_sha = bridge.startup.digest(original / 'youki')
        selected = {'owner': self.descriptor['owner'], 'public_status': {'docker_context': self.descriptor},
                    'runtime_evidence': {'youki_sha256': self.runtime_sha},
                    'configuration': {'artifact': {'youki_sha256': self.runtime_sha,
                        'version_sha256': bridge.startup.digest(original / 'version.json')}}}
        bridge.startup.document(evidence / 'machine-exact-runtime-binding.json', selected)
        self.harness = SimpleNamespace(root=root, evidence=evidence, cli=cli, prefix=root / 'prefix',
            env={'PATH': '/usr/bin:/bin', 'VZ_RUNTIME_DAEMON_SOCKET': str(root / 'daemon.sock')},
            info={'developer_bundle': str(original), 'inputs': {
                str(path): bridge.startup.digest(path) for path in original.iterdir()}},
            staged_inputs={str(path): bridge.startup.digest(path) for path in staged.iterdir()},
            record=SimpleNamespace(canaries=[], receipts=[]))
        self.pins = {p: bridge.startup.digest(Path(p)) for p in bridge.required_source_paths() + [str(cli)]}
        self.output = evidence / 'runtime-audit'
        self.session = bridge.Session(self.harness, self.descriptor, self.output, self.pins, frames.SESSION)
        self.events = frames.journal()
        self.responses = [self.snapshot(), self.snapshot(self.events),
                          frames.chunk_raw(self.snapshot(self.events), self.events, 0), self.snapshot(self.events)]
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.executor = stack.enter_context(patch.object(bridge.startup, 'execute_observer', side_effect=self.execute))

    def snapshot(self, events=b''):
        return frames.snapshot_raw(events).replace(frames.RUNTIME.encode(), self.runtime_sha.encode())

    def execute(self, *_args, **_kwargs):
        return SimpleNamespace(returncode=0, stdout=self.responses.pop(0), stderr=b'')

    def complete(self):
        self.session.enroll()
        return self.session.capture()

    def replace(self, name, change):
        path = self.output / name
        value = bridge.document(path)
        change(value)
        path.write_bytes(bridge.evidence.canonical(value))

    def test_real_parser_exact_machine_runtime_boot_and_separate_ledger(self):
        proof = self.complete()
        self.assertEqual(self.executor.call_count, 4)
        self.assertEqual(self.harness.record.receipts, [])
        self.assertEqual(proof['journal']['record_count'], 2)
        self.assertEqual(proof['journal']['invocation_count'], 1)
        self.assertNotIn('invocations', proof['journal'])
        self.assertEqual(len(self.session.validated_journal['invocations']), 1)
        self.assertEqual(proof['owner'], self.descriptor['owner'])
        self.assertEqual(proof['boot_id'], frames.BOOT)
        self.assertEqual(proof['runtime_sha256'], self.runtime_sha)
        self.assertFalse(proof['normal_Up_startup_invocations_covered'])
        self.assertFalse(proof['public_Stop_invocations_covered'])
        self.assertFalse(proof['full_process_absence_certified'])
        for call in self.executor.call_args_list:
            args, kwargs = call
            self.assertEqual(args[0][:13], [str(self.harness.cli), 'exec', '--environment', 'env-exact',
                '--machine', 'machine-exact', '--no-stdin', '--timeout', '30', '--', '/bin/busybox', 'sh', '-c'])
            self.assertIn('/mnt/linux-bin/youki', args[0][13])
            self.assertEqual(kwargs['env'], self.harness.env)
            self.assertEqual(str(kwargs['cwd']), str(self.harness.root / 'project'))
        for receipt, command in zip(self.session.record.receipts[1:], proof['commands']):
            self.assertEqual(command['finished_unix_ns'], receipt['started_unix_ns'] + receipt['elapsed_ns'])
            self.assertEqual(receipt['termination_scope'], 'observer_pid_only')
        self.session.assert_enrolled_certain()

    def test_inert_replay_consumes_original_records_without_dispatch_or_writes(self):
        proof = self.complete()
        before = {p.name: p.read_bytes() for p in self.output.iterdir()}
        with ExitStack() as stack:
            for module, name in ((bridge.startup, 'write'), (bridge.startup, 'document'),
                                 (bridge.startup, 'execute_observer'), (bridge.startup.subprocess, 'Popen')):
                stack.enter_context(patch.object(module, name, side_effect=AssertionError('replay side effect')))
            repeated, journal = self.session.replay()
            self.assertEqual(repeated, proof)
            self.assertEqual(journal, self.session.validated_journal)
            replay = bridge.ReplaySession(self.session)
            self.assertEqual(replay.count, 0)
            with self.assertRaises(ValueError):
                replay.assert_complete()
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.output.iterdir()})

    def test_normal_nonzero_enrollment_is_sticky_uncertainty_without_adoption(self):
        self.executor.side_effect = None
        self.executor.return_value = SimpleNamespace(returncode=1, stdout=b'partially created', stderr=b'')
        with self.assertRaises(ValueError):
            self.session.enroll()
        self.assertFalse(self.session.record.receipts[0]['effects_uncertain'])
        self.assertTrue(self.session.enrollment_uncertain)
        self.assertFalse((self.output / 'enrollment.json').exists())
        for action in (self.session.enroll, self.session.assert_enrolled_certain, self.session.capture):
            with self.assertRaises(ValueError):
                action()
        self.assertEqual(self.executor.call_count, 1)

    def test_successful_exec_with_invalid_enrollment_retains_uncertainty(self):
        self.responses[0] = self.snapshot(self.events)
        with self.assertRaises(ValueError):
            self.session.enroll()
        self.assertTrue(self.session.enrollment_uncertain)
        self.assertEqual((self.output / '001-enroll.stdout').read_bytes(), self.snapshot(self.events))

    def test_enrollment_readback_failure_restores_sticky_uncertainty(self):
        original = bridge.startup.document
        def corrupt(path, value):
            if path.name == 'enrollment.json':
                value = dict(value, incarnation_id='foreign')
            original(path, value)
        with patch.object(bridge.startup, 'document', side_effect=corrupt), self.assertRaises(ValueError):
            self.session.enroll()
        self.assertTrue(self.session.enrollment_uncertain)

    def test_capture_proof_readback_is_checked_before_positive_return(self):
        self.session.enroll()
        original = bridge.startup.document
        def corrupt(path, value):
            if path.name == 'capture.json':
                value = dict(value, full_process_absence_certified=True)
            original(path, value)
        with patch.object(bridge.startup, 'document', side_effect=corrupt), self.assertRaises(ValueError):
            self.session.capture()
        self.assertFalse(self.session.capture_complete)
        with self.assertRaises(ValueError):
            self.session.assert_enrolled_certain()

    def test_changed_runtime_cli_env_topology_and_authenticated_binding_rejected(self):
        self.complete()
        paths = [self.harness.cli, self.harness.prefix / 'linux/developer/youki',
                 Path(self.harness.info['developer_bundle']) / 'version.json',
                 self.harness.evidence / 'topology.json',
                 self.harness.evidence / 'machine-exact-runtime-binding.json',
                 self.harness.root / 'project/vz.json']
        for path in paths:
            original = path.read_bytes()
            path.chmod(0o600)
            path.write_bytes(original + b' ')
            try:
                with self.subTest(path=path), self.assertRaises(ValueError):
                    self.session.replay()
            finally:
                path.write_bytes(original)
        self.harness.env['VZ_RUNTIME_DAEMON_SOCKET'] += '-foreign'
        with self.assertRaises(ValueError):
            self.session.replay()

    def test_every_source_pin_required_and_invalid_session_rejected_before_creation(self):
        for selected in self.pins:
            pins = dict(self.pins)
            del pins[selected]
            with self.subTest(selected=selected), self.assertRaises(ValueError):
                bridge.Session(self.harness, self.descriptor, self.output.parent / 'other', pins, frames.SESSION)
        for session_id in ('short', 'a' * 63 + ';', True):
            with self.assertRaises(ValueError):
                bridge.Session(self.harness, self.descriptor, self.output.parent / 'other', self.pins, session_id)
        self.assertFalse((self.output.parent / 'other').exists())

    def test_resealed_receipts_cannot_change_source_selected_argv_or_types(self):
        self.complete()
        cases = [('001-enroll.intent.json', lambda row: row.update(index=True)),
                 ('001-enroll.intent.json', lambda row: row.update(started_unix_ns=True)),
                 ('002-snapshot-before.result.json', lambda row: row.update(index=True)),
                 ('002-snapshot-before.result.json', lambda row: row.update(exit_code=False)),
                 ('002-snapshot-before.result.json', lambda row: row.update(retained_stdout_bytes=True)),
                 ('002-snapshot-before.result.json', lambda row: row.update(effects_uncertain=True)),
                 ('002-snapshot-before.result.json', lambda row: row.update(capture_complete=False)),
                 ('002-snapshot-before.result.json', lambda row: row.update(elapsed_ns=bridge.MAX_ELAPSED_NS + 1)),
                 ('002-snapshot-before.result.json', lambda row: row.update(executable='/bin/true')),
                 ('002-snapshot-before.result.json', lambda row: row.update(stdout_sha256='0' * 64)),
                 ('capture.json', lambda row: row.update(incarnation_generation=2)),
                 ('enrollment.json', lambda row: row.update(boot_id='foreign'))]
        for name, change in cases:
            original = (self.output / name).read_bytes()
            self.replace(name, change)
            try:
                with self.subTest(name=name), self.assertRaises(ValueError):
                    self.session.replay()
            finally:
                (self.output / name).write_bytes(original)
        for name in ('001-enroll.intent.json', '001-enroll.result.json'):
            self.replace(name, lambda row: row['argv'].__setitem__(5, 'foreign-machine'))
        with self.assertRaises(ValueError):
            self.session.replay()

    def test_failed_capture_boot_change_and_partial_chunk_withhold_stop(self):
        self.session.enroll()
        self.responses[0] = self.responses[0].replace(frames.BOOT.encode(), b'd1234567-1111-2222-3333-0123456789ab')
        with self.assertRaises(ValueError):
            self.session.capture()
        self.assertFalse(self.session.capture_complete)
        with self.assertRaises(ValueError):
            self.session.assert_enrolled_certain()
        with self.assertRaises(ValueError):
            self.session.capture()
        self.assertEqual(self.executor.call_count, 2)

    def test_raw_journal_is_parsed_even_when_all_transport_hashes_match(self):
        malformed = self.events.split(b'\n')[0] + b'\n'
        snapshot = self.snapshot(malformed)
        self.responses = [self.snapshot(), snapshot, frames.chunk_raw(snapshot, malformed, 0), snapshot]
        with self.assertRaises(ValueError):
            self.complete()
        self.assertFalse(self.session.capture_complete)
        self.assertFalse((self.output / 'capture.json').exists())
        self.assertEqual(self.executor.call_count, 4)

    def test_multichunk_full_journal_replay_and_retained_stream_bound(self):
        templates = [bridge.evidence.parse(line) for line in self.events.splitlines()]
        records = []
        for invocation in range(1600):
            for offset, template in enumerate(templates):
                row = dict(template, sequence=invocation * 2 + offset + 1,
                           invocation_id='%d:20:%d' % (invocation + 10, invocation * 2 + 30),
                           pid=invocation + 10, monotonic_ns=invocation * 2 + offset + 30,
                           wall_time_ns=invocation * 2 + offset + 101)
                records.append(bridge.evidence.canonical(row))
        events = b''.join(records)
        self.assertGreater(len(events), bridge.probe.CHUNK_SIZE)
        snapshot = self.snapshot(events)
        count = (len(events) + bridge.probe.CHUNK_SIZE - 1) // bridge.probe.CHUNK_SIZE
        self.responses = [self.snapshot(), snapshot] + [
            frames.chunk_raw(snapshot, events, index) for index in range(count)] + [snapshot]
        proof = self.complete()
        self.assertEqual(proof['journal']['record_count'], 3200)
        self.assertEqual(proof['journal']['invocation_count'], 1600)
        self.assertEqual(proof['journal']['journal_sha256'], bridge.driver.sha256(events))
        self.assertEqual(self.executor.call_count, count + 3)
        self.assertEqual(self.session.replay()[0], proof)
        self.assertTrue(all(path.stat().st_size <= bridge.startup.LIMIT for path in self.output.iterdir()))

    def test_nonquiescent_final_snapshot_is_not_retried_or_certified(self):
        self.responses[-1] = self.snapshot(self.events + b' ')
        with self.assertRaises(ValueError):
            self.complete()
        self.assertEqual(self.executor.call_count, 4)
        self.assertFalse(self.session.capture_complete)
        with self.assertRaises(ValueError):
            self.session.assert_enrolled_certain()

    def test_command_windows_cannot_be_resealed_out_of_order(self):
        self.complete()
        first = bridge.document(self.output / '001-enroll.intent.json')['started_unix_ns']
        for name in ('002-snapshot-before.intent.json', '002-snapshot-before.result.json'):
            self.replace(name, lambda row: row.update(started_unix_ns=first))
        with self.assertRaises(ValueError):
            self.session.replay()

    def test_extra_missing_symlink_and_duplicate_documents_rejected(self):
        self.complete()
        extra = self.output / 'unselected.stdout'
        extra.write_bytes(b'unselected')
        with self.assertRaises(ValueError):
            self.session.replay()
        extra.unlink()
        path = self.output / '003-chunk-0000.stdout'
        saved = self.output.parent / 'saved.stdout'
        path.rename(saved)
        with self.assertRaises(FileNotFoundError):
            self.session.replay()
        path.symlink_to(saved)
        with self.assertRaises(OSError):
            self.session.replay()
        path.unlink()
        saved.rename(path)
        (self.output / 'capture.json').write_bytes(b'{"schema_version":1,"schema_version":1}\n')
        with self.assertRaises(ValueError):
            self.session.replay()

    def test_nonempty_stderr_and_uncertain_observer_fail_before_parser(self):
        self.executor.side_effect = None
        self.executor.return_value = SimpleNamespace(returncode=-15, stdout=b'partial', stderr=b'diagnostic')
        with self.assertRaises(ValueError):
            self.session.enroll()
        self.assertTrue(self.session.record.receipts[0]['effects_uncertain'])
        self.assertTrue(self.session.enrollment_uncertain)
        self.assertEqual((self.output / '001-enroll.stderr').read_bytes(), b'diagnostic')


if __name__ == '__main__':
    unittest.main()
