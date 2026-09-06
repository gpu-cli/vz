"""Actual local pipe transport plus mocked Docker ownership; no VM or daemon."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_ssh_cache_capture as capture


def archive(payload=b'public-cache'):
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode='w', format=tarfile.USTAR_FORMAT) as tar:
        row = tarfile.TarInfo('cache.db'); row.size = len(payload)
        tar.addfile(row, io.BytesIO(payload))
    return out.getvalue()


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.root.chmod(0o700)

    def transport(self, source, *, limit=1024*1024, timeout=5, stderr_limit=128):
        path = self.root / ('raw-' + str(len(list(self.root.iterdir()))))
        with path.open('x+b') as output, (path.with_suffix('.stderr')).open('x+b') as error:
            try:
                result = capture.capture_process([sys.executable, '-c', source], executable=sys.executable,
                    environment={'PATH': '/usr/bin:/bin'}, cwd=self.root, descriptor=output.fileno(),
                    stderr_descriptor=error.fileno(), canaries=(b'private-canary',), maximum=limit,
                    timeout=timeout, stderr_limit=stderr_limit)
            except BaseException as failure:
                failure.test_path = path
                raise
        return path, result

    def test_actual_stream_hashes_every_byte_without_stdout_buffer(self):
        path, row = self.transport("import os\nfor i in range(8): os.write(1,b'x'*65536)")
        self.assertEqual(row['size'], 8*65536)
        self.assertEqual(row['sha256'], hashlib.sha256(b'x'*(8*65536)).hexdigest())
        self.assertEqual(path.stat().st_size, row['size'])
        self.assertTrue(row['capture_complete'] and row['owned_process_reaped'])

    def test_actual_stdout_limit_retains_bounded_quarantine_and_reaps(self):
        with self.assertRaises(capture.CaptureError) as raised:
            self.transport("import os,time\nos.write(1,b'x'*65536);time.sleep(30)", limit=100)
        row = raised.exception.capture_observation
        self.assertTrue(row['owned_process_reaped'])
        self.assertLessEqual(raised.exception.test_path.stat().st_size, 100)

    def test_actual_stderr_limit_and_nonzero_exit_are_not_success(self):
        for source in ("import os,time\nos.write(2,b'x'*1024);time.sleep(30)",
                       "import os,sys\nos.write(1,b'partial');sys.exit(7)"):
            with self.subTest(source=source), self.assertRaises(capture.CaptureError) as raised:
                self.transport(source)
            self.assertTrue(raised.exception.capture_observation['owned_process_reaped'])
            self.assertNotIn('partial', str(raised.exception))

    def test_actual_timeout_reaps_owned_process_only(self):
        with self.assertRaises(capture.CaptureError) as raised:
            self.transport('import time;time.sleep(30)', timeout=.1)
        row = raised.exception.capture_observation
        self.assertTrue(row['owned_process_reaped'])
        self.assertLess(row['elapsed_ns'], 5*10**9)
        with self.assertRaises(ProcessLookupError): os.kill(row['pid'], 0)

    def test_actual_nonzero_reaped_exit_never_signals_reusable_group_number(self):
        with patch.object(capture.os, 'killpg', wraps=os.killpg) as kill:
            with self.assertRaises(capture.CaptureError) as raised:
                self.transport("import os,sys;os.write(1,b'complete');sys.exit(7)")
        kill.assert_not_called()
        row = raised.exception.capture_observation
        self.assertEqual(row['exit_code'], 7)
        self.assertTrue(row['owned_process_reaped'])
        self.assertIsNone(row['pending_process_pid'])

    def test_reaped_handle_never_checks_or_signals_current_pid_owner(self):
        process = SimpleNamespace(pid=123456789, returncode=7, wait=Mock())
        with patch.object(capture.os, 'getpgid') as group, patch.object(capture.os, 'getsid') as session, \
                patch.object(capture.os, 'killpg') as kill:
            self.assertFalse(capture.terminate_owned(process, process.pid))
        group.assert_not_called(); session.assert_not_called(); kill.assert_not_called()
        process.wait.assert_not_called()

    def test_changed_popen_pid_refuses_signal_before_os_lookup(self):
        process = SimpleNamespace(pid=123456789, returncode=None, wait=Mock())
        with patch.object(capture.os, 'getpgid') as group, patch.object(capture.os, 'killpg') as kill:
            with self.assertRaises(capture.CaptureError): capture.terminate_owned(process, 123456788)
        group.assert_not_called(); kill.assert_not_called(); process.wait.assert_not_called()

    def test_actual_unreaped_leader_with_pipe_holder_is_killed_and_reaped(self):
        source = "import os,time\nos.fork()\ntime.sleep(30)"
        with patch.object(capture.os, 'killpg', wraps=os.killpg) as kill:
            with self.assertRaises(capture.CaptureError) as raised:
                self.transport(source, timeout=.3)
        row = raised.exception.capture_observation
        self.assertEqual(str(raised.exception), 'capture_deadline')
        self.assertTrue(row['owned_process_reaped'])
        self.assertEqual(row['exit_code'], -capture.signal.SIGKILL)
        kill.assert_called_once_with(row['pid'], capture.signal.SIGKILL)

    def test_unavailable_group_or_session_identity_never_signals_or_reaps(self):
        for lookup in ('getpgid', 'getsid'):
            process = SimpleNamespace(pid=123456789, returncode=None, wait=Mock())
            with patch.object(capture.os, 'getpgid', return_value=process.pid), \
                    patch.object(capture.os, 'getsid', return_value=process.pid), \
                    patch.object(capture.os, lookup, side_effect=ProcessLookupError()), \
                    patch.object(capture.os, 'killpg') as kill:
                with self.assertRaises(ProcessLookupError): capture.terminate_owned(process, process.pid)
            kill.assert_not_called(); process.wait.assert_not_called()

    def test_identity_drift_refuses_signal_retains_original_error_and_pending_handle(self):
        original_group = os.getpgid
        failure = None
        try:
            with patch.object(capture.os, 'getpgid', side_effect=lambda pid: original_group(pid)+1), \
                    patch.object(capture.os, 'killpg', wraps=os.killpg) as kill:
                with self.assertRaises(capture.CaptureError) as raised:
                    self.transport('import time;time.sleep(30)', timeout=.1)
                failure = raised.exception
                kill.assert_not_called()
            self.assertEqual(str(failure), 'capture_deadline')
            self.assertEqual(failure.capture_cleanup_error, 'CaptureError')
            self.assertFalse(failure.capture_observation['owned_process_reaped'])
            self.assertIsNone(failure.capture_pending_process.returncode)
            self.assertEqual(failure.capture_pending_pid, failure.capture_pending_process.pid)
        finally:
            if failure is not None:
                capture.terminate_owned(failure.capture_pending_process, failure.capture_pending_pid)

    def test_signal_denial_keeps_direct_child_handle_for_authenticated_reconciliation(self):
        failure = None
        try:
            with patch.object(capture.os, 'killpg', side_effect=PermissionError('denied')):
                with self.assertRaises(capture.CaptureError) as raised:
                    self.transport('import time;time.sleep(30)', timeout=.1)
                failure = raised.exception
            self.assertEqual(str(failure), 'capture_deadline')
            self.assertEqual(failure.capture_cleanup_error, 'PermissionError')
            self.assertIsNone(failure.capture_pending_process.returncode)
            self.assertFalse(failure.capture_observation['owned_process_reaped'])
        finally:
            if failure is not None:
                capture.terminate_owned(failure.capture_pending_process, failure.capture_pending_pid)

    def test_actual_split_stdout_and_stderr_canaries_stay_private(self):
        for fd in (1, 2):
            source = "import os,time\nos.write(%d,b'private-');time.sleep(.03);os.write(%d,b'canary')" % (fd, fd)
            with self.subTest(fd=fd), self.assertRaises(ValueError) as raised:
                self.transport(source)
            self.assertNotIn('private-canary', str(raised.exception))
            self.assertTrue(raised.exception.capture_observation['owned_process_reaped'])

    def fake(self):
        evidence = self.root / 'evidence'; evidence.mkdir(mode=0o700)
        config = self.root / 'docker'; config.mkdir(mode=0o700)
        (config/'config.json').write_text('{}\n')
        descriptor = {'owner': {'project_id': 'prj', 'environment_id': 'env', 'machine_id': 'mch'},
                      'name': 'exact-context', 'endpoint': 'unix:///private/socket', 'engine_id': 'exact-engine'}
        volume = {'Name': 'exact-volume', 'Driver': 'local', 'Scope': 'local',
                  'Labels': {capture.buildkit.LABEL: 'token'}, 'Options': None,
                  'CreatedAt': '2026-09-06T00:00:00Z', 'Mountpoint': '/var/lib/docker/volumes/exact/_data'}
        builder = SimpleNamespace(descriptor=descriptor, mapping={'container_id': 'a'*64, 'image_id': 'sha256:'+'b'*64},
            role='source', token='token', identity_sha256='c'*64, container_name='exact-container',
            volume_name='exact-volume', volume=volume, ownership={'role': 'source'}, tag='exact:builder', registered=False,
            harness=SimpleNamespace(evidence=evidence, config=config, env={'PATH': '/usr/bin:/bin'},
                info={'clients': {'docker': {'canonical': sys.executable, 'sha256': 'f'*64}}}))
        selected = capture.Capture(builder, (b'private-canary',), self.root/'private', evidence/'capture')
        selected.local_guard = Mock(side_effect=lambda: (selected.private.check(), selected.evidence.check()))
        selected.guard = Mock()
        state = {'Running': False, 'Status': 'exited', 'Pid': 0, 'ExitCode': 1, 'Error': '',
                 'Paused': False, 'Restarting': False, 'OOMKilled': False, 'Dead': False,
                 'StartedAt': '2026-09-06T00:00:00Z', 'FinishedAt': '2026-09-06T00:00:02Z'}
        stopped = {'Id': 'a'*64, 'Image': 'sha256:'+'b'*64, 'Name': '/exact-container',
                   'Config': {'Labels': {capture.buildkit.LABEL: 'token'}, 'Env': capture.buildkit.ENV,
                              'Entrypoint': ['/usr/bin/buildkitd'], 'Cmd': capture.buildkit.FLAGS},
                   'HostConfig': {'Runtime': 'youki', 'Privileged': True, 'Init': True, 'CgroupnsMode': 'private',
                                  'NetworkMode': 'bridge', 'RestartPolicy': {'Name': 'no'}},
                   'State': state, 'RestartCount': 0,
                   'Mounts': [{'Type': 'volume', 'Name': builder.volume_name, 'Destination': '/var/lib/buildkit',
                               'Source': volume['Mountpoint'], 'RW': True}]}
        proof = {'container_id': stopped['Id'], 'started_at': state['StartedAt'], 'finished_at': state['FinishedAt'],
                 'owner': descriptor['owner'], 'context': descriptor['name'], 'role': 'source',
                 'identity_sha256': builder.identity_sha256, 'engine_id': descriptor['engine_id'],
                 'source_commit': capture.shutdown.SOURCE_COMMIT, 'buildkitd_sha256': capture.shutdown.DAEMON_SHA256,
                 'exit_code': 1, 'signal': 'SIGTERM',
                 'scope': 'PINNED_BUILDKIT_ONE_SIGTERM_NORMAL_EXIT_NOT_FILESYSTEM_CLOSURE',
                 'engine_since': '2026-09-06T00:00:01Z', 'engine_until': '2026-09-06T00:00:03Z'}
        return selected, stopped, proof

    @staticmethod
    def captured(payload):
        def perform(*args, **kwargs):
            os.write(kwargs['descriptor'], payload); os.fsync(kwargs['descriptor'])
            return {'pid': 123, 'exit_code': 0, 'elapsed_ns': 1, 'size': len(payload),
                    'sha256': hashlib.sha256(payload).hexdigest(), 'capture_complete': True,
                    'owned_process_reaped': True}
        return perform

    def test_actual_scanner_then_exclusive_promotion_and_same_owner_three_guards(self):
        selected, stopped, proof = self.fake()
        payload = archive()
        with patch.object(capture, 'capture_process', side_effect=self.captured(payload)) as dispatch:
            result = selected.run(stopped, proof)
        self.assertEqual(selected.guard.call_count, 3)
        self.assertEqual((selected.evidence.path/'cache.tar').read_bytes(), payload)
        self.assertFalse((selected.private.path/'cache.quarantine.tar').exists())
        self.assertEqual((selected.evidence.path/'cache.tar').stat().st_nlink, 1)
        self.assertTrue(result['capture']['archive_published'])
        self.assertFalse(result['builder_restarted'])
        self.assertEqual(dispatch.call_args.args[0][-3:], ['cp', 'a'*64+':/var/lib/buildkit/.', '-'])
        self.assertTrue((selected.evidence.path/'capture.result.json').is_file())
        with self.assertRaises(capture.CaptureError): selected.run(stopped, proof)

    def test_nested_private_canary_never_promoted_and_failure_contains_no_bytes(self):
        selected, stopped, proof = self.fake()
        payload = archive(b'private-canary')
        with patch.object(capture, 'capture_process', side_effect=self.captured(payload)):
            with self.assertRaises(ValueError): selected.run(stopped, proof)
        self.assertEqual((selected.private.path/'cache.quarantine.tar').read_bytes(), payload)
        self.assertFalse((selected.evidence.path/'cache.tar').exists())
        self.assertNotIn(b'private-canary', b''.join(p.read_bytes() for p in selected.evidence.path.iterdir()))

    def test_stop_pid_lifetime_owner_and_volume_rejected_before_capture(self):
        selected, stopped, proof = self.fake()
        for field, value in (('Pid', 1), ('Running', True), ('ExitCode', 0), ('FinishedAt', '2026-09-06T00:00:04Z')):
            changed = copy.deepcopy(stopped); changed['State'][field] = value
            with self.subTest(field=field), self.assertRaises(capture.CaptureError):
                capture.stopped_binding(changed, proof, selected.owner)
        wrong = copy.deepcopy(proof); wrong['owner']['machine_id'] = 'foreign'
        with self.assertRaises(capture.CaptureError): capture.stopped_binding(stopped, wrong, selected.owner)
        changed = copy.deepcopy(stopped); changed['Mounts'][0]['Name'] = 'foreign'
        with self.assertRaises(capture.CaptureError): capture.stopped_binding(changed, proof, selected.owner)
        selected.private.release(); selected.evidence.release()

    def test_post_capture_guard_failure_retains_quarantine(self):
        selected, stopped, proof = self.fake()
        selected.guard.side_effect = [None, capture.CaptureError('owner_changed')]
        with patch.object(capture, 'capture_process', side_effect=self.captured(archive())):
            with self.assertRaises(capture.CaptureError): selected.run(stopped, proof)
        self.assertTrue((selected.private.path/'cache.quarantine.tar').is_file())
        self.assertFalse((selected.evidence.path/'cache.tar').exists())

    def test_archive_replacement_after_scan_is_not_promoted(self):
        selected, stopped, proof = self.fake()
        original = capture.cache.scan
        def replace(path, **kwargs):
            row = original(path, **kwargs)
            path.write_bytes(archive(b'changed'))
            return row
        with patch.object(capture, 'capture_process', side_effect=self.captured(archive())), \
                patch.object(capture.cache, 'scan', side_effect=replace):
            with self.assertRaises(capture.CaptureError): selected.run(stopped, proof)
        self.assertFalse((selected.evidence.path/'cache.tar').exists())

    def test_existing_destination_is_never_overwritten(self):
        selected, stopped, proof = self.fake()
        (selected.evidence.path/'cache.tar').write_bytes(b'foreign-existing')
        with patch.object(capture, 'capture_process', side_effect=self.captured(archive())):
            with self.assertRaises(FileExistsError): selected.run(stopped, proof)
        self.assertEqual((selected.evidence.path/'cache.tar').read_bytes(), b'foreign-existing')
        self.assertTrue((selected.private.path/'cache.quarantine.tar').is_file())

    def test_guard_rejects_foreign_volume_reference_and_engine_reroute(self):
        selected, stopped, proof = self.fake()
        owner = selected.owner
        records = {
            ('context', owner['descriptor']['name']): {'Name': owner['descriptor']['name'],
                'Endpoints': {'docker': {'Host': owner['descriptor']['endpoint']}}},
            ('container', stopped['Id']): stopped,
            ('image', owner['image_tag']): {'Id': stopped['Image'], 'Config': {'Labels': {capture.buildkit.LABEL: owner['token']}}},
            ('volume', owner['volume_name']): owner['volume']}
        selected.object = Mock(side_effect=lambda kind, name: records[(kind, name)])
        info = {'ID': owner['descriptor']['engine_id'], 'OSType': 'linux', 'Architecture': 'arm64',
                'DefaultRuntime': 'youki', 'Runtimes': {'youki': {'path': '/mnt/linux-bin/youki'}}}
        references = (stopped['Id']+'\n').encode()
        selected.command = Mock(side_effect=lambda args: json.dumps(info).encode() if args[0] == 'info' else references)
        capture.Capture.guard(selected, stopped, proof)
        references += b'foreign-container\n'
        with self.assertRaises(capture.CaptureError): capture.Capture.guard(selected, stopped, proof)
        references = (stopped['Id']+'\n').encode()
        info['ID'] = 'foreign-engine'
        with self.assertRaises(capture.CaptureError): capture.Capture.guard(selected, stopped, proof)
        selected.private.release(); selected.evidence.release()

    def test_incomplete_guard_receipt_prevents_archive_publication(self):
        selected, stopped, proof = self.fake()
        selected.record.receipts.append({'effects_uncertain': True})
        with patch.object(capture, 'capture_process', side_effect=self.captured(archive())):
            with self.assertRaises(capture.CaptureError): selected.run(stopped, proof)
        self.assertFalse((selected.evidence.path/'cache.tar').exists())
        self.assertTrue((selected.private.path/'cache.quarantine.tar').is_file())

    def test_quarantine_beneath_harness_evidence_rejected_before_creating_it(self):
        selected, _, _ = self.fake()
        builder = selected.builder
        selected.private.release(); selected.evidence.release()
        with self.assertRaises(capture.CaptureError):
            capture.Capture(builder, (b'private-canary',), builder.harness.evidence/'private-bad',
                            builder.harness.evidence/'public-new')
        self.assertFalse((builder.harness.evidence/'private-bad').exists())

    def test_injected_write_failure_preserves_exception_and_reaps(self):
        def broken(fd, data):
            # Parent-only patch; child executes a fresh interpreter.
            raise OSError('private-canary')
        with patch.object(capture.os, 'write', side_effect=broken):
            with self.assertRaises(OSError) as raised:
                self.transport("import os,time;os.write(1,b'public');time.sleep(30)")
        self.assertEqual(str(raised.exception), 'private-canary')
        self.assertTrue(raised.exception.capture_observation['owned_process_reaped'])

    def test_dispatch_failure_message_not_serialized_to_evidence(self):
        selected, stopped, proof = self.fake()
        with patch.object(capture, 'capture_process', side_effect=OSError('private-canary')):
            with self.assertRaises(OSError): selected.run(stopped, proof)
        combined = b''.join(p.read_bytes() for p in selected.evidence.path.iterdir())
        self.assertNotIn(b'private-canary', combined)
        failure = json.loads((selected.evidence.path/'capture.failure.json').read_bytes())
        self.assertEqual(failure['error_type'], 'OSError')
        self.assertFalse(failure['archive_published'])


if __name__ == '__main__':
    unittest.main()
