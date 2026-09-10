"""Local config admission tests; no Docker or credential helper dispatch."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

import docker_host_driver as driver


class ManagedConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='vz-driver-config-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.config = self.root / 'machine'
        self.config.mkdir(mode=0o700)
        self.plugins = self.root / 'bootstrap-plugins'
        self.plugins.mkdir(mode=0o700)
        clients = {}
        for name in ('compose', 'buildx'):
            path = self.plugins / ('docker-' + name)
            path.write_bytes(b'not executed')
            path.chmod(0o500)
            clients[name] = {'path': str(path), 'sha256': driver.sha256(path.read_bytes())}
        self.selected = object.__new__(driver.Driver)
        self.selected.inputs = SimpleNamespace(raw={'docker_config': str(self.config), 'clients': clients})
        self.selected.record = SimpleNamespace(run=Mock(side_effect=AssertionError('dispatch')))
        self.legacy = {'currentContext': 'default', 'cliPluginsExtraDirs': [str(self.plugins)]}
        self.managed = dict(self.legacy, auths={}, credHelpers={'vz-managed-file-store.invalid': ''})

    def write(self, value):
        raw = (json.dumps(value, sort_keys=True) + '\n').encode()
        (self.config / 'config.json').write_bytes(raw)
        (self.config / 'config.json').chmod(0o600)
        return raw

    def test_old_no_auth_and_exact_managed_guard_are_admitted_without_dispatch(self):
        for value in (self.legacy, self.managed):
            raw = self.write(value)
            self.assertEqual(self.selected.validate_config(), driver.sha256(raw))
        self.selected.record.run.assert_not_called()

    def test_credentials_other_helpers_and_unknown_settings_rejected(self):
        cases = [('auths', {'registry.invalid': {'auth': 'public-dummy'}}), ('auths', None),
                 ('auths', []), ('credHelpers', {}), ('credHelpers', None),
                 ('credHelpers', {'vz-managed-file-store.invalid': 'osxkeychain'}),
                 ('credHelpers', {'foreign.invalid': ''}),
                 ('credHelpers', {'vz-managed-file-store.invalid': '', 'foreign.invalid': 'pass'}),
                 ('credsStore', ''), ('credsStore', 'osxkeychain'), ('proxies', {}), ('unknown', False)]
        for key, value in cases:
            candidate = copy.deepcopy(self.managed)
            candidate[key] = value
            self.write(candidate)
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.selected.validate_config()
        for key in ('auths', 'credHelpers'):
            candidate = copy.deepcopy(self.managed)
            del candidate[key]
            self.write(candidate)
            with self.assertRaises(ValueError):
                self.selected.validate_config()

    def test_plugin_admission_and_config_immutability_remain_exact(self):
        self.write(self.managed)
        self.selected.config_snapshot = self.selected.validate_config()
        changed = dict(self.managed, currentContext='foreign')
        self.write(changed)
        with self.assertRaisesRegex(ValueError, 'client config changed'):
            self.selected.command(['info'])
        self.selected.record.run.assert_not_called()
        for plugins in (None, [], ['/foreign'], [str(self.plugins), str(self.plugins)]):
            self.write(dict(self.managed, cliPluginsExtraDirs=plugins))
            with self.assertRaises(ValueError):
                self.selected.validate_config()
        self.write(self.managed)
        (self.plugins / 'docker-foreign').write_bytes(b'not executed')
        with self.assertRaisesRegex(ValueError, 'unknown discovery plugin'):
            self.selected.validate_config()

    def test_managed_config_cannot_redirect_via_symlink(self):
        self.write(self.managed)
        linked = self.root / 'linked'
        linked.symlink_to(self.config, target_is_directory=True)
        self.selected.inputs.raw['docker_config'] = str(linked)
        with self.assertRaises(ValueError):
            self.selected.validate_config()


if __name__ == '__main__':
    unittest.main()


class BoundedProcessGroupTests(unittest.TestCase):
    """`execute` must survive Darwin refusing a group that is already dead."""

    def child(self, source: str) -> list:
        return [sys.executable, '-c', source]

    def await_zombie(self, pid: int) -> None:
        """Block until `pid` has exited but has not been reaped.

        `os.waitid(WNOWAIT)` is the direct expression of this and is absent on
        this interpreter, so the state is read where the kernel publishes it.
        Waiting on the observed state keeps the test deterministic; a fixed
        pause would only be a bet on how fast the child exits.
        """
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            state = subprocess.run(['/bin/ps', '-o', 'stat=', '-p', str(pid)],
                                   capture_output=True, text=True).stdout.strip()
            if state.startswith('Z'):
                return
            time.sleep(0.01)
        self.fail(f'child {pid} never became an unreaped zombie')

    def test_a_group_that_died_before_the_kill_keeps_the_reason_it_was_killed(self):
        # The child writes past the retained bound and exits at once, so by the
        # time `execute` kills the group every member is an unreaped zombie and
        # Darwin answers EPERM. The caller must still be told what it asked
        # about -- the output bound -- not how the corpse was disposed of.
        original = driver.collect_output

        def wait_for_the_zombie(process, timeout, limit):
            try:
                return original(process, timeout, limit)
            finally:
                # Block until the child has exited WITHOUT reaping it, so the
                # group is provably all-zombie when the kill lands.
                self.await_zombie(process.pid)

        with patch.object(driver, 'collect_output', wait_for_the_zombie), \
             self.assertRaises(driver.OutputLimitExceeded) as raised:
            driver.execute(self.child("import os; os.write(1, b'x' * 10000)"),
                           timeout=30, max_stream_bytes=64, check=False,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(raised.exception.stdout, b'x' * 64)
        self.assertEqual(raised.exception.observed_bytes['stdout'], 65)

    def test_a_group_that_refuses_the_kill_and_stays_alive_is_still_reported(self):
        # EPERM is only benign because nothing survived it. A process that is
        # genuinely alive and cannot be killed must never be reported as a
        # bounded one, so the reap below it stays the arbiter.
        process = MagicMock()
        process.pid = 424242
        process.returncode = None
        process.wait.side_effect = subprocess.TimeoutExpired(['vz'], 5)
        with patch.object(driver.subprocess, 'Popen', return_value=process), \
             patch.object(driver, 'collect_output',
                          side_effect=driver.OutputLimitExceeded('stdout exceeded')), \
             patch.object(driver.os, 'killpg', side_effect=PermissionError(1, 'nope')), \
             self.assertRaises(subprocess.TimeoutExpired):
            driver.execute(['vz'], timeout=1, max_stream_bytes=64, check=False)
        process.wait.assert_called_once_with(timeout=5)
