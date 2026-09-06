"""Local config admission tests; no Docker or credential helper dispatch."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

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
