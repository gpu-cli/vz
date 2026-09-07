"""Private temporary files only; no Docker, registry or credential helper calls."""
import base64
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_registry_credentials as subject

PASSWORD = b'a192c582b772d45e13c7b6da90c81452'
AUTHORITY = '172.30.241.2:5443'
PLUGINS = {'compose': '/owned/plugins/docker-compose', 'buildx': '/owned/plugins/docker-buildx'}


class CredentialsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve() / 'docker-client'
        self.root.mkdir(mode=0o700)
        self.owner = {'project_id': 'prj_test', 'environment_id': 'env_test', 'machine_id': 'mch_test'}
        self.descriptor = {'owner': self.owner, 'config_dir': str(self.root)}
        info = self.root.stat()
        self.claim = {'schema_version': 1, 'owner': self.owner, 'nonce': 'lop_' + 'a' * 32,
                      'directory': {'device': info.st_dev, 'inode': info.st_ino}}
        self.write('vz-owner.json', self.claim)
        self.config = {'auths': {}, 'credHelpers': subject.GUARD.copy(), 'currentContext': 'default',
                       'cliPluginsExtraDirs': ['/owned/plugins']}
        self.write('config.json', self.config)

    def write(self, name, value, replace=False):
        path = self.root / (name + '.new' if replace else name)
        path.write_bytes(json.dumps(value).encode())
        path.chmod(0o600)
        if replace:
            path.replace(self.root / name)

    def store(self):
        return subject.Store(self.descriptor, plugin_paths=PLUGINS, authority=AUTHORITY,
                             username='fixture', password=PASSWORD)

    def login(self):
        result = copy.deepcopy(self.config)
        result['auths'] = {AUTHORITY: {'auth': base64.b64encode(b'fixture:' + PASSWORD).decode()}}
        return result

    def test_atomic_login_logout_and_public_proof(self):
        store = self.store()
        empty = store.snapshot(expected='empty')
        self.assertTrue(store.check_unchanged(empty)['unchanged'])
        self.write('config.json', self.login(), replace=True)
        proof = store.check_transition(empty, expected='login')
        self.assertTrue(proof['specific_auth_transition'])
        login = store.snapshot(expected='login')
        self.write('config.json', self.config, replace=True)
        self.assertTrue(store.check_transition(login, expected='empty')['specific_auth_transition'])
        public = json.dumps(proof) + repr(store) + repr(login)
        for secret in (PASSWORD.decode(), base64.b64encode(b'fixture:' + PASSWORD).decode()):
            self.assertNotIn(secret, public)
        self.assertFalse(proof['private_config_hash_published'])
        self.assertFalse(proof['registry_authentication_certified'])

    def test_unchanged_rejects_replacement_even_equal_bytes(self):
        store = self.store()
        before = store.snapshot(expected='empty')
        self.write('config.json', self.config, replace=True)
        with self.assertRaises(subject.CredentialError):
            store.check_unchanged(before)
        with self.assertRaises(subject.CredentialError):
            store.snapshot(expected='empty')

    def test_foreign_auth_wrong_credentials_and_noncanonical_auth(self):
        variants = []
        for encoded in ('!!!!', base64.b64encode(b'other:' + PASSWORD).decode(),
                        base64.b64encode(b'fixture:wrong').decode(), self.login()['auths'][AUTHORITY]['auth'] + '\n'):
            row = self.login(); row['auths'][AUTHORITY]['auth'] = encoded; variants.append(row)
        row = self.login(); row['auths']['foreign.invalid'] = row['auths'][AUTHORITY]; variants.append(row)
        row = self.login(); row['auths'][AUTHORITY]['identitytoken'] = 'not-admitted'; variants.append(row)
        for row in variants:
            self.write('config.json', row)
            with self.assertRaises(subject.CredentialError) as caught:
                self.store().snapshot(expected='login')
            self.assertNotIn(PASSWORD.decode(), str(caught.exception))

    def test_unknown_settings_helpers_context_and_plugins(self):
        for key, value in (('credsStore', ''), ('proxies', {}), ('HttpHeaders', {}),
                           ('credHelpers', {}), ('currentContext', 'foreign'),
                           ('cliPluginsExtraDirs', ['/foreign'])):
            row = copy.deepcopy(self.config); row[key] = value
            self.write('config.json', row)
            with self.assertRaises(subject.CredentialError):
                self.store().snapshot(expected='empty')

    def test_claim_replaced_changed_and_foreign_owner(self):
        for mutation in ('replace', 'nonce', 'owner'):
            self.write('vz-owner.json', self.claim)
            store = self.store(); before = store.snapshot(expected='empty')
            row = copy.deepcopy(self.claim)
            if mutation == 'nonce': row['nonce'] = 'lop_' + 'b' * 32
            if mutation == 'owner': row['owner']['machine_id'] = 'mch_foreign'
            self.write('vz-owner.json', row, replace=mutation == 'replace')
            with self.assertRaises(subject.CredentialError): store.check_unchanged(before)

    def test_file_and_directory_permissions_links_and_bounds(self):
        path = self.root / 'config.json'
        path.chmod(0o644)
        with self.assertRaises(subject.CredentialError): self.store()
        path.chmod(0o600)
        self.root.chmod(0o755)
        with self.assertRaises(subject.CredentialError): self.store()
        self.root.chmod(0o700)
        os.link(path, self.root / 'alias')
        with self.assertRaises(subject.CredentialError): self.store()
        (self.root / 'alias').unlink()
        path.unlink(); path.symlink_to(self.root / 'vz-owner.json')
        with self.assertRaises(subject.CredentialError): self.store()
        path.unlink(); path.write_bytes(b' ' * (subject.LIMIT + 1)); path.chmod(0o600)
        with self.assertRaises(subject.CredentialError): self.store()

    def test_duplicate_json_and_no_transition_laundering(self):
        store = self.store(); before = store.snapshot(expected='empty')
        with self.assertRaises(subject.CredentialError): store.check_transition(before, expected='empty')
        (self.root / 'config.json').write_bytes(b'{"auths":{},"auths":{}}')
        with self.assertRaises(subject.CredentialError): self.store().snapshot(expected='empty')

    def test_mutation_between_reads_rejected(self):
        original = subject._file
        reads = [0]
        def mutate(path, maximum):
            result = original(path, maximum)
            if path.name == 'config.json':
                reads[0] += 1
                if reads[0] == 1: self.write('config.json', self.config, replace=True)
            return result
        with patch.object(subject, '_file', side_effect=mutate), self.assertRaises(subject.CredentialError):
            self.store()

    def test_foreign_snapshot_and_descriptor_copies(self):
        store = self.store(); before = store.snapshot(expected='empty')
        other = self.store()
        with self.assertRaises(subject.CredentialError): other.check_unchanged(before)
        self.descriptor['owner']['machine_id'] = 'mch_changed'
        self.assertTrue(store.check_unchanged(before)['unchanged'])
        with self.assertRaises(subject.CredentialError):
            self.store()

    def test_claim_schema_unknown_fields_and_bool_rejected(self):
        for key, value in (('unknown', None), ('schema_version', True), ('nonce', 'lop_invalid'),
                           ('directory', {'device': True, 'inode': self.root.stat().st_ino})):
            row = copy.deepcopy(self.claim); row[key] = value
            self.write('vz-owner.json', row)
            with self.assertRaises(subject.CredentialError): self.store()

    def test_fifo_never_blocks_and_foreign_uid_rejected(self):
        path = self.root / 'config.json'
        path.unlink(); os.mkfifo(path, 0o600)
        with self.assertRaises(subject.CredentialError): self.store()
        path.unlink(); self.write('config.json', self.config)
        with patch.object(subject.os, 'geteuid', return_value=os.geteuid() + 1), self.assertRaises(subject.CredentialError):
            self.store()

    def test_whole_directory_replacement_rejected(self):
        store = self.store(); before = store.snapshot(expected='empty')
        saved = self.root.with_name('old-store')
        self.root.rename(saved); self.root.mkdir(mode=0o700)
        info = self.root.stat()
        claim = copy.deepcopy(self.claim)
        claim['directory'] = {'device': info.st_dev, 'inode': info.st_ino}
        self.write('vz-owner.json', claim); self.write('config.json', self.config)
        with self.assertRaises(subject.CredentialError): store.check_unchanged(before)

    def test_auth_mutation_cannot_change_other_settings(self):
        store = self.store(); before = store.snapshot(expected='empty')
        changed = self.login(); del changed['currentContext']
        self.write('config.json', changed, replace=True)
        with self.assertRaises(subject.CredentialError): store.check_transition(before, expected='login')


if __name__ == '__main__':
    unittest.main()
