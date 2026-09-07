"""Native crypto checks; run with the runner's pinned isolated dependency lock."""
import copy
import io
import json
import pickle
import tarfile
import unittest
from unittest import mock

import bcrypt
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.verification import PolicyBuilder, Store, VerificationError

import linux_docker_registry_fixture as fixture
import linux_docker_registry_secrets as subject

OWNER = {'project_id': 'project', 'environment_id': 'environment', 'machine_id': 'machine'}
NOW = 1788652800 * 10**9


class SecretsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = subject.Secrets.generate(OWNER, 'run', now_unix_ns=NOW)

    def fresh_copy(self):
        result = subject.Secrets()
        for name in subject.Secrets.__slots__:
            value = getattr(self.original, name)
            setattr(result, name, copy.deepcopy(value) if type(value) is dict else value)
        return result

    def test_native_chain_password_and_conservative_scope(self):
        proof = self.original.validate_private()
        self.assertTrue(proof['certificate_chain_verified'])
        self.assertTrue(proof['wrong_ca_rejected'])
        self.assertTrue(proof['password_positive_and_negative_verified'])
        self.assertFalse(proof['handshake_certified'])
        self.assertFalse(proof['registry_authentication_certified'])

    def test_public_metadata_matches_external_certificate_pins(self):
        row = self.original.public()
        self.assertEqual(fixture.sha(self.original.ca_pem()), row['ca_sha256'])
        result = fixture.validate_tls_public(row, spec=fixture.resource_spec(OWNER, 'run'),
            expected=self.original.pins, observed_unix_ns=NOW)
        self.assertFalse(result['certificate_chain_verified'])
        self.assertEqual(row['san_ips'], [fixture.ADDRESS])
        self.assertEqual(row['san_dns'], [])

    def test_native_wrong_ca_and_wrong_address_fail(self):
        server = x509.load_pem_x509_certificate(self.original.privatefiles()['server.crt'])
        for ca, address in ((self.original.ca_pem(wrong=True), fixture.ADDRESS),
                            (self.original.ca_pem(), '172.30.241.3')):
            verifier = PolicyBuilder().store(Store([x509.load_pem_x509_certificate(ca)])).time(
                subject._time(NOW)).build_server_verifier(x509.IPAddress(subject.ipaddress.ip_address(address)))
            with self.assertRaises(VerificationError):
                verifier.verify(server, [])

    def test_distinct_machine_generation_is_fresh(self):
        owner = dict(OWNER, machine_id='second')
        second = subject.Secrets.generate(owner, 'run', now_unix_ns=NOW)
        self.assertNotEqual(second.password(), self.original.password())
        self.assertNotEqual(second.ca_pem(), self.original.ca_pem())
        self.assertNotEqual(second.privatefiles()['server.key'], self.original.privatefiles()['server.key'])
        self.assertEqual(second.public()['owner'], owner)

    def test_password_htpasswd_exact_bcrypt_contract(self):
        valid, invalid = self.original.password(), self.original.password('invalid')
        self.assertRegex(valid, b'^[0-9a-f]{48}$')
        self.assertRegex(invalid, b'^[0-9a-f]{48}$')
        username, password_hash = self.original.privatefiles()['htpasswd'].strip().split(b':')
        self.assertEqual(username, subject.USERNAME.encode())
        self.assertTrue(password_hash.startswith(b'$2b$12$'))
        self.assertTrue(bcrypt.checkpw(valid, password_hash))
        self.assertFalse(bcrypt.checkpw(invalid, password_hash))

    def test_public_serialization_contains_no_private_canaries_or_hashes(self):
        public = json.dumps(self.original.public()).encode()
        for private in self.original.canaries():
            self.assertNotIn(private, public)
            self.assertNotIn(fixture.sha(private).encode(), public)
        self.assertEqual(repr(self.original), '<Secrets: private in-memory registry fixture>')
        self.assertNotIn('password', public.decode())
        with self.assertRaises(subject.SecretsError):
            pickle.dumps(self.original)

    def test_canaries_cover_secrets_and_private_key_body(self):
        files = self.original.privatefiles()
        key = files['server.key']
        config = json.loads(files['config.yml'])
        canaries = self.original.canaries()
        for private in (self.original.password(), self.original.password('invalid'), key,
                        config['http']['secret'].encode(), files['htpasswd'].strip().split(b':')[1]):
            self.assertIn(private, canaries)
        self.assertIn(b''.join(line for line in key.splitlines() if not line.startswith(b'-----')), canaries)
        self.assertLess(len(canaries), 64)

    def test_config_is_fixed_private_nonloopback_tls_and_auth(self):
        config = json.loads(self.original.privatefiles()['config.yml'])
        self.assertEqual(set(config), {'version', 'log', 'storage', 'auth', 'http'})
        self.assertEqual(config['log'], {'level': 'info', 'formatter': 'json', 'accesslog': {'disabled': True}})
        self.assertEqual(config['http']['addr'], '172.30.241.2:5443')
        self.assertEqual(config['http']['draintimeout'], '5s')
        self.assertEqual(config['auth']['htpasswd']['path'], '/run/vz-registry/htpasswd')
        self.assertEqual(config['http']['tls'], {'certificate': '/run/vz-registry/server.crt',
            'key': '/run/vz-registry/server.key', 'minimumtls': 'tls1.2'})
        self.assertEqual(config['storage'], {'filesystem': {'rootdirectory': '/var/lib/registry'}})

    def test_bounded_private_tar_exact_regular_files(self):
        raw = self.original.provision_tar()
        self.assertLess(len(raw), subject.LIMIT)
        with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
            self.assertEqual(archive.getnames(), sorted(subject.FILES))
            for member in archive.getmembers():
                self.assertTrue(member.isreg())
                self.assertEqual((member.uid, member.gid, member.mode, member.mtime), (0, 0, 0o600, 0))
                self.assertEqual(archive.extractfile(member).read(), self.original.privatefiles()[member.name])

    def test_privatefiles_public_and_pins_are_defensive_copies(self):
        files = self.original.privatefiles()
        files['config.yml'] = b'foreign'
        public = self.original.public()
        public['owner']['machine_id'] = 'foreign'
        pins = self.original.pins
        pins['ca_sha256'] = '0' * 64
        self.assertNotEqual(self.original.privatefiles()['config.yml'], b'foreign')
        self.assertEqual(self.original.public()['owner'], OWNER)
        self.assertNotEqual(self.original.pins['ca_sha256'], '0' * 64)

    def test_expired_certificate_rejected_without_private_error(self):
        with self.assertRaisesRegex(subject.SecretsError, '^registry secrets: private validation failed$'):
            self.original.validate_private(observed_unix_ns=NOW + 8 * 86400 * 10**9)

    def test_tampering_fixed_diagnostics_no_secrets(self):
        for name in subject.FILES:
            item = self.fresh_copy()
            item._files[name] = b'SECRET-MUST-NOT-APPEAR'
            with self.assertRaisesRegex(subject.SecretsError, '^registry secrets: private validation failed$'):
                item.validate_private()

    def test_native_exception_is_redacted(self):
        with mock.patch.object(subject.serialization, 'load_pem_private_key', side_effect=ValueError('PRIVATE')):
            with self.assertRaisesRegex(subject.SecretsError, '^registry secrets: private validation failed$'):
                self.original.validate_private()

    def test_mismatched_native_key_is_rejected(self):
        item = self.fresh_copy()
        item._key_pem = subject.ec.generate_private_key(subject.ec.SECP256R1()).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        item._files['server.key'] = item._key_pem
        with self.assertRaises(subject.SecretsError):
            item.validate_private()

    def test_dependencies_are_exact(self):
        self.assertEqual(subject.dependency_inputs(), {'cryptography': '50.0.1', 'bcrypt': '5.0.0'})
        with mock.patch.object(subject.importlib.metadata, 'version', return_value='0.0.0'):
            with self.assertRaises(subject.SecretsError):
                subject.dependency_inputs()

    def test_invalid_owner_time_and_selectors_rejected(self):
        with self.assertRaises(fixture.FixtureError):
            subject.Secrets.generate(dict(OWNER, machine_id='../other'), 'run', now_unix_ns=NOW)
        for now in (True, 0, '123', 2**63):
            with self.assertRaises(subject.SecretsError):
                subject.Secrets.generate(OWNER, 'run', now_unix_ns=now)
        with self.assertRaises(subject.SecretsError):
            self.original.password('unknown')
        with self.assertRaises(subject.SecretsError):
            self.original.ca_pem(wrong=1)


if __name__ == '__main__':
    unittest.main()
