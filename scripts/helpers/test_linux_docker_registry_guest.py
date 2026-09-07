"""Pure plan/protocol tests plus shell parse-only checks; no guest execution."""
import base64
import copy
import hashlib
import re
import subprocess
import unittest
from unittest import mock

import linux_docker_registry_guest as guest

OWNER = {'project_id': 'prj_fixture', 'environment_id': 'env_fixture', 'machine_id': 'mch_fixture'}
NONCE = '12345678-1234-4234-8234-123456789abc'
IDS = {'root_identity': '12:345', 'ca_identity': '12:346'}


class GuestPlanTests(unittest.TestCase):
    def setUp(self):
        self.plan = guest.plan(OWNER, 'run-test', NONCE)
        self.files = {name: (name + '\npublic-unit-test-bytes\n').encode() for name in guest.FILES}

    def test_exact_resource_paths_and_owner_isolation(self):
        self.assertRegex(self.plan['directory'], r'^/run/vz-registry-[0-9a-f]{24}$')
        self.assertEqual(self.plan['ca_directory'], '/etc/docker/certs.d/172.30.241.2:5443')
        other = guest.plan(dict(OWNER, machine_id='mch_other'), 'run-test', NONCE)
        self.assertNotEqual(self.plan['directory'], other['directory'])
        self.assertEqual(self.plan['ca_directory'], other['ca_directory'])  # distinct guest Machines
        self.assertNotEqual(self.plan['directory'], guest.plan(OWNER, 'other-run', NONCE)['directory'])

    def test_unsafe_owner_nonce_and_plan_drift_rejected(self):
        for nonce in ['', '";exit 0', NONCE.upper(), '../wrong']:
            with self.assertRaises(ValueError):
                guest.plan(OWNER, 'run', nonce)
        with self.assertRaises(ValueError):
            guest.plan(dict(OWNER, machine_id='../root'), 'run', NONCE)
        for key, value in [('directory', '/run'), ('ca_directory', '/etc'), ('authority', '127.0.0.1:1'),
                           ('files', ['key']), ('extra', 'secret')]:
            changed = copy.deepcopy(self.plan); changed[key] = value
            with self.assertRaises(ValueError):
                guest.setup_script(changed)

    def test_exact_private_protocol_with_separate_initial_wrong_ca(self):
        raw = guest.encode_payload(self.plan, self.files, trust_ca=b'wrong-public-ca')
        rows = raw.split(b'\n')
        self.assertEqual(rows[:2], [b'VZ_REGISTRY_PRIVATE_V1', NONCE.encode()])
        self.assertEqual(rows[-2:], [b'END', b''])
        values = [self.files[name] for name in guest.FILES] + [b'wrong-public-ca']
        for index, value in enumerate(values):
            self.assertEqual(rows[2 + index * 2], hashlib.sha256(value).hexdigest().encode())
            self.assertEqual(base64.b64decode(rows[3 + index * 2], validate=True), value)
        self.assertLess(len(raw), 65536)

    def test_default_trust_is_real_ca(self):
        rows = guest.encode_payload(self.plan, self.files).split(b'\n')
        self.assertEqual(base64.b64decode(rows[-3]), self.files['ca.crt'])

    def test_file_inventory_types_individual_and_aggregate_bounds(self):
        for changed in [{}, dict(self.files, foreign=b'x'), dict(self.files, **{'server.key': ''}),
                        dict(self.files, **{'server.key': b''}),
                        dict(self.files, **{'server.key': b'x' * 16385}),
                        {name: b'x' * 16384 for name in guest.FILES}]:
            with self.assertRaises(ValueError):
                guest.encode_payload(self.plan, changed)
        with self.assertRaises(ValueError):
            guest.encode_payload(self.plan, self.files, trust_ca='not-bytes')

    def test_no_secret_or_hash_leaks_into_public_scripts_and_ack(self):
        secret = b'PRIVATE-SECRET-unit-canary-123456'
        self.files['server.key'] = secret
        payload = guest.encode_payload(self.plan, self.files)
        public = '\n'.join([guest.setup_script(self.plan), guest.admit_script(self.plan), guest.inspect_script(self.plan, IDS),
                            guest.cleanup_script(self.plan, IDS), repr(self.plan)]).encode()
        for value in [secret, base64.b64encode(secret), hashlib.sha256(secret).hexdigest().encode()]:
            self.assertNotIn(value, public)
        self.assertIn(base64.b64encode(secret), payload)  # explicitly PRIVATE transport

    def test_fixed_ack_and_identity_binding_all_actions(self):
        for action in ['ADMIT', 'INSPECT', 'CLEANUP']:
            raw = ('VZ_REGISTRY_' + action + '_V1\n' + NONCE + '\n12:345\n12:346\nEND\n').encode()
            self.assertEqual(guest.parse_ack(raw, self.plan, action=action, expected=IDS), IDS)
            for value in [raw[:-1], raw + b'x', raw.replace(b'12:345', b'12:347'),
                          raw.replace(b'12:345', b'0:1'), raw.replace(NONCE.encode(), b'foreign')]:
                with self.assertRaises(ValueError):
                    guest.parse_ack(value, self.plan, action=action, expected=IDS)

    def test_private_actions_have_only_preselected_fixed_stdout(self):
        for action in ['SETUP', 'TRUST']:
            expected = ('vz-registry-' + action.lower() + ' ' + NONCE + '\n').encode()
            self.assertEqual(guest.fixed_ack(self.plan, action=action), expected)
            with self.assertRaises(ValueError):
                guest.parse_ack(expected, self.plan, action=action)
        with self.assertRaises(ValueError):
            guest.fixed_ack(self.plan, action='ADMIT')
        self.assertTrue(guest.setup_script(self.plan).endswith('printf \'vz-registry-setup %s\\n\' "$nonce"\n'))
        self.assertTrue(guest.install_trust_script(self.plan, IDS, '1' * 64, '2' * 64).endswith(
            'printf \'vz-registry-trust %s\\n\' "$nonce"\n'))

    def test_admission_observes_then_validates_private_claim_before_public_ack(self):
        script = guest.admit_script(self.plan)
        self.assertIn('root_id=$(ident "$root")', script)
        self.assertIn('ca_id=$(ident "$ca")', script)
        self.assertIn('regular "$root/.owner" 256', script)
        self.assertIn('stat -c \'%s\' "$root/.owner"', script)
        self.assertLess(script.index('fingerprint "$root/$name"'), script.index('ack ADMIT'))
        self.assertNotIn('head -c 65536', script)  # no private stdin

    def test_no_dispatch_or_file_io_in_plan_encoding(self):
        with mock.patch('subprocess.Popen', side_effect=AssertionError('dispatch')), \
             mock.patch('builtins.open', side_effect=AssertionError('filesystem')):
            guest.setup_script(self.plan)
            guest.admit_script(self.plan)
            guest.encode_payload(self.plan, self.files)
            guest.inspect_script(self.plan, IDS)
            guest.cleanup_script(self.plan, IDS)

    def test_setup_create_only_and_bounded_no_tar_or_arbitrary_paths(self):
        script = guest.setup_script(self.plan)
        self.assertIn('head -c 65536', script)
        self.assertIn('regular "$root/.payload" 65535', script)
        self.assertIn('set -C', script)
        self.assertIn('mkdir -m 700 "$root"', script)
        self.assertIn('mkdir -m 700 "$ca"', script)
        self.assertIn('ca.crt server.crt server.key htpasswd config.yml trust.crt', script)
        self.assertIn('base64 -w 0 "$target"', script)
        self.assertNotIn('tar ', script)
        self.assertNotIn('mkdir -p', script)
        self.assertNotIn('rm -r', script)

    def test_cleanup_checks_inventory_and_hashes_before_exact_removal(self):
        script = guest.cleanup_script(self.plan, IDS)
        self.assertLess(script.index('test "$count" = 9'), script.index('"$bb" rm "$ca/ca.crt"'))
        self.assertLess(script.index('fingerprint "$root/$name"'), script.index('"$bb" rm "$root/$name"'))
        self.assertIn('test "$certs_created" = 1', script)
        self.assertIn('test "$(ident /etc/docker/certs.d)" = "$certs_id"', script)
        self.assertIn('"$bb" rmdir /etc/docker/certs.d', script)
        self.assertNotRegex(script, r'\brm\s+-[rf]')
        self.assertIn('head -c 4097 || exit 1; printf', script)
        self.assertIn('test "${#inventory}" -le 4097', script)

    def test_trust_phase_is_public_digest_bound_and_same_inode(self):
        old, new = '1' * 64, '2' * 64
        script = guest.install_trust_script(self.plan, IDS, old, new)
        self.assertIn('test "$got" = ' + old, script)
        self.assertEqual(script.count('test "$got" = ' + new), 2)
        self.assertIn('test "$(ident "$ca/ca.crt")" = "$trust_id"', script)
        self.assertIn('head -c 16385', script)
        self.assertNotIn('>| "$root/server.key"', script)
        for bad in ['', 'x' * 64, '"; rm /']:
            with self.assertRaises(ValueError):
                guest.install_trust_script(self.plan, IDS, bad, new)
        with self.assertRaises(ValueError):
            guest.install_trust_script(self.plan, IDS, old, old)

    def test_all_source_scripts_parse_only_without_execution(self):
        # -n parses public script text; it does not execute paths, commands or
        # redirections. This is NOT a BusyBox runtime/ownership correctness test.
        for script in [guest.setup_script(self.plan), guest.admit_script(self.plan), guest.inspect_script(self.plan, IDS),
                       guest.cleanup_script(self.plan, IDS),
                       guest.install_trust_script(self.plan, IDS, '1' * 64, '2' * 64)]:
            result = subprocess.run(['/bin/sh', '-n'], input=script.encode(), capture_output=True, timeout=5)
            self.assertEqual((result.returncode, result.stdout, result.stderr), (0, b'', b''))


if __name__ == '__main__':
    unittest.main()
