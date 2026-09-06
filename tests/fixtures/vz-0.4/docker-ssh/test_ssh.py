import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import ssh_probe as probe
import server
import packages


def item():
    return {'schema_version': 1, 'token': 'vzssh-' + 'a' * 24,
            'host': '172.19.0.3', 'port': 2222,
            'host_key_fingerprint': 'SHA256:' + 'B' * 43}


class Protocol(unittest.TestCase):
    def test_request_exact(self):
        self.assertEqual(probe.request(json.dumps(item()).encode()), item())
        for key, value in [('schema_version', True), ('port', True), ('port', 22),
                           ('host', '127.0.0.1'), ('host', '169.254.1.1'),
                           ('host', '0.0.0.0'), ('host', '8.8.8.8'),
                           ('host', '-oProxyCommand=x'), ('host', '172.019.0.3'),
                           ('token', 'x\nsecret'), ('host_key_fingerprint', 'SHA256:bad')]:
            with self.subTest(key=key, value=value):
                value_item = item(); value_item[key] = value
                with self.assertRaises((ValueError, TypeError)):
                    probe.request(json.dumps(value_item).encode())
        for value in [dict(item(), extra=True), {k: v for k, v in item().items() if k != 'port'}]:
            with self.assertRaises(ValueError):
                probe.request(json.dumps(value).encode())
        with self.assertRaises(ValueError):
            probe.request(json.dumps(item()).encode()[:-1] + b',"port":2222}')

    def test_known_host_exact_wire_key_and_address(self):
        key = b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20' + b'x' * 32
        raw = b'[172.19.0.3]:2222 ssh-ed25519 ' + base64.b64encode(key) + b'\n'
        self.assertEqual(probe.known_hosts(raw, item()), 'SHA256:' +
                         base64.b64encode(hashlib.sha256(key).digest()).decode().rstrip('='))
        for changed in [raw + raw, raw[:-1], raw.replace(b'172.19.0.3', b'172.19.0.4'),
                        raw.replace(b'ssh-ed25519 ', b'ssh-rsa '), raw + b'comment',
                        b'[172.19.0.3]:2222 ssh-ed25519 !!!!\n']:
            with self.assertRaises(ValueError):
                probe.known_hosts(changed, item())

    def test_success_exact_response_and_no_stderr(self):
        self.assertEqual(probe.classify(item(), 0, probe.response(item()), b''), 'authenticated')
        for code, stdout, stderr in [(0, b'wrong\n', b''), (0, probe.response(item()), b'warning'),
                                     (1, probe.response(item()), b''), (True, b'', b'')]:
            if type(code) is bool:
                with self.assertRaises(ValueError): probe.classify(item(), code, stdout, stderr)
            else:
                self.assertEqual(probe.classify(item(), code, stdout, stderr), 'operational_failure')

    def test_exact_publickey_denial_only(self):
        denied = b'vzssh@172.19.0.3: Permission denied (publickey).\r\n'
        self.assertEqual(probe.classify(item(), 255, b'', denied), 'publickey_denied')
        for bad in [denied.replace(b'publickey', b'publickey,password'), denied.replace(b'vzssh', b'root'),
                    denied.replace(b'172.19.0.3', b'172.19.0.4'), denied.replace(b'\r\n', b'\n'),
                    denied + b'extra\n', b'Connection refused\r\n',
                    b'Connection timed out\r\n', b'Could not resolve hostname\r\n',
                    b'no matching cipher found\r\n', b'Bad configuration option\r\n']:
            self.assertEqual(probe.classify(item(), 255, b'', bad), 'operational_failure')
        self.assertEqual(probe.classify(item(), 255, b'output', denied), 'operational_failure')
        self.assertEqual(probe.classify(item(), 1, b'', denied), 'operational_failure')

    def test_hostkey_exact_fingerprint_path_and_complete_grammar(self):
        raw = probe.hostkey_error(item())
        self.assertEqual(probe.classify(item(), 255, b'', raw), 'hostkey_denied')
        for bad in [raw.replace(b'B' * 43, b'C' * 43), raw.replace(b':1\r\n', b':2\r\n'),
                    raw.replace(b'172.19.0.3', b'172.19.0.4'), raw[:-1],
                    raw + b'Connection reset\r\n', b'Host key verification failed.\r\n']:
            self.assertEqual(probe.classify(item(), 255, b'', bad), 'operational_failure')

    def test_options_no_ambient_credentials_or_network_proxy(self):
        command = probe.argv(item(), True)
        options = {command[n + 1] for n, value in enumerate(command) if value == '-o'}
        self.assertEqual(command[:7], ['/usr/bin/ssh', '-F', '/dev/null', '-T', '-n', '-4', '-o'])
        self.assertTrue({'BatchMode=yes', 'StrictHostKeyChecking=yes', 'IdentityFile=none',
                         'PasswordAuthentication=no', 'KbdInteractiveAuthentication=no',
                         'ForwardAgent=no', 'ControlPath=none', 'ProxyCommand=none',
                         'GlobalKnownHostsFile=/dev/null', 'IdentityAgent=' + probe.AGENT} <= options)
        self.assertIn('IdentityAgent=none', probe.argv(item(), False))
        self.assertEqual(command[-5:], ['2222', '-l', 'vzssh', '172.19.0.3', 'vz-public-response'])

    def test_denial_emits_only_public_bound_record(self):
        raw_error = b'vzssh@172.19.0.3: Permission denied (publickey).\r\n'
        output = io.StringIO()
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(probe.os.path, 'lexists', return_value=False), contextlib.redirect_stdout(output):
            result = probe.execute('undeclared', item(), lambda args: (255, b'', raw_error))
        row = json.loads(output.getvalue())
        self.assertEqual(result, 41)
        self.assertEqual(row['outcome'], 'publickey_denied')
        self.assertEqual(row['stderr_sha256'], hashlib.sha256(raw_error).hexdigest())
        self.assertNotIn('Permission denied', output.getvalue())

    def test_unrelated_output_never_leaks(self):
        secret = b'PRIVATE KEY MATERIAL NOT EVIDENCE'
        output = io.StringIO()
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(probe.os.path, 'lexists', return_value=False), contextlib.redirect_stdout(output):
            result = probe.execute('undeclared', item(), lambda args: (255, secret, secret))
        self.assertEqual(result, 70)
        self.assertNotIn(secret.decode(), output.getvalue())
        self.assertEqual(json.loads(output.getvalue())['outcome'], 'operational_failure')

    def test_missing_or_foreign_socket_does_not_become_auth_denial(self):
        for env in ({'SSH_AUTH_SOCK': '/foreign'}, {'SSH_AUTH_SOCK': probe.AGENT}):
            with mock.patch.dict(os.environ, env, clear=True), self.assertRaises(ValueError):
                probe.execute('undeclared', item(), mock.Mock())
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(probe.os, 'lstat', return_value=mock.Mock(st_mode=stat.S_IFREG)), self.assertRaises(ValueError):
            probe.execute('mounted', item(), mock.Mock())

    def test_mount_absence_rejects_surviving_socket_or_environment(self):
        with mock.patch.object(probe.os.path, 'lexists', return_value=True), self.assertRaises(ValueError):
            probe.absent(item())
        with mock.patch.object(probe.os.path, 'lexists', return_value=False), mock.patch.dict(os.environ, {'SSH_AUTH_SOCK': probe.AGENT}), self.assertRaises(ValueError):
            probe.absent(item())

    def test_regular_reader_rejects_redirects_fifo_and_oversize(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); file = root / 'file'; file.write_bytes(b'public')
            self.assertEqual(probe.read_regular(file), b'public')
            link = root / 'link'; link.symlink_to(file)
            fifo = root / 'fifo'; os.mkfifo(fifo)
            for path in (link, fifo):
                with self.assertRaises((ValueError, OSError)): probe.read_regular(path)
            with self.assertRaises(ValueError): probe.read_regular(file, 2)

    def test_real_bounded_subprocess_not_ssh_network(self):
        with mock.patch.dict(os.environ, {'SSH_AUTH_SOCK': '/ambient', 'PRIVATE_CANARY': 'secret'}):
            code, stdout, stderr = probe.capture([sys.executable, '-c',
                'import os; assert "HOME" not in os.environ; assert "SSH_AUTH_SOCK" not in os.environ; assert "PRIVATE_CANARY" not in os.environ; print("public")'])
        self.assertEqual((code, stdout, stderr), (0, b'public\n', b''))

    def test_package_stdout_limit_does_not_change_environment(self):
        with mock.patch.dict(os.environ, {'TAR_OPTIONS': '--unsafe', 'SSH_AUTH_SOCK': '/ambient'}):
            result = probe.capture([sys.executable, '-c',
                'import os; assert "TAR_OPTIONS" not in os.environ; assert "SSH_AUTH_SOCK" not in os.environ; os.write(1,b"p"*16384)'], stdout_limit=16384)
        self.assertEqual(result, (0, b'p' * 16384, b''))

    def test_package_stdout_limit_is_bounded_before_spawn(self):
        for value in (True, 0, -1, 32 * 1024 * 1024 + 1, 'large'):
            with self.subTest(value=value), mock.patch.object(probe.subprocess, 'Popen') as spawn, self.assertRaises(ValueError):
                probe.capture(['/fake'], stdout_limit=value)
            spawn.assert_not_called()

    def test_enlarged_package_stdout_never_enlarges_stderr(self):
        process = mock.Mock(pid=123456); process.poll.return_value = -9
        selector = mock.MagicMock(); selector.__enter__.return_value = selector
        selector.get_map.return_value = {1: True}
        selector.select.return_value = [(mock.Mock(fd=1, data=1), None)]
        with mock.patch.object(probe.subprocess, 'Popen', return_value=process), \
             mock.patch.object(probe.selectors, 'DefaultSelector', return_value=selector), \
             mock.patch.object(probe.os, 'set_blocking'), \
             mock.patch.object(probe.os, 'read', return_value=b'x' * (probe.LIMIT + 1)), \
             mock.patch.object(probe.os, 'killpg') as kill, self.assertRaises(ValueError):
            probe.capture(['/fake'], stdout_limit=16 * 1024 * 1024)
        kill.assert_called_once_with(process.pid, probe.signal.SIGKILL)
        process.wait.assert_called_once_with(timeout=5)

    def test_subprocess_overflow_fails_and_reaps_owned_child(self):
        process = mock.Mock(pid=123456)
        process.poll.return_value = -9
        selector = mock.MagicMock()
        selector.__enter__.return_value = selector
        selector.get_map.return_value = {1: True}
        selector.select.return_value = [(mock.Mock(fd=1, data=0), None)]
        with mock.patch.object(probe.subprocess, 'Popen', return_value=process), \
             mock.patch.object(probe.selectors, 'DefaultSelector', return_value=selector), \
             mock.patch.object(probe.os, 'set_blocking'), \
             mock.patch.object(probe.os, 'read', return_value=b'x' * (probe.LIMIT + 1)), \
             mock.patch.object(probe.os, 'killpg') as kill, self.assertRaises(ValueError):
            probe.capture(['/fake/owned-child'])
        kill.assert_called_once_with(process.pid, probe.signal.SIGKILL)
        process.wait.assert_called_once_with(timeout=5)
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()

    def test_subprocess_timeout_fails_and_reaps_owned_child(self):
        process = mock.Mock(pid=123456)
        process.poll.return_value = -9
        selector = mock.MagicMock()
        selector.__enter__.return_value = selector
        selector.get_map.return_value = {1: True}
        with mock.patch.object(probe.subprocess, 'Popen', return_value=process), \
             mock.patch.object(probe.selectors, 'DefaultSelector', return_value=selector), \
             mock.patch.object(probe.os, 'set_blocking'), \
             mock.patch.object(probe.time, 'monotonic', side_effect=[0, 100]), \
             mock.patch.object(probe.os, 'killpg') as kill, self.assertRaises(ValueError):
            probe.capture(['/fake/owned-child'])
        kill.assert_called_once_with(process.pid, probe.signal.SIGKILL)
        process.wait.assert_called_once_with(timeout=5)


class Recipe(unittest.TestCase):
    def test_shared_required_recipe_and_no_undeclared_mount(self):
        required = (ROOT / 'Dockerfile.ssh').read_text()
        self.assertEqual(required.count('--mount=type=ssh'), 1)
        self.assertIn('id=fixture,required=true,target=' + probe.AGENT, required)
        self.assertIn('RUN python3 /fixture/ssh_probe.py absent\nFROM scratch', required)
        self.assertNotIn('--mount', (ROOT / 'Dockerfile.undeclared').read_text())
        contract = json.loads((ROOT / 'contract.json').read_text())
        self.assertEqual({contract['recipes'][name]['dockerfile'] for name in
                          ('declared', 'wrong_host', 'provider_omitted')}, {'Dockerfile.ssh'})
        self.assertIsNone(contract['recipes']['provider_omitted']['probe_exit'])

    def test_server_disables_other_auth_and_execution_paths(self):
        config = (ROOT / 'sshd_config').read_text().splitlines()
        for exact in ['PermitRootLogin no', 'AllowUsers vzssh', 'UsePAM no',
                      'AuthenticationMethods publickey', 'PasswordAuthentication no',
                      'DisableForwarding yes', 'PermitTTY no', 'PermitUserRC no',
                      'PermitUserEnvironment no',
                      'ForceCommand /usr/local/bin/python3 /fixture/server.py response']:
            self.assertIn(exact, config)
        self.assertFalse(any(line.startswith(('Include ', 'Subsystem ', 'AcceptEnv ')) for line in config))

    def test_server_missing_account_fails_without_account_mutation(self):
        with mock.patch.object(server.pwd, 'getpwnam', side_effect=KeyError), self.assertRaises(KeyError):
            server.accounts()

    def test_server_response_only_exact_public_token(self):
        with mock.patch.object(server, 'read_regular', return_value=probe.response(item())):
            self.assertEqual(server.public_response(), probe.response(item()))
        for value in [b'private-key', probe.response(item()) + b'extra\n']:
            with mock.patch.object(server, 'read_regular', return_value=value), self.assertRaises(ValueError):
                server.public_response()

    def account_root(self, root):
        (root / 'etc').mkdir()
        for name, data in {'passwd': b'root:x:0:0:root:/root:/bin/bash\n',
                           'group': b'root:x:0:\n', 'shadow': b'root:!:::::::\n',
                           'gshadow': b'root:!::\n'}.items():
            (root / 'etc' / name).write_bytes(data)

    def test_fresh_accounts_preserve_root_and_are_password_unusable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); self.account_root(root)
            original = {f.name: f.read_bytes() for f in (root / 'etc').iterdir()}
            server.install_accounts(root)
            for name, data in original.items():
                self.assertTrue((root / 'etc' / name).read_bytes().startswith(data))
            self.assertIn(b'vzssh:*:::::::\n', (root / 'etc/shadow').read_bytes())
            self.assertIn(b'vzssh:x:10001:10001:', (root / 'etc/passwd').read_bytes())
            frozen = {f.name: f.read_bytes() for f in (root / 'etc').iterdir()}
            with self.assertRaises(ValueError): server.install_accounts(root)
            self.assertEqual(frozen, {f.name: f.read_bytes() for f in (root / 'etc').iterdir()})

    def test_account_collision_admitted_before_any_mutation(self):
        for name, extra in [('passwd', b'other:x:10001:2:other:/home/other:/bin/sh\n'),
                            ('group', b'other:x:10002:\n'), ('shadow', b'vzssh:!:::::::\n')]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); self.account_root(root)
                path = root / 'etc' / name; path.write_bytes(path.read_bytes() + extra)
                frozen = {f.name: f.read_bytes() for f in (root / 'etc').iterdir()}
                with self.assertRaises(ValueError): server.install_accounts(root)
                self.assertEqual(frozen, {f.name: f.read_bytes() for f in (root / 'etc').iterdir()})

    def test_account_file_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); self.account_root(root)
            (root / 'etc/shadow').rename(root / 'original-shadow')
            (root / 'etc/shadow').symlink_to(root / 'original-shadow')
            with self.assertRaises(OSError): server.install_accounts(root)

    def test_all_recipes_have_same_pinned_default_and_no_network_install(self):
        for name in ('Dockerfile.ssh', 'Dockerfile.undeclared', 'Dockerfile.server'):
            text = (ROOT / name).read_text()
            self.assertTrue(text.startswith('ARG FIXTURE_BASE=docker.io/library/python@sha256:d04f49'))
            self.assertIn('RUN --network=none python3 /fixture/packages.py', text)
            self.assertNotIn('apt-get', text)
            self.assertNotIn('dpkg -i', text)


class Packages(unittest.TestCase):
    def fixture(self, root):
        for patcher in getattr(self, '_fixture_patches', []):
            patcher.stop()
        self._fixture_patches = []
        self.metadata_overrides = {}
        directory = root / 'packages'; directory.mkdir()
        (root / 'usr/bin').mkdir(parents=True)
        (root / 'usr/sbin').mkdir()
        (root / 'usr/lib/aarch64-linux-gnu').mkdir(parents=True)
        extractor = root / 'usr/bin/dpkg-deb'; extractor.write_bytes(b'fake extractor'); extractor.chmod(0o755)
        extraction = {'aliases': dict(packages.ALIASES)}
        for name, path in [('tar', '/usr/bin/tar'), ('loader', '/usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1')]:
            file = root / path.lstrip('/'); file.write_bytes(('fake ' + name).encode()); file.chmod(0o755)
            extraction[name] = {'path': path, 'size': file.stat().st_size, 'sha256': hashlib.sha256(file.read_bytes()).hexdigest()}
        for path, target in packages.ALIASES.items():
            (root / path.lstrip('/')).symlink_to(target)
        pins = {'schema_version': 1, 'dpkg_deb_sha256': hashlib.sha256(extractor.read_bytes()).hexdigest(),
                'packages': [], 'extraction': extraction}
        for index in range(8):
            name = 'p' + str(index); filename = name + '_1_arm64.deb'; data = name.encode()
            (directory / filename).write_bytes(data)
            pins['packages'].append({'filename': filename, 'package': name, 'version': '1',
                                     'architecture': 'arm64', 'sha256': hashlib.sha256(data).hexdigest(),
                                     'size': len(data)})
        pinfile = root / 'pins.json'; pinfile.write_text(json.dumps(pins))
        (directory / 'manifest.json').write_bytes(pinfile.read_bytes())
        original_guard, original_read = packages.runtime_guard, packages.read_regular
        # Guard still checks real temporary symlinks/files/hashes. Only their
        # host ownership is represented as guest root; never inspect host /lib.
        original_lstat, original_readlink = os.lstat, os.readlink
        def mapped(path):
            path = Path(path)
            return path if root in path.parents else root / str(path).lstrip('/')
        def lstat(path):
            info = original_lstat(mapped(path))
            values = list(info); values[4] = values[5] = 0
            if stat.S_ISLNK(info.st_mode):
                values[0] = stat.S_IFLNK | 0o777
            for index, value in self.metadata_overrides.get(str(path), {}).items():
                values[index] = value
            return os.stat_result(values)
        def guarded(value):
            with mock.patch.object(packages.os, 'lstat', side_effect=lstat), \
                 mock.patch.object(packages.os, 'readlink', side_effect=lambda p: original_readlink(mapped(p))), \
                 mock.patch.object(packages, 'read_regular', side_effect=lambda p, limit: original_read(mapped(p), limit)):
                return original_guard(value)
        patcher = mock.patch.object(packages, 'runtime_guard', side_effect=guarded)
        patcher.start(); self.addCleanup(patcher.stop)
        self._fixture_patches.append(patcher)
        original_temp = tempfile.mkdtemp
        def temporary(*args, **kwargs):
            if kwargs.get('prefix') == 'vz-ssh-package-':
                return original_temp(prefix='spool-', dir=root)
            return original_temp(*args, **kwargs)
        temp_patch = mock.patch.object(packages.tempfile, 'mkdtemp', side_effect=temporary)
        temp_patch.start(); self.addCleanup(temp_patch.stop)
        self._fixture_patches.append(temp_patch)
        return directory, pinfile, extractor

    def test_extract_only_exact_admitted_bytes_and_never_install(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); pkg, pins, extractor = self.fixture(root)
            runner = mock.Mock(side_effect=lambda argv, **kwargs: (0, b'inert tar fixture' if '--fsys-tarfile' in argv else b'', b''))
            with mock.patch.object(packages, 'DPKG', str(extractor)), contextlib.redirect_stdout(io.StringIO()):
                packages.extract(pkg, pins, runner)
            self.assertEqual(runner.call_count, 16)
            for first, second in zip(runner.call_args_list[::2], runner.call_args_list[1::2]):
                self.assertEqual(first.args[0][:2], [str(extractor), '--fsys-tarfile'])
                self.assertEqual(first.kwargs, {'stdout_limit': 16 * 1024 * 1024})
                self.assertEqual(second.args[0][:3], ['/usr/bin/tar', '--extract', '--preserve-permissions'])
                self.assertEqual(second.args[0][-4:], ['--directory', '/', '--keep-directory-symlink', '--warning=no-timestamp'])
                self.assertEqual(second.kwargs, {})
            self.assertFalse(list(root.glob('spool-*')))

    def test_package_drift_missing_extra_and_symlink_fail_before_first_effect(self):
        for mutation in ('bytes', 'extra', 'missing', 'symlink', 'extractor'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); pkg, pins, extractor = self.fixture(root)
                target = pkg / 'p7_1_arm64.deb'
                if mutation == 'bytes': target.write_bytes(b'drift')
                elif mutation == 'extra': (pkg / 'extra').write_bytes(b'extra')
                elif mutation == 'missing': target.unlink()
                elif mutation == 'extractor': extractor.write_bytes(b'drift')
                else:
                    target.unlink(); target.symlink_to(pkg / 'p6_1_arm64.deb')
                runner = mock.Mock()
                with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises((ValueError, OSError)):
                    packages.extract(pkg, pins, runner)
                runner.assert_not_called()

    def test_extract_error_is_operational_and_stops_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            pkg, pins, extractor = self.fixture(Path(directory))
            runner = mock.Mock(return_value=(1, b'', b'error'))
            with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(ValueError):
                packages.extract(pkg, pins, runner)
            self.assertEqual(runner.call_count, 1)

    def test_boolean_schema_and_duplicate_manifest_refused(self):
        for mutation in ('bool', 'duplicate'):
            with tempfile.TemporaryDirectory() as directory:
                pkg, pins, extractor = self.fixture(Path(directory))
                if mutation == 'bool':
                    value = json.loads(pins.read_bytes()); value['schema_version'] = True
                    (pkg / 'manifest.json').write_text(json.dumps(value))
                else:
                    (pkg / 'manifest.json').write_bytes(pins.read_bytes()[:-1] + b',"schema_version":1}')
                runner = mock.Mock()
                with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(ValueError):
                    packages.extract(pkg, pins, runner)
                runner.assert_not_called()

    def test_alias_tool_and_loader_drift_rejected_before_any_runner(self):
        for kind in ('alias-target', 'alias-directory', 'tar-bytes', 'loader-bytes', 'tool-mode', 'alias-owner', 'tool-owner'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); pkg, pins, extractor = self.fixture(root)
                if kind.startswith('alias-') and kind != 'alias-owner':
                    (root / 'lib').unlink()
                    if kind == 'alias-directory': (root / 'lib').mkdir()
                    else: (root / 'lib').symlink_to('usr/bin')
                elif kind == 'tar-bytes': (root / 'usr/bin/tar').write_bytes(b'changed')
                elif kind == 'loader-bytes': (root / 'usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1').write_bytes(b'changed')
                elif kind == 'tool-mode': (root / 'usr/bin/tar').chmod(0o700)
                elif kind == 'alias-owner': self.metadata_overrides['/lib'] = {4: 1}
                elif kind == 'tool-owner': self.metadata_overrides['/usr/bin/tar'] = {5: 1}
                runner = mock.Mock()
                with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises((ValueError, OSError)):
                    packages.extract(pkg, pins, runner)
                runner.assert_not_called()

    def test_alias_replaced_during_tar_is_not_repaired_or_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); pkg, pins, extractor = self.fixture(root)
            def execute(argv, **kwargs):
                if '--fsys-tarfile' in argv: return 0, b'inert tar fixture', b''
                (root / 'lib').rename(root / 'old-lib')
                (root / 'lib').symlink_to('usr/lib')
                return 0, b'', b''
            runner = mock.Mock(side_effect=execute)
            with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(ValueError):
                packages.extract(pkg, pins, runner)
            self.assertEqual(runner.call_count, 2)
            self.assertTrue((root / 'old-lib').is_symlink())
            self.assertFalse(list(root.glob('spool-*')))

    def test_spool_is_private_exact_and_absent_after_completed_tar_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); pkg, pins, extractor = self.fixture(root)
            def execute(argv, **kwargs):
                if '--fsys-tarfile' in argv: return 0, b'inert tar fixture', b''
                spool = Path(argv[argv.index('--file') + 1])
                self.assertEqual(spool.read_bytes(), b'inert tar fixture')
                self.assertEqual(stat.S_IMODE(spool.stat().st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(spool.parent.stat().st_mode), 0o700)
                return 1, b'', b'error'
            runner = mock.Mock(side_effect=execute)
            with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(ValueError):
                packages.extract(pkg, pins, runner)
            self.assertEqual(runner.call_count, 2)
            self.assertFalse(list(root.glob('spool-*')))

    def test_capture_exception_retains_spool_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); pkg, pins, extractor = self.fixture(root)
            runner = mock.Mock(side_effect=[(0, b'inert tar fixture', b''), TimeoutError('unproven reap')])
            with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(TimeoutError):
                packages.extract(pkg, pins, runner)
            self.assertEqual(runner.call_count, 2)
            spools = list(root.glob('spool-*/data.tar'))
            self.assertEqual(len(spools), 1)
            self.assertEqual(spools[0].read_bytes(), b'inert tar fixture')

    def test_passthrough_bounds_and_unexpected_diagnostics_prevent_tar(self):
        for result in ((0, b'', b''), (0, b'x' * (packages.LIMIT + 1), b''), (0, b'tar', b'warning'), (False, b'tar', b'')):
            with self.subTest(size=len(result[1])), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); pkg, pins, extractor = self.fixture(root)
                runner = mock.Mock(return_value=result)
                with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(ValueError):
                    packages.extract(pkg, pins, runner)
                self.assertEqual(runner.call_count, 1)
                self.assertFalse(list(root.glob('spool-*')))

    def test_extraction_contract_rejects_unknown_keys_paths_and_boolean_sizes(self):
        for change in ('extra', 'path', 'size', 'alias'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); pkg, pins, extractor = self.fixture(root)
                value = json.loads(pins.read_bytes())
                if change == 'extra': value['extraction']['extra'] = False
                elif change == 'path': value['extraction']['tar']['path'] = '/bin/tar'
                elif change == 'size': value['extraction']['tar']['size'] = True
                else: value['extraction']['aliases']['/lib'] = 'foreign'
                pins.write_text(json.dumps(value)); (pkg / 'manifest.json').write_bytes(pins.read_bytes())
                runner = mock.Mock()
                with mock.patch.object(packages, 'DPKG', str(extractor)), self.assertRaises(ValueError):
                    packages.extract(pkg, pins, runner)
                runner.assert_not_called()


if __name__ == '__main__':
    unittest.main()
