"""Cross-Machine registry credential controls, independent replay and leak scans.

Nothing here mutates Docker, a VM or credential state. The controls read four
Machine-private Docker config directories, host default/helper state and durable
receipts, then publish only structure, counts and hashes of non-secret files.

Sibling/neighbor stores are built with a fresh random placeholder credential:
the ``empty`` policy never compares credential bytes, so "no auth entry at this
authority" is provable without knowing any Machine's password. The active
Machine's ``login`` policy is checked only through its own Session store.
"""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat

import docker_host_driver as driver
import linux_docker_private_stdin as private_stdin
import linux_docker_registry_credentials as credentials
import linux_docker_registry_fixture as fixture

USERNAME = 'vz-registry-user'
LABEL_PREFIX = 'registry-'
PLUGINS = ('compose', 'buildx')
FILE_LIMIT = 256 * 1024 * 1024
WINDOW = 8 * 1024 * 1024
ORBSTACK_CONTEXT = hashlib.sha256(b'orbstack').hexdigest()


class ControlError(ValueError):
    """Static diagnostics only; never private bytes or their hashes."""


def require(condition, reason):
    if not condition:
        raise ControlError('registry controls rejected: ' + reason)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _owner_key(descriptor):
    return json.dumps(descriptor['owner'], sort_keys=True, separators=(',', ':'))


def _lstat_state(path):
    """Structure-only state: hashes/sizes/times, never contents."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return {'present': False}
    state = {'present': True, 'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid,
             'size': info.st_size, 'mtime_ns': info.st_mtime_ns,
             'kind': 'symlink' if stat.S_ISLNK(info.st_mode) else 'directory' if stat.S_ISDIR(info.st_mode)
             else 'regular' if stat.S_ISREG(info.st_mode) else 'other'}
    if state['kind'] == 'regular':
        require(info.st_size <= FILE_LIMIT, 'host default file exceeds bound')
        state['sha256'] = _sha(Path(path).read_bytes())
    elif state['kind'] == 'symlink':
        target = Path(path).resolve()
        state['target_sha256'] = _sha(target.read_bytes()) if target.is_file() else None
    return state


def _regular_bytes(path):
    """Bounded read of one owned regular file; links and special files fail."""
    info = os.lstat(path)
    require(stat.S_ISREG(info.st_mode), 'scan tree contains a non-regular file')
    require(info.st_uid == os.geteuid(), 'scan tree file not owned by current user')
    require(info.st_size <= FILE_LIMIT, 'scan tree file exceeds bound')
    raw = Path(path).read_bytes()
    require(len(raw) == info.st_size, 'scan tree file changed during read')
    return raw


def _walk(root):
    root = Path(root)
    if not root.exists():
        return []
    require(root.is_dir() and not root.is_symlink(), 'scan root must be a real directory')
    rows = []
    for current, directories, files in os.walk(root):
        for name in directories:
            require(not (Path(current) / name).is_symlink(), 'scan tree contains a directory symlink')
        for name in sorted(files):
            rows.append(Path(current) / name)
    return sorted(rows)


def canary_variants(private_fixture):
    """Raw, base64, urlsafe-base64, hex, quoted and Docker auth-blob forms."""
    canaries = list(private_fixture.canaries())
    require(canaries and all(type(value) is bytes and value for value in canaries), 'private canaries required')
    variants = set()
    for value in canaries:
        variants.update(private_stdin._variants(value))
    for role in ('valid', 'invalid'):
        password = private_fixture.password(role=role)
        require(type(password) is bytes and password in canaries, 'password must be a declared canary')
        blob = USERNAME.encode('ascii') + b':' + password
        variants.update(private_stdin._variants(blob))
        variants.add(base64.b64encode(blob))
        variants.add(blob.hex().encode())
    return sorted(variants)


def scan_bytes(raw, variants):
    """Fail closed via driver.contains_canary over overlapping windows."""
    if not raw:
        return False
    overlap = max(len(value) for value in variants) * 2
    position = 0
    while position < len(raw):
        window = raw[max(0, position - overlap):position + WINDOW]
        if driver.contains_canary((window,), variants):
            return True
        position += WINDOW
    return False


class Controls:
    def __init__(self, harness, contexts, selected_descriptors, sentinel_descriptor):
        require(type(contexts) is list and len(contexts) == 4, 'exactly four Machine descriptors required')
        require(type(selected_descriptors) is list and len(selected_descriptors) == 3, 'three selected Machines required')
        require(all(any(row is item for item in contexts) for row in selected_descriptors), 'selected Machine not in contexts')
        require(any(sentinel_descriptor is item for item in contexts), 'sentinel Machine not in contexts')
        require(not any(sentinel_descriptor is item for item in selected_descriptors), 'sentinel cannot be selected')
        require(len({_owner_key(row) for row in contexts}) == 4, 'Machine owners must be distinct')
        require(len({row['name'] for row in contexts}) == 4, 'Machine context names must be distinct')
        self.harness = harness
        self.contexts = [copy.deepcopy(row) for row in contexts]
        self.selected = [_owner_key(row) for row in selected_descriptors]
        self.sentinel = _owner_key(sentinel_descriptor)
        self.authority = fixture.resource_spec(contexts[0]['owner'], harness.info['run_id'])['authority']
        self.plugin_paths = {name: str(Path(harness.config) / 'cli-plugins' / ('docker-' + name)) for name in PLUGINS}
        self.docker = harness.info['clients']['docker']['canonical']
        self.cli = str(harness.cli)
        self._stores, self._empty, self._host_baseline = {}, {}, None
        self.proofs = []

    def __repr__(self):
        return '<RegistryCredentialControls>'

    # -- credential directories -------------------------------------------------

    def _descriptor(self, key):
        rows = [row for row in self.contexts if _owner_key(row) == key]
        require(len(rows) == 1, 'unknown Machine')
        return rows[0]

    def _store(self, key):
        if key not in self._stores:
            # Fresh random placeholder: never a real password, never reused.
            placeholder = os.urandom(24).hex().encode('ascii')
            try:
                self._stores[key] = credentials.Store(self._descriptor(key), plugin_paths=self.plugin_paths,
                    authority=self.authority, username=USERNAME, password=placeholder)
            except credentials.CredentialError:
                raise ControlError('registry controls rejected: Machine credential store admission') from None
        return self._stores[key]

    def _snapshot_empty(self, key):
        try:
            self._empty[key] = self._store(key).snapshot(expected='empty')
        except credentials.CredentialError:
            raise ControlError('registry controls rejected: Machine config is not in empty state') from None

    def _unchanged(self, key):
        require(key in self._empty, 'no empty baseline for Machine')
        try:
            proof = self._store(key).check_unchanged(self._empty[key])
        except credentials.CredentialError:
            raise ControlError('registry controls rejected: sibling/neighbor credential store changed') from None
        return proof

    def _auth_authorities(self, key):
        """Authority keys under auths only; values are never read into proofs."""
        path = Path(self._descriptor(key)['config_dir']) / 'config.json'
        try:
            raw, _ = credentials._file(path, credentials.LIMIT)
            parsed = credentials._decode(raw)
        except (credentials.CredentialError, OSError, ValueError):
            raise ControlError('registry controls rejected: unreadable Machine config') from None
        require(type(parsed) is dict and type(parsed.get('auths')) is dict, 'Machine config auths shape')
        return sorted(parsed['auths'])

    def distinctness(self):
        paths, realpaths, identities, rows = set(), set(), set(), []
        for row in self.contexts:
            config_dir = row['config_dir']
            require(type(config_dir) is str and Path(config_dir).is_absolute(), 'absolute config_dir required')
            info = os.lstat(config_dir)
            require(stat.S_ISDIR(info.st_mode), 'config_dir must be a directory, not a link')
            require(info.st_uid == os.geteuid(), 'config_dir not owned by current user')
            require(stat.S_IMODE(info.st_mode) & 0o077 == 0 and stat.S_IMODE(info.st_mode) <= 0o700,
                    'config_dir mode must be 0700 or tighter')
            paths.add(config_dir)
            realpaths.add(os.path.realpath(config_dir))
            identities.add((info.st_dev, info.st_ino))
            rows.append({'owner': copy.deepcopy(row['owner']), 'config_dir': config_dir, 'context': row['name'],
                         'realpath': os.path.realpath(config_dir), 'device': info.st_dev, 'inode': info.st_ino,
                         'mode': stat.S_IMODE(info.st_mode),
                         'role': 'sentinel' if _owner_key(row) == self.sentinel else 'selected'})
        require(len(paths) == 4 and len(realpaths) == 4 and len(identities) == 4, 'four distinct config directories required')
        return {'distinct_paths': True, 'distinct_realpaths': True, 'distinct_inodes': True,
                'same_authority_intentional': True, 'authority': self.authority, 'machines': rows}

    # -- host defaults -------------------------------------------------------------

    def host_state(self):
        home = os.environ.get('HOME')
        require(type(home) is str and Path(home).is_absolute(), 'HOME required for host default controls')
        docker_home = Path(home) / '.docker'
        files = {'home_config': str(docker_home / 'config.json')}
        if os.environ.get('DOCKER_CONFIG'):
            files['env_config'] = str(Path(os.environ['DOCKER_CONFIG']) / 'config.json')
        files['orbstack_context_meta'] = str(docker_home / 'contexts' / 'meta' / ORBSTACK_CONTEXT / 'meta.json')
        files['orbstack_context_tls'] = str(docker_home / 'contexts' / 'tls' / ORBSTACK_CONTEXT)
        files['harness_default_config'] = str(Path(self.harness.config) / 'config.json')
        states = {name: _lstat_state(path) for name, path in files.items()}
        helpers, directories = [], []
        for source in (os.environ.get('PATH', ''), self.harness.env.get('PATH', '')):
            for entry in source.split(os.pathsep):
                if entry and entry not in directories:
                    directories.append(entry)
        for entry in directories:
            try:
                names = sorted(os.listdir(entry))
            except OSError:
                continue
            for name in names:
                if name.startswith('docker-credential-'):
                    info = os.lstat(os.path.join(entry, name))
                    helpers.append({'name': name, 'directory': entry, 'size': info.st_size, 'mtime_ns': info.st_mtime_ns})
        plugins = Path(self.harness.config) / 'cli-plugins'
        return {'paths': files, 'states': states, 'credential_helpers_on_path': helpers,
                'shared_plugin_dir': str(plugins), 'shared_plugin_dir_digest': driver.tree_digest(plugins),
                'private_contents_published': False}

    def _host_unchanged(self):
        require(self._host_baseline is not None, 'baseline required first')
        current = self.host_state()
        require(current == self._host_baseline, 'host default/helper/shared plugin state changed')
        return {'host_defaults_unchanged': True, 'credential_helpers_unchanged': True, 'shared_plugins_unchanged': True}

    # -- phases ------------------------------------------------------------------

    def _publish(self, phase, proof):
        proof = dict(proof, phase=phase, schema_version=1, scope='cross_Machine_registry_credential_controls',
                     private_config_published=False, private_config_hash_published=False)
        self.proofs.append(copy.deepcopy(proof))
        return proof

    def baseline(self):
        require(self._host_baseline is None, 'baseline already recorded')
        distinct = self.distinctness()
        for row in self.contexts:
            self._snapshot_empty(_owner_key(row))
            require(self._auth_authorities(_owner_key(row)) == [], 'baseline auths must be empty')
        self._host_baseline = self.host_state()
        return self._publish('baseline', {'distinctness': distinct, 'host': self._host_baseline,
            'machines_empty': [copy.deepcopy(row['owner']) for row in self.contexts], 'all_four_empty': True})

    def check_after_login(self, active_descriptor):
        active = _owner_key(active_descriptor)
        require(active in self.selected, 'active Machine must be a selected Machine')
        require(self._auth_authorities(active) == [self.authority], 'active Machine must hold exactly the authority entry')
        siblings = []
        for row in self.contexts:
            key = _owner_key(row)
            if key == active:
                continue
            proof = self._unchanged(key)
            require(self._auth_authorities(key) == [], 'sibling/neighbor holds an auth entry')
            siblings.append({'owner': copy.deepcopy(row['owner']), 'role': 'sentinel' if key == self.sentinel else 'sibling',
                             'unchanged_since_empty_baseline': proof['unchanged'] is True, 'auth_entries': 0})
        return self._publish('after_login', {'active': copy.deepcopy(active_descriptor['owner']),
            'active_policy_check': 'delegated_to_Session_store', 'active_auth_authorities': [self.authority],
            'siblings': siblings, 'siblings_unchanged': True, **self._host_unchanged()})

    def check_after_logout(self, active_descriptor):
        active = _owner_key(active_descriptor)
        require(active in self.selected, 'active Machine must be a selected Machine')
        # Docker rewrites config.json on logout; identity differs but the
        # policy must be empty again. Refresh this Machine's empty baseline.
        self._snapshot_empty(active)
        rows = []
        for row in self.contexts:
            key = _owner_key(row)
            require(self._auth_authorities(key) == [], 'Machine holds an auth entry after logout')
            proof = self._unchanged(key)
            rows.append({'owner': copy.deepcopy(row['owner']), 'empty': True, 'unchanged': proof['unchanged'] is True,
                         'baseline_refreshed': key == active})
        return self._publish('after_logout', {'active': copy.deepcopy(active_descriptor['owner']),
            'machines': rows, 'all_four_empty': True, **self._host_unchanged()})

    def final(self):
        distinct = self.distinctness()
        rows = []
        for row in self.contexts:
            key = _owner_key(row)
            require(self._auth_authorities(key) == [], 'Machine holds an auth entry at final')
            proof = self._unchanged(key)
            # Empty-state config holds no secret: its content hash is publishable.
            raw, _ = credentials._file(Path(row['config_dir']) / 'config.json', credentials.LIMIT)
            rows.append({'owner': copy.deepcopy(row['owner']), 'config_dir': row['config_dir'],
                         'role': 'sentinel' if key == self.sentinel else 'selected', 'empty': True,
                         'unchanged_since_last_empty_baseline': proof['unchanged'] is True,
                         'empty_config_sha256': _sha(raw), 'empty_config_bytes': len(raw)})
        return self._publish('final', {'distinctness': distinct, 'machines': rows, 'all_four_empty': True,
                                       **self._host_unchanged()})

    # -- independent replay and scans -----------------------------------------

    def _private_receipts(self, session):
        output = Path(session.commands.output)
        require(output.is_dir() and not output.is_symlink(), 'private command receipt directory required')
        info = os.lstat(output)
        require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700, 'private receipt directory ownership')
        names = sorted(os.listdir(output))
        terminal = [name for name in names if re.fullmatch(r'command-[0-9]{4}-[a-z][a-z0-9-]{0,63}\.json', name)]
        intents = [name for name in names if name.endswith('.intent.json')]
        require(len(terminal) == len(intents) == len(session.commands.receipts), 'private receipt count mismatch')
        require(len(names) == 4 * len(terminal), 'unexpected files in private receipt directory')
        descriptor = session.descriptor
        owner = descriptor['owner']
        rows = []
        for position, name in enumerate(terminal):
            stem = name[:-len('.json')]
            require(stem + '.intent.json' in names and stem + '.stdout' in names and stem + '.stderr' in names,
                    'private receipt set incomplete')
            row = json.loads(_regular_bytes(output / name))
            live = session.commands.receipts[position]
            require(row == live, 'durable private receipt differs from live receipt')
            require(row['index'] == position + 1 and stem == 'command-' + str(row['index']).zfill(4) + '-' + row['label']
                    and row['durable_complete'] is True, 'private receipt identity')
            capture = row['capture']
            require(capture['effects_uncertain'] is False and capture['capture_complete'] is True and
                    capture['acknowledged'] is True and capture['owned_process_reaped'] is True and
                    capture['process_ownership_unresolved'] is False and capture['pending_process_retained'] is False and
                    capture['error'] is None and capture['stdin_write_complete'] is True and capture['stdin_eof_count'] == 1 and
                    type(capture['returncode']) is int and capture['returncode'] == capture['expected_exit'] and
                    capture['private_input_hash_published'] is False and capture['private_plan_published'] is False,
                    'private receipt not terminal/reaped/acknowledged')
            require(capture['environment'] == dict(self.harness.env) and capture['cwd'] == str(session.project),
                    'private receipt environment/cwd')
            stdout, stderr = _regular_bytes(output / (stem + '.stdout')), _regular_bytes(output / (stem + '.stderr'))
            require(_sha(stdout) == capture['expected_stdout_sha256'] and _sha(stderr) == capture['expected_stderr_sha256'],
                    'private receipt output identity')
            argv = capture['argv']
            if capture['executable'] == self.docker:
                require(argv[:5] == ['docker', '--config', descriptor['config_dir'], '--context', descriptor['name']],
                        'private docker receipt not pinned to active Machine config')
            else:
                require(capture['executable'] == self.cli and argv[:6] == [self.cli, 'exec', '--environment',
                        owner['environment_id'], '--machine', owner['machine_id']], 'private exec receipt not pinned to Machine')
            rows.append({'index': row['index'], 'label': row['label'], 'executable': capture['executable'],
                         'exit': capture['returncode'], 'stdout_sha256': _sha(stdout), 'stderr_sha256': _sha(stderr)})
        return rows

    def _binds(self, argv, descriptor):
        owner = descriptor['owner']
        if argv[:1] == ['docker']:
            return argv[1:5] == ['--config', descriptor['config_dir'], '--context', descriptor['name']]
        if argv[:2] == [self.cli, 'exec']:
            return argv[2:6] == ['--environment', owner['environment_id'], '--machine', owner['machine_id']]
        return False

    def _host_receipts(self, session):
        evidence = Path(self.harness.evidence)
        rows, files = [], []
        for row in self.harness.record.receipts:
            if not row['label'].startswith(LABEL_PREFIX):
                continue
            owners = [item for item in self.contexts if self._binds(row['argv'], item)]
            require(len(owners) == 1, 'registry host receipt not bound to exactly one Machine')
            if _owner_key(owners[0]) != _owner_key(session.descriptor):
                continue
            require(row['effects_uncertain'] is False and row['capture_complete'] is True and
                    type(row['exit_code']) is int and row['exit_code'] >= 0 and row['error'] is None and
                    not row.get('secret_leak_detected') and row['hashes_cover'] == 'complete_streams',
                    'registry host receipt not terminal or complete')
            require(row['executable'] in (self.docker, self.cli), 'registry host receipt executable not pinned')
            stem = str(row['index']).zfill(3) + '-' + row['label']
            result = json.loads(_regular_bytes(evidence / (stem + '.result.json')))
            require(result == row, 'durable host receipt differs from live receipt')
            require(json.loads(_regular_bytes(evidence / (stem + '.intent.json')))['index'] == row['index'], 'host intent index')
            stdout, stderr = _regular_bytes(evidence / (stem + '.stdout')), _regular_bytes(evidence / (stem + '.stderr'))
            require(_sha(stdout) == row['stdout_sha256'] and _sha(stderr) == row['stderr_sha256'] and
                    len(stdout) == row['retained_stdout_bytes'] and len(stderr) == row['retained_stderr_bytes'],
                    'host receipt stream identity')
            files.extend(evidence / (stem + suffix) for suffix in ('.result.json', '.intent.json', '.stdout', '.stderr'))
            rows.append({'index': row['index'], 'label': row['label'], 'executable': row['executable'],
                         'exit': row['exit_code'], 'stdout_sha256': row['stdout_sha256'], 'stderr_sha256': row['stderr_sha256']})
        for mutation in self.harness.mutations:
            if not (mutation['label'].startswith(LABEL_PREFIX) and mutation['owner'] == session.descriptor['owner']):
                continue
            stem = 'mutation-' + str(mutation['index']).zfill(3)
            result = json.loads(_regular_bytes(evidence / (stem + '.result.json')))
            require(result['effects_uncertain'] is False and result['error'] is None and result['exit_code'] == 0 and
                    result['label'] == mutation['label'], 'registry mutation lacks certain completion')
            files.extend(evidence / (stem + suffix) for suffix in ('.result.json', '.intent.json'))
        return rows, files

    def _scan(self, roots, files, variants):
        scanned, total = [], 0
        seen = set()
        for path in list(files) + [item for root in roots for item in _walk(root)]:
            key = os.path.realpath(path)
            if key in seen:
                continue
            seen.add(key)
            raw = _regular_bytes(path)
            require(not scan_bytes(raw, variants), 'private canary or reversible encoding found in retained evidence')
            total += len(raw)
            scanned.append({'path': str(path), 'bytes': len(raw), 'sha256': _sha(raw)})
        return {'files': scanned, 'file_count': len(scanned), 'byte_count': total}

    def replay_and_scan(self, session, output_dir):
        output_dir = Path(output_dir)
        require(output_dir.is_absolute(), 'absolute output directory required')
        require(session.credential_state == 'login', 'Session must still hold its login before replay')
        require(session.workload_complete is True and session.cleanup_complete is False and not session.failed,
                'Session must be complete, uncleaned and unfailed')
        require(any(session.descriptor is row or session.descriptor == row for row in self.contexts) and
                _owner_key(session.descriptor) in self.selected, 'Session Machine must be selected')
        try:
            session.store.snapshot(expected='login')
        except credentials.CredentialError:
            raise ControlError('registry controls rejected: Session store is not in login state') from None
        preservation = self.check_after_login(session.descriptor)
        private_rows = self._private_receipts(session)
        host_rows, host_files = self._host_receipts(session)
        require(private_rows and host_rows, 'replay requires private and host receipts')
        variants = canary_variants(session.private_fixture)
        config_dir = Path(session.descriptor['config_dir'])
        # config.json legitimately holds the auth blob in login state; scan the
        # rest of the private client directory for stray caches or logs.
        config_files = [path for path in _walk(config_dir) if path != config_dir / 'config.json']
        scan = self._scan([session.output, session.commands.output, output_dir], host_files + config_files, variants)
        proof = {'schema_version': 1, 'scope': 'independent_registry_replay_and_leak_scan',
                 'owner': copy.deepcopy(session.descriptor['owner']),
                 'independent_command_replay_complete': True,
                 'private_receipts': private_rows, 'host_receipts': host_rows,
                 'private_receipt_count': len(private_rows), 'host_receipt_count': len(host_rows),
                 'pinned_docker_executable': self.docker, 'pinned_cli_executable': self.cli,
                 'canary_variant_count': len(variants), 'canary_encodings': ['raw', 'base64', 'urlsafe_base64', 'hex',
                     'percent', 'json_escaped', 'docker_auth_blob'],
                 'scan': scan, 'no_canary_found': True,
                 'scanned_roots': [str(session.output), str(session.commands.output), str(output_dir), str(config_dir)],
                 'login_state_preserved_before_cleanup': True, 'siblings_preserved': preservation['siblings_unchanged'],
                 'host_defaults_preserved': preservation['host_defaults_unchanged'],
                 'private_config_published': False, 'canary_values_published': False,
                 'release_certified': False}
        require(not driver.contains_canary((json.dumps(proof).encode(),), variants), 'proof rejected')
        self.proofs.append(copy.deepcopy(proof))
        return proof
