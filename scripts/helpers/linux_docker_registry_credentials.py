"""Private, read-only admission of mutable Machine Docker file credentials.

The descriptor and plugin executable identities must already be authenticated
by the caller. This module checks the current private store, not a Docker login
or TLS handshake. Config bytes and their hashes are never public evidence.
Reads are quiescent/coherent, not a hostile same-UID tamper-resistance claim.
"""
import base64
import copy
import hmac
import json
import os
from pathlib import Path
import re
import stat

import linux_docker_artifact_stream as artifact

LIMIT = 1024 * 1024
GUARD = {'vz-managed-file-store.invalid': ''}


class CredentialError(ValueError):
    """Static errors only; no private data, paths or decoder exceptions."""


def require(condition):
    if not condition:
        raise CredentialError('private credential admission rejected')


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result)
            result[key] = value
        return result
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: require(False))
    except (ValueError, UnicodeError, RecursionError):
        raise CredentialError('private credential JSON rejected') from None


def _file(path, maximum):
    with artifact._opened(path) as fd:
        info = os.fstat(fd)
        require(stat.S_IMODE(info.st_mode) == 0o600 and info.st_uid == os.geteuid()
                and info.st_nlink == 1 and 0 < info.st_size <= maximum)
        raw = bytearray()
        while len(raw) <= info.st_size:
            chunk = os.read(fd, min(65536, info.st_size + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        require(len(raw) == info.st_size)
        signature = (info.st_dev, info.st_ino, info.st_mode, info.st_uid,
                     info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    return bytes(raw), signature


class Snapshot:
    __slots__ = ('_store', '_raw', '_signature', '_policy', '_state')

    def __init__(self, store, raw, signature, policy, state):
        self._store, self._raw, self._signature = store, raw, signature
        self._policy, self._state = policy, state

    def __repr__(self):
        return '<PrivateCredentialSnapshot>'


class Store:
    __slots__ = ('_descriptor', '_path', '_plugins', '_authority', '_credential',
                 '_directory', '_claim', '_claim_signature', '_failed')

    def __init__(self, descriptor, *, plugin_paths, authority, username, password):
        try:
            require(type(descriptor) is dict and type(descriptor.get('owner')) is dict)
            owner = descriptor['owner']
            require(set(owner) == {'project_id', 'environment_id', 'machine_id'} and all(
                type(value) is str and re.fullmatch('[A-Za-z0-9_-]{1,128}', value) for value in owner.values()))
            require(type(descriptor.get('config_dir')) is str)
            self._path = Path(descriptor['config_dir'])
            require(str(self._path) == descriptor['config_dir'] and self._path.is_absolute()
                    and self._path.name == 'docker-client')
            require(type(plugin_paths) is dict and set(plugin_paths) == {'compose', 'buildx'} and all(
                type(value) is str and Path(value).is_absolute() and str(Path(value)) == value and
                '..' not in Path(value).parts for value in plugin_paths.values()))
            self._plugins = sorted({str(Path(value).parent) for value in plugin_paths.values()})
            require(type(authority) is str and re.fullmatch('[a-zA-Z0-9.-]+:[0-9]{1,5}', authority)
                    and 1 <= int(authority.rsplit(':', 1)[1]) <= 65535)
            require(type(username) is str and re.fullmatch('[a-zA-Z0-9_-]{1,128}', username))
            require(type(password) is bytes and re.fullmatch(b'[0-9a-f]{32,256}', password))
            self._credential = username.encode('ascii') + b':' + password
            self._authority, self._descriptor = authority, copy.deepcopy(descriptor)
            self._directory = self._claim = self._claim_signature = None
            self._failed = False
            self._observe()
        except (OSError, artifact.ArtifactError, ValueError, TypeError, KeyError):
            raise CredentialError('private credential store rejected') from None

    def __repr__(self):
        return '<PrivateCredentialStore>'

    def _observe(self):
        with artifact._opened(self._path, directory=True) as fd:
            info = os.fstat(fd)
            require(stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.geteuid())
            directory = (info.st_dev, info.st_ino, info.st_uid, stat.S_IMODE(info.st_mode))
            claim, claim_signature = _file(self._path / 'vz-owner.json', 16384)
            parsed = _decode(claim)
            require(type(parsed) is dict and set(parsed) == {'schema_version', 'owner', 'nonce', 'directory'}
                    and type(parsed['schema_version']) is int and parsed['schema_version'] == 1
                    and parsed['owner'] == self._descriptor['owner'] and type(parsed['nonce']) is str
                    and re.fullmatch('lop_[0-9a-f]{32}', parsed['nonce'])
                    and type(parsed['directory']) is dict and set(parsed['directory']) == {'device', 'inode'}
                    and all(type(value) is int for value in parsed['directory'].values())
                    and parsed['directory'] == {'device': info.st_dev, 'inode': info.st_ino})
            if self._directory is not None:
                require(directory == self._directory and claim == self._claim and claim_signature == self._claim_signature)
            raw, signature = _file(self._path / 'config.json', LIMIT)
            # Exact repeated raw-byte/identity reads, with claim checked again,
            # reject replacement during observation but permit between phases.
            require(_file(self._path / 'vz-owner.json', 16384) == (claim, claim_signature))
            require(_file(self._path / 'config.json', LIMIT) == (raw, signature))
        if self._directory is None:
            self._directory, self._claim, self._claim_signature = directory, claim, claim_signature
        return raw, signature

    def _policy(self, raw, expected):
        require(expected in ('empty', 'login'))
        parsed = _decode(raw)
        require(type(parsed) is dict and {'auths', 'credHelpers'} <= set(parsed) <=
                {'auths', 'credHelpers', 'currentContext', 'cliPluginsExtraDirs'})
        require(parsed['credHelpers'] == GUARD and type(parsed['auths']) is dict)
        require(parsed.get('currentContext', 'default') == 'default')
        require(parsed.get('cliPluginsExtraDirs') == self._plugins)
        auths = parsed['auths']
        if expected == 'empty':
            require(auths == {})
        else:
            require(set(auths) == {self._authority})
            value = auths[self._authority]
            require(type(value) is dict and set(value) == {'auth'} and type(value['auth']) is str)
            encoded = value['auth'].encode('ascii')
            decoded = base64.b64decode(encoded, validate=True)
            require(hmac.compare_digest(base64.b64encode(decoded), encoded)
                    and hmac.compare_digest(decoded, self._credential))
        return {key: value for key, value in parsed.items() if key != 'auths'}

    def snapshot(self, *, expected):
        try:
            require(not self._failed)
            raw, signature = self._observe()
            policy = self._policy(raw, expected)
            return Snapshot(self, raw, signature, policy, expected)
        except (OSError, artifact.ArtifactError, ValueError, UnicodeError, TypeError, KeyError):
            self._failed = True
            raise CredentialError('private credential snapshot rejected') from None

    def _check(self, previous, expected, unchanged):
        try:
            require(type(previous) is Snapshot and previous._store is self and not self._failed)
            if not unchanged:
                require((previous._state, expected) in (('empty', 'login'), ('login', 'empty')))
            current = self.snapshot(expected=expected)
            require(current._policy == previous._policy)
            if unchanged:
                require(current._raw == previous._raw and current._signature == previous._signature)
            return {'schema_version': 1, 'scope': 'private_Machine_file_credentials_only',
                'owner': copy.deepcopy(self._descriptor['owner']), 'config_dir': str(self._path),
                'expected_state': expected, 'unchanged': unchanged, 'specific_auth_transition': not unchanged,
                'directory_and_claim_unchanged': True, 'private_config_published': False,
                'private_config_hash_published': False, 'registry_authentication_certified': False}
        except (ValueError, TypeError, AttributeError):
            self._failed = True
            raise CredentialError('private credential check rejected') from None

    def check_unchanged(self, snapshot):
        return self._check(snapshot, snapshot._state if type(snapshot) is Snapshot else None, True)

    def check_transition(self, snapshot, *, expected):
        return self._check(snapshot, expected, False)
