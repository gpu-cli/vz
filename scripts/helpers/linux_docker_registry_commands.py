"""Registered, durable public receipts for source-selected private stdin.

Never persists the internal input plan, private bytes, unexpected output or
private hashes. Caller owns registry operation semantics and Machine guards;
this owner adds exact routing/pins and a sticky transport/durability fence.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat

import linux_docker_artifact_stream as artifact
import linux_docker_private_stdin as private_stdin


class CommandError(ValueError):
    pass


def require(condition):
    if not condition:
        raise CommandError('private registry command rejected')


def _identity(path):
    with artifact._opened(path, directory=True) as fd:
        info = os.fstat(fd)
        require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700)
        return info.st_dev, info.st_ino


def _hash(path):
    with artifact._opened(path) as fd:
        before = os.fstat(fd)
        require(0 < before.st_size <= 256 * 1024 * 1024 and before.st_mode & 0o111)
        digest, count = hashlib.sha256(), 0
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            count += len(chunk)
            require(count <= before.st_size)
            digest.update(chunk)
        require(count == before.st_size)
    return digest.hexdigest()


class Commands:
    def __init__(self, harness, descriptor, project, index):
        self._failed = True
        self.harness, self.owners, self.receipts = harness, [], []
        if not hasattr(harness, 'registry_commands'):
            harness.registry_commands = []
        require(type(harness.registry_commands) is list and type(index) is int and 0 <= index <= 3)
        self._descriptor, self._env = copy.deepcopy(descriptor), dict(harness.env)
        self._project = Path(project)
        require(self._project.is_absolute() and str(self._project) == os.fspath(project))
        # Pin project namespace identity but do not require private project mode.
        with artifact._opened(self._project, directory=True) as fd:
            info = os.fstat(fd); self._project_identity = (info.st_dev, info.st_ino)
        self._cli = str(harness.cli)
        docker = harness.info['clients']['docker']
        self._docker = docker['canonical']
        self._pins = {self._cli: harness.staged_inputs[self._cli], self._docker: docker['sha256']}
        require(harness.info['inputs'].get(self._docker) == docker['sha256'])
        self.output = Path(harness.evidence) / ('registry-private-' + str(index))
        require(not os.path.lexists(self.output))
        # Register before any evidence write or possible future dispatch.
        harness.registry_commands.append(self)
        try:
            self.output.mkdir(mode=0o700)
            self._output_identity = _identity(self.output)
            self._guard()
            self._failed = False
        except BaseException:
            raise CommandError('private command owner admission failed') from None

    def _guard(self):
        h = self.harness
        require(sum(owner is self for owner in h.registry_commands) == 1 and h.env == self._env
                and sum(row == self._descriptor for row in h.descriptors) == 1)
        require(h.staged_inputs.get(self._cli) == self._pins[self._cli]
                and str(h.cli) == self._cli and h.info['inputs'].get(self._docker) == self._pins[self._docker]
                and h.info['clients']['docker']['canonical'] == self._docker)
        require(h.info['clients']['docker']['sha256'] == self._pins[self._docker])
        require(_identity(self.output) == self._output_identity)
        with artifact._opened(self._project, directory=True) as fd:
            info = os.fstat(fd)
            require((info.st_dev, info.st_ino) == self._project_identity)
        for path, expected in self._pins.items():
            require(_hash(Path(path)) == expected)

    def _persist(self, filename, raw):
        require(_identity(self.output) == self._output_identity)
        path = self.output / filename
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        with artifact._opened(self.output, directory=True) as directory:
            os.fsync(directory)
        with artifact._opened(path) as descriptor:
            info = os.fstat(descriptor)
            require(info.st_size == len(raw) and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600)
            retained = bytearray()
            while len(retained) <= len(raw):
                block = os.read(descriptor, min(65536, len(raw) + 1 - len(retained)))
                if not block: break
                retained.extend(block)
            require(bytes(retained) == raw)

    def _document(self, filename, value):
        self._persist(filename, json.dumps(value, sort_keys=True, allow_nan=False).encode() + b'\n')

    def private(self, label, argv, *, executable, private_input, expected_stdout,
                expected_stderr=b'', expected_exit=0, timeout=30):
        try:
            self.assert_certain()
            require(type(label) is str and re.fullmatch('[a-z][a-z0-9-]{0,63}', label)
                    and not any(row['label'] == label for row in self.receipts))
            require(type(private_input) is bytes and not any(value in label.encode('ascii')
                    for value in private_stdin._variants(private_input)))
            executable = os.fspath(executable)
            require(type(argv) is list and executable in self._pins)
            if executable == self._docker:
                require(argv[:5] == ['docker', '--config', self._descriptor['config_dir'],
                                     '--context', self._descriptor['name']] and len(argv) > 5)
            else:
                owner = self._descriptor['owner']
                require(argv[:6] == [self._cli, 'exec', '--environment', owner['environment_id'],
                                     '--machine', owner['machine_id']] and len(argv) > 6)
            capture = private_stdin.Capture(argv, executable=executable, cwd=self._project, env=self._env,
                private_input=private_input, expected_stdout=expected_stdout, expected_stderr=expected_stderr,
                expected_exit=expected_exit, timeout_seconds=timeout)
            self._failed = True
            self.owners.append(capture)
            row = {'index': len(self.owners), 'label': label, 'capture': capture.receipt, 'durable_complete': False}
            self.receipts.append(row)
            stem = 'command-' + str(row['index']).zfill(4) + '-' + label
            self._document(stem + '.intent.json', copy.deepcopy(row))
            result = capture.run()
            row['capture'] = capture.receipt
            require(type(result.stdout) is bytes and type(result.stderr) is bytes
                    and result.stdout in (b'', expected_stdout) and result.stderr in (b'', expected_stderr))
            self._persist(stem + '.stdout', result.stdout)
            self._persist(stem + '.stderr', result.stderr)
            self._guard()
            row['durable_complete'] = True
            self._document(stem + '.json', copy.deepcopy(row))
            require(result.receipt == capture.receipt and result.receipt['acknowledged'] is True
                    and result.receipt['effects_uncertain'] is False and result.pending_process is None)
            self._failed = False
            self.assert_certain()
            return result
        except BaseException:
            self._failed = True
            raise CommandError('private registry command incomplete') from None

    def assert_certain(self):
        try:
            require(not self._failed and len(self.owners) == len(self.receipts))
            self._guard()
            for owner, row in zip(self.owners, self.receipts):
                require(row['durable_complete'] is True and owner.pending_process is None
                        and owner.receipt == row['capture'] and owner.receipt['acknowledged'] is True
                        and owner.receipt['owned_process_reaped'] is True and owner.receipt['effects_uncertain'] is False)
        except BaseException:
            self._failed = True
            raise CommandError('private registry commands remain uncertain') from None
