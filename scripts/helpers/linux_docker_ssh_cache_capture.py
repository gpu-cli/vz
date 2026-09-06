"""One-shot stopped-builder cache quarantine; no restart, extraction or repair.

Only a complete, identity-bound, scanner-approved archive is published. Failed
bytes remain in the caller's private quarantine, outside evidence. Host capture
processes have their own session; termination never targets a guest process.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import time

import docker_host_driver as driver
import linux_docker_artifact_stream as stream
import linux_docker_buildkit_builder as buildkit
import linux_docker_buildkit_shutdown as shutdown
import linux_docker_image_input as image_input
import linux_docker_ssh_cache as cache
from linux_docker_ssh_agent import _Root

TIMEOUT = 180
STDERR_LIMIT = 65536
CHUNK = 65536


class CaptureError(ValueError):
    """Static diagnostic, never arbitrary process/archive content."""


def require(ok, code):
    if not ok:
        raise CaptureError(code)


def metadata(info):
    return tuple(getattr(info, 'st_' + key) for key in
                 ('dev', 'ino', 'mode', 'nlink', 'uid', 'gid', 'size', 'mtime_ns', 'ctime_ns'))


def terminate_owned(process, owned_pid):
    """Signal only while the unreaped direct child still pins the owned PID.

    No poll precedes this check: polling can reap an exited leader while a
    descendant still holds a pipe. A reaped leader's number is no longer an
    ownership handle and must never be used to signal a potentially reused
    process group. This Popen is local to the capture thread until failure.
    If the OS cannot report an exited-but-unreaped leader's group/session,
    fail closed and retain the handle; do not infer a safe group from its PID.
    """
    if process.returncode is not None:
        return False
    require(type(owned_pid) is int and owned_pid > 0 and process.pid == owned_pid,
            'capture_child_identity_changed')
    require(os.getpgid(owned_pid) == owned_pid and os.getsid(owned_pid) == owned_pid,
            'capture_process_group_identity_changed')
    try:
        os.killpg(owned_pid, signal.SIGKILL)
    except ProcessLookupError:
        # The unreaped leader still prevents PID reuse; an empty/dead group
        # requires only reaping, not another signal or a broader target.
        pass
    process.wait(timeout=5)
    return True


def capture_process(argv, *, executable, environment, cwd, descriptor, canaries,
                    maximum, timeout=TIMEOUT, stderr_limit=STDERR_LIMIT, stderr_descriptor=None):
    """Bounded selector transport; stdout is never accumulated in memory."""
    require(type(maximum) is int and maximum > 0 and type(timeout) in (int, float) and timeout > 0 and
            type(stderr_limit) is int and stderr_limit > 0, 'invalid_capture_limits')
    require(not driver.contains_canary((json.dumps(argv).encode(), json.dumps(environment).encode()), canaries),
            'private_capture_arguments')
    start = time.monotonic_ns()
    deadline = time.monotonic() + timeout
    process = owned_pid = None
    stderr, count, digest = bytearray(), 0, hashlib.sha256()
    scanner = stream.CanaryScanner(canaries)
    error = None
    try:
        process = subprocess.Popen(argv, executable=executable, cwd=cwd, env=environment,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            close_fds=True, start_new_session=True)
        owned_pid = process.pid
        with selectors.DefaultSelector() as selector:
            for index, pipe in enumerate((process.stdout, process.stderr)):
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, index)
            while selector.get_map():
                left = deadline - time.monotonic()
                require(left > 0, 'capture_deadline')
                for key, _ in selector.select(min(left, .25)):
                    block = os.read(key.fd, CHUNK)
                    if not block:
                        selector.unregister(key.fileobj)
                        continue
                    if key.data == 0:
                        count += len(block)
                        require(count <= maximum, 'capture_archive_limit')
                        scanner.feed(block)
                        digest.update(block)
                        offset = 0
                        while offset < len(block):
                            written = os.write(descriptor, block[offset:])
                            require(written > 0, 'capture_write_incomplete')
                            offset += written
                    else:
                        require(len(stderr) + len(block) <= stderr_limit, 'capture_stderr_limit')
                        stderr.extend(block)
                        if stderr_descriptor is not None:
                            offset = 0
                            while offset < len(block):
                                written = os.write(stderr_descriptor, block[offset:])
                                require(written > 0, 'capture_stderr_write_incomplete')
                                offset += written
            code = process.wait(timeout=max(.001, deadline - time.monotonic()))
        os.fsync(descriptor)
        if stderr_descriptor is not None:
            os.fsync(stderr_descriptor)
        require(not driver.contains_canary((bytes(stderr),), canaries), 'capture_stderr_canary')
        require(code == 0 and count > 0 and not stderr, 'capture_command_unsuccessful')
        return {'pid': process.pid, 'process_group': process.pid, 'session_id': process.pid,
                'exit_code': code, 'elapsed_ns': time.monotonic_ns() - start,
                'size': count, 'sha256': digest.hexdigest(), 'stderr_sha256': driver.sha256(bytes(stderr)),
                'stderr_bytes': len(stderr), 'capture_complete': True, 'owned_process_reaped': True}
    except BaseException as original:
        error = original
        raise
    finally:
        if process is not None:
            try:
                # A still-unreaped leader pins its number even if already a
                # zombie with pipe-holding descendants. A completed wait does
                # not: terminate_owned refuses to signal that reaped PID.
                if error is not None or process.returncode is None:
                    terminate_owned(process, owned_pid)
            except BaseException as cleanup_error:
                if error is not None:
                    error.capture_cleanup_error = type(cleanup_error).__name__
                    error.capture_pending_process = process
                    error.capture_pending_pid = owned_pid
                else:
                    cleanup_error.capture_pending_process = process
                    cleanup_error.capture_pending_pid = owned_pid
                    raise
            finally:
                for pipe in (process.stdout, process.stderr):
                    if pipe is not None:
                        pipe.close()
        if error is not None:
            error.capture_observation = {'pid': process.pid if process is not None else None,
                'exit_code': process.returncode if process is not None else None,
                'elapsed_ns': time.monotonic_ns() - start, 'observed_stdout_bytes': count,
                'quarantined_stdout_bytes': os.fstat(descriptor).st_size,
                'quarantined_stderr_bytes': len(stderr), 'capture_complete': False,
                'owned_process_reaped': process is not None and process.returncode is not None,
                'pending_process_pid': getattr(error, 'capture_pending_pid', None)}


def stopped_binding(item, proof, owner):
    state = item['State']
    require(item['Id'] == owner['mapping']['container_id'] and item['Image'] == owner['mapping']['image_id'] and
            item['Name'] == '/' + owner['container_name'] and
            item['Config']['Labels'] == {buildkit.LABEL: owner['token']} and
            item['Config']['Env'] == buildkit.ENV and item['Config']['Entrypoint'] == ['/usr/bin/buildkitd'] and
            item['Config']['Cmd'] == buildkit.FLAGS, 'foreign_stopped_builder')
    host = item['HostConfig']
    require(host['Runtime'] == 'youki' and host['Privileged'] is True and host['Init'] is True and
            host['CgroupnsMode'] == 'private' and host['NetworkMode'] == 'bridge' and
            not host.get('Binds') and not host.get('PortBindings') and host['RestartPolicy']['Name'] == 'no',
            'stopped_builder_policy_changed')
    require(type(item['RestartCount']) is int and item['RestartCount'] == 0 and
            state['Running'] is False and state['Status'] == 'exited' and type(state['Pid']) is int and state['Pid'] == 0 and
            type(state['ExitCode']) is int and state['ExitCode'] == 1 and state['Error'] == '' and
            all(state[k] is False for k in ('Paused', 'Restarting', 'OOMKilled', 'Dead')), 'builder_not_quiescent')
    expected = {'container_id': item['Id'], 'started_at': state['StartedAt'], 'finished_at': state['FinishedAt'],
                'owner': owner['descriptor']['owner'], 'context': owner['descriptor']['name'],
                'role': owner['role'], 'identity_sha256': owner['identity_sha256'],
                'engine_id': owner['descriptor']['engine_id'], 'source_commit': shutdown.SOURCE_COMMIT,
                'buildkitd_sha256': shutdown.DAEMON_SHA256, 'exit_code': 1, 'signal': 'SIGTERM',
                'scope': 'PINNED_BUILDKIT_ONE_SIGTERM_NORMAL_EXIT_NOT_FILESYSTEM_CLOSURE'}
    require(all(proof.get(key) == value for key, value in expected.items()), 'normal_stop_receipt_mismatch')
    require(shutdown.timestamp(state['StartedAt']) <= shutdown.timestamp(proof['engine_since']) <=
            shutdown.timestamp(state['FinishedAt']) <= shutdown.timestamp(proof['engine_until']),
            'normal_stop_clock_mismatch')
    mounts = item['Mounts']
    require(len(mounts) == 1 and mounts[0]['Type'] == 'volume' and mounts[0]['Name'] == owner['volume_name'] and
            mounts[0]['Destination'] == '/var/lib/buildkit' and mounts[0]['Source'] == owner['volume']['Mountpoint'] and
            mounts[0]['RW'] is True, 'foreign_builder_cache_mount')


class Capture:
    def __init__(self, builder, canaries, private_root, evidence_root):
        private_root, evidence_root = Path(private_root), Path(evidence_root)
        require(private_root != evidence_root and private_root not in evidence_root.parents and
                evidence_root not in private_root.parents, 'quarantine_must_be_outside_evidence')
        self.builder = builder
        self.owner = self.snapshot()
        self.canaries = tuple(canaries)
        require(bool(self.canaries), 'private_key_canaries_required')
        stream.CanaryScanner(self.canaries)
        h = builder.harness
        require(Path(h.evidence) != private_root and Path(h.evidence) not in private_root.parents,
                'quarantine_inside_harness_evidence')
        self.environment = dict(h.env)
        require(not set(self.environment).intersection({'SSH_AUTH_SOCK', 'SSH_AGENT_PID', 'DOCKER_HOST', 'DOCKER_CONTEXT'}),
                'foreign_agent_or_docker_environment')
        self.client = copy.deepcopy(h.info['clients']['docker'])
        self.executable = Path(self.client['canonical'])
        self.config = Path(self.owner['descriptor']['config_dir'])
        self.private = _Root(private_root)
        try:
            self.evidence = _Root(evidence_root)
        except BaseException:
            self.private.release()
            raise
        self.record = driver.Recorder(evidence_root, self.environment, list(self.canaries), max_stream_bytes=1024*1024)
        self.attempted = False
        self.config_digest = None
        self.proof = None
        self.pending_process = None

    def snapshot(self):
        b = self.builder
        return copy.deepcopy({'descriptor': b.descriptor, 'mapping': b.mapping, 'role': b.role, 'token': b.token,
            'identity_sha256': b.identity_sha256, 'container_name': b.container_name, 'volume_name': b.volume_name,
            'volume': b.volume, 'ownership': b.ownership, 'image_tag': b.tag})

    def document(self, name, value):
        data = (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()
        require(not driver.contains_canary((data,), self.canaries), 'private_evidence_rejected')
        self.evidence.write(name, data)

    def local_guard(self):
        self.private.check(); self.evidence.check()
        require(self.snapshot() == self.owner and self.builder.registered is False and
                dict(self.builder.harness.env) == self.environment, 'capture_owner_or_environment_changed')
        require(self.executable == self.executable.resolve(strict=True) and
                stream.scan_file(self.executable)['sha256'] == self.client['sha256'], 'capture_client_changed')
        require(self.config == self.config.resolve(strict=True) and
                driver.tree_digest(self.config) == self.config_digest, 'capture_client_config_changed')

    def argv(self, args):
        return ['docker', '--config', str(self.config), '--context', self.owner['descriptor']['name'], *args]

    def command(self, args):
        self.local_guard()
        result = self.record.run(self.argv(args), executable=str(self.executable), timeout=20, mutation=False)
        require(result.returncode == 0 and not result.timed_out and not result.stderr, 'capture_guard_command_failed')
        return result.stdout

    def object(self, kind, value):
        rows = image_input.parse(self.command([kind, 'inspect', value]))
        require(type(rows) is list and len(rows) == 1, 'ambiguous_capture_object')
        return rows[0]

    def guard(self, stopped, proof):
        owner = self.owner
        descriptor = owner['descriptor']
        context = self.object('context', descriptor['name'])
        require(context['Name'] == descriptor['name'] and
                context['Endpoints']['docker']['Host'] == descriptor['endpoint'] and
                not context['Endpoints']['docker'].get('SkipTLSVerify'), 'capture_context_rerouted')
        info = image_input.parse(self.command(['info', '--format', '{{json .}}']))
        require(info['ID'] == descriptor['engine_id'] and info['OSType'] == 'linux' and
                info['Architecture'] in ('arm64', 'aarch64') and info['DefaultRuntime'] == 'youki' and
                info['Runtimes']['youki']['path'] == '/mnt/linux-bin/youki', 'capture_engine_changed')
        current = self.object('container', owner['mapping']['container_id'])
        stopped_binding(current, proof, owner)
        keys = ('Id', 'Image', 'Name', 'Config', 'HostConfig', 'Mounts', 'State', 'RestartCount')
        require({key: current[key] for key in keys} == {key: stopped[key] for key in keys}, 'stopped_builder_changed')
        image = self.object('image', owner['image_tag'])
        require(image['Id'] == owner['mapping']['image_id'] and image['Config']['Labels'] == {buildkit.LABEL: owner['token']},
                'capture_builder_image_changed')
        volume = buildkit.volume_identity(self.object('volume', owner['volume_name']), owner['volume_name'], owner['token'])
        require(volume == owner['volume'], 'capture_volume_changed')
        references = self.command(['container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'volume=' + owner['volume_name']])
        require(references == (owner['mapping']['container_id'] + '\n').encode(), 'foreign_cache_volume_reference')
        self.local_guard()

    def run(self, stopped_inspect, stop_proof):
        require(not self.attempted, 'cache_capture_cannot_repeat')
        self.attempted = True
        fd = stderr_fd = None
        receipt = {'schema_version': 1, 'host_outcome': 'not_dispatched', 'capture_complete': False,
                   'effects_uncertain': True, 'archive_published': False}
        try:
            stopped, proof = copy.deepcopy(stopped_inspect), copy.deepcopy(stop_proof)
            self.config_digest = driver.tree_digest(self.config)
            self.local_guard()
            require(os.fstat(self.private.fd).st_dev == os.fstat(self.evidence.fd).st_dev,
                    'capture_promotion_requires_same_filesystem')
            stopped_binding(stopped, proof, self.owner)
            self.guard(stopped, proof)
            name = 'cache.quarantine.tar'
            fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.private.fd)
            stderr_fd = os.open('capture.stderr', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                0o600, dir_fd=self.private.fd)
            os.fsync(self.private.fd)
            argv = self.argv(['cp', self.owner['mapping']['container_id'] + ':/var/lib/buildkit/.', '-'])
            receipt.update(argv=argv, executable=str(self.executable), environment=self.environment,
                           cwd=str(self.private.path), started_unix_ns=time.time_ns(),
                           timeout_seconds=TIMEOUT, maximum_archive_bytes=cache.Limits().archive_bytes,
                           stderr_limit=STDERR_LIMIT, termination_scope='owned_host_process_group')
            self.document('capture.intent.json', dict(receipt, owner=self.owner, normal_stop=proof,
                                                     quarantine=str(self.private.path / name)))
            result = capture_process(argv, executable=str(self.executable), environment=self.environment,
                cwd=self.private.path, descriptor=fd, stderr_descriptor=stderr_fd,
                canaries=self.canaries, maximum=cache.Limits().archive_bytes)
            receipt.update(result, host_outcome='exited', effects_uncertain=False)
            self.document('capture.result.json', receipt)
            signature = metadata(os.fstat(fd))
            require(stat.S_ISREG(os.fstat(fd).st_mode) and os.fstat(fd).st_nlink == 1 and
                    os.fstat(fd).st_size == result['size'], 'quarantine_capture_metadata_changed')
            self.guard(stopped, proof)
            scan = cache.scan(self.private.path / name, canaries=self.canaries)
            require(scan['complete'] is True and scan['archive'] == {key: result[key] for key in ('size', 'sha256')},
                    'cache_scan_capture_mismatch')
            self.guard(stopped, proof)
            require(metadata(os.fstat(fd)) == signature and
                    metadata(os.stat(name, dir_fd=self.private.fd, follow_symlinks=False)) == signature,
                    'quarantine_changed_after_scan')
            require(all(row['effects_uncertain'] is False and row.get('capture_complete') is True and
                        row.get('secret_leak_detected') is False and row.get('raw_streams_retained') is True
                        for row in self.record.receipts), 'capture_guard_receipts_incomplete')
            self.document('cache-scan.json', scan)
            # link() is exclusive (no overwrite); the source is already scanned
            # and its pinned inode checked. Drop only that authenticated private
            # name, yielding a single-link regular evidence file for replay.
            os.link(name, 'cache.tar', src_dir_fd=self.private.fd, dst_dir_fd=self.evidence.fd, follow_symlinks=False)
            require(os.stat('cache.tar', dir_fd=self.evidence.fd, follow_symlinks=False).st_ino == os.fstat(fd).st_ino,
                    'cache_promotion_identity_mismatch')
            os.unlink(name, dir_fd=self.private.fd)
            os.fsync(self.private.fd); os.fsync(self.evidence.fd)
            receipt['archive_published'] = True
            self.proof = {'schema_version': 1, 'scope': 'owned_stopped_builder_cache_capture_and_literal_canary_scan',
                          'owner': self.owner, 'normal_stop': proof, 'archive': dict(scan['archive'], filename='cache.tar'),
                          'scan': scan, 'capture': dict(receipt), 'builder_restarted': False,
                          'guard_command_count': len(self.record.receipts), 'guard_receipts_complete': True,
                          'guest_mutation_requested': False, 'broader_cleanup_authorized': False}
            self.document('cache-capture.json', self.proof)
            return self.proof
        except BaseException as error:
            self.pending_process = getattr(error, 'capture_pending_process', None)
            receipt.update(getattr(error, 'capture_observation', {}))
            receipt.update(host_outcome='failed', error_type=type(error).__name__,
                           cleanup_error_type=getattr(error, 'capture_cleanup_error', None))
            # Preserve the original exception. Evidence records only its type,
            # never a path/member name or subprocess message containing a key.
            try:
                self.document('capture.failure.json', receipt)
            except BaseException as evidence_error:
                error.capture_evidence_error = type(evidence_error).__name__
            raise
        finally:
            if fd is not None:
                os.close(fd)
            if stderr_fd is not None:
                os.close(stderr_fd)
            self.private.release(); self.evidence.release()
