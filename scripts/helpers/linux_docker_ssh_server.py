"""Exact-owned DEV SSH server lifecycle; no host agent or fixture SSH retries.

All Engine commands use a dedicated canary-aware Driver recorder. A failed or
uncertain command retains objects; cleanup is explicitly authorized only after
the caller's complete SSH acceptance. This is not an SSH conformance validator.
"""
import copy
import ipaddress
import json
import os
from pathlib import Path

import docker_host_driver as driver
from linux_docker_buildkit_shutdown import EVENT_FORMAT, timestamp
import linux_docker_image_input as image_input

require = driver.require
LABEL = 'dev.vz.ssh-proof'
DIRECTORY = '/run/vz-ssh-server/'
ENTRYPOINT = ['python3', '-u', '-c']
# Stopped-container cp never executes a payload. Only these three owned paths
# are normalized before the fixed source-verified server replaces PID 1.
START = '''import os,stat
from pathlib import Path
root=Path('/run/vz-ssh-server')
for name,mode in [('host_key',0o600),('authorized_keys',0o444),('response.txt',0o444)]:
 p=root/name
 fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 try:
  s=os.fstat(fd)
  assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and 0<s.st_size<=8192
  os.fchown(fd,0,0);os.fchmod(fd,mode)
 finally: os.close(fd)
os.execv('/usr/local/bin/python3',['python3','-u','/fixture/server.py','serve'])
'''
READY = '''import os,socket,time
from pathlib import Path
end=time.monotonic()+10
while True:
 try:
  assert Path('/proc/1/comm').read_bytes()==b'sshd\\n'
  with socket.create_connection(('127.0.0.1',2222),timeout=.2) as s:
   assert s.recv(256).startswith(b'SSH-2.0-OpenSSH_9.2p1 ')
  break
 except (OSError,AssertionError):
  if time.monotonic()>=end: raise RuntimeError('SSH readiness deadline') from None
  time.sleep(.05)
print('VZ_SSH_SERVER_READY')
'''


def build_arguments(inputs, context, token):
    return ['buildx', 'build', '--builder', inputs['builder']['name'], '--platform', 'linux/arm64',
            '--progress', 'rawjson', '--file', str(context / 'Dockerfile.server'),
            '--provenance=false', '--sbom=false', '--network=none', '--load',
            '--label', LABEL + '=' + token, '--tag', token + ':server',
            '--build-arg', 'FIXTURE_BASE=' + inputs['images']['base']['reference'], str(context)]


def container_identity(item, *, cid, image, token, status):
    require(item['Id'] == cid and item['Image'] == image and item['Name'] == '/' + token and
            item['Config']['Labels'] == {LABEL: token} and
            item['Config']['Entrypoint'] == ENTRYPOINT[:1] and item['Config']['Cmd'] == ENTRYPOINT[1:] + [START] and
            item['Path'] == ENTRYPOINT[0] and item['Args'] == ENTRYPOINT[1:] + [START],
            'SSH server container ownership/configuration changed')
    host, state = item['HostConfig'], item['State']
    require(host['NetworkMode'] == 'bridge' and host['Runtime'] == 'youki' and
            host['Privileged'] is False and not host.get('Binds') and not host.get('Mounts') and
            not host.get('PortBindings') and host.get('PublishAllPorts') is False and
            host['RestartPolicy'] == {'Name': 'no', 'MaximumRetryCount': 0} and
            not host.get('AutoRemove') and not item['Mounts'] and
            not item['NetworkSettings'].get('Ports') and not item['Config'].get('ExposedPorts'),
            'SSH server escaped default bridge or private storage policy')
    require(type(item['RestartCount']) is int and item['RestartCount'] == 0 and
            state['Status'] == status and state['Running'] is (status == 'running') and
            all(state[key] is False for key in ('Paused', 'Restarting', 'Dead', 'OOMKilled')) and
            state['Error'] == '' and type(state['Pid']) is int and
            (state['Pid'] > 0 if status == 'running' else state['Pid'] == 0),
            'SSH server lifecycle changed')
    if status == 'running':
        networks = item['NetworkSettings']['Networks']
        require(set(networks) == {'bridge'}, 'SSH server has foreign network attachment')
        value = networks['bridge']['IPAddress']
        address = ipaddress.IPv4Address(value)
        require(str(address) == value and address.is_private and not address.is_loopback and
                not address.is_unspecified and not address.is_link_local and not address.is_multicast,
                'SSH server lacks exact usable private bridge IPv4')
        return value
    return None


def stopped_proof(before, after, raw_events, since, until, token):
    """OpenSSH 9.2p1 sshd.c exits 0 for SIGTERM; require actual Engine events."""
    begin, end = timestamp(since), timestamp(until)
    require(0 < end - begin <= 60 * 10**9 and
            before['State']['StartedAt'] == after['State']['StartedAt'] and
            timestamp(before['State']['StartedAt']) <= begin <= timestamp(after['State']['FinishedAt']) <= end and
            type(after['State']['ExitCode']) is int and after['State']['ExitCode'] == 0,
            'SSH server did not stop normally within owned lifetime')
    require(type(raw_events) is bytes and len(raw_events) <= 65536 and raw_events.endswith(b'\n'),
            'SSH stop events incomplete')
    rows = [image_input.parse(line) for line in raw_events.splitlines()]
    require(len(rows) == 3, 'SSH stop requires exactly three events')
    found = {}
    for row in rows:
        require(set(row) == {'type', 'action', 'id', 'attributes', 'scope', 'time_nano'} and
                row['id'] == before['Id'] and row['type'] == 'container' and row['scope'] == 'local' and
                type(row['time_nano']) is int and begin <= row['time_nano'] <= end,
                'foreign SSH stop event')
        action, attrs = row['action'], row['attributes']
        require(action in ('kill', 'die', 'stop') and action not in found, 'duplicate or unknown SSH stop action')
        extras = {'kill': {'signal'}, 'die': {'exitCode', 'execDuration'}, 'stop': set()}[action]
        require(set(attrs) == {LABEL, 'name', 'image'} | extras and attrs[LABEL] == token and
                attrs['name'] == token and attrs['image'] == before['Image'], 'foreign SSH stop ownership')
        if action == 'kill':
            require(attrs['signal'] == '15', 'SSH server was force-killed')
        if action == 'die':
            require(attrs['exitCode'] == '0' and isinstance(attrs['execDuration'], str) and
                    attrs['execDuration'].isascii() and attrs['execDuration'].isdigit(), 'SSH server abnormal exit')
        found[action] = row
    return {'schema_version': 1, 'container_id': before['Id'], 'signal': 'SIGTERM', 'exit_code': 0,
            'engine_since': since, 'engine_until': until, 'events': found,
            'events_sha256': driver.sha256(raw_events), 'filesystem_closure_certified': False}


class Server:
    def __init__(self, admitted_inputs, base_fixture, output, ssh_context, agent, token):
        driver.checked_text(token, r'vzssh-[0-9a-f]{24}', 'SSH server token')
        self.context = Path(ssh_context)
        require(self.context.is_absolute() and self.context == self.context.resolve(strict=True) and
                not any(c in str(self.context) for c in '\x00\r\n,'), 'canonical SSH context required')
        self.context_digest = driver.tree_digest(self.context)
        self.agent, self.token = agent, token
        self.driver = driver.Driver(admitted_inputs, Path(base_fixture), Path(output))
        self.record = self.driver.record
        self.driver.record.canaries.extend(agent.canaries())
        self.inputs_snapshot = copy.deepcopy(admitted_inputs.raw)
        self.scope_snapshot = copy.deepcopy(admitted_inputs.scope)
        self.tag = token + ':server'
        self.container_id = self.image_id = self.host = None
        self.started_identity = None
        self.container_configuration = self.image_configuration = None
        self.created_configuration = None
        self.inspected_status = self.start_acknowledgement = self.start_normalization = None
        self.bridge_configuration = None
        self.cleanup_authorized = self.prepared = self.closed = self.attempted = False
        self.failed = False
        self.paths = dict(agent.paths)
        self.agent_proof = agent.verify()
        require(self.agent_proof['owner'] == {key: admitted_inputs.scope[key] for key in
                ('project_id', 'environment_id', 'machine_id')}, 'SSH agent belongs to another Machine')
        self.response = ('vz-ssh-response:' + token + '\n').encode()
        self.response_path = self.driver.output / 'public-response.txt'
        with self.response_path.open('xb') as stream:
            stream.write(self.response)
        self.document('server-input.json', {'schema_version': 1, 'token': token,
            'scope': self.scope_snapshot, 'context': str(self.context), 'context_sha256': self.context_digest,
            'image_tag': self.tag, 'fingerprints': self.agent_proof['fingerprints'],
            'private_inputs_in_context': False})

    def document(self, name, value):
        require(not driver.contains_canary((json.dumps(value).encode(),), self.driver.record.canaries),
                'private SSH evidence rejected')
        self.driver.record.persist(self.driver.output / name, value, create=True)

    def guard(self):
        require(self.driver.inputs.raw == self.inputs_snapshot and self.driver.inputs.scope == self.scope_snapshot,
                'SSH server admitted ownership changed')
        require(driver.tree_digest(self.context) == self.context_digest, 'SSH context changed')
        for path in self.context.rglob('*'):
            if path.is_file():
                require(not driver.contains_canary((driver.regular(path),), self.driver.record.canaries),
                        'private SSH key appeared in build context')
        self.driver.builder_guard()

    def command(self, args, **kwargs):
        self.guard()
        return self.driver.command(args, **kwargs)

    def object(self, kind, value):
        result = self.command([kind, 'inspect', value])
        rows = image_input.parse(result.stdout)
        require(not result.stderr and type(rows) is list and len(rows) == 1, 'ambiguous SSH object inspection')
        return rows[0]

    def absent(self, kind, value):
        args = [kind, 'ls', '--quiet']
        args += (['--all', '--no-trunc', '--filter', 'name=^/' + value + '$'] if kind == 'container'
                 else ['--no-trunc', '--filter', 'reference=' + value])
        result = self.command(args)
        require(not result.stdout.strip() and not result.stderr, 'SSH object preexists or remains')

    def removed_id(self, kind, value):
        result = self.command([kind, 'inspect', value], expected=1)
        require(image_input.parse(result.stdout) == [] and result.stderr.decode().strip() in {
            'Error response from daemon: No such ' + kind + ': ' + value,
            'Error: No such ' + kind + ': ' + value,
            'Error: No such object: ' + value}, 'exact SSH object ID absence unproven')

    def image(self):
        row = self.object('image', self.tag)
        require(row['Id'] == self.image_id and row['Os'] == 'linux' and row['Architecture'] == 'arm64' and
                row['Config']['Labels'] == {LABEL: self.token} and row['RepoTags'] == [self.tag],
                'SSH server image ownership changed')
        configuration = row['Config']
        require(self.image_configuration in (None, configuration), 'SSH server image configuration changed')
        self.image_configuration = copy.deepcopy(configuration)
        return row

    def inspect(self, status):
        self.image()
        item = self.object('container', self.container_id)
        host = container_identity(item, cid=self.container_id, image=self.image_id, token=self.token, status=status)
        configuration = {key: item[key] for key in ('Config', 'HostConfig', 'Mounts')}
        normalization = None
        if self.container_configuration is not None and json.dumps(self.container_configuration, sort_keys=True) != json.dumps(configuration, sort_keys=True):
            normalization = self.start_configuration_transition(configuration, status)
        if status == 'running' or self.bridge_configuration is not None:
            self.bridge(item if status == 'running' else None)
        if status == 'running':
            identity = (item['State']['Pid'], item['State']['StartedAt'], host)
            require(self.started_identity in (None, identity), 'SSH server process or address changed')
            self.started_identity, self.host = identity, host
        if normalization is not None:
            self.document('server-start-normalization.json', normalization)
            self.start_normalization = normalization
        if self.created_configuration is None and status == 'created':
            self.created_configuration = copy.deepcopy(configuration)
        self.container_configuration = copy.deepcopy(configuration)
        self.inspected_status = status
        return item

    def start_configuration_transition(self, configuration, status):
        """Admit only Moby 29.7.2's unsupported cgroup-v2 OOM start rewrite.

        daemon/start.go revalidates the stored HostConfig; daemon_unix.go's
        verifyPlatformContainerResources changes false to nil when SysInfo
        reports OomKillDisable unsupported. Creation had defaulted nil to false.
        This is not a general inspect normalization or a runtime policy waiver.
        """
        before = self.container_configuration
        require(status == 'running' and self.inspected_status == 'created' and
                self.started_identity is None and self.start_normalization is None and
                self.created_configuration is not None and self.start_acknowledgement is not None,
                'SSH server full configuration changed outside acknowledged first start')
        require('OomKillDisable' in before['HostConfig'] and
                before['HostConfig']['OomKillDisable'] is False and
                'OomKillDisable' in configuration['HostConfig'] and
                configuration['HostConfig']['OomKillDisable'] is None,
                'SSH server OOM start transition differs')
        expected = copy.deepcopy(before)
        expected['HostConfig']['OomKillDisable'] = None
        require(json.dumps(expected, sort_keys=True) == json.dumps(configuration, sort_keys=True),
                'SSH server full configuration changed beyond OOM start transition')
        result = self.command(['info', '--format', '{{json .}}'])
        require(type(result.returncode) is int and result.returncode == 0 and
                result.timed_out is False and not result.stderr, 'SSH start policy command failed')
        policy = image_input.parse(result.stdout)
        require(type(policy) is dict and policy.get('ID') == self.scope_snapshot['engine_id'] and
                policy.get('ServerVersion') == '29.7.2' and policy.get('CgroupVersion') == '2' and
                policy.get('OomKillDisable') is False, 'SSH start Engine OOM policy differs')
        return {'schema_version': 1, 'scope': copy.deepcopy(self.scope_snapshot),
                'container_id': self.container_id, 'image_id': self.image_id,
                'transition': 'created-to-running-unsupported-oom-kill-disable',
                'source_commit': '6a43e3d5afddf4111da0f864bbc7cae5d7e95001',
                'start_acknowledgement': copy.deepcopy(self.start_acknowledgement),
                'policy_command_index': result.index, 'policy_stdout_sha256': driver.sha256(result.stdout),
                'policy': {key: policy[key] for key in ('ID', 'ServerVersion', 'CgroupVersion', 'OomKillDisable')},
                'created_configuration': copy.deepcopy(self.created_configuration),
                'running_configuration': copy.deepcopy(configuration)}

    def acknowledge_start(self, result):
        require(self.start_acknowledgement is None and type(result.returncode) is int and
                result.returncode == 0 and result.timed_out is False and
                type(result.index) is int and result.index > 0 and
                result.stdout == (self.container_id + '\n').encode() and not result.stderr,
                'SSH start acknowledgement differs')
        self.start_acknowledgement = {'command_index': result.index, 'container_id': self.container_id,
                                     'stdout_sha256': driver.sha256(result.stdout), 'exit_code': 0}

    def bridge(self, item=None):
        """Bind the server to Engine's exact bridge, not a host-published port.

        The source builder and server are distinct endpoints on this same
        default bridge. Worker "host" networking shares the builder container's
        namespace, not the Machine's host namespace.
        """
        row = self.object('network', 'bridge')
        driver.checked_text(row['Id'], r'[0-9a-f]{64}', 'SSH bridge network ID')
        require(row['Name'] == 'bridge' and row['Driver'] == 'bridge' and row['Scope'] == 'local' and
                row['Internal'] is False, 'SSH default bridge policy differs')
        identity = {key: row[key] for key in ('Id', 'Name', 'Driver', 'Scope', 'Internal', 'EnableIPv6', 'IPAM', 'Options', 'Labels')}
        builder_pin = self.inputs_snapshot['builder']
        builder = self.object('container', builder_pin['container_id'])
        require(builder['Id'] == builder_pin['container_id'] and builder['Image'] == builder_pin['image_id'] and
                builder['State']['Running'] is True and builder['HostConfig']['NetworkMode'] == 'bridge' and
                set(builder['NetworkSettings']['Networks']) == {'bridge'}, 'SSH source builder bridge ownership differs')
        builder_network = builder['NetworkSettings']['Networks']['bridge']
        require(builder_network['NetworkID'] == row['Id'] and builder_network['IPAddress'],
                'SSH source builder is not on server default bridge')
        identity['builder_endpoint'] = {'container_id': builder['Id'], **{key: builder_network[key] for key in
            ('NetworkID', 'EndpointID', 'IPAddress', 'IPPrefixLen', 'Gateway')}}
        require(self.bridge_configuration in (None, identity), 'SSH default bridge identity changed')
        self.bridge_configuration = copy.deepcopy(identity)
        endpoints = [builder_network]
        if item is not None:
            endpoints.append(item['NetworkSettings']['Networks']['bridge'])
            require(endpoints[0]['IPAddress'] != endpoints[1]['IPAddress'], 'SSH server aliases builder endpoint')
        for network in endpoints:
            require(network['NetworkID'] == row['Id'], 'SSH server attached to foreign bridge ID')
            address = ipaddress.IPv4Address(network['IPAddress'])
            matching = [entry for entry in row['IPAM']['Config'] if ':' not in entry['Subnet'] and
                        address in ipaddress.IPv4Network(entry['Subnet'])]
            require(len(matching) == 1 and network['Gateway'] == matching[0]['Gateway'] and
                    network['IPPrefixLen'] == ipaddress.IPv4Network(matching[0]['Subnet']).prefixlen,
                    'SSH bridge address differs from admitted IPAM')
        return identity

    def prepare(self):
        require(not self.attempted and not self.closed, 'SSH server preparation cannot repeat')
        self.attempted = True
        try:
            self.absent('image', self.tag)
            self.absent('container', self.token)
            self.command(build_arguments(self.inputs_snapshot, self.context, self.token), timeout=180)
            item = self.object('image', self.tag)
            self.image_id = driver.checked_text(item['Id'], r'sha256:[0-9a-f]{64}', 'SSH image ID')
            self.image()
            created = self.command(['container', 'create', '--name', self.token, '--network', 'bridge',
                '--runtime', 'youki', '--restart', 'no', '--label', LABEL + '=' + self.token,
                '--entrypoint', ENTRYPOINT[0], self.image_id, *ENTRYPOINT[1:], START])
            self.container_id = driver.checked_text(created.stdout.decode().strip(), r'[0-9a-f]{64}', 'SSH container ID')
            require(not created.stderr, 'SSH create error stream')
            self.document('server-ownership.json', {'schema_version': 1, 'scope': self.scope_snapshot,
                'container_id': self.container_id, 'image_id': self.image_id, 'image_tag': self.tag,
                'token': self.token, 'context_sha256': self.context_digest, 'private_copy_destinations':
                [DIRECTORY + name for name in ('host_key', 'authorized_keys', 'response.txt')]})
            self.inspect('created')
            for source, name in ((self.paths['host_private_key'], 'host_key'),
                                 (self.paths['auth_public_key'], 'authorized_keys'),
                                 (self.response_path, 'response.txt')):
                require(dict(self.agent.paths) == self.paths, 'SSH private input paths changed')
                self.inspect('created')
                copied = self.command(['cp', str(source), self.container_id + ':' + DIRECTORY + name])
                require(not copied.stdout and not copied.stderr, 'SSH private copy diagnostic rejected')
            started = self.command(['container', 'start', self.container_id])
            self.acknowledge_start(started)
            ready = self.command(['exec', self.container_id, 'python3', '-c', READY], timeout=15)
            require(ready.stdout == b'VZ_SSH_SERVER_READY\n' and not ready.stderr, 'SSH server readiness failed')
            public = self.command(['exec', self.container_id, '/usr/bin/ssh-keygen', '-y', '-f', DIRECTORY + 'host_key'])
            # Pinned ssh-keygen.c do_print_public preserves the private key's
            # comment; compare the complete admitted public key line.
            expected = driver.regular(self.paths['host_public_key'])
            require(public.stdout == expected and not public.stderr, 'SSH server host key differs')
            before = self.inspect('running')
            self.prepared = True
            request = {'schema_version': 1, 'token': self.token, 'host': self.host, 'port': 2222,
                       'host_key_fingerprint': self.agent_proof['fingerprints']['host']}
            self.document('server-ready.json', {'request': request, 'before': before, 'image_id': self.image_id,
                                              'bridge': self.bridge_configuration})
            return request
        except BaseException:
            self.failed = True
            raise

    def verify(self):
        require(self.prepared and not self.closed and not self.failed, 'SSH server unavailable')
        require(self.agent.verify()['fingerprints'] == self.agent_proof['fingerprints'], 'SSH keys changed')
        return self.inspect('running')

    def cleanup(self):
        require(self.cleanup_authorized and self.prepared and not self.closed and not self.failed and
                all(not row['effects_uncertain'] for row in self.driver.record.receipts),
                'SSH cleanup withheld after incomplete acceptance or uncertain effects')
        try:
            before = self.verify()
            self.guard()
            since = self.driver._engine_system_time
            stopped = self.command(['container', 'stop', '--signal', 'SIGTERM', '--timeout', '30', self.container_id], timeout=40)
            require(stopped.stdout == (self.container_id + '\n').encode() and not stopped.stderr, 'SSH stop acknowledgement differs')
            after = self.inspect('exited')
            self.guard()
            until = self.driver._engine_system_time
            events = self.command(['events', '--since', since, '--until', until, '--filter', 'container=' + self.container_id,
                '--filter', 'event=kill', '--filter', 'event=die', '--filter', 'event=stop', '--format', EVENT_FORMAT], timeout=10)
            require(not events.stderr, 'SSH stop event errors')
            proof = stopped_proof(before, after, events.stdout, since, until, self.token)
            logs = self.command(['logs', self.container_id], timeout=15)
            # Complete bounded streams, not a tail. Recorder withholds private
            # canaries before publishing and failure prevents object removal.
            proof['logs'] = {'command_index': logs.index, 'stdout_sha256': driver.sha256(logs.stdout),
                             'stderr_sha256': driver.sha256(logs.stderr), 'complete_streams': True}
            proof['bridge'] = self.bridge_configuration
            self.document('server-stop.json', proof)
            self.inspect('exited')
            removed = self.command(['container', 'rm', self.container_id])
            require(removed.stdout == (self.container_id + '\n').encode() and not removed.stderr,
                    'SSH container removal not acknowledged')
            self.absent('container', self.token)
            self.removed_id('container', self.container_id)
            self.image()
            removed = self.command(['image', 'rm', self.tag])
            require(not removed.stderr, 'SSH image removal error')
            self.absent('image', self.tag)
            self.removed_id('image', self.image_id)
            self.closed = True
            self.document('server-cleanup.json', {'schema_version': 1, 'container_id': self.container_id,
                'image_id': self.image_id, 'token': self.token, 'normal_stop': proof,
                'container_removed': True, 'image_tag_removed': True, 'broader_cleanup_authorized': False})
            return proof
        except BaseException:
            self.failed = True
            raise
