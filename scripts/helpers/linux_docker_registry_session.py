"""Installed exact-Machine registry operations; caller owns topology and cleanup.

This implementation is a DEV building block. It does not publish release PASS
IDs: independent command replay, credential controls across Machines and the
aggregate lifecycle remain the orchestrator's responsibility. Secrets stay in
memory or the exact guest-owned private fixture, never public command input.
"""
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_registry_fixture as fixture
import linux_docker_registry_guest as guest
import linux_docker_registry_credentials as credentials
import linux_docker_registry_image as image
import linux_docker_registry_route as route

require = driver.require

# Pinned from offline binary admission (Distribution v3.1.1 built with Go 1.25.9);
# runtime log rows must match these pins, never define them.
REGISTRY_VERSION = '3.1.1'  # observed logrus 'version' field of the pinned v3.1.1 binary (candidates 1-2)
REGISTRY_GO_VERSION = 'go1.25.9'
# Distribution v3.1.1 registry/registry.go: logrus JSONFormatter (RFC3339Nano) on
# a context carrying instance.id/version/go.version; our config sets no log.fields,
# so startup rows carry exactly these keys.
STARTUP_KEYS = frozenset({'time', 'level', 'msg', 'go.version', 'instance.id', 'version'})
# Request/response/error keys from internal/dcontext/http.go GetRequestLogger and
# GetResponseLogger, registry/handlers/app.go (auth.user.name, err.*, vars.*) and the
# optional log.fields service key. Anything else is not a Distribution 3.1.1 record.
RECORD_KEYS = STARTUP_KEYS | {
    'service', 'auth.user.name', 'http.request.id', 'http.request.method', 'http.request.host',
    'http.request.uri', 'http.request.referer', 'http.request.useragent', 'http.request.remoteaddr',
    'http.request.contenttype', 'http.response.written', 'http.response.status',
    'http.response.contenttype', 'http.response.duration', 'err.code', 'err.message', 'err.detail',
    'vars.name', 'vars.reference', 'vars.digest', 'vars.uuid',
    # registry/auth/htpasswd/access.go logs a failed basic-auth attempt with
    # WithFields username/error ("user failed to authenticate"); observed candidate 3.
    'username', 'error'}
# Distribution v3.1.1 registry/registry.go constructs http.Server{Handler: handler}
# without ErrorLog, so Go net/http server.go conn.serve logs failed handshakes via
# log.Printf (LstdFlags date/time prefix) as "http: TLS handshake error from %s: %v".
# crypto/tls conn.go wraps a received alert as net.OpError{Op: "remote error"} whose
# text is "tls: " + alert.go's name. Go 1.25 handshake_client.go verifyServerCertificate
# sends alertBadCertificate for EVERY certificate verification failure (the Go client
# never sends alertUnknownCA), so the Engine's wrong-CA rejection is exactly one
# "remote error: tls: bad certificate" line from the bridge gateway peer.
HANDSHAKE_PREFIX = r'\d{4}/\d\d/\d\d \d\d:\d\d:\d\d http: TLS handshake error from '
HANDSHAKE_SUFFIX = r':([1-9][0-9]{0,4}): remote error: tls: ([a-z ]{1,64})'
WRONG_CA_ALERTS = ('bad certificate',)


# Moby/containerd push existence probes against an empty repository: HEAD each blob
# and the tag before uploading. Distribution answers 404 and app.go logs the request
# at level error ("response completed with error", err.code BLOB_UNKNOWN /
# MANIFEST_UNKNOWN rendered by their messages). Observed exactly once per push in
# installed candidate 5; admitted only when the caller declares the completed push.
PUSH_PROBES = {'blob': 2, 'manifest': 1}


def _push_probe(row, probes):
    """Classify one level=error row as an exact authorized 404 push probe or reject it."""
    repo = probes['repository_name']
    require(row['msg'] == 'response completed with error' and row['http.request.method'] == 'HEAD' and
            row['http.response.status'] == 404 and row['http.response.contenttype'] == 'application/json' and
            row.get('auth.user.name') == probes['username'] and row.get('vars.name') == repo and
            row['http.request.host'] == probes['host'], 'registry error record is not an authorized push probe')
    uri = row['http.request.uri']
    if uri == '/v2/' + repo + '/manifests/subject':
        require(row['err.code'] == 'manifest unknown' and row['err.message'] == 'manifest unknown' and
                row['err.detail'] == 'unknown tag=subject' and row.get('vars.reference') == 'subject',
                'registry manifest probe fields')
        return 'manifest'
    digest = row.get('vars.digest')
    require(type(digest) is str and re.fullmatch('sha256:[0-9a-f]{64}', digest) is not None and
            uri == '/v2/' + repo + '/blobs/' + digest and row['err.code'] == 'blob unknown' and
            row['err.message'] == 'blob unknown to registry' and row['err.detail'] == digest and
            digest in probes['blob_digests'], 'registry blob probe fields')
    return 'blob'


def classify_log(raw, *, instance_id, gateway, phase, handshake_alerts=(), push_probes=None):
    """Every complete-log line is a Distribution record of this instance or an allowed
    Go TLS handshake error from the owned gateway; nothing else is admitted or filtered."""
    require(type(raw) is bytes and len(raw) <= route.MAX_BYTES and (not raw or raw.endswith(b'\n')),
            'registry log bounds')
    require(type(instance_id) is str and re.fullmatch(route.UUID, instance_id) is not None, 'registry instance id')
    require(type(phase) is str and re.fullmatch('[a-z-]{1,32}', phase) is not None, 'registry log phase')
    require(type(handshake_alerts) is tuple and all(a in WRONG_CA_ALERTS for a in handshake_alerts),
            'registry handshake alert allowlist')
    require(type(gateway) is str and re.fullmatch(r'[0-9.]{7,15}', gateway) is not None, 'registry gateway')
    pattern = HANDSHAKE_PREFIX + re.escape(gateway) + HANDSHAKE_SUFFIX
    lines = raw.split(b'\n')[:-1]
    records, reasons, levels, probes = 0, {}, {}, {}
    require(push_probes is None or (type(push_probes) is dict and set(push_probes) ==
            {'repository_name', 'username', 'host', 'blob_digests'}), 'registry push probe declaration')
    for line in lines:
        require(0 < len(line) <= route.MAX_LINE, 'registry log line bounds')
        if line.startswith(b'{'):
            row = fixture.decode(line)
            require(type(row) is dict and STARTUP_KEYS <= set(row) <= RECORD_KEYS, 'registry log record fields')
            require(all(type(row[key]) is str for key in STARTUP_KEYS), 'registry log record scalars')
            require(row['instance.id'] == instance_id and row['version'] == REGISTRY_VERSION and
                    row['go.version'] == REGISTRY_GO_VERSION, 'registry log record identity')
            if row['level'] == 'error':
                require(push_probes is not None, 'registry log record level')
                kind = _push_probe(row, push_probes)
                probes[kind] = probes.get(kind, 0) + 1
            else:
                require(row['level'] in ('info', 'warning'), 'registry log record level')
            route.timestamp_ns(row['time'])
            records += 1
            levels[row['level']] = levels.get(row['level'], 0) + 1
            continue
        try:
            text = line.decode('ascii')
        except UnicodeError:
            raise driver.Rejected('registry log line is neither JSON nor ASCII (' + phase + ')') from None
        match = re.fullmatch(pattern, text)
        require(match is not None, 'registry log line is neither a Distribution record nor a gateway TLS '
                'handshake error (' + phase + ')')
        require(int(match[1]) <= 65535, 'registry handshake peer port')
        require(match[2] in handshake_alerts, 'unexpected TLS handshake alert during ' + phase)
        reasons[match[2]] = reasons.get(match[2], 0) + 1
    require(push_probes is None or probes == PUSH_PROBES, 'registry push probe count')
    return {'phase': phase, 'lines': len(lines), 'json_records': records, 'record_levels': levels,
            'push_not_found_probes': probes,
            'handshake_errors': sum(reasons.values()), 'handshake_reasons': reasons,
            'handshake_alerts_allowed': list(handshake_alerts), 'instance_id': instance_id,
            'raw_bytes': len(raw), 'raw_sha256': hashlib.sha256(raw).hexdigest(), 'filtered_lines': 0}


def startup_identity(raw, *, authority):
    """Startup rows before any client connection: all JSON, one instance, TLS listener."""
    require(type(raw) is bytes and 0 < len(raw) <= route.MAX_BYTES and raw.endswith(b'\n'), 'registry startup bounds')
    rows = [fixture.decode(line) for line in raw.split(b'\n')[:-1]]
    require(all(type(row) is dict and set(row) == STARTUP_KEYS and
                all(type(value) is str for value in row.values()) for row in rows), 'registry startup fields')
    instances = {row['instance.id'] for row in rows}
    require(len(instances) == 1, 'one registry process instance required')
    instance_id = route.token(next(iter(instances)), route.UUID)
    require(all(row['version'] == REGISTRY_VERSION and row['go.version'] == REGISTRY_GO_VERSION and
                row['level'] == 'info' for row in rows), 'registry startup identity')
    for row in rows:
        route.timestamp_ns(row['time'])
    listening = 'listening on ' + authority + ', tls'
    require(sum(row['msg'] == listening for row in rows) == 1 and
            not any(row['msg'].startswith('listening on ') and row['msg'] != listening for row in rows),
            'registry TLS listener record')
    return {'instance_id': instance_id, 'rows': len(rows), 'listening': listening, 'version': REGISTRY_VERSION,
            'go_version': REGISTRY_GO_VERSION, 'raw_bytes': len(raw), 'raw_sha256': hashlib.sha256(raw).hexdigest()}


def labels(spec):
    return [value for pair in sorted(spec['labels'].items()) for value in ('--label', '='.join(pair))]


def network_identity(raw, spec):
    rows = fixture.decode(raw)
    require(type(rows) is list and len(rows) == 1, 'one registry network required')
    row = rows[0]
    require(row['Name'] == spec['network_name'] and re.fullmatch('[0-9a-f]{64}', row['Id']) and
            row['Driver'] == 'bridge' and row['Scope'] == 'local' and row['Internal'] is True and
            row['Labels'] == spec['labels'] and not row.get('EnableIPv6') and
            row['IPAM']['Driver'] == 'default' and row['IPAM']['Config'] ==
            [{'Subnet': spec['subnet'], 'Gateway': spec['gateway']}], 'registry network identity')
    return row


def volume_identity(raw, spec):
    rows = fixture.decode(raw)
    require(type(rows) is list and len(rows) == 1, 'one registry volume required')
    row = rows[0]
    require(row['Name'] == spec['volume_name'] and row['Driver'] == 'local' and
            row['Scope'] == 'local' and row['Labels'] == spec['labels'] and not row.get('Options'),
            'registry volume identity')
    return row


class Session:
    """Register before prepare; uncertain effects never authorize cleanup."""
    def __init__(self, harness, descriptor, project, index, private_fixture):
        from linux_docker_registry_commands import Commands
        self.harness, self.descriptor, self.project = harness, descriptor, Path(project)
        self.spec = fixture.resource_spec(descriptor['owner'], harness.info['run_id'])
        self.recipe = image.contract(self.spec)
        self.private_fixture = private_fixture
        private_fixture.validate_private(observed_unix_ns=time.time_ns())
        fixture.validate_tls_public(private_fixture.public(), spec=self.spec, expected=private_fixture.pins,
                                    observed_unix_ns=time.time_ns())
        self.plan = guest.plan(descriptor['owner'], harness.info['run_id'], str(uuid.uuid4()))
        self.output = startup.private(harness.evidence / ('registry-machine-' + str(index)))
        self.commands = Commands(harness, descriptor, project, index)
        self.store = credentials.Store(descriptor, plugin_paths={
            name: str(harness.config / 'cli-plugins' / ('docker-' + name)) for name in ('compose', 'buildx')},
            authority=self.spec['authority'], username='vz-registry-user',
            password=private_fixture.password())
        self.initial_credentials = self.store.snapshot(expected='empty')
        self.credential_state = 'empty'
        self.identities = self.network = self.volume = self.container_id = None
        self.instance_id = self.startup_log = None
        self.wrong_ca_probed = False
        self.prepared = self.workload_complete = self.cleanup_complete = False
        self.failed = False
        if not hasattr(harness, 'registry_sessions'):
            harness.registry_sessions = []
        harness.registry_sessions.append(self)

    def __repr__(self):
        return '<InstalledRegistrySession>'

    def document(self, name, value):
        require(not driver.contains_canary((json.dumps(value).encode(),), self.private_fixture.canaries()),
                'private registry proof rejected')
        startup.document(self.output / name, value)

    def certain(self):
        require(not self.failed, 'registry session requires reconciliation')
        self.commands.assert_certain()
        require(not self.harness.effects_uncertain and
                all(not row['effects_uncertain'] for row in self.harness.record.receipts),
                'registry host command effects remain uncertain')
        self.store.snapshot(expected=self.credential_state)
        monitor = self.harness.monitor
        if monitor is not None and monitor.thread.is_alive():
            monitor.check()

    def docker(self, label, args, *, mutate=False, **options):
        self.certain()
        call = self.harness.mutate if mutate else self.harness.docker
        return call('registry-' + label, self.descriptor, args, **options)

    def exec_argv(self, script, *, stdin=False):
        owner = self.descriptor['owner']
        return [str(self.harness.cli), 'exec', '--environment', owner['environment_id'],
                '--machine', owner['machine_id'], *([] if stdin else ['--no-stdin']),
                '--timeout', '30', '--', '/bin/busybox', 'sh', '-c', script]

    def public_exec(self, label, script):
        self.certain()
        raw, stderr, code = self.harness.command('registry-' + label, self.exec_argv(script), cwd=self.project)
        require(code == 0 and not stderr, 'registry guest observation failed')
        return raw

    def private_exec(self, label, script, payload, acknowledgment):
        self.certain()
        return self.commands.private(label, self.exec_argv(script, stdin=True),
            executable=str(self.harness.cli), private_input=payload,
            expected_stdout=acknowledgment, expected_stderr=b'', timeout=30)

    def inspect_server(self, *, running):
        raw, stderr, _ = self.docker('server-inspect', ['container', 'inspect', self.container_id])
        require(not stderr, 'registry inspect stderr')
        rows = fixture.decode(raw)
        require(len(rows) == 1, 'one registry server')
        row = rows[0]
        require(row['Id'] == self.container_id and row['Name'] == '/' + self.spec['container_name'] and
                row['Image'] == self.harness.info['registry']['manifest_digest'] and
                row['Config']['Labels'] == self.spec['labels'] and row['State']['Running'] is running and
                row['RestartCount'] == 0 and not row['HostConfig']['PortBindings'] and
                row['HostConfig']['RestartPolicy']['Name'] == 'no', 'registry server identity')
        mounts = {item['Destination']: item for item in row['Mounts']}
        require(set(mounts) == {'/run/vz-registry', '/var/lib/registry'} and
                mounts['/run/vz-registry']['Type'] == 'bind' and
                mounts['/run/vz-registry']['Source'] == self.plan['directory'] and
                mounts['/run/vz-registry']['RW'] is False and
                mounts['/var/lib/registry']['Name'] == self.spec['volume_name'] and
                mounts['/var/lib/registry']['Type'] == 'volume' and
                mounts['/var/lib/registry']['RW'] is True, 'registry server mounts')
        if running:
            require(set(row['NetworkSettings']['Networks']) == {self.spec['network_name']}, 'registry network inventory')
            net = row['NetworkSettings']['Networks'][self.spec['network_name']]
            # Moby leaves the endpoint Gateway empty on --internal networks (no
            # default route); the bridge address in the network IPAM config is the
            # daemon-side peer the route verifier binds. Observed candidate 1.
            require(net['NetworkID'] == self.network['Id'] and net['IPAddress'] == self.spec['address'] and
                    net['IPAMConfig'] == {'IPv4Address': self.spec['address']} and net['IPPrefixLen'] == 24 and
                    net['Gateway'] == '' and not net['Links'] and not net['Aliases'], 'registry private route')
        return row

    def logs(self):
        raw, stderr, _ = self.docker('server-logs', ['logs', self.container_id])
        require(not raw and len(stderr) <= route.MAX_BYTES, 'registry JSON stderr bound')
        return stderr

    def classify(self, raw, phase):
        """Complete raw log so far: startup prefix intact, exactly the expected handshake errors."""
        require(self.instance_id is not None and raw.startswith(self.startup_log), 'registry log prefix changed')
        probes = None
        if self.workload_complete:
            expected = self.recipe['expected']
            probes = {'repository_name': self.spec['repository'].split('/', 1)[1], 'username': 'vz-registry-user',
                      'host': self.spec['authority'],
                      'blob_digests': (expected['config_digest'], expected['layer_digest'])}
        proof = classify_log(raw, instance_id=self.instance_id, gateway=self.spec['gateway'], phase=phase,
                             handshake_alerts=WRONG_CA_ALERTS if self.wrong_ca_probed else (), push_probes=probes)
        # Moby daemon/pkg/registry/service.go Auth iterates lookupV2Endpoints; a
        # non-insecure private authority yields one TLS endpoint, whose PingV2Registry
        # dials once, so the wrong-CA probe leaves exactly one handshake error forever.
        require(proof['handshake_errors'] == int(self.wrong_ca_probed), 'registry TLS handshake error count')
        return proof

    def retain_log(self, name, raw, phase):
        """Persist complete raw stderr after classification and a private canary scan."""
        proof = self.classify(raw, phase)
        require(not driver.contains_canary((raw,), self.private_fixture.canaries()), 'private registry log rejected')
        startup.write(self.output / ('registry-stderr-' + name + '.log'), raw)
        proof['retained_file'] = 'registry-stderr-' + name + '.log'
        self.document(name + '-log.json', proof)
        return proof

    def prepare(self):
        """Create exact resources; failed admission leaves them visibly uncertain."""
        require(not self.prepared and self.container_id is None, 'registry setup already attempted')
        self.certain()
        try:
            canaries = list(self.private_fixture.canaries())
            self.harness.record.canaries.extend(canaries)
            self.harness.sensitive_canaries.extend(canaries)
            self.document('public-fixture.json', self.private_fixture.public())
            self.document('guest-plan.json', self.plan)
            raw, _, _ = self.docker('network-baseline', ['network', 'ls', '--no-trunc', '--format', '{{json .}}'])
            networks = [json.loads(line) for line in raw.splitlines()]
            require(all(row['Name'] != self.spec['network_name'] for row in networks), 'registry network preexists')
            for row in networks:
                before, _, _ = self.docker('network-range', ['network', 'inspect', row['ID']])
                import ipaddress
                ranges = fixture.decode(before)[0]['IPAM'].get('Config') or []
                require(all(not ipaddress.ip_network(item['Subnet']).overlaps(ipaddress.ip_network(self.spec['subnet']))
                            for item in ranges if 'Subnet' in item and ':' not in item['Subnet']), 'registry subnet overlaps')
            raw, _, _ = self.docker('volume-baseline', ['volume', 'ls', '--quiet'])
            require(self.spec['volume_name'] not in raw.decode().splitlines(), 'registry volume preexists')
            self.harness.exact_absent(self.descriptor, 'container', self.spec['container_name'])
            self.harness.exact_absent(self.descriptor, 'image', 'docker.io/library/registry:3.1.1')
            self.docker('fixture-load', ['image', 'load', '--platform', 'linux/arm64', '--input',
                self.harness.info['registry_archive']], mutate=True, timeout=120)
            self.private_exec('setup', guest.setup_script(self.plan), guest.encode_payload(self.plan,
                self.private_fixture.privatefiles(), trust_ca=self.private_fixture.ca_pem(wrong=True)),
                guest.fixed_ack(self.plan, action='SETUP'))
            admitted = self.public_exec('guest-admit', guest.admit_script(self.plan))
            self.identities = guest.parse_ack(admitted, self.plan, action='ADMIT')
            self.document('guest-identities.json', self.identities)
            self.docker('network-create', ['network', 'create', '--driver', 'bridge', '--internal',
                '--subnet', self.spec['subnet'], '--gateway', self.spec['gateway'], *labels(self.spec),
                self.spec['network_name']], mutate=True)
            raw, _, _ = self.docker('network-inspect', ['network', 'inspect', self.spec['network_name']])
            self.network = network_identity(raw, self.spec)
            self.docker('volume-create', ['volume', 'create', '--driver', 'local', *labels(self.spec),
                                         self.spec['volume_name']], mutate=True)
            raw, _, _ = self.docker('volume-inspect', ['volume', 'inspect', self.spec['volume_name']])
            self.volume = volume_identity(raw, self.spec)
            raw, _, _ = self.docker('server-create', ['container', 'create', '--name', self.spec['container_name'],
                '--pull', 'never', '--restart', 'no', '--network', self.spec['network_name'], '--ip', self.spec['address'],
                *labels(self.spec), '--mount', 'type=bind,src=' + self.plan['directory'] + ',dst=/run/vz-registry,readonly',
                '--mount', 'type=volume,src=' + self.spec['volume_name'] + ',dst=/var/lib/registry',
                '--entrypoint', '/bin/registry', self.harness.info['registry']['manifest_digest'],
                'serve', '/run/vz-registry/config.yml'], mutate=True)
            self.container_id = driver.checked_text(raw.decode().strip(), '[0-9a-f]{64}', 'registry CID')
            self.inspect_server(running=False)
            self.docker('server-start', ['container', 'start', self.container_id], mutate=True)
            deadline = time.monotonic() + 30
            while True:
                self.inspect_server(running=True)
                raw = self.logs()
                # No client has connected yet, so every row must already be JSON;
                # a plaintext row here is a failure, not something to skip.
                rows = [fixture.decode(line) for line in raw.splitlines()]
                if any(type(row.get('msg')) is str and row['msg'].startswith('listening on ') for row in rows):
                    break
                require(time.monotonic() < deadline, 'registry listener readiness deadline')
                time.sleep(0.2)
            identity = startup_identity(raw, authority=self.spec['authority'])
            self.instance_id, self.startup_log = identity['instance_id'], raw
            self.document('startup-log.json', identity)
            self.prepared = True
            self.document('prepared.json', {'owner': self.descriptor['owner'], 'container_id': self.container_id,
                'network_id': self.network['Id'], 'volume': self.volume, 'instance_id': self.instance_id,
                'tls_handshake_proven': False})
        except BaseException:
            self.failed = True
            raise

    def login(self, *, case, role, expected_stdout, expected_stderr, expected_exit):
        require(self.prepared and role in ('valid', 'invalid'), 'registry not prepared')
        require(case in ('wrong-ca', 'invalid', 'valid'), 'registry login case')
        self.certain()
        before = self.store.snapshot(expected='empty')
        password = self.private_fixture.password(role=role)
        result = self.commands.private('login-' + case, ['docker', '--config', self.descriptor['config_dir'],
            '--context', self.descriptor['name'], 'login', '--username', 'vz-registry-user', '--password-stdin',
            self.spec['authority']], executable=self.harness.info['clients']['docker']['canonical'],
            private_input=password + b'\n', expected_stdout=expected_stdout,
            expected_stderr=expected_stderr, expected_exit=expected_exit, timeout=30)
        proof = (self.store.check_transition(before, expected='login') if expected_exit == 0 else
                 self.store.check_unchanged(before))
        if expected_exit == 0:
            self.credential_state = 'login'
        self.document('login-' + case + '-credentials.json', proof)
        return result

    def guest_seconds(self):
        raw = self.public_exec('clock', 'exec /bin/busybox date +%s')
        require(re.fullmatch(b'[1-9][0-9]{8,10}\n', raw), 'guest wall clock format')
        return int(raw)

    def authenticate(self):
        """Wrong trust, wrong password, then one source-attributed good login."""
        self.certain()
        authority = self.spec['authority']
        # Moby daemon/pkg/registry/auth.go PingV2Registry returns http.Client.Do's
        # url.Error (Op "Get") wrapping crypto/tls CertificateVerificationError
        # ("tls: failed to verify certificate: %s") around x509.UnknownAuthorityError.
        # The wrong CA's subject ("vz registry wrong CA") never matches the server
        # issuer, so x509 finds no candidate parent and appends no hint text.
        self.login(case='wrong-ca', role='valid', expected_stdout=b'', expected_exit=1,
            expected_stderr=('Error response from daemon: Get "https://' + authority +
                '/v2/": tls: failed to verify certificate: x509: certificate signed by unknown authority\n').encode())
        self.wrong_ca_probed = True
        self.document('login-wrong-ca-log.json', self.classify(self.logs(), 'wrong-ca'))
        self.private_exec('trust', guest.install_trust_script(self.plan, self.identities,
                fixture.sha(self.private_fixture.ca_pem(wrong=True)), fixture.sha(self.private_fixture.ca_pem())),
                self.private_fixture.ca_pem(), guest.fixed_ack(self.plan, action='TRUST'))
        inspected = self.public_exec('guest-inspect', guest.inspect_script(self.plan, self.identities))
        guest.parse_ack(inspected, self.plan, action='INSPECT', expected=self.identities)
        # daemon/pkg/registry/auth.go loginV2: non-200 ping response yields
        # "login attempt to %s failed with status: %d %s" with http.StatusText.
        self.login(case='invalid', role='invalid', expected_stdout=b'', expected_exit=1,
            expected_stderr=('Error response from daemon: login attempt to https://' + authority +
                             '/v2/ failed with status: 401 Unauthorized\n').encode())
        self.document('login-invalid-log.json', self.classify(self.logs(), 'invalid-login'))
        self.unauthenticated_push()
        self.document('unauthorized-push-log.json', self.classify(self.logs(), 'unauthorized-push'))
        # The command window is bracketed using the same guest clock as its
        # registry logs. Second-resolution samples give explicit interval bounds;
        # the exact append-only log delta additionally excludes earlier requests.
        before = self.logs()
        self.classify(before, 'pre-login')
        start = self.guest_seconds() * 10**9
        # Docker CLI v29.4.0 cli/config/credentials/file_store.go Store: Fprintln of
        # unencryptedWarning (leading and trailing newline) with the config filename.
        warning = ("\nWARNING! Your credentials are stored unencrypted in '" +
            self.descriptor['config_dir'] + "/config.json'.\n"
            "Configure a credential helper to remove this warning. See\n"
            "https://docs.docker.com/go/credential-store/\n\n").encode()
        self.login(case='valid', role='valid', expected_stdout=b'Login Succeeded\n',
                   expected_stderr=warning, expected_exit=0)
        end = (self.guest_seconds() + 1) * 10**9
        after = self.logs()
        require(after.startswith(before) and len(after) > len(before), 'registry log prefix changed')
        retained = self.retain_log('authenticate', after, 'authenticate')
        raw, _, _ = self.docker('engine-version', ['version', '--format', '{{json .Server}}'])
        version = fixture.decode(raw)
        engine = {key: version[field] for key, field in (
            ('version', 'Version'), ('go_version', 'GoVersion'), ('git_commit', 'GitCommit'),
            ('kernel_version', 'KernelVersion'), ('os', 'Os'), ('arch', 'Arch'))}
        proof = route.validate(after[len(before):], engine=engine, cli_version='29.4.0',
            registry={'instance_id': self.instance_id, 'go_version': REGISTRY_GO_VERSION,
                'version': REGISTRY_VERSION, 'host': authority, 'remote_ip': self.spec['gateway'],
                'realm': 'vz-private-registry'},
            username='vz-registry-user', window_ns=(start, end), canaries=self.private_fixture.canaries())
        proof.update(container_id=self.container_id, owner=self.descriptor['owner'],
                     log_prefix_bytes=len(before), guest_clock_resolution_ns=10**9,
                     complete_log_sha256=retained['raw_sha256'], complete_log_bytes=retained['raw_bytes'],
                     complete_log_handshake_errors=retained['handshake_errors'])
        self.document('login-route.json', proof)
        return proof

    def storage_inventory(self):
        self.inspect_server(running=True)
        raw, stderr, _ = self.docker('storage-inventory', ['exec', self.container_id, '/bin/busybox',
            'find', '/var/lib/registry', '-type', 'f', '-exec', '/bin/busybox', 'sha256sum', '{}', ';'])
        require(not stderr and len(raw) <= 1024 * 1024 and (not raw or raw.endswith(b'\n')),
                'registry storage inventory bound')
        lines = raw.splitlines()
        require(len(lines) <= 4096 and len(set(lines)) == len(lines) and all(
            re.fullmatch(b'[0-9a-f]{64}  /var/lib/registry/[A-Za-z0-9_./-]+', line) for line in lines),
                'registry storage inventory format')
        return sorted(line.decode('ascii') for line in lines)

    def unauthenticated_push(self):
        """An actual native push without client credentials must not store bytes."""
        selected = self.recipe['commands']
        before = self.store.snapshot(expected='empty')
        storage = self.storage_inventory()
        raw, _, _ = self.docker('unauthorized-image-baseline', selected['inventory'])
        baseline = image.validate_absent(raw, spec=self.spec)['image_ids']
        seed_path = self.output / 'unauthorized-subject.tar'
        startup.write(seed_path, image.seed(self.spec))
        self.docker('unauthorized-subject-load', [*selected['load'], '--input', str(seed_path)], mutate=True)
        self.docker('unauthorized-subject-tag', selected['tag_remote'], mutate=True)
        raw, _, _ = self.docker('unauthorized-subject-inspect', selected['inspect_tagged'])
        image.validate_inspect(raw, spec=self.spec, stage='tagged')
        # containerd core/remotes/docker/authorizer.go AddResponses: a Basic challenge
        # without credentials fails with "%w: no basic auth credentials" (ErrInvalidAuthorization);
        # Moby's own integration-cli push test asserts this same substring.
        _, stderr, code = self.docker('unauthorized-push', selected['push'], success=False, timeout=60)
        require(code == 1 and b'no basic auth credentials' in stderr,
                'unauthenticated push did not fail for expected authorization cause')
        require(self.storage_inventory() == storage, 'unauthorized push changed registry storage')
        self.document('unauthorized-push-credentials.json', self.store.check_unchanged(before))
        self.docker('unauthorized-remove-remote', selected['remove_remote'], mutate=True)
        self.docker('unauthorized-remove-seed', selected['remove_seed'], mutate=True)
        raw, _, _ = self.docker('unauthorized-image-final', selected['inventory'])
        require(image.validate_absent(raw, spec=self.spec)['image_ids'] == baseline,
                'unauthorized test image inventory differs')
        self.document('unauthorized-push.json', {'expected_authorization_denial': True,
            'registry_file_inventory_unchanged': True, 'client_credentials_unchanged': True,
            'subject_removed': True, 'full_registry_authentication_certified': False})

    def roundtrip(self):
        """Push a known manifest, erase local copies, pull by digest and inspect bytes."""
        self.certain()
        selected = self.recipe['commands']
        before = self.store.snapshot(expected='login')
        raw, _, _ = self.docker('image-before', selected['inventory'])
        baseline = image.validate_absent(raw, spec=self.spec)['image_ids']
        seed_path = self.output / 'subject.tar'
        startup.write(seed_path, image.seed(self.spec))
        self.docker('subject-load', [*selected['load'], '--input', str(seed_path)], mutate=True)
        raw, _, _ = self.docker('subject-loaded', selected['inspect_loaded'])
        image.validate_inspect(raw, spec=self.spec, stage='loaded')
        self.docker('subject-tag', selected['tag_remote'], mutate=True)
        raw, _, _ = self.docker('subject-tagged', selected['inspect_tagged'])
        image.validate_inspect(raw, spec=self.spec, stage='tagged')
        self.docker('subject-push', selected['push'], mutate=True, timeout=60)
        remote = {}
        for kind in ('manifest', 'config', 'layer'):
            identity = self.recipe['expected'][kind + '_digest'][7:]
            path = '/var/lib/registry/docker/registry/v2/blobs/sha256/' + identity[:2] + '/' + identity + '/data'
            raw, stderr, _ = self.docker('remote-' + kind, ['exec', self.container_id, '/bin/busybox', 'cat', path])
            require(not stderr, 'remote registry blob observation failed')
            remote[kind] = raw
        remote_proof = image.validate_remote(**remote)
        self.document('remote-content.json', remote_proof)
        self.docker('remove-remote', selected['remove_remote'], mutate=True)
        self.docker('remove-seed', selected['remove_seed'], mutate=True)
        raw, _, _ = self.docker('pre-pull-absence', selected['inventory'])
        require(image.validate_absent(raw, spec=self.spec)['image_ids'] == baseline,
                'local subject remains or baseline changed before pull')
        self.docker('pull-digest', selected['pull_digest'], mutate=True, timeout=60)
        raw, _, _ = self.docker('subject-pulled', selected['inspect_pulled'])
        image.validate_inspect(raw, spec=self.spec, stage='pulled')
        self.docker('tag-export', selected['tag_export'], mutate=True)
        raw, _, _ = self.docker('subject-export-tagged', selected['inspect_export-tagged'])
        image.validate_inspect(raw, spec=self.spec, stage='export-tagged')
        raw, stderr, _ = self.docker('save-export', selected['save_export'], timeout=60)
        require(not stderr, 'registry subject export stderr')
        exported = image.validate_export(raw, spec=self.spec)
        self.document('export-content.json', exported)
        self.docker('remove-export', selected['remove_seed'], mutate=True)
        self.docker('remove-digest', selected['remove_digest'], mutate=True)
        raw, _, _ = self.docker('final-image-absence', selected['inventory'])
        require(image.validate_absent(raw, spec=self.spec)['image_ids'] == baseline, 'registry image baseline differs')
        self.document('workload-credentials.json', self.store.check_unchanged(before))
        self.workload_complete = True
        return {'remote_content': remote_proof, 'export': exported, 'subject_absent_before_pull': True,
                'subject_absent_after_workload': True, 'baseline_image_ids_preserved': True,
                'release_certified': False, 'independent_command_replay_complete': False}

    def cleanup(self):
        """Only completed workloads authorize this exact, non-recursive cleanup."""
        self.certain()
        require(self.workload_complete and not self.cleanup_complete, 'registry cleanup not admitted')
        before = self.store.snapshot(expected='login')
        self.docker('logout', ['logout', self.spec['authority']], mutate=True)
        self.document('logout-credentials.json', self.store.check_transition(before, expected='empty'))
        self.credential_state = 'empty'
        self.inspect_server(running=True)
        final = self.retain_log('final', self.logs(), 'final')
        self.docker('server-stop', ['container', 'stop', '--timeout', '10', self.container_id], mutate=True)
        row = self.inspect_server(running=False)
        require(row['State']['ExitCode'] == 0 and not row['State']['OOMKilled'], 'registry did not stop cleanly')
        self.docker('server-remove', ['container', 'rm', self.container_id], mutate=True)
        self.harness.exact_absent(self.descriptor, 'container', self.spec['container_name'])
        raw, _, _ = self.docker('network-final', ['network', 'inspect', self.spec['network_name']])
        row = network_identity(raw, self.spec)
        require(row['Id'] == self.network['Id'] and not row.get('Containers'), 'registry network still attached')
        raw, _, _ = self.docker('volume-final', ['volume', 'inspect', self.spec['volume_name']])
        require(volume_identity(raw, self.spec) == self.volume, 'registry volume replaced')
        self.docker('volume-remove', ['volume', 'rm', self.spec['volume_name']], mutate=True)
        self.docker('network-remove', ['network', 'rm', self.network['Id']], mutate=True)
        raw = self.public_exec('guest-cleanup', guest.cleanup_script(self.plan, self.identities))
        guest.parse_ack(raw, self.plan, action='CLEANUP', expected=self.identities)
        self.docker('registry-image-remove', ['image', 'rm', '--no-prune', 'docker.io/library/registry:3.1.1'], mutate=True)
        self.harness.exact_absent(self.descriptor, 'image', 'docker.io/library/registry:3.1.1')
        self.cleanup_complete = True
        proof = {'owner': self.descriptor['owner'], 'container_id': self.container_id,
                 'instance_id': self.instance_id, 'final_log_sha256': final['raw_sha256'],
                 'final_log_handshake_errors': final['handshake_errors'],
                 'owned_registry_removed': True, 'guest_private_fixture_removed': True,
                 'full_environment_delete_certified': False}
        self.document('cleanup.json', proof)
        return proof
