"""Installed-Machine adapter for the DEV published-port and network-cleanup recipes.

Covers `docker.network.published_ports` and `docker.network.cleanup` on one
authenticated Developer Linux Machine through its private `--config`/`--context`
route. The workload image is the harness sentinel's digest-pinned developer probe
rootfs (BusyBox `httpd`, imported by `image import`); nothing is pulled.

A published port binds on the Machine, which is the Docker host of these
containers, so both the response fetch and the listener table are read through
the public `vz exec` route rather than from inside the container that published
the port. The listener table is what decides `wildcard_or_lan_listener`: an
inspect record only reports what was requested, never what the kernel bound.

Cleanup is the same claim in reverse and is reported in the `final-cleanup`
phase: once the owned containers and network are gone, no listener, route or
bridge interface of theirs may remain, and an inventory digest of everything the
run never owned must be unchanged. No exception path removes a container or
network; a failed Session stays registered on `harness.netpolicy_sessions`.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_buildkit_cgroup as binding

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_Machine_published_ports_and_network_cleanup_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
FIXTURE = REPO / 'tests/fixtures/vz-0.4/docker-netpolicy'
LABEL = 'dev.vz.netpolicy-proof'
BUSYBOX = '/bin/busybox'
PROBE_SHA256 = 'd107c0f92c6fd97aa06356c302636b36084e3b13b10bde95c4449b70567180c9'
PROBE_BYTES = 881
CONTAINER_PORT = 8080
LOOPBACK = '127.0.0.1'
ROLES = ('alpha', 'beta')
EXEC_TIMEOUT = 30
RUN_TIMEOUT = 60
# A published port must appear on loopback only. Anything bound to the wildcard
# or to a routable Machine address is a LAN listener, which the contract forbids.
WILDCARD = ('0.0.0.0', '::', '*')


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_netpolicy_machine.py', 'docker_host_driver.py', 'linux_docker_buildkit_cgroup.py',
        'installed_developer_startup.py', 'linux_docker_e2e.py')] + [
        str(FIXTURE / 'probe.sh'), str(FIXTURE / 'fixture.json')]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact netpolicy source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'netpolicy source changed: ' + name)


def unique(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate JSON field')
        row[key] = value
    return row


def parse(raw):
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT, 'bounded JSON stream required')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
    except (UnicodeError, ValueError) as error:
        raise ValueError('netpolicy: malformed JSON output') from error


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def response_bytes(owner, role):
    return ('vznet|' + owner + '|' + role).encode('ascii')


def fixture_contract(root=FIXTURE):
    """Pinned probe bytes and a contract that repeats every module constant."""
    root = Path(root)
    script = driver.regular(root / 'probe.sh', LIMIT)
    require(len(script) == PROBE_BYTES and sha256(script) == PROBE_SHA256, 'netpolicy probe bytes differ from pin')
    contract = parse(driver.regular(root / 'fixture.json', LIMIT))
    require(type(contract) is dict and contract.get('schema_version') == 1, 'netpolicy fixture contract shape')
    probe = contract['probe']
    require(probe['path'] == 'probe.sh' and probe['sha256'] == PROBE_SHA256 and probe['bytes'] == PROBE_BYTES and
            probe['interpreter'] == [BUSYBOX, 'sh', '-c'] and probe['container_port'] == CONTAINER_PORT and
            probe['cases'] == ['report', 'serve'], 'probe contract differs from pin')
    published = contract['published_ports']
    require(published['listener_address'] == LOOPBACK and published['assigned_ports_unique'] is True and
            published['wildcard_or_lan_listener'] is False and published['roles'] == list(ROLES) and
            published['response_template'] == 'vznet|{owner}|{role}', 'published-port contract differs from pin')
    require(contract['cleanup'] == {'owned_routes_dns_listeners_mounts_absent': True,
                                    'unrelated_inventory_sha256_unchanged': True},
            'cleanup contract differs from pin')
    return {'script': script.decode('ascii'), 'contract': contract, 'script_sha256': PROBE_SHA256,
            'contract_sha256': sha256(driver.regular(root / 'fixture.json', LIMIT))}


def sentinel_image(harness, descriptor):
    rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and row.get('descriptor') == descriptor]
    require(len(rows) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', rows[0].get('image_id') or ''),
            'exact owned sentinel image required')
    return rows[0]['image_id']


def listener_rows(raw, ports):
    """Parse the Machine's own TCP listener table for the owned ports only.

    `netstat -tln` prints `tcp <recvq> <sendq> <local> <foreign> LISTEN`. Only
    rows whose local port is one this run published are returned; everything
    else on the Machine is none of this suite's business.
    """
    require(type(raw) is bytes and len(raw) <= LIMIT, 'bounded listener table required')
    wanted = {str(port) for port in ports}
    rows = []
    for line in raw.decode('ascii', 'replace').splitlines():
        fields = line.split()
        if len(fields) < 4 or not fields[0].startswith('tcp'):
            continue
        local = fields[3]
        address, _, port = local.rpartition(':')
        if port not in wanted:
            continue
        rows.append({'address': address.strip('[]'), 'port': int(port)})
    return rows


class Session:
    """Owned containers and one owned network of a Machine; never self-cleans."""

    def __init__(self, harness, descriptor, image_id, token, script):
        self.harness, self.descriptor, self.image_id, self.token = harness, descriptor, image_id, token
        self.script = script
        self.network_name = token + '-net'
        self.names, self.ids, self.ports = {}, {}, {}
        self.network_id = None
        self.cleanup_complete = False

    def exec_argv(self, script):
        owner = self.descriptor['owner']
        return [str(self.harness.cli), 'exec', '--environment', owner['environment_id'],
                '--machine', owner['machine_id'], '--no-stdin', '--timeout', str(EXEC_TIMEOUT),
                '--', BUSYBOX, 'sh', '-c', script]

    def public_exec(self, label, script, *, allow_failure=False):
        project = binding.project_binding(self.harness, self.descriptor)
        raw, stderr, code = self.harness.command('netpolicy-' + label, self.exec_argv(script),
                                                 cwd=Path(project['project_path']),
                                                 timeout=EXEC_TIMEOUT + 10, success=False)
        require(type(code) is int and (code == 0 or allow_failure), 'netpolicy guest observation failed')
        require(binding.project_binding(self.harness, self.descriptor) == project,
                'Machine/project binding changed during the guest observation')
        return raw, stderr, code

    def create_network(self):
        require(self.network_name not in self.network_inventory('before-create')['names'],
                'owned network name already exists')
        raw, stderr, _ = self.harness.mutate('netpolicy-network-create', self.descriptor,
            ['network', 'create', '--label', LABEL + '=' + self.token, self.network_name])
        require(stderr == b'', 'network create diagnostics')
        self.network_id = driver.checked_text(raw.decode('ascii').strip(), r'[0-9a-f]{64}', 'owned network ID')
        return self.network_id

    def network_inventory(self, label):
        raw, stderr, _ = self.harness.docker('netpolicy-network-ls-' + label, self.descriptor,
                                             ['network', 'ls', '--format', '{{.ID}} {{.Name}}'])
        require(stderr == b'', 'network inventory diagnostics')
        ids, names = [], []
        for line in raw.decode('ascii').splitlines():
            identity, _, name = line.partition(' ')
            require(identity and name, 'malformed network inventory row')
            ids.append(identity)
            names.append(name)
        require(len(set(ids)) == len(ids), 'ambiguous network inventory')
        return {'ids': sorted(ids), 'names': sorted(names)}

    def serve(self, role):
        """One published container: an ephemeral Machine port on loopback only."""
        name = self.token + '-' + role
        require(role not in self.ids, 'netpolicy role already used')
        self.names[role] = name
        self.harness.exact_absent(self.descriptor, 'container', name)
        raw, stderr, _ = self.harness.mutate('netpolicy-run-' + role, self.descriptor,
            ['run', '--detach', '--network', self.network_name, '--restart', 'no', '--name', name,
             '--label', LABEL + '=' + self.token,
             # An explicit loopback host address with no host port: the Engine
             # assigns the port, so two containers cannot collide by construction
             # and the assignment is the Engine's, not this suite's.
             '--publish', LOOPBACK + '::' + str(CONTAINER_PORT),
             self.image_id, BUSYBOX, 'sh', '-c', self.script, 'vznet', 'serve', self.token, role],
            timeout=RUN_TIMEOUT)
        require(stderr == b'', role + ' run diagnostics')
        self.ids[role] = driver.checked_text(raw.decode('ascii').strip(), r'[0-9a-f]{64}', role + ' container ID')
        return self.ids[role]

    def published_port(self, role):
        """The Engine's own record of where it published the container port."""
        raw, stderr, _ = self.harness.docker('netpolicy-port-' + role, self.descriptor,
                                             ['container', 'inspect', self.ids[role]])
        rows = parse(raw)
        require(stderr == b'' and type(rows) is list and len(rows) == 1, 'ambiguous published container')
        item = rows[0]
        require(item['Id'] == self.ids[role] and item['Name'] == '/' + self.names[role] and
                item['Config']['Labels'][LABEL] == self.token and item['HostConfig']['Runtime'] == 'youki' and
                item['State']['Running'] is True, role + ' published container identity or state differs')
        bindings = item['NetworkSettings']['Ports'][str(CONTAINER_PORT) + '/tcp']
        require(type(bindings) is list and len(bindings) == 1, role + ' has no single published binding')
        row = bindings[0]
        require(row['HostIp'] == LOOPBACK, role + ' published to ' + repr(row['HostIp']) + ', not loopback')
        port = driver.checked_text(row['HostPort'], r'[1-9][0-9]{2,4}', role + ' host port')
        self.ports[role] = int(port)
        return {'role': role, 'container_id': self.ids[role], 'host_ip': LOOPBACK, 'host_port': int(port),
                'container_port': CONTAINER_PORT}

    def fetch(self, role):
        """Read the published response from the Machine, not from a container."""
        port = self.ports[role]
        raw, _, _ = self.public_exec('fetch-' + role,
            BUSYBOX + ' wget -q -O - http://' + LOOPBACK + ':' + str(port) + '/')
        require(raw == response_bytes(self.token, role), role + ' published port returned other bytes')
        return sha256(raw)

    def listeners(self, label, *, ports=None):
        raw, _, _ = self.public_exec(label, BUSYBOX + ' netstat -tln')
        return listener_rows(raw, self.ports.values() if ports is None else ports)

    def unrelated_digest(self, label):
        """Digest of everything on the Machine this run never owned."""
        inventory = self.network_inventory(label)
        owned = {self.network_id} if self.network_id else set()
        rows = sorted(set(inventory['ids']) - owned)
        return {'networks': len(rows), 'sha256': sha256(json.dumps(rows, sort_keys=True,
                                                                   separators=(',', ':')).encode('ascii'))}

    def remove(self):
        for role in list(self.ids):
            self.harness.mutate('netpolicy-remove-' + role, self.descriptor,
                                ['container', 'rm', '--force', self.ids.pop(role)])
            self.harness.exact_absent(self.descriptor, 'container', self.names[role])
        if self.network_id is not None:
            self.harness.mutate('netpolicy-network-rm', self.descriptor, ['network', 'rm', self.network_id])
            require(self.network_name not in self.network_inventory('after-remove')['names'],
                    'owned network survived its own removal')
            self.network_id = None
        self.cleanup_complete = True


def published_ports(session):
    """Loopback-only publication, unique assignments, and the exact response."""
    session.create_network()
    records = []
    for role in ROLES:
        session.serve(role)
        records.append(session.published_port(role))
    ports = [row['host_port'] for row in records]
    require(len(set(ports)) == len(ports), 'the Engine assigned one host port twice')
    responses = {row['role']: session.fetch(row['role']) for row in records}
    require(len(set(responses.values())) == len(responses), 'two roles returned identical bytes')
    rows = session.listeners('listeners-published')
    # One listening socket per owned port, all on loopback. A wildcard or
    # routable bind would also answer from the LAN, which the contract forbids.
    require(len(rows) == len(ports), 'owned published ports have ' + str(len(rows)) + ' listeners, not ' + str(len(ports)))
    for row in rows:
        require(row['address'] == LOOPBACK, 'owned port ' + str(row['port']) + ' is bound to ' + repr(row['address']))
        require(row['address'] not in WILDCARD, 'owned port bound to a wildcard address')
    require({row['port'] for row in rows} == set(ports), 'the listener table does not match the published ports')
    return {'listener_address': LOOPBACK, 'assigned_ports_unique': True,
            'selected_service_response': 'fixture.service_response',
            'wildcard_or_lan_listener': False, 'published': records,
            'response_sha256': responses, 'listeners': rows}


def network_cleanup(session, before):
    """Nothing owned survives removal, and nothing unrelated changed."""
    ports = sorted(session.ports.values())
    network_id = session.network_id
    session.remove()
    remaining = session.listeners('listeners-retired', ports=ports)
    require(not remaining, 'a listener survived on an owned published port: ' + json.dumps(remaining))
    raw, _, code = session.public_exec('routes-retired',
        BUSYBOX + ' ip route show 2>/dev/null; ' + BUSYBOX + ' ip -o link show 2>/dev/null', allow_failure=True)
    text = raw.decode('ascii', 'replace')
    # Moby names a bridge `br-<first 12 of the network ID>`; its interface and
    # any route through it must be gone with the network.
    require(network_id is not None and ('br-' + network_id[:12]) not in text,
            'the owned network bridge interface or route survived removal')
    after = session.unrelated_digest('after-cleanup')
    require(after['sha256'] == before['sha256'],
            'an unrelated network changed across the owned cleanup')
    return {'owned_routes_dns_listeners_mounts_absent': True, 'unrelated_inventory_sha256_unchanged': True,
            'retired_ports': ports, 'retired_network': network_id,
            'unrelated_inventory_sha256': after['sha256'], 'unrelated_networks': after['networks'],
            'full_host_listener_certification': False}


def run_machine(harness, descriptor, scope, proof, images, index):
    """Published ports and their cleanup on one Machine, then exact owned removal.

    The caller must already authenticate descriptor/scope/proof through normal
    Up and retain the sentinel monitor. No exception path removes a container or
    network; a failed Session stays registered on `harness.netpolicy_sessions`.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index < 3, 'bounded netpolicy Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    owner = {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')}
    require(descriptor['owner'] == owner, 'netpolicy Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'netpolicy Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    fixture = fixture_contract()
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents netpolicy dispatch')
    sessions = getattr(harness, 'netpolicy_sessions', None)
    if sessions is None:
        sessions = harness.netpolicy_sessions = []
    require(len(sessions) == index and all(item.cleanup_complete is True for item in sessions),
            'earlier netpolicy Session lacks completed cleanup')
    output = harness.evidence / ('netpolicy-machine-' + str(index))
    require(not os.path.lexists(output), 'netpolicy Machine evidence directory preexists')
    image_id = sentinel_image(harness, descriptor)
    harness.monitor.check()
    startup.private(output)
    started = time.time_ns()
    token = 'vznet-' + uuid.uuid4().hex[:24]
    session = Session(harness, descriptor, image_id, token, fixture['script'])
    sessions.append(session)
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'started_unix_ns': started,
              'token': token, 'image_id': image_id, 'probe_sha256': fixture['script_sha256'],
              'fixture_contract_sha256': fixture['contract_sha256'], 'roles': list(ROLES),
              'host_route': 'public_vz_exec_the_Machine_is_the_Docker_host_of_these_published_ports'}
    startup.document(output / 'netpolicy-machine.intent.json', intent)
    baseline = session.unrelated_digest('before-workload')
    result = {'schema_version': 1, 'scope': SCOPE, 'kind': 'installed_netpolicy_raw_evidence',
              'token': token, 'image_id': image_id, 'owner': copy.deepcopy(owner),
              'machine_scope': copy.deepcopy(scope), 'started_unix_ns': started,
              'probe_sha256': fixture['script_sha256'], 'fixture_contract_sha256': fixture['contract_sha256'],
              'unrelated_baseline': baseline,
              'published_ports': published_ports(session),
              'network_cleanup': network_cleanup(session, baseline),
              'compatibility_certified': False, 'release_scenarios_passed': [],
              'remaining': ['host-wide listener and route certification beyond the owned ports',
                            'aggregate release certification and physical evidence']}
    result['ended_unix_ns'] = time.time_ns()
    result['cleanup_complete'] = True
    startup.document(output / 'machine-netpolicy-validation.json', result)
    return result
