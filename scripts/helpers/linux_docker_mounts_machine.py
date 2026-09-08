"""Installed-Machine adapter for the DEV storage-mount recipes.

Covers `docker.storage.bind_mounts`, `named_volumes`, `tmpfs`,
`read_only_mounts` and `ownership` on one authenticated Developer Linux Machine
through its private `--config`/`--context` route. The workload image is the
harness sentinel's digest-pinned developer probe rootfs (BusyBox, imported by
`image import`); nothing is pulled.

The Docker host of a Linux Machine is the Machine, so "host bytes" are written
and read back through the public `vz exec` route, never through the container
under test. The probe fixture only reports what it observed: this module decides
whether a refusal was the required outcome, so a write the Engine allowed can
never be read as a denial.

`other_machine_cannot_read` is a cross-Machine claim: each Machine records its
own owned volume name and full volume inventory, and `verify_machines` requires
that no Machine's inventory contains another Machine's volume. No exception path
removes a container or volume; a failed Session stays registered on
`harness.mounts_sessions` with `cleanup_complete` False.
"""
import base64
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
SCOPE = 'DEV_installed_Machine_storage_mounts_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
FIXTURE = REPO / 'tests/fixtures/vz-0.4/docker-mounts'
LABEL = 'dev.vz.mounts-proof'
BUSYBOX = '/bin/busybox'
PROBE_SHA256 = 'ab68ad96992b947c66b1e7b134401c0b875f799bbd93d00934e21e6577487e4c'
PROBE_BYTES = 2848
# bytes(range(256)) * 16: every byte value, so a text-mangling transport fails.
INPUT = bytes(range(256)) * 16
INPUT_SHA256 = 'c8f5d0341d54d951a71b136e6e2afcb14d11ed8489a7ae126a8fee0df6ecf193'
# The probe writes the input digest plus cut's newline; 64 hex bytes + '\n'.
OUTPUT = (INPUT_SHA256 + '\n').encode('ascii')
OUTPUT_SHA256 = 'c3700c93de3acbdf0602e91d3148af7b43a6d4dc2be9068ad988b1adc74da771'
# Linux TMPFS_MAGIC 0x01021994, as `stat -f -c %t` prints it (hex, no prefix).
TMPFS_MAGIC = '1021994'
UID = GID = 10001
CASES = ('bind', 'ownership', 'readonly', 'tmpfs_recreate', 'tmpfs_write', 'volume_read', 'volume_write')
EXEC_TIMEOUT = 30
RUN_TIMEOUT = 60


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_mounts_machine.py', 'docker_host_driver.py', 'linux_docker_buildkit_cgroup.py',
        'installed_developer_startup.py', 'linux_docker_e2e.py')] + [
        str(FIXTURE / 'probe.sh'), str(FIXTURE / 'fixture.json')]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact mounts source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'mounts source changed: ' + name)


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
        raise ValueError('mounts: malformed JSON output') from error


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def fixture_contract(root=FIXTURE):
    """Pinned probe bytes and a contract that repeats every module constant."""
    root = Path(root)
    script = driver.regular(root / 'probe.sh', LIMIT)
    require(len(script) == PROBE_BYTES and sha256(script) == PROBE_SHA256, 'mounts probe bytes differ from pin')
    contract = parse(driver.regular(root / 'fixture.json', LIMIT))
    require(type(contract) is dict and contract.get('schema_version') == 1, 'mounts fixture contract shape')
    probe = contract['probe']
    require(probe['path'] == 'probe.sh' and probe['sha256'] == PROBE_SHA256 and probe['bytes'] == PROBE_BYTES and
            probe['interpreter'] == [BUSYBOX, 'sh', '-c'] and probe['cases'] == sorted(CASES),
            'probe contract differs from pin')
    bind = contract['bind']
    require(bind['host_to_container_sha256'] == INPUT_SHA256 and bind['host_to_container_bytes'] == len(INPUT) and
            bind['container_to_host_sha256'] == OUTPUT_SHA256 and bind['container_to_host_bytes'] == len(OUTPUT) and
            bind['only_declared_workspace_visible'] is True and
            (bind['input_name'], bind['output_name'], bind['undeclared_sibling']) == ('input', 'output', 'undeclared'),
            'bind contract differs from pin')
    require(contract['tmpfs'] == {'filesystem_type': 'tmpfs', 'filesystem_magic': TMPFS_MAGIC,
                                  'data_absent_after_recreate': True}, 'tmpfs contract differs from pin')
    require(contract['read_only'] == {'root_write': 'refused', 'readonly_mount_write': 'refused',
                                      'declared_writable_mount_write': 'written'}, 'read-only contract differs from pin')
    require(contract['ownership'] == {'uid': UID, 'gid': GID, 'forbidden_write': 'refused',
                                      'writable_mount_write': 'written'}, 'ownership contract differs from pin')
    return {'script': script.decode('ascii'), 'contract': contract, 'script_sha256': PROBE_SHA256,
            'contract_sha256': sha256(driver.regular(root / 'fixture.json', LIMIT))}


def report(raw, stderr, owner, case):
    """Parse the probe's own lines; it reports facts and never decides them."""
    require(stderr == b'', 'mounts probe emitted diagnostics: ' + repr(stderr[:200]))
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT and raw.endswith(b'\n'), 'unterminated probe report')
    rows = {}
    lines = raw.decode('ascii').splitlines()
    for line in lines:
        parts = line.split(' ')
        require(len(parts) == 3 and parts[0] == 'VZMOUNT' and parts[1] == owner, 'foreign or malformed probe line')
        key, _, value = parts[2].partition('=')
        require(key and key not in rows, 'duplicate or nameless probe key: ' + key)
        rows[key] = value
    require(rows.get('case') == case and rows.get('end') == case, 'probe reported another case')
    require(rows.get('uid') is not None and rows.get('gid') is not None, 'probe did not report its identity')
    return rows


def sentinel_image(harness, descriptor):
    rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and row.get('descriptor') == descriptor]
    require(len(rows) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', rows[0].get('image_id') or ''),
            'exact owned sentinel image required')
    return rows[0]['image_id']


class Session:
    """Owned containers and one owned volume of a Machine; never self-cleans."""

    def __init__(self, harness, descriptor, image_id, token, script):
        self.harness, self.descriptor, self.image_id, self.token = harness, descriptor, image_id, token
        self.script = script
        self.root = '/run/vz-mounts-' + token
        self.volume_name = token + '-state'
        self.names, self.ids = {}, {}
        self.volume_created = False
        self.cleanup_complete = False

    # ---- public Exec: the Machine is the Docker host of these mounts ----

    def exec_argv(self, script):
        owner = self.descriptor['owner']
        return [str(self.harness.cli), 'exec', '--environment', owner['environment_id'],
                '--machine', owner['machine_id'], '--no-stdin', '--timeout', str(EXEC_TIMEOUT),
                '--', BUSYBOX, 'sh', '-c', script]

    def public_exec(self, label, script):
        # The public CLI binds by working directory, so the project path comes
        # from the same authenticated binding the process observer uses.
        project = binding.project_binding(self.harness, self.descriptor)
        raw, stderr, code = self.harness.command('mounts-' + label, self.exec_argv(script),
                                                 cwd=Path(project['project_path']),
                                                 timeout=EXEC_TIMEOUT + 10, success=False)
        require(type(code) is int and code == 0 and stderr == b'', 'mounts guest observation failed; evidence retained')
        require(binding.project_binding(self.harness, self.descriptor) == project,
                'Machine/project binding changed during the guest observation')
        return raw

    def seed_host(self):
        """Write the declared source and an undeclared sibling into the Machine."""
        payload = base64.b64encode(INPUT).decode('ascii')
        script = ('set -eu; ' + BUSYBOX + ' mkdir -p ' + self.root + '/workspace ' + self.root + '/undeclared; '
                  "printf '%s' '" + payload + "' | " + BUSYBOX + ' base64 -d > ' + self.root + '/workspace/input; '
                  "printf 'undeclared' > " + self.root + '/undeclared/marker; '
                  + BUSYBOX + ' sha256sum ' + self.root + '/workspace/input | ' + BUSYBOX + ' cut -d" " -f1')
        raw = self.public_exec('seed', script)
        require(raw.decode('ascii').strip() == INPUT_SHA256, 'seeded host bytes differ from the pinned input')
        return {'root': self.root, 'input_sha256': INPUT_SHA256, 'input_bytes': len(INPUT)}

    def read_host_output(self):
        script = (BUSYBOX + ' sha256sum ' + self.root + '/workspace/output | ' + BUSYBOX + ' cut -d" " -f1; '
                  + BUSYBOX + ' wc -c < ' + self.root + '/workspace/output | ' + BUSYBOX + ' tr -d " "')
        values = self.public_exec('read-output', script).decode('ascii').split()
        require(len(values) == 2 and values[0] == OUTPUT_SHA256 and values[1] == str(len(OUTPUT)),
                'container-to-host bytes differ from the pinned output')
        return {'output_sha256': OUTPUT_SHA256, 'output_bytes': len(OUTPUT)}

    def retire_host(self):
        self.public_exec('retire', 'set -eu; ' + BUSYBOX + ' rm -rf ' + self.root + '; '
                         '[ ! -e ' + self.root + ' ] && printf retired')

    # ---- owned Docker resources ----

    def run(self, role, case, args, *, extra=(), timeout=RUN_TIMEOUT):
        name = self.token + '-' + role
        require(role not in self.ids, 'mounts role already used')
        self.names[role] = name
        self.harness.exact_absent(self.descriptor, 'container', name)
        # `mutate` already refuses a nonzero host completion; the probe reports
        # a refused write rather than failing, so exit 0 is the only outcome.
        raw, stderr, _ = self.harness.mutate('mounts-run-' + role, self.descriptor,
            ['run', '--network', 'none', '--restart', 'no', '--name', name, '--label', LABEL + '=' + self.token,
             *args, self.image_id, BUSYBOX, 'sh', '-c', self.script, 'vzmounts', case, self.token, *extra],
            timeout=timeout)
        self.ids[role] = name
        return report(raw, stderr, self.token, case)

    def volume_absent(self, label):
        # `exact_absent` filters non-container kinds by `reference=`, which is an
        # image filter the volume endpoint rejects; list and compare exactly.
        require(self.volume_name not in self.volume_inventory(label), 'owned volume name already exists')

    def create_volume(self):
        self.volume_absent('before-create')
        raw, stderr, _ = self.harness.mutate('mounts-volume-create', self.descriptor,
            ['volume', 'create', '--label', LABEL + '=' + self.token, self.volume_name])
        require(stderr == b'' and raw.decode('ascii').strip() == self.volume_name, 'volume create acknowledgement differs')
        self.volume_created = True
        return self.volume_name

    def inspect_volume(self):
        raw, stderr, _ = self.harness.docker('mounts-volume-inspect', self.descriptor,
                                             ['volume', 'inspect', self.volume_name])
        rows = parse(raw)
        require(stderr == b'' and type(rows) is list and len(rows) == 1, 'ambiguous owned volume')
        row = rows[0]
        require(row['Name'] == self.volume_name and row['Labels'] == {LABEL: self.token} and
                row['Driver'] == 'local' and row['Scope'] == 'local', 'owned volume identity differs')
        return {'name': row['Name'], 'driver': row['Driver'], 'scope': row['Scope'], 'labels': row['Labels'],
                'mountpoint': row['Mountpoint']}

    def volume_inventory(self, label):
        raw, stderr, _ = self.harness.docker('mounts-volume-ls-' + label, self.descriptor, ['volume', 'ls', '--quiet'])
        require(stderr == b'', 'volume inventory diagnostics')
        names = raw.decode('ascii').split()
        require(len(names) == len(set(names)) and len(names) <= 4096, 'ambiguous volume inventory')
        return sorted(names)

    def remove(self):
        for role in list(self.ids):
            self.harness.mutate('mounts-remove-' + role, self.descriptor, ['container', 'rm', self.ids.pop(role)])
            self.harness.exact_absent(self.descriptor, 'container', self.names[role])
        if self.volume_created:
            self.harness.mutate('mounts-volume-rm', self.descriptor, ['volume', 'rm', self.volume_name])
            self.volume_absent('after-remove')
            self.volume_created = False
        self.retire_host()
        self.cleanup_complete = True


def bind_mounts(session, seeded):
    """host->container bytes, container->host bytes, and nothing else visible."""
    rows = session.run('bind', 'bind',
                       ['--mount', 'type=bind,src=' + session.root + '/workspace,dst=/workspace'],
                       extra=[session.root + '/undeclared'])
    require(rows['input_sha256'] == INPUT_SHA256 and rows['input_bytes'] == str(len(INPUT)),
            'container read other bytes than the host wrote')
    require(rows['output_sha256'] == OUTPUT_SHA256, 'container wrote other bytes than the pinned output')
    # The undeclared sibling of the declared source shares its parent on the
    # Machine; a container that can see it is not confined to its declaration.
    require(rows['undeclared'] == 'absent', 'an undeclared host path was visible in the container')
    require(rows['workspace_source_count'] == '1', 'the declared workspace has more than one source')
    written = session.read_host_output()
    return {'host_to_container_sha256': INPUT_SHA256, 'host_to_container_bytes': len(INPUT),
            'only_declared_workspace_visible': True, 'source': seeded['root'] + '/workspace', **written}


def named_volumes(session):
    """One owned volume, reused by a second container on the same Machine."""
    name = session.create_volume()
    identity = session.inspect_volume()
    first = session.run('volume-write', 'volume_write',
                        ['--mount', 'type=volume,src=' + name + ',dst=/data'])
    digest = first['payload_sha256']
    require(re.fullmatch('[0-9a-f]{64}', digest), 'volume payload digest shape')
    require(digest == sha256(session.token.encode('ascii')), 'volume payload is not this run\'s owned bytes')
    second = session.run('volume-read', 'volume_read',
                         ['--mount', 'type=volume,src=' + name + ',dst=/data'])
    require(second['payload_sha256'] == digest and second['payload_bytes'] == str(len(session.token)),
            'the reused volume did not return the first container\'s bytes')
    return {'volume_identity': 'owned_named_volume', 'same_machine_reuse_persists': True,
            'payload_sha256': digest, 'volume': identity,
            'inventory': session.volume_inventory('owned')}


def tmpfs(session):
    """A real tmpfs, and nothing of it surviving into a fresh container."""
    first = session.run('tmpfs-write', 'tmpfs_write', ['--tmpfs', '/scratch'])
    require(first['scratch_fstype'] == TMPFS_MAGIC, 'mount at /scratch is not tmpfs')
    require(first['payload_sha256'] == sha256(session.token.encode('ascii')), 'tmpfs payload differs')
    second = session.run('tmpfs-recreate', 'tmpfs_recreate', ['--tmpfs', '/scratch'])
    require(second['scratch_fstype'] == TMPFS_MAGIC and second['payload'] == 'absent',
            'tmpfs data survived a recreate')
    return {'filesystem_type': 'tmpfs', 'filesystem_magic': TMPFS_MAGIC, 'data_absent_after_recreate': True}


def read_only_mounts(session):
    """Root and the read-only bind refuse writes; the declared writable one does not."""
    rows = session.run('readonly', 'readonly',
                       ['--read-only',
                        '--mount', 'type=bind,src=' + session.root + '/workspace,dst=/ro,readonly',
                        '--tmpfs', '/rw:mode=1777'])
    require(rows['root_write'] == 'refused', 'a --read-only root accepted a write')
    require(rows['readonly_mount_write'] == 'refused', 'a readonly bind mount accepted a write')
    require(rows['declared_writable_mount_write'] == 'written', 'the declared writable mount refused a write')
    require(rows['readonly_source_sha256'] == INPUT_SHA256, 'the read-only source is not the pinned input')
    return {'root_write': 'reject', 'readonly_mount_write': 'reject', 'declared_writable_mount_write': 'success'}


def ownership(session):
    """The requested uid/gid runs the process and owns what it creates."""
    rows = session.run('ownership', 'ownership',
                       ['--user', str(UID) + ':' + str(GID),
                        '--read-only',
                        '--mount', 'type=bind,src=' + session.root + '/workspace,dst=/ro,readonly',
                        '--tmpfs', '/rw:mode=1777'])
    require(rows['uid'] == str(UID) and rows['gid'] == str(GID), 'the container did not run as the requested uid/gid')
    require(rows['declared_writable_mount_write'] == 'written', 'the declared writable mount refused the owned write')
    require(rows['created_uid'] == str(UID) and rows['created_gid'] == str(GID),
            'a created file does not carry the requested ownership')
    require(rows['forbidden_write'] == 'refused', 'a forbidden write was accepted')
    return {'uid_gid': str(UID) + ':' + str(GID), 'created_file_ownership': str(UID) + ':' + str(GID),
            'forbidden_write': 'reject'}


def verify_machines(observations):
    """No Machine's volume inventory contains another Machine's owned volume."""
    require(type(observations) is list and len(observations) >= 2, 'cross-Machine volume isolation needs two Machines')
    owned, inventories = [], []
    for row in observations:
        volume = row['named_volumes']
        owned.append(volume['volume']['name'])
        inventories.append(set(volume['inventory']))
    require(len(set(owned)) == len(owned), 'two Machines claimed the same owned volume name')
    for index, inventory in enumerate(inventories):
        require(owned[index] in inventory, 'a Machine cannot see its own owned volume')
        foreign = [name for position, name in enumerate(owned) if position != index and name in inventory]
        require(not foreign, 'a Machine can see another Machine\'s owned volume: ' + ', '.join(foreign))
    return {'schema_version': 1, 'scope': SCOPE, 'machines': len(observations),
            'other_machine_cannot_read': True, 'owned_volumes': sorted(owned),
            'full_storage_isolation_certified': False}


def run_machine(harness, descriptor, scope, proof, images, index):
    """Five storage recipes on one Machine, then exact owned cleanup.

    The caller must already authenticate descriptor/scope/proof through normal
    Up and retain the sentinel monitor. No exception path removes a container or
    volume; a failed Session stays registered on `harness.mounts_sessions`.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index < 3, 'bounded mounts Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    owner = {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')}
    require(descriptor['owner'] == owner, 'mounts Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'mounts Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    fixture = fixture_contract()
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents mounts dispatch')
    sessions = getattr(harness, 'mounts_sessions', None)
    if sessions is None:
        sessions = harness.mounts_sessions = []
    require(len(sessions) == index and all(item.cleanup_complete is True for item in sessions),
            'earlier mounts Session lacks completed cleanup')
    output = harness.evidence / ('mounts-machine-' + str(index))
    require(not os.path.lexists(output), 'mounts Machine evidence directory preexists')
    image_id = sentinel_image(harness, descriptor)
    harness.monitor.check()
    startup.private(output)
    started = time.time_ns()
    token = 'vzmounts-' + uuid.uuid4().hex[:24]
    session = Session(harness, descriptor, image_id, token, fixture['script'])
    sessions.append(session)
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'started_unix_ns': started,
              'token': token, 'image_id': image_id, 'probe_sha256': fixture['script_sha256'],
              'fixture_contract_sha256': fixture['contract_sha256'], 'cases': list(CASES),
              'host_route': 'public_vz_exec_the_Machine_is_the_Docker_host_of_these_mounts'}
    startup.document(output / 'mounts-machine.intent.json', intent)
    seeded = session.seed_host()
    result = {'schema_version': 1, 'scope': SCOPE, 'kind': 'installed_mounts_raw_evidence',
              'token': token, 'image_id': image_id, 'owner': copy.deepcopy(owner),
              'machine_scope': copy.deepcopy(scope), 'started_unix_ns': started,
              'probe_sha256': fixture['script_sha256'], 'fixture_contract_sha256': fixture['contract_sha256'],
              'host_seed': seeded,
              'bind_mounts': bind_mounts(session, seeded),
              'named_volumes': named_volumes(session),
              'tmpfs': tmpfs(session),
              'read_only_mounts': read_only_mounts(session),
              'ownership': ownership(session),
              'compatibility_certified': False, 'release_scenarios_passed': [],
              'cross_machine_volume_isolation_required': True,
              'remaining': ['cross-Machine volume isolation is decided by verify_machines',
                            'aggregate release certification and physical evidence']}
    session.remove()
    result['ended_unix_ns'] = time.time_ns()
    result['cleanup_complete'] = True
    startup.document(output / 'machine-mounts-validation.json', result)
    return result
