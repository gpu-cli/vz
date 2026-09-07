"""Installed-Machine adapter for the DEV persistence and daemon-recovery recipe.

Covers `docker.storage.persistence` and `docker.operation.daemon_restart_recovery`
(both `phase: persisted-recovery`) on authenticated Developer Linux Machines
through their private `--config`/`--context` routes. The workload image is the
harness sentinel's digest-pinned developer probe rootfs (BusyBox, imported by
`image import`); nothing is pulled. Per selected Machine the Session creates one
labelled named volume, one `--restart always` container that writes the pinned
fixture sentinel into that volume, and one exited container whose writable
layer holds the same pinned bytes. Volume, container and image identities plus
the Machine's Engine ID and incarnation are recorded before and after the
recovery cycle.

Recovery path (honest scope). The guest agent supervises dockerd in
`crates/vz-guest-agent/src/docker.rs::supervise_docker`: when dockerd exits
outside an ordered shutdown the supervisor bails ("owned dockerd exited outside
shutdown"), retains the child handles, and `ensure_started` then refuses with
"Docker supervisor terminated; Machine recovery is required". There is no
in-guest restart loop (docs/docker-facade-artifacts.md: "the former automatic
sibling-kill/restart loop is not safe recovery evidence"). Signalling dockerd
would therefore make the Machine's Engine permanently unavailable and break the
harness's own public Stop, so this module never sends it a signal. The only
documented recovery of the exact owned daemon today is public `vz stop` of the
owning Environment (ShutdownDocker gracefully reaps dockerd/containerd and
closes the ext4 data disk) followed by public `vz up`, which starts fresh
supervision on the same persistent disk. This module exercises exactly that
path through `harness.stop`/`harness.up` and reports
`in_guest_dockerd_restart_supported: False`; it does not fake an in-place
daemon restart. A read-only public Exec observes the guest boot ID and the
single dockerd PID/argv before and after the cycle to prove the owned daemon
was actually restarted (new boot, one dockerd, identical pinned argv).

Environment granularity. Stop/Up acts on a whole Environment, so a cycle
restarts every Machine in it. The harness selects both primary Machines
(indexes 0 and 1) and the first neighbor Machine (index 2). Index 0 therefore
prepares Sessions for BOTH primary Machines, cycles the primary Environment once,
verifies every Machine in it (both Sessions, both sentinels, sibling
inventories), and cleans up only machine index 0's objects. Index 1 finds its
already-verified Session, re-verifies it read-only against the live
incarnation, and performs its own exact cleanup. Index 2 prepares one Session on
the first neighbor Machine, cycles the neighbor Environment once, verifies both
neighbor Machines (the unselected second neighbor Machine contributes an
inventory snapshot and its restarted sentinel) and cleans up. During each cycle
the harness's continuous SentinelMonitor cannot observe the stopped Environment,
so it is positively stopped, a phase monitor over the OTHER Environment's
sentinels runs across Stop/Up, and a resumed monitor over all four sentinels is
installed on `harness.monitor` after the restarted sentinels are started again.
Retired monitors stay on `harness.recovery_monitors`.

No exception path removes any object: a failed Session stays registered on
`harness.recovery_sessions` with `cleanup_complete` False and `failed` True, and
its containers still reference the sentinel image so the harness's own image
removal fails closed. No retries, no force flags, no prune. Readiness after Up
is a declared poll (condition, interval, deadline, samples) recorded in the
evidence. Nothing here certifies a release scenario.
"""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tarfile
import threading
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_Machine_persistence_stop_up_recovery_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
FIXTURE = REPO / 'tests/fixtures/vz-0.4/docker-recovery'
SUPERVISION_SOURCE = REPO / 'crates/vz-guest-agent/src/docker.rs'
LABEL = 'dev.vz.linux-compose-proof'
BUSYBOX = '/bin/busybox'
SENTINEL_SHA256 = 'e8bde1b5f95fce44ceaa03005051f8d2e01847f9e052f4ff00557a5a49c074aa'
SENTINEL_BYTES = 43
VOLUME_PATH = '/data/persistence-sentinel'
VOLUME_TARGET = '/data'
WRITABLE_PATH = '/vz-recovery-writable-sentinel'
ALWAYS_COMMAND = [BUSYBOX, 'sleep', '7200']
RESTART_POLICY = {'Name': 'always', 'MaximumRetryCount': 0}
READINESS = {'condition': 'docker info --format {{.ID}} through the same private context returns the pinned Engine ID '
                          'and, where a Session exists, container inspect of the restart=always container reports '
                          'State.Running true with a StartedAt later than before Stop',
             'interval_seconds': 1, 'deadline_seconds': 60,
             'clock': 'host time_ns measured from the positive public Up terminal receipt'}
RECOVERY_PATH = {'kind': 'public_stop_then_public_up_of_the_owning_environment',
                 'in_guest_dockerd_restart_supported': False,
                 'supervision_source': 'crates/vz-guest-agent/src/docker.rs'}
SUPERVISION_TRUTH = ('crates/vz-guest-agent/src/docker.rs supervise_docker: dockerd exit outside shutdown bails '
                     '"owned dockerd exited outside shutdown" and retains the child handles; ensure_started refuses '
                     '"Docker supervisor terminated; Machine recovery is required"; no in-guest restart exists')
# Pinned from crates/vz-guest-agent/src/docker.rs dockerd_args(); a drift here is a review event, not noise.
DOCKERD_ARGV = ['/mnt/vz-docker-bin/dockerd', '--host', 'unix:///run/vz-docker/docker.sock',
                '--containerd', '/run/vz-docker/containerd/containerd.sock',
                '--data-root', '/var/lib/docker/engine', '--exec-root', '/run/vz-docker/dockerd',
                '--pidfile', '/run/vz-docker/dockerd.pid', '--config-file', '/var/lib/docker/config/daemon.json',
                '--add-runtime', 'youki=/mnt/linux-bin/youki', '--default-runtime', 'youki']
DAEMON_SCRIPT = '\n'.join((
    'set -eu', 'bb=/bin/busybox', 'found=0', "printf 'VZ_RECOVERY_DAEMON_V1\\n'",
    "printf 'BOOT_ID=%s\\n' \"$(\"$bb\" cat /proc/sys/kernel/random/boot_id)\"",
    'for d in /proc/[0-9]*; do',
    '  [ "$("$bb" cat "$d/comm" 2>/dev/null || true)" = dockerd ] || continue',
    '  found=$((found+1))',
    "  printf 'PID=%s\\n' \"${d#/proc/}\"",
    "  \"$bb\" tr '\\0' '\\n' < \"$d/cmdline\" | while IFS= read -r a; do printf 'ARG=%s\\n' \"$a\"; done",
    'done',
    "printf 'COUNT=%s\\n' \"$found\"",
    "printf 'VZ_RECOVERY_DAEMON_END\\n'"))
# Scenario selection knowledge (linux_docker_e2e.ComposeHarness.scenario): both
# primary Machines are selected (indexes 0 and 1); only the first neighbor
# Machine is selected (index 2). Index 0's cycle therefore owns two Sessions.
SESSIONS_PER_CYCLE = {0: 2, 2: 1}
MAX_INDEX = 2


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_recovery_machine.py', 'docker_host_driver.py', 'installed_developer_startup.py',
        'linux_docker_e2e.py')] + [str(FIXTURE / 'persistence-sentinel.txt'), str(FIXTURE / 'fixture.json'),
                                   str(SUPERVISION_SOURCE)]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact recovery source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'recovery source changed: ' + name)


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
        raise ValueError('recovery: malformed JSON output') from error


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def same(left, right, reason):
    require(canonical(left) == canonical(right), reason)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def fixture_contract(root=FIXTURE):
    """Pinned sentinel bytes and a fixture contract that repeats every module constant."""
    root = Path(root)
    sentinel = driver.regular(root / 'persistence-sentinel.txt', LIMIT)
    require(len(sentinel) == SENTINEL_BYTES and sha256(sentinel) == SENTINEL_SHA256, 'persistence sentinel bytes differ from pin')
    text = sentinel.decode('ascii')
    require(text.endswith('\n') and text.count('\n') == 1 and re.fullmatch(r'[A-Za-z0-9._-]+', text[:-1]),
            'persistence sentinel must be one printf-safe ASCII line')
    contract = parse(driver.regular(root / 'fixture.json', LIMIT))
    require(type(contract) is dict and contract.get('schema_version') == 1 and contract.get('scope') == SCOPE,
            'recovery fixture contract shape')
    pinned = contract['persistence_sentinel']
    require(pinned['path'] == 'persistence-sentinel.txt' and pinned['sha256'] == SENTINEL_SHA256 and
            pinned['bytes'] == SENTINEL_BYTES and pinned['volume_path'] == VOLUME_PATH and
            pinned['writable_layer_path'] == WRITABLE_PATH and pinned['interpreter'] == [BUSYBOX, 'sh', '-c'],
            'persistence sentinel contract differs from pin')
    workload = contract['workload']
    require(workload['restart_policy'] == RESTART_POLICY['Name'] and workload['always_command'] == ALWAYS_COMMAND and
            workload['network_mode'] == 'none', 'workload contract differs from pin')
    same(contract['readiness_poll'], READINESS, 'readiness poll contract differs from pin')
    same(contract['recovery_path'], RECOVERY_PATH, 'recovery path contract differs from pin')
    return {'sentinel': text, 'sentinel_line': text[:-1], 'sentinel_sha256': SENTINEL_SHA256, 'contract': contract,
            'contract_sha256': sha256(driver.regular(root / 'fixture.json', LIMIT))}


def parse_daemon(raw, stderr):
    """Guest boot ID plus exactly one dockerd process with the pinned argv."""
    require(stderr == b'', 'daemon observation emitted stderr')
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT, 'bounded daemon observation required')
    try:
        lines = raw.decode('utf-8').split('\n')
    except UnicodeError as error:
        raise ValueError('recovery: non-UTF-8 daemon observation') from error
    require(len(lines) >= 5 and lines[0] == 'VZ_RECOVERY_DAEMON_V1' and lines[-1] == '' and
            lines[-2] == 'VZ_RECOVERY_DAEMON_END', 'incomplete daemon observation frames')
    body = lines[1:-2]
    require(body and body[0].startswith('BOOT_ID=') and body[-1] == 'COUNT=1', 'exactly one dockerd process required')
    boot_id = body[0][len('BOOT_ID='):]
    require(re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', boot_id), 'guest boot ID shape')
    pids, argv = [], []
    for line in body[1:-1]:
        kind, separator, value = line.partition('=')
        require(separator == '=' and kind in ('PID', 'ARG'), 'unknown daemon observation frame')
        (pids if kind == 'PID' else argv).append(value)
    require(len(pids) == 1 and re.fullmatch(r'[1-9][0-9]*', pids[0]), 'one dockerd PID required')
    require(argv == DOCKERD_ARGV, 'dockerd argv differs from the pinned guest supervisor: ' + repr(argv))
    return {'boot_id': boot_id, 'pid': int(pids[0]), 'argv': list(argv), 'stdout_sha256': sha256(raw)}


def parse_ids(raw, stderr, pattern, reason):
    require(stderr == b'', reason + ' emitted stderr')
    require(type(raw) is bytes and (raw == b'' or raw.endswith(b'\n')), reason + ' incomplete')
    items = raw.decode('ascii').split()
    require(all(re.fullmatch(pattern, item) for item in items) and len(set(items)) == len(items), reason + ' shape')
    return sorted(items)


def parse_sha256sum(raw, stderr, path):
    require(stderr == b'', 'sha256sum emitted stderr')
    match = re.fullmatch(r'([0-9a-f]{64})  ' + re.escape(path) + r'\n', (raw or b'').decode('ascii', 'replace'))
    require(match is not None, 'sha256sum output shape')
    return match[1]


def parse_diff(raw, stderr, path):
    require(stderr == b'', 'diff emitted stderr')
    lines = (raw or b'').decode('ascii', 'replace').split('\n')
    require(lines[-1] == '' and 'A ' + path in lines[:-1], 'writable layer lacks the pinned added file')
    return lines[:-1]


def tar_member(raw, name):
    """Exactly one regular member from `docker cp <container>:<path> -`."""
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        members = archive.getmembers()
        require(len(members) == 1 and members[0].isfile() and members[0].name == name, 'cp tar inventory differs')
        return archive.extractfile(members[0]).read()


def project_path(harness, owner):
    """Resolve the retained exact owned project from the harness topology document."""
    try:
        raw = startup.read_private_regular(Path(harness.evidence) / 'topology.json', startup.LIMIT)
        topology = json.loads(raw, object_pairs_hook=unique)
        project = startup.canonical(topology['project']) if isinstance(topology.get('project'), str) else None
        definition = json.loads(startup.read_private_regular(project / 'vz.json', startup.LIMIT),
                                object_pairs_hook=unique) if project is not None and project.is_dir() else None
        root = startup.canonical(str(harness.root))
    except (OSError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError('recovery: unavailable or malformed owned project binding') from error
    require(project is not None and definition is not None and project.parent == root,
            'project is not an exact owned fixture child')
    require(isinstance(definition, dict) and definition.get('schema_version') == 1 and
            definition.get('project_id') == owner['project_id'], 'project definition owner differs')
    environments = [topology.get(key) for key in ('primary', 'neighbor')]
    require(any(isinstance(env, dict) and env.get('environment_id') == owner['environment_id'] and
                env.get('project_id') == owner['project_id'] for env in environments),
            'retained topology lacks the owning Environment')
    return project


def observe_daemon(harness, descriptor, project, label):
    """Bounded read-only public Exec: guest boot ID and the exact owned dockerd."""
    owner = descriptor['owner']
    raw, stderr, code = harness.command(label, [harness.cli, 'exec', '--environment', owner['environment_id'],
        '--machine', owner['machine_id'], '--no-stdin', '--timeout', '30', '--', BUSYBOX, 'sh', '-c', DAEMON_SCRIPT],
        cwd=project, timeout=40, success=False)
    require(type(code) is int and code == 0, 'public Exec failed; raw diagnostic retained')
    proof = parse_daemon(raw, stderr)
    proof.update(owner=copy.deepcopy(owner), incarnation_id=descriptor['incarnation_id'],
                 incarnation_generation=descriptor['incarnation_generation'], command_label=label)
    return proof


def inventory(harness, descriptor, label):
    """Object identity sets; states legitimately change across a VM restart."""
    raw, stderr, _ = harness.docker(label + '-containers', descriptor, ['container', 'ls', '--all', '--quiet', '--no-trunc'])
    containers = parse_ids(raw, stderr, r'[0-9a-f]{64}', 'container inventory')
    raw, stderr, _ = harness.docker(label + '-volumes', descriptor, ['volume', 'ls', '--quiet'])
    volumes = parse_ids(raw, stderr, r'[A-Za-z0-9][A-Za-z0-9_.-]*', 'volume inventory')
    raw, stderr, _ = harness.docker(label + '-images', descriptor, ['image', 'ls', '--all', '--quiet', '--no-trunc'])
    images = parse_ids(raw, stderr, r'sha256:[0-9a-f]{64}', 'image inventory')
    return {'containers': containers, 'volumes': volumes, 'images': images}


def sentinel_row(harness, descriptor):
    rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and
            row.get('descriptor', {}).get('owner') == descriptor['owner']]
    require(len(rows) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', rows[0].get('image_id') or '') and
            re.fullmatch(r'[0-9a-f]{64}', rows[0].get('container_id') or ''), 'exact owned sentinel required')
    return rows[0]


def stable_context(before, after):
    require(all(before[key] == after[key] for key in ('owner', 'name', 'endpoint', 'config_dir', 'engine_id')),
            'Machine context/Engine identity changed across Stop/Up')
    require(after['incarnation_generation'] > before['incarnation_generation'] and
            after['incarnation_id'] != before['incarnation_id'], 'Machine incarnation did not advance')


def phase_monitor(harness, rows, output):
    """A SentinelMonitor with its own evidence directory; the harness's default path is single-use."""
    import linux_docker_e2e as gate

    class PhaseMonitor(gate.SentinelMonitor):
        def __init__(self, harness, rows, output):
            require(rows, 'phase monitor needs at least one sentinel')
            self.harness, self.rows = harness, rows
            self.output = startup.private(output)
            self.record = startup.Recorder(self.output, harness.env)
            self.finished, self.first = threading.Event(), threading.Event()
            self.samples, self.errors = [], []
            self.thread = threading.Thread(target=self.loop, name='vz-recovery-liveness', daemon=False)
    return PhaseMonitor(harness, rows, output)


def readiness_poll(harness, descriptor, label, started_ns, session=None):
    """Declared poll: condition, interval, deadline and every sample are recorded; no retries elsewhere."""
    deadline = started_ns + READINESS['deadline_seconds'] * 10 ** 9
    interval_ns = READINESS['interval_seconds'] * 10 ** 9
    samples = []
    while True:
        sample = {'index': len(samples) + 1, 'unix_ns': time.time_ns(), 'healthy': False, 'detail': None}
        raw, stderr, code = harness.docker(label + '-info-' + str(sample['index']), descriptor,
                                           ['info', '--format', '{{.ID}}'], timeout=15, success=False)
        engine_ok = code == 0 and stderr == b'' and raw.decode('ascii', 'replace').strip() == descriptor['engine_id']
        sample['detail'] = {'engine_exit': code, 'engine_ok': engine_ok}
        if engine_ok and session is not None:
            raw, stderr, code = harness.docker(label + '-always-' + str(sample['index']), descriptor,
                                               ['container', 'inspect', session.ids['always']], timeout=15, success=False)
            running = False
            if code == 0 and stderr == b'':
                items = parse(raw)
                running = (type(items) is list and len(items) == 1 and items[0].get('Id') == session.ids['always'] and
                           items[0]['State'].get('Running') is True and
                           items[0]['State'].get('StartedAt') > session.before['always']['State']['StartedAt'])
            sample['detail'].update(always_exit=code, always_running=running)
            sample['healthy'] = running
        else:
            sample['healthy'] = engine_ok and session is None
        samples.append(sample)
        if sample['healthy']:
            break
        require(time.time_ns() + interval_ns <= deadline,
                'readiness deadline exceeded: ' + repr(samples[-1]))
        time.sleep(READINESS['interval_seconds'])
    return {'declared': dict(READINESS), 'started_unix_ns': started_ns, 'healthy_unix_ns': samples[-1]['unix_ns'],
            'healthy_after_seconds': (samples[-1]['unix_ns'] - started_ns) / 10 ** 9, 'samples': samples}


class Session:
    """Owned volume and containers of one Machine; never cleans up on its own."""
    def __init__(self, harness, descriptor, image_id, token, index, fixture):
        self.harness, self.descriptor, self.image_id, self.token, self.index = harness, descriptor, image_id, token, index
        self.fixture = fixture
        self.owner = copy.deepcopy(descriptor['owner'])
        self.names = {'volume': token + '-vol', 'always': token + '-always', 'stopped': token + '-stopped'}
        self.ids = {}
        self.before, self.after, self.live_descriptor = None, None, None
        self.verified_incarnations = []
        self.cleanup_complete = False
        self.failed = False

    def docker(self, label, args, descriptor=None, **kwargs):
        return self.harness.docker('recovery-' + label, descriptor or self.descriptor, args, **kwargs)

    def mutate(self, label, args, **kwargs):
        return self.harness.mutate('recovery-' + label, self.descriptor, args, **kwargs)

    def inspect_container(self, role, label, descriptor=None):
        raw, stderr, _ = self.docker(label, ['container', 'inspect', self.ids[role]], descriptor)
        items = parse(raw)
        require(stderr == b'' and type(items) is list and len(items) == 1, 'ambiguous container inspection')
        item = items[0]
        require(item['Id'] == self.ids[role] and item['Name'] == '/' + self.names[role] and item['Image'] == self.image_id and
                item['Config']['Labels'][LABEL] == self.token and item['HostConfig']['Runtime'] == 'youki' and
                item['HostConfig']['NetworkMode'] == 'none', role + ' container identity differs')
        if role == 'always':
            require(item['HostConfig']['RestartPolicy'] == RESTART_POLICY, 'restart policy differs from request')
            mounts = [{key: mount[key] for key in ('Type', 'Name', 'Destination')} for mount in item['Mounts']]
            require(mounts == [{'Type': 'volume', 'Name': self.names['volume'], 'Destination': VOLUME_TARGET}],
                    'always container volume mount differs')
        else:
            require(item['HostConfig']['RestartPolicy']['Name'] in ('', 'no') and item['Mounts'] == [],
                    'stopped container must have no restart policy or mounts')
        return item

    def inspect_volume(self, label, descriptor=None):
        raw, stderr, _ = self.docker(label, ['volume', 'inspect', self.names['volume']], descriptor)
        items = parse(raw)
        require(stderr == b'' and type(items) is list and len(items) == 1, 'ambiguous volume inspection')
        item = items[0]
        require(item['Name'] == self.names['volume'] and item['Labels'] == {LABEL: self.token} and
                item['Driver'] == 'local' and item['Scope'] == 'local', 'volume identity differs')
        return item

    def volume_bytes(self, label, descriptor=None):
        raw, stderr, _ = self.docker(label + '-cat', ['exec', self.ids['always'], BUSYBOX, 'cat', VOLUME_PATH], descriptor)
        require(stderr == b'' and raw == self.fixture['sentinel'].encode(), 'volume sentinel bytes differ from pin')
        raw, stderr, _ = self.docker(label + '-sha256', ['exec', self.ids['always'], BUSYBOX, 'sha256sum', VOLUME_PATH], descriptor)
        require(parse_sha256sum(raw, stderr, VOLUME_PATH) == SENTINEL_SHA256, 'in-container volume digest differs from pin')
        return SENTINEL_SHA256

    def writable_bytes(self, label, descriptor=None):
        raw, stderr, _ = self.docker(label + '-cp', ['container', 'cp', self.ids['stopped'] + ':' + WRITABLE_PATH, '-'], descriptor)
        require(stderr == b'', 'cp emitted stderr')
        data = tar_member(raw, WRITABLE_PATH.lstrip('/'))
        require(data == self.fixture['sentinel'].encode() and sha256(data) == SENTINEL_SHA256,
                'writable-layer sentinel bytes differ from pin')
        raw, stderr, _ = self.docker(label + '-diff', ['container', 'diff', self.ids['stopped']], descriptor)
        return {'sha256': sha256(data), 'diff': parse_diff(raw, stderr, WRITABLE_PATH)}

    def snapshot(self, label, descriptor=None):
        descriptor = descriptor or self.descriptor
        always = self.inspect_container('always', label + '-always', descriptor)
        stopped = self.inspect_container('stopped', label + '-stopped', descriptor)
        require(always['State']['Running'] is True and always['State']['Status'] == 'running', 'always container not running')
        require(stopped['State']['Running'] is False and stopped['State']['Status'] == 'exited' and
                stopped['State']['ExitCode'] == 0, 'stopped container is not cleanly exited')
        raw, stderr, _ = self.docker(label + '-image', ['image', 'inspect', self.image_id, '--format', '{{.Id}}'], descriptor)
        require(stderr == b'' and raw.decode('ascii', 'replace').strip() == self.image_id, 'image identity differs')
        raw, stderr, _ = self.docker(label + '-engine', ['info', '--format', '{{.ID}}'], descriptor)
        require(stderr == b'' and raw.decode('ascii', 'replace').strip() == descriptor['engine_id'], 'Engine identity differs')
        return {'volume': self.inspect_volume(label + '-volume', descriptor), 'always': always, 'stopped': stopped,
                'volume_sentinel_sha256': self.volume_bytes(label + '-volume-bytes', descriptor),
                'writable_layer': self.writable_bytes(label + '-writable', descriptor),
                'image_id': self.image_id, 'engine_id': descriptor['engine_id'],
                'incarnation_id': descriptor['incarnation_id'],
                'incarnation_generation': descriptor['incarnation_generation'], 'unix_ns': time.time_ns()}

    def prepare(self):
        for kind, name in (('volume', self.names['volume']), ('container', self.names['always']),
                           ('container', self.names['stopped'])):
            if kind == 'volume':
                # `volume ls --filter name=` is a substring match; list and compare exactly.
                raw, stderr, _ = self.docker('volume-absent', ['volume', 'ls', '--quiet'])
                require(name not in parse_ids(raw, stderr, r'[A-Za-z0-9][A-Za-z0-9_.-]*', 'volume inventory'),
                        'owned volume name already exists')
            else:
                self.harness.exact_absent(self.descriptor, kind, name)
        raw, stderr, _ = self.mutate('volume-create', ['volume', 'create', '--label', LABEL + '=' + self.token, self.names['volume']])
        require(stderr == b'' and raw == (self.names['volume'] + '\n').encode(), 'volume create output differs')
        raw, stderr, _ = self.mutate('run-always', ['run', '--detach', '--restart', 'always', '--network', 'none',
                                     '--volume', self.names['volume'] + ':' + VOLUME_TARGET, '--label', LABEL + '=' + self.token,
                                     '--name', self.names['always'], self.image_id, *ALWAYS_COMMAND])
        require(stderr == b'', 'always run emitted stderr')
        self.ids['always'] = driver.checked_text(raw.decode().strip(), r'[0-9a-f]{64}', 'always container ID')
        # Written once by the host through exec, never by the container command,
        # so a restart cannot recreate a passing sentinel.
        self.mutate('write-volume-sentinel', ['exec', self.ids['always'], BUSYBOX, 'sh', '-c',
                    'printf "%s\\n" "$1" > "$2"', 'sh', self.fixture['sentinel_line'], VOLUME_PATH])
        raw, stderr, _ = self.mutate('run-stopped', ['run', '--network', 'none', '--label', LABEL + '=' + self.token,
                                     '--name', self.names['stopped'], self.image_id, BUSYBOX, 'sh', '-c',
                                     'printf "%s\\n" "$1" > "$2"', 'sh', self.fixture['sentinel_line'], WRITABLE_PATH])
        require(raw == b'' and stderr == b'', 'stopped writer emitted output')
        raw, stderr, _ = self.docker('stopped-id', ['container', 'ls', '--all', '--quiet', '--no-trunc',
                                                     '--filter', 'name=^/' + self.names['stopped'] + '$'])
        self.ids['stopped'] = driver.checked_text(raw.decode().strip(), r'[0-9a-f]{64}', 'stopped container ID')
        self.before = self.snapshot('before')
        return self.before

    def verify_after(self, live):
        """Same context, new incarnation: identities and pinned bytes unchanged, always restarted."""
        require(self.before is not None, 'Session was never prepared')
        require(live['owner'] == self.owner, 'live descriptor belongs to another Machine')
        stable_context(self.descriptor, live)
        after = self.snapshot('after-' + str(len(self.verified_incarnations)), live)
        before = self.before
        same(after['volume'], before['volume'], 'volume identity/labels/mountpoint changed across Stop/Up')
        for role in ('always', 'stopped'):
            for key in ('Id', 'Name', 'Image', 'Created'):
                require(after[role][key] == before[role][key], role + ' container ' + key + ' changed')
            same(after[role]['Config']['Labels'], before[role]['Config']['Labels'], role + ' labels changed')
            same(after[role]['HostConfig']['RestartPolicy'], before[role]['HostConfig']['RestartPolicy'], role + ' policy changed')
        require(after['always']['State']['StartedAt'] > before['always']['State']['StartedAt'],
                'always container was not restarted with a later StartedAt')
        require(after['stopped']['State']['FinishedAt'] == before['stopped']['State']['FinishedAt'] and
                after['stopped']['State']['StartedAt'] == before['stopped']['State']['StartedAt'],
                'stopped container was started by recovery')
        require(after['image_id'] == before['image_id'] and after['engine_id'] == before['engine_id'] and
                after['volume_sentinel_sha256'] == before['volume_sentinel_sha256'] == SENTINEL_SHA256 and
                after['writable_layer']['sha256'] == SENTINEL_SHA256 and
                after['writable_layer']['diff'] == before['writable_layer']['diff'], 'persisted identity or bytes differ')
        require(after['incarnation_generation'] > before['incarnation_generation'], 'incarnation did not advance')
        self.live_descriptor = copy.deepcopy(live)
        self.verified_incarnations.append(live['incarnation_id'])
        self.after = after
        return after

    def cleanup(self):
        """Exact owned cleanup, admitted only after verification; no force, no prune."""
        require(self.after is not None and self.live_descriptor is not None, 'cleanup requires a verified Session')
        live = self.live_descriptor
        self.harness.mutate('recovery-stop-always', live, ['container', 'stop', self.ids['always']], timeout=60)
        item = self.inspect_container('always', 'cleanup-always', live)
        require(item['State']['Running'] is False, 'always container still running after stop')
        for role in ('always', 'stopped'):
            self.harness.mutate('recovery-remove-' + role, live, ['container', 'rm', self.ids[role]])
            self.harness.exact_absent(live, 'container', self.names[role])
        self.harness.mutate('recovery-remove-volume', live, ['volume', 'rm', self.names['volume']])
        raw, stderr, _ = self.harness.docker('recovery-volume-gone', live, ['volume', 'ls', '--quiet'])
        require(self.names['volume'] not in parse_ids(raw, stderr, r'[A-Za-z0-9][A-Za-z0-9_.-]*', 'volume inventory'),
                'owned volume still exists')
        self.cleanup_complete = True
        return {'containers_removed': [self.names['always'], self.names['stopped']], 'volume_removed': self.names['volume'],
                'force_used': False}

    def record(self):
        return {'index': self.index, 'owner': copy.deepcopy(self.owner), 'token': self.token, 'names': dict(self.names),
                'ids': dict(self.ids), 'image_id': self.image_id, 'before': copy.deepcopy(self.before),
                'after': copy.deepcopy(self.after), 'verified_incarnations': list(self.verified_incarnations),
                'cleanup_complete': self.cleanup_complete, 'failed': self.failed}


def restart_sentinel(harness, row, live):
    """The harness sentinel has no restart policy: it must be exited, then start it exactly once."""
    require(row['descriptor']['owner'] == live['owner'], 'sentinel belongs to another Machine')
    raw, stderr, _ = harness.docker('recovery-sentinel-persisted', live, ['container', 'inspect', row['container_id']])
    items = parse(raw)
    require(stderr == b'' and len(items) == 1, 'exact persisted sentinel missing')
    item = items[0]
    require(item['Id'] == row['container_id'] and item['Image'] == row['image_id'] and
            item['Config']['Labels'][LABEL] == row['token'] and item['State']['Running'] is False and
            item['RestartCount'] == 0, 'sentinel changed ownership or unexpectedly runs after Stop/Up')
    exited = {key: item['State'][key] for key in ('Status', 'ExitCode', 'FinishedAt')}
    raw, stderr, _ = harness.mutate('recovery-sentinel-start', live, ['container', 'start', row['container_id']])
    require(stderr == b'' and raw.decode().strip() == row['container_id'], 'started sentinel identity changed')
    raw, stderr, _ = harness.docker('recovery-sentinel-restarted', live, ['container', 'inspect', row['container_id']])
    items = parse(raw)
    require(stderr == b'' and len(items) == 1 and items[0]['Id'] == row['container_id'] and
            items[0]['State']['Running'] is True and items[0]['RestartCount'] == 0, 'restarted sentinel identity differs')
    raw, stderr, _ = harness.docker('recovery-sentinel-bytes', live, ['exec', row['container_id'], '/bin/cat', '/sentinel'])
    require(stderr == b'' and raw == (row['token'] + '\n').encode(), 'host-written sentinel writable-layer bytes changed')
    previous = {'started_at': row['started_at'], 'incarnation_id': row['descriptor']['incarnation_id']}
    row['started_at'] = items[0]['State']['StartedAt']
    row['descriptor'] = copy.deepcopy(live)
    return {'container_id': row['container_id'], 'exited_after_stop_up': exited, 'previous': previous,
            'started_at': row['started_at'], 'incarnation_id': live['incarnation_id'],
            'scope': 'explicit_restart_of_exact_owned_persisted_sentinel_writable_layer_bytes'}


def cycle_environment(harness, project, environment_id, sessions, output):
    """Public Stop then Up of one Environment with continuous other-Environment liveness."""
    before_status = harness.status(project, environment_id)
    require(before_status['state'] == 'ready' and before_status['environment_id'] == environment_id, 'Environment not ready')
    before_contexts = harness.inspect(before_status)
    machine_ids = [machine['machine_id'] for machine in before_status['machines']]
    by_machine = {context['owner']['machine_id']: context for context in before_contexts}
    rows = [sentinel_row(harness, context) for context in before_contexts]
    other_rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and
                  row['descriptor']['owner']['environment_id'] != environment_id]
    other_contexts = [row['descriptor'] for row in other_rows]
    require(len(rows) == 2 and len(other_rows) == 2, 'four sentinels across two Environments required')
    other_environment = other_rows[0]['descriptor']['owner']['environment_id']
    require(all(row['descriptor']['owner']['environment_id'] == other_environment for row in other_rows),
            'other sentinels span more than one Environment')
    other_before = harness.status(project, other_environment)
    require(other_before['state'] == 'ready', 'other Environment not ready before cycle')
    other_before_contexts = harness.inspect(other_before)
    for session in sessions:
        require(session.owner['environment_id'] == environment_id and session.before is not None, 'foreign or unprepared Session')
    daemons_before = {mid: observe_daemon(harness, by_machine[mid], project, 'recovery-daemon-before-' + mid) for mid in machine_ids}
    inventories_before = {context['owner']['machine_id']: inventory(harness, context, 'recovery-inventory-before-' + context['owner']['machine_id'])
                          for context in before_contexts + other_before_contexts}
    for session in sessions:
        owned = inventories_before[session.owner['machine_id']]
        require(session.ids['always'] in owned['containers'] and session.ids['stopped'] in owned['containers'] and
                session.names['volume'] in owned['volumes'] and session.image_id in owned['images'], 'inventory lacks owned objects')
    daemon_identity = harness.daemon_fingerprint()
    require(daemon_identity == harness.daemon_identity, 'installed vz daemon identity changed before cycle')
    # Monitor handoff: the harness monitor cannot observe a stopped Environment.
    monitors = getattr(harness, 'recovery_monitors', None)
    if monitors is None:
        monitors = harness.recovery_monitors = []
    harness.monitor.check()
    harness.monitor.stop()
    monitors.append(harness.monitor)
    phase = phase_monitor(harness, other_rows, output / 'other-environment-liveness')
    phase.start()
    stop_started = time.time_ns()
    stopped = harness.stop(project, environment_id)
    stop_completed = time.time_ns()
    require([m['machine_id'] for m in stopped['machines']] == machine_ids, 'Stop changed Machine inventory')
    phase.check()
    up_started = time.time_ns()
    restarted = harness.up(project, environment_id)
    up_completed = time.time_ns()
    phase.check()
    require(restarted['environment_id'] == environment_id and restarted['project_id'] == before_status['project_id'] and
            restarted['name'] == before_status['name'] and restarted['state'] == 'ready' and
            [m['machine_id'] for m in restarted['machines']] == machine_ids and
            restarted['definition_digest'] == before_status['definition_digest'] and
            restarted['lifecycle_generation'] > before_status['lifecycle_generation'],
            'Up changed Environment/Machine identity')
    live_contexts = harness.inspect(restarted)
    live_by_machine = {context['owner']['machine_id']: context for context in live_contexts}
    require(set(live_by_machine) == set(by_machine), 'Up changed Machine set')
    for mid in machine_ids:
        stable_context(by_machine[mid], live_by_machine[mid])
    require(harness.daemon_fingerprint() == daemon_identity, 'installed vz daemon replaced across Stop/Up: fallback daemon')
    sessions_by_machine = {session.owner['machine_id']: session for session in sessions}
    polls = {mid: readiness_poll(harness, live_by_machine[mid], 'recovery-ready-' + mid, up_completed,
                                 sessions_by_machine.get(mid)) for mid in machine_ids}
    daemons_after = {mid: observe_daemon(harness, live_by_machine[mid], project, 'recovery-daemon-after-' + mid) for mid in machine_ids}
    for mid in machine_ids:
        require(daemons_after[mid]['boot_id'] != daemons_before[mid]['boot_id'], 'guest did not reboot: daemon not restarted')
        require(daemons_after[mid]['argv'] == daemons_before[mid]['argv'] == DOCKERD_ARGV, 'dockerd argv changed')
    for session in sessions:
        session.verify_after(live_by_machine[session.owner['machine_id']])
    sentinels = [restart_sentinel(harness, row, live_by_machine[row['descriptor']['owner']['machine_id']]) for row in rows]
    phase.check()
    inventories_after = {context['owner']['machine_id']: inventory(harness, context, 'recovery-inventory-after-' + context['owner']['machine_id'])
                         for context in live_contexts + other_before_contexts}
    same(inventories_after, inventories_before, 'object inventories changed across Stop/Up')
    other_after = harness.status(project, other_environment)
    require(harness.inspect(other_after) == other_before_contexts and
            other_after['lifecycle_generation'] == other_before['lifecycle_generation'] and other_after['state'] == 'ready',
            'other Environment changed during the cycle')
    phase.check()
    phase.stop()
    monitors.append(phase)
    all_rows = [row for row in harness.owned if row.get('kind') == 'sentinel']
    resumed = phase_monitor(harness, all_rows, output / 'resumed-liveness')
    resumed.start()
    harness.monitor = resumed
    record = {'environment_id': environment_id, 'other_environment_id': other_environment, 'machine_ids': machine_ids,
              'before_status': before_status, 'stopped_status': stopped, 'restarted_status': restarted,
              'contexts_before': before_contexts, 'contexts_after': live_contexts,
              'timing': {'stop_started_unix_ns': stop_started, 'stop_completed_unix_ns': stop_completed,
                         'up_started_unix_ns': up_started, 'up_completed_unix_ns': up_completed,
                         'stop_up_wall_seconds': (up_completed - stop_started) / 10 ** 9,
                         'public_up_wall_seconds': (up_completed - up_started) / 10 ** 9},
              'daemons_before': daemons_before, 'daemons_after': daemons_after, 'readiness_polls': polls,
              'inventories_before': inventories_before, 'inventories_after': inventories_after,
              'sentinel_restarts': sentinels, 'installed_daemon': daemon_identity,
              'other_environment_liveness': phase.summary(), 'sessions_verified': [s.owner['machine_id'] for s in sessions],
              'recovery_path': dict(RECOVERY_PATH), 'supervision_truth': SUPERVISION_TRUTH,
              'document': str(output / ('cycle-' + environment_id + '.json'))}
    startup.document(output / ('cycle-' + environment_id + '.json'), record)
    return record


def run_machine(harness, descriptor, scope, proof, images, index):
    """Persistence workload, Environment Stop/Up recovery, verification, exact cleanup.

    The caller must already authenticate descriptor/scope/proof through normal
    Up and retain the sentinel monitor. Images must be the admission-only pins
    (the sentinel BusyBox rootfs is the only workload image). No exception path
    removes an object; a failed Session stays registered on
    `harness.recovery_sessions`. See the module docstring for why index 0 owns
    two Sessions and why the harness monitor is replaced across a cycle.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index <= MAX_INDEX, 'bounded recovery Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    same(descriptor['owner'], {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
         'recovery Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'recovery Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    base = {key: harness.info['python_image'][key] for key in ('reference', 'id', 'platform')}
    same(images, {'base': base, 'compose': base}, 'recovery suite accepts only admission-only image pins')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    fixture = fixture_contract()
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents recovery dispatch')
    sessions = getattr(harness, 'recovery_sessions', None)
    if sessions is None:
        sessions = harness.recovery_sessions = []
    cycles = getattr(harness, 'recovery_cycles', None)
    if cycles is None:
        cycles = harness.recovery_cycles = {}
    require(all(item.cleanup_complete is True and item.failed is False for item in sessions[:index]),
            'earlier recovery Session lacks completed cleanup')
    require(len(sessions) in (index, index + 1), 'recovery Session registration out of order')
    output = harness.evidence / ('recovery-machine-' + str(index))
    require(not os.path.lexists(output), 'recovery Machine evidence directory preexists')
    owner = descriptor['owner']
    environment_id = owner['environment_id']
    project = project_path(harness, owner)
    harness.monitor.check()
    startup.private(output)
    started = time.time_ns()
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'started_unix_ns': started,
              'fixture_contract_sha256': fixture['contract_sha256'], 'persistence_sha256': SENTINEL_SHA256,
              'readiness_poll': dict(READINESS), 'recovery_path': dict(RECOVERY_PATH),
              'supervision_truth': SUPERVISION_TRUTH, 'environment_cycled_before': environment_id in cycles,
              'input_images_scope': 'admission_only_unused_sentinel_rootfs_is_the_workload_image'}
    startup.document(output / 'recovery-machine.intent.json', intent)
    created = []
    try:
        if environment_id not in cycles:
            require(len(sessions) == index, 'Sessions already exist for an uncycled Environment')
            count = SESSIONS_PER_CYCLE.get(index)
            require(count is not None, 'Environment cycle may only begin at index 0 or 2')
            status = harness.status(project, environment_id)
            require(status['state'] == 'ready', 'owning Environment is not ready')
            contexts = harness.inspect(status)
            require(descriptor in contexts, 'descriptor is not a live Machine of its Environment')
            ordered = [descriptor] + [context for context in contexts if context != descriptor]
            require(len(ordered) == 2, 'exact two-Machine Environment required')
            for offset, context in enumerate(ordered[:count]):
                image_id = sentinel_row(harness, context)['image_id']
                token = 'vzrecover-' + uuid.uuid4().hex[:24]
                session = Session(harness, context, image_id, token, index + offset, fixture)
                sessions.append(session)
                created.append(session)
                session.prepare()
                startup.document(output / ('workload-before-' + context['owner']['machine_id'] + '.json'), session.record())
            cycles[environment_id] = cycle_environment(harness, project, environment_id, created, output)
            session = created[0]
        else:
            require(len(sessions) > index, 'no Session registered for an already cycled Environment')
            session = sessions[index]
            require(session.owner == owner and session.after is not None and session.cleanup_complete is False and
                    session.failed is False, 'registered Session does not belong to this Machine or is unverified')
            live = harness.inspect(harness.status(project, environment_id))
            live = next(context for context in live if context['owner'] == owner)
            require(live == session.live_descriptor, 'live Machine incarnation changed since verification')
            # Read-only re-verification against the same live incarnation.
            session.verify_after(live)
        harness.monitor.check()
        startup.document(output / 'workload-after.json', session.record())
        # Only a complete verification admits the exact owned cleanup.
        cleanup = session.cleanup()
        final = inventory(harness, session.live_descriptor, 'recovery-inventory-final')
        require(session.image_id in final['images'] and session.names['volume'] not in final['volumes'] and
                not any(cid in final['containers'] for cid in session.ids.values()), 'cleanup left owned objects or removed image')
    except BaseException:
        touched = created if created else sessions[index:index + 1]
        for item in touched:
            if not item.cleanup_complete:
                item.failed = True
        raise
    verify_sources(pins)
    cycle = cycles[environment_id]
    result = {'schema_version': 1, 'scope': SCOPE, 'machine_scope': copy.deepcopy(scope), 'index': index,
              'started_unix_ns': started, 'ended_unix_ns': time.time_ns(), 'source_pins': pins, 'token': session.token,
              'session': session.record(), 'cycle': {key: cycle[key] for key in (
                  'environment_id', 'other_environment_id', 'machine_ids', 'timing', 'daemons_before', 'daemons_after',
                  'readiness_polls', 'sentinel_restarts', 'sessions_verified', 'installed_daemon')},
              'cycle_document': cycle['document'],
              'cleanup': cleanup, 'persistence_sha256': SENTINEL_SHA256,
              'scenarios': {'docker.storage.persistence': 'dev_observed_via_public_stop_up_not_release_certified',
                            'docker.operation.daemon_restart_recovery':
                                'dev_observed_owned_dockerd_restarted_by_public_stop_up_not_in_place_daemon_restart_not_release_certified'},
              'recovery_path': dict(RECOVERY_PATH), 'supervision_truth': SUPERVISION_TRUTH,
              'readiness_poll': dict(READINESS), 'test_case_retries': 0,
              'docker_parity_certified': False, 'release_certified': False}
    startup.document(output / 'machine-recovery-validation.json', result)
    same(parse(driver.regular(output / 'machine-recovery-validation.json', LIMIT)), result, 'retained recovery result differs')
    return result
