"""Installed-Machine adapter for the DEV concurrent-clients recipe.

Covers `docker.operation.concurrent_clients` on one authenticated Developer
Linux Machine through its private `--config`/`--context` route. The manifest
`expected` block names five numbers and one predicate, and this module proves
each with its own recipe against the same Machine:

  ready_containers / ready_window_seconds
      Twenty containers of the harness sentinel's digest-pinned developer probe
      rootfs are dispatched from one host rendezvous by twenty independent
      client processes, each with its own command recorder. Readiness is the
      Engine's own `State.Health.Status`, driven by a healthcheck that greps the
      container's *own* marker out of the file its own entrypoint wrote, so a
      container that started but never reached its ready point is never counted.

  parallel_execs
      Eight `docker exec` clients, again one recorder each, enter eight distinct
      running containers. The probe rendezvouses *inside the guest* over one
      Machine-local bind mount before any of them reports: an exec only returns
      once all eight participants have arrived, so the eight guest processes are
      proven to have been live at the same instant. A host thread lifetime alone
      is never evidence that the guest work overlapped.

      Eight *distinct* containers, not eight execs into one. This was recorded
      here as "a second exec into a container already running one is refused
      outright by the OCI runtime", which measurement disproved: concurrent
      execs succeed about 96% of the time. The real fault was a name collision
      in youki's tenant notify socket -- one fastrand draw plus an existence
      test, over a generator seeded from Instant::now() and ThreadId with no OS
      entropy, so two execs starting in the same clock bucket drew the same name
      and the loser's later bind() got EADDRINUSE. Fixed in the pinned youki
      patch set (vz-notify-socket-v1). Distinct containers are kept because they
      make each participant's marker unambiguous, not because one container
      cannot take two execs.

  parallel_builds
      Delegated whole to `linux_docker_build_parallel`, which already proves
      four concurrent BuildKit workers by an in-RUN barrier inside the guest and
      replays each slot's OCI export independently. Nothing is reimplemented and
      `docker.build.parallel` stays the `parallel` suite's own claim.

  parallel_pulls
      A pull needs a registry, and this suite refuses to fake one. It runs the
      same offline-admitted, digest-pinned Distribution v3.1.1 image the
      `registry` suite admits, but as a plain-HTTP server published on the
      Machine's loopback only. That is not a weakened posture: Moby's own
      default insecure range is `127.0.0.0/8` (`secure_registry_config` in
      `linux_docker_e2e` already pins the Engine to exactly `{"::1/128",
      "127.0.0.0/8"}`), so no daemon reconfiguration, CA, or credential state is
      involved, and the registry authentication claims stay where they are
      proven, in the `registry` suite. Four repositories with four distinct
      2 MiB layers are imported and pushed, removed locally, then pulled by four
      independent clients from one rendezvous. Concurrency is decided from the
      registry's *own* access log: each repository's service window is derived
      from the record timestamp minus the record's handling duration, and the
      four windows must mutually overlap. Client process lifetime is retained
      too, but it is not what decides the claim.

  all_results_exact_owner_correlated
      `correlate` requires every result of every recipe to carry this run's
      token, to name its own slot exactly once, and never to carry another
      slot's payload: a bijection between what was requested and what came back.

No exception path removes a container, image, or registry: a failed Session
stays registered on `harness.concurrency_sessions` with `cleanup_complete`
False. Nothing here certifies a release scenario.
"""
import base64
import copy
from concurrent.futures import ThreadPoolExecutor
import decimal
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
import linux_docker_buildkit_cgroup as binding
import linux_docker_registry_machine as registry

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_Machine_concurrent_clients_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
FIXTURE = REPO / 'tests/fixtures/vz-0.4/docker-concurrency'
LABEL = 'dev.vz.concurrency-proof'
BUSYBOX = '/bin/busybox'
PROBE_SHA256 = '4da8a15ffc67d092644ef7565478de89417fcccce9ddcde6e8b747a031c1e7fb'
PROBE_BYTES = 1026
# Enough containers to show whether they failed for one reason or many.
UNREADY_LOG_LIMIT = 3
# Observed on hardware: `formatter: json` selects the application log's formatter,
# not the access log's. Distribution v3 writes the access log to stdout in Apache
# combined format and the JSON application log -- the only stream carrying
# http.response.duration -- to stderr.
ACCESSLOG_STREAM = ('stdout (Apache combined); JSON application log with '
                    'http.response.duration on stderr')

# The manifest `expected` block of docker.operation.concurrent_clients, repeated
# here and again in the fixture contract so a silent edit to either fails.
READY_CONTAINERS = 20
READY_WINDOW_SECONDS = 60
PARALLEL_EXECS = 8
PARALLEL_PULLS = 4
PARALLEL_BUILDS = 4

READY_PATH = '/run/vz-concurrency-ready'
RENDEZVOUS_PATH = '/vz-rendezvous'
READY_TEMPLATE = 'VZREADY {marker}'
EXEC_TEMPLATE = 'VZEXEC {marker} {slot} {arrived} {waited}'
HEALTH_TEMPLATE = BUSYBOX + " grep -qx 'VZREADY {marker}' " + READY_PATH
CASES = ('ready', 'report')
# The container's own idle life. Longer than the whole recipe, so a container
# that is gone at cleanup exited on its own and is a failure, not a race.
IDLE_SECONDS = 3600
# Bounded guest-side wait for the eight-way exec rendezvous, in whole seconds
# because BusyBox `sleep` fractional support is not a pin this suite may assume.
RENDEZVOUS_POLL_SECONDS = 30
EXEC_TIMEOUT = 90
RUN_TIMEOUT = 120
PULL_TIMEOUT = 180
READY_POLL_INTERVAL_SECONDS = 1.0

# Registry recipe. The listener is inside the container's own network namespace;
# what the Machine exposes is one Engine-assigned ephemeral loopback port.
REGISTRY_PORT = 5000
LOOPBACK = '127.0.0.1'
REGISTRY_TAG = 'docker.io/library/registry:3.1.1'
REGISTRY_LOG_VERSION = '3.1.1'
REGISTRY_GUEST_CONFIG = '/vz-registry/config.yml'
REGISTRY_STORAGE = '/var/lib/registry'
LAYER_BYTES = 2 * 1024 * 1024
PULL_TAG = 'v1'
MAX_LOG_BYTES = 1024 * 1024
MAX_LOG_LINE = 16384

# Distribution v3.1.1 registry/registry.go: logrus JSONFormatter (RFC3339Nano)
# over a context carrying instance.id/version/go.version. This configuration
# sets no log.fields and no auth, so no record may carry an auth user name.
STARTUP_KEYS = frozenset({'time', 'level', 'msg', 'go.version', 'instance.id', 'version'})
RECORD_KEYS = STARTUP_KEYS | {
    'http.request.id', 'http.request.method', 'http.request.host', 'http.request.uri',
    'http.request.referer', 'http.request.useragent', 'http.request.remoteaddr',
    'http.request.contenttype', 'http.response.written', 'http.response.status',
    'http.response.contenttype', 'http.response.duration',
    'err.code', 'err.message', 'err.detail',
    'vars.name', 'vars.reference', 'vars.digest', 'vars.uuid'}
UUID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
TIMESTAMP = re.compile(r'(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.(\d{1,9}))?Z')
# Go time.Duration.String(): the sub-minute forms a request handler can produce.
DURATION = re.compile(r'(0|[1-9][0-9]{0,11})(?:\.([0-9]{1,9}))?(ns|us|µs|ms|s)')
DURATION_SCALE = {'ns': 1, 'us': 1000, 'µs': 1000, 'ms': 1000000, 's': 1000000000}
# `/v2/` is the Engine's API ping; everything else this recipe causes is a
# repository-scoped manifest or blob read.
REQUEST_URI = re.compile(r'/v2/(?:$|([a-z0-9][a-z0-9._/-]{0,190})/(manifests|blobs)/([A-Za-z0-9:._-]{1,150})$)')


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_concurrency_machine.py', 'linux_docker_build_parallel.py',
        'linux_docker_parallel_evidence.py', 'linux_docker_registry_machine.py',
        'docker_host_driver.py', 'linux_docker_buildkit_cgroup.py',
        'installed_developer_startup.py',
        'linux_docker_e2e.py')] + [str(FIXTURE / 'probe.sh'), str(FIXTURE / 'fixture.json')]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()),
            'exact concurrency source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest,
                'concurrency source changed: ' + name)


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
        raise ValueError('concurrency: malformed JSON output') from error


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def fixture_contract(root=FIXTURE):
    """Pinned probe bytes and a contract that repeats every module constant."""
    root = Path(root)
    script = driver.regular(root / 'probe.sh', LIMIT)
    require(len(script) == PROBE_BYTES and sha256(script) == PROBE_SHA256,
            'concurrency probe bytes differ from pin')
    contract = parse(driver.regular(root / 'fixture.json', LIMIT))
    require(type(contract) is dict and contract.get('schema_version') == 1, 'concurrency fixture contract shape')
    probe = contract['probe']
    require(probe['path'] == 'probe.sh' and probe['sha256'] == PROBE_SHA256 and probe['bytes'] == PROBE_BYTES and
            probe['interpreter'] == [BUSYBOX, 'sh', '-c'] and probe['cases'] == sorted(CASES) and
            probe['ready_path'] == READY_PATH and probe['rendezvous_path'] == RENDEZVOUS_PATH and
            probe['ready_template'] == READY_TEMPLATE and probe['exec_template'] == EXEC_TEMPLATE and
            probe['health_command_template'] == HEALTH_TEMPLATE, 'probe contract differs from pin')
    require(contract['concurrent_clients'] == {
        'parallel_execs': PARALLEL_EXECS, 'parallel_pulls': PARALLEL_PULLS,
        'parallel_builds': PARALLEL_BUILDS, 'ready_containers': READY_CONTAINERS,
        'ready_window_seconds': READY_WINDOW_SECONDS,
        'all_results_exact_owner_correlated': True}, 'concurrent-clients contract differs from pin')
    require(contract['pull_registry'] == {
        'accesslog_disabled': False, 'accesslog_stream': ACCESSLOG_STREAM,
        'container_port': REGISTRY_PORT,
        'insecure_by_default_cidr': '127.0.0.0/8', 'layer_bytes': LAYER_BYTES,
        'listen': '0.0.0.0:' + str(REGISTRY_PORT), 'published_host_ip': LOOPBACK,
        'repositories': PARALLEL_PULLS, 'scheme': 'http',
        'storage_rootdirectory': REGISTRY_STORAGE}, 'pull registry contract differs from pin')
    return {'script': script.decode('ascii'), 'contract': contract, 'script_sha256': PROBE_SHA256,
            'contract_sha256': sha256(driver.regular(root / 'fixture.json', LIMIT))}


def manifest_expectations(path=None):
    """The scenario's own `expected` block, read from the frozen manifest."""
    import linux_docker_scenarios as scenarios
    rows = scenarios.manifest() if path is None else scenarios.manifest(Path(path))
    expected = rows['docker.operation.concurrent_clients']['expected']
    require(expected == {'parallel_execs': PARALLEL_EXECS, 'parallel_pulls': PARALLEL_PULLS,
                         'parallel_builds': PARALLEL_BUILDS, 'ready_containers': READY_CONTAINERS,
                         'ready_window_seconds': READY_WINDOW_SECONDS,
                         'all_results_exact_owner_correlated': True},
            'manifest expected block differs from this suite\'s pins')
    return dict(expected)


# ---------------------------------------------------------------------------
# Pure observers. Every one of these decides an outcome from raw bytes; nothing
# below is allowed to report a conclusion the guest handed it.
# ---------------------------------------------------------------------------

def health_rows(raw, expected_ids):
    """Parse `{{.Id}} {{.State.Status}} {{health}}` for exactly the owned set."""
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT and raw.endswith(b'\n'),
            'unterminated container state table')
    rows = {}
    for line in raw.decode('ascii').splitlines():
        fields = line.split(' ')
        require(len(fields) == 3, 'malformed container state row: ' + repr(line))
        identity, state, health = fields
        require(re.fullmatch('[0-9a-f]{64}', identity), 'container state row identity')
        require(identity not in rows, 'duplicate container in the state table')
        require(state in ('created', 'running', 'paused', 'restarting', 'removing', 'exited', 'dead'),
                'unknown container state: ' + repr(state))
        require(health in ('none', 'starting', 'healthy', 'unhealthy'), 'unknown health state: ' + repr(health))
        rows[identity] = {'state': state, 'health': health}
    require(sorted(rows) == sorted(expected_ids), 'the state table is not exactly the owned containers')
    return rows


def all_ready(rows):
    """True only when every owned container is running AND Engine-healthy."""
    return all(row['state'] == 'running' and row['health'] == 'healthy' for row in rows.values())


def verify_ready(rows, expected_ids, *, dispatched_unix_ns, ready_unix_ns):
    """Twenty simultaneously dispatched containers, all healthy inside the window."""
    require(len(expected_ids) == READY_CONTAINERS and len(set(expected_ids)) == READY_CONTAINERS,
            'exactly twenty distinct owned containers required')
    require(sorted(rows) == sorted(expected_ids), 'ready observation is not the owned container set')
    unready = sorted(identity for identity, row in rows.items()
                     if row['state'] != 'running' or row['health'] != 'healthy')
    require(not unready, 'containers never became Engine-healthy: ' + ', '.join(unready))
    require(type(dispatched_unix_ns) is int and type(ready_unix_ns) is int and
            0 < dispatched_unix_ns < ready_unix_ns, 'ready window clock')
    window = ready_unix_ns - dispatched_unix_ns
    require(window <= READY_WINDOW_SECONDS * 10 ** 9,
            'twenty containers took ' + str(window) + ' ns, beyond the ' +
            str(READY_WINDOW_SECONDS) + ' s window')
    return {'ready_containers': READY_CONTAINERS, 'ready_window_seconds': READY_WINDOW_SECONDS,
            'observed_window_ns': window, 'dispatched_unix_ns': dispatched_unix_ns,
            'ready_unix_ns': ready_unix_ns, 'readiness_source': 'engine_state_health_status',
            'containers': sorted(expected_ids)}


def parse_exec_report(raw, stderr, marker, slot):
    """One exec's own two lines: its container's marker, then its slot record.

    The probe reports; it never decides. A report that names another container's
    marker, another slot, or fewer than the full participant set is refused here.
    """
    require(stderr == b'', 'concurrency exec emitted diagnostics: ' + repr(stderr[:200]))
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT and raw.endswith(b'\n'), 'unterminated exec report')
    lines = raw.decode('ascii').splitlines()
    require(len(lines) == 2, 'exec report must be exactly the ready line and the exec line')
    require(lines[0] == READY_TEMPLATE.format(marker=marker),
            'exec read another container\'s ready marker: ' + repr(lines[0]))
    fields = lines[1].split(' ')
    require(len(fields) == 5 and fields[0] == 'VZEXEC', 'malformed exec line: ' + repr(lines[1]))
    require(fields[1] == marker, 'exec line names another container: ' + repr(fields[1]))
    require(fields[2] == str(slot), 'exec line names another slot: ' + repr(fields[2]))
    require(re.fullmatch('[0-9]{1,3}', fields[3]) and re.fullmatch('[0-9]{1,3}', fields[4]),
            'exec line counters')
    arrived, waited = int(fields[3]), int(fields[4])
    require(arrived == PARALLEL_EXECS,
            'the guest rendezvous saw ' + str(arrived) + ' of ' + str(PARALLEL_EXECS) + ' participants')
    require(waited <= RENDEZVOUS_POLL_SECONDS, 'exec waited past its own bound')
    return {'marker': marker, 'slot': slot, 'arrived': arrived, 'waited_seconds': waited}


def mutual_overlap(intervals, reason):
    """One instant every interval contains; anything less is not concurrency."""
    require(type(intervals) is list and len(intervals) >= 2, 'two or more intervals required')
    for item in intervals:
        require(type(item) in (list, tuple) and len(item) == 2 and
                all(type(value) is int for value in item) and item[0] < item[1],
                'malformed interval: ' + repr(item))
    latest_start = max(item[0] for item in intervals)
    earliest_end = min(item[1] for item in intervals)
    require(latest_start < earliest_end, reason + ' (latest start ' + str(latest_start) +
            ' is not before earliest end ' + str(earliest_end) + ')')
    return {'intervals': [list(item) for item in intervals], 'participants': len(intervals),
            'overlap_started_unix_ns': latest_start, 'overlap_ended_unix_ns': earliest_end,
            'overlap_ns': earliest_end - latest_start}


def timestamp_ns(value):
    """RFC3339Nano in UTC, as Distribution's logrus JSON formatter writes it."""
    require(type(value) is str, 'registry timestamp type')
    match = TIMESTAMP.fullmatch(value)
    require(match is not None, 'registry timestamp syntax: ' + repr(value))
    import calendar
    import datetime
    try:
        moment = datetime.datetime(int(match[1]), int(match[2]), int(match[3]),
                                   int(match[4]), int(match[5]), int(match[6]))
    except ValueError:
        raise ValueError('registry timestamp value: ' + repr(value)) from None
    return calendar.timegm(moment.timetuple()) * 10 ** 9 + int((match[7] or '').ljust(9, '0'))


def duration_ns(value):
    """Go time.Duration.String() for a sub-minute HTTP handler, in nanoseconds."""
    require(type(value) is str, 'registry duration type')
    match = DURATION.fullmatch(value)
    require(match is not None, 'registry duration syntax: ' + repr(value))
    scale = DURATION_SCALE[match[3]]
    whole = decimal.Decimal(match[1] + '.' + (match[2] or '0'))
    total = int((whole * scale).to_integral_value(rounding=decimal.ROUND_FLOOR))
    require(0 <= total <= 120 * 10 ** 9, 'registry duration out of bounds: ' + repr(value))
    return total


def registry_records(raw, *, instance_id):
    """Every complete line is a Distribution record of this owned instance.

    Nothing is filtered: an unknown line fails the run rather than being skipped,
    because a filtered log cannot bound what the registry actually served.
    """
    require(type(raw) is bytes and len(raw) <= MAX_LOG_BYTES and (not raw or raw.endswith(b'\n')),
            'registry log bounds')
    require(type(instance_id) is str and re.fullmatch(UUID_PATTERN, instance_id), 'registry instance id')
    rows = []
    for line in raw.split(b'\n')[:-1]:
        require(0 < len(line) <= MAX_LOG_LINE, 'registry log line bounds')
        require(line.startswith(b'{'), 'registry log line is not a Distribution record: ' + repr(line[:120]))
        row = parse(line + b'\n')
        require(type(row) is dict and STARTUP_KEYS <= set(row) <= RECORD_KEYS, 'registry log record fields')
        require(all(type(row[key]) is str for key in STARTUP_KEYS), 'registry log record scalars')
        require(row['instance.id'] == instance_id and row['version'] == REGISTRY_LOG_VERSION and
                row['go.version'] == registry.GO_VERSION, 'registry log record identity')
        require(row['level'] in ('info', 'warning', 'error'), 'registry log record level')
        timestamp_ns(row['time'])
        rows.append(row)
    return rows


def registry_startup(raw):
    """The startup prefix: one instance, and a listener actually announced."""
    rows = [parse(line + b'\n') for line in raw.split(b'\n')[:-1] if line.startswith(b'{')]
    require(rows and len(rows) == len(raw.split(b'\n')[:-1]), 'registry startup log is not all JSON')
    identities = {row.get('instance.id') for row in rows}
    require(len(identities) == 1, 'registry startup log names more than one instance')
    instance_id = identities.pop()
    require(type(instance_id) is str and re.fullmatch(UUID_PATTERN, instance_id), 'registry instance id')
    listening = [row for row in rows if type(row.get('msg')) is str and row['msg'].startswith('listening on ')]
    require(len(listening) == 1, 'registry did not announce exactly one listener')
    registry_records(raw, instance_id=instance_id)
    return {'instance_id': instance_id, 'listener': listening[0]['msg'],
            'startup_records': len(rows), 'startup_sha256': sha256(raw)}


def served_requests(rows, repositories):
    """Repository-scoped GETs only, each an interval on the registry's own clock.

    The record timestamp is when Distribution wrote the completed response, and
    `http.response.duration` is how long it spent handling it, so [t-d, t] is the
    server's own service interval. No client-side clock is involved.
    """
    require(type(repositories) is tuple and len(repositories) == PARALLEL_PULLS and
            len(set(repositories)) == PARALLEL_PULLS, 'four distinct owned repositories required')
    served, pings = [], 0
    for row in rows:
        if 'http.request.uri' not in row:
            continue
        require(row['level'] == 'info', 'the registry served a request with an error record: ' +
                repr(row.get('err.code')))
        # containerd's resolver probes a manifest with HEAD before it reads it,
        # so both verbs are ordinary reads. Anything that writes is not a pull.
        require(row.get('http.request.method') in ('GET', 'HEAD'),
                'unexpected method during the pull phase: ' + repr(row.get('http.request.method')))
        require(row.get('http.response.status') == 200, 'unexpected response status during the pull phase: ' +
                repr(row.get('http.response.status')))
        require('http.response.duration' in row, 'registry served a request without its own handling duration')
        match = REQUEST_URI.fullmatch(row['http.request.uri'])
        require(match is not None, 'unknown registry request URI: ' + repr(row['http.request.uri']))
        completed = timestamp_ns(row['time'])
        elapsed = duration_ns(row['http.response.duration'])
        if match[1] is None:
            pings += 1
            continue
        repository = match[1]
        require(repository in repositories, 'the registry served an unowned repository: ' + repr(repository))
        if 'vars.name' in row:
            require(row['vars.name'] == repository, 'registry record repository disagrees with its own URI')
        served.append({'repository': repository, 'kind': match[2], 'reference': match[3],
                       'started_unix_ns': completed - elapsed, 'completed_unix_ns': completed,
                       'duration_ns': elapsed})
    return {'served': served, 'pings': pings}


def pull_windows(served, repositories):
    """Each repository's whole service window, and the proof they overlapped."""
    require(type(served) is list and served, 'the registry served no repository request')
    windows = {}
    for row in served:
        window = windows.setdefault(row['repository'], [row['started_unix_ns'], row['completed_unix_ns']])
        window[0] = min(window[0], row['started_unix_ns'])
        window[1] = max(window[1], row['completed_unix_ns'])
    missing = sorted(set(repositories) - set(windows))
    require(not missing, 'the registry never served: ' + ', '.join(missing))
    for repository in repositories:
        manifests = [row for row in served if row['repository'] == repository and row['kind'] == 'manifests']
        require(manifests, 'no manifest was served for ' + repository)
    # A degenerate window cannot witness an overlap with anything, so a
    # repository whose every record reported a zero duration is refused rather
    # than silently widened.
    for repository, window in windows.items():
        require(window[0] < window[1], 'the service window of ' + repository + ' has no duration')
    ordered = [windows[repository] for repository in repositories]
    proof = mutual_overlap(ordered, 'the registry did not serve the four repositories concurrently')
    return {'windows': {repository: list(windows[repository]) for repository in repositories},
            'overlap': proof, 'served_requests': len(served)}


def payload_bytes(marker, size):
    """Deterministic incompressible-enough layer bytes, derived from the marker."""
    require(type(marker) is str and marker and type(size) is int and 0 < size <= 16 * 1024 * 1024,
            'bounded layer payload required')
    seed = ('vz-concurrency-layer|' + marker).encode('ascii')
    out = bytearray()
    counter = 0
    while len(out) < size:
        out.extend(hashlib.sha256(seed + counter.to_bytes(8, 'big')).digest())
        counter += 1
    return bytes(out[:size])


def layer_tar(marker, size=LAYER_BYTES):
    """One-file rootfs tar; distinct per marker, so no two slots share a layer."""
    payload = payload_bytes(marker, size)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w', format=tarfile.GNU_FORMAT) as archive:
        item = tarfile.TarInfo('vz-concurrency-' + marker)
        item.size, item.mode, item.mtime, item.uid, item.gid = len(payload), 0o644, 0, 0, 0
        item.uname = item.gname = ''
        archive.addfile(item, io.BytesIO(payload))
    return buffer.getvalue()


def verify_pull_results(rows):
    """Each pull returned exactly its own slot's image, and no other slot's."""
    require(type(rows) is list and len(rows) == PARALLEL_PULLS, 'four pull results required')
    require([row['slot'] for row in rows] == list(range(PARALLEL_PULLS)), 'pull slot order differs')
    for row in rows:
        require(re.fullmatch('sha256:[0-9a-f]{64}', row['pushed_image_id']) and
                row['pulled_image_id'] == row['pushed_image_id'],
                'slot ' + str(row['slot']) + ' pulled another image than it pushed')
        require(re.fullmatch('sha256:[0-9a-f]{64}', row['repo_digest']), 'pull repository digest shape')
        require(row['pulled_repo_digest'] == row['repo_digest'],
                'slot ' + str(row['slot']) + ' pulled another manifest than it pushed')
        require(row['reference'].endswith('/' + row['repository'] + ':' + PULL_TAG),
                'pull reference does not name its own repository')
    for field in ('pushed_image_id', 'repo_digest', 'repository', 'reference'):
        values = [row[field] for row in rows]
        require(len(set(values)) == PARALLEL_PULLS, 'two pull slots share their ' + field)
    return {'parallel_pulls': PARALLEL_PULLS,
            'slots': [{'slot': row['slot'], 'repository': row['repository'],
                       'image_id': row['pulled_image_id'], 'repo_digest': row['repo_digest']} for row in rows]}


def correlate(result):
    """`all_results_exact_owner_correlated`: a bijection, owned end to end.

    Every result of every recipe must carry this run's token, name its own slot
    exactly once, and never carry another slot's payload. This is what refuses a
    run whose concurrent clients answered each other's work.
    """
    token = result['token']
    require(type(token) is str and re.fullmatch('vzconc-[0-9a-f]{24}', token), 'owned run token shape')
    ready = result['ready_containers']
    markers = result['container_markers']
    require(type(markers) is dict and len(markers) == READY_CONTAINERS, 'twenty owned container markers required')
    require(sorted(markers) == sorted(ready['containers']), 'markers do not name the ready containers')
    require(len(set(markers.values())) == READY_CONTAINERS, 'two containers share a marker')
    require(all(value.startswith(token + '-c') for value in markers.values()),
            'a container marker is not owned by this run')

    execs = result['parallel_execs']['slots']
    require(type(execs) is list and len(execs) == PARALLEL_EXECS, 'eight exec results required')
    require(sorted(row['slot'] for row in execs) == list(range(PARALLEL_EXECS)),
            'exec slots are missing or duplicated')
    require(len({row['container_id'] for row in execs}) == PARALLEL_EXECS,
            'two execs entered the same container')
    for row in execs:
        require(row['container_id'] in markers, 'an exec entered a container this run does not own')
        require(row['report']['marker'] == markers[row['container_id']],
                'exec slot ' + str(row['slot']) + ' reported another container\'s marker')
        require(row['report']['slot'] == row['slot'], 'exec slot reported another slot')
        foreign = sorted(other['report']['marker'] for other in execs
                         if other['slot'] != row['slot'] and other['report']['marker'] == row['report']['marker'])
        require(not foreign, 'two execs reported the same marker')

    builds = result['parallel_builds']['slots']
    require(type(builds) is list and len(builds) == PARALLEL_BUILDS, 'four build results required')
    require(sorted(row['slot'] for row in builds) == list(range(PARALLEL_BUILDS)),
            'build slots are missing or duplicated')
    require(len({row['payload_sha256'] for row in builds}) == PARALLEL_BUILDS,
            'two builds exported the same payload')
    require(len({row['run_digest'] for row in builds}) == PARALLEL_BUILDS,
            'two builds reused one guest RUN identity')

    pulls = result['parallel_pulls']['slots']
    require(type(pulls) is list and len(pulls) == PARALLEL_PULLS, 'four pull results required')
    require(sorted(row['slot'] for row in pulls) == list(range(PARALLEL_PULLS)),
            'pull slots are missing or duplicated')
    require(len({row['image_id'] for row in pulls}) == PARALLEL_PULLS, 'two pulls returned one image')
    require(all(row['repository'].startswith(token + '/slot') for row in pulls),
            'a pulled repository is not owned by this run')
    return {'all_results_exact_owner_correlated': True,
            'correlated_results': READY_CONTAINERS + PARALLEL_EXECS + PARALLEL_BUILDS + PARALLEL_PULLS,
            'ready_containers': READY_CONTAINERS, 'parallel_execs': PARALLEL_EXECS,
            'parallel_builds': PARALLEL_BUILDS, 'parallel_pulls': PARALLEL_PULLS,
            'owner_token': token}


def sentinel_image(harness, descriptor):
    rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and row.get('descriptor') == descriptor]
    require(len(rows) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', rows[0].get('image_id') or ''),
            'exact owned sentinel image required')
    return rows[0]['image_id']


def run_parallel(count, action, *, rendezvous_timeout=60):
    """Dispatch `count` independent clients from one host rendezvous.

    Every dispatched worker is joined, including when another fails, and every
    slot's own cause is named: these workers rendezvous, so when one never
    arrives the rest fail waiting for it and a prefix would name symptoms only.
    No failure retries, restarts a client, or admits cleanup.
    """
    require(type(count) is int and 2 <= count <= 32, 'bounded parallel client count required')
    barrier = threading.Barrier(count, timeout=rendezvous_timeout)
    results, failures = [None] * count, []

    def worker(index):
        barrier.wait()
        return action(index)

    with ThreadPoolExecutor(max_workers=count, thread_name_prefix='vz-concurrency') as executor:
        futures = [executor.submit(worker, index) for index in range(count)]
        for index, future in enumerate(futures):
            try:
                results[index] = future.result()
            except BaseException as error:  # noqa: BLE001 - every cause is reported, none is swallowed
                failures.append((index, error))
    if failures:
        detail = '; '.join('slot ' + str(index) + ': ' + type(error).__name__ + ': ' + str(error)[:400]
                           for index, error in failures)
        raise RuntimeError('concurrent clients failed: ' +
                           ','.join(str(index) for index, _ in failures) + ' (' + detail + ')') from failures[0][1]
    return results


class Client:
    """One independent host client with its own recorder; never shared."""

    def __init__(self, session, kind, index):
        self.session, self.kind, self.index = session, kind, index
        self.output = startup.private(session.output / (kind + '-' + str(index)))
        self.record = startup.Recorder(self.output, dict(session.harness.env))

    def docker(self, label, args, *, timeout):
        descriptor = self.session.descriptor
        argv = ['docker', '--config', descriptor['config_dir'], '--context', descriptor['name'], *args]
        return self.record.run(label, argv, cwd=self.session.harness.root,
                               executable=self.session.harness.info['clients']['docker']['canonical'],
                               timeout=timeout)

    def interval(self):
        """This client's own recorded command bracket, from its own receipt."""
        require(len(self.record.receipts) == 1, 'exactly one recorded command per parallel client')
        receipt = self.record.receipts[0]
        require(receipt['capture_complete'] is True and receipt['effects_uncertain'] is False and
                receipt['exit_code'] == 0, 'parallel client command did not complete exactly')
        started = receipt['started_unix_ns']
        require(type(started) is int and type(receipt['elapsed_ns']) is int and receipt['elapsed_ns'] > 0,
                'parallel client clock')
        return [started, started + receipt['elapsed_ns']]


class Session:
    """Owned containers, images and one owned registry; never self-cleans."""

    def __init__(self, harness, descriptor, image_id, token, script, output):
        self.harness, self.descriptor, self.image_id, self.token = harness, descriptor, image_id, token
        self.script = script
        self.output = output
        self.root = '/run/vz-concurrency-' + token
        self.names = [token + '-c' + str(index).zfill(2) for index in range(READY_CONTAINERS)]
        self.markers = {name: name for name in self.names}
        self.ids = {}
        self.registry_name = token + '-registry'
        self.registry_id = None
        self.registry_loaded = False
        self.registry_port = None
        self.registry_instance_id = None
        self.pull_references = []
        self.cleanup_complete = False
        self.failed = False

    # ---- the Machine is the Docker host of these containers ----

    def exec_argv(self, script):
        owner = self.descriptor['owner']
        return [str(self.harness.cli), 'exec', '--environment', owner['environment_id'],
                '--machine', owner['machine_id'], '--no-stdin', '--timeout', str(EXEC_TIMEOUT),
                '--', BUSYBOX, 'sh', '-c', script]

    def public_exec(self, label, script):
        project = binding.project_binding(self.harness, self.descriptor)
        raw, stderr, code = self.harness.command('concurrency-' + label, self.exec_argv(script),
                                                 cwd=Path(project['project_path']),
                                                 timeout=EXEC_TIMEOUT + 10, success=False)
        require(type(code) is int and code == 0 and stderr == b'',
                'concurrency guest observation failed; evidence retained')
        require(binding.project_binding(self.harness, self.descriptor) == project,
                'Machine/project binding changed during the guest observation')
        return raw

    def seed_host(self, config):
        """The rendezvous directory and the registry configuration, on the Machine."""
        payload = base64.b64encode(config).decode('ascii')
        script = ('set -eu; ' + BUSYBOX + ' mkdir -p ' + self.root + '/rendezvous ' + self.root + '/registry; '
                  + BUSYBOX + ' chmod 0777 ' + self.root + '/rendezvous; '
                  "printf '%s' '" + payload + "' | " + BUSYBOX + ' base64 -d > ' + self.root + '/registry/config.yml; '
                  + BUSYBOX + ' sha256sum ' + self.root + '/registry/config.yml | ' + BUSYBOX + ' cut -d" " -f1')
        raw = self.public_exec('seed', script)
        digest = raw.decode('ascii').strip()
        require(digest == sha256(config), 'seeded registry configuration differs from the pinned bytes')
        return {'root': self.root, 'registry_config_sha256': digest, 'registry_config_bytes': len(config)}

    def rendezvous_inventory(self):
        raw = self.public_exec('rendezvous', BUSYBOX + ' ls ' + self.root + '/rendezvous')
        return sorted(raw.decode('ascii').split())

    def retire_host(self):
        self.public_exec('retire', 'set -eu; ' + BUSYBOX + ' rm -rf ' + self.root + '; '
                         '[ ! -e ' + self.root + ' ] && printf retired')

    def container_inventory(self, label):
        raw, stderr, _ = self.harness.docker('concurrency-container-ls-' + label, self.descriptor,
                                             ['container', 'ls', '--all', '--no-trunc', '--format', '{{.Names}}'])
        require(stderr == b'', 'container inventory diagnostics')
        names = raw.decode('ascii').split()
        require(len(names) <= 4096, 'unbounded container inventory')
        return sorted(names)

    def names_absent(self, label):
        inventory = set(self.container_inventory(label))
        owned = set(self.names)
        # The registry is launched before the twenty containers so their lifetime
        # stays short, so its own name is a collision only while this Session has
        # not created it -- otherwise the guard would refuse our own registry.
        if self.registry_id is None:
            owned.add(self.registry_name)
        collisions = sorted(inventory & owned)
        require(not collisions, 'owned container name already exists: ' + ', '.join(collisions))

    # ---- recipe 1: twenty ready containers ----

    def launch(self):
        """Twenty independent clients, one host rendezvous, one dispatch instant."""
        self.names_absent('before-launch')
        clients = [Client(self, 'ready', index) for index in range(READY_CONTAINERS)]

        def start(index):
            name = self.names[index]
            marker = self.markers[name]
            raw, stderr, _ = clients[index].docker('run', [
                'run', '--detach', '--network', 'none', '--restart', 'no',
                '--name', name, '--label', LABEL + '=' + self.token,
                '--mount', 'type=bind,src=' + self.root + '/rendezvous,dst=' + RENDEZVOUS_PATH,
                '--health-cmd', HEALTH_TEMPLATE.format(marker=marker),
                '--health-interval', '1s', '--health-timeout', '5s', '--health-retries', '3',
                self.image_id, BUSYBOX, 'sh', '-c', self.script, 'vzconc', 'ready', marker,
                str(IDLE_SECONDS)], timeout=RUN_TIMEOUT)
            require(stderr == b'', 'container ' + name + ' run diagnostics: ' + repr(stderr[:200]))
            return driver.checked_text(raw.decode('ascii').strip(), r'[0-9a-f]{64}', name + ' container ID')

        identities = run_parallel(READY_CONTAINERS, start)
        require(len(set(identities)) == READY_CONTAINERS, 'the Engine returned one container ID twice')
        self.ids = {self.names[index]: identities[index] for index in range(READY_CONTAINERS)}
        dispatched = min(client.interval()[0] for client in clients)
        return clients, dispatched

    def observe_ready(self, dispatched):
        """Poll the Engine's own health state until every owned container is healthy."""
        identities = [self.ids[name] for name in self.names]
        deadline = dispatched + READY_WINDOW_SECONDS * 10 ** 9
        rows = None
        while True:
            raw, stderr, _ = self.harness.docker('concurrency-ready-poll', self.descriptor,
                ['container', 'inspect', '--format',
                 '{{.Id}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}',
                 *identities])
            require(stderr == b'', 'ready poll diagnostics')
            observed = time.time_ns()
            rows = health_rows(raw, identities)
            if all_ready(rows):
                return rows, observed
            if observed >= deadline:
                # Status alone does not say why. Without the container's own
                # output a readiness failure means reaching into a retained
                # Engine by hand, so record it here while the Engine is live.
                unready = {identity: row for identity, row in sorted(rows.items()) if row['health'] != 'healthy'}
                require(False, 'twenty containers were not all healthy inside the window: ' +
                        canonical(unready) + ' logs: ' + canonical(self.unready_logs(unready)))
            time.sleep(READY_POLL_INTERVAL_SECONDS)

    def unready_logs(self, unready):
        """Last output of each container that never became healthy."""
        logs = {}
        for identity in list(unready)[:UNREADY_LOG_LIMIT]:
            try:
                raw, stderr, _ = self.harness.docker('concurrency-unready-logs', self.descriptor,
                                                     ['container', 'logs', '--tail', '5', identity])
                text = (raw + stderr).decode('utf-8', 'replace').strip()
            except Exception as error:  # diagnosis must never mask the failure it explains
                text = 'unavailable: ' + type(error).__name__ + ': ' + str(error)[:200]
            logs[identity] = text[-400:]
        return logs

    # ---- recipe 2: eight parallel execs ----

    def parallel_execs(self):
        """Eight clients into eight distinct containers, rendezvousing in the guest."""
        require(self.rendezvous_inventory() == [], 'the guest rendezvous directory is not empty')
        selected = [self.names[index] for index in range(PARALLEL_EXECS)]
        clients = [Client(self, 'exec', index) for index in range(PARALLEL_EXECS)]

        def enter(index):
            name = selected[index]
            marker = self.markers[name]
            raw, stderr, _ = clients[index].docker('exec', [
                'exec', self.ids[name], BUSYBOX, 'sh', '-c', self.script, 'vzconc', 'report',
                marker, str(index), str(PARALLEL_EXECS), str(RENDEZVOUS_POLL_SECONDS)],
                timeout=EXEC_TIMEOUT)
            return parse_exec_report(raw, stderr, marker, index)

        reports = run_parallel(PARALLEL_EXECS, enter, rendezvous_timeout=30)
        intervals = [client.interval() for client in clients]
        overlap = mutual_overlap(intervals, 'the eight exec clients did not overlap')
        arrivals = self.rendezvous_inventory()
        require(arrivals == sorted('slot-' + str(index) for index in range(PARALLEL_EXECS)),
                'the guest rendezvous holds ' + repr(arrivals) + ', not the eight owned slots')
        return {'parallel_execs': PARALLEL_EXECS,
                'guest_rendezvous': {'path': RENDEZVOUS_PATH, 'participants': PARALLEL_EXECS,
                                     'arrivals': arrivals,
                                     'basis': 'each exec returned only after all eight were live in the guest'},
                'client_overlap': overlap,
                'slots': [{'slot': index, 'container': selected[index], 'container_id': self.ids[selected[index]],
                           'report': reports[index]} for index in range(PARALLEL_EXECS)]}

    # ---- recipe 4: an owned private registry and four parallel pulls ----

    def registry_config(self):
        # JSON is a YAML subset. No auth, no TLS and no proxy: the server is
        # reachable only through an Engine-published loopback binding, which is
        # Moby's own default insecure range, so no daemon policy is relaxed.
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        return (canonical({'version': '0.1',
                           'log': {'level': 'info', 'formatter': 'json', 'accesslog': {'disabled': False}},
                           'storage': {'filesystem': {'rootdirectory': REGISTRY_STORAGE}},
                           'http': {'addr': '0.0.0.0:' + str(REGISTRY_PORT), 'secret': secret,
                                    'draintimeout': '5s'}}) + '\n').encode('ascii')

    def registry_logs(self):
        raw, stderr, _ = self.harness.docker('concurrency-registry-logs', self.descriptor,
                                             ['logs', self.registry_id])
        # Distribution v3 uses BOTH streams: the Apache-combined access log goes
        # to stdout, and the structured JSON application log -- the one carrying
        # http.response.duration, which is what the service windows are computed
        # from -- goes to stderr. `formatter: json` selects the application log's
        # formatter, not the access log's, so stdout is never JSON and requiring
        # it to be empty could never hold.
        require(len(raw) <= MAX_LOG_BYTES and len(stderr) <= MAX_LOG_BYTES, 'registry log volume')
        require(stderr.strip().startswith(b'{'), 'registry application log is not JSON on stderr')
        return stderr

    def start_registry(self):
        """Load the pinned Distribution image, serve it on one loopback binding."""
        record = registry.verify_registry_info(self.harness)
        self.harness.exact_absent(self.descriptor, 'image', REGISTRY_TAG)
        self.harness.mutate('concurrency-registry-load', self.descriptor,
                            ['image', 'load', '--platform', 'linux/arm64', '--input',
                             self.harness.info['registry_archive']], timeout=180)
        self.registry_loaded = True
        raw, stderr, _ = self.harness.mutate('concurrency-registry-create', self.descriptor,
            ['container', 'create', '--name', self.registry_name, '--restart', 'no', '--pull', 'never',
             '--label', LABEL + '=' + self.token,
             '--publish', LOOPBACK + '::' + str(REGISTRY_PORT),
             '--mount', 'type=bind,src=' + self.root + '/registry,dst=/vz-registry,readonly',
             '--entrypoint', '/bin/registry', record['manifest_digest'], 'serve', REGISTRY_GUEST_CONFIG])
        require(stderr == b'', 'registry create diagnostics')
        self.registry_id = driver.checked_text(raw.decode('ascii').strip(), r'[0-9a-f]{64}', 'registry container ID')
        self.harness.mutate('concurrency-registry-start', self.descriptor,
                            ['container', 'start', self.registry_id])
        deadline = time.monotonic() + 30
        while True:
            raw = self.registry_logs()
            if raw and any(b'listening on ' in line for line in raw.split(b'\n')):
                break
            require(time.monotonic() < deadline, 'registry listener readiness deadline')
            time.sleep(0.2)
        startup_proof = registry_startup(raw)
        self.registry_instance_id = startup_proof['instance_id']
        self.registry_port = self.published_port()
        return {'container_id': self.registry_id, 'manifest_digest': record['manifest_digest'],
                'version': record['version'], 'go_version': record['go_version'],
                'host_ip': LOOPBACK, 'host_port': self.registry_port, 'container_port': REGISTRY_PORT,
                'scheme': 'http', 'authority': LOOPBACK + ':' + str(self.registry_port),
                'insecure_basis': 'moby_default_insecure_registry_cidr_127.0.0.0/8',
                **startup_proof}

    def published_port(self):
        raw, stderr, _ = self.harness.docker('concurrency-registry-inspect', self.descriptor,
                                             ['container', 'inspect', self.registry_id])
        rows = parse(raw)
        require(stderr == b'' and type(rows) is list and len(rows) == 1, 'ambiguous registry container')
        item = rows[0]
        require(item['Id'] == self.registry_id and item['Name'] == '/' + self.registry_name and
                item['Config']['Labels'][LABEL] == self.token and item['HostConfig']['Runtime'] == 'youki' and
                item['State']['Running'] is True and item['RestartCount'] == 0,
                'registry container identity or state differs')
        bindings = item['NetworkSettings']['Ports'][str(REGISTRY_PORT) + '/tcp']
        require(type(bindings) is list and len(bindings) == 1, 'registry has no single published binding')
        row = bindings[0]
        require(row['HostIp'] == LOOPBACK, 'registry published to ' + repr(row['HostIp']) + ', not loopback')
        return int(driver.checked_text(row['HostPort'], r'[1-9][0-9]{2,4}', 'registry host port'))

    def image_inventory(self, label):
        raw, stderr, _ = self.harness.docker('concurrency-image-ls-' + label, self.descriptor,
                                             ['image', 'ls', '--all', '--quiet', '--no-trunc'])
        require(stderr == b'', 'image inventory diagnostics')
        ids = raw.decode('ascii').split()
        require(all(re.fullmatch(r'sha256:[0-9a-f]{64}', item) for item in ids), 'image inventory shape')
        return sorted(set(ids))

    def image_identity(self, label, reference):
        raw, stderr, _ = self.harness.docker('concurrency-image-inspect-' + label, self.descriptor,
                                             ['image', 'inspect', reference])
        rows = parse(raw)
        require(stderr == b'' and type(rows) is list and len(rows) == 1, 'ambiguous owned image: ' + reference)
        item = rows[0]
        # The reference carries a registry port, so the tag is the last colon.
        repository = reference.rsplit(':', 1)[0]
        digests = [value for value in (item.get('RepoDigests') or []) if value.startswith(repository + '@')]
        require(len(digests) <= 1, 'ambiguous repository digest for ' + reference)
        return {'id': driver.checked_text(item['Id'], r'sha256:[0-9a-f]{64}', 'image ID'),
                'repo_digest': digests[0].split('@', 1)[1] if digests else None}

    def publish_images(self):
        """Four distinct owned layers, imported, pushed, then locally removed."""
        authority = LOOPBACK + ':' + str(self.registry_port)
        rows = []
        for slot in range(PARALLEL_PULLS):
            repository = self.token + '/slot' + str(slot)
            reference = authority + '/' + repository + ':' + PULL_TAG
            marker = self.token + '-l' + str(slot)
            archive = self.harness.root / (self.token + '-slot' + str(slot) + '.tar')
            raw_tar = layer_tar(marker)
            archive.write_bytes(raw_tar)
            self.harness.exact_absent(self.descriptor, 'image', reference)
            with archive.open('rb') as stream:
                self.harness.mutate('concurrency-import-' + str(slot), self.descriptor,
                                    ['image', 'import', '--change', 'LABEL ' + LABEL + '=' + self.token,
                                     '-', reference], stdin=stream, timeout=RUN_TIMEOUT)
            imported = self.image_identity('imported-' + str(slot), reference)
            self.harness.mutate('concurrency-push-' + str(slot), self.descriptor,
                                ['push', reference], timeout=PULL_TIMEOUT)
            pushed = self.image_identity('pushed-' + str(slot), reference)
            require(pushed['id'] == imported['id'], 'the push changed the local image identity')
            require(pushed['repo_digest'] is not None, 'the push produced no repository digest')
            rows.append({'slot': slot, 'repository': repository, 'reference': reference, 'marker': marker,
                         'layer_bytes': len(raw_tar), 'layer_sha256': sha256(raw_tar),
                         'pushed_image_id': pushed['id'], 'repo_digest': pushed['repo_digest']})
            self.pull_references.append(reference)
        require(len({row['layer_sha256'] for row in rows}) == PARALLEL_PULLS, 'two slots share a layer')
        for row in rows:
            self.harness.mutate('concurrency-image-rm-' + str(row['slot']), self.descriptor,
                                ['image', 'rm', row['reference']])
            self.harness.exact_absent(self.descriptor, 'image', row['reference'])
        remaining = self.image_inventory('after-push')
        present = sorted({row['pushed_image_id'] for row in rows} & set(remaining))
        require(not present, 'a pushed image was not removed before the pull: ' + ', '.join(present))
        return rows

    def parallel_pulls(self, published):
        """Four independent clients, one rendezvous, and the registry's own log."""
        before = self.registry_logs()
        clients = [Client(self, 'pull', slot) for slot in range(PARALLEL_PULLS)]

        # Only the client's own recorder is touched inside a worker: the
        # harness recorder indexes its receipts by list length and is not
        # safe to share across these threads.
        def pull(slot):
            return clients[slot].docker('pull', ['pull', published[slot]['reference']], timeout=PULL_TIMEOUT)

        run_parallel(PARALLEL_PULLS, pull)
        intervals = [client.interval() for client in clients]
        client_overlap = mutual_overlap(intervals, 'the four pull clients did not overlap')
        after = self.registry_logs()
        require(after.startswith(before), 'the registry log prefix changed during the pull phase')
        delta = after[len(before):]
        pulled = [self.image_identity('pulled-' + str(slot), published[slot]['reference'])
                  for slot in range(PARALLEL_PULLS)]
        require(self.registry_instance_id is not None, 'registry instance identity unknown')
        rows = registry_records(delta, instance_id=self.registry_instance_id)
        repositories = tuple(row['repository'] for row in published)
        service = served_requests(rows, repositories)
        server = pull_windows(service['served'], repositories)
        results = [{'slot': slot, 'repository': published[slot]['repository'],
                    'reference': published[slot]['reference'],
                    'pushed_image_id': published[slot]['pushed_image_id'],
                    'repo_digest': published[slot]['repo_digest'],
                    'pulled_image_id': pulled[slot]['id'],
                    'pulled_repo_digest': pulled[slot]['repo_digest']} for slot in range(PARALLEL_PULLS)]
        proof = verify_pull_results(results)
        return dict(proof, client_overlap=client_overlap, server_overlap=server,
                    registry_pings=service['pings'], registry_delta_records=len(rows),
                    registry_delta_sha256=sha256(delta),
                    concurrency_basis='distribution_access_log_service_windows_not_client_lifetime',
                    full_registry_conformance_certified=False)

    # ---- exact owned removal; no exception path reaches here ----

    def remove(self):
        for name in list(self.ids):
            self.harness.mutate('concurrency-remove-' + name, self.descriptor,
                                ['container', 'rm', '--force', self.ids.pop(name)])
            self.harness.exact_absent(self.descriptor, 'container', name)
        for reference in list(self.pull_references):
            # These exist again: the pull recipe is what put them back.
            self.harness.mutate('concurrency-pulled-rm', self.descriptor, ['image', 'rm', reference])
            self.harness.exact_absent(self.descriptor, 'image', reference)
            self.pull_references.remove(reference)
        if self.registry_id is not None:
            self.harness.mutate('concurrency-registry-remove', self.descriptor,
                                ['container', 'rm', '--force', self.registry_id])
            self.harness.exact_absent(self.descriptor, 'container', self.registry_name)
            self.registry_id = None
        if self.registry_loaded:
            self.harness.mutate('concurrency-registry-image-rm', self.descriptor, ['image', 'rm', REGISTRY_TAG])
            self.harness.exact_absent(self.descriptor, 'image', REGISTRY_TAG)
            self.registry_loaded = False
        self.names_absent('after-remove')
        self.retire_host()
        self.cleanup_complete = True


def parallel_builds(harness, descriptor, scope, proof, images, index, root):
    """Four concurrent BuildKit workers, delegated whole to the parallel suite.

    Nothing about the build recipe is reimplemented here: the same barrier that
    proves four guest RUN workers overlapped, the same per-slot OCI replay and
    the same group validation. `docker.build.parallel` stays that suite's claim;
    this suite reads only the four slot results it needs for its own correlation.
    """
    from linux_docker_e2e import input_mapping
    from linux_docker_build_parallel import SLOTS, ParallelDriver, execute_slots, specification
    from linux_docker_parallel_evidence import validate_group, validate_slot
    require(len(SLOTS) == PARALLEL_BUILDS, 'the parallel suite no longer builds four slots')
    require(not os.path.lexists(root), 'concurrency build evidence directory preexists')
    root.mkdir(mode=0o700)
    builder = harness.prepare_builder(descriptor)
    inputs = input_mapping(harness, scope, proof, images) | {'builder': builder.mapping}
    admitted = driver.Inputs(inputs, suite='build')
    admitted.verify_runtime_evidence()
    selected, operations, positions = [], [], []
    for slot in SLOTS:
        operation = specification(slot, root / ('slot-' + str(slot)), Path(harness.info['parallel_fixture']),
                                  harness.info['parallel_fixture_sha256'], inputs['run_id'])
        item = ParallelDriver(admitted, Path(harness.info['fixture']), root / ('slot-' + str(slot)))
        positions.append(len(harness.drivers))
        harness.drivers.append(item)
        harness.driver_cleanup_verified.append(False)
        selected.append(item)
        operations.append(operation)
    started = time.time_ns()
    solved = execute_slots(selected, operations)
    replays = [validate_slot(item.output, inputs, operation) for item, (operation, _) in zip(selected, solved)]
    group = validate_group(replays)
    runtime = builder.verify(require_invocation=True)
    for position in positions:
        harness.driver_cleanup_verified[position] = True
    slots = []
    for replay, (operation, artifact) in zip(replays, solved):
        envelope = replay['guest_run_envelope']
        slots.append({'slot': replay['slot'], 'payload_sha256': operation['payload']['sha256'],
                      'run_digest': replay['run_interval']['digest'],
                      'guest_run_envelope': [envelope['started_ns'], envelope['completed_ns']]})
    require(sorted(row['slot'] for row in slots) == list(range(PARALLEL_BUILDS)), 'build slot inventory')
    overlap = mutual_overlap([row['guest_run_envelope'] for row in slots],
                             'the four guest build workers did not overlap')
    return {'parallel_builds': PARALLEL_BUILDS, 'started_unix_ns': started, 'ended_unix_ns': time.time_ns(),
            'builder': builder.ownership, 'builder_runtime': runtime, 'group': group, 'slots': slots,
            'guest_overlap': overlap,
            'concurrency_basis': 'in_run_guest_barrier_and_independent_slot_replay',
            'delegated_to': 'linux_docker_build_parallel'}


def run_machine(harness, descriptor, scope, proof, images, index):
    """Four concurrency recipes on one Machine, then exact owned cleanup.

    The caller must already authenticate descriptor/scope/proof through normal
    Up, retain the sentinel monitor, prepare the base/Compose images the build
    recipe needs and admit the pinned registry inputs. No exception path removes
    a container, image or registry; a failed Session stays registered on
    `harness.concurrency_sessions`.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index < 3, 'bounded concurrency Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    owner = {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')}
    require(descriptor['owner'] == owner, 'concurrency Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and
            descriptor['incarnation_id'] == scope['machine_incarnation'],
            'concurrency Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    require(type(images) is dict and re.fullmatch(r'sha256:[0-9a-f]{64}', images.get('base', {}).get('id') or ''),
            'prepared build base image pin required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    fixture = fixture_contract()
    expected = manifest_expectations()
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents concurrency dispatch')
    sessions = getattr(harness, 'concurrency_sessions', None)
    if sessions is None:
        sessions = harness.concurrency_sessions = []
    require(len(sessions) == index and all(item.cleanup_complete is True for item in sessions),
            'earlier concurrency Session lacks completed cleanup')
    output = harness.evidence / ('concurrency-machine-' + str(index))
    require(not os.path.lexists(output), 'concurrency Machine evidence directory preexists')
    image_id = sentinel_image(harness, descriptor)
    harness.monitor.check()
    startup.private(output)
    started = time.time_ns()
    token = 'vzconc-' + uuid.uuid4().hex[:24]
    session = Session(harness, descriptor, image_id, token, fixture['script'], output)
    sessions.append(session)
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'started_unix_ns': started,
              'token': token, 'image_id': image_id, 'probe_sha256': fixture['script_sha256'],
              'fixture_contract_sha256': fixture['contract_sha256'], 'manifest_expected': expected,
              'host_route': 'public_vz_exec_the_Machine_is_the_Docker_host_of_these_clients',
              'pull_route': 'owned_private_distribution_registry_on_the_machine_loopback_only',
              'exec_topology': 'eight_distinct_containers_the_oci_runtime_refuses_a_second_exec_into_one'}
    startup.document(output / 'concurrency-machine.intent.json', intent)
    try:
        config = session.registry_config()
        seeded = session.seed_host(config)
        # The long-running recipes come first on purpose. Each of the twenty
        # ready containers carries a one-second healthcheck, and a healthcheck
        # is a youki invocation: leaving them alive across the four builds and
        # the image publication would spend thousands of runtime invocations on
        # a Machine for no evidence at all.
        builds = parallel_builds(harness, descriptor, scope, proof, images, index, output / 'builds')
        registry_proof = session.start_registry()
        published = session.publish_images()
        clients, dispatched = session.launch()
        rows, ready_unix_ns = session.observe_ready(dispatched)
        ready = verify_ready(rows, [session.ids[name] for name in session.names],
                             dispatched_unix_ns=dispatched, ready_unix_ns=ready_unix_ns)
        ready['client_dispatch'] = mutual_overlap([client.interval() for client in clients],
                                                  'the twenty run clients did not overlap')
        execs = session.parallel_execs()
        pulls = session.parallel_pulls(published)
        harness.monitor.check()
        result = {'schema_version': 1, 'scope': SCOPE, 'kind': 'installed_concurrency_raw_evidence',
                  'token': token, 'image_id': image_id, 'owner': copy.deepcopy(owner),
                  'machine_scope': copy.deepcopy(scope), 'index': index, 'started_unix_ns': started,
                  'source_pins': pins, 'probe_sha256': fixture['script_sha256'],
                  'fixture_contract_sha256': fixture['contract_sha256'], 'manifest_expected': expected,
                  'host_seed': seeded, 'container_markers': {session.ids[name]: session.markers[name]
                                                             for name in session.names},
                  'ready_containers': ready, 'parallel_execs': execs, 'parallel_builds': builds,
                  'registry': registry_proof, 'published_images': published, 'parallel_pulls': pulls,
                  'compatibility_certified': False, 'release_scenarios_passed': [],
                  'remaining': ['aggregate release certification and physical evidence',
                                'registry authentication and TLS remain the registry suite\'s claim']}
        result['correlation'] = correlate(result)
        startup.document(output / 'workload.json', result)
        session.remove()
    except BaseException:
        session.failed = True
        raise
    verify_sources(pins)
    result['ended_unix_ns'] = time.time_ns()
    result['cleanup_complete'] = True
    result['scenarios'] = {'docker.operation.concurrent_clients': 'dev_observed_not_release_certified'}
    result['test_case_retries'] = 0
    result['docker_parity_certified'] = False
    startup.document(output / 'machine-concurrency-validation.json', result)
    return result
