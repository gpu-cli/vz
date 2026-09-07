"""Installed-Machine adapter for the read-only Engine version/API handshake DEV recipe.

Covers `docker.engine.version` and `docker.engine.api_negotiation` for one
authenticated Developer Linux Machine through its private `--config`/`--context`
route. Every expectation is pinned to upstream source at the manifest's exact
Engine commit and CLI tag (see SOURCES); the checked-in compatibility manifest is
reconciled against those pins before the first command. Nothing here mutates the
Machine: there are no containers, images or volumes to clean up, so no exception
path performs any removal. Nothing here certifies a release scenario.

Per-call `DOCKER_API_VERSION` overrides never touch `harness.env`: each override
runs through a private `startup.Recorder` whose environment is an explicit copy
of the harness environment plus that single variable.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import time

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_buildkit_cgroup as cgroup

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_Machine_engine_version_api_handshake_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
MANIFEST = REPO / 'config/docker-compatibility-v0.4.json'
SCENARIOS = ('docker.engine.version', 'docker.engine.api_negotiation')

# Upstream pins. The daemon values come from moby/moby at the manifest's exact
# Engine source commit; the client values come from the moby client module that
# docker/cli v29.4.0 vendors (vendor.mod: github.com/moby/moby/client v0.4.0).
CLIENT = {'Version': '29.4.0', 'GitCommit': '9d7ad9f', 'Os': 'darwin', 'Arch': 'arm64',
          'MaxAPIVersion': '1.54', 'MinAPIVersion': '1.40'}
SERVER = {'Version': '29.7.2', 'GitCommit': '6a43e3d', 'Os': 'linux', 'Arch': 'arm64',
          'MaxAPIVersion': '1.55', 'DefaultMinAPIVersion': '1.40', 'ConfigurableFloor': '1.24'}
# Highest mutually supported: min(client MaxAPIVersion 1.54, server MaxAPIVersion 1.55).
NEGOTIATED = '1.54'
ACCEPTED = ('1.40', '1.55')
REJECTED = {
    '1.39': 'client version 1.39 is too old. Minimum supported API version is 1.40, '
            'please upgrade your client to a newer version',
    '1.56': 'client version 1.56 is too new. Maximum supported API version is 1.55'}
DAEMON_ERROR_PREFIX = 'Error response from daemon: '
COMPONENTS_REQUIRED = ('Engine', 'containerd', 'docker-init', 'youki')
COMPONENTS_FORBIDDEN = ('runc', 'crun')
SOURCES = {
    'server_versions': 'https://raw.githubusercontent.com/moby/moby/6a43e3d5afddf4111da0f864bbc7cae5d7e95001/'
                       'daemon/config/config.go (MaxAPIVersion = "1.55", defaultMinAPIVersion = "1.40", '
                       'MinAPIVersion = "1.24")',
    'server_rejection_text': 'https://raw.githubusercontent.com/moby/moby/6a43e3d5afddf4111da0f864bbc7cae5d7e95001/'
                             'daemon/server/middleware/version.go (versionUnsupportedError.Error)',
    'client_versions': 'https://raw.githubusercontent.com/docker/cli/v29.4.0/vendor/github.com/moby/moby/client/'
                       'client.go (MaxAPIVersion = "1.54", MinAPIVersion = "1.40"; manual override unvalidated)',
    'client_vendor': 'https://raw.githubusercontent.com/docker/cli/v29.4.0/vendor.mod '
                     '(github.com/moby/moby/api v1.54.1, github.com/moby/moby/client v0.4.0)',
    'client_error_prefix': 'https://raw.githubusercontent.com/docker/cli/v29.4.0/vendor/github.com/moby/moby/client/'
                           'request.go (checkResponseErr: "Error response from daemon: %w")',
    'client_version_command': 'https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/command/system/version.go '
                              '(DefaultAPIVersion: client.MaxAPIVersion; Server nil on error; client printed first)'}
VERSION_ARGS = ['version', '--format', '{{json .}}']
INFO_ARGS = ['info', '--format', '{{json .}}']
FLAG_SCRIPT = '\n'.join((
    'set -eu', 'bb=/bin/busybox', 'found=0', "printf 'VZ_DOCKERD_FLAGS_V1\\n'",
    'for d in /proc/[0-9]*; do',
    '  [ "$("$bb" cat "$d/comm" 2>/dev/null || true)" = dockerd ] || continue',
    '  found=$((found+1))',
    "  printf 'PID=%s\\n' \"${d#/proc/}\"",
    "  \"$bb\" tr '\\0' '\\n' < \"$d/cmdline\" | while IFS= read -r a; do printf 'ARG=%s\\n' \"$a\"; done",
    "  \"$bb\" tr '\\0' '\\n' < \"$d/environ\" | while IFS= read -r e; do",
    "    case \"$e\" in DOCKER_MIN_API_VERSION=*) printf 'ENV=%s\\n' \"$e\";; esac; done",
    'done',
    "printf 'COUNT=%s\\n' \"$found\"",
    "printf 'VZ_DOCKERD_FLAGS_END\\n'"))


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_handshake_machine.py', 'linux_docker_buildkit_cgroup.py',
        'docker_host_driver.py', 'installed_developer_startup.py', 'linux_docker_e2e.py')] + [str(MANIFEST)]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact handshake source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'handshake source changed: ' + name)


def unique(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate JSON field')
        row[key] = value
    return row


def parse(raw):
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT, 'bounded JSON stream required')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
    except (UnicodeError, ValueError) as error:
        raise ValueError('handshake: malformed JSON output') from error
    require(type(value) is dict, 'JSON object required')
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def same(left, right, reason):
    require(canonical(left) == canonical(right), reason)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def manifest_expectations():
    """Reconcile the checked-in manifest with the upstream pins before dispatch.

    The manifest is admission input, never the source of truth: a manifest that
    disagrees with the pinned upstream constants fails closed here.
    """
    manifest = parse(driver.regular(MANIFEST, LIMIT))
    versions = manifest['candidate_versions']
    engine, api, cli = versions['docker_engine'], versions['engine_api'], versions['docker_cli']
    require(engine['minimum'] == engine['maximum'] == SERVER['Version'] and engine['build'] == SERVER['GitCommit'] and
            engine['source_commit'].startswith(SERVER['GitCommit']), 'manifest Engine pin differs from upstream pin')
    require(api['minimum'] == SERVER['DefaultMinAPIVersion'] and api['maximum'] == SERVER['MaxAPIVersion'] and
            api['minimum_override_allowed'] is False and api['upstream_configurable_floor'] == SERVER['ConfigurableFloor'],
            'manifest API window differs from upstream daemon/config/config.go')
    require(cli['minimum'] == cli['maximum'] == CLIENT['Version'] and cli['build'] == CLIENT['GitCommit'],
            'manifest CLI pin differs from upstream pin')
    scenarios = {row['id']: row for row in manifest['scenarios'] if row['id'] in SCENARIOS}
    require(set(scenarios) == set(SCENARIOS), 'manifest scenarios missing')
    same(scenarios['docker.engine.version']['expected'],
         {'server_version': SERVER['Version'], 'client_version': CLIENT['Version'],
          'target_os': SERVER['Os'], 'target_architecture': SERVER['Arch']}, 'docker.engine.version expectation differs')
    same(scenarios['docker.engine.api_negotiation']['expected'],
         {'server_default_min_api': SERVER['DefaultMinAPIVersion'], 'server_max_api': SERVER['MaxAPIVersion'],
          'automatic_negotiation': 'highest_mutually_supported', 'api_1_39': 'reject', 'api_1_56': 'reject',
          'min_api_override_absent': True}, 'docker.engine.api_negotiation expectation differs')
    for version in REJECTED:
        require(version not in ACCEPTED and not (SERVER['DefaultMinAPIVersion'] <= version <= SERVER['MaxAPIVersion']),
                'rejected probe version lies inside the server window')
    for version in ACCEPTED:
        require(SERVER['DefaultMinAPIVersion'] <= version <= SERVER['MaxAPIVersion'], 'accepted probe outside window')
    require(NEGOTIATED == min(CLIENT['MaxAPIVersion'], SERVER['MaxAPIVersion']), 'negotiated pin inconsistent')
    return {'manifest_sha256': sha256(driver.regular(MANIFEST, LIMIT)), 'scenarios': copy.deepcopy(scenarios),
            'candidate_versions': {'docker_engine': engine, 'engine_api': api, 'docker_cli': cli}}


def version_string(value):
    return driver.checked_text(value, r'1\.[0-9]{2}', 'API version')


def check_client(client, descriptor, api_version):
    require(type(client) is dict, 'client version object required')
    for key in ('Version', 'GitCommit', 'Os', 'Arch'):
        require(client.get(key) == CLIENT[key], 'client ' + key + ' differs from pinned CLI: ' + repr(client.get(key)))
    require(client.get('Context') == descriptor['name'], 'client context differs from the Machine context')
    require(version_string(client.get('DefaultAPIVersion')) == CLIENT['MaxAPIVersion'],
            'client DefaultAPIVersion differs from vendored client MaxAPIVersion')
    require(version_string(client.get('ApiVersion')) == api_version, 'client ApiVersion differs: ' + repr(client.get('ApiVersion')))
    # The pinned OrbStack-distributed CLI 29.4.0 is built without a platform
    # name, so `docker version` emits no Client.Platform (observed candidate 1).
    require('Platform' not in client, 'client platform unexpectedly present: ' + repr(client.get('Platform')))


def check_server(server):
    require(type(server) is dict, 'server version object required')
    for key in ('Version', 'GitCommit', 'Os', 'Arch'):
        require(server.get(key) == SERVER[key], 'server ' + key + ' differs from pinned Engine: ' + repr(server.get(key)))
    require(version_string(server.get('ApiVersion')) == SERVER['MaxAPIVersion'], 'server ApiVersion differs')
    require(version_string(server.get('MinAPIVersion')) == SERVER['DefaultMinAPIVersion'],
            'server MinAPIVersion differs from the unoverridden default')
    components = server.get('Components')
    require(type(components) is list and all(type(row) is dict and type(row.get('Name')) is str for row in components),
            'component list required')
    names = [row['Name'] for row in components]
    require(len(set(names)) == len(names) and set(COMPONENTS_REQUIRED) <= set(names) and
            not set(COMPONENTS_FORBIDDEN) & set(names), 'unexpected Engine component set: ' + repr(names))
    runtime = next(row for row in components if row['Name'] == 'youki')
    # Moby fillDefaultRuntimeVersion parses `youki --version` ("youki version 0.7.0")
    # into the observed ": 0.7.0"; the commit string is the bundle's patched youki.
    require(runtime.get('Version') == ': 0.7.0' and type(runtime.get('Details')) is dict and
            type(runtime['Details'].get('GitCommit')) is str and runtime['Details']['GitCommit'].startswith('0.7.0-'),
            'youki runtime component differs from the pinned guest bundle: ' + repr(runtime))
    engine = next(row for row in components if row['Name'] == 'Engine')
    details = engine.get('Details')
    require(engine.get('Version') == SERVER['Version'] and type(details) is dict and
            details.get('ApiVersion') == SERVER['MaxAPIVersion'] and details.get('MinAPIVersion') == SERVER['DefaultMinAPIVersion'] and
            details.get('Os') == SERVER['Os'] and details.get('Arch') == SERVER['Arch'] and
            details.get('GitCommit') == SERVER['GitCommit'], 'Engine component details differ from pinned Engine')
    return {'component_names': names, 'engine_details': copy.deepcopy(details),
            'kernel_version': server.get('KernelVersion'), 'go_version': server.get('GoVersion')}


def check_info(info, descriptor):
    require(type(info) is dict and info.get('ID') == descriptor['engine_id'], 'info came from a different Engine')
    require(info.get('ServerVersion') == SERVER['Version'] and info.get('OSType') == SERVER['Os'] and
            info.get('Architecture') == 'aarch64', 'info version/platform differs from pinned Engine')
    runtimes = info.get('Runtimes')
    require(info.get('DefaultRuntime') == 'youki' and type(runtimes) is dict and 'youki' in runtimes,
            'Engine default runtime is not youki')
    require(info.get('CgroupVersion') == '2', 'cgroup v2 Engine required')
    return {'default_runtime': 'youki', 'runtimes': sorted(runtimes), 'server_version': info['ServerVersion'],
            'cgroup_version': info['CgroupVersion'], 'warnings': list(info.get('Warnings') or [])}


def parse_flags(raw):
    require(type(raw) is bytes and len(raw) <= LIMIT, 'bounded flag stream required')
    try:
        lines = raw.decode('utf-8').split('\n')
    except UnicodeError as error:
        raise ValueError('handshake: non-UTF-8 daemon flag stream') from error
    require(len(lines) >= 4 and lines[0] == 'VZ_DOCKERD_FLAGS_V1' and lines[-1] == '' and
            lines[-2] == 'VZ_DOCKERD_FLAGS_END', 'incomplete daemon flag frames')
    body = lines[1:-2]
    require(body and body[-1].startswith('COUNT=') and body[-1] == 'COUNT=1', 'exactly one dockerd process required')
    pids, args, env = [], [], []
    for line in body[:-1]:
        kind, separator, value = line.partition('=')
        require(separator == '=' and kind in ('PID', 'ARG', 'ENV'), 'unknown daemon flag frame')
        (pids if kind == 'PID' else args if kind == 'ARG' else env).append(value)
    require(len(pids) == 1 and re.fullmatch(r'[1-9][0-9]*', pids[0]), 'one dockerd PID required')
    require(args and args[0].endswith('dockerd'), 'dockerd argv0 required')
    require(not any(arg == '--min-api-version' or arg.startswith('--min-api-version=') for arg in args),
            'daemon --min-api-version override present')
    require(env == [], 'daemon DOCKER_MIN_API_VERSION override present')
    return {'pid': int(pids[0]), 'argv': args, 'min_api_override_absent': True}


def daemon_flags(harness, descriptor):
    """Bounded read-only public Exec: dockerd argv and DOCKER_MIN_API_VERSION absence."""
    owner = descriptor['owner']
    binding = cgroup.project_binding(harness, descriptor)
    raw, stderr, code = harness.command('handshake-daemon-flags', [harness.cli, 'exec', '--environment',
        owner['environment_id'], '--machine', owner['machine_id'], '--no-stdin', '--timeout', '30', '--',
        '/bin/busybox', 'sh', '-c', FLAG_SCRIPT], cwd=Path(binding['project_path']), timeout=40, success=False)
    require(type(code) is int and code == 0 and stderr == b'', 'public Exec failed; raw diagnostic retained')
    require(cgroup.project_binding(harness, descriptor) == binding, 'owned project binding changed during observation')
    proof = parse_flags(raw)
    proof.update(stdout_sha256=sha256(raw), stderr_sha256=sha256(stderr), **binding)
    return proof


def override_probe(harness, descriptor, output, version):
    """Run `docker version` with one explicit DOCKER_API_VERSION through a private Recorder.

    The environment is an explicit copy of the harness environment plus that
    single variable; `harness.env` and `harness.record` are never modified.
    """
    version_string(version)
    directory = startup.private(output / ('api-' + version.replace('.', '-')))
    environment = dict(harness.env)
    require('DOCKER_API_VERSION' not in environment, 'harness environment already overrides the API version')
    environment['DOCKER_API_VERSION'] = version
    recorder = startup.Recorder(directory, environment)
    argv = ['docker', '--config', descriptor['config_dir'], '--context', descriptor['name'], *VERSION_ARGS]
    raw, stderr, code = recorder.run('handshake-api-' + version, argv,
                                     executable=harness.info['clients']['docker']['canonical'],
                                     cwd=harness.root, timeout=60, success=False)
    require(dict(harness.env) == {k: v for k, v in environment.items() if k != 'DOCKER_API_VERSION'},
            'harness environment changed during override probe')
    require(len(recorder.receipts) == 1 and recorder.receipts[0]['capture_complete'] is True and
            recorder.receipts[0]['effects_uncertain'] is False, 'override probe capture incomplete')
    value = parse(raw)
    require(set(value) == {'Client', 'Server'}, 'version JSON shape')
    check_client(value['Client'], descriptor, version)
    row = {'api_version': version, 'exit_code': code, 'stdout_sha256': sha256(raw), 'stderr_sha256': sha256(stderr),
           'environment_variable': 'DOCKER_API_VERSION', 'negotiation': 'disabled_by_explicit_override'}
    if version in REJECTED:
        text = stderr.decode('utf-8', 'strict').rstrip('\n')
        require(code == 1 and value['Server'] is None and text == DAEMON_ERROR_PREFIX + REJECTED[version],
                'API ' + version + ' rejection differs: exit ' + repr(code) + ', stderr ' + repr(text))
        row.update(outcome='rejected', daemon_error=REJECTED[version])
    else:
        require(version in ACCEPTED, 'unknown probe version')
        require(code == 0 and stderr == b'', 'API ' + version + ' acceptance differs')
        row.update(outcome='accepted', server=check_server(value['Server']))
    return row


def run_machine(harness, descriptor, scope, proof, images, index):
    """Read-only handshake on one authenticated Machine; retained raw streams.

    The caller must already authenticate descriptor/scope/proof through normal
    Up and retain the sentinel monitor. No Docker object is created, so nothing
    is ever cleaned up, on success or on exception.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index <= 3, 'bounded handshake Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    same(descriptor['owner'], {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
         'handshake Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'handshake Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    manifest = manifest_expectations()
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents handshake dispatch')
    output = harness.evidence / ('handshake-machine-' + str(index))
    require(not os.path.lexists(output), 'handshake Machine evidence directory preexists')
    startup.private(output)
    harness.monitor.check()
    started = time.time_ns()
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'started_unix_ns': started,
              'manifest': manifest, 'upstream_pins': {'client': CLIENT, 'server': SERVER, 'negotiated': NEGOTIATED,
              'accepted': list(ACCEPTED), 'rejected': REJECTED}, 'sources': SOURCES,
              'input_images_scope': 'admission_only_unused_by_handshake_recipe', 'mutations': 0}
    startup.document(output / 'handshake-machine.intent.json', intent)
    raw, stderr, _ = harness.docker('handshake-version', descriptor, VERSION_ARGS)
    require(stderr == b'', 'negotiated version emitted stderr')
    value = parse(raw)
    require(set(value) == {'Client', 'Server'}, 'version JSON shape')
    check_client(value['Client'], descriptor, NEGOTIATED)
    server = check_server(value['Server'])
    require(value['Client']['ApiVersion'] == min(value['Client']['DefaultAPIVersion'], value['Server']['ApiVersion']),
            'negotiated version is not the highest mutually supported')
    negotiated = {'client_api_version': NEGOTIATED, 'client_default_api_version': CLIENT['MaxAPIVersion'],
                  'server_api_version': SERVER['MaxAPIVersion'], 'server_min_api_version': SERVER['DefaultMinAPIVersion'],
                  'stdout_sha256': sha256(raw), 'stderr_sha256': sha256(stderr), **server}
    raw, stderr, _ = harness.docker('handshake-info', descriptor, INFO_ARGS)
    require(stderr == b'', 'info emitted stderr')
    info = check_info(parse(raw), descriptor)
    info.update(stdout_sha256=sha256(raw), stderr_sha256=sha256(stderr))
    flags = daemon_flags(harness, descriptor)
    probes = [override_probe(harness, descriptor, output, version) for version in ('1.39', '1.56', *ACCEPTED)]
    require([row['outcome'] for row in probes] == ['rejected', 'rejected', 'accepted', 'accepted'], 'probe outcomes')
    harness.monitor.check()
    verify_sources(pins)
    result = {'schema_version': 1, 'scope': SCOPE, 'machine_scope': copy.deepcopy(scope), 'index': index,
              'started_unix_ns': started, 'ended_unix_ns': time.time_ns(), 'source_pins': pins,
              'manifest': manifest, 'sources': SOURCES, 'negotiated': negotiated, 'info': info,
              'daemon_flags': flags, 'api_version_probes': probes,
              'scenarios': {'docker.engine.version': 'dev_observed_not_release_certified',
                            'docker.engine.api_negotiation': 'dev_observed_not_release_certified'},
              'mutations': 0, 'cleanup_required': False, 'test_case_retries': 0,
              'input_images_scope': 'admission_only_unused_by_handshake_recipe',
              'docker_parity_certified': False, 'release_certified': False}
    startup.document(output / 'machine-handshake-validation.json', result)
    same(parse(driver.regular(output / 'machine-handshake-validation.json', LIMIT)), result,
         'retained handshake result differs')
    return result
