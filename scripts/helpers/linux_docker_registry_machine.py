"""Installed-Machine adapter for the DEV registry login/push/pull recipe.

Normal Up, authenticated Machine inputs, sentinel provisioning/monitoring and
public Stops belong to the topology harness. This adapter admits the pinned
Distribution inputs read-only before any mutation, generates one in-memory
private fixture per Machine, and sequences the existing Session phases with the
cross-Machine credential controls between them. It never calls cleanup after an
exception; a failed Session stays registered on the harness and fences the
harness's own final cleanup. Nothing here certifies a release scenario.
"""
import copy
import os
from pathlib import Path
import re
import time

import docker_host_driver as driver
import installed_developer_startup as startup

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_Machine_registry_login_push_pull_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
PIN = REPO / 'config/docker-registry-artifact-v3.1.1.json'
REQUIREMENTS = HELPERS / 'registry-requirements.txt'
WRAPPER = REPO / 'scripts/run-linux-docker-registry-e2e.sh'
# Removal of any of these fails closed even though the closure is globbed.
REQUIRED_HELPERS = ('linux_docker_registry_acquire.py', 'linux_docker_registry_archive.py',
                    'linux_docker_registry_binary.py', 'linux_docker_registry_commands.py',
                    'linux_docker_registry_credentials.py', 'linux_docker_registry_fixture.py',
                    'linux_docker_registry_guest.py', 'linux_docker_registry_image.py',
                    'linux_docker_registry_machine.py', 'linux_docker_registry_route.py',
                    'linux_docker_registry_secrets.py', 'linux_docker_registry_session.py')
BINARY_SHA256 = '669f0d9892da6ccd44a40954f39a3b929f4455d7ed02a806828346feac572834'
GO_VERSION = 'go1.25.9'
VERSION = 'v3.1.1'
MANIFEST_DIGEST = 'sha256:bc68ba48dae0e0423bb885c8d07d20c3210febbe996d38d54d32c574fda690ae'
REGISTRY_KEYS = frozenset(('schema_version', 'scope', 'pin', 'layout', 'layout_inventory_sha256', 'pins_sha256',
                           'reference', 'manifest_digest', 'config_digest', 'upstream_index_digest',
                           'archive_sha256', 'archive_bytes', 'archive_members', 'binary_sha256',
                           'binary_size', 'binary_layer_digest', 'version', 'go_version',
                           'dependencies', 'binary_executed', 'release_certified'))


def required_source_paths():
    """Every registry helper plus its transport, pin, lock, and host wrapper."""
    helpers = sorted(str(path) for path in HELPERS.glob('linux_docker_registry_*.py'))
    require(all(str(HELPERS / name) in helpers for name in REQUIRED_HELPERS), 'registry helper closure incomplete')
    return [*helpers, str(HELPERS / 'linux_docker_private_stdin.py'), str(HELPERS / 'linux_docker_artifact_stream.py'),
            str(HELPERS / 'docker_host_driver.py'), str(HELPERS / 'installed_developer_startup.py'),
            str(HELPERS / 'linux_docker_e2e.py'), str(PIN), str(REQUIREMENTS), str(WRAPPER)]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact registry source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'registry source changed: ' + name)


def locked_dependencies(versions):
    """The lock must pin exactly the versions the secrets module admitted."""
    raw = driver.regular(REQUIREMENTS, LIMIT).decode('ascii')
    pinned = {}
    for line in raw.splitlines():
        match = re.fullmatch(r'([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+-]+) *\\?', line.strip())
        if match:
            require(match.group(1) not in pinned, 'duplicate lock pin')
            pinned[match.group(1)] = match.group(2)
    require(type(versions) is dict and versions and all(pinned.get(name) == version for name, version in versions.items()),
            'registry Python dependencies differ from the lock')
    return dict(versions)


def admit_inputs(archive, layout):
    """Read-only admission of layout, archive bytes, binary metadata and deps.

    Runs before any client execution, state creation or VM provisioning. Heavy
    imports stay inside so foreign suites never require the isolated Python
    dependencies. Returns the public `info['registry']` record.
    """
    import linux_docker_registry_archive as archive_mod
    import linux_docker_registry_binary as binary
    import linux_docker_registry_fixture as fixture
    import linux_docker_registry_secrets as secrets
    archive, layout = Path(archive), Path(layout)
    require(archive.is_absolute() and layout.is_absolute() and archive == archive.resolve(strict=True) and
            layout == layout.resolve(strict=True) and layout.is_dir(), 'canonical absolute registry inputs required')
    require(layout != archive.parent and layout not in archive.parents, 'archive inside registry layout')
    pins = fixture.decode(fixture._read(PIN))
    layout_proof = fixture.validate_layout(layout, pins=pins)
    archive_proof = archive_mod.validate_archive(archive, layout=layout, pins=pins)
    binary_proof = binary.validate_layout_binary(layout, pins=pins)
    require(archive_proof['layout_inventory_sha256'] == binary_proof['layout_inventory_sha256'] ==
            layout_proof['inventory']['inventory_sha256'] and
            archive_proof['pins_sha256'] == binary_proof['pins_sha256'] == layout_proof['pins_sha256'],
            'registry admissions observed different layouts')
    require(layout_proof['manifest_digest'] == archive_proof['manifest_digest'] == MANIFEST_DIGEST and
            layout_proof['config_digest'] == pins['config']['digest'], 'registry manifest pin differs')
    require(binary_proof['binary_sha256'] == BINARY_SHA256 and binary_proof['go_version'] == GO_VERSION and
            binary_proof['module_version'] == VERSION and binary_proof['binary_executed'] is False,
            'registry binary pin differs')
    dependencies = locked_dependencies(secrets.dependency_inputs())
    record = {'schema_version': 1, 'scope': SCOPE, 'pin': str(PIN), 'layout': str(layout),
              'layout_inventory_sha256': layout_proof['inventory']['inventory_sha256'],
              'pins_sha256': layout_proof['pins_sha256'], 'reference': layout_proof['reference'],
              'manifest_digest': layout_proof['manifest_digest'], 'config_digest': layout_proof['config_digest'],
              'upstream_index_digest': layout_proof['upstream_index_digest'],
              'archive_sha256': archive_proof['archive_sha256'], 'archive_bytes': archive_proof['archive_bytes'],
              'archive_members': archive_proof['regular_members'],
              'binary_sha256': binary_proof['binary_sha256'], 'binary_size': binary_proof['binary_size'],
              'binary_layer_digest': binary_proof['layer_digest'], 'version': binary_proof['module_version'],
              'go_version': binary_proof['go_version'], 'dependencies': dependencies,
              'binary_executed': False, 'release_certified': False}
    require(set(record) == REGISTRY_KEYS, 'registry admission record shape')
    return record


def verify_registry_info(harness):
    info = harness.info
    record = info.get('registry')
    require(type(record) is dict and set(record) == REGISTRY_KEYS and record['manifest_digest'] == MANIFEST_DIGEST and
            record['binary_sha256'] == BINARY_SHA256 and record['go_version'] == GO_VERSION and
            record['version'] == VERSION and record['release_certified'] is False, 'registry admission record differs')
    archive = info.get('registry_archive')
    require(type(archive) is str and Path(archive).is_absolute() and
            info['inputs'].get(archive) == record['archive_sha256'] and
            startup.digest(Path(archive)) == record['archive_sha256'], 'registry load archive changed')
    require(info['inputs'].get(record['pin']) == startup.digest(PIN), 'registry pin changed')
    return copy.deepcopy(record)


def same(left, right, reason):
    import linux_docker_registry_fixture as fixture
    require(fixture.canonical(left) == fixture.canonical(right), reason)


def controls_of(harness):
    controls = getattr(harness, 'registry_controls', None)
    require(controls is not None and all(callable(getattr(controls, name, None)) for name in
            ('check_after_login', 'check_after_logout', 'replay_and_scan')),
            'cross-Machine registry credential controls required before dispatch')
    return controls


def run_machine(harness, descriptor, scope, proof, images, index):
    """One registered Session: prepare, authenticate, roundtrip, replay, cleanup.

    The caller must already authenticate descriptor/scope/proof through normal
    Up, retain the sentinel monitor and construct the credential controls. No
    exception path calls session.cleanup(); an incomplete Session remains on
    `harness.registry_sessions` and withholds the harness's normal cleanup.
    """
    from linux_docker_registry_secrets import Secrets
    from linux_docker_registry_session import Session
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index <= 3, 'bounded registry Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    same(descriptor['owner'], {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
         'registry Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'registry Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    base = {key: harness.info['python_image'][key] for key in ('reference', 'id', 'platform')}
    same(images, {'base': base, 'compose': base}, 'registry suite accepts only admission-only image pins')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    registry = verify_registry_info(harness)
    controls = controls_of(harness)
    project = getattr(harness, 'registry_project', None)
    require(type(project) is str and Path(project).is_absolute() and Path(project).is_dir(),
            'registry project directory required')
    sessions = getattr(harness, 'registry_sessions', [])
    require(len(sessions) == index and all(item.cleanup_complete is True for item in sessions),
            'earlier registry Session lacks completed cleanup')
    output = harness.evidence / ('registry-machine-' + str(index))
    require(not os.path.lexists(output), 'registry Machine evidence directory preexists')
    harness.monitor.check()
    started = time.time_ns()
    # In-memory only: never documented, pickled or passed as public argv.
    private = Secrets.generate(descriptor['owner'], harness.info['run_id'], now_unix_ns=started)
    session = Session(harness, descriptor, project, index, private)
    require(harness.registry_sessions[-1] is session and len(harness.registry_sessions) == index + 1 and
            session.output == output and output.is_dir(), 'registry Session registration differs')
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'registry': registry,
              'started_unix_ns': started, 'input_images_scope': 'admission_only_unused_by_registry_recipe',
              'private_fixture_scope': 'in_memory_only_never_documented'}
    session.document('registry-machine.intent.json', intent)
    timings = {}
    def phase(name, call, *args):
        begin = time.time_ns()
        value = call(*args)
        timings[name] = {'started_unix_ns': begin, 'ended_unix_ns': time.time_ns()}
        return value
    phase('prepare', session.prepare)
    route = phase('authenticate', session.authenticate)
    login_controls = phase('controls_after_login', controls.check_after_login, descriptor)
    workload = phase('roundtrip', session.roundtrip)
    require(session.workload_complete is True and type(workload) is dict, 'registry workload incomplete')
    session.document('workload.json', workload)
    replay = phase('replay_and_scan', controls.replay_and_scan, session, output)
    require(type(replay) is dict and bool(replay), 'independent registry replay/scan proof required')
    session.document('independent-replay.json', replay)
    # Only a passed replay/scan admits the exact owned cleanup.
    cleanup = phase('cleanup', session.cleanup)
    require(session.cleanup_complete is True and type(cleanup) is dict, 'registry cleanup incomplete')
    logout_controls = phase('controls_after_logout', controls.check_after_logout, descriptor)
    session.commands.assert_certain()
    session.certain()
    verify_sources(pins)
    result = {'schema_version': 1, 'scope': SCOPE, 'machine_scope': copy.deepcopy(scope), 'index': index,
              'started_unix_ns': started, 'ended_unix_ns': time.time_ns(), 'timings': timings,
              'source_pins': pins, 'registry': registry, 'login_route': route, 'workload': workload,
              'independent_validation': replay, 'cleanup': cleanup,
              'credential_controls': {'after_login': login_controls, 'after_logout': logout_controls},
              'test_case_retries': 0, 'input_images_scope': 'admission_only_unused_by_registry_recipe',
              'docker_parity_certified': False, 'release_certified': False}
    session.document('machine-registry-validation.json', result)
    return result
