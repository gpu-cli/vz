"""Unused installed-Machine adapter for the bounded image DEV recipe.

Normal Up, authenticated Machine inputs, sentinel provisioning/monitoring and
public Stops belong to the topology harness. This adapter never builds an image,
provisions a builder, starts a container or invokes cleanup after an exception.
The normal Driver's required base/compose pins are admission-only here; the
image recipe consumes its own source-selected tiny subject and decoy archives.
"""
import copy
import os
from pathlib import Path
import re
import time

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_image_roundtrip as image

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_source_selected_Machine_image_roundtrip_not_release_certification'


def required_source_paths():
    root = Path(__file__).resolve().parent
    return [str(root / name) for name in (
        'linux_docker_image_machine.py', 'linux_docker_image_roundtrip.py',
        'linux_docker_image_fixture.py', 'linux_docker_image_archive.py',
        'linux_docker_container_commands.py', 'linux_docker_compose_evidence.py',
        'linux_docker_interactive_capture.py', 'linux_docker_interactive_evidence.py',
        'docker_host_driver.py', 'installed_developer_startup.py', 'linux_docker_e2e.py')]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact image source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'image source changed: ' + name)


def same(left, right, reason):
    require(image.canonical(left) == image.canonical(right), reason)


def retained(output, name):
    return image.interactive.parse(driver.regular(output / name, LIMIT))


def boundary(harness, selected, inputs, pins):
    """Certainty without the final-cleanup flag: that flag is still false here."""
    require(len(harness.drivers) == len(harness.driver_cleanup_verified) and
            sum(item is selected for item in harness.drivers) == 1 and
            all(flag is (False if item is selected else True) for item, flag in
                zip(harness.drivers, harness.driver_cleanup_verified)),
            'image Driver ownership or previous semantic cleanup unresolved')
    verify_sources(pins)
    same(selected.inputs.raw, inputs, 'image Machine inputs changed')
    selected.inputs.verify_runtime_evidence()
    require(driver.tree_digest(selected.fixture) == inputs['fixture_sha256'], 'Driver fixture changed')
    harness.monitor.check()
    for session in getattr(harness, 'runtime_audits', []):
        session.assert_enrolled_certain()
    require(not harness.effects_uncertain, 'uncertain harness mutation prevents image cleanup')
    for record in (harness.record, *(item.record for item in harness.drivers)):
        require(not getattr(record, 'pending_interactions', []) and
                all(row.get('effects_uncertain') is False for row in record.receipts),
                'uncertain or pending command prevents image cleanup')


def run_machine(harness, descriptor, scope, proof, images, index):
    """One registered Driver, one exercise, explicit cleanup, then final replay.

    The caller must already authenticate descriptor/scope/proof through normal
    Up and retain the sentinel monitor. No exception path calls lane.cleanup().
    Its exact instance remains on the registered Driver for deliberate later
    disposition; an unresolved flag fences the harness's normal public cleanup.
    """
    from linux_docker_e2e import input_mapping
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index < 256, 'bounded image Machine index required')
    require(len(harness.drivers) == len(harness.driver_cleanup_verified), 'Driver ownership registry differs')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    same(descriptor['owner'], {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
         'image Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'image Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    output = harness.evidence / ('image-machine-' + str(index))
    require(not os.path.lexists(output), 'image Machine evidence directory preexists')
    inputs = copy.deepcopy(input_mapping(harness, scope, proof, images))
    same(inputs.get('scope'), scope, 'mapped image scope differs from authenticated input')
    same(inputs.get('runtime_evidence'), proof, 'mapped image runtime proof differs')
    same(inputs.get('images'), images, 'mapped image admission pins differ')
    admitted = driver.Inputs(copy.deepcopy(inputs), suite='compose')
    require(admitted.verify_runtime_evidence() is not None, 'runtime proof admission required')
    selected = driver.Driver(admitted, Path(harness.info['fixture']), output)
    position = len(harness.drivers)
    harness.drivers.append(selected)
    harness.driver_cleanup_verified.append(False)
    # Retain ownership before constructing/exercising a potentially failing lane.
    selected.image_roundtrip = image.ImageRoundTrip(selected)
    lane = selected.image_roundtrip
    started = time.time_ns()
    startup.document(output / 'inputs.json', inputs)
    intent = {
        'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
        'source_pins': pins, 'started_unix_ns': started,
        'input_images_scope': 'admission_only_not_claimed_present_or_executed'}
    startup.document(output / 'image-machine.intent.json', intent)
    same(retained(output, 'inputs.json'), inputs, 'retained initial image inputs differ')
    same(retained(output, 'image-machine.intent.json'), intent, 'retained image intent differs')
    boundary(harness, selected, inputs, pins)
    workload = lane.exercise()
    startup.document(output / 'workload.json', workload)
    before = image.replay(output, inputs, environment=selected.env)
    same(before, workload, 'image workload replay differs')
    same(retained(output, 'workload.json'), workload, 'retained image workload differs')
    boundary(harness, selected, inputs, pins)
    cleanup = lane.cleanup()
    startup.document(output / 'cleanup.json', cleanup)
    after = image.replay(output, inputs, environment=selected.env, cleanup=True)
    same(after, cleanup, 'image cleanup replay differs')
    same(retained(output, 'workload.json'), workload, 'original image workload changed')
    same(retained(output, 'cleanup.json'), cleanup, 'retained image cleanup differs')
    same(retained(output, 'inputs.json'), inputs, 'retained image inputs differ')
    same(retained(output, 'image-machine.intent.json'), intent, 'original image intent changed')
    require(cleanup['workload_complete'] is True and cleanup['cleanup_complete'] is True and
            cleanup['full_baseline_restored'] is True and cleanup['subject_absent'] is True and
            cleanup['decoy_retained'] is False and type(cleanup['command_count']) is int and
            cleanup['command_count'] == selected.record.count, 'image cleanup incomplete')
    boundary(harness, selected, inputs, pins)
    result = {'schema_version': 1, 'scope': SCOPE, 'machine_scope': copy.deepcopy(scope),
        'started_unix_ns': started, 'ended_unix_ns': time.time_ns(), 'source_pins': pins,
        'workload': workload, 'cleanup': cleanup, 'independent_validation': after,
        'test_case_retries': 0, 'image_build_dispatched_by_adapter': False,
        'input_images_scope': 'admission_only_not_claimed_present_or_executed',
        'docker_parity_certified': False, 'release_acceptance_certified': False}
    startup.document(output / 'machine-image-validation.json', result)
    same(retained(output, 'machine-image-validation.json'), result, 'retained image Machine result differs')
    verify_sources(pins)
    harness.driver_cleanup_verified[position] = True
    return result
