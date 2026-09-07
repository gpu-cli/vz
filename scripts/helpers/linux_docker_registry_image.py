"""Source-selected registry image bytes/commands and pure admission only.

No dispatch, filesystem access, authentication or operation-order proof. Caller
must establish absence before pull and preserve complete unrelated-image state.
Pinned Moby 29.7.2 image_push.go pushes the manifest unchanged; image_inspect.go
collectRepoTagsAndDigests includes digest names in BOTH reference arrays.
"""
import copy
import re

import linux_docker_image_fixture as image
import linux_docker_image_archive as archive
import linux_docker_registry_fixture as registry


def require(condition):
    if not condition:
        raise ValueError('registry image admission rejected')


def contract(spec):
    require(type(spec) is dict and spec == registry.resource_spec(spec.get('owner'), spec.get('run_id')))
    expected = image.fixture('subject')
    token = spec['labels']['com.vz.registry.owner']
    local = 'docker.io/library/vzregistry-' + token + ':subject'
    remote = spec['repository'] + ':subject'
    digested = spec['repository'] + '@' + expected['manifest_digest']
    return {'schema_version': 1, 'scope': 'source_selected_registry_image_recipe_only',
        'seed_reference': local, 'export_reference': local, 'remote_reference': remote,
        'digest_reference': digested, 'expected': expected,
        'commands': {
            'inventory': ['image', 'ls', '--all', '--no-trunc', '--quiet'],
            'load': ['image', 'load', '--platform', 'linux/arm64'],
            'tag_remote': ['tag', local, remote], 'push': ['push', remote],
            'remove_remote': ['image', 'rm', '--no-prune', remote],
            'remove_seed': ['image', 'rm', '--no-prune', local],
            'remove_digest': ['image', 'rm', '--no-prune', digested],
            'pull_digest': ['pull', '--platform', 'linux/arm64', digested],
            'tag_export': ['tag', digested, local],
            'save_export': ['image', 'save', '--platform', 'linux/arm64', local],
            'inspect_loaded': ['image', 'inspect', '--platform', 'linux/arm64', local],
            'inspect_tagged': ['image', 'inspect', '--platform', 'linux/arm64', remote],
            'inspect_pulled': ['image', 'inspect', '--platform', 'linux/arm64', digested],
            'inspect_export-tagged': ['image', 'inspect', '--platform', 'linux/arm64', local]},
        'operation_order_certified': False, 'registry_execution_certified': False}


def seed(spec):
    return image.archive('subject', contract(spec)['seed_reference'])


def validate_remote(manifest, config, layer):
    """Validate independently acquired exact remote bytes, not their transport."""
    expected = image.fixture('subject')
    rows = {}
    for name, raw in (('manifest', manifest), ('config', config), ('layer', layer)):
        require(type(raw) is bytes and len(raw) == expected[name + '_size'] and
                image.sha256(raw) == expected[name + '_digest'][7:])
        rows[name] = {'size': len(raw), 'sha256': image.sha256(raw)}
    require(manifest == image.canonical(expected['manifest']) and config == image.canonical(expected['config']))
    return {'schema_version': 1, 'scope': 'exact_remote_subject_bytes_only', 'blobs': rows,
            'manifest_digest': expected['manifest_digest'], 'config_digest': expected['config_digest'],
            'layer_digest': expected['layer_digest'], 'diff_id': expected['diff_id'],
            'remote_transport_authenticated': False, 'registry_execution_certified': False}


def _references(recipe, stage):
    local = image.familiar_reference(recipe['seed_reference'])
    remote, digested = recipe['remote_reference'], recipe['digest_reference']
    local_digest = local.rsplit(':', 1)[0] + '@' + recipe['expected']['manifest_digest']
    states = {'loaded': ([local], [local_digest]),
              'tagged': ([local, remote], [local_digest, digested]),
              'pulled': ([digested], [digested]),
              'export-tagged': ([digested, local], [digested, local_digest])}
    require(type(stage) is str and stage in states)
    return tuple(sorted(values) for values in states[stage])


def validate_inspect(raw, *, spec, stage):
    recipe = contract(spec)
    expected = recipe['expected']
    rows = registry.decode(raw)
    require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict)
    row = rows[0]
    require(row.get('Id') == expected['manifest_digest'] and row.get('Architecture') == 'arm64'
            and row.get('Os') == 'linux' and row.get('Variant') in (None, '', 'v8')
            and row.get('Created') == image.CREATED and
            row.get('RootFS') == {'Type': 'layers', 'Layers': [expected['diff_id']]})
    tags, digests = _references(recipe, stage)
    for key, values in (('RepoTags', tags), ('RepoDigests', digests)):
        require(type(row.get(key)) is list and all(type(value) is str for value in row[key])
                and sorted(row[key]) == values)
    config = row.get('Config')
    require(type(config) is dict and config.get('Labels') == expected['labels'] and config.get('WorkingDir') == '/')
    empty = {'Hostname': '', 'Domainname': '', 'User': '', 'AttachStdin': False, 'AttachStdout': False,
             'AttachStderr': False, 'Tty': False, 'OpenStdin': False, 'StdinOnce': False,
             'ArgsEscaped': False, 'Image': '', 'NetworkDisabled': False, 'MacAddress': '', 'StopSignal': '',
             'ExposedPorts': None, 'Env': None, 'Cmd': None, 'Healthcheck': None, 'Volumes': None,
             'Entrypoint': None, 'OnBuild': None, 'StopTimeout': None, 'Shell': None}
    for key, value in config.items():
        if key not in ('Labels', 'WorkingDir'):
            require(key in empty and image.canonical(value) == image.canonical(empty[key]))
    return {'schema_version': 1, 'scope': 'source_selected_image_inspect_only', 'stage': stage,
            'manifest_digest': expected['manifest_digest'], 'config_digest_certified': False,
            'config_projection': copy.deepcopy(config), 'diff_id': expected['diff_id'],
            'repo_tags': tags, 'repo_digests': digests, 'raw_sha256': image.sha256(raw),
            'registry_execution_certified': False}


def validate_export(raw, *, spec):
    recipe = contract(spec)
    expected = recipe['expected']
    return archive.validate(raw, expected_manifest_digest=expected['manifest_digest'],
        expected_config_digest=expected['config_digest'], expected_layer_digest=expected['layer_digest'],
        expected_diff_id=expected['diff_id'], expected_reference=recipe['export_reference'],
        expected_payload_path=expected['payload']['path'], expected_payload_sha256=expected['payload']['sha256'],
        expected_payload_size=expected['payload']['size'], expected_labels=expected['labels'])


def validate_absent(raw_image_ids, *, spec):
    recipe = contract(spec)
    require(type(raw_image_ids) is bytes and len(raw_image_ids) <= 65536 and
            (not raw_image_ids or raw_image_ids.endswith(b'\n')))
    lines = raw_image_ids.splitlines()
    require(len(lines) <= 1024 and all(re.fullmatch(b'sha256:[0-9a-f]{64}', value) for value in lines))
    ids = sorted({value.decode('ascii') for value in lines})
    require(len(ids) <= 256 and recipe['expected']['manifest_digest'] not in ids)
    return {'schema_version': 1, 'scope': 'bounded_complete_image_ID_inventory_only',
            'image_ids': ids, 'raw_sha256': image.sha256(raw_image_ids), 'subject_manifest_absent': True,
            'unrelated_baseline_preserved_certified': False, 'historical_pull_certified': False}
