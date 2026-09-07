"""Offline admission of externally pinned Distribution registry fixture bytes.

No network, subprocess, extraction, credential access, or Machine mutation.
The selected-only OCI layout retains the original upstream multi-platform index
as a provenance blob; unrelated platform blobs are deliberately not imported.
Pins must arrive from independently admitted acquisition, never from this
validator's observations. TLS metadata checks do not verify X.509 signatures.
"""
import copy
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re

import linux_docker_artifact_stream as artifact

INDEX = 'application/vnd.oci.image.index.v1+json'
DOCKER_INDEX = 'application/vnd.docker.distribution.manifest.list.v2+json'
MANIFEST = 'application/vnd.oci.image.manifest.v1+json'
DOCKER_MANIFEST = 'application/vnd.docker.distribution.manifest.v2+json'
CONFIG = 'application/vnd.oci.image.config.v1+json'
DOCKER_CONFIG = 'application/vnd.docker.container.image.v1+json'
LAYERS = {
    'application/vnd.oci.image.layer.v1.tar': 'uncompressed',
    'application/vnd.oci.image.layer.v1.tar+gzip': 'gzip',
    'application/vnd.docker.image.rootfs.diff.tar': 'uncompressed',
    'application/vnd.docker.image.rootfs.diff.tar.gzip': 'gzip',
}
REPOSITORY, TAG = 'docker.io/library/registry', '3.1.1'
JSON_LIMIT = 1024 * 1024
LAYER_LIMIT = 64 * 1024 * 1024
TREE_LIMIT = 128 * 1024 * 1024
UNCOMPRESSED_LIMIT = 256 * 1024 * 1024
LAYOUT_LIMITS = artifact.Limits(file_bytes=LAYER_LIMIT, tree_bytes=TREE_LIMIT,
    entries=64, depth=4, path_bytes=128)
LAYER_LIMITS = artifact.Limits(compressed_bytes=LAYER_LIMIT,
    uncompressed_bytes=UNCOMPRESSED_LIMIT, member_bytes=UNCOMPRESSED_LIMIT,
    metadata_bytes=4 * 1024 * 1024, entries=20000)
SUBNET, GATEWAY, ADDRESS, PORT = '172.30.241.0/24', '172.30.241.1', '172.30.241.2', 5443


class FixtureError(ValueError):
    """Fixed diagnostics: never include supplied data or filesystem paths."""


def require(condition, code):
    if not condition:
        raise FixtureError('registry fixture: ' + code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii') + b'\n'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def fields(row, required, optional=()):
    require(type(row) is dict and set(required) <= set(row) <= set(required) | set(optional), 'object fields')


def integer(value, maximum, *, minimum=1):
    require(type(value) is int and minimum <= value <= maximum, 'integer bounds')
    return value


def digest(value, *, bare=False):
    require(type(value) is str and re.fullmatch(('[0-9a-f]{64}' if bare else 'sha256:[0-9a-f]{64}'), value),
            'digest syntax')
    return value


def pairs(items):
    row = {}
    for key, value in items:
        require(key not in row, 'duplicate JSON key')
        row[key] = value
    return row


def decode(raw):
    require(type(raw) is bytes and 0 < len(raw) <= JSON_LIMIT, 'JSON byte bounds')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: require(False, 'JSON constant'))
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        if isinstance(error, FixtureError):
            raise
        raise FixtureError('registry fixture: malformed JSON') from None


def platform(row):
    fields(row, ('os', 'architecture'), ('variant',))
    require(row['os'] == 'linux' and row['architecture'] == 'arm64' and row.get('variant', 'v8') == 'v8',
            'linux arm64 platform required')
    return {'os': 'linux', 'architecture': 'arm64', 'variant': 'v8'}


def descriptor(row, media, *, layer=False):
    fields(row, ('mediaType', 'digest', 'size'), ('diff_id',) if layer else ())
    require(row['mediaType'] in media, 'descriptor media type')
    digest(row['digest'])
    integer(row['size'], LAYER_LIMIT if layer else JSON_LIMIT)
    if layer:
        require('diff_id' in row, 'layer diff ID required')
        digest(row['diff_id'])
    return {key: row[key] for key in ('mediaType', 'digest', 'size')}


def validate_pins(pins):
    fields(pins, ('schema_version', 'source', 'layout_index', 'config', 'layers'))
    require(type(pins['schema_version']) is int and pins['schema_version'] == 1, 'pin schema')
    source = pins['source']
    fields(source, ('repository', 'tag', 'index', 'platform_manifest', 'platform'))
    require(source['repository'] == REPOSITORY and source['tag'] == TAG, 'Distribution release source')
    descriptor(source['index'], (INDEX, DOCKER_INDEX))
    descriptor(source['platform_manifest'], (MANIFEST, DOCKER_MANIFEST))
    platform(source['platform'])
    fields(pins['layout_index'], ('sha256', 'size'))
    digest(pins['layout_index']['sha256'], bare=True)
    integer(pins['layout_index']['size'], JSON_LIMIT)
    descriptor(pins['config'], (CONFIG, DOCKER_CONFIG))
    require(type(pins['layers']) is list and 1 <= len(pins['layers']) <= 16, 'layer count')
    for layer in pins['layers']:
        descriptor(layer, LAYERS, layer=True)
    identities = [source['index']['digest'], source['platform_manifest']['digest'],
                  pins['config']['digest'], *(item['digest'] for item in pins['layers'])]
    require(len(set(identities)) == len(identities), 'duplicate pinned blob')
    require(sum(item['size'] for item in pins['layers']) <= TREE_LIMIT, 'compressed layer aggregate')


def selected_index(pins):
    """Exact loadable root wrapper; the upstream index is retained separately."""
    validate_pins(pins)
    selected = dict(pins['source']['platform_manifest'])
    selected['platform'] = dict(pins['source']['platform'])
    selected['annotations'] = {'io.containerd.image.name': REPOSITORY + ':' + TAG,
                               'org.opencontainers.image.ref.name': TAG}
    return {'schemaVersion': 2, 'mediaType': INDEX, 'manifests': [selected]}


def blob_path(identity):
    return 'blobs/sha256/' + digest(identity)[7:]


def _read(path):
    with artifact._opened(path) as fd:
        size = os.fstat(fd).st_size
        require(size <= JSON_LIMIT, 'metadata file bounds')
        output = bytearray()
        while len(output) <= size:
            chunk = os.read(fd, min(65536, size + 1 - len(output)))
            if not chunk:
                break
            output.extend(chunk)
        require(len(output) == size, 'metadata changed')
    return bytes(output)


def _read_blob(root, pin):
    raw = _read(root / blob_path(pin['digest']))
    require(len(raw) == pin['size'] and 'sha256:' + sha(raw) == pin['digest'], 'metadata blob pin changed')
    return raw


def _validate_layout(root, pins):
    validate_pins(pins)
    # Retain expected inputs independently of caller mutation during admission.
    pins = copy.deepcopy(pins)
    root = Path(root)
    before = artifact.inventory_tree(root, limits=LAYOUT_LIMITS)
    files = {row['path']: row for row in before['files']}
    expected = [pins['source']['index'], pins['source']['platform_manifest'], pins['config'], *pins['layers']]
    require(set(files) == {'oci-layout', 'index.json', *(blob_path(item['digest']) for item in expected)},
            'exact selected layout inventory')
    require({row['path'] for row in before['directories']} == {'blobs', 'blobs/sha256'},
            'exact layout directories')
    require(all(row['mode'] in (0o400, 0o444, 0o600, 0o644) for row in before['files']) and
            all(row['mode'] in (0o500, 0o555, 0o700, 0o755) for row in before['directories']),
            'unsafe layout mode')
    for item in expected:
        observed = files[blob_path(item['digest'])]
        require(observed['sha256'] == item['digest'][7:] and observed['size'] == item['size'], 'pinned blob differs')
    require({key: files['index.json'][key] for key in ('sha256', 'size')} == pins['layout_index'],
            'selected index pin differs')
    require(_read(root / 'oci-layout') == canonical({'imageLayoutVersion': '1.0.0'}), 'OCI layout version')
    require(_read(root / 'index.json') == canonical(selected_index(pins)), 'selected index wrapper differs')
    source = pins['source']
    upstream = decode(_read_blob(root, source['index']))
    fields(upstream, ('schemaVersion', 'mediaType', 'manifests'), ('annotations',))
    require(type(upstream['schemaVersion']) is int and upstream['schemaVersion'] == 2 and
            upstream['mediaType'] == source['index']['mediaType'] and type(upstream['manifests']) is list and
            1 <= len(upstream['manifests']) <= 64, 'upstream index schema')
    matches = []
    for item in upstream['manifests']:
        fields(item, ('mediaType', 'digest', 'size', 'platform'), ('annotations',))
        digest(item['digest'])
        integer(item['size'], JSON_LIMIT)
        require(type(item['platform']) is dict, 'upstream platform object')
        if item['platform'].get('os') == 'linux' and item['platform'].get('architecture') == 'arm64':
            require(platform(item['platform']) == platform(source['platform']), 'selected upstream platform')
            matches.append({key: item[key] for key in ('mediaType', 'digest', 'size')})
    require(matches == [source['platform_manifest']], 'one exact upstream arm64 selection')
    manifest = decode(_read_blob(root, source['platform_manifest']))
    fields(manifest, ('schemaVersion', 'mediaType', 'config', 'layers'), ('annotations',))
    require(type(manifest['schemaVersion']) is int and manifest['schemaVersion'] == 2 and
            manifest['mediaType'] == source['platform_manifest']['mediaType'] and
            manifest['config'] == pins['config'] and manifest['layers'] ==
            [{key: layer[key] for key in ('mediaType', 'digest', 'size')} for layer in pins['layers']],
            'selected manifest descriptors differ')
    config = decode(_read_blob(root, pins['config']))
    require(type(config) is dict, 'image configuration object')
    platform({key: config[key] for key in ('os', 'architecture', 'variant') if key in config})
    require(config.get('rootfs') == {'type': 'layers', 'diff_ids': [item['diff_id'] for item in pins['layers']]},
            'ordered rootfs diff IDs differ')
    require(type(config.get('config')) is dict, 'image runtime configuration')
    layers, total = [], 0
    for item in pins['layers']:
        observed = artifact.scan_layer(root / blob_path(item['digest']), compression=LAYERS[item['mediaType']],
                                       limits=LAYER_LIMITS)
        total += observed['uncompressed_size']
        require(total <= UNCOMPRESSED_LIMIT, 'uncompressed layer aggregate')
        require(observed['compressed_sha256'] == item['digest'][7:] and observed['compressed_size'] == item['size']
                and observed['diff_id'] == item['diff_id'], 'decoded layer identity differs')
        layers.append({'digest': item['digest'], 'size': item['size'], 'diff_id': observed['diff_id'],
                       'uncompressed_size': observed['uncompressed_size'], 'members': len(observed['members'])})
    after = artifact.inventory_tree(root, limits=LAYOUT_LIMITS)
    require(after == before, 'layout changed during admission')
    return {'schema_version': 1, 'scope': 'offline_pinned_Distribution_arm64_fixture_bytes_only',
            'pins_sha256': sha(canonical(pins)), 'inventory': before,
            'upstream_index_digest': source['index']['digest'], 'manifest_digest': source['platform_manifest']['digest'],
            'config_digest': pins['config']['digest'], 'platform': platform(source['platform']),
            'layers': layers, 'uncompressed_bytes': total, 'reference': REPOSITORY + ':' + TAG,
            'upstream_other_platforms_included': False, 'source_authenticity_certified': False,
            'registry_execution_certified': False, 'docker_load_certified': False}


def validate_layout(root, *, pins):
    """Read only a quiescent layout; pins and later dispatch binding are external."""
    try:
        return _validate_layout(root, pins)
    except (artifact.ArtifactError, OSError, UnicodeError, KeyError, TypeError, OverflowError):
        raise FixtureError('registry fixture: unsafe or inconsistent offline artifact') from None


def resource_spec(owner, run_id):
    fields(owner, ('project_id', 'environment_id', 'machine_id'))
    require(all(type(value) is str and re.fullmatch('[A-Za-z0-9_-]{1,128}', value) for value in owner.values()),
            'exact bounded owner')
    require(type(run_id) is str and re.fullmatch('[A-Za-z0-9_-]{1,128}', run_id), 'bounded run identity')
    suffix = sha(canonical({'owner': owner, 'run_id': run_id}))[:24]
    name = 'vz-registry-' + suffix
    network = ipaddress.ip_network(SUBNET)
    address = ipaddress.ip_address(ADDRESS)
    require(address in network and address.is_private and not address.is_loopback and
            address not in (network.network_address, network.broadcast_address, ipaddress.ip_address(GATEWAY)),
            'private nonloopback fixture address')
    authority = ADDRESS + ':' + str(PORT)
    return {'schema_version': 1, 'owner': dict(owner), 'run_id': run_id,
            'network_name': name + '-network', 'container_name': name + '-server', 'volume_name': name + '-data',
            'subnet': SUBNET, 'gateway': GATEWAY, 'address': ADDRESS, 'port': PORT, 'authority': authority,
            'internal_network': True, 'published_ports': [], 'repository': authority + '/' + name,
            'guest_ca_directory': '/etc/docker/certs.d/' + authority,
            'container_fixture_directory': '/run/vz-registry',
            'labels': {'com.vz.registry.fixture': 'v1', 'com.vz.registry.owner': suffix},
            'network_availability_certified': False, 'machine_binding_certified': False}


def validate_tls_public(metadata, *, spec, expected, observed_unix_ns):
    """Bind public observations, not self-asserted certificate signature proof.

    `expected` fingerprints come from independently admitted certificate bytes.
    Caller must separately verify their X.509 chain and actual TLS handshake.
    Private keys, passwords, and arbitrary metadata fields are never accepted.
    """
    require(type(spec) is dict, 'resource spec object')
    require(spec == resource_spec(spec.get('owner'), spec.get('run_id')), 'source-selected resource spec')
    fields(expected, ('ca_sha256', 'certificate_sha256', 'spki_sha256'))
    for value in expected.values():
        digest(value, bare=True)
    fields(metadata, ('schema_version', 'owner', 'run_id', 'authority', 'ca_sha256', 'certificate_sha256',
                      'spki_sha256', 'issuer_ca_sha256', 'san_ips', 'san_dns', 'is_ca', 'ca_is_ca',
                      'key_usage', 'extended_key_usage', 'not_before_unix_ns', 'not_after_unix_ns'))
    require(type(metadata['schema_version']) is int and metadata['schema_version'] == 1 and
            metadata['owner'] == spec['owner'] and metadata['run_id'] == spec['run_id'] and
            metadata['authority'] == spec['authority'], 'TLS metadata owner or authority')
    require(all(metadata[key] == value for key, value in expected.items()) and
            metadata['issuer_ca_sha256'] == expected['ca_sha256'], 'TLS public pin binding')
    require(metadata['san_ips'] == [spec['address']] and metadata['san_dns'] == [] and
            metadata['is_ca'] is False and metadata['ca_is_ca'] is True,
            'TLS SAN or CA constraints')
    require(metadata['key_usage'] in (['digital_signature'], ['digital_signature', 'key_encipherment']) and
            metadata['extended_key_usage'] == ['server_auth'], 'TLS certificate usage')
    maximum = 2**63 - 1
    now = integer(observed_unix_ns, maximum)
    start = integer(metadata['not_before_unix_ns'], maximum)
    end = integer(metadata['not_after_unix_ns'], maximum)
    require(start <= now < end and end - start <= 31 * 24 * 3600 * 10**9, 'TLS fixture validity window')
    return {'schema_version': 1, 'scope': 'externally_pinned_public_TLS_metadata_only',
            'metadata_sha256': sha(canonical(metadata)), 'authority': spec['authority'],
            **expected, 'certificate_chain_verified': False, 'handshake_certified': False}


def required_source_paths():
    return [str(Path(__file__).resolve()), str(Path(artifact.__file__).resolve())]


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--layout', required=True, type=Path)
    parser.add_argument('--pin', required=True, type=Path)
    args = parser.parse_args()
    try:
        layout = args.layout if args.layout.is_absolute() else Path.cwd() / args.layout
        pin = args.pin if args.pin.is_absolute() else Path.cwd() / args.pin
        result = validate_layout(layout, pins=decode(_read(pin)))
        print(json.dumps(result, sort_keys=True))
    except (FixtureError, artifact.ArtifactError, OSError):
        parser.exit(1, 'registry fixture offline admission failed\n')
