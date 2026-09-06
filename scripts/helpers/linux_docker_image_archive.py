"""Pure, bounded verifier for the tiny image-round-trip fixture's Docker save.

Not a general TAR/OCI reader or a Machine/round-trip certification. No extraction
or dispatch occurs. Naming and compatibility records follow pinned Moby 29.7.2's
vendored containerd core/images/archive exporter.go and reference.go. Both the
canonical seed headers and that exporter's headers are admitted; blob identity
and the sole public layer payload are checked independently of those headers.
"""
import hashlib
import json
import re
import tarfile

LIMIT = 1024 * 1024
INDEX = 'application/vnd.oci.image.index.v1+json'
MANIFEST = 'application/vnd.oci.image.manifest.v1+json'
CONFIG = 'application/vnd.oci.image.config.v1+json'
LAYER = 'application/vnd.oci.image.layer.v1.tar'
CREATED = '1970-01-01T00:00:00Z'


def require(condition, code):
    if not condition:
        raise ValueError('image archive: ' + code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def fields(row, required, optional=()):
    require(type(row) is dict and set(required) <= row.keys() <=
            set(required) | set(optional), 'object fields')


def digest(value):
    require(type(value) is str and re.fullmatch(r'sha256:[0-9a-f]{64}', value),
            'digest syntax')
    return value


def _pairs(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate JSON key')
        row[key] = value
    return row


def _json(raw):
    require(len(raw) <= 65536, 'JSON byte bound')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                          parse_constant=lambda _: require(False, 'JSON constant'))
    except (UnicodeError, RecursionError, json.JSONDecodeError):
        raise ValueError('image archive: malformed JSON') from None


def _path(name):
    require(type(name) is str and len(name) <= 255 and
            re.fullmatch(r'[A-Za-z0-9_./-]+', name) and
            all(part not in ('', '.', '..') for part in name.split('/')),
            'unsafe member path')
    return name


def _tar(raw, *, outer):
    require(type(raw) is bytes and 1024 <= len(raw) <= LIMIT and len(raw) % 512 == 0,
            'TAR byte bound/alignment')
    files, directories, seen = {}, set(), set()
    offset = 0
    while offset + 512 <= len(raw):
        header = raw[offset:offset + 512]
        if header == bytes(512):
            require(offset + 1024 <= len(raw) and not any(raw[offset:]),
                    'TAR end/trailing bytes')
            return files, directories
        require(header[257:265] == tarfile.POSIX_MAGIC, 'USTAR required')
        try:
            item = tarfile.TarInfo.frombuf(header, 'ascii', 'strict')
        except (tarfile.HeaderError, UnicodeError, ValueError):
            raise ValueError('image archive: TAR header') from None
        require(item.type in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE),
                'unsupported TAR type')
        is_dir = item.type == tarfile.DIRTYPE
        name = _path(item.name[:-1] if is_dir and item.name.endswith('/') else item.name)
        require(name not in seen and len(seen) < 8, 'duplicate/excess TAR member')
        seen.add(name)
        require(item.uid == item.gid == item.mtime == 0 and not item.uname and
                not item.gname and not item.linkname and item.devmajor == item.devminor == 0,
                'TAR ownership/metadata')
        require(type(item.size) is int and 0 <= item.size <= LIMIT, 'TAR size')
        if is_dir:
            require(outer and name in ('blobs', 'blobs/sha256') and
                    item.size == 0 and item.mode == 0o755, 'TAR directory')
            directories.add(name)
        else:
            modes = (0o644, 0o444) if outer and (name == 'oci-layout' or
                                                name.startswith('blobs/sha256/')) else (0o644,)
            require(item.mode in modes, 'TAR file mode')
        start = offset + 512
        end = start + item.size
        padded = end + (-item.size % 512)
        require(padded <= len(raw) and not any(raw[end:padded]), 'TAR data/padding')
        if not is_dir:
            files[name] = raw[start:end]
        offset = padded
    raise ValueError('image archive: missing TAR end')


def _reference(value):
    require(type(value) is str and len(value) <= 255, 'reference')
    prefix = 'docker.io/library/'
    familiar = value[len(prefix):] if value.startswith(prefix) else value
    match = re.fullmatch(r'([a-z0-9]+(?:[._-][a-z0-9]+)*):([a-z0-9][a-z0-9_.-]{0,127})', familiar)
    require(match is not None and len(match[1]) <= 100, 'explicit local reference')
    return familiar, prefix + familiar, match[2]


def _platform(row):
    fields(row, ('architecture', 'os'), ('variant',))
    require(row['architecture'] == 'arm64' and row['os'] == 'linux' and
            row.get('variant', 'v8') == 'v8', 'platform')


def _descriptor(row, media, expected, content, *, indexed=False):
    fields(row, ('mediaType', 'digest', 'size'), ('annotations', 'platform') if indexed else ())
    require(row['mediaType'] == media and row['digest'] == expected and
            type(row['size']) is int and row['size'] == len(content), 'descriptor binding')
    if 'platform' in row:
        _platform(row['platform'])


def validate(raw, *, expected_manifest_digest, expected_config_digest,
             expected_layer_digest, expected_diff_id, expected_reference,
             expected_payload_path, expected_payload_sha256, expected_payload_size,
             expected_labels):
    """Validate only source-selected archive bytes; expected identities come from caller."""
    identities = tuple(digest(item) for item in (expected_manifest_digest,
                       expected_config_digest, expected_layer_digest))
    require(len(set(identities)) == 3 and digest(expected_diff_id) == expected_layer_digest,
            'single uncompressed layer identity')
    require(type(expected_payload_sha256) is str and
            re.fullmatch(r'[0-9a-f]{64}', expected_payload_sha256) and
            type(expected_payload_size) is int and 0 < expected_payload_size <= LIMIT,
            'payload expectation')
    require(type(expected_labels) is dict and 0 < len(expected_labels) <= 16 and
            all(type(key) is str and type(value) is str and len(key) <= 256 and
                len(value) <= 256 for key, value in expected_labels.items()), 'label expectation')
    require(type(expected_payload_path) is str, 'payload path expectation')
    payload_path = _path(expected_payload_path[1:] if expected_payload_path.startswith('/')
                         else expected_payload_path)
    familiar, full, tag = _reference(expected_reference)
    files, directories = _tar(raw, outer=True)
    paths = ['blobs/sha256/' + item[7:] for item in identities]
    require(set(files) == {'oci-layout', 'index.json', 'manifest.json', *paths},
            'exact archive inventory')
    require(directories in (set(), {'blobs', 'blobs/sha256'}), 'directory inventory')
    for identity, path in zip(identities, paths):
        require('sha256:' + sha(files[path]) == identity, 'blob digest')
    require(_json(files['oci-layout']) == {'imageLayoutVersion': '1.0.0'}, 'OCI layout')
    index = _json(files['index.json'])
    fields(index, ('schemaVersion', 'mediaType', 'manifests'))
    require(type(index['schemaVersion']) is int and index['schemaVersion'] == 2 and
            index['mediaType'] == INDEX and type(index['manifests']) is list and
            len(index['manifests']) == 1, 'one OCI image')
    desc = index['manifests'][0]
    _descriptor(desc, MANIFEST, identities[0], files[paths[0]], indexed=True)
    require(desc.get('annotations') == {'io.containerd.image.name': full,
            'org.opencontainers.image.ref.name': tag}, 'index naming annotations')
    manifest = _json(files[paths[0]])
    fields(manifest, ('schemaVersion', 'mediaType', 'config', 'layers'))
    require(type(manifest['schemaVersion']) is int and manifest['schemaVersion'] == 2 and
            manifest['mediaType'] == MANIFEST and type(manifest['layers']) is list and
            len(manifest['layers']) == 1, 'manifest schema/layers')
    _descriptor(manifest['config'], CONFIG, identities[1], files[paths[1]])
    _descriptor(manifest['layers'][0], LAYER, identities[2], files[paths[2]])
    config = _json(files[paths[1]])
    fields(config, ('architecture', 'os', 'created', 'config', 'rootfs', 'history'), ('variant',))
    _platform({key: config[key] for key in ('architecture', 'os', 'variant') if key in config})
    require(config['created'] == CREATED and config['config'] ==
            {'Labels': expected_labels, 'WorkingDir': '/'}, 'scratch config')
    require(config['rootfs'] == {'type': 'layers', 'diff_ids': [expected_diff_id]}, 'rootfs')
    history = config['history']
    require(type(history) is list and len(history) == 1, 'history count')
    fields(history[0], ('created', 'created_by'))
    require(history[0]['created'] == CREATED and type(history[0]['created_by']) is str and
            history[0]['created_by'] == 'vz04-image-fixture-v1 ' +
            expected_labels.get('com.vz.fixture.role', ''), 'fixture history')
    compatible = _json(files['manifest.json'])
    require(compatible == [{'Config': paths[1], 'RepoTags': [familiar],
                            'Layers': [paths[2]]}], 'Docker compatibility manifest')
    payloads, layer_directories = _tar(files[paths[2]], outer=False)
    require(not layer_directories and set(payloads) == {payload_path}, 'one layer payload')
    payload = payloads[payload_path]
    require(len(payload) == expected_payload_size and sha(payload) == expected_payload_sha256,
            'payload content')
    return {'schema_version': 1, 'scope': 'docker_save_scratch_archive_bytes_only',
            'archive_sha256': sha(raw), 'archive_bytes': len(raw),
            'regular_members': len(files), 'directory_members': len(directories),
            'manifest_digest': identities[0], 'config_digest': identities[1],
            'layer_digest': identities[2], 'diff_id': expected_diff_id,
            'reference': familiar, 'payload_sha256': sha(payload),
            'payload_bytes': len(payload), 'machine_binding_certified': False,
            'docker_round_trip_certified': False}
