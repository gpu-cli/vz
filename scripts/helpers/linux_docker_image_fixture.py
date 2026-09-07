"""Deterministic tiny OCI subject/decoy seeds, with Docker-save compatibility.

No clock, environment, random input, file I/O, extraction or Docker dispatch.
Only index naming annotations and manifest.json RepoTags depend on reference.
Naming follows containerd's archive exporter: full io.containerd.image.name,
tag-only org.opencontainers.image.ref.name, and familiar Docker RepoTags.
See pinned BuildKit v0.19.0 vendor/containerd/v2/core/images/archive/
exporter.go addNameAnnotation/dockerManifest and reference.go ociReferenceName.
The seed has data only; it is not a runnable OS or a Docker-load certification.
"""
import hashlib
import json
import re
import tarfile

LIMIT = 1024 * 1024
CREATED = '1970-01-01T00:00:00Z'
INDEX_TYPE = 'application/vnd.oci.image.index.v1+json'
MANIFEST_TYPE = 'application/vnd.oci.image.manifest.v1+json'
CONFIG_TYPE = 'application/vnd.oci.image.config.v1+json'
LAYER_TYPE = 'application/vnd.oci.image.layer.v1.tar'
PLATFORM = {'architecture': 'arm64', 'os': 'linux'}
ROLES = ('subject', 'decoy')
PREFIX = 'docker.io/library/'


def require(value, reason):
    if not value:
        raise ValueError('image fixture: ' + reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True,
                      allow_nan=False).encode('ascii') + b'\n'


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def reference_parts(reference):
    """Narrow local-test lane: explicit docker.io/library repository and tag."""
    require(type(reference) is str and len(reference) <= 255 and reference.startswith(PREFIX),
            'explicit canonical reference required')
    remainder = reference[len(PREFIX):]
    match = re.fullmatch(r'([a-z0-9]+(?:[._-][a-z0-9]+)*):([a-z0-9][a-z0-9_.-]{0,127})', remainder)
    require(match is not None and len(match[1]) <= 100, 'bounded repository and explicit tag required')
    return match[1], match[2]


def familiar_reference(reference):
    repository, tag = reference_parts(reference)
    return repository + ':' + tag


def _tar(entries):
    """Source-selected regular USTAR entries, 512-byte alignment, two EOF blocks."""
    output = bytearray()
    require(type(entries) is dict and 0 < len(entries) <= 6, 'bounded tar inventory')
    for name, content in sorted(entries.items()):
        require(type(name) is str and re.fullmatch(r'[a-z0-9][a-z0-9./_-]{0,99}', name) and
                all(part not in ('', '.', '..') for part in name.split('/')) and
                type(content) is bytes and len(content) <= LIMIT, 'source tar member')
        header = tarfile.TarInfo(name)
        header.type = tarfile.REGTYPE
        header.mode = 0o644
        header.uid = header.gid = header.mtime = 0
        header.uname = header.gname = ''
        header.linkname = ''
        header.size = len(content)
        output.extend(header.tobuf(format=tarfile.USTAR_FORMAT, encoding='ascii', errors='strict'))
        output.extend(content)
        output.extend(b'\0' * (-len(content) % 512))
        require(len(output) + 1024 <= LIMIT, 'tar byte limit')
    output.extend(b'\0' * 1024)
    return bytes(output)


def _descriptor(media_type, raw):
    return {'mediaType': media_type, 'digest': 'sha256:' + sha256(raw), 'size': len(raw)}


def _content(role):
    require(type(role) is str and role in ROLES, 'unknown role')
    payload = ('vz04-image-fixture-v1\nrole=' + role + '\n').encode('ascii')
    layer = _tar({'payload.txt': payload})
    layer_descriptor = _descriptor(LAYER_TYPE, layer)
    labels = {'com.vz.fixture': 'docker-image-v1', 'com.vz.fixture.role': role}
    config = {'architecture': 'arm64', 'os': 'linux', 'created': CREATED,
              'config': {'Labels': labels, 'WorkingDir': '/'},
              'rootfs': {'type': 'layers', 'diff_ids': [layer_descriptor['digest']]},
              'history': [{'created': CREATED, 'created_by': 'vz04-image-fixture-v1 ' + role}]}
    config_raw = canonical(config)
    config_descriptor = _descriptor(CONFIG_TYPE, config_raw)
    manifest = {'schemaVersion': 2, 'mediaType': MANIFEST_TYPE,
                'config': config_descriptor, 'layers': [layer_descriptor]}
    manifest_raw = canonical(manifest)
    manifest_descriptor = _descriptor(MANIFEST_TYPE, manifest_raw)
    identity = {'schema_version': 1, 'role': role, 'platform': dict(PLATFORM), 'created': CREATED,
                'labels': dict(labels), 'payload': {'path': '/payload.txt', 'tar_path': 'payload.txt',
                    'sha256': sha256(payload), 'size': len(payload), 'mode': 0o644, 'uid': 0, 'gid': 0},
                'manifest_digest': manifest_descriptor['digest'], 'manifest_size': len(manifest_raw),
                'config_digest': config_descriptor['digest'], 'config_size': len(config_raw),
                'layer_digest': layer_descriptor['digest'], 'layer_size': len(layer),
                'diff_id': layer_descriptor['digest'], 'config': config, 'manifest': manifest}
    blobs = {'blobs/sha256/' + sha256(raw): raw for raw in (manifest_raw, config_raw, layer)}
    require(len(blobs) == 3, 'distinct content blobs required')
    return identity, blobs


def fixture(role):
    """Fresh JSON-compatible expected content identity, independent of tag."""
    return _content(role)[0]


def archive(role, reference, extra_annotations=None):
    """Return <=1MiB OCI-layout TAR; no mutable source files are consulted.

    extra_annotations models store-added index annotations (containerd's
    distribution-source label after a registry pull); default is naming only.
    """
    _, tag = reference_parts(reference)
    identity, blobs = _content(role)
    descriptor = {'mediaType': MANIFEST_TYPE, 'digest': identity['manifest_digest'],
                  'size': identity['manifest_size'], 'platform': dict(PLATFORM),
                  'annotations': {'io.containerd.image.name': reference,
                                  'org.opencontainers.image.ref.name': tag, **(extra_annotations or {})}}
    index = {'schemaVersion': 2, 'mediaType': INDEX_TYPE, 'manifests': [descriptor]}
    compatible = [{'Config': 'blobs/sha256/' + identity['config_digest'][7:],
                   'RepoTags': [familiar_reference(reference)],
                   'Layers': ['blobs/sha256/' + identity['layer_digest'][7:]]}]
    entries = dict(blobs, **{'oci-layout': canonical({'imageLayoutVersion': '1.0.0'}),
                            'index.json': canonical(index), 'manifest.json': canonical(compatible)})
    return _tar(entries)
