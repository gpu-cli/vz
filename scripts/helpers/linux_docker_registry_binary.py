"""Read-only identity/build-info admission of the pinned ARM64 registry binary.

The binary is read from admitted OCI layer streams into bounded memory: never
extracted or executed. This verifies compiled metadata, not source reproducibility
or runtime log authenticity. Inline Go build-info format follows Go's
src/debug/buildinfo/buildinfo.go (32-byte header, two uvarint-prefixed strings).
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import tarfile

import linux_docker_registry_fixture as fixture

BINARY_SHA256 = '669f0d9892da6ccd44a40954f39a3b929f4455d7ed02a806828346feac572834'
BINARY_SIZE = 50331832
LAYER_DIGEST = 'sha256:bfa447a3f3472696dd72ce5d846c0a3320427a4b66a4d145c0f75b3b6b9efb8a'
GO_VERSION = 'go1.25.9'
MODULE_PATH = 'github.com/distribution/distribution/v3'
MODULE_VERSION = 'v3.1.1'
MAIN_PATH = MODULE_PATH + '/cmd/registry'
VCS_REVISION = '9a8d98b679740cd514aa7e7d84d23d442a5ef54c'
BUILD = {'-buildmode': 'exe', '-compiler': 'gc', '-trimpath': 'true',
         'CGO_ENABLED': '0', 'GOARCH': 'arm64', 'GOOS': 'linux', 'GOARM64': 'v8.0',
         'vcs': 'git', 'vcs.revision': VCS_REVISION,
         'vcs.time': '2026-05-01T15:28:51Z', 'vcs.modified': 'false'}
MAGIC = b'\xff Go buildinf:'
START = bytes.fromhex('3077af0c9274080241e1c107e6d618e6')
END = bytes.fromhex('f932433186182072008242104116d8f2')
LIMIT = 64 * 1024 * 1024


class BinaryError(ValueError):
    """Fixed diagnostics: no arbitrary binary or path content."""


def require(condition, code):
    if not condition:
        raise BinaryError('registry binary: ' + code)


def _string(raw, position):
    size, start = 0, position
    for shift in range(0, 70, 7):
        require(position < len(raw), 'truncated build-info length')
        byte = raw[position]
        position += 1
        size |= (byte & 127) << shift
        if byte < 128:
            require(position - start == 1 or byte != 0, 'noncanonical build-info length')
            break
    else:
        raise BinaryError('registry binary: overflowing build-info length')
    require(0 < size <= 1024 * 1024 and position + size <= len(raw), 'build-info string bounds')
    return raw[position:position + size], position + size


def _inline(raw):
    require(type(raw) is bytes and 64 <= len(raw) <= LIMIT, 'binary bounds')
    require(raw[:6] == b'\x7fELF\x02\x01' and struct.unpack_from('<H', raw, 18)[0] == 183,
            'ELF64 little-endian ARM64 required')
    positions, start = [], 0
    while True:
        position = raw.find(MAGIC, start)
        if position < 0:
            break
        start = position + 1
        if position % 16 == 0:
            positions.append(position)
    require(len(positions) == 1, 'unique aligned Go build-info header')
    position = positions[0]
    require(position + 32 <= len(raw) and raw[position + 14] == 8 and
            raw[position + 15] == 2 and raw[position + 16:position + 32] == bytes(16),
            'inline little-endian Go build-info header')
    version, position = _string(raw, position + 32)
    framed, _ = _string(raw, position)
    require(framed.startswith(START) and framed.endswith(END) and framed[-17:-16] == b'\n',
            'module framing')
    try:
        version = version.decode('ascii')
        lines = framed[16:-16].decode('utf-8').splitlines()
    except UnicodeError:
        raise BinaryError('registry binary: build-info encoding') from None
    require(1 <= len(lines) <= 4096, 'module line bounds')
    paths, modules, build = [], [], {}
    for line in lines:
        require(0 < len(line) <= 16384, 'module line bounds')
        fields = line.split('\t')
        if fields[0] == 'path':
            require(len(fields) == 2, 'main path fields')
            paths.append(fields[1])
        elif fields[0] == 'mod':
            require(len(fields) == 4, 'main module fields')
            modules.append(fields[1:])
        elif fields[0] == 'build':
            require(len(fields) == 2 and '=' in fields[1], 'build setting fields')
            key, value = fields[1].split('=', 1)
            require(key not in build, 'duplicate build setting')
            build[key] = value
        else:
            require(fields[0] == 'dep' and len(fields) == 4, 'dependency metadata fields')
    require(len(paths) == len(modules) == 1, 'one main module and path')
    return {'go_version': version, 'main_path': paths[0], 'module_path': modules[0][0],
            'module_version': modules[0][1], 'module_sum': modules[0][2], 'build_settings': build}


def validate_binary(raw):
    require(type(raw) is bytes and len(raw) == BINARY_SIZE and
            hashlib.sha256(raw).hexdigest() == BINARY_SHA256, 'exact compiled binary pin')
    parsed = _inline(raw)
    require(parsed == {'go_version': GO_VERSION, 'main_path': MAIN_PATH, 'module_path': MODULE_PATH,
        'module_version': MODULE_VERSION, 'module_sum': '', 'build_settings': BUILD}, 'compiled metadata pins')
    return {'schema_version': 1, 'scope': 'pinned_unexecuted_registry_binary_metadata_only',
            'binary_sha256': BINARY_SHA256, 'binary_size': BINARY_SIZE, **parsed,
            'binary_executed': False, 'reproducible_build_certified': False,
            'registry_execution_certified': False}


def validate_layout_binary(layout, *, pins):
    """Bind exact compiled bytes to all admitted layers, including overrides."""
    try:
        layout = Path(layout)
        before = fixture.validate_layout(layout, pins=pins)
        found = []
        for layer in pins['layers']:
            with fixture.artifact._opened(layout / fixture.blob_path(layer['digest'])) as fd:
                import os
                with os.fdopen(os.dup(fd), 'rb') as stream:
                    with tarfile.open(fileobj=stream, mode='r|gz') as archive:
                        for member in archive:
                            name = member.name
                            while name.startswith('./'):
                                name = name[2:]
                            require(name not in ('.wh.bin', 'bin/.wh.registry', 'bin/.wh..wh..opq', '.wh..wh..opq'),
                                    'binary directory whiteout')
                            if name.rstrip('/') == 'bin':
                                require(member.isdir(), 'binary directory replacement')
                            if name != 'bin/registry':
                                continue
                            require(member.isreg() and member.size == BINARY_SIZE and
                                    layer['digest'] == LAYER_DIGEST, 'exact binary layer member')
                            source = archive.extractfile(member)
                            require(source is not None, 'binary stream unavailable')
                            raw = source.read(BINARY_SIZE + 1)
                            found.append(validate_binary(raw))
        require(len(found) == 1, 'one binary and no layer replacement')
        require(fixture.validate_layout(layout, pins=pins) == before, 'layout changed during binary admission')
        return {**found[0], 'layer_digest': LAYER_DIGEST, 'member': 'bin/registry',
                'layout_inventory_sha256': before['inventory']['inventory_sha256'],
                'pins_sha256': before['pins_sha256']}
    except (fixture.FixtureError, fixture.artifact.ArtifactError, OSError, tarfile.TarError, EOFError):
        raise BinaryError('registry binary: unsafe or inconsistent source layout') from None


def required_source_paths():
    return sorted({str(Path(__file__).resolve()), *fixture.required_source_paths()})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--layout', type=Path, required=True)
    parser.add_argument('--pin', type=Path, required=True)
    args = parser.parse_args()
    try:
        layout = args.layout if args.layout.is_absolute() else Path.cwd() / args.layout
        pin = args.pin if args.pin.is_absolute() else Path.cwd() / args.pin
        print(json.dumps(validate_layout_binary(layout, pins=fixture.decode(fixture._read(pin))), sort_keys=True))
    except (BinaryError, fixture.FixtureError, fixture.artifact.ArtifactError, OSError):
        parser.exit(1, 'registry binary offline admission failed\n')
