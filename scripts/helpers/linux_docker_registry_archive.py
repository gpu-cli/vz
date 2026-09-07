"""Deterministic load-input TAR from an independently admitted OCI layout.

Only sorted regular USTAR members are emitted, with fixed public metadata and
zero padding. No extraction or Docker dispatch. Failed creation retains its
partial file without authorizing load; callers must retain a successful proof.
Existing destinations and outputs inside the input layout are refused.
"""
import copy
import hashlib
import os
from pathlib import Path
import stat
import tarfile

import linux_docker_registry_fixture as fixture

CHUNK = 65536
ARCHIVE_LIMIT = fixture.TREE_LIMIT + 65536


class ArchiveError(ValueError):
    """Fixed diagnostics, never raw input content."""


def require(condition, code):
    if not condition:
        raise ArchiveError('registry archive: ' + code)


def header(row):
    item = tarfile.TarInfo(row['path'])
    item.type, item.size, item.mode = tarfile.REGTYPE, row['size'], 0o644
    item.uid = item.gid = item.mtime = 0
    item.uname = item.gname = item.linkname = ''
    return item.tobuf(format=tarfile.USTAR_FORMAT, encoding='ascii', errors='strict')


def _chunks(layout, admission):
    """Recheck each actual source stream against the admitted file hash."""
    for row in admission['inventory']['files']:
        yield header(row)
        digest, count = hashlib.sha256(), 0
        with fixture.artifact._opened(layout / row['path']) as fd:
            require(os.fstat(fd).st_size == row['size'], 'source size changed')
            while True:
                raw = os.read(fd, min(CHUNK, row['size'] + 1 - count))
                if not raw:
                    break
                count += len(raw)
                require(count <= row['size'], 'source size changed')
                digest.update(raw)
                yield raw
        require(count == row['size'] and digest.hexdigest() == row['sha256'], 'source content changed')
        padding = -row['size'] % 512
        if padding:
            yield bytes(padding)
    yield bytes(1024)


def _proof(admission, size, digest):
    return {'schema_version': 1, 'scope': 'deterministic_pinned_registry_OCI_load_input_only',
            'archive_sha256': digest, 'archive_bytes': size,
            'regular_members': len(admission['inventory']['files']),
            'layout_inventory_sha256': admission['inventory']['inventory_sha256'],
            'pins_sha256': admission['pins_sha256'], 'manifest_digest': admission['manifest_digest'],
            'reference': admission['reference'], 'docker_load_certified': False,
            'registry_execution_certified': False}


def validate_archive(path, *, layout, pins):
    """Read-only exact-byte replay against the same external source pins."""
    try:
        layout, pins = Path(layout), copy.deepcopy(pins)
        before = fixture.validate_layout(layout, pins=pins)
        digest, total = hashlib.sha256(), 0
        with fixture.artifact._opened(path) as fd:
            require(os.fstat(fd).st_size <= ARCHIVE_LIMIT, 'archive size bound')
            for expected in _chunks(layout, before):
                actual = bytearray()
                while len(actual) < len(expected):
                    block = os.read(fd, len(expected) - len(actual))
                    require(bool(block), 'truncated archive')
                    actual.extend(block)
                require(actual == expected, 'archive differs from canonical pinned input')
                digest.update(actual)
                total += len(actual)
                require(total <= ARCHIVE_LIMIT, 'archive size bound')
            require(not os.read(fd, 1) and os.fstat(fd).st_size == total, 'archive trailing bytes')
        require(fixture.validate_layout(layout, pins=pins) == before, 'layout changed during replay')
        return _proof(before, total, digest.hexdigest())
    except (fixture.FixtureError, fixture.artifact.ArtifactError, OSError, UnicodeError, tarfile.TarError):
        raise ArchiveError('registry archive: unsafe or inconsistent input') from None


def create_archive(layout, *, pins, output):
    """Create one fresh bounded output and independently replay it before return."""
    parent_fd = None
    try:
        layout, pins, output = Path(layout), copy.deepcopy(pins), Path(output)
        require(output.is_absolute() and output.parent.resolve(strict=True) == output.parent,
                'canonical absolute destination required')
        require(output != layout and layout not in output.parents, 'destination inside input layout')
        before = fixture.validate_layout(layout, pins=pins)
        parent_fd = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        parent_info = os.fstat(parent_fd)
        require(parent_info.st_uid == os.geteuid() and not stat.S_IMODE(parent_info.st_mode) & 0o022,
                'destination parent ownership')
        identity = lambda item: (item.st_dev, item.st_ino, item.st_mode, item.st_uid, item.st_gid)
        fd = os.open(output.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=parent_fd)
        digest, total = hashlib.sha256(), 0
        with os.fdopen(fd, 'wb') as stream:
            output_info = os.fstat(stream.fileno())
            for raw in _chunks(layout, before):
                total += len(raw)
                require(total <= ARCHIVE_LIMIT, 'archive size bound')
                stream.write(raw)
                digest.update(raw)
            stream.flush()
            os.fsync(stream.fileno())
            require(identity(os.stat(output.name, dir_fd=parent_fd, follow_symlinks=False)) == identity(output_info)
                    and os.fstat(stream.fileno()).st_nlink == 1, 'destination replaced')
        os.fsync(parent_fd)
        require(identity(output.parent.stat()) == identity(parent_info), 'destination parent replaced')
        require(fixture.validate_layout(layout, pins=pins) == before, 'layout changed during creation')
        observed = validate_archive(output, layout=layout, pins=pins)
        require(observed == _proof(before, total, digest.hexdigest()), 'retained archive replay differs')
        require(identity(os.stat(output.name, dir_fd=parent_fd, follow_symlinks=False)) == identity(output_info)
                and identity(output.parent.stat()) == identity(parent_info), 'destination changed after replay')
        return observed
    except (fixture.FixtureError, fixture.artifact.ArtifactError, OSError, UnicodeError, tarfile.TarError):
        raise ArchiveError('registry archive: creation failed; any partial output retained') from None
    finally:
        if parent_fd is not None:
            os.close(parent_fd)


def required_source_paths():
    return [str(Path(__file__).resolve()), *fixture.required_source_paths()]


if __name__ == '__main__':
    import argparse
    import json
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--layout', required=True, type=Path)
    parser.add_argument('--pin', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    try:
        absolute = lambda path: path if path.is_absolute() else Path.cwd() / path
        pin = fixture.decode(fixture._read(absolute(args.pin)))
        options = {'layout': absolute(args.layout), 'pins': pin}
        result = (validate_archive(absolute(args.output), **options) if args.verify_only else
                  create_archive(output=absolute(args.output), **options))
        print(json.dumps(result, sort_keys=True))
    except (ArchiveError, fixture.FixtureError, fixture.artifact.ArtifactError, OSError):
        parser.exit(1, 'registry archive operation failed; any partial output retained\n')
