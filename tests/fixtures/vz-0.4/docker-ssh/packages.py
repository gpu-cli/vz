"""Extract exact authenticated DEB bytes; never execute maintainer scripts."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from ssh_probe import capture, emit, read_regular, require, unique

DPKG = '/usr/bin/dpkg-deb'
LIMIT = 16 * 1024 * 1024
ALIASES = {'/bin': 'usr/bin', '/sbin': 'usr/sbin', '/lib': 'usr/lib',
           '/usr/lib/ld-linux-aarch64.so.1': 'aarch64-linux-gnu/ld-linux-aarch64.so.1'}


def extraction_contract(pins):
    selected = pins['extraction']
    require(type(selected) is dict and set(selected) == {'tar', 'loader', 'aliases'} and
            selected['aliases'] == ALIASES)
    for role, path in [('tar', '/usr/bin/tar'), ('loader', '/usr/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1')]:
        row = selected[role]
        require(type(row) is dict and set(row) == {'path', 'sha256', 'size'} and row['path'] == path and
                type(row['size']) is int and 0 < row['size'] <= LIMIT and
                type(row['sha256']) is str and re.fullmatch(r'[0-9a-f]{64}', row['sha256']))


def runtime_guard(pins):
    """Never repair or follow an unauthenticated merged-/usr alias."""
    extraction_contract(pins)
    proof = {}
    for path, target in ALIASES.items():
        info = os.lstat(path)
        require(stat.S_ISLNK(info.st_mode) and info.st_uid == info.st_gid == 0 and
                stat.S_IMODE(info.st_mode) == 0o777 and os.readlink(path) == target)
        proof[path] = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid, target)
    tools = [{'path': DPKG, 'sha256': pins['dpkg_deb_sha256']},
             pins['extraction']['tar'], pins['extraction']['loader']]
    for row in tools:
        info = os.lstat(row['path'])
        require(stat.S_ISREG(info.st_mode) and info.st_uid == info.st_gid == 0 and
                stat.S_IMODE(info.st_mode) == 0o755)
        data = read_regular(row['path'], LIMIT)
        require(hashlib.sha256(data).hexdigest() == row['sha256'] and
                ('size' not in row or len(data) == row['size']))
        proof[row['path']] = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
                              len(data), row['sha256'])
    return proof


def admit(directory, pins_path):
    pins_raw = read_regular(pins_path, 16384)
    pins = json.loads(pins_raw, object_pairs_hook=unique)
    supplied = json.loads(read_regular(directory / 'manifest.json', 16384), object_pairs_hook=unique)
    require(type(supplied) is dict and json.dumps(supplied, sort_keys=True) == json.dumps(pins, sort_keys=True) and
            set(pins) == {'schema_version', 'dpkg_deb_sha256', 'packages', 'extraction'} and
            type(pins['schema_version']) is int and pins['schema_version'] == 1 and
            type(pins['packages']) is list and len(pins['packages']) == 8 and
            type(pins['dpkg_deb_sha256']) is str and re.fullmatch(r'[0-9a-f]{64}', pins['dpkg_deb_sha256']))
    require(stat.S_ISDIR(directory.lstat().st_mode))
    expected = {'manifest.json'}
    names = []
    for row in pins['packages']:
        require(type(row) is dict and set(row) == {
            'filename', 'package', 'version', 'architecture', 'sha256', 'size'})
        name = row['filename']
        require(type(name) is str and re.fullmatch(r'[A-Za-z0-9+._~%-]+_arm64\.deb', name) and
                Path(name).name == name and
                name not in expected and row['architecture'] == 'arm64' and
                type(row['size']) is int and 0 < row['size'] <= LIMIT and
                type(row['package']) is str and re.fullmatch(r'[a-z0-9][a-z0-9+.-]*', row['package']) and
                row['package'] not in names and type(row['version']) is str and
                re.fullmatch(r'[0-9][A-Za-z0-9.+:~\-]*', row['version']) and
                type(row['sha256']) is str and re.fullmatch(r'[0-9a-f]{64}', row['sha256']))
        names.append(row['package'])
        expected.add(name)
        data = read_regular(directory / name, LIMIT)
        require(len(data) == row['size'] and hashlib.sha256(data).hexdigest() == row['sha256'])
    require(names == sorted(names))
    seen = set()
    with os.scandir(directory) as entries:
        for entry in entries:
            require(len(seen) < 9)
            seen.add(entry.name)
    require(seen == expected)
    runtime_guard(pins)
    return pins, hashlib.sha256(pins_raw).hexdigest()


def extract(directory=Path('/fixture/packages'), pins_path=Path('/fixture/package-pins.json'), runner=capture):
    # All inputs and the base-owned extractor are checked before the first write.
    pins, pins_sha = admit(directory, pins_path)
    admitted_runtime = runtime_guard(pins)
    for row in pins['packages']:
        require(runtime_guard(pins) == admitted_runtime)
        code, archive, stderr = runner([DPKG, '--fsys-tarfile', str(directory / row['filename'])], stdout_limit=LIMIT)
        require(type(archive) is bytes and 0 < len(archive) <= LIMIT and type(code) is int and code == 0 and stderr == b'')
        require(runtime_guard(pins) == admitted_runtime)
        # dpkg-deb --extract unsets TAR_OPTIONS and replaces /lib's symlink.
        # Spool bounded data, then invoke authenticated GNU tar explicitly.
        spool_root = Path(tempfile.mkdtemp(prefix='vz-ssh-package-', dir='/tmp'))
        spool = spool_root / 'data.tar'
        with spool.open('xb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(archive); stream.flush(); os.fsync(stream.fileno())
        require(read_regular(spool, LIMIT) == archive)
        # If capture raises (including unproven reaping), retain this private
        # spool until the owning Machine is stopped; never remove live inputs.
        code, stdout, stderr = runner([pins['extraction']['tar']['path'], '--extract', '--preserve-permissions',
            '--file', str(spool), '--directory', '/', '--keep-directory-symlink', '--warning=no-timestamp'])
        require(read_regular(spool, LIMIT) == archive)
        spool.unlink(); spool_root.rmdir()
        require(type(code) is int and code == 0 and stdout == stderr == b'')
        require(runtime_guard(pins) == admitted_runtime)
        require(hashlib.sha256(read_regular(directory / row['filename'], LIMIT)).hexdigest() == row['sha256'])
    emit({'schema_version': 1, 'type': 'openssh_packages_extracted',
          'package_pins_sha256': pins_sha, 'packages': [row['package'] for row in pins['packages']],
          'maintainer_scripts_executed': False})


if __name__ == '__main__':
    try:
        extract()
    except Exception:
        emit({'schema_version': 1, 'type': 'openssh_package_error', 'outcome': 'operational_failure'})
        raise SystemExit(70)
