"""Extract exact authenticated DEB bytes; never execute maintainer scripts."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from ssh_probe import capture, emit, read_regular, require, unique

DPKG = '/usr/bin/dpkg-deb'
LIMIT = 32 * 1024 * 1024


def admit(directory, pins_path):
    pins_raw = read_regular(pins_path, 16384)
    pins = json.loads(pins_raw, object_pairs_hook=unique)
    supplied = json.loads(read_regular(directory / 'manifest.json', 16384), object_pairs_hook=unique)
    require(type(supplied) is dict and json.dumps(supplied, sort_keys=True) == json.dumps(pins, sort_keys=True) and
            set(pins) == {'schema_version', 'dpkg_deb_sha256', 'packages'} and
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
    require(hashlib.sha256(read_regular(DPKG, LIMIT)).hexdigest() == pins['dpkg_deb_sha256'])
    require(stat.S_IMODE(os.lstat(DPKG).st_mode) == 0o755)
    return pins, hashlib.sha256(pins_raw).hexdigest()


def extract(directory=Path('/fixture/packages'), pins_path=Path('/fixture/package-pins.json'), runner=capture):
    # All inputs and the base-owned extractor are checked before the first write.
    pins, pins_sha = admit(directory, pins_path)
    for row in pins['packages']:
        code, stdout, stderr = runner([DPKG, '--extract', str(directory / row['filename']), '/'])
        require(code == 0 and stdout == stderr == b'')
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
