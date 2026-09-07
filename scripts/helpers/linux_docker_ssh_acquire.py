"""Fetch the pinned Debian inputs the SSH suite verifies, by digest.

`linux_docker_ssh_input.verify` already performs the whole trust chain offline
— a real `gpgv` check of the Release, then every package descriptor against the
signed index — but it requires the bytes to already be on disk, and nothing put
them there. The result was an input directory made by hand, which a fresh
checkout does not have and the gate therefore could not name.

This fetches them. Every file is named by the pin, bounded before it is read,
and rejected unless its size and SHA-256 match exactly; a mismatch is a refusal,
never a warning. Rows the pin gives no `repository_path` are deliberately not
fetched here: they are records of the admission that produced the pin, and
guessing where they came from is the mutable-source hazard the pin exists to
prevent.

Nothing is executed, unpacked or installed. Acquisition is not verification:
`verify` remains the only thing that decides these bytes are trustworthy.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT = 30
DEADLINE = 300
MAX_REDIRECTS = 5
USER_AGENT = 'vz-public-debian-input/1'


def require(value, code):
    if not value:
        raise ValueError('ssh input acquisition: ' + code)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


class PublicSnapshot:
    """Bounded reads from one pinned snapshot host, no proxy, no ambient state.

    snapshot.debian.org answers these paths with a redirect and serves the bytes
    from the target, so redirects are followed — but only within the snapshot
    host, and only a bounded number of times. The registry transport refuses
    redirects outside blob fetches; this is the same discipline for a different
    server, not a copy of it.
    """

    def __init__(self, base_url):
        parsed = urllib.parse.urlsplit(base_url)
        require(parsed.scheme == 'https' and parsed.hostname, 'snapshot base URL')
        self.base_url = base_url if base_url.endswith('/') else base_url + '/'
        self.host = parsed.hostname
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=ssl.create_default_context()))

    def __repr__(self):
        return '<PublicSnapshot %s>' % self.host

    def get(self, repository_path, *, limit):
        require(isinstance(repository_path, str) and repository_path
                and not repository_path.startswith('/')
                and '..' not in repository_path.split('/'), 'repository path')
        require(isinstance(limit, int) and 0 < limit <= 64 * 1024 * 1024, 'response bound')
        url = urllib.parse.urljoin(self.base_url, repository_path)
        deadline = time.monotonic() + DEADLINE
        for _ in range(MAX_REDIRECTS):
            require(time.monotonic() < deadline, 'public request deadline')
            require(urllib.parse.urlsplit(url).hostname == self.host, 'redirect left the snapshot host')
            request = urllib.request.Request(url, headers={
                'User-Agent': USER_AGENT, 'Accept-Encoding': 'identity'})
            try:
                response = self.opener.open(request, timeout=TIMEOUT)
            except urllib.error.HTTPError as error:
                try:
                    if error.code in (301, 302, 303, 307, 308):
                        location = error.headers.get('Location')
                        require(isinstance(location, str), 'redirect location missing')
                        url = urllib.parse.urljoin(url, location)
                        continue
                    raise ValueError('ssh input acquisition: HTTP request rejected') from None
                finally:
                    error.close()
            except (OSError, urllib.error.URLError):
                raise ValueError('ssh input acquisition: public transport failed') from None
            with response:
                require(response.status == 200
                        and response.headers.get('Content-Encoding', 'identity') == 'identity',
                        'HTTP response shape')
                raw = response.read(limit + 1)
            require(len(raw) <= limit, 'response exceeds its pinned bound')
            return raw
        raise ValueError('ssh input acquisition: too many redirects')


def fetchable(pin):
    """Every pinned row that says where it came from, as (row, limit).

    A row without a `repository_path` is retained provenance, not an input to
    fetch, and is skipped rather than guessed at.
    """
    bounds = pin['bounds']
    rows = [(pin['release'], bounds['release_bytes']),
            (dict(pin['packages_index'],
                  repository_path='dists/%s/%s' % (pin['snapshot']['suite'],
                                                   pin['packages_index']['release_path'])),
             bounds['packages_compressed_bytes'])]
    rows += [(row, bounds['deb_bytes_each']) for row in pin['packages']]
    rows += [(row, bounds['packages_compressed_bytes']) for row in pin['source_proofs']
             if row.get('repository_path')]
    return rows


def acquire(destination, pin, *, transport=None):
    """Write every fetchable pinned input into a fresh directory.

    Returns the filenames written. Files the pin retains as provenance are not
    written here; the caller supplies them.
    """
    destination = Path(destination)
    require(destination.is_absolute() and destination.parent == destination.parent.resolve()
            and not destination.exists() and not destination.is_symlink(),
            'fresh canonical destination required')
    transport = transport or PublicSnapshot(pin['snapshot']['base_url'])
    rows = fetchable(pin)
    require(rows, 'pin names nothing fetchable')
    destination.mkdir(mode=0o700)
    written = []
    for row, limit in rows:
        name = row['filename']
        require(Path(name).name == name and name not in ('.', '..'), 'input filename must be local')
        raw = transport.get(row['repository_path'], limit=limit)
        require(len(raw) == row['size'], 'fetched %s has the wrong size' % name)
        require(sha256(raw) == row['sha256'], 'fetched %s has the wrong digest' % name)
        with (destination / name).open('xb') as stream:
            stream.write(raw)
        written.append(name)
    return written
