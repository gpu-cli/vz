"""Acquire public Distribution inputs without Docker, user auth or proxy state.

Explicit maintainer resolution freezes a version tag once; replay fetches only
the supplied immutable descriptors. This is input acquisition, not a registry
compatibility test. Anonymous pull tokens and signed redirect URLs stay private.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

import linux_docker_registry_fixture as fixture

REPOSITORY = 'docker.io/library/registry'
VERSION = '3.1.1'
REGISTRY = 'https://registry-1.docker.io/v2/library/registry/'
TOKEN = 'https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/registry:pull'
INDEX = 'application/vnd.oci.image.index.v1+json'
MANIFEST = 'application/vnd.oci.image.manifest.v1+json'
ACCEPT = ', '.join((INDEX, MANIFEST, 'application/vnd.docker.distribution.manifest.list.v2+json',
                    'application/vnd.docker.distribution.manifest.v2+json'))
METADATA_LIMIT = 1024 * 1024
BLOB_LIMIT = 128 * 1024 * 1024
TOTAL_LIMIT = 256 * 1024 * 1024


def require(value, code):
    if not value:
        raise ValueError('registry acquisition: ' + code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True,
                      allow_nan=False).encode('ascii') + b'\n'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def parse(raw):
    require(type(raw) is bytes and len(raw) <= METADATA_LIMIT, 'metadata byte limit')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                          parse_constant=lambda _: require(False, 'JSON constant'))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError('registry acquisition: malformed metadata') from None


def descriptor(row):
    require(type(row) is dict and {'mediaType', 'digest', 'size'} <= row.keys() and
            row.keys() <= {'mediaType', 'digest', 'size', 'platform', 'annotations'}, 'descriptor fields')
    require(type(row['digest']) is str and re.fullmatch(r'sha256:[0-9a-f]{64}', row['digest']) and
            type(row['size']) is int and 0 < row['size'] <= BLOB_LIMIT and
            type(row['mediaType']) is str, 'descriptor identity')
    return {key: row[key] for key in ('mediaType', 'digest', 'size')}


def verified(raw, pin):
    pin = descriptor(pin)
    require(len(raw) == pin['size'] and 'sha256:' + sha(raw) == pin['digest'], 'content mismatch')
    return raw


def selected_wrapper(selected):
    row = descriptor(selected)
    row['platform'] = selected['platform']
    row['annotations'] = {'io.containerd.image.name': REPOSITORY + ':' + VERSION,
                          'org.opencontainers.image.ref.name': VERSION}
    return canonical({'schemaVersion': 2, 'mediaType': INDEX, 'manifests': [row]})


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def _destination(url, *, blob=False):
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == 'https' and not parsed.username and not parsed.password and
            parsed.port in (None, 443) and not parsed.fragment, 'unsafe HTTPS destination')
    host = parsed.hostname
    require(host in {'registry-1.docker.io', 'auth.docker.io'} or
            (blob and (host in {'production.cloudflare.docker.com', 'production.cloudfront.docker.com'} or
             (type(host) is str and host.endswith('.r2.cloudflarestorage.com')))),
            'unapproved public input host')
    return parsed


class PublicRegistry:
    def __init__(self):
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
            urllib.request.HTTPSHandler(context=ssl.create_default_context()))
        self._token = None

    def __repr__(self):
        return '<PublicRegistry anonymous pull transport>'

    def get(self, url, *, limit, authenticated=False, blob=False):
        require(type(limit) is int and 0 < limit <= BLOB_LIMIT, 'response bound')
        # A read1 performs at most one buffered/raw read. Checking the monotonic
        # deadline between reads prevents a slow, continuously arriving body
        # from extending socket inactivity timeouts indefinitely. An in-flight
        # socket operation remains bounded by its separate 30-second timeout.
        deadline = time.monotonic() + 60
        for _ in range(5):
            require(time.monotonic() < deadline, 'public request deadline')
            parsed = _destination(url, blob=blob)
            headers = {'Accept': ACCEPT, 'User-Agent': 'vz-public-registry-input/1',
                       'Accept-Encoding': 'identity'}
            if authenticated and parsed.hostname == 'registry-1.docker.io':
                if self._token is None:
                    token = parse(self.get(TOKEN, limit=METADATA_LIMIT)[0])
                    self._token = token.get('token')
                    require(type(self._token) is str and 0 < len(self._token) <= 32768 and
                            re.fullmatch(r'[A-Za-z0-9_.-]+', self._token), 'anonymous token format')
                headers['Authorization'] = 'Bearer ' + self._token
            try:
                response = self.opener.open(urllib.request.Request(url, headers=headers), timeout=30)
            except urllib.error.HTTPError as error:
                try:
                    if error.code in (301, 302, 303, 307, 308) and blob:
                        location = error.headers.get('Location')
                        require(type(location) is str, 'redirect location missing')
                        url = urllib.parse.urljoin(url, location)
                        continue
                    raise ValueError('registry acquisition: HTTP request rejected') from None
                finally:
                    error.close()
            except (OSError, urllib.error.URLError):
                raise ValueError('registry acquisition: public transport failed') from None
            with response:
                require(response.status == 200 and response.headers.get('Content-Encoding', 'identity') ==
                        'identity', 'HTTP response shape')
                chunks = bytearray()
                while len(chunks) <= limit:
                    require(time.monotonic() < deadline, 'public request deadline')
                    chunk = response.read1(min(65536, limit + 1 - len(chunks)))
                    if not chunk:
                        break
                    chunks.extend(chunk)
                raw = bytes(chunks)
                require(len(raw) <= limit, 'HTTP response exceeds bound')
                public = {'status': response.status, 'bytes': len(raw), 'sha256': sha(raw),
                          'docker_content_digest': response.headers.get('Docker-Content-Digest')}
                return raw, public
        raise ValueError('registry acquisition: redirect limit')


def acquire(output, *, pins=None, transport=None):
    """Create a fresh public input layout; failures retain partial acquisition."""
    output = Path(output)
    require(output.is_absolute() and output.parent == output.parent.resolve(strict=True) and
            not output.exists() and not output.is_symlink(), 'fresh canonical output required')
    transport = transport or PublicRegistry()
    if pins is not None:
        fixture.validate_pins(pins)
        pins = copy.deepcopy(pins)
        require(pins['schema_version'] == 1 and pins['source']['repository'] == REPOSITORY and
                pins['source']['tag'] == VERSION, 'unsupported frozen source')
        index_reference = descriptor(pins['source']['index'])['digest']
    else:
        index_reference = VERSION
    output.mkdir(mode=0o700)
    layout = output / 'layout'
    (layout / 'blobs' / 'sha256').mkdir(mode=0o700, parents=True)
    records, total = [], 0

    def save(path, raw):
        with path.open('xb') as stream:
            stream.write(raw)

    def fetch(kind, reference, expected=None):
        nonlocal total
        raw, receipt = transport.get(REGISTRY + kind + '/' + reference,
            limit=BLOB_LIMIT if kind == 'blobs' else METADATA_LIMIT, authenticated=True, blob=kind == 'blobs')
        if expected is not None:
            verified(raw, expected)
        reported = receipt['docker_content_digest']
        require(reported is None or reported == 'sha256:' + sha(raw), 'HTTP digest mismatch')
        total += len(raw)
        require(total <= TOTAL_LIMIT, 'total input bound')
        save(layout / 'blobs' / 'sha256' / sha(raw), raw)
        records.append({'kind': kind, 'reference': reference, **receipt})
        return raw

    raw_index = fetch('manifests', index_reference, pins['source']['index'] if pins else None)
    index = parse(raw_index)
    require(index.get('schemaVersion') == 2 and index.get('mediaType') in
            {INDEX, 'application/vnd.docker.distribution.manifest.list.v2+json'} and
            type(index.get('manifests')) is list and 0 < len(index['manifests']) <= 64, 'upstream index')
    matches = [row for row in index['manifests'] if row.get('platform') in
               ({'os': 'linux', 'architecture': 'arm64', 'variant': 'v8'},
                {'os': 'linux', 'architecture': 'arm64'})]
    require(len(matches) == 1, 'unique arm64 selection')
    selected = matches[0]
    manifest_pin = descriptor(selected)
    raw_manifest = fetch('manifests', selected['digest'], manifest_pin)
    manifest = parse(raw_manifest)
    require(manifest.get('schemaVersion') == 2 and manifest.get('mediaType') == selected['mediaType'] and
            selected['mediaType'] in {MANIFEST, 'application/vnd.docker.distribution.manifest.v2+json'},
            'manifest format')
    config_pin = descriptor(manifest['config'])
    config = parse(fetch('blobs', config_pin['digest'], config_pin))
    require(config.get('os') == 'linux' and config.get('architecture') == 'arm64' and
            config.get('variant', 'v8') == 'v8', 'config platform')
    layers, diff_ids = manifest['layers'], config['rootfs']['diff_ids']
    require(config['rootfs']['type'] == 'layers' and type(layers) is list and 0 < len(layers) <= 32 and
            type(diff_ids) is list and len(diff_ids) == len(layers), 'layer inventory')
    layer_pins = []
    for row, diff_id in zip(layers, diff_ids):
        layer = descriptor(row)
        require(re.fullmatch(r'sha256:[0-9a-f]{64}', diff_id) and layer['mediaType'] in
                {'application/vnd.oci.image.layer.v1.tar+gzip', 'application/vnd.oci.image.layer.v1.tar',
                 'application/vnd.docker.image.rootfs.diff.tar.gzip'}, 'layer format')
        fetch('blobs', layer['digest'], layer)
        layer_pins.append({**layer, 'diff_id': diff_id})
    wrapper = selected_wrapper(selected)
    resolved = {'schema_version': 1, 'source': {'repository': REPOSITORY, 'tag': VERSION,
        'index': {'mediaType': index['mediaType'], 'digest': 'sha256:' + sha(raw_index), 'size': len(raw_index)},
        'platform_manifest': manifest_pin, 'platform': selected['platform']}, 'config': config_pin,
        'layers': layer_pins, 'layout_index': {'sha256': sha(wrapper), 'size': len(wrapper)}}
    require(pins is None or resolved == pins, 'resolved inputs differ from frozen pins')
    save(layout / 'index.json', wrapper)
    save(layout / 'oci-layout', canonical({'imageLayoutVersion': '1.0.0'}))
    save(output / 'pin-proposal.json', canonical(resolved))
    save(output / 'acquisition.json', canonical({'schema_version': 1,
        'scope': 'public_input_acquisition_not_runtime_or_layer_diff_id_verification',
        'resolution': 'explicit_version_tag' if pins is None else 'immutable_descriptor_replay',
        'requests': records, 'total_bytes': total, 'user_credentials_used': False,
        'docker_dispatched': False, 'request_retries': 0}))
    return resolved


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--resolve-version', action='store_true')
    mode.add_argument('--pin', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    try:
        pin = parse(fixture._read(args.pin.absolute())) if args.pin else None
        result = acquire(args.output, pins=pin)
        print(json.dumps({'scope': 'public_input_acquisition_only',
                          'manifest': result['source']['platform_manifest']['digest']}))
    except (ValueError, OSError, KeyError, TypeError):
        parser.exit(1, 'registry acquisition failed; partial public inputs retained\n')
