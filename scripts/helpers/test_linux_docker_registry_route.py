"""Inert source-shaped records; not a physical Distribution/login certificate."""
import copy
import json
import unittest
from unittest import mock

import linux_docker_registry_route as route


ENGINE = {'version': '29.7.2', 'go_version': 'go1.25.7', 'git_commit': '6a43e3d',
          'kernel_version': '6.12.85', 'os': 'linux', 'arch': 'arm64'}
REGISTRY = {'instance_id': '11111111-1111-4111-8111-111111111111',
            'go_version': 'go1.25.5', 'version': '3.1.1',
            'host': '172.30.241.2:5443', 'remote_ip': '172.30.241.1', 'realm': 'vz-registry'}
START = route.timestamp_ns('2026-09-06T20:00:00Z')


def records():
    common = {'time': '2026-09-06T20:00:00.001Z', 'level': 'info',
              'msg': 'authorized request', 'go.version': REGISTRY['go_version'],
              'instance.id': REGISTRY['instance_id'], 'version': '3.1.1', 'service': 'registry',
              'http.request.id': '22222222-2222-4222-8222-222222222222',
              'http.request.method': 'GET', 'http.request.host': REGISTRY['host'],
              'http.request.uri': '/v2/', 'http.request.remoteaddr': '172.30.241.1:45678',
              'http.request.useragent': route.engine_user_agent(ENGINE, '29.4.0'),
              'auth.user.name': 'fixture-user'}
    final = dict(common, time='2026-09-06T20:00:00.002000001Z', msg='response completed')
    final.update({'http.response.status': 200, 'http.response.written': 2,
                  'http.response.contenttype': 'application/json', 'http.response.duration': '1.2ms'})
    return [common, final]


def encode(rows):
    return b''.join(json.dumps(row, separators=(',', ':')).encode() + b'\n' for row in rows)


class RouteTests(unittest.TestCase):
    def check(self, rows=None, raw=None, **kwargs):
        selected = {'engine': ENGINE, 'cli_version': '29.4.0', 'registry': REGISTRY,
                    'username': 'fixture-user', 'window_ns': (START, START + 1000000000)}
        selected.update(kwargs)
        return route.validate(encode(records() if rows is None else rows) if raw is None else raw, **selected)

    def reject(self, rows=None, raw=None, **kwargs):
        with self.assertRaises(route.RouteError):
            self.check(rows, raw, **kwargs)

    def test_exact_source_ua_and_inference_scope_no_io(self):
        with mock.patch('subprocess.Popen', side_effect=AssertionError('process')), \
             mock.patch('socket.socket', side_effect=AssertionError('network')), \
             mock.patch('builtins.open', side_effect=AssertionError('file')):
            proof = self.check()
        self.assertEqual(proof['records'], 2)
        self.assertEqual(len(proof['authenticated_requests']), 1)
        self.assertFalse(proof['direct_auth_endpoint_trace'])
        self.assertTrue(proof['external_owner_binding_required'])
        self.assertFalse(proof['tls_validation_proven'])
        self.assertEqual(proof['engine_user_agent'],
            'docker/29.7.2 go/go1.25.7 git-commit/6a43e3d kernel/6.12.85 os/linux arch/arm64 '
            r'UpstreamClient(Docker-Client/29.4.0 \(darwin\))')

    def test_anonymous_challenge_has_no_invented_status_or_username(self):
        rows = records()
        challenge = dict(rows[0], time='2026-09-06T20:00:00.0001Z', level='warning',
            msg='error authorizing context: basic authentication challenge for realm "vz-registry": invalid authorization credential')
        challenge['http.request.id'] = '33333333-3333-4333-8333-333333333333'
        del challenge['auth.user.name']
        self.assertEqual(self.check([challenge] + rows)['anonymous_challenges'], 1)
        challenge['http.response.status'] = 401
        self.reject([challenge] + rows)

    def test_exact_engine_and_upstream_tokens_not_substrings(self):
        ua = records()[0]['http.request.useragent']
        for value in ['Docker-Client/29.4.0 (darwin)', ua + ' forged', ua.replace('arm64', 'amd64'),
                      ua.replace('6.12.85', '6.12.86'), ua.replace('go1.25.7', 'go1.25.6'),
                      ua.replace('6a43e3d', 'aaaaaaa'), ua.replace('29.4.0', '29.4.1'),
                      ua.replace(r'\(darwin\)', '(darwin)'), ua.replace('os/linux', 'os/darwin')]:
            with self.subTest(value=value):
                rows = records()
                for row in rows:
                    row['http.request.useragent'] = value
                self.reject(rows)

    def test_malformed_or_drifted_external_pins(self):
        for key, value in [('version', '29.7.3'), ('go_version', ''), ('arch', 'amd64'),
                           ('git_commit', 'branch'), ('kernel_version', 'kernel/foreign'), ('os', True)]:
            engine = dict(ENGINE); engine[key] = value
            self.reject(engine=engine)
        self.reject(cli_version='29.4.1')

    def test_foreign_instance_process_host_peer_username(self):
        for key, value in [('instance.id', '44444444-4444-4444-8444-444444444444'),
                           ('go.version', 'go1.25.4'), ('version', '3.1.0'),
                           ('http.request.host', '172.30.241.3:5443'),
                           ('http.request.remoteaddr', '172.30.241.3:1234'),
                           ('auth.user.name', 'other')]:
            rows = records()
            for row in rows:
                row[key] = value
            self.reject(rows)

    def test_loopback_public_and_malformed_authorities_rejected(self):
        for host in ['127.0.0.1:5443', '8.8.8.8:5443', '::1:5443', '172.30.241.2:0',
                     '172.30.241.2:65536', '172.30.241.2:05443']:
            registry = dict(REGISTRY, host=host)
            self.reject(registry=registry)
        self.reject(registry=dict(REGISTRY, remote_ip='127.0.0.1'))

    def test_success_requires_paired_auth_and_complete_final_records(self):
        first, final = records()
        for rows in [[], [first], [final], [final, first], [first, first, final],
                     [first, final, final], [first, final, first], records() + records()]:
            self.reject(rows)

    def test_pair_identity_drift_and_unrelated_requests(self):
        for key, value in [('http.request.remoteaddr', '172.30.241.1:45679'),
                           ('http.request.id', '44444444-4444-4444-8444-444444444444'),
                           ('http.request.method', 'HEAD'), ('http.request.uri', '/v2/?token=x'),
                           ('level', 'warning')]:
            rows = records(); rows[-1][key] = value; self.reject(rows)

    def test_response_exact_types_sizes_status_and_media(self):
        for key, values in [('http.response.status', [True, '200', 201, 301, 401, 500]),
                            ('http.response.written', [True, '2', 0, 3]),
                            ('http.response.contenttype', ['text/plain', 'application/json; charset=utf-8']),
                            ('http.response.duration', ['0s', '-1s', '1200000', '0.1ns', '121s'])]:
            for value in values:
                rows = records(); rows[-1][key] = value; self.reject(rows)

    def test_timestamp_nanoseconds_and_offset_equivalence(self):
        self.assertEqual(route.timestamp_ns('2026-09-06T22:00:00.002000001+02:00'), START + 2000001)
        rows = records(); rows[-1]['time'] = '2026-09-06T22:00:00.002000001+02:00'
        self.check(rows)
        self.assertEqual(route.duration_ns('1m0.000000001s'), 60000000001)
        self.assertEqual(route.duration_ns('1.2µs'), 1200)

    def test_stale_future_reversed_and_cross_window_duration(self):
        for value in ['2026-09-06T19:59:59Z', '2026-09-06T20:00:01.000000001Z',
                      '2026-09-06T20:00:00.0009Z', '2026-09-06T20:00:00.1234567890Z']:
            rows = records(); rows[-1]['time'] = value; self.reject(rows)
        rows = records(); rows[-1]['http.response.duration'] = '3ms'; self.reject(rows)
        for window in [(True, START), (START, START), (START, START - 1),
                       (START, START + route.MAX_WINDOW_NS + 1)]:
            self.reject(window_ns=window)

    def test_unknown_sensitive_fields_and_canaries_never_echoed(self):
        for key in ['Authorization', 'http.request.headers', 'password', 'identitytoken',
                    'auth', 'data', 'unknown']:
            rows = records(); rows[0][key] = 'private-canary'; self.reject(rows)
        for raw in [encode(records()).replace(b'authorized request', b'private-canary'),
                    encode(records()).replace(b'authorized request', b'private-\\u0063anary')]:
            with self.assertRaises(route.RouteError) as caught:
                self.check(raw=raw, canaries=[b'private-canary'])
            self.assertNotIn('private-canary', str(caught.exception))
            self.assertEqual(str(caught.exception), 'registry route: private canary')

    def test_duplicate_json_truncation_encoding_and_bounds(self):
        raw = encode(records())
        for changed in [raw[:-1], raw + b'\n', b'not-json\n', b'\xff\n',
                        raw.replace(b'"level":"info"', b'"level":"info","level":"info"', 1),
                        raw.replace(b'"level":"info"', b'"level":"info","\\u006cevel":"info"', 1),
                        b' ' * (route.MAX_LINE + 1) + b'\n', raw * 129,
                        b'[' * 1000 + b'0' + b']' * 1000 + b'\n']:
            self.reject(raw=changed)

    def test_source_selected_optional_service_not_arbitrary_metadata(self):
        rows = records()
        for row in rows:
            del row['service']
        self.check(rows)
        rows[0]['service'] = 'foreign'; self.reject(rows)


if __name__ == '__main__':
    unittest.main()
