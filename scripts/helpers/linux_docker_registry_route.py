"""Pure source-attributed Engine login-route inference, not a direct /auth trace.

Contract: raw is the COMPLETE, newline-aligned logrus JSON stderr delta of an
unmodified Distribution 3.1.1 process during one serialized login. Do not feed
Gorilla's separate combined stdout log or silently filter unknown records.
The caller independently binds byte offsets, server CID/owner/incarnation,
nonloopback internal-network/no-published-port topology, TLS policy, exact CLI
command/result and these pins. window_ns is an independently authenticated
SAME-GUEST Unix clock interval enclosing that command, not unadjusted host time.
No filesystem, processes, credentials, or ownership claims are manufactured here.

Primary source contracts (tag v3.1.1 in distribution/distribution):
registry/handlers/app.go dispatcher/authorized/apiBase emits authorized request
then response completed (same http.request.id); only the latter has response
statistics. internal/dcontext/http.go selects the named request/response fields;
registry/registry.go configures RFC3339Nano JSON, separately from combined logs.
registry/auth/htpasswd/access.go and registry/auth/auth.go define the anonymous
challenge text. Moby 6a43e3d5 daemon/auth.go, dockerversion/useragent.go and
daemon/server/server.go add the Engine UA and escaped upstream CLI UA. Docker
CLI v29.4.0 cli/command/registry/login.go's fallback uses only the CLI UA.

This supports successful basic-auth login only. It is not an arbitrary registry
log validator, failed-login classifier, TLS certificate verifier, socket trace,
or proof that a malicious same-owner process cannot forge User-Agent strings.

Route witness: every admitted record must carry http.request.method GET and
http.request.uri /v2/ (the Engine login ping), the exact Engine User-Agent with
the escaped upstream CLI identity, and a remoteaddr on the owned bridge gateway.
That is Distribution's own per-request server-side record, produced inside the
registry process, of the request the daemon made. It excludes the CLI's
client-side fallback (whose UA is only the bare CLI UA and whose peer would not
be the guest daemon), but it is still inference from the server's log, not a
captured wire trace of the daemon's /auth handling or the TLS socket.
"""
import calendar
import datetime
from decimal import Decimal
import hashlib
import ipaddress
import json
import re

from docker_host_driver import contains_canary

MAX_BYTES = 1024 * 1024
MAX_LINE = 16384
MAX_ROWS = 256
MAX_WINDOW_NS = 120 * 1000000000
UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
COMMON = {'time', 'level', 'msg', 'go.version', 'instance.id', 'version',
          'http.request.id', 'http.request.method', 'http.request.host',
          'http.request.uri', 'http.request.useragent', 'http.request.remoteaddr'}
RESPONSE = {'http.response.status', 'http.response.written',
            'http.response.contenttype', 'http.response.duration'}
# Moby daemon/pkg/registry/auth.go PingV2Registry and loginV2 both issue GET on
# the trimmed endpoint + "/v2/"; nothing else is an Engine login route.
LOGIN_METHOD, LOGIN_URI = 'GET', '/v2/'


class RouteError(ValueError):
    """Diagnostics never interpolate supplied log content or credentials."""


def require(value, code):
    if not value:
        raise RouteError('registry route: ' + code)


def fields(value, names):
    require(type(value) is dict and set(value) == set(names), 'expected pin fields')


def token(value, pattern=r'[A-Za-z0-9_.+-]{1,128}'):
    require(type(value) is str and re.fullmatch(pattern, value) is not None, 'public token')
    return value


def engine_user_agent(engine, cli_version):
    """Pins come from authenticated binary/version/Engine evidence, not logs."""
    fields(engine, ('version', 'go_version', 'git_commit', 'kernel_version', 'os', 'arch'))
    for value in engine.values():
        token(value)
    require(engine['version'] == '29.7.2' and engine['os'] == 'linux' and engine['arch'] == 'arm64',
            'pinned Engine release/platform')
    require(re.fullmatch(r'go[0-9]+\.[0-9]+(?:\.[0-9]+)?', engine['go_version']) is not None,
            'Engine Go version')
    require(re.fullmatch(r'[0-9a-f]{7,40}', engine['git_commit']) is not None, 'Engine commit')
    require(cli_version == '29.4.0', 'pinned CLI release')
    names = [('docker', 'version'), ('go', 'go_version'), ('git-commit', 'git_commit'),
             ('kernel', 'kernel_version'), ('os', 'os'), ('arch', 'arch')]
    return ' '.join(name + '/' + engine[key] for name, key in names) + (
        ' UpstreamClient(Docker-Client/' + cli_version + r' \(darwin\))')


def timestamp_ns(value):
    """RFC3339 with 0..9 fractional digits; no floats or local timezone use."""
    require(type(value) is str, 'timestamp type')
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)', value)
    require(match is not None, 'timestamp syntax')
    try:
        date = datetime.datetime.strptime(match[1], '%Y-%m-%dT%H:%M:%S')
        seconds = calendar.timegm(date.timetuple())
        zone = match[3]
        if zone != 'Z':
            hours, minutes = int(zone[1:3]), int(zone[4:])
            require(hours <= 23 and minutes <= 59, 'timezone bounds')
            seconds -= (1 if zone[0] == '+' else -1) * (hours * 3600 + minutes * 60)
        return seconds * 1000000000 + int((match[2] or '').ljust(9, '0'))
    except (ValueError, OverflowError):
        raise RouteError('registry route: timestamp value') from None


def duration_ns(value):
    # time.Duration.String uses ns/us/µs/ms or h/m/s and fractional subunits.
    require(type(value) is str and 0 < len(value) <= 64, 'duration bounds')
    parts = re.findall(r'(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|h|m|s)', value)
    require(parts and ''.join(n + u for n, u in parts) == value, 'duration syntax')
    units = {'ns': 1, 'us': 1000, 'µs': 1000, 'μs': 1000, 'ms': 1000000,
             's': 1000000000, 'm': 60000000000, 'h': 3600000000000}
    total = sum(Decimal(n) * units[u] for n, u in parts)
    require(total == total.to_integral_value() and 0 < total <= MAX_WINDOW_NS, 'duration value')
    return int(total)


def unique_pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def validate(raw_delta, *, engine, cli_version, registry, username, window_ns, canaries=()):
    """Validate exact source-shaped successful login records; return public proof.

    registry has instance_id/go_version/version/host/remote_ip/realm. Version is
    the exact externally pinned Distribution version string (3.1.1 or v3.1.1).
    Host is a nonloopback RFC1918 IPv4:port; remote_ip is the owned bridge gateway.
    Optional log.fields service is accepted only as literal registry. All other
    unknown fields, even outside selected success rows, are rejected.
    """
    require(type(raw_delta) is bytes and 0 < len(raw_delta) <= MAX_BYTES, 'raw byte bounds')
    require(type(canaries) in (list, tuple) and len(canaries) <= 64 and
            all(type(x) is bytes and 0 < len(x) <= 16384 for x in canaries), 'canary bounds')
    # Scan before JSON admission; contains_canary also decodes JSON escapes and
    # base64 without including supplied content in diagnostics.
    require(not contains_canary((raw_delta,), canaries), 'private canary')
    ua = engine_user_agent(engine, cli_version)
    fields(registry, ('instance_id', 'go_version', 'version', 'host', 'remote_ip', 'realm'))
    token(registry['instance_id'], UUID)
    token(registry['go_version'], r'go[0-9]+\.[0-9]+(?:\.[0-9]+)?')
    require(registry['version'] in ('3.1.1', 'v3.1.1'), 'Distribution release')
    token(registry['realm'], r'[A-Za-z0-9_.-]{1,64}')
    token(username, r'[A-Za-z0-9_.-]{1,64}')
    try:
        host, port = registry['host'].split(':')
        address, peer = ipaddress.IPv4Address(host), ipaddress.IPv4Address(registry['remote_ip'])
        private = [ipaddress.IPv4Network(x) for x in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')]
        require(str(address) == host and str(peer) == registry['remote_ip'] and
                any(address in n for n in private) and any(peer in n for n in private) and
                re.fullmatch(r'[1-9][0-9]{0,4}', port) and int(port) <= 65535, 'private network authority')
    except (ValueError, TypeError, AttributeError):
        raise RouteError('registry route: private network authority') from None
    require(type(window_ns) in (tuple, list) and len(window_ns) == 2 and
            all(type(x) is int and x > 0 for x in window_ns) and
            0 < window_ns[1] - window_ns[0] <= MAX_WINDOW_NS, 'guest command window')
    require(raw_delta.endswith(b'\n'), 'truncated final record')
    lines = raw_delta.split(b'\n')[:-1]
    require(0 < len(lines) <= MAX_ROWS and all(0 < len(x) <= MAX_LINE for x in lines), 'record bounds')
    pending, done, challenges, previous = {}, {}, set(), window_ns[0]
    challenge = ('error authorizing context: basic authentication challenge for realm "' +
                 registry['realm'] + '": invalid authorization credential')
    for line in lines:
        try:
            row = json.loads(line.decode('utf-8'), object_pairs_hook=unique_pairs,
                             parse_constant=lambda _: require(False, 'JSON constant'))
        except (UnicodeError, ValueError, RecursionError):
            raise RouteError('registry route: malformed JSON') from None
        require(type(row) is dict, 'record object')
        msg = row.get('msg')
        require(msg in ('authorized request', 'response completed', challenge), 'unexpected record')
        wanted = COMMON | ({'auth.user.name'} if msg != challenge else set())
        if msg == 'response completed':
            wanted |= RESPONSE
        require(wanted <= set(row) <= wanted | {'service'}, 'record fields')
        require('service' not in row or row['service'] == 'registry', 'service field')
        require(all(type(v) in (str, int) for v in row.values()), 'record scalar types')
        require(row['go.version'] == registry['go_version'] and row['version'] == registry['version'] and
                row['instance.id'] == registry['instance_id'], 'registry identity')
        request = token(row['http.request.id'], UUID)
        require(row['http.request.method'] == LOGIN_METHOD and row['http.request.uri'] == LOGIN_URI and
                row['http.request.host'] == registry['host'] and row['http.request.useragent'] == ua,
                'Engine request route')
        remote = row['http.request.remoteaddr']
        require(type(remote) is str and re.fullmatch(re.escape(str(peer)) + r':[1-9][0-9]{0,4}', remote),
                'registry peer')
        require(int(remote.rsplit(':', 1)[1]) <= 65535, 'peer port')
        stamp = timestamp_ns(row['time'])
        require(previous <= stamp <= window_ns[1], 'record outside ordered guest window')
        previous = stamp
        require(request not in done and request not in challenges, 'reused request ID')
        if msg == challenge:
            require(row['level'] == 'warning' and request not in pending, 'challenge lifecycle')
            challenges.add(request)
        elif msg == 'authorized request':
            require(row['level'] == 'info' and row['auth.user.name'] == username and request not in pending,
                    'authorization lifecycle')
            pending[request] = row
        else:
            require(request in pending and row['level'] == 'info' and row['auth.user.name'] == username,
                    'completion without authorization')
            first = pending.pop(request)
            require(all(row[k] == first[k] for k in COMMON - {'time', 'msg'}), 'request identity drift')
            require(type(row['http.response.status']) is int and row['http.response.status'] == 200 and
                    type(row['http.response.written']) is int and row['http.response.written'] == 2 and
                    row['http.response.contenttype'] == 'application/json', 'successful base response')
            duration = duration_ns(row['http.response.duration'])
            require(stamp - duration >= window_ns[0], 'request predates guest command window')
            done[request] = {'request_id': request, 'authorized_ns': timestamp_ns(first['time']),
                             'completed_ns': stamp, 'duration_ns': duration}
    require(done and not pending, 'missing or incomplete successful request')
    return {'schema_version': 1, 'scope': 'source_attributed_Engine_registry_login_route_inference',
            'direct_auth_endpoint_trace': False, 'external_owner_binding_required': True,
            'tls_validation_proven': False, 'raw_sha256': hashlib.sha256(raw_delta).hexdigest(),
            'raw_bytes': len(raw_delta), 'records': len(lines), 'engine_user_agent': ua,
            'registry_instance_id': registry['instance_id'], 'window_ns': list(window_ns),
            'authenticated_requests': list(done.values()), 'anonymous_challenges': len(challenges),
            'server_side_request_witness': {
                'source': 'distribution_request_context_json_record', 'method': LOGIN_METHOD,
                'uri': LOGIN_URI, 'host': registry['host'], 'user_agent_kind': 'engine_with_upstream_cli',
                'remote_peer': registry['remote_ip'], 'asserted_on_every_record': True},
            'client_side_cli_fallback_excluded_by': 'engine_user_agent_and_daemon_remoteaddr',
            'wire_trace_captured': False}
