"""Fixed-path diagnostic enrollment and quiescent audit acquisition primitives.

The host binds public Exec to an exact Machine and independently proves writer
quiescence. Repeated metadata/full-content checks detect ordinary concurrent
changes, not malicious-root path substitution/ABA. Shell path checks are not
fd-relative hostile-root containment. No journal lock delays OCI cleanup; no
snapshot creates, truncates, rotates, repairs or seals the guest journal.
"""
import base64
import hashlib
import re

import linux_docker_runtime_audit as audit

CHUNK_SIZE = 1024 * 1024
JOURNAL_LIMIT = audit.JOURNAL_LIMIT
ROOT = '/var/lib/docker/runtime-audit'
RUNTIME = '/mnt/linux-bin/youki'
SNAPSHOT_LIMIT = 8192
SCOPE = 'diagnostic_quiescent_acquisition_not_hostile_root_tamper_resistance'


def require(value, message):
    if not value:
        raise ValueError('runtime audit capture: ' + message)


def pins(session_id, runtime_sha256):
    require(type(session_id) is str and re.fullmatch('[0-9a-f]{64}', session_id), 'session pin')
    require(type(runtime_sha256) is str and re.fullmatch('[0-9a-f]{64}', runtime_sha256), 'runtime pin')


def _common(session_id, runtime_sha256):
    pins(session_id, runtime_sha256)
    return r'''set -eu
set -o pipefail
export LC_ALL=C
umask 077
bb=/bin/busybox
root=/var/lib/docker/runtime-audit
runtime=/mnt/linux-bin/youki
session=SESSION_PIN
runtime_pin=RUNTIME_PIN
fail() { printf 'runtime audit capture rejected\n' >&2; exit 1; }
meta() { "$bb" stat -c '%f|%u|%g|%h|%d|%i|%s|%Y|%Z' "$1"; }
directory() {
  test ! -L "$1" && test -d "$1" || fail
  test "$("$bb" stat -c '%u' "$1")" = 0 || fail
  mode=$("$bb" stat -c '%a' "$1")
  case "$mode" in 700|755|750|555) ;; *) fail ;; esac
}
parents() {
  test "$("$bb" id -u)" = 0 || fail
  for path in / /var /var/lib /var/lib/docker /mnt /mnt/linux-bin; do directory "$path"; done
  test ! -L "$runtime" && test -f "$runtime" || fail
  runtime_size=$("$bb" stat -c '%s' "$runtime")
  test "$runtime_size" -gt 0 && test "$runtime_size" -le 67108864 || fail
  runtime_before=$(meta "$runtime")
  got=$("$bb" sha256sum "$runtime"); got=${got%% *}
  test "$got" = "$runtime_pin" && test "$(meta "$runtime")" = "$runtime_before" || fail
}
regular() {
  test ! -L "$1" && test -f "$1" || fail
  test "$("$bb" stat -c '%u|%g|%a|%h' "$1")" = '0|0|600|1' || fail
}
snapshot() {
  parents
  directory "$root"
  test "$("$bb" stat -c '%a' "$root")" = 700 || fail
  for name in enrollment.json events.jsonl status; do regular "$root/$name"; done
  before_root=$(meta "$root")
  before_enrollment=$(meta "$root/enrollment.json")
  before_events=$(meta "$root/events.jsonl")
  before_status=$(meta "$root/status")
  size=$("$bb" stat -c '%s' "$root/events.jsonl")
  test "$size" -le 16777216 || fail
  test "$("$bb" stat -c '%s' "$root/enrollment.json")" -le 512 || fail
  test "$("$bb" stat -c '%s' "$root/status")" = 9 || fail
  boot=$("$bb" cat /proc/sys/kernel/random/boot_id)
  enrolled=$("$bb" head -c 513 "$root/enrollment.json" | "$bb" base64 -w 0)
  status=$("$bb" head -c 12 "$root/status" | "$bb" base64 -w 0)
  hash=$("$bb" sha256sum "$root/events.jsonl"); hash=${hash%% *}
  test "$before_root" = "$(meta "$root")" && test "$before_enrollment" = "$(meta "$root/enrollment.json")" || fail
  test "$before_events" = "$(meta "$root/events.jsonl")" && test "$before_status" = "$(meta "$root/status")" || fail
  test "$boot" = "$("$bb" cat /proc/sys/kernel/random/boot_id)" || fail
  parents
  printf 'VZ_RUNTIME_AUDIT_SNAPSHOT_V1\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\nEND\n' "$boot" "$runtime_pin" "$enrolled" "$status" "$before_root" "$before_enrollment" "$before_events" "$before_status" "$hash"
}
'''.replace('SESSION_PIN', session_id).replace('RUNTIME_PIN', runtime_sha256)


def enrollment_script(session_id, runtime_sha256):
    """One fresh mkdir, marker last; failures preserve partial effects for audit."""
    return _common(session_id, runtime_sha256) + r'''
parents
test ! -e "$root" && test ! -L "$root" || fail
"$bb" mkdir -m 700 "$root"
set -C
: > "$root/events.jsonl"
printf 'complete\n' > "$root/status"
"$bb" sync "$root/events.jsonl" "$root/status" "$root"
boot=$("$bb" cat /proc/sys/kernel/random/boot_id)
printf '{"schema_version":1,"session_id":"%s","boot_id":"%s"}\n' "$session" "$boot" > "$root/enrollment.json"
"$bb" sync "$root/enrollment.json" "$root"
snapshot
'''


def snapshot_script(session_id, runtime_sha256):
    return _common(session_id, runtime_sha256) + '\nsnapshot\n'


def chunk_script(session_id, runtime_sha256, index, size, sha256):
    require(type(index) is int and 0 <= index < 16, 'chunk index')
    require(type(size) is int and 0 < size <= JOURNAL_LIMIT and index * CHUNK_SIZE < size, 'chunk size')
    require(type(sha256) is str and re.fullmatch('[0-9a-f]{64}', sha256), 'journal digest')
    return _common(session_id, runtime_sha256) + r'''
first=$(snapshot)
test "$("$bb" stat -c '%s' "$root/events.jsonl")" = 'SIZE_PIN' || fail
digest=$("$bb" sha256sum "$root/events.jsonl"); digest=${digest%% *}
test "$digest" = 'HASH_PIN' || fail
printf 'VZ_RUNTIME_AUDIT_CHUNK_V1\nINDEX_PIN\n'
printf '%s\n' "$first" | "$bb" base64 -w 0
printf '\n'
"$bb" dd if="$root/events.jsonl" bs=1048576 skip=INDEX_PIN count=1 iflag=fullblock 2>/dev/null | "$bb" base64 -w 0
printf '\n'
last=$(snapshot)
test "$first" = "$last" || fail
printf '%s\n' "$last" | "$bb" base64 -w 0
printf '\nEND\n'
'''.replace('SIZE_PIN', str(size)).replace('HASH_PIN', sha256).replace('INDEX_PIN', str(index))


def _base64(raw, limit):
    require(type(raw) is bytes and len(raw) <= ((limit + 2) // 3) * 4, 'base64 bound')
    try:
        value = base64.b64decode(raw, validate=True)
    except (ValueError, base64.binascii.Error):
        raise ValueError('runtime audit capture: base64 encoding') from None
    require(len(value) <= limit and base64.b64encode(value) == raw, 'noncanonical base64')
    return value


def _lines(raw, limit, count, header):
    require(type(raw) is bytes and 0 < len(raw) <= limit and raw.endswith(b'\n'), 'frame bounds')
    rows = raw.split(b'\n')
    require(len(rows) == count + 1 and rows[-1] == b'' and rows[0] == header and rows[-2] == b'END', 'frame structure')
    return rows[:-1]


def _metadata(raw, kind):
    parts = raw.split(b'|')
    require(len(parts) == 9 and re.fullmatch(b'[0-9a-f]{4,8}', parts[0]) and
            all(re.fullmatch(b'0|[1-9][0-9]{0,19}', part) for part in parts[1:]), 'metadata format')
    values = [int(parts[0], 16)] + [int(part) for part in parts[1:]]
    mode, uid, gid, links, dev, inode, size, mtime, ctime = values
    require(all(value <= audit.UINT64 for value in values) and uid == gid == 0 and dev > 0 and inode > 0 and
            mtime > 0 and ctime > 0, 'metadata identity')
    require(mode == (0o40700 if kind == 'root' else 0o100600) and links >= 1 and
            (kind == 'root' or links == 1), 'protected mode/type/links')
    return values


def parse_snapshot(raw, *, session_id, runtime_sha256, expected_boot_id=None, enrolled=False):
    pins(session_id, runtime_sha256)
    require(type(enrolled) is bool, 'enrollment selector')
    rows = _lines(raw, SNAPSHOT_LIMIT, 11, b'VZ_RUNTIME_AUDIT_SNAPSHOT_V1')
    try:
        boot = rows[1].decode('ascii')
    except UnicodeError:
        raise ValueError('runtime audit capture: boot encoding') from None
    require(audit.boot_id(boot) and (expected_boot_id is None or expected_boot_id == boot), 'boot binding')
    require(rows[2] == runtime_sha256.encode(), 'runtime binding')
    enrollment = _base64(rows[3], 512)
    status = _base64(rows[4], 12)
    audit.enrollment(enrollment, expected_session_id=session_id, expected_boot_id=boot)
    require(status == b'complete\n', 'incomplete status')
    metadata = {name: _metadata(rows[index], name) for index, name in
                enumerate(('root', 'enrollment', 'events', 'status'), 5)}
    require(len({row[4] for row in metadata.values()}) == 1 and
            len({row[5] for row in metadata.values()}) == 4, 'filesystem or inode alias')
    size = metadata['events'][6]
    require(size <= JOURNAL_LIMIT and metadata['enrollment'][6] == len(enrollment) and
            metadata['status'][6] == len(status), 'file size binding')
    require(re.fullmatch(b'[0-9a-f]{64}', rows[9]), 'journal digest encoding')
    digest = rows[9].decode('ascii')
    if enrolled:
        require(size == 0 and digest == hashlib.sha256(b'').hexdigest(), 'fresh journal not empty')
    return {'schema_version': 1, 'session_id': session_id, 'boot_id': boot,
            'runtime_sha256': runtime_sha256, 'journal_size': size, 'journal_sha256': digest,
            'enrollment_base64': rows[3].decode('ascii'), 'status_base64': rows[4].decode('ascii'),
            'metadata': metadata}


def same_enrollment(enrolled, snapshot):
    require(enrolled['journal_size'] == 0 and enrolled['journal_sha256'] == hashlib.sha256(b'').hexdigest(), 'initial empty journal')
    for key in ('schema_version', 'session_id', 'boot_id', 'runtime_sha256', 'enrollment_base64', 'status_base64'):
        require(enrolled[key] == snapshot[key], 'enrollment identity changed')
    for name in ('root', 'enrollment', 'events', 'status'):
        before, after = enrolled['metadata'][name], snapshot['metadata'][name]
        require(before[:6] == after[:6], 'enrolled filesystem identity changed')
        if name != 'events':
            require(before == after, 'protected enrollment metadata changed')


def parse_chunk(raw, *, snapshot, index):
    size = snapshot['journal_size']
    require(type(index) is int and 0 <= index < 16 and index * CHUNK_SIZE < size, 'chunk index')
    rows = _lines(raw, 2 * CHUNK_SIZE, 6, b'VZ_RUNTIME_AUDIT_CHUNK_V1')
    require(rows[1] == str(index).encode(), 'chunk sequence')
    for value in (rows[2], rows[4]):
        parsed = parse_snapshot(_base64(value, SNAPSHOT_LIMIT), session_id=snapshot['session_id'],
                                runtime_sha256=snapshot['runtime_sha256'], expected_boot_id=snapshot['boot_id'])
        require(parsed == snapshot, 'chunk snapshot changed')
    chunk = _base64(rows[3], CHUNK_SIZE)
    require(len(chunk) == min(CHUNK_SIZE, size - index * CHUNK_SIZE), 'short or oversized chunk')
    return chunk


def assemble(snapshot, chunks, final_snapshot):
    require(snapshot == final_snapshot, 'final snapshot changed')
    size = snapshot['journal_size']
    require(0 < size <= JOURNAL_LIMIT and type(chunks) is list and
            len(chunks) == (size + CHUNK_SIZE - 1) // CHUNK_SIZE, 'complete chunk inventory')
    for index, chunk in enumerate(chunks):
        require(type(chunk) is bytes and len(chunk) == min(CHUNK_SIZE, size - index * CHUNK_SIZE), 'chunk length')
    events = b''.join(chunks)
    require(len(events) == size and hashlib.sha256(events).hexdigest() == snapshot['journal_sha256'], 'full journal digest')
    enrollment = _base64(snapshot['enrollment_base64'].encode(), 512)
    status = _base64(snapshot['status_base64'].encode(), 12)
    validation = audit.validate(events, enrollment_raw=enrollment, status_raw=status,
                                expected_session_id=snapshot['session_id'], expected_boot_id=snapshot['boot_id'])
    return {'enrollment': enrollment, 'status': status, 'events': events, 'validation': validation}
