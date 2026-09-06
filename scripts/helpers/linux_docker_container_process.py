"""Bounded, read-only guest process/cgroup observations for the pinned Docker lane.

This is interval evidence, not an atomic process-table transaction or historical
runtime-invocation audit. Callers authenticate the exact container generation and
Machine before and after public Exec. Shared user/cgroup namespaces are recorded,
never treated as private absence targets. No process is signalled or adopted.
Stable same-birth Z/X records may lack mount/cgroup namespaces after nsproxy
release; they remain recorded processes, never disappearance evidence. Namespace
membership checks do not certify destruction of namespace objects or FD refs.
"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import re

import linux_docker_buildkit_cgroup as binding

FIELD_LIMIT = 16384
STREAM_LIMIT = 4 * 1024 * 1024
PROCESS_LIMIT = 1024
GROUP_LIMIT = 128
NS = ('pid', 'mnt', 'user', 'cgroup')

# The non-newline suffix preserves the original byte count inside command
# substitution. Check before removing any original trailing newline. Every
# inventory acquisition uses this function, including final rechecks.
BOUNDED_LIST = r'''bounded_list() {
  _bounded_data=$("$@" | "$bb" head -c 16385 || exit 1; printf '.') || return 1
  _bounded_data=${_bounded_data%.}
  [ "${#_bounded_data}" -le 16384 ] || return 1
  printf '%s' "$_bounded_data"
}'''


def require(value, message):
    if not value:
        raise ValueError('container process: ' + message)


def policy(engine_policy, inspected):
    require(isinstance(engine_policy, dict) and engine_policy.get('CgroupDriver') == 'cgroupfs' and
            engine_policy.get('CgroupVersion') == '2', 'requires observed unified cgroupfs Engine')
    host = inspected.get('HostConfig', {})
    require(isinstance(host, dict) and host.get('CgroupParent') == '',
            'nondefault container cgroup parent unsupported')
    cid = inspected.get('Id')
    require(isinstance(cid, str) and re.fullmatch(r'[0-9a-f]{64}', cid), 'invalid exact container ID')
    return cid


def probe_script(cid):
    require(isinstance(cid, str) and re.fullmatch(r'[0-9a-f]{64}', cid), 'invalid exact container ID')
    # readlink/stat paths are source-selected. Depth 9 is a sentinel: the parser
    # accepts at most 8 descendants, so deeper trees fail rather than truncate.
    # Bounded find output is buffered
    # before iteration; a truncated/failed pipeline never emits a positive END.
    # A per-process subshell buffers partial records until all reads succeed.
    return r'''set -eu
set -o pipefail
LC_ALL=C
export LC_ALL
bb=/bin/busybox
''' + BOUNDED_LIST + r'''
root=/sys/fs/cgroup/docker/''' + cid + r'''
field() { printf '%s=' "$1"; "$bb" head -c 16385 "$2" | "$bb" base64 -w 0 || return 1; printf '\n'; }
cmd() { _field_key=$1; shift; printf '%s=' "$_field_key"; "$bb" "$@" | "$bb" base64 -w 0 || return 1; printf '\n'; }
literal() { printf '%s=' "$1"; printf '%s' "$2" | "$bb" base64 -w 0; printf '\n'; }
dirs() { bounded_list "$bb" find "$root" -maxdepth 9 -type d | "$bb" sort; }
printf 'VZ_CONTAINER_PROCESS_V1\n'
field boot_before /proc/sys/kernel/random/boot_id
field uptime_before /proc/uptime
cmd cgroup_fs stat -f -c %t /sys/fs/cgroup
field mountinfo /proc/self/mountinfo
for n in pid mnt user cgroup; do cmd guest.$n readlink /proc/1/ns/$n; cmd observer.$n readlink /proc/self/ns/$n; done
for p in /sys /sys/fs /sys/fs/cgroup /sys/fs/cgroup/docker "$root"; do
  [ ! -L "$p" ] || exit 71
done
if [ -d "$root" ]; then
  literal group_state present
  groups=$(dirs)
  [ "${#groups}" -le 16384 ] || exit 72
  literal groups_before "$groups"
  count=0
  for d in $groups; do
    count=$((count+1)); [ "$count" -le 128 ] || exit 73
    [ ! -L "$d" ] || exit 74
    key=g.$count
    literal "$key.path" "$d"
    cmd "$key.inode_before" stat -c %d:%i "$d"
    cmd "$key.fs" stat -f -c %t "$d"
    for f in cgroup.type cgroup.events cgroup.procs; do
      [ ! -L "$d/$f" ] || exit 75
      field "$key.$f" "$d/$f"
    done
    cmd "$key.inode_after" stat -c %d:%i "$d"
  done
  groups_after=$(dirs)
  literal groups_after "$groups_after"
else
  [ ! -e "$root" ] || exit 76
  literal group_state absent
fi
paths=$(bounded_list "$bb" find /proc -mindepth 1 -maxdepth 1 -type d)
[ "${#paths}" -le 16384 ] || exit 77
literal process_paths "$paths"
count=0
for p in $paths; do
  v=${p#/proc/}
  case "$v" in ''|*[!0-9]*) continue;; esac
  count=$((count+1)); [ "$count" -le 1024 ] || exit 78
  if row=$(
    set -e
    field p.$v.stat_before "$p/stat" || exit 1
    field p.$v.status "$p/status" || exit 1
    field p.$v.membership "$p/cgroup" || exit 1
    for n in pid mnt user cgroup; do
      if target=$("$bb" readlink "$p/ns/$n"); then literal p.$v.$n "$target";
      else literal p.$v.$n unavailable; fi
    done
    for n in pid mnt user cgroup; do
      if target=$("$bb" readlink "$p/ns/$n"); then literal p.$v.$n.after "$target";
      else literal p.$v.$n.after unavailable; fi
    done
    field p.$v.membership_after "$p/cgroup" || exit 1
    field p.$v.stat_after "$p/stat" || exit 1
  ) 2>/dev/null; then
    printf '%s\n' "$row"
  else
    [ ! -e "$p" ] || exit 79
    literal p.$v.gone absent
  fi
done
if [ -d "$root" ]; then
  literal group_state_end present
  groups_end=$(dirs)
  literal groups_end "$groups_end"
  count=0
  for d in $groups; do
    count=$((count+1))
    cmd g.$count.inode_end stat -c %d:%i "$d"
    field g.$count.procs_end "$d/cgroup.procs"
    field g.$count.events_end "$d/cgroup.events"
  done
else
  [ ! -e "$root" ] || exit 80
  literal group_state_end absent
fi
field uptime_after /proc/uptime
field boot_after /proc/sys/kernel/random/boot_id
printf 'VZ_CONTAINER_PROCESS_END\n'
'''


def unpack(raw):
    require(type(raw) is bytes and len(raw) <= STREAM_LIMIT, 'oversized or invalid stream')
    try:
        lines = raw.decode('ascii').splitlines(keepends=True)
        require(lines[0] == 'VZ_CONTAINER_PROCESS_V1\n' and
                lines[-1] == 'VZ_CONTAINER_PROCESS_END\n' and len(lines) <= 16000,
                'incomplete or excessive frames')
        result = {}
        for line in lines[1:-1]:
            key, encoded = line[:-1].split('=', 1)
            require(line.endswith('\n') and re.fullmatch(r'[a-z0-9_.]+', key) and key not in result,
                    'duplicate or invalid frame')
            data = base64.b64decode(encoded, validate=True)
            require(len(data) <= FIELD_LIMIT and base64.b64encode(data).decode() == encoded,
                    'oversized or noncanonical frame')
            result[key] = data.decode('ascii')
        return result
    except (ValueError, UnicodeError, IndexError) as error:
        raise ValueError('container process: malformed bounded frame stream') from error


def process_stat(value, pid):
    match = re.fullmatch(r'([1-9][0-9]*) \(([^\n]*)\) ([A-Za-z]) (.+)\n', value)
    require(match is not None and int(match[1]) == pid, 'invalid process stat identity')
    rest = match[4].split(' ')
    require(len(rest) >= 19 and all(re.fullmatch(r'-?[0-9]+', x) for x in rest), 'invalid stat fields')
    # Early boot kernel tasks can legitimately start in tick zero. The selected
    # Docker init below must have a positive post-boot birth.
    require(int(rest[18]) >= 0, 'invalid process birth')
    return {'pid': pid, 'starttime_ticks': int(rest[18]), 'state': match[3],
            'kernel_thread': bool(int(rest[5]) & 0x200000)}


def namespace(value, kind):
    require(re.fullmatch(kind + r':\[[1-9][0-9]*\]\n?', value), 'invalid namespace identity')
    return value.strip()


def membership(value):
    require(re.fullmatch(r'0::/[^\n]*\n', value), 'non-unified membership')
    path = value[3:-1]
    require(path == '/' or re.fullmatch(r'/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+', path) and
            not any(x in ('.', '..') for x in path.split('/')[1:]), 'unsafe membership path')
    return path


def pids(value):
    require(value == '' or re.fullmatch(r'(?:[1-9][0-9]*\n)+', value), 'invalid cgroup task list')
    values = sorted({int(x) for x in value.splitlines()})
    require(len(values) <= PROCESS_LIMIT and all(x < 2**31 for x in values), 'excessive cgroup tasks')
    return values


def events(value):
    require(re.fullmatch(r'populated [01]\nfrozen [01]\n', value), 'unsupported cgroup event schema')
    return {k: int(v) for k, v in (line.split(' ') for line in value.splitlines())}


def validate(raw, *, inspected, engine_policy, phase, previous=None, expected_boot_id=None):
    """Replay a source-fixed observation, with externally supplied inspect/policy.

    Stopped/removed without a running predecessor proves only owned cgroup
    quiescence. It never invents an unobserved short-lived PID birth or namespace.
    """
    cid = policy(engine_policy, inspected)
    require(phase in ('running', 'stopped', 'removed'), 'unknown phase')
    state = inspected.get('State', {})
    require(isinstance(state, dict), 'missing inspect state')
    if phase == 'running':
        require(state.get('Running') is True and type(state.get('Pid')) is int and 1 < state['Pid'] < 2**31 and
                isinstance(state.get('StartedAt'), str) and state['StartedAt'], 'missing running generation')
    else:
        require(phase == 'removed' or state.get('Running') is False and type(state.get('Pid')) is int and state['Pid'] == 0,
                'absence requires stopped inspect')
    fields = unpack(raw)
    used = set()
    def take(key):
        require(key in fields, 'missing required field')
        used.add(key)
        return fields[key]
    boot = take('boot_before')
    require(re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\n', boot) and
            boot != '00000000-0000-0000-0000-000000000000\n' and take('boot_after') == boot,
            'guest boot changed or invalid')
    if expected_boot_id is not None:
        require(boot.strip() == expected_boot_id, 'external Machine boot identity differs')
    if phase != 'running' and previous is None:
        require(expected_boot_id is not None, 'cgroup-only absence needs externally bound Machine boot')
    uptime = []
    for suffix in ('before', 'after'):
        value = take('uptime_' + suffix)
        require(re.fullmatch(r'[0-9]+\.[0-9]{2} [0-9]+\.[0-9]{2}\n', value), 'invalid guest uptime')
        uptime.append(int(value.split(' ')[0].replace('.', '')))
    require(0 <= uptime[1] - uptime[0] <= 3000, 'invalid or excessive observation interval')
    require(take('cgroup_fs') == '63677270\n', 'not cgroup2 filesystem')
    guest = {n: namespace(take('guest.' + n), n) for n in NS}
    require(all(namespace(take('observer.' + n), n) == guest[n] for n in NS), 'foreign observer namespace')
    root = '/sys/fs/cgroup/docker/' + cid
    owned = '/docker/' + cid
    mounts = []
    for line in take('mountinfo').splitlines():
        words = line.split(' ')
        require(words.count('-') == 1 and words.index('-') >= 6 and len(words) == words.index('-') + 4,
                'invalid observer mountinfo')
        require(not (words[4] == root or words[4].startswith(root + '/')), 'owned cgroup bind mount unsupported')
        if words[4] == '/sys/fs/cgroup':
            mounts.append(words)
    require(len(mounts) == 1 and mounts[0][3] == '/' and
            mounts[0][mounts[0].index('-') + 1] == 'cgroup2', 'foreign observer cgroup mount root')
    group_state = take('group_state')
    require(group_state in ('present', 'absent') and take('group_state_end') == group_state,
            'cgroup existence changed during observation')
    groups = []
    if group_state == 'present':
        paths = take('groups_before').splitlines()
        require(0 < len(paths) <= GROUP_LIMIT and paths == sorted(set(paths)) and paths[0] == root and
                all(p == root or p.startswith(root + '/') for p in paths), 'invalid owned cgroup inventory')
        require(all(p == root or p.rsplit('/', 1)[0] in paths for p in paths), 'missing intermediate cgroup directory')
        require(take('groups_after').splitlines() == paths and take('groups_end').splitlines() == paths,
                'cgroup inventory changed')
        for index, path in enumerate(paths, 1):
            require(re.fullmatch(re.escape(root) + r'(?:/[A-Za-z0-9_.-]+){0,8}', path) and
                    all(x not in ('.', '..') for x in path.split('/')), 'unsafe/deep cgroup path')
            key = 'g.' + str(index) + '.'
            require(take(key + 'path') == path and take(key + 'fs') == '63677270\n', 'foreign cgroup')
            inode = take(key + 'inode_before')
            require(re.fullmatch(r'[0-9]+:[1-9][0-9]*\n', inode) and
                    take(key + 'inode_after') == inode == take(key + 'inode_end'), 'cgroup identity changed')
            require(take(key + 'cgroup.type') == 'domain\n', 'unsupported threaded/invalid cgroup')
            before, after = pids(take(key + 'cgroup.procs')), pids(take(key + 'procs_end'))
            start_events, end_events = events(take(key + 'cgroup.events')), events(take(key + 'events_end'))
            if phase != 'running':
                require(not before and not after and start_events['populated'] == end_events['populated'] == 0,
                        'owned cgroup still populated')
            groups.append({'path': path[len('/sys/fs/cgroup'):], 'inode': inode.strip(),
                           'pids_before': before, 'pids_after': after, 'events_before': start_events,
                           'events_after': end_events})
    ids = sorted({int(k.split('.')[1]) for k in fields if re.fullmatch(r'p\.[0-9]+\..+', k)})
    require(0 < len(ids) <= PROCESS_LIMIT and all(0 < p < 2**31 for p in ids), 'invalid process inventory')
    paths = take('process_paths').splitlines()
    require(len(paths) == len(set(paths)) and all(re.fullmatch(r'/proc/[A-Za-z0-9_.-]+', p) for p in paths),
            'invalid process directory inventory')
    listed = sorted(int(p[6:]) for p in paths if re.fullmatch(r'/proc/[1-9][0-9]*', p))
    require(ids == listed, 'missing or unlisted process records')
    processes, vanished = [], []
    for pid in ids:
        key = 'p.' + str(pid) + '.'
        if key + 'gone' in fields:
            require(take(key + 'gone') == 'absent', 'invalid vanished process')
            vanished.append(pid)
            continue
        before = process_stat(take(key + 'stat_before'), pid)
        after = process_stat(take(key + 'stat_after'), pid)
        require(before['starttime_ticks'] == after['starttime_ticks'] and
                before['kernel_thread'] == after['kernel_thread'], 'process birth changed while reading')
        status = take(key + 'status')
        matches = re.findall(r'^NSpid:\s+([0-9]+(?:\s+[0-9]+)*)$', status, flags=re.M)
        require(len(matches) == 1, 'missing/ambiguous namespace PID status')
        nspid = [int(x) for x in matches[0].split()]
        require(nspid[0] == pid and 1 <= len(nspid) <= 32 and all(0 < x < 2**31 for x in nspid),
                'namespace PID mapping differs')
        ns = {}
        # Pinned Linux 6.12.85: do_exit (kernel/exit.c) releases nsproxy via
        # exit_task_namespaces (kernel/nsproxy.c) before exit_notify. mntns_get
        # (fs/namespace.c) and cgroupns_get (kernel/cgroup/namespace.c) then
        # return NULL; fs/nsfs.c ns_get_name reports ENOENT. pidns_get and
        # userns_get instead use the task PID/credentials. Only a stable terminal
        # record may use this narrow missing-nsproxy case; PID/user must remain
        # readable. This does not mean the task or its old namespace vanished.
        terminal = before['state'] == after['state'] and before['state'] in ('Z', 'X')
        for n in NS:
            value = take(key + n)
            require(value == take(key + n + '.after'), 'namespace changed during process observation')
            if value == 'unavailable':
                require((n == 'mnt' and before['kernel_thread']) or
                        (n in ('mnt', 'cgroup') and terminal), 'unreadable live process namespace')
                ns[n] = None
            else:
                ns[n] = namespace(value, n)
        member = take(key + 'membership')
        require(member == take(key + 'membership_after'), 'process cgroup changed during observation')
        path = membership(member)
        if phase != 'running':
            require(path != owned and not path.startswith(owned + '/'), 'process remains in owned cgroup subtree')
        processes.append(dict(before, state_after=after['state'], namespaces=ns, nspid=nspid, cgroup=path))
    require(set(fields) == used, 'unknown or contradictory fields')
    target = None
    if phase == 'running':
        selected = [p for p in processes if p['pid'] == state['Pid']]
        require(len(selected) == 1, 'running inspect PID unavailable')
        target = selected[0]
        require(target['starttime_ticks'] > 0 and target['state'] not in ('Z', 'X', 'x') and
                target['state_after'] not in ('Z', 'X', 'x') and
                not target['kernel_thread'] and
                target['nspid'][-1] == 1 and len(target['nspid']) >= 2 and
                all(target['namespaces'][n] != guest[n] for n in ('pid', 'mnt')),
                'target lacks live private PID/mount namespaces')
        require(target['cgroup'] == owned or target['cgroup'].startswith(owned + '/'), 'target outside owned cgroup')
        require(any(g['path'] == target['cgroup'] and target['pid'] in g['pids_before'] and
                    target['pid'] in g['pids_after'] for g in groups), 'target missing from stable owned cgroup')
    if previous is not None:
        require(isinstance(previous, dict) and previous.get('phase') == 'running' and
                previous.get('container_id') == cid and previous.get('boot_id') == boot.strip() and
                previous.get('started_at') == state.get('StartedAt') and previous.get('target') is not None,
                'foreign previous process generation')
        require(phase != 'running', 'previous proof only used for absence')
        old = previous['target']
        prior_groups = {g['path']: g['inode'] for g in previous['groups']}
        require(all(g['path'] not in prior_groups or g['inode'] == prior_groups[g['path']] for g in groups),
                'previous owned cgroup identity was replaced')
        require(all(not (p['pid'] == old['pid'] and p['starttime_ticks'] == old['starttime_ticks']) and
                    all(p['namespaces'][n] != old['namespaces'][n] for n in ('pid', 'mnt'))
                    for p in processes), 'old process birth or private namespace remains')
        require(old['pid'] not in vanished, 'old PID disappearance raced observation')
    return {'schema_version': 1, 'scope': 'bounded_interval_not_historical_runtime_or_full_process_certification',
            'phase': phase, 'container_id': cid, 'started_at': state.get('StartedAt'), 'boot_id': boot.strip(),
            'uptime_centiseconds': uptime, 'guest_namespaces': guest, 'target': target,
            # Full records remain in the raw stdout, independently parsed above
            # on every replay. Avoid copying unrelated process detail into each
            # nested parent request/result; this digest never replaces parsing.
            'process_count': len(processes),
            'process_inventory_sha256': hashlib.sha256(json.dumps(processes, sort_keys=True,
                separators=(',', ':'), ensure_ascii=True).encode('ascii')).hexdigest(),
            'vanished_pids': vanished, 'groups': groups,
            'owned_cgroup_absent': group_state == 'absent',
            'recorded_birth_and_private_namespace_members_absent': previous is not None,
            'full_process_absence_certified': False, 'stdout_sha256': hashlib.sha256(raw).hexdigest()}


def capture(harness, descriptor, inspected, *, engine_policy, phase='running', previous=None,
            expected_boot_id=None, label='container-process'):
    """One exact-Machine public Exec; caller must bracket with ownership guards."""
    cid = policy(engine_policy, inspected)
    project = binding.project_binding(harness, descriptor)
    owner = descriptor['owner']
    raw, stderr, code = harness.command(label, [harness.cli, 'exec', '--environment', owner['environment_id'],
        '--machine', owner['machine_id'], '--no-stdin', '--timeout', '30', '--', '/bin/busybox', 'sh', '-c',
        probe_script(cid)], cwd=Path(project['project_path']), timeout=40, success=False)
    require(type(code) is int and code == 0 and stderr == b'', 'public Exec failed; retain raw evidence')
    require(binding.project_binding(harness, descriptor) == project, 'Machine/project binding changed')
    proof = validate(raw, inspected=inspected, engine_policy=engine_policy, phase=phase, previous=previous,
                     expected_boot_id=expected_boot_id)
    proof.update(owner=dict(owner), command_label=label, **project)
    return proof
