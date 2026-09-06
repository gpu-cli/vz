"""Pure synthetic kernel-record adversaries; no guest/runtime launch."""
import base64
import copy
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

import linux_docker_container_process as process

CID = 'a' * 64
BOOT = 'b1234567-1111-2222-3333-0123456789ab'
POLICY = {'CgroupDriver': 'cgroupfs', 'CgroupVersion': '2'}
INSPECT = {'Id': CID, 'HostConfig': {'CgroupParent': ''},
           'State': {'Running': True, 'Pid': 100, 'StartedAt': '2026-09-06T12:00:00.000000001Z'}}


def stat(pid, birth=10, state='S', kernel=False):
    fields = ['0'] * 49
    fields[5] = str(0x200000 if kernel else 0)
    fields[18] = str(birth)
    return str(pid) + ' (a name with ) parens) ' + state + ' ' + ' '.join(fields) + '\n'


def add_process(fields, pid, *, owned=False, birth=10, state='S', kernel=False):
    key = 'p.' + str(pid) + '.'
    fields[key + 'stat_before'] = fields[key + 'stat_after'] = stat(pid, birth, state, kernel)
    fields[key + 'status'] = 'Name:\ttest\nNSpid:\t' + str(pid) + ('\t1' if owned else '') + '\n'
    fields[key + 'membership'] = fields[key + 'membership_after'] = '0::' + (
        '/docker/' + CID if owned else '/') + '\n'
    for index, name in enumerate(process.NS):
        value = name + ':[' + str(index + (100 if owned and name in ('pid', 'mnt') else 1)) + ']'
        if kernel and name == 'mnt':
            value = 'unavailable'
        fields[key + name] = fields[key + name + '.after'] = value


def snapshot(*, running=True, present=True):
    fields = {'boot_before': BOOT + '\n', 'boot_after': BOOT + '\n',
              'uptime_before': '100.00 500.00\n', 'uptime_after': '100.50 501.00\n',
              'cgroup_fs': '63677270\n',
              'mountinfo': '1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n'}
    for index, name in enumerate(process.NS):
        fields['guest.' + name] = fields['observer.' + name] = name + ':[' + str(index + 1) + ']\n'
    fields['group_state'] = fields['group_state_end'] = 'present' if present else 'absent'
    if present:
        path = '/sys/fs/cgroup/docker/' + CID
        for key in ('groups_before', 'groups_after', 'groups_end'):
            fields[key] = path
        fields['g.1.path'] = path
        for suffix in ('before', 'after', 'end'):
            fields['g.1.inode_' + suffix] = '25:999\n'
        fields['g.1.fs'] = '63677270\n'
        fields['g.1.cgroup.type'] = 'domain\n'
        fields['g.1.cgroup.procs'] = fields['g.1.procs_end'] = '100\n' if running else ''
        fields['g.1.cgroup.events'] = fields['g.1.events_end'] = 'populated ' + str(int(running)) + '\nfrozen 0\n'
    add_process(fields, 1)
    if running:
        add_process(fields, 100, owned=True)
    fields['process_paths'] = '/proc/1\n/proc/sys' + ('\n/proc/100' if running else '')
    return fields


def packed(fields):
    return ('VZ_CONTAINER_PROCESS_V1\n' + ''.join(k + '=' + base64.b64encode(v.encode()).decode() + '\n'
            for k, v in fields.items()) + 'VZ_CONTAINER_PROCESS_END\n').encode()


def validate(fields, phase='running', previous=None, **kwargs):
    inspected = copy.deepcopy(INSPECT)
    if phase == 'stopped':
        inspected['State'].update(Running=False, Pid=0)
    return process.validate(packed(fields), inspected=inspected, engine_policy=POLICY,
                            phase=phase, previous=previous, **kwargs)


class ProcessTests(unittest.TestCase):
    def test_running_birth_private_namespaces_and_shared_namespaces(self):
        proof = validate(snapshot())
        self.assertEqual(proof['target']['starttime_ticks'], 10)
        self.assertEqual(proof['target']['nspid'], [100, 1])
        for name in ('user', 'cgroup'):
            self.assertEqual(proof['target']['namespaces'][name], proof['guest_namespaces'][name])
        self.assertFalse(proof['full_process_absence_certified'])
        self.assertFalse(proof['recorded_birth_and_private_namespace_members_absent'])
        self.assertNotIn('processes', proof)
        self.assertEqual(proof['process_count'], 2)
        self.assertRegex(proof['process_inventory_sha256'], r'^[0-9a-f]{64}$')
        self.assertEqual(proof['process_inventory_sha256'], validate(snapshot())['process_inventory_sha256'])
        fields = snapshot()
        fields['p.1.stat_before'] = fields['p.1.stat_after'] = stat(1, birth=11)
        self.assertNotEqual(proof['process_inventory_sha256'], validate(fields)['process_inventory_sha256'])

    def test_stopped_and_removed_empty_or_absent(self):
        previous = validate(snapshot())
        for phase in ('stopped', 'removed'):
            for present in (True, False):
                proof = validate(snapshot(running=False, present=present), phase, previous)
                self.assertTrue(proof['recorded_birth_and_private_namespace_members_absent'])
                self.assertFalse(proof['full_process_absence_certified'])
                self.assertEqual(proof['owned_cgroup_absent'], not present)

    def test_cgroup_only_requires_external_boot_never_invents_birth(self):
        fields = snapshot(running=False, present=False)
        with self.assertRaises(ValueError):
            validate(fields, 'removed')
        proof = validate(fields, 'removed', expected_boot_id=BOOT)
        self.assertIsNone(proof['target'])
        self.assertFalse(proof['recorded_birth_and_private_namespace_members_absent'])
        with self.assertRaises(ValueError):
            validate(fields, 'removed', expected_boot_id='foreign')

    def test_pid_reuse_distinct_birth_and_private_namespaces(self):
        previous = validate(snapshot())
        fields = snapshot(running=False)
        add_process(fields, 100, birth=11)
        fields['process_paths'] += '\n/proc/100'
        self.assertTrue(validate(fields, 'stopped', previous)['recorded_birth_and_private_namespace_members_absent'])
        for state in ('S', 'Z', 'X'):
            fields['p.100.stat_before'] = fields['p.100.stat_after'] = stat(100, 10, state)
            with self.subTest(state=state), self.assertRaises(ValueError):
                validate(fields, 'stopped', previous)

    def test_remaining_private_namespace_or_actual_membership_rejected(self):
        previous = validate(snapshot())
        for name in ('pid', 'mnt', 'membership'):
            fields = snapshot(running=False)
            add_process(fields, 200)
            fields['process_paths'] += '\n/proc/200'
            if name == 'membership':
                fields['p.200.membership'] = fields['p.200.membership_after'] = '0::/docker/' + CID + '/child\n'
            else:
                fields['p.200.' + name] = fields['p.200.' + name + '.after'] = previous['target']['namespaces'][name]
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate(fields, 'removed', previous)

    def test_live_namespace_unreadable_and_moving_rejected(self):
        for key, value in (('p.100.pid', 'unavailable'), ('p.100.pid.after', 'pid:[900]'),
                           ('p.100.membership_after', '0::/\n'), ('p.100.stat_after', stat(100, 11)),
                           ('p.100.status', 'Name:\ttest\n'), ('p.100.stat_before', stat(100, state='Z'))):
            fields = snapshot(); fields[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(fields)

    def test_kernel_thread_missing_mount_namespace_is_not_user_process(self):
        fields = snapshot(); add_process(fields, 2, kernel=True, birth=0); fields['process_paths'] += '\n/proc/2'
        self.assertEqual(validate(fields)['process_count'], 3)
        fields['p.2.stat_before'] = fields['p.2.stat_after'] = stat(2)
        with self.assertRaises(ValueError):
            validate(fields)

    def test_unrelated_stable_zombie_is_recorded_not_absent(self):
        previous = validate(snapshot())
        for state in ('Z', 'X'):
            fields = snapshot(running=False)
            add_process(fields, 20, state=state)
            fields['process_paths'] += '\n/proc/20'
            for name in ('mnt', 'cgroup'):
                fields['p.20.' + name] = fields['p.20.' + name + '.after'] = 'unavailable'
            with self.subTest(state=state):
                proof = validate(fields, 'stopped', previous)
                self.assertEqual(proof['process_count'], 2)
                self.assertEqual(proof['vanished_pids'], [])
                self.assertTrue(proof['recorded_birth_and_private_namespace_members_absent'])
                self.assertFalse(proof['full_process_absence_certified'])

    def test_zombie_changed_live_birth_and_unreadable_pid_user_rejected(self):
        baseline = snapshot()
        add_process(baseline, 20, state='Z')
        baseline['process_paths'] += '\n/proc/20'
        for name in ('mnt', 'cgroup'):
            baseline['p.20.' + name] = baseline['p.20.' + name + '.after'] = 'unavailable'
        mutations = [ {'p.20.stat_after': stat(20, state='X')},
                      {'p.20.stat_after': stat(20, birth=11, state='Z')},
                      {'p.20.stat_before': stat(20), 'p.20.stat_after': stat(20)},
                      {'p.20.mnt.after': 'mnt:[5]'} ]
        for name in ('pid', 'user'):
            mutations.append({'p.20.' + name: 'unavailable', 'p.20.' + name + '.after': 'unavailable'})
        for mutation in mutations:
            fields = copy.deepcopy(baseline); fields.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate(fields)

    def test_zombie_old_birth_namespace_or_owned_membership_never_absent(self):
        previous = validate(snapshot())
        for reason in ('birth', 'pid', 'mnt', 'membership'):
            fields = snapshot(running=False)
            pid = 100 if reason == 'birth' else 20
            add_process(fields, pid, state='Z')
            fields['process_paths'] += '\n/proc/' + str(pid)
            prefix = 'p.' + str(pid) + '.'
            for name in ('mnt', 'cgroup'):
                fields[prefix + name] = fields[prefix + name + '.after'] = 'unavailable'
            if reason in ('pid', 'mnt'):
                fields[prefix + reason] = fields[prefix + reason + '.after'] = previous['target']['namespaces'][reason]
            elif reason == 'membership':
                fields[prefix + 'membership'] = fields[prefix + 'membership_after'] = '0::/docker/' + CID + '\n'
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                validate(fields, 'stopped', previous)

    def test_inventory_byte_limit_before_newline_stripping(self):
        # Execute only the exact bounded-list function with a finite local
        # Python producer and a head applet shim; never run the guest probe.
        script = ('set -eu\nset -o pipefail\nLC_ALL=C\nexport LC_ALL\n'
                  'bb() { command "$@"; }\nbb=bb\n' + process.BOUNDED_LIST +
                  '\nvalue=$(bounded_list "$@")\nprintf accepted\n')
        for size in (16384, 16385, 16386):
            producer = 'import sys; sys.stdout.buffer.write(b"x" * ' + str(size - 1) + ' + b"\\n")'
            row = subprocess.run(['/bin/bash', '-c', script, 'bound-test', sys.executable, '-c', producer],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5,
                                 env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
            with self.subTest(size=size):
                self.assertEqual(row.returncode == 0, size == 16384)
                self.assertEqual(row.stdout, b'accepted' if size == 16384 else b'')

    def test_vanished_process_record_and_missing_inventory(self):
        fields = snapshot(); fields['process_paths'] += '\n/proc/20'; fields['p.20.gone'] = 'absent'
        self.assertEqual(validate(fields)['vanished_pids'], [20])
        del fields['p.20.gone']
        with self.assertRaises(ValueError):
            validate(fields)
        previous = validate(snapshot())
        fields = snapshot(running=False); fields['process_paths'] += '\n/proc/100'; fields['p.100.gone'] = 'absent'
        with self.assertRaises(ValueError):
            validate(fields, 'stopped', previous)

    def test_cgroup_mutations_populated_and_prefix_alias(self):
        for key, value in (('g.1.inode_end', '25:1000\n'), ('groups_end', ''),
                           ('g.1.cgroup.type', 'domain invalid\n'), ('g.1.fs', '0\n'),
                           ('g.1.procs_end', '200\n'), ('g.1.events_end', 'populated 1\nfrozen 0\n')):
            fields = snapshot(running=False); fields[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(fields, 'stopped', expected_boot_id=BOOT)
        fields = snapshot()
        fields['p.100.membership'] = fields['p.100.membership_after'] = '0::/docker/' + CID + '-evil\n'
        with self.assertRaises(ValueError):
            validate(fields)

    def test_depth_sentinel_and_replaced_previous_inode(self):
        for depth in (8, 9):
            fields = snapshot()
            root = fields['g.1.path']
            for number in range(1, depth + 1):
                child = root + '/child' * number
                for name in ('groups_before', 'groups_after', 'groups_end'):
                    fields[name] += '\n' + child
                key_prefix = 'g.' + str(number + 1) + '.'
                for key, value in list(fields.items()):
                    if key.startswith('g.1.'):
                        fields[key.replace('g.1.', key_prefix)] = value
                fields[key_prefix + 'path'] = child
                for suffix in ('before', 'after', 'end'):
                    fields[key_prefix + 'inode_' + suffix] = '25:' + str(1000 + number) + '\n'
            if depth == 8:
                self.assertEqual(len(validate(fields)['groups']), 9)
            else:
                with self.assertRaises(ValueError):
                    validate(fields)
        previous = validate(snapshot())
        fields = snapshot(running=False)
        for suffix in ('before', 'after', 'end'):
            fields['g.1.inode_' + suffix] = '25:1000\n'
        with self.assertRaises(ValueError):
            validate(fields, 'stopped', previous)

    def test_foreign_mount_observer_boot_and_policy(self):
        for key, value in (('boot_after', '00000000-0000-0000-0000-000000000000\n'),
                           ('observer.pid', 'pid:[50]\n'), ('uptime_after', '200.00 501.00\n'),
                           ('mountinfo', '1 0 0:1 /foreign /sys/fs/cgroup rw - cgroup2 cgroup rw\n')):
            fields = snapshot(); fields[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(fields)
        fields = snapshot()
        fields['mountinfo'] += '2 1 0:1 /evil /sys/fs/cgroup/docker/' + CID + ' rw - cgroup2 cgroup rw\n'
        with self.assertRaises(ValueError):
            validate(fields)
        for policy in ({'CgroupDriver': 'systemd', 'CgroupVersion': '2'},
                       {'CgroupDriver': 'cgroupfs', 'CgroupVersion': '1'}):
            with self.assertRaises(ValueError):
                process.validate(packed(snapshot()), inspected=INSPECT, engine_policy=policy, phase='running')

    def test_foreign_previous_generation(self):
        previous = validate(snapshot())
        for key, value in (('container_id', 'b' * 64), ('boot_id', 'foreign'), ('started_at', 'foreign'),
                           ('phase', 'stopped'), ('target', None)):
            row = copy.deepcopy(previous); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(snapshot(running=False), 'stopped', row)

    def test_frame_truncation_duplicate_unknown_limit_and_noncanonical(self):
        raw = packed(snapshot())
        for value in (raw[:-1], raw.replace(b'boot_before=', b'unknown='),
                      raw.replace(b'boot_before=', b'boot_after='),
                      raw.replace(b'boot_before=', b'boot_before=!'), b'x' * (process.STREAM_LIMIT + 1)):
            with self.subTest(size=len(value)), self.assertRaises(ValueError):
                process.validate(value, inspected=INSPECT, engine_policy=POLICY, phase='running')
        fields = snapshot(); fields['p.1.status'] = 'x' * (process.FIELD_LIMIT + 1)
        with self.assertRaises(ValueError):
            validate(fields)

    def test_script_is_read_only_source_bound_and_limits_explicit(self):
        script = process.probe_script(CID)
        for word in ('kill ', 'killall', 'mkdir', 'mount ', 'rm ', 'chmod', 'chown'):
            self.assertNotIn(word, script)
        self.assertIn('/sys/fs/cgroup/docker/' + CID, script)
        self.assertIn('head -c 16385', script)
        self.assertIn('"$count" -le 1024', script)
        self.assertIn('"$count" -le 128', script)
        with self.assertRaises(ValueError):
            process.probe_script(CID + '; touch /bad')

    def test_capture_exact_public_machine_command_and_failure_preservation(self):
        descriptor = {'owner': {'environment_id': 'env-a', 'machine_id': 'mch-a', 'project_id': 'prj-a'}}
        project = {'project_path': '/private/tmp/owned/project'}
        harness = SimpleNamespace(cli='/exact/vz', command=mock.Mock(return_value=(packed(snapshot()), b'', 0)))
        with mock.patch.object(process.binding, 'project_binding', return_value=project):
            proof = process.capture(harness, descriptor, INSPECT, engine_policy=POLICY)
        args, kwargs = harness.command.call_args
        self.assertEqual(args[1][:11], ['/exact/vz', 'exec', '--environment', 'env-a', '--machine', 'mch-a',
                                      '--no-stdin', '--timeout', '30', '--', '/bin/busybox'])
        self.assertEqual(args[1][-1], process.probe_script(CID))
        self.assertEqual(kwargs, {'cwd': Path(project['project_path']), 'timeout': 40, 'success': False})
        self.assertEqual(proof['owner'], descriptor['owner'])
        harness.command.return_value = (b'partial', b'error', 1)
        with mock.patch.object(process.binding, 'project_binding', return_value=project), self.assertRaises(ValueError):
            process.capture(harness, descriptor, INSPECT, engine_policy=POLICY)


if __name__ == '__main__':
    unittest.main()
