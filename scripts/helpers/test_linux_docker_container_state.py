"""Synthetic raw-state adversaries; no Docker/VM or process execution."""
import copy
import json
import unittest

import linux_docker_container_state as state

CID = 'a' * 64
IMAGE = 'sha256:' + 'b' * 64
TOKEN = 'vzio-' + 'c' * 24
NAME = 'owned-container'
ZERO = state.ZERO


def stamp(second):
    return '2026-09-06T12:00:%02dZ' % second


def row(status='running', health=False):
    config = {'Labels': {state.LABEL: TOKEN}, 'Cmd': ['service', TOKEN],
              'Entrypoint': list(state.ENTRYPOINT), 'User': '', 'WorkingDir': '/workspace',
              'Tty': False, 'OpenStdin': False, 'Volumes': None}
    if health:
        config['Healthcheck'] = {'Test': state.health_command(TOKEN), 'Retries': 1,
                                'Interval': 10**9, 'Timeout': 10**9, 'StartPeriod': 30*10**9}
    return {'Id': CID, 'Name': '/' + NAME, 'Image': IMAGE, 'Created': stamp(0), 'Config': config,
            'Path': 'python3', 'Args': ['-u', '/fixture/probe.py', 'service', TOKEN],
            'HostConfig': {'Runtime': 'youki', 'NetworkMode': 'none', 'Privileged': False,
                           'PublishAllPorts': False, 'Binds': None, 'Mounts': None,
                           'VolumesFrom': None, 'Devices': [], 'CapAdd': None,
                           'PortBindings': {}, 'PidMode': '', 'IpcMode': 'private',
                           'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': 0},
                           'OomKillDisable': None},
            'RestartCount': 0, 'Mounts': [],
            'State': {'Status': status, 'Running': status == 'running', 'Paused': False,
                      'Restarting': False, 'Dead': False, 'OOMKilled': False, 'Error': '',
                      'Pid': 100 if status == 'running' else 0, 'ExitCode': 143 if status == 'exited' else 0,
                      'StartedAt': ZERO if status == 'created' else stamp(1),
                      'FinishedAt': stamp(2) if status == 'exited' else ZERO}}


def admit(value, **kwargs):
    return state.inspect_container(json.dumps([value]).encode(), cid=CID, name=NAME,
                                   image_id=IMAGE, token=TOKEN, command=['service', TOKEN],
                                   state=value['State']['Status'], **kwargs)


def health_samples():
    out = []
    for index, status in enumerate(('starting', 'healthy', 'unhealthy')):
        value = row(health=True)
        value['State']['Health'] = {'Status': status, 'FailingStreak': 1 if index == 2 else 0,
                                   'Log': [{'Start': stamp(2+index*2), 'End': stamp(3+index*2),
                                            'ExitCode': 0 if index == 1 else 1, 'Output': ''}]}
        out.append(admit(value, health=True))
    return out


def events(actions=('create', 'start', 'die', 'destroy')):
    result = []
    for index, action in enumerate(actions):
        attrs = {state.LABEL: TOKEN, 'name': NAME, 'image': IMAGE}
        if action == 'die':
            attrs.update(exitCode='143', execDuration='1')
        result.append({'type': 'container', 'action': action, 'scope': 'local', 'id': CID,
                       'attributes': attrs, 'time_nano': 100+index})
    return result


def event_validate(rows):
    return state.validate_events(b''.join(json.dumps(x).encode()+b'\n' for x in rows),
                                 cid=CID, name=NAME, image=IMAGE, token=TOKEN, since=100, until=1000)


class InspectTests(unittest.TestCase):
    def test_three_states_and_non_signal_claim(self):
        for expected in ('created', 'running', 'exited'):
            value = row(expected)
            self.assertEqual(admit(value), value)
        proof = state.stopped(admit(row('exited')), 143)
        self.assertFalse(proof['signal_delivery_certified'])
        self.assertTrue(proof['youki_process_inventory_required'])

    def test_command_entrypoint_and_interactive_binding(self):
        value = row()
        value['Config']['Tty'] = value['Config']['OpenStdin'] = True
        self.assertEqual(admit(value, interactive=True, tty=True), value)
        with self.assertRaises(ValueError):
            admit(value)
        value = row()
        value['Config']['Entrypoint'] = ['/bin/sh', '-c']
        value['Path'] = '/bin/sh'
        value['Args'] = ['-c', 'service', TOKEN]
        with self.assertRaises(ValueError):
            admit(value)
        admit(value, entrypoint=['/bin/sh', '-c'])

    def test_identity_configuration_and_type_tampering(self):
        changes = [('Id', 'd'*64), ('Name', '/foreign'), ('Image', 'sha256:'+'e'*64),
                   ('Config.Labels', {state.LABEL: 'vzio-'+'d'*24}), ('Config.Cmd', ['exit', '0']),
                   ('Config.User', '10001'), ('Config.WorkingDir', '/'), ('Config.Tty', 0),
                   ('HostConfig.Runtime', 'runc'), ('HostConfig.NetworkMode', 'host'),
                   ('HostConfig.Privileged', 0), ('HostConfig.Binds', ['/host:/guest']),
                   ('HostConfig.Mounts', [{}]), ('HostConfig.CapAdd', ['SYS_ADMIN']),
                   ('HostConfig.Devices', [{}]), ('HostConfig.PidMode', 'host'),
                   ('Mounts', [{}]), ('Config.Volumes', {'/data': {}}),
                   ('HostConfig.RestartPolicy.MaximumRetryCount', False), ('RestartCount', True),
                   ('State.Pid', True), ('State.Running', 1), ('State.OOMKilled', True),
                   ('State.Error', 'failure'), ('State.ExitCode', False), ('State.StartedAt', ZERO),
                   ('State.FinishedAt', stamp(30)), ('Path', '/bin/sh')]
        for field, value in changes:
            with self.subTest(field=field):
                candidate = row(); target = candidate
                parts = field.split('.')
                for part in parts[:-1]:
                    target = target[part]
                target[parts[-1]] = value
                with self.assertRaises(ValueError):
                    admit(candidate)

    def test_raw_shape_duplicates_and_limits(self):
        args = dict(cid=CID, name=NAME, image_id=IMAGE, token=TOKEN,
                    command=['service', TOKEN], state='running')
        for raw in (b'[]', b'{}', json.dumps([row(), row()]).encode(), b'x'*65537,
                    json.dumps([row()]).replace('"Pid": 100', '"Pid": 100, "Pid": 100').encode()):
            with self.assertRaises(ValueError):
                state.inspect_container(raw, **args)

    def test_created_and_exited_coherence(self):
        for status, field, value in [('created', 'Pid', 1), ('created', 'StartedAt', stamp(1)),
                                     ('created', 'ExitCode', 37), ('exited', 'Pid', 100),
                                     ('exited', 'FinishedAt', ZERO), ('exited', 'FinishedAt', stamp(0))]:
            candidate = row(status); candidate['State'][field] = value
            with self.assertRaises(ValueError):
                admit(candidate)

    def test_same_identity_and_restart(self):
        before, after = admit(row()), admit(row())
        self.assertTrue(state.same_generation(before, after))
        with self.assertRaises(ValueError):
            state.new_generation(before, after)
        after['State']['StartedAt'] = stamp(3)
        self.assertTrue(state.new_generation(before, after))  # PID may be recycled.
        with self.assertRaises(ValueError):
            state.same_generation(before, after)
        after['Config']['UnrelatedNewField'] = 'drift'
        with self.assertRaises(ValueError):
            state.same_identity(before, after)

    def test_start_policy_single_exact_transition(self):
        before = row('created'); before['HostConfig']['OomKillDisable'] = False
        after = row()
        saved = copy.deepcopy(before)
        policy = {'ID': 'engine-id', 'ServerVersion': '29.7.2', 'CgroupVersion': '2', 'OomKillDisable': False}
        args = dict(start_policy=policy, engine_id='engine-id', start_acknowledged=True)
        self.assertTrue(state.same_identity(before, after, **args))
        self.assertEqual(before, saved)
        for field, value in [('ID', 'other'), ('ServerVersion', '29.7.1'), ('CgroupVersion', 2),
                             ('OomKillDisable', 0), ('OomKillDisable', True)]:
            bad = dict(policy); bad[field] = value
            with self.assertRaises(ValueError):
                state.same_identity(before, after, **dict(args, start_policy=bad))
        for alternate in ({}, dict(args, start_acknowledged=False), dict(args, start_acknowledged=1)):
            with self.assertRaises(ValueError):
                state.same_identity(before, after, **alternate)
        after['Config']['User'] = '0'
        with self.assertRaises(ValueError):
            state.same_identity(before, after, **args)
        after = row(); before['State']['Status'] = 'running'
        with self.assertRaises(ValueError):
            state.same_identity(before, after, **args)
        before = row('created'); before['HostConfig'].pop('OomKillDisable')
        with self.assertRaises(ValueError):
            state.same_identity(before, row(), **args)


class HealthTests(unittest.TestCase):
    def test_real_probe_sequence_and_intermediate_polls(self):
        samples = health_samples()
        samples.insert(1, copy.deepcopy(samples[0]))
        empty = copy.deepcopy(samples[0]); empty['State']['Health']['Log'] = []
        samples.insert(0, empty)
        self.assertEqual(state.health_transition(samples, TOKEN)['transitions'], ['starting', 'healthy', 'unhealthy'])

    def test_status_not_sufficient_without_actual_probe(self):
        for field, value in [('Log', []), ('FailingStreak', True), ('Status', 'healthy')]:
            samples = health_samples(); samples[0]['State']['Health'][field] = value
            with self.assertRaises(ValueError):
                state.health_transition(samples, TOKEN)

    def test_health_probe_and_identity_tampering(self):
        for kind in ('output', 'exit_bool', 'end', 'generation', 'order', 'command', 'streak'):
            samples = health_samples()
            log = samples[1]['State']['Health']['Log'][0]
            if kind == 'output': log['Output'] = 'error'
            if kind == 'exit_bool': log['ExitCode'] = False
            if kind == 'end': log['End'] = stamp(0)
            if kind == 'generation': samples[1]['State']['Pid'] += 1
            if kind == 'order': samples.reverse()
            if kind == 'command': samples[1]['Config']['Healthcheck']['Test'][-1] = 'foreign'
            if kind == 'streak': samples[2]['State']['Health']['FailingStreak'] = 0
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                state.health_transition(samples, TOKEN)

    def test_health_configuration_is_pinned(self):
        for field, value in [('Interval', 2*10**9), ('Timeout', True), ('Retries', True),
                             ('StartPeriod', 0), ('Test', ['CMD', 'true']), ('Unknown', 0)]:
            candidate = row(health=True); candidate['Config']['Healthcheck'][field] = value
            with self.assertRaises(ValueError):
                admit(candidate, health=True)
        with self.assertRaises(ValueError):
            admit(row(health=True))


class EventTests(unittest.TestCase):
    def test_required_and_preserved_extras_restarts(self):
        rows = events(('create', 'attach', 'start', 'health_status: healthy', 'kill', 'die',
                       'stop', 'start', 'restart', 'exec_create: python3', 'exec_start: python3',
                       'exec_die', 'die', 'destroy'))
        proof = event_validate(rows)
        self.assertEqual(proof['events'], rows)
        self.assertEqual(proof['lifecycle'], ['create', 'start', 'die', 'start', 'die', 'destroy'])
        self.assertFalse(proof['signal_delivery_certified'])

    def test_wrong_ownership_window_and_duplicates(self):
        for kind in ('id', 'token', 'name', 'image', 'early', 'future', 'bool', 'duplicate', 'reverse'):
            rows = events()
            if kind == 'id': rows[1]['id'] = 'd'*64
            if kind in ('token', 'name', 'image'):
                rows[1]['attributes'][state.LABEL if kind == 'token' else kind] = 'other'
            if kind == 'early': rows[1]['time_nano'] = 99
            if kind == 'future': rows[1]['time_nano'] = 1001
            if kind == 'bool': rows[1]['time_nano'] = True
            if kind == 'duplicate': rows.insert(2, copy.deepcopy(rows[1]))
            if kind == 'reverse': rows.reverse()
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                event_validate(rows)

    def test_invalid_lifecycle_and_unknown_extras(self):
        for actions in [('create', 'die', 'destroy'), ('create', 'start', 'destroy'),
                        ('create', 'start', 'start', 'die', 'destroy'),
                        ('create', 'start', 'die'), ('create', 'start', 'die', 'destroy', 'stop'),
                        ('create', 'start', 'made-up', 'die', 'destroy')]:
            with self.subTest(actions=actions), self.assertRaises(ValueError):
                event_validate(events(actions))

    def test_exit_cause_and_raw_shape(self):
        for code in ('-1', '256', '0137', 'not-an-exit'):
            rows = events(); rows[2]['attributes']['exitCode'] = code
            with self.assertRaises(ValueError):
                event_validate(rows)
        raw = b''.join(json.dumps(x).encode()+b'\n' for x in events())
        args = dict(cid=CID, name=NAME, image=IMAGE, token=TOKEN, since=100, until=1000)
        for bad in (raw[:-1], raw.replace(b'"scope": "local"', b'"scope": "local", "scope": "local"'), b'\n'+raw):
            with self.assertRaises(ValueError):
                state.validate_events(bad, **args)


if __name__ == '__main__':
    unittest.main()
