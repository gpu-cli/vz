"""Installed-Machine adapter for same-Environment Docker isolation.

Covers `docker.operation.same_environment_isolation`: two Developer Linux
Machines in one Environment, whose private Engines share nothing. Each Machine
creates one owned container, image tag, volume and network, then records its
complete container/image/volume/network inventory, its own event stream and its
build-cache record ids. `verify_machines` requires that no identity owned by one
Machine appears anywhere in another's inventory, and that a lifecycle operation
on one Machine leaves the other's container in the same running generation.

The workload image is the harness sentinel's digest-pinned developer probe
rootfs; nothing is pulled and no fixture script is needed, because the claim is
about what each Engine can see rather than about what runs inside a container.

Cache records are reported with their observed counts, so a reader can tell a
real disjointness from a vacuous one. `full_isolation_certified` stays false:
this covers the categories it enumerates on the Machines it was given, and the
sibling-Environment claim is a separate row that needs a third Environment.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_same_Environment_Machine_isolation_not_release_certification'
HELPERS = Path(__file__).resolve().parent
LABEL = 'dev.vz.isolation-proof'
BUSYBOX = '/bin/busybox'
SLEEP_SECONDS = 600
RUN_TIMEOUT = 60
KINDS = ('container', 'image', 'volume', 'network')


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_isolation_machine.py', 'docker_host_driver.py', 'linux_docker_container_state.py',
        'installed_developer_startup.py', 'linux_docker_e2e.py')]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact isolation source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'isolation source changed: ' + name)


def unique(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate JSON field')
        row[key] = value
    return row


def parse(raw):
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT, 'bounded JSON stream required')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
    except (UnicodeError, ValueError) as error:
        raise ValueError('isolation: malformed JSON output') from error


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def engine_time(value):
    require(type(value) is int and value > 0, 'invalid Engine time')
    return '%d.%09d' % divmod(value, 1000000000)


def sentinel_image(harness, descriptor):
    rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and row.get('descriptor') == descriptor]
    require(len(rows) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', rows[0].get('image_id') or ''),
            'exact owned sentinel image required')
    return rows[0]['image_id']


class Session:
    """One Machine's owned resources and its complete visible inventory."""

    def __init__(self, harness, descriptor, image_id, token):
        self.harness, self.descriptor, self.image_id, self.token = harness, descriptor, image_id, token
        self.names = {'container': token + '-live', 'image': token + ':owned',
                      'volume': token + '-state', 'network': token + '-net'}
        self.owned = {}
        self.created = []
        self.since = None
        self.cleanup_complete = False

    def docker(self, label, args):
        raw, stderr, _ = self.harness.docker('isolation-' + label, self.descriptor, args)
        require(stderr == b'', label + ' emitted diagnostics')
        return raw

    def engine_clock(self, label):
        """The Engine's own clock: a host clock cannot bound its event history."""
        from linux_docker_container_state import timestamp
        value = parse(self.docker(label, ['info', '--format', '{{json .}}']))
        require(value['ID'] == self.descriptor['engine_id'], 'foreign Engine clock')
        return timestamp(value['SystemTime'])

    def create(self):
        """One resource of every kind, each carrying this Machine's own token."""
        self.since = self.engine_clock('clock-start')
        raw, stderr, _ = self.harness.mutate('isolation-network-create', self.descriptor,
            ['network', 'create', '--label', LABEL + '=' + self.token, self.names['network']])
        require(stderr == b'', 'network create diagnostics')
        self.owned['network'] = driver.checked_text(raw.decode('ascii').strip(), r'[0-9a-f]{64}', 'owned network ID')
        raw, stderr, _ = self.harness.mutate('isolation-volume-create', self.descriptor,
            ['volume', 'create', '--label', LABEL + '=' + self.token, self.names['volume']])
        require(stderr == b'' and raw.decode('ascii').strip() == self.names['volume'], 'volume create differs')
        self.owned['volume'] = self.names['volume']
        # A distinct tag on the shared sentinel layers: the tag is this
        # Machine's, the content is content-addressed and identical everywhere,
        # which is not cross-visibility.
        self.harness.mutate('isolation-image-tag', self.descriptor, ['image', 'tag', self.image_id, self.names['image']])
        self.owned['image'] = self.names['image']
        raw, stderr, _ = self.harness.mutate('isolation-run', self.descriptor,
            ['run', '--detach', '--network', self.names['network'], '--restart', 'no',
             '--name', self.names['container'], '--label', LABEL + '=' + self.token,
             '--mount', 'type=volume,src=' + self.names['volume'] + ',dst=/data',
             self.image_id, BUSYBOX, 'sleep', str(SLEEP_SECONDS)], timeout=RUN_TIMEOUT)
        require(stderr == b'', 'run diagnostics')
        self.owned['container'] = driver.checked_text(raw.decode('ascii').strip(), r'[0-9a-f]{64}', 'owned container ID')
        self.created = list(KINDS)
        return dict(self.owned)

    def generation(self):
        raw = self.docker('inspect-live', ['container', 'inspect', self.owned['container']])
        rows = parse(raw)
        require(type(rows) is list and len(rows) == 1, 'ambiguous owned container')
        state = rows[0]['State']
        require(state['Running'] is True and type(state['Pid']) is int and state['Pid'] > 0,
                'the owned container is not running')
        return {'id': rows[0]['Id'], 'started_at': state['StartedAt'], 'pid': state['Pid']}

    def inventory(self, label):
        """Everything this Machine's own Engine can see, by identity."""
        listings = {'container': ['container', 'ls', '--all', '--quiet', '--no-trunc'],
                    'image': ['image', 'ls', '--all', '--format', '{{.Repository}}:{{.Tag}}'],
                    'volume': ['volume', 'ls', '--quiet'],
                    'network': ['network', 'ls', '--quiet', '--no-trunc']}
        rows = {}
        for kind, args in listings.items():
            values = self.docker(label + '-' + kind, args).decode('ascii').split()
            require(len(values) <= 4096, 'unbounded inventory')
            rows[kind] = sorted(set(values))
        rows['cache'] = self.cache_records(label)
        rows['event'] = self.events(label)
        return rows

    def cache_records(self, label):
        """Local build-cache record ids, reported with their observed count."""
        raw = self.docker(label + '-cache', ['system', 'df', '--verbose', '--format', '{{json .BuildCache}}'])
        text = raw.decode('ascii').strip()
        if not text or text == 'null':
            return []
        rows = parse(text.encode('ascii'))
        require(type(rows) is list, 'unexpected build cache shape')
        ids = sorted({row['ID'] for row in rows if isinstance(row, dict) and row.get('ID')})
        return ids

    def events(self, label):
        """This Machine's own event history over its own Engine-clock window.

        A relative window is not usable here: the bound has to come from the
        Engine that recorded the events, and the read must be closed or the
        client streams live and never returns.
        """
        require(self.since is not None, 'the owned Engine window was never opened')
        until = self.engine_clock(label + '-clock-end')
        require(until >= self.since, 'the Engine clock went backwards')
        raw = self.docker(label + '-events', ['events', '--since', engine_time(self.since),
                                              '--until', engine_time(until),
                                              '--filter', 'label=' + LABEL + '=' + self.token,
                                              '--format', '{{.Actor.ID}}'])
        rows = sorted(set(raw.decode('ascii').split()))
        require(rows, 'the owned Engine reported no event for resources it just created')
        return rows

    def remove(self):
        if 'container' in self.owned:
            self.harness.mutate('isolation-remove-container', self.descriptor,
                                ['container', 'rm', '--force', self.owned.pop('container')])
        if 'image' in self.owned:
            self.harness.mutate('isolation-remove-image', self.descriptor, ['image', 'rm', self.owned.pop('image')])
        if 'volume' in self.owned:
            self.harness.mutate('isolation-remove-volume', self.descriptor, ['volume', 'rm', self.owned.pop('volume')])
        if 'network' in self.owned:
            self.harness.mutate('isolation-remove-network', self.descriptor, ['network', 'rm', self.owned.pop('network')])
        self.cleanup_complete = True


def verify_machines(observations):
    """No identity owned by one Machine is visible to another in the Environment.

    Two Machines of one Environment is the manifest's own precondition, checked
    here rather than inferred from whatever the caller passed. Lifecycle effects
    come from each Machine's own recheck of the Machines that preceded it: an
    observation made after it had already mutated its own Engine.
    """
    require(type(observations) is list and len(observations) >= 2, 'same-Environment isolation needs two Machines')
    by_machine = {row['owner']['machine_id']: row for row in observations}
    require(len(by_machine) == len(observations), 'two observations came from the same Machine')
    environments = {}
    for row in observations:
        environments.setdefault(row['owner']['environment_id'], []).append(row)
    shared = [rows for rows in environments.values() if len(rows) >= 2]
    require(len(shared) == 1, 'exactly one Environment must hold two of these Machines')
    same = shared[0]
    cross = []
    for row in same:
        for other in same:
            if other is row:
                for kind, identity in row['owned'].items():
                    require(identity in row['inventory'][kind], 'a Machine cannot see its own ' + kind)
                continue
            for kind, identity in row['owned'].items():
                if identity in other['inventory'][kind]:
                    cross.append({'kind': kind, 'identity': identity,
                                  'owner': row['owner']['machine_id'], 'seen_by': other['owner']['machine_id']})
            for kind in ('cache', 'event'):
                for value in sorted(set(row['inventory'][kind]) & set(other['inventory'][kind])):
                    cross.append({'kind': kind, 'identity': value, 'owner': row['owner']['machine_id'],
                                  'seen_by': other['owner']['machine_id']})
    require(not cross, 'cross-visible resources between Machines of one Environment: ' + json.dumps(cross[:5]))
    effects, rechecked = [], 0
    for row in observations:
        for recheck in row['rechecks']:
            earlier = by_machine.get(recheck['machine_id'])
            require(earlier is not None, 'a recheck names an unknown Machine')
            rechecked += 1
            if recheck['generation'] != earlier['generation']:
                effects.append({'machine_id': recheck['machine_id'], 'observed_by': row['owner']['machine_id']})
    require(not effects, 'a Machine container changed generation while another Machine was mutated: ' +
            json.dumps(effects[:5]))
    require(rechecked >= 1, 'no Machine observed an earlier Machine after mutating its own Engine')
    # Disjointness over an empty set holds vacuously. Each Machine of the pair
    # just created labelled resources, so its own Engine must have recorded
    # events for them; a silent event stream proves nothing about isolation.
    silent = [row['owner']['machine_id'] for row in same if not row['inventory']['event']]
    require(not silent, 'a Machine reported no event of its own: ' + ', '.join(silent))
    return {'schema_version': 1, 'scope': SCOPE,
            'developer_linux_machines': len(same), 'environment_count': 1,
            'cross_visible_containers_images_volumes_networks_events_caches': 0,
            'cross_machine_lifecycle_effects': 0, 'rechecks': rechecked,
            'environment_id': same[0]['owner']['environment_id'],
            'machines': sorted(row['owner']['machine_id'] for row in same),
            'observed_cache_records': {row['owner']['machine_id']: len(row['inventory']['cache'])
                                       for row in observations},
            'observed_event_identities': {row['owner']['machine_id']: len(row['inventory']['event'])
                                          for row in observations},
            'full_isolation_certified': False}


def retire(harness):
    """Remove every Machine's owned set once the cross-Machine claim is decided."""
    sessions = getattr(harness, 'isolation_sessions', [])
    require(bool(sessions), 'no isolation Session to retire')
    for session in sessions:
        session.remove()
    return {'schema_version': 1, 'scope': SCOPE, 'retired': len(sessions),
            'cleanup_complete': all(session.cleanup_complete for session in sessions)}


def run_machine(harness, descriptor, scope, proof, images, index):
    """One Machine's owned set, its complete inventory, and a recheck of the rest.

    The per-Machine result is deliberately not a decision: `verify_machines`
    decides once every slice exists, and `retire` removes the owned sets after.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index < 3, 'bounded isolation Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    owner = {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')}
    require(descriptor['owner'] == owner, 'isolation Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'isolation Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents isolation dispatch')
    sessions = getattr(harness, 'isolation_sessions', None)
    if sessions is None:
        sessions = harness.isolation_sessions = []
    require(len(sessions) == index, 'earlier isolation Session missing')
    output = harness.evidence / ('isolation-machine-' + str(index))
    require(not os.path.lexists(output), 'isolation Machine evidence directory preexists')
    image_id = sentinel_image(harness, descriptor)
    harness.monitor.check()
    startup.private(output)
    started = time.time_ns()
    token = 'vziso-' + uuid.uuid4().hex[:24]
    session = Session(harness, descriptor, image_id, token)
    sessions.append(session)
    startup.document(output / 'isolation-machine.intent.json',
                     {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
                      'machine_scope': copy.deepcopy(scope), 'source_pins': pins,
                      'started_unix_ns': started, 'token': token, 'image_id': image_id})
    owned = session.create()
    inventory = session.inventory('owned')
    generation = session.generation()
    # Every earlier Machine is re-read now, after this Machine mutated its own
    # Engine. That is the lifecycle-effect observation, made from the far side.
    rechecks = [{'machine_id': earlier.descriptor['owner']['machine_id'], 'generation': earlier.generation()}
                for earlier in sessions[:-1]]
    result = {'schema_version': 1, 'scope': SCOPE, 'kind': 'installed_isolation_raw_evidence',
              'token': token, 'image_id': image_id, 'owner': copy.deepcopy(owner),
              'machine_scope': copy.deepcopy(scope), 'started_unix_ns': started,
              'owned': owned, 'inventory': inventory, 'generation': generation, 'rechecks': rechecks,
              'compatibility_certified': False, 'release_scenarios_passed': [],
              'cross_machine_decision_required': True,
              'remaining': ['cross-Machine visibility is decided by verify_machines',
                            'sibling-Environment isolation needs a third Environment'],
              'ended_unix_ns': time.time_ns()}
    startup.document(output / 'machine-isolation-validation.json', result)
    return result
