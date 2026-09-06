"""Inert run137 observers and raw-progress adversaries; no Docker dispatch."""
import copy
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_container_kill as m

CID, TOKEN = 'a'*64, 'vzio-'+'b'*24
NAME, IMAGE = TOKEN+'-sigkill', 'sha256:'+'c'*64
INPUTS = {'docker_config': '/private/config', 'scope': {'docker_context': 'owned'},
          'clients': {'docker': {'path': '/pinned/docker'}}}
TERMINATION = {'cid': CID, 'command_index': 99, 'started_unix_ns': 150}


class Replay(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='vz-run137-replay-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.args, self.plan, ready = m.specification(NAME, IMAGE, TOKEN)
        self.ready = ready
        self.capture = {'started': {'unix_ns': 100, 'monotonic_ns': 1000},
            'completed': {'unix_ns': 180, 'monotonic_ns': 1080}, 'read_progress': [
            {'index': 0, 'stream': 'stdout', 'observed_bytes': {'stdout': len(ready['stdout']), 'stderr': 0, 'tty': 0},
             'observed': {'unix_ns': 120, 'monotonic_ns': 1020}},
            {'index': 1, 'stream': 'stderr', 'observed_bytes': {'stdout': len(ready['stdout']), 'stderr': len(ready['stderr']), 'tty': 0},
             'observed': {'unix_ns': 130, 'monotonic_ns': 1030}}]}
        self.capture_proof = {'terminal_receipt_sha256': 'd'*64, 'exit_code': 137}
        for i in range(1,6):
            self.write('command-%05d.json'%i, {})
            self.write('command-%05d.intent.json'%i, {})
        self.terminal = {'started_unix_ns': 90, 'elapsed_ns': 100, 'interaction_capture': self.capture}
        self.write('command-00003.json', self.terminal)
        self.write('kill-ready.json', {'schema_version': 1, 'command_index': 3,
            'read_observation': self.capture['read_progress'][1]})
        for name, raw in ready.items(): (self.root/('command-00003.'+name)).write_bytes(raw)
        self.write('command-00003.acknowledgement.json', {'command_index': 3, 'assertion': m.ACK,
            'terminal_receipt_sha256': 'd'*64, 'effects_uncertain': False})
        self.write('kill-disposition.json', {'schema_version': 1, 'thread_joined': True,
            'capture_error_type': None, 'orchestration_error_type': None, 'pending_interactions': 0})
        self.validator = self.enter(patch.object(m.interactive,'validate_recorded',return_value=self.capture_proof))
        self.guards = self.enter(patch.object(m,'validate_guard',side_effect=self.guard))

    def enter(self, cm):
        result=cm.__enter__(); self.addCleanup(cm.__exit__,None,None,None); return result

    def write(self, name, row):
        (self.root/name).write_bytes(m.interactive.canonical(row))

    def guard(self, output, inputs, a, b):
        self.assertEqual((output,inputs),(self.root,INPUTS))
        return {'commands': [{'receipt': {'started_unix_ns': i*10 if i<3 else 200+i*10,
            'elapsed_ns': 1}, 'terminal_sha256': str(i)*64} for i in (a,b)]}

    def proof(self, termination=TERMINATION):
        return m.rebuild(self.root,INPUTS,NAME,IMAGE,TOKEN,environment={},termination=termination)

    def test_exact_source_run_plan_and_expected137(self):
        proof=self.proof()
        self.assertEqual(self.args,['run','--pull','never','--network','none','--restart','no',
            '--name',NAME,'--label','dev.vz.container-io='+TOKEN,IMAGE,'service',TOKEN])
        self.assertEqual(self.plan['actions'],[{'kind':'close_stdin'}])
        self.assertNotIn('signal',json.dumps(self.plan))
        self.assertEqual(self.validator.call_args.kwargs['expected_exit'],137)
        self.assertEqual(self.validator.call_args.kwargs['argv'][:5],['docker','--config','/private/config','--context','owned'])
        self.assertEqual(m.replay_kill(self.root,INPUTS,NAME,IMAGE,TOKEN,proof,environment={},termination=TERMINATION),proof)

    def test_missing_unobserved_reordered_and_late_ready_fail(self):
        for kind in ('missing','reversed','count','early','bool'):
            terminal=copy.deepcopy(self.terminal); termination=copy.deepcopy(TERMINATION)
            if kind=='missing':terminal['interaction_capture']['read_progress'].pop()
            if kind=='reversed':terminal['interaction_capture']['read_progress'].reverse()
            if kind=='count':terminal['interaction_capture']['read_progress'][1]['observed_bytes']['stderr']-=1
            if kind=='early':termination['started_unix_ns']=130
            if kind=='bool':termination['started_unix_ns']=True
            self.write('command-00003.json',terminal)
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.proof(termination)

    def test_extra_signal_record_or_noncanonical_ready_fails(self):
        for raw in (self.ready['stdout']+b'KILLED\n',b'x'+self.ready['stdout'][1:]):
            (self.root/'command-00003.stdout').write_bytes(raw)
            with self.assertRaises(ValueError):self.proof()

    def test_foreign_or_unbound_termination_fails(self):
        for changed in (TERMINATION|{'cid':'name'},TERMINATION|{'command_index':False},
                        TERMINATION|{'extra':'unbound'}, {'cid':CID}):
            with self.subTest(changed=changed),self.assertRaises(ValueError):self.proof(changed)
        proof=self.proof()
        with self.assertRaises(ValueError):
            m.replay_kill(self.root,INPUTS,NAME,IMAGE,TOKEN,proof,environment={},termination=TERMINATION|{'cid':'e'*64})

    def test_ack_disposition_or_extra_ledger_fail(self):
        proof=self.proof()
        for filename, key, value in [('command-00003.acknowledgement.json','assertion','synthetic exit137'),
            ('kill-disposition.json','thread_joined',False),('kill-disposition.json','pending_interactions',1),
            ('kill-disposition.json','capture_error_type','TimeoutError')]:
            path=self.root/filename; original=path.read_bytes();row=json.loads(original);row[key]=value;self.write(filename,row)
            with self.subTest(key=key),self.assertRaises(ValueError):
                m.replay_kill(self.root,INPUTS,NAME,IMAGE,TOKEN,proof,environment={},termination=TERMINATION)
            path.write_bytes(original)
        self.write('command-00006.intent.json',{})
        with self.assertRaises(ValueError):self.proof()

    def test_capture_failure_cannot_be_replaced_by_semantics(self):
        self.validator.side_effect=ValueError('capture137 missing')
        with self.assertRaises(ValueError):self.proof()


class Orchestration(unittest.TestCase):
    def test_registered_before_dispatch_kill_after_both_reads_ack_after_validation(self):
        with tempfile.TemporaryDirectory(prefix='vz-run137-thread-') as tmp:
            root=Path(tmp).resolve(); output=root/'kill-run';output.mkdir()
            events=[];registered=[];killed=threading.Event()
            item=SimpleNamespace(inputs=SimpleNamespace(raw=INPUTS),fixture=root,output=root,
                                 guard=lambda:events.append('main-guard'))
            def persist(path,row,**_):path.write_bytes(m.interactive.canonical(row))
            record=SimpleNamespace(count=0,max_stream_bytes=m.LIMIT,pending_interactions=[],persist=persist,
                acknowledge_negative=lambda *args:events.append('ack'))
            observer=SimpleNamespace(output=output,record=record,env={})
            def guard():record.count+=2;events.append('observer-guard')
            observer.guard=guard
            _,_,ready=m.specification(NAME,IMAGE,TOKEN)
            def command(args,**kwargs):
                self.assertEqual(registered,[observer]);self.assertIsNone(kwargs['expected'])
                record.count+=1;events.append('run')
                for i,stream in enumerate(('stdout','stderr')):
                    kwargs['progress_observer']({'index':i,'stream':stream,'observed_bytes':{
                        'stdout':len(ready['stdout']),'stderr':len(ready['stderr']) if i else 0,'tty':0},
                        'observed':{'unix_ns':time.time_ns(),'monotonic_ns':time.monotonic_ns()}})
                    if not i:self.assertFalse(killed.is_set())
                self.assertTrue(killed.wait(2));return SimpleNamespace(index=3,returncode=137)
            observer.command=command
            def terminate():
                self.assertTrue((output/'kill-ready.json').exists());events.append('KILL');killed.set();return TERMINATION
            def rebuild(*args,**kwargs):events.append('validate');return {'scope':'inert'}
            with patch.object(m,'Driver',return_value=observer),patch.object(m,'rebuild',side_effect=rebuild), \
                    patch.object(m,'replay_kill',side_effect=lambda *args,**kwargs:args[5]):
                proof=m.run_kill(item,NAME,IMAGE,TOKEN,terminate=terminate,register_observer=registered.append)
            self.assertEqual(events,['main-guard','observer-guard','run','main-guard','KILL','observer-guard','main-guard','validate','ack'])
            self.assertFalse(observer.follow_thread.is_alive());self.assertEqual(proof,{'scope':'inert'})
            self.assertTrue((output/'kill-proof.json').exists())

    def test_failed_capture_retains_handle_and_never_kills_or_acknowledges(self):
        with tempfile.TemporaryDirectory(prefix='vz-run137-failed-') as tmp:
            root=Path(tmp).resolve();output=root/'kill-run';output.mkdir(); registered=[]
            item=SimpleNamespace(inputs=None,fixture=root,output=root,guard=lambda:None)
            record=SimpleNamespace(count=0,max_stream_bytes=m.LIMIT,pending_interactions=[object()],
                persist=lambda p,r,**kw:p.write_bytes(m.interactive.canonical(r)),acknowledge_negative=Mock())
            observer=SimpleNamespace(output=output,record=record)
            observer.guard=lambda:setattr(record,'count',record.count+2)
            error=RuntimeError('retained original error');observer.command=Mock(side_effect=error)
            terminate=Mock()
            with patch.object(m,'Driver',return_value=observer),self.assertRaises(ValueError):
                m.run_kill(item,NAME,IMAGE,TOKEN,terminate=terminate,register_observer=registered.append)
            self.assertEqual(registered,[observer]);terminate.assert_not_called();record.acknowledge_negative.assert_not_called()
            self.assertIs(observer.follow_state['error'],error)
            self.assertFalse(observer.follow_thread.is_alive())
            self.assertEqual(json.loads((output/'kill-disposition.json').read_bytes())['pending_interactions'],1)

    def test_termination_failure_joins_retained_thread_without_retry_or_ack(self):
        with tempfile.TemporaryDirectory(prefix='vz-run137-kill-failed-') as tmp:
            root=Path(tmp).resolve();output=root/'kill-run';output.mkdir();registered=[];release=threading.Event()
            item=SimpleNamespace(inputs=None,fixture=root,output=root,guard=lambda:None)
            record=SimpleNamespace(count=0,max_stream_bytes=m.LIMIT,pending_interactions=[],
                persist=lambda p,r,**kw:p.write_bytes(m.interactive.canonical(r)),acknowledge_negative=Mock())
            observer=SimpleNamespace(output=output,record=record)
            observer.guard=lambda:setattr(record,'count',record.count+2)
            _,_,ready=m.specification(NAME,IMAGE,TOKEN)
            def command(*args,**kwargs):
                kwargs['progress_observer']({'index':1,'stream':'stderr','observed_bytes':{
                    'stdout':len(ready['stdout']),'stderr':len(ready['stderr']),'tty':0},
                    'observed':{'unix_ns':time.time_ns(),'monotonic_ns':time.monotonic_ns()}})
                self.assertTrue(release.wait(2));return SimpleNamespace(index=3,returncode=137)
            observer.command=command
            error=RuntimeError('external kill incomplete')
            def failed_termination():release.set();raise error
            terminate=Mock(side_effect=failed_termination)
            with patch.object(m,'Driver',return_value=observer),self.assertRaises(RuntimeError):
                m.run_kill(item,NAME,IMAGE,TOKEN,terminate=terminate,register_observer=registered.append)
            terminate.assert_called_once();record.acknowledge_negative.assert_not_called()
            self.assertIs(observer.follow_state['orchestration_error'],error)
            self.assertFalse(observer.follow_thread.is_alive())
            self.assertFalse((output/'kill-proof.json').exists())


if __name__=='__main__':unittest.main()
