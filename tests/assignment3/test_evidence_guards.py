import importlib.util
import json
from pathlib import Path
import subprocess
import threading
import copy
import pytest
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

from scripts.load_transactions import run,success
from scripts.recover_lost_worker import delete_uid,require_same_surviving_hardware
from scripts import cluster_state as cs
from scripts.audit_ledger import validate


def test_identical_kubernetes_uids_cannot_target_the_wrong_vm_copy():
    def node(name,hardware):
        return {'metadata':{'name':name,'uid':'preserved-by-full-clone'},'status':{'nodeInfo':{'systemUUID':hardware}}}
    topology={'nodes':{'items':[node('cp','original-cp'),node('b','original-b'),node('c','lost-c')]}}
    live={'cp':node('cp','original-cp'),'b':node('b','original-b'),'replacement':node('replacement','fresh')}
    require_same_surviving_hardware(topology,live,'c')
    live['cp']=node('cp','isolated-copy-cp')
    with pytest.raises(RuntimeError,match='hardware differs'):
        require_same_surviving_hardware(topology,live,'c')


def test_unavailable_replica_is_reported_without_hiding_unknown(monkeypatch):
    spec=importlib.util.spec_from_file_location('audit_under_test',Path(__file__).with_name('audit.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    def kb(*args,**kwargs):
        if args[0]=='get':return json.dumps({'items':[{'metadata':{'name':'bank-a-3'}}]})
        raise RuntimeError('kubelet unreachable')
    monkeypatch.setattr(module,'kubectl',kb)
    assert module.replica_roles('bank-a')['bank-a-3'].startswith('UNAVAILABLE:')


def baseline():
    return {'pass':True,'balances':{'bank-a':{f'A{i:04}':1000 for i in range(1,1001)},
                                   'bank-b':{f'A{i:04}':1000 for i in range(1001,2001)}},
        'clusters':{},'ledger':{'transactions':[],'votes':[],'acks':[],
                              'local':{'bank-a':[],'bank-b':[]},'participants':{'bank-a':[],'bank-b':[]}}}


@pytest.mark.parametrize('initial_unavailable',[False,True])
def test_audit_cli_writes_baseline_comparison_report(monkeypatch,tmp_path,initial_unavailable):
    spec=importlib.util.spec_from_file_location('audit_cli',Path(__file__).with_name('audit.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    result=baseline();result['total']=2000000;result['created_utc']=cs.utc();result['databases']={}
    for name in cs.CLUSTERS:
        roles={name+'-1':'PRIMARY',name+'-2':'STANDBY',name+'-3':'STANDBY'}
        result['clusters'][name]={'spec':{'instances':3},'status':{'readyInstances':3},'instances':[
            {'ready':True,'pg':{'standby':i>0,'prepared':[],'wal_receiver':[{'status':'streaming'}] if i else []}} for i in range(3)]}
        if name!='bank-meta':result['databases'][name]={'accounts':1000,'db_state':'f|100|0','replica_roles':roles}
    result['transactions']={'pending_count':0,'distinct_request_ids':0,'rows_count':0,
                            'meta_replica_roles':{'meta-1':'PRIMARY','meta-2':'STANDBY','meta-3':'STANDBY'},'meta_db_state':'f|100'}
    before=tmp_path/'before.json';before.write_text(json.dumps(result))
    output=tmp_path/'after.json'
    calls=[]
    def snapshot():
        value=copy.deepcopy(result)
        if initial_unavailable and not calls:
            del value['clusters']['bank-a']['instances'][2]['pg']
        calls.append(True)
        return value
    monkeypatch.setattr(module,'snapshot',snapshot)
    monkeypatch.setattr(module.time,'sleep',lambda _:None)
    monkeypatch.setattr('sys.argv',['audit.py','--wait-recovery','5','--baseline',str(before),'--output',str(output)])
    module.main()
    assert json.loads(output.read_text())['pass'] is True
    assert len(calls)==(3 if initial_unavailable else 2)


def test_equal_total_cannot_hide_unjournaled_effect():
    before=baseline();after=baseline()
    after['balances']['bank-a']['A0001']-=1
    after['balances']['bank-b']['A1001']+=1
    checks=validate(after,before)
    assert not checks['each_balance_matches_ledger_replay']


def test_client_ack_missing_from_db_fails(tmp_path):
    path=tmp_path/'load.jsonl'
    path.write_text(json.dumps({'event':'intent','payload':{'source':'A0001','destination':'A1001','amount':1,'request_id':'r'}})+'\n'+json.dumps({'event':'result','request_id':'r','success':True})+'\n')
    result=baseline()
    assert not validate(result,load_log=path)['acknowledged_requests_preserved']


def test_completed_commit_without_participant_fails():
    result=baseline()
    result['ledger']['transactions']=[{'tx_id':'a'*32,'request_id':'r','phase':'COMPLETED','decision':'COMMIT','source_shard':'A','destination_shard':'B'}]
    assert not validate(result)['2pc_votes_decisions_acks_match_participants']


def test_http_success_requires_terminal_valid_body():
    payload={'request_id':'r'}
    for code,body in [(202,{'ok':True,'request_id':'r','status':'COMMITTED'}),
                       (200,{'ok':False,'request_id':'r','status':'COMMITTED'}),
                       (200,{'ok':True,'request_id':'r','status':'PENDING'}),
                       (200,{'ok':True,'request_id':'r','status':'PENDING','operation':'same_shard_transfer'}),
                       (200,{'ok':True,'request_id':'another','status':'COMMITTED'})]:
        assert not success(code,body,payload)


def test_load_retries_same_payload_and_records_unknown(tmp_path):
    payloads=[]
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            payloads.append(payload)
            self.send_response(202 if len(payloads)==1 else 200);self.end_headers()
            self.wfile.write(json.dumps({'ok':len(payloads)>1,'request_id':payload['request_id'],
                                         'status':'PENDING' if len(payloads)==1 else 'COMMITTED'}).encode())
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        report=run('http://127.0.0.1:'+str(server.server_port),5,1,tmp_path/'load.jsonl',run_id='test',requests_count=1)
        assert payloads[0]==payloads[1]
        assert report['counts']['validated_success']==1 and report['counts']['not_success']==1
    finally:server.shutdown();server.server_close()


def test_raw_delete_transmits_uid_precondition(monkeypatch):
    captured=[]
    class Handler(BaseHTTPRequestHandler):
        def do_DELETE(self):
            if self.headers.get('Transfer-Encoding')=='chunked':
                body=b''
                while True:
                    size=int(self.rfile.readline().strip(),16)
                    if not size:self.rfile.readline();break
                    body+=self.rfile.read(size);self.rfile.read(2)
            else:body=self.rfile.read(int(self.headers['Content-Length']))
            captured.append(json.loads(body))
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers()
            self.wfile.write(b'{"kind":"Status","apiVersion":"v1","status":"Success"}')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    def kb(*args,stdin=None):
        result=subprocess.run(['kubectl','--server=http://127.0.0.1:'+str(server.server_port),*args],
                              input=stdin,capture_output=True,text=True,timeout=10)
        assert result.returncode==0,result.stderr
        return result.stdout
    monkeypatch.setattr(cs,'kb',kb)
    try:
        delete_uid('persistentvolumeclaims','disposable','old-uid')
        assert captured[0]['preconditions']=={'uid':'old-uid'}
    finally:server.shutdown();server.server_close()
