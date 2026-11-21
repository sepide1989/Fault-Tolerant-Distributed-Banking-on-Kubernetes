"""Repeatable banking experiments with request IDs, expected balances and DB checks.

Use in conjunction with audit.py. Examples:
  python tests/assignment3/run_experiments.py normal
  python tests/assignment3/run_experiments.py duplicate
  python tests/assignment3/run_experiments.py concurrent
  python tests/assignment3/run_experiments.py pod-failure
  python tests/assignment3/run_experiments.py primary-failure
  python tests/assignment3/run_experiments.py coordinator-failure

Requires `kubectl` and `requests` (pip install requests).
"""
import argparse
import concurrent.futures as futures
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
from audit import snapshot

import requests

URL=os.environ.get('GATEWAY_URL','http://127.0.0.1:8000').rstrip('/')
NS=os.environ.get('NAMESPACE','banking')
PREFIX='k3-'+uuid.uuid4().hex[:10]
S=requests.Session()
S.trust_env=False


def rid(s): return PREFIX+'-'+s


def get(account, timeout=15):
    r=S.get(URL+'/api/accounts/'+account,timeout=timeout)
    r.raise_for_status()
    return r.json()['balance']


def post(path,payload):
    try:
        r=S.post(URL+path,json=payload,timeout=65)
        return r.status_code,r.json()
    except requests.RequestException as exc:
        return 0, {'transport_error':str(exc)}


def finish(id, started_response=None, timeout=180):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        try:
            r=S.get(URL+'/api/requests/'+id,timeout=12)
            if r.ok and r.json().get('status') in ('COMMITTED','ABORTED'):
                return r.json()
        except requests.RequestException:
            pass
        time.sleep(2)
    raise AssertionError('2PC request did not resolve: '+id+' '+str(started_response))


def must_commit(payload):
    id=payload['request_id']
    status,res=post('/api/transfer',payload)
    if status not in (200,202,409):
        raise AssertionError(f'transfer failed with ambiguous response {status}: {res}')
    end=finish(id,res)
    if end['status']!='COMMITTED':
        raise AssertionError(f'transfer did not commit: {end}')
    return end


def expect(condition, message):
    if not condition: raise AssertionError(message)
    print('[PASS]',message,flush=True)


def kb(*args):
    p=subprocess.run(['kubectl','-n',NS,*args],capture_output=True,text=True,timeout=50)
    if p.returncode:
        raise RuntimeError(p.stderr)
    return p.stdout.strip()


def primary(cluster):
    return kb('get','cluster',cluster,'-o','jsonpath={.status.currentPrimary}')


def pods(label):
    return kb('get','pod','-l','app='+label,'-o','jsonpath={.items[*].metadata.name}').split()


def normal():
    # All operations are reversed: conservation baseline is preserved.
    account='A0018'
    before=get(account)
    status,p=post('/api/deposit',dict(account=account,amount=17,request_id=rid('deposit')))
    expect(status==200 and get(account)==before+17,'deposit is durable and visible')
    status,p=post('/api/withdraw',dict(account=account,amount=17,request_id=rid('withdraw')))
    expect(status==200 and get(account)==before,'withdraw reverses deposit')
    a,b='A0040','A0041'
    before_a,before_b=get(a),get(b)
    status,_=post('/api/transfer',dict(source=a,destination=b,amount=5,request_id=rid('local-forward')))
    expect(status==200 and get(a)==before_a-5 and get(b)==before_b+5,'same-shard atomic transfer')
    status,_=post('/api/transfer',dict(source=b,destination=a,amount=5,request_id=rid('local-back')))
    expect(status==200 and get(a)==before_a and get(b)==before_b,'same-shard balances restored')
    a,b='A0500','A1500'
    before_a,before_b=get(a),get(b)
    must_commit(dict(source=a,destination=b,amount=7,request_id=rid('cross-forward')))
    expect(get(a)==before_a-7 and get(b)==before_b+7,'cross-shard 2PC commit')
    must_commit(dict(source=b,destination=a,amount=7,request_id=rid('cross-back')))
    expect(get(a)==before_a and get(b)==before_b,'cross-shard balances restored')


def duplicate():
    account='A0015'
    before=get(account)
    payload={'account':account,'amount':9,'request_id':rid('same-request')}
    with futures.ThreadPoolExecutor(max_workers=16) as pool:
        results=list(pool.map(lambda _:post('/api/deposit',payload), range(30)))
    expect(all(code==200 for code,_ in results),'all retries return the stored local result')
    expect(get(account)==before+9,'30 repeated writes applied ONCE')
    code,_=post('/api/deposit',{**payload,'amount':10})
    expect(code==409,'request ID with different payload rejected')
    code,_=post('/api/withdraw',{'account':account,'amount':9,'request_id':rid('cleanup')})
    expect(code==200 and get(account)==before,'exact account balance restored')
    a,b='A0530','A1530'
    s0,d0=get(a),get(b)
    p={'source':a,'destination':b,'amount':3,'request_id':rid('same-cross')}
    with futures.ThreadPoolExecutor(max_workers=12) as pool:
        results=list(pool.map(lambda _:post('/api/transfer',p),range(20)))
    final=finish(p['request_id'])
    expect(final['status']=='COMMITTED','duplicate cross-shard transfer eventually committed')
    expect(get(a)==s0-3 and get(b)==d0+3,'20 duplicate cross-shard transfer requests applied ONCE')
    must_commit({'source':b,'destination':a,'amount':3,'request_id':rid('cross-cleanup')})
    expect(get(a)==s0 and get(b)==d0,'cross-shard account balances restored')


def concurrent():
    # Opposite directions, equal counts; 2PC may abort lock-contended requests;
    # the global total and pending transaction states must still be correct.
    a,b='A0400','A0401'
    old_a,old_b=get(a),get(b)
    operations=[]
    for i in range(25):
        operations.extend([
            {'source':a,'destination':b,'amount':1,'request_id':rid('ab'+str(i))},
            {'source':b,'destination':a,'amount':1,'request_id':rid('ba'+str(i))},
        ])
    with futures.ThreadPoolExecutor(max_workers=14) as pool:
        outcomes=list(pool.map(lambda x:post('/api/transfer',x),operations))
    # Row-lock contention may legitimately abort operations: count actual successes
    # and check the exact balances implied by committed requests, NOT an assumption
    # that all 50 attempts must succeed.
    expect(all(code in (200,409) for code,_ in outcomes),
           'every concurrent request either committed or was explicitly rejected')
    ab=sum(code==200 for i,(code,_) in enumerate(outcomes) if i%2==0)
    ba=sum(code==200 for i,(code,_) in enumerate(outcomes) if i%2==1)
    expect(get(a)==old_a-ab+ba and get(b)==old_b+ab-ba,
           'committed local transfers match their exact SQL balance effects')
    a,b='A0500','A1500'
    old_sum=get(a)+get(b)
    cross=[{'source':a if i%2==0 else b,'destination':b if i%2==0 else a,
            'amount':1,'request_id':rid('cross-'+str(i))} for i in range(20)]
    with futures.ThreadPoolExecutor(max_workers=10) as pool:
        raw=list(pool.map(lambda x:post('/api/transfer',x),cross))
    resolved=[finish(p['request_id'],r) for p,r in zip(cross,raw)]
    expect(all(x['status'] in ('COMMITTED','ABORTED') for x in resolved),'all distributed transactions terminal')
    expect(get(a)+get(b)==old_sum,'money conserved across concurrent 2PC transfers')


def pod_failure():
    names=pods('bank-a')
    expect(len(names)>=2,'two application Pods exist before failure')
    old=get('A0032')
    print('[INJECT] deleting application Pod',names[0])
    kb('delete','pod',names[0],'--wait=false')
    until=time.monotonic()+120
    while time.monotonic()<until:
        try:
            if get('A0032')==old:
                break
        except Exception:pass
        time.sleep(2)
    else: raise AssertionError('healthy application replicas did not serve during failure')
    expect(True,'healthy replicas continue serving after application Pod deletion')


def primary_failure():
    before=get('A0512')
    target=primary('bank-a')
    print('[INJECT] graceful deletion of PostgreSQL primary',target)
    kb('delete','pod',target,'--wait=false')
    until=time.monotonic()+240
    while time.monotonic()<until:
        try:
            if primary('bank-a')!=target and get('A0512')==before:
                break
        except Exception:
            pass
        time.sleep(4)
    else: raise AssertionError('DB failover did not restore reads in expected window')
    expect(True,'primary failure recovers committed account balance')


def coordinator_failure():
    # Both Gateway Pods are stateLESS and all 2PC decisions are in PostgreSQL.
    to_send=[{'source':'A0540','destination':'A1540','amount':1,
              'request_id':rid('coordinator-'+str(i))} for i in range(12)]
    old_sum=get('A0540')+get('A1540')
    with futures.ThreadPoolExecutor(max_workers=8) as pool:
        pending_tasks=[pool.submit(post,'/api/transfer',p) for p in to_send]
        names=pods('gateway')
        expect(bool(names),'Gateway Pod exists')
        print('[INJECT] terminating Gateway Pod',names[0])
        # For stateless Gateway only; NEVER force-delete a PostgreSQL primary.
        kb('delete','pod',names[0],'--force','--grace-period=0','--wait=false')
        raw=[f.result() for f in pending_tasks]
    final=[]
    for payload,(_,res) in zip(to_send,raw):
        # The Gateway may have died BEFORE registration. Retry SAME request ID;
        # this is essential to avoid treating an unknown outcome as success.
        try:
            final.append(finish(payload['request_id'],res,timeout=25))
        except AssertionError:
            post('/api/transfer',payload)
            final.append(finish(payload['request_id'],res))
    expect(all(x['status'] in ('COMMITTED','ABORTED') for x in final),'interrupted 2PC requests recover to terminal states')
    expect(get('A0540')+get('A1540')==old_sum,'no partial transfer after Gateway interruption')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('scenario',choices=['normal','duplicate','concurrent','pod-failure','primary-failure','coordinator-failure'])
    args=parser.parse_args()
    print('Gateway:',URL,'prefix:',PREFIX,'scenario:',args.scenario,flush=True)
    before=snapshot()
    globals()[args.scenario.replace('-','_')]()
    # The recovery worker may need a few seconds to finish an acknowledged COMMIT.
    deadline=time.monotonic()+180
    while True:
        after=snapshot()
        if after['transactions']['pending_count']==0 or time.monotonic()>deadline:
            break
        time.sleep(4)
    expect(before['total']==after['total'], 'REAL PostgreSQL sum of 2,000 accounts conserved')
    expect(after['transactions']['pending_count']==0, 'no 2PC transactions left unresolved')
    expect(after['transactions']['committed_count']>=before['transactions']['committed_count'],
           'durable coordinator COMMIT history not lost')
    expect(all(v['db_state'].startswith('f|') for v in after['databases'].values()),
           'both authoritative databases are writable primaries')
    result={'scenario':args.scenario,'utc':datetime.now(timezone.utc).isoformat(),
            'before':before,'after':after,'status':'PASS'}
    path=Path('reports/assignment3')/(args.scenario+'-'+PREFIX+'.json')
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print('[PASS] DB-verified experiment. Evidence saved to',path,flush=True)

if __name__=='__main__': main()
