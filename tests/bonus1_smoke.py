"""Run inside Gateway container after `--profile bonus1 up -d --build`.
Starts/validates B->C migration; safe to rerun AFTER completion with read-only checks.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import time
import uuid
import requests
import psycopg

ROOT='http://localhost:8000'
MIGRATOR='http://migrator:8050'


def get(url):
    r=requests.get(url,timeout=10);r.raise_for_status();return r.json()


def transfer(src,dst,amount=1):
    rid='bonus1-'+uuid.uuid4().hex
    payload={'source':src,'destination':dst,'amount':amount,'request_id':rid}
    for n in range(30):
        try:
            r=requests.post(ROOT+'/api/transfer',json=payload,timeout=30)
            if r.status_code==200:
                return r.json()
            if r.status_code in (202,409,503):
                data=r.json()
                # 409 banking insufficiency is final. Stale epochs / mid-cutover retry.
                if 'insufficient' in data.get('error',''):
                    return data
        except requests.RequestException:
            pass
        time.sleep(.3)
    raise AssertionError('transfer never resolved: '+repr(payload))


def bal(acc):
    return get(ROOT+'/api/accounts/'+acc)['balance']


def main():
    state=get(MIGRATOR+'/status')
    print('initial migration state:',state)
    if state['phase']=='NOT_STARTED':
        print('starting online migration')
        requests.post(MIGRATOR+'/start',timeout=10).raise_for_status()
    deadline=time.time()+180
    while time.time()<deadline:
        state=get(MIGRATOR+'/status')
        if state['phase'] in ('CATCHUP','COMPLETED'):
            break
        print('waiting snapshot:',state['phase']);time.sleep(.5)
    else:
        raise AssertionError('snapshot did not finish')

    if state['phase']!='COMPLETED':
        accounts=('A1600','A1700','A1200','A0500')
        before=sum(map(bal,accounts))
        print('sending transfers WHILE migration is live...')
        operations=[('A1600','A1200'),('A1200','A1700'),('A1700','A0500'),('A0500','A1600')]*3
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda t: transfer(*t),operations))
        mid=sum(map(bal,accounts))
        assert before==mid, ('money changed during live copy',before,mid)
        print('conservation during catchup:',mid)
        requests.post(MIGRATOR+'/cutover',timeout=10).raise_for_status()
        deadline=time.time()+180
        while time.time()<deadline:
            state=get(MIGRATOR+'/status')
            if state['phase']=='COMPLETED':
                break
            print('cutover:',state['phase'], 'checkpoint:',state['applied_seq'])
            time.sleep(1)
        assert state['phase']=='COMPLETED',state

    topo=get(ROOT+'/api/topology')
    print('topology:',topo['shard_map'])
    assert topo['shard_map']['version']>=2
    assert get(ROOT+'/api/accounts/A1600')['shard']=='C'
    assert get(ROOT+'/api/accounts/A1200')['shard']=='B'

    # Direct stale request B must be fenced AT THE WRITE PATH, not only Gateway.
    stale=requests.post('http://bank-b1:8000/internal/deposit',json={
        'account':'A1600','amount':1,'request_id':'stale-'+uuid.uuid4().hex,'epoch':1
    },timeout=10)
    assert stale.status_code==409,(stale.status_code,stale.text)
    print('STALE_ROUTE blocked on old B server:',stale.json())

    with psycopg.connect('postgresql://bank:bankpass@db-b:5432/bank') as b:
        assert b.execute("SELECT COUNT(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'").fetchone()[0]==0
    with psycopg.connect('postgresql://bank:bankpass@db-c:5432/bank') as c:
        assert c.execute("SELECT COUNT(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'").fetchone()[0]==500
    print('BONUS I SMOKE PASS: 500 moved; routed to C; fenced old B; money conserved')


if __name__=='__main__':
    main()
