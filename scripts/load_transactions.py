"""Repeatable concurrent workload with crash-durable client request evidence.

All retries reuse the exact payload/ID. Transport errors and 202/5xx are UNKNOWN,
and only a validated terminal 200 counts as success. Audit resolves actual effects.
"""
import argparse
from collections import Counter
import concurrent.futures
import json
import os
from pathlib import Path
import random
import threading
import time
import uuid
import requests


def success(code,body,payload):
    return code==200 and isinstance(body,dict) and body.get('ok') is True and body.get('request_id')==payload['request_id'] and (
        body.get('status')=='COMMITTED' or (body.get('operation')=='same_shard_transfer' and 'status' not in body))


def run(gateway,seconds,workers,log,seed=7,run_id=None,retries=1,requests_count=None,timeout=35):
    run_id=run_id or uuid.uuid4().hex[:12]
    end=time.monotonic()+seconds;lock=threading.Lock();counter=Counter();sequence=0
    log=Path(log);log.parent.mkdir(parents=True,exist_ok=True)
    # Never overwrite an earlier acknowledgment ledger.
    with log.open('x',encoding='utf-8') as stream:
        def record(event):
            with lock:
                stream.write(json.dumps({'time_unix':time.time(),**event},ensure_ascii=False)+'\n')
                stream.flush();os.fsync(stream.fileno())
        def worker(_):
            nonlocal sequence
            session=requests.Session();session.trust_env=False
            while time.monotonic()<end:
                with lock:
                    index=sequence;sequence+=1
                if requests_count is not None and index>=requests_count:break
                # Seed+sequence determines payload independently of thread order.
                rng=random.Random(seed+index)
                source,destination=rng.choice([('A0400','A0401'),('A0401','A0400'),('A0500','A1500'),('A1500','A0500')])
                payload={'source':source,'destination':destination,'amount':1,
                         'request_id':f'k3-{run_id}-{index:08}'}
                for attempt in range(retries+1):
                    record({'event':'intent','index':index,'attempt':attempt,'payload':payload})
                    code=0;body={};error=None
                    try:
                        response=session.post(gateway.rstrip('/')+'/api/transfer',json=payload,timeout=(3,timeout))
                        code=response.status_code
                        try:body=response.json()
                        except ValueError:error='invalid JSON'
                    except requests.RequestException as exc:error=type(exc).__name__
                    ok=success(code,body,payload)
                    record({'event':'result','index':index,'attempt':attempt,'request_id':payload['request_id'],
                            'code':code,'body':body,'error':error,'success':ok})
                    with lock:counter[str(code) if code else 'transport']+=1;counter['validated_success' if ok else 'not_success']+=1
                    if ok or (code==409 and body.get('status')=='ABORTED') or code==400:break
                    if attempt<retries:time.sleep(min(.5*2**attempt,2))
            session.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(worker,range(workers)))
    result={'run_id':run_id,'seed':seed,'counts':dict(counter),'log':str(log),
            'note':'Unknown responses require DB audit; HTTP counters are not correctness proof.'}
    log.with_suffix('.summary.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--gateway',default='http://127.0.0.1:8000')
    p.add_argument('--seconds',type=int,default=120);p.add_argument('--workers',type=int,default=4)
    p.add_argument('--seed',type=int,default=7);p.add_argument('--run-id');p.add_argument('--retries',type=int,default=1)
    p.add_argument('--requests',type=int);p.add_argument('--timeout',type=float,default=35)
    p.add_argument('--output',default=None);a=p.parse_args()
    if a.seconds<=0 or a.workers<=0 or not 0<=a.retries<=3:p.error('positive duration/workers and retries 0..3 required')
    print(json.dumps(run(a.gateway,a.seconds,a.workers,a.output or f'reports/assignment3/load-{uuid.uuid4().hex[:12]}.jsonl',
        a.seed,a.run_id,a.retries,a.requests,a.timeout),indent=2))


if __name__=='__main__':main()
