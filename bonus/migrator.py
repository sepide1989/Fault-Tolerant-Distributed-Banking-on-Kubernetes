"""Online B->C migration: consistent snapshot + durable ordered outbox + fencing.

The B change cursor row is incremented in the SAME transaction as each change;
its row lock ensures the committed outbox has a serial, gap-free order.
Only the final cutover briefly fences the migrating 500-account range.
"""
import logging
import os
import threading
import time
from contextlib import closing

import psycopg
from psycopg.types.json import Jsonb
from flask import Flask, jsonify, request

from bonus.bootstrap import DSNS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
app = Flask(__name__)
WORK = 'B-to-C-1501-2000'
_lock = threading.Lock()


def connect(name):
    return psycopg.connect(DSNS[name], connect_timeout=4)


def job():
    with connect('META') as con:
        row = con.execute('SELECT phase,cutover_requested,snapshot_seq,applied_seq,epoch,error FROM migration_job WHERE job_id=%s',(WORK,)).fetchone()
    return (dict(zip(('phase','cutover_requested','snapshot_seq','applied_seq','epoch','error'),row)) if row else {'phase':'NOT_STARTED'})


def update(phase=None, **fields):
    with connect('META') as con:
        sets=[];vals=[]
        if phase is not None:
            fields['phase']=phase
        for k,v in fields.items():
            if k not in ('phase','cutover_requested','snapshot_seq','applied_seq','epoch','error'):
                raise ValueError('unknown job field')
            sets.append(k+'=%s');vals.append(v)
        sets.append('updated_at=now()')
        con.execute('UPDATE migration_job SET '+','.join(sets)+' WHERE job_id=%s',(*vals,WORK))
    log.info('MIGRATE state=%s',job())


def snapshot():
    """On interrupted snapshot restart fully: old B still owns and serves writes."""
    with psycopg.connect(DSNS['B'], autocommit=True, connect_timeout=4) as source:
        source.execute('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        baseline = source.execute("SELECT seq FROM migration_cursor WHERE key='B'").fetchone()[0]
        accounts = source.execute("SELECT account_id,balance FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000' ORDER BY account_id").fetchall()
        requests = source.execute("""SELECT request_id,op_type,payload,result FROM local_requests
            WHERE (payload->>'account' BETWEEN 'A1501' AND 'A2000')
               OR (payload->>'source' BETWEEN 'A1501' AND 'A2000')
               OR (payload->>'destination' BETWEEN 'A1501' AND 'A2000')""").fetchall()
        with connect('C') as target:
            # C is not writable by clients before activation. Any interrupted
            # snapshot is discarded and recopied from a new consistent snapshot.
            state = target.execute("SELECT writable FROM ownership WHERE zone='moving' FOR UPDATE").fetchone()
            if state is None or state[0]:
                raise RuntimeError('cannot take snapshot of an active C owner')
            target.execute("DELETE FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'")
            target.execute('DELETE FROM local_requests')
            target.execute('DELETE FROM migration_applied')
            with target.cursor() as cur:
                for i in range(0,len(accounts),50):
                    cur.executemany('INSERT INTO accounts(account_id,balance) VALUES(%s,%s)',accounts[i:i+50])
                    if os.getenv('MIGRATION_SNAPSHOT_DELAY'):
                        time.sleep(float(os.getenv('MIGRATION_SNAPSHOT_DELAY')))
                cur.executemany('INSERT INTO local_requests(request_id,op_type,payload,result) VALUES(%s,%s,%s,%s)',[
                    (r[0],r[1],Jsonb(r[2]),Jsonb(r[3]) if r[3] is not None else None) for r in requests
                ])
        # Snapshot is committed in C before checkpoint in metadata; crash
        # before this point triggers a new full snapshot on next startup.
        update('CATCHUP',snapshot_seq=baseline,applied_seq=baseline,error=None)
        source.rollback()
        log.info('SNAPSHOT completed: accounts=%d requests=%d seq=%d',len(accounts),len(requests),baseline)


def current_seq():
    with connect('B') as con:
        return con.execute("SELECT seq FROM migration_cursor WHERE key='B'").fetchone()[0]


def catchup(max_batch=250):
    checkpoint=job()['applied_seq']
    with connect('B') as source:
        entries=source.execute('SELECT seq,kind,account_id,request_id,payload FROM migration_outbox WHERE seq>%s ORDER BY seq LIMIT %s',(checkpoint,max_batch)).fetchall()
    if not entries:
        return 0
    # Idempotent replay: C.applied rows and data update commit atomically.
    with connect('C') as target:
        for seq,kind,account,request_id,payload in entries:
            new=target.execute('INSERT INTO migration_applied(seq) VALUES(%s) ON CONFLICT DO NOTHING RETURNING seq',(seq,)).fetchone()
            if not new:
                continue
            if kind == 'account':
                target.execute('INSERT INTO accounts(account_id,balance) VALUES(%s,%s) ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance',(account,payload['balance']))
            elif kind == 'request':
                target.execute('INSERT INTO local_requests(request_id,op_type,payload,result) VALUES(%s,%s,%s,%s) '
                    'ON CONFLICT(request_id) DO UPDATE SET result=excluded.result',(
                    request_id,payload['op_type'],Jsonb(payload['payload']),Jsonb(payload['result'])))
            else:
                raise RuntimeError('unexpected outbox kind')
    # Crash between C commit and metadata checkpoint is safe: replay is idempotent.
    if os.getenv('MIGRATION_REPLAY_DELAY'):
        time.sleep(float(os.getenv('MIGRATION_REPLAY_DELAY')))
    update(applied_seq=entries[-1][0])
    return len(entries)


def catchup_all():
    while catchup():
        pass


def fence_old_owner():
    # A prepared 2PC participant holds FOR SHARE on ownership until resolved;
    # this UPDATE blocks until commit/rollback of all in-flight prepared work.
    # Retry safely if the durable Coordinator needs time to recover.
    with connect('B') as con:
        con.execute("SET LOCAL lock_timeout='2500ms'")
        con.execute("UPDATE ownership SET writable=FALSE,epoch=2 WHERE zone='moving' AND (writable=TRUE OR epoch<>2)")
    log.info('FENCING: B writes disabled for A1501..A2000')


def validate_frozen():
    with connect('B') as b, connect('C') as c:
        old=b.execute("SELECT account_id,balance FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000' ORDER BY account_id").fetchall()
        new=c.execute("SELECT account_id,balance FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000' ORDER BY account_id").fetchall()
        if old != new or len(new)!=500:
            raise RuntimeError('balance verification mismatch; refusing activation')
        query="""SELECT request_id,op_type,payload,result FROM local_requests
            WHERE (payload->>'account' BETWEEN 'A1501' AND 'A2000')
               OR (payload->>'source' BETWEEN 'A1501' AND 'A2000')
               OR (payload->>'destination' BETWEEN 'A1501' AND 'A2000') ORDER BY request_id"""
        b_requests=b.execute(query).fetchall()
        c_requests=c.execute(query).fetchall()
        if b_requests != c_requests:
            raise RuntimeError('idempotency journal mismatch; refusing activation')
        remaining=c.execute('SELECT COUNT(*) FROM migration_applied').fetchone()[0]
        log.info('verified 500 account balances; %s replay positions persisted',remaining)


def activate():
    # Before map changes, archive B's committed local request IDs for exactly-once
    # handling of retries whose original same-shard route becomes cross-shard.
    with connect('C') as c:
        legacy=c.execute('SELECT request_id,op_type,payload,result FROM local_requests WHERE result IS NOT NULL').fetchall()
    with connect('META') as metadata:
        with metadata.cursor() as cur:
            cur.executemany('INSERT INTO legacy_local_requests(request_id,op_type,payload,result) '
                'VALUES(%s,%s,%s,%s) ON CONFLICT(request_id) DO NOTHING',[
                 (req_id,op,Jsonb(payload),Jsonb(result)) for req_id,op,payload,result in legacy
                ])
    # On restart, repeated activation is safe. Never re-enable B.
    with connect('C') as c:
        c.execute("UPDATE ownership SET writable=TRUE,epoch=2 WHERE zone='moving'")
    with connect('META') as con:
        con.execute('BEGIN')
        record=con.execute('SELECT shard FROM shard_ranges WHERE first_number=1501 FOR UPDATE').fetchone()
        if record is None:
            con.execute("UPDATE map_epoch SET epoch=GREATEST(epoch+1,2) WHERE id=1")
            con.execute("UPDATE shard_ranges SET last_number=1500 WHERE first_number=1001 AND shard='B'")
            con.execute("INSERT INTO shard_ranges(first_number,last_number,shard,epoch) VALUES(1501,2000,'C',2)")
        elif record[0] != 'C':
            raise RuntimeError('conflicting shard map')
    update('CLEANUP')
    log.info('ACTIVATED C as owner; Shard Map epoch=2')


def cleanup():
    # Deleting stale rows only AFTER active owner and map have been verified.
    with connect('META') as con:
        owner=con.execute('SELECT shard,epoch FROM shard_ranges WHERE first_number=1501').fetchone()
        if owner != ('C',2):
            raise RuntimeError('cutover not verified')
    with connect('C') as con:
        if con.execute("SELECT COUNT(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'").fetchone()[0] != 500:
            raise RuntimeError('C account count mismatch')
    with connect('B') as con:
        con.execute("DELETE FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'")
        # Retain local_requests and durable outbox for audit / deduplication.
    update('COMPLETED')
    log.info('Migration COMPLETED; B rows removed after verified C activation')


def step():
    state=job()
    phase=state['phase']
    if phase == 'NOT_STARTED' or phase == 'COMPLETED':
        return
    if phase in ('INIT','SNAPSHOT'):
        update('SNAPSHOT')
        snapshot()
    elif phase == 'CATCHUP':
        # Never wait for quiescence while B is writable; a hot workload
        # could otherwise prevent cutover forever.
        catchup()
        if job()['cutover_requested']:
            update('FENCING')
    elif phase == 'FENCING':
        if os.getenv('MIGRATION_FENCE_DELAY'):
            time.sleep(float(os.getenv('MIGRATION_FENCE_DELAY')))
        # Idempotent after crash between old-owner fencing and state change.
        fence_old_owner()
        update('FINAL_REPLAY')
    elif phase == 'FINAL_REPLAY':
        catchup_all()
        if job()['applied_seq'] < current_seq():
            return
        validate_frozen()
        update('ACTIVATING')
    elif phase == 'ACTIVATING':
        activate()
    elif phase == 'CLEANUP':
        if os.getenv('MIGRATION_CLEANUP_DELAY'):
            time.sleep(float(os.getenv('MIGRATION_CLEANUP_DELAY')))
        cleanup()
    else:
        raise RuntimeError('unexpected state '+phase)


def worker():
    while True:
        try:
            with _lock:
                step()
        except (psycopg.Error,RuntimeError) as e:
            log.exception('Migration paused; will retry after failure: %s',e)
            # Error is visible, but state stays retryable.
            try:
                update(error=str(e)[:400])
            except Exception:
                pass
        time.sleep(0.7)


@app.get('/health')
def health():
    return jsonify(ok=True,service='migrator')


@app.get('/status')
def status():
    return jsonify(ok=True,**job())


@app.post('/start')
def start():
    with connect('META') as con:
        con.execute("INSERT INTO migration_job(job_id,phase) VALUES(%s,'INIT') ON CONFLICT DO NOTHING",(WORK,))
    return jsonify(ok=True,**job())


@app.post('/cutover')
def cutover():
    if job()['phase'] not in ('CATCHUP','FENCING','FINAL_REPLAY','ACTIVATING','CLEANUP','COMPLETED'):
        return jsonify(ok=False,error='wait for CATCHUP phase'),409
    update(cutover_requested=True)
    return jsonify(ok=True,**job())


threading.Thread(target=worker,name='migration-state-machine',daemon=True).start()
