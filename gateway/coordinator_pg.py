"""Kubernetes-safe durable 2PC coordinator using synchronously replicated PostgreSQL.

The service contains NO SQLite state. Transaction decisions live in the CNPG
`bank-meta` primary/standbys. PostgreSQL session advisory locks serialize
execution/recovery across *different* Gateway and recovery Pods.
"""
import logging
import os
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar

import psycopg
from psycopg.types.json import Jsonb
from gateway.routing import ReplicaUnavailable
from gateway.db_runtime import connection_options, watch_sync_wait, bounded_db_wait

log = logging.getLogger(__name__)


class DuplicateRequestMismatch(Exception):
    pass


class Coordinator:
    def __init__(self, router, path=None):
        self.router = router
        self.meta_dsn = os.environ['META_DSN']
        self._arm_crash_after_one_ack = False
        self._session = ContextVar('coordinator_session', default=None)
        # Schema is bootstrapped separately by a Kubernetes Job, BEFORE Gateway.

    @contextmanager
    def connection(self):
        # State reads/writes under tx_lock MUST use its very same session.
        # Reconnecting after losing that session would evade its fencing lock.
        current = self._session.get()
        if current is not None:
            with bounded_db_wait(current):
                with current.transaction():
                    yield current
        else:
            con=watch_sync_wait(psycopg.connect(self.meta_dsn, connect_timeout=4,
                options=connection_options(), keepalives_idle=5, keepalives_interval=2,
                keepalives_count=2, tcp_user_timeout=10000))
            with bounded_db_wait(con):
                with con:
                    yield con

    @contextmanager
    def tx_lock(self, tx_id):
        """Non-blocking, session-wide lock (not just a per-process Python lock).

        The lock session uses autocommit so commits in other connections never
        release it; PG closes the lock on process death or DB failover.
        """
        with psycopg.connect(self.meta_dsn, connect_timeout=4, autocommit=True,
                             options=connection_options()) as con:
            watch_sync_wait(con)
            acquired = con.execute(
                'SELECT pg_try_advisory_lock(hashtextextended(%s,0))', (tx_id,)
            ).fetchone()[0]
            token = self._session.set(con)
            try:
                yield acquired
            finally:
                self._session.reset(token)
                if acquired:
                    try:
                        con.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', (tx_id,))
                    except psycopg.Error:
                        pass

    def register(self, request_id, source, destination, amount, source_shard, dest_shard):
        if source_shard == dest_shard:
            raise ValueError('2PC requires distinct shards')
        now = time.time()
        tx_id = uuid.uuid4().hex
        with self.connection() as con:
            inserted = con.execute(
                '''INSERT INTO transactions(tx_id,request_id,source_account,
                destination_account,amount,source_shard,destination_shard,
                phase,decision,created_at,updated_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,'INITIATED',NULL,%s,%s)
                ON CONFLICT(request_id) DO NOTHING RETURNING tx_id''',
                (tx_id, request_id, source, destination, amount, source_shard, dest_shard, now, now)
            ).fetchone()
            if inserted:
                return tx_id, True
            existing = con.execute('SELECT * FROM transactions WHERE request_id=%s', (request_id,)).fetchone()
            if not existing:
                raise RuntimeError('concurrent transaction registration is not yet visible')
            if not (existing[2] == source and existing[3] == destination and existing[4] == amount):
                raise DuplicateRequestMismatch('request_id used for another transfer')
            return existing[0], False

    def reserve_request(self, request_id, operation, payload):
        # One ID namespace across shards and local/2PC routes. The registry
        # itself has no banking effect and is safe to migrate idempotently.
        with self.connection() as con:
            con.execute('''INSERT INTO request_registry(request_id,operation,payload)
                           VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',
                        (request_id,operation,Jsonb(payload)))
            row=con.execute('SELECT operation,payload FROM request_registry WHERE request_id=%s FOR UPDATE',
                            (request_id,)).fetchone()
            if row != (operation,payload):
                raise DuplicateRequestMismatch('request_id used for a different operation/payload')
            con.execute('UPDATE request_registry SET request_id=request_id WHERE request_id=%s',(request_id,))

    def registered_request(self, request_id):
        with self.connection() as con:
            row=con.execute('SELECT operation,payload FROM request_registry WHERE request_id=%s',(request_id,)).fetchone()
        return {'operation':row[0],'payload':row[1]} if row else None

    def _record_phase(self, tx_id, phase, decision=None, error=None):
        with self.connection() as con:
            row = con.execute('SELECT decision,phase FROM transactions WHERE tx_id=%s FOR UPDATE', (tx_id,)).fetchone()
            if not row:
                raise RuntimeError('unknown transaction')
            previous = row[0]
            if previous and decision and previous != decision:
                raise RuntimeError('FATAL: attempted to reverse durable 2PC decision')
            if row[1] == 'COMPLETED' and phase != 'COMPLETED':
                raise RuntimeError('cannot reopen completed transaction')
            con.execute('''UPDATE transactions SET phase=%s,decision=COALESCE(%s,decision),
                           error=COALESCE(%s,error),updated_at=%s WHERE tx_id=%s''',
                        (phase,decision,error,time.time(),tx_id))

    def _vote(self, tx_id, shard, vote):
        with self.connection() as con:
            con.execute('''INSERT INTO votes(tx_id,shard,vote) VALUES(%s,%s,%s)
                           ON CONFLICT(tx_id,shard) DO UPDATE SET vote=excluded.vote''',
                        (tx_id,shard,vote))

    def _ack(self, tx_id, shard, decision):
        with self.connection() as con:
            con.execute('''INSERT INTO acknowledgements(tx_id,shard,decision)
                           VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',
                        (tx_id,shard,decision))

    def raw_transaction(self, tx_id=None, request_id=None):
        with self.connection() as con:
            if tx_id:
                row = con.execute('SELECT * FROM transactions WHERE tx_id=%s', (tx_id,))
            else:
                row = con.execute('SELECT * FROM transactions WHERE request_id=%s', (request_id,))
            found = row.fetchone()
            return dict(zip([d.name for d in row.description], found)) if found else None

    def status(self, tx_id=None, request_id=None):
        record = self.raw_transaction(tx_id=tx_id,request_id=request_id)
        if record is None:
            return {'status':'UNKNOWN', 'transaction_id':tx_id, 'request_id':request_id}
        if record['phase'] == 'COMPLETED':
            # Read-back alone cannot prove replication after a lost COMMIT reply
            # or canceled SyncRep wait. Force WAL before returning terminal state.
            self._record_phase(record['tx_id'], 'COMPLETED', record['decision'])
        with self.connection() as con:
            votes = dict(con.execute('SELECT shard,vote FROM votes WHERE tx_id=%s',(record['tx_id'],)).fetchall())
            acked = [x[0] for x in con.execute('''SELECT shard FROM acknowledgements
                            WHERE tx_id=%s ORDER BY shard''',(record['tx_id'],)).fetchall()]
        return {
            'transaction_id':record['tx_id'], 'request_id':record['request_id'],
            'source':record['source_account'], 'destination':record['destination_account'],
            'amount':record['amount'], 'phase':record['phase'], 'decision':record['decision'],
            'status':('PENDING' if record['phase'] != 'COMPLETED' else
                      ('COMMITTED' if record['decision']=='COMMIT' else 'ABORTED')),
            'votes':votes, 'acknowledged_by':acked, 'error':record['error'],
        }

    def arm_crash(self):
        self._arm_crash_after_one_ack = True
        return {'armed':True, 'trigger':'After durable COMMIT and first shard ACK'}

    def execute_new(self, tx_id):
        with self.tx_lock(tx_id) as acquired:
            if not acquired:
                return self.status(tx_id=tx_id)
            row=self.raw_transaction(tx_id=tx_id)
            if not row or row['phase'] != 'INITIATED':
                return self.status(tx_id=tx_id)
            self._record_phase(tx_id,'PREPARING')
            modifications={
                row['source_shard']:[{'account':row['source_account'],'delta':-row['amount']}],
                row['destination_shard']:[{'account':row['destination_account'],'delta':row['amount']}],
            }
            for shard in sorted(modifications):
                try:
                    code,resp=self.router.request(shard,'POST','/internal/2pc/prepare',{
                        'tx_id':tx_id, 'modifications':modifications[shard],
                        'epoch':self.router.locate(modifications[shard][0]['account'])[1],
                    })
                    if code==200 and resp.get('vote')=='YES':
                        self._vote(tx_id,shard,'YES')
                    else:
                        self._vote(tx_id,shard,'NO')
                        self._record_phase(tx_id,'ABORT_DECIDED',decision='ABORT',
                                           error=resp.get('reason') or resp.get('error') or 'participant rejected')
                        return self._finish_decision(tx_id)
                except ReplicaUnavailable as exc:
                    # An unanswered prepare may still be in-flight: durably record ABORT.
                    self._record_phase(tx_id,'ABORT_DECIDED',decision='ABORT',error=str(exc))
                    return self._finish_decision(tx_id)
            self._record_phase(tx_id,'PREPARED')
            # Critical correctness boundary: durable metadata COMMIT first.
            self._record_phase(tx_id,'COMMIT_DECIDED',decision='COMMIT')
            return self._finish_decision(tx_id)

    def _finish_decision(self, tx_id):
        row=self.raw_transaction(tx_id=tx_id)
        decision=row['decision']
        if decision not in ('COMMIT','ABORT'):
            raise RuntimeError('2PC decision is not durable yet')
        # A former attempt may have committed locally before losing its session.
        # Reconfirm replication before sending ANY participant decision.
        self._record_phase(tx_id, row['phase'], decision=decision)
        shards={row['source_shard'],row['destination_shard']}
        with self.connection() as con:
            acked={x[0] for x in con.execute('''SELECT shard FROM acknowledgements
               WHERE tx_id=%s AND decision=%s''',(tx_id,decision)).fetchall()}
        action='commit' if decision=='COMMIT' else 'abort'
        for shard in sorted(shards):
            if shard in acked: continue
            try:
                code,body=self.router.request(shard,'POST',f'/internal/2pc/{action}',{'tx_id':tx_id})
                if code==200 and body.get('ok') and body.get('outcome')==('COMMITTED' if decision=='COMMIT' else 'ABORTED'):
                    self._ack(tx_id,shard,decision)
                    acked.add(shard)
                    if decision=='COMMIT' and self._arm_crash_after_one_ack:
                        self._arm_crash_after_one_ack=False
                        log.error('FAULT INJECTION: gateway killed after one commit ACK: %s',tx_id)
                        os._exit(42)
                else:
                    log.warning('decision pending tx=%s shard=%s response=%s',tx_id,shard,body)
            except ReplicaUnavailable as exc:
                log.warning('decision pending tx=%s shard=%s error=%s',tx_id,shard,exc)
        if shards == acked:
            self._record_phase(tx_id,'COMPLETED')
        return self.status(tx_id=tx_id)

    def recover_once(self):
        with self.connection() as con:
            pending=[r[0] for r in con.execute('''SELECT tx_id FROM transactions
                WHERE phase != 'COMPLETED' AND (decision IS NOT NULL OR updated_at < %s)
                ORDER BY updated_at LIMIT 200''',
                (time.time()-float(os.getenv('RECOVERY_STALE_SECONDS','30')),)).fetchall()]
        for tx_id in pending:
            try:
                with self.tx_lock(tx_id) as acquired:
                    if not acquired: continue
                    row=self.raw_transaction(tx_id=tx_id)
                    if not row or row['phase']=='COMPLETED':continue
                    if row['decision'] is None:
                        self._record_phase(tx_id,'ABORT_DECIDED',decision='ABORT',
                                           error='recovered after interruption before commit decision')
                    self._finish_decision(tx_id)
            except Exception:
                log.exception('recovery retry required for %s', tx_id)
