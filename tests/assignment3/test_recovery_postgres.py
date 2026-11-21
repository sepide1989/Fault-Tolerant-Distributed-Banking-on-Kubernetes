"""Real PostgreSQL regression tests; never uses the banking VM databases.

BANKING_TEST_DSN must address a disposable PostgreSQL with max_prepared_transactions.
Example: BANKING_TEST_DSN=postgresql://postgres@127.0.0.1:55439/postgres
"""
import concurrent.futures
from contextlib import contextmanager
import importlib
import os
import time
import uuid

import psycopg
from psycopg import sql
import pytest

from gateway.k8s_bootstrap import SHARD_SCHEMA, META_COORD_SCHEMA
from gateway.coordinator_pg import Coordinator, DuplicateRequestMismatch


def test_restore_cutoff_creates_archivable_record_without_ledger_changes(databases,monkeypatch):
    from scripts import verify_external_restore as drill
    with psycopg.connect(databases['META'],autocommit=True) as con:
        before=con.execute('SELECT row_to_json(t)::text FROM transactions t ORDER BY tx_id').fetchall()
        start=con.execute('SELECT pg_current_wal_insert_lsn()::text').fetchone()[0]
        monkeypatch.setattr(drill.cs,'sql_pod',lambda pod,query:str(con.execute(query).fetchone()[0]))
        cutoff,wanted=drill.create_archive_cutoff('disposable','test_dr_'+uuid.uuid4().hex[:12])
        assert con.execute('SELECT pg_wal_lsn_diff(%s::pg_lsn,%s::pg_lsn)>0',(cutoff,start)).fetchone()[0]
        current=con.execute('SELECT pg_walfile_name(pg_current_wal_insert_lsn())').fetchone()[0]
        assert current>wanted
        assert before==con.execute('SELECT row_to_json(t)::text FROM transactions t ORDER BY tx_id').fetchall()


@pytest.fixture(scope='module')
def databases():
    dsn = os.getenv('BANKING_TEST_DSN')
    if not dsn:
        pytest.skip('BANKING_TEST_DSN not provided: live-cluster evidence is separate')
    names = {s:'banking_test_'+uuid.uuid4().hex[:12] for s in ('A','B','META')}
    dsns = {}
    with psycopg.connect(dsn, autocommit=True) as admin:
        for shard, name in names.items():
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
            dsns[shard] = psycopg.conninfo.make_conninfo(dsn, dbname=name)
    for shard, subdsn in dsns.items():
        with psycopg.connect(subdsn) as con:
            con.execute(META_COORD_SCHEMA if shard=='META' else SHARD_SCHEMA)
            if shard != 'META':
                low, high = (1,1000) if shard=='A' else (1001,2000)
                con.execute('CREATE TABLE ownership(first_number int,last_number int,epoch int,writable bool)')
                con.execute('INSERT INTO ownership VALUES(%s,%s,1,true)', (low,high))
    yield dsns
    # Leave only disposable test DBs until this fixture ends; no live PVC/VM operation.
    with psycopg.connect(dsn, autocommit=True) as admin:
        for name in names.values():
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


@pytest.fixture
def system(databases, monkeypatch):
    monkeypatch.setenv('META_DSN',databases['META'])
    monkeypatch.setenv('RECOVERY_STALE_SECONDS','0')
    bank = importlib.import_module('bank.app')
    # PostgreSQL GIDs are server-wide. Production uses separate PG servers per
    # shard; our disposable server uses separate databases, so namespace GIDs.
    monkeypatch.setattr(bank, 'gid', lambda tx: 'bank_'+bank.check_tx_id(tx)+'_'+bank.SHARD)
    for shard, dsn in databases.items():
        with psycopg.connect(dsn, autocommit=True) as con:
            # Clean ONLY explicitly named, randomly created disposable databases.
            for (gid,) in con.execute('SELECT gid FROM pg_prepared_xacts WHERE database=current_database()'):
                con.execute(sql.SQL('ROLLBACK PREPARED {}').format(sql.Literal(gid)))
            if shard=='META':
                con.execute('TRUNCATE transactions,votes,acknowledgements,request_registry')
            else:
                con.execute('TRUNCATE accounts,local_requests,participant_results,participant_fences')
                con.execute('INSERT INTO accounts VALUES(%s,1000)', ('A0500' if shard=='A' else 'A1500',))

    class Router:
        fail_commit_b = False
        fail_after_ack = False
        def locate(self, account):
            return ('A' if int(account[1:])<=1000 else 'B'),1
        def request(self, shard, method, path, data=None):
            from gateway.routing import ReplicaUnavailable
            if self.fail_commit_b and shard=='B' and path.endswith('/commit'):
                raise ReplicaUnavailable('injected disconnect after decision')
            bank.DSN=databases[shard]
            bank.SHARD=shard
            with bank.app.test_client() as client:
                response=client.open(path,method=method,json=data)
                return response.status_code,response.get_json()

    router=Router()
    return Coordinator(router), router, bank, databases


def register(coord, amount=3):
    return coord.register('test-'+uuid.uuid4().hex,'A0500','A1500',amount,'A','B')[0]


def test_commit_recovery_idempotency(system):
    coord, router, bank, dsns=system
    tx=register(coord)
    router.fail_commit_b=True
    state=coord.execute_new(tx)
    assert state['status']=='PENDING' and state['decision']=='COMMIT'
    assert state['acknowledged_by']==['A']
    router.fail_commit_b=False
    Coordinator(router).recover_once()
    assert coord.status(tx_id=tx)['status']=='COMMITTED'
    for _ in range(3):
        coord.recover_once()
    for shard, expected in [('A',997),('B',1003)]:
        with psycopg.connect(dsns[shard]) as con:
            assert con.execute('SELECT balance FROM accounts').fetchone()[0]==expected
    row=coord.raw_transaction(tx_id=tx)
    assert coord.register(row['request_id'],'A0500','A1500',3,'A','B')==(tx,False)
    with pytest.raises(DuplicateRequestMismatch):
        coord.register(row['request_id'],'A0500','A1500',4,'A','B')


def test_undecided_prepared_recovers_through_coordinator(system):
    coord, router, bank, dsns=system
    tx=register(coord)
    with coord.tx_lock(tx) as acquired:
        assert acquired
        coord._record_phase(tx,'PREPARING')
        for shard, account, delta in [('A','A0500',-3),('B','A1500',3)]:
            code, body=router.request(shard,'POST','/internal/2pc/prepare',
                {'tx_id':tx,'modifications':[{'account':account,'delta':delta}],'epoch':1})
            assert code==200 and body['vote']=='YES'
            coord._vote(tx,shard,'YES')
        coord._record_phase(tx,'PREPARED')
    Coordinator(router).recover_once()
    assert coord.status(tx_id=tx)['status']=='ABORTED'
    for shard in ('A','B'):
        with psycopg.connect(dsns[shard]) as con:
            assert con.execute('SELECT balance FROM accounts').fetchone()[0]==1000
            assert con.execute('SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()').fetchone()[0]==0


def test_lock_session_loss_cannot_reconnect_and_reverse_decision(system):
    coord, router, bank, dsns=system
    tx=register(coord)
    with coord.tx_lock(tx) as acquired:
        assert acquired
        pid=coord._session.get().info.backend_pid
        with psycopg.connect(dsns['META'],autocommit=True) as con:
            con.execute('SELECT pg_terminate_backend(%s)',(pid,))
        Coordinator(router).recover_once()
        with pytest.raises(psycopg.Error):
            coord._record_phase(tx,'COMMIT_DECIDED',decision='COMMIT')
    assert coord.status(tx_id=tx)['status']=='ABORTED'


def test_idle_lock_session_expires_and_recovery_is_fenced(system,monkeypatch):
    coord,router,bank,dsns=system
    monkeypatch.setenv('DB_IDLE_SESSION_TIMEOUT_MS','300')
    tx=register(coord)
    with coord.tx_lock(tx) as acquired:
        assert acquired
        time.sleep(.6)  # Models an unresponsive coordinator, not an SQL decision.
        Coordinator(router).recover_once()
        with pytest.raises(psycopg.Error):
            coord._record_phase(tx,'COMMIT_DECIDED',decision='COMMIT')
    assert coord.status(tx_id=tx)['status']=='ABORTED'


def test_participant_delayed_prepare_cannot_pass_abort_tombstone(system):
    coord, router, bank, dsns=system
    tx=register(coord)
    assert router.request('A','POST','/internal/2pc/abort',{'tx_id':tx})[0]==200
    code,body=router.request('A','POST','/internal/2pc/prepare',
        {'tx_id':tx,'modifications':[{'account':'A0500','delta':-3}],'epoch':1})
    assert code==200 and body['vote']=='NO'
    bank.DSN=dsns['A']
    with bank.participant_session(tx):
        assert router.request('A','POST','/internal/2pc/abort',{'tx_id':tx})[0]==503


def test_local_duplicate_applies_once(system):
    coord, router, bank, dsns=system
    bank.DSN=dsns['A']; bank.SHARD='A'
    payload={'account':'A0500','amount':1,'request_id':'identical-local','epoch':1}
    def request_once(_):
        with bank.app.test_client() as client:
            return client.post('/internal/deposit',json=payload).status_code
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        assert set(pool.map(request_once,range(16)))=={200}
    with psycopg.connect(dsns['A']) as con:
        assert con.execute('SELECT balance FROM accounts').fetchone()[0]==1001


def test_request_id_namespace_spans_shards_and_operations(system):
    coord,router,bank,dsns=system
    coord.reserve_request('global-id','deposit',{'account':'A0500','amount':1})
    Coordinator(router).reserve_request('global-id','deposit',{'account':'A0500','amount':1})
    for operation,payload in [('deposit',{'account':'A1500','amount':1}),
                              ('withdraw',{'account':'A0500','amount':1})]:
        with pytest.raises(DuplicateRequestMismatch):coord.reserve_request('global-id',operation,payload)


def test_sync_wait_warning_is_not_success_and_retry_barrier(system,monkeypatch):
    coord, router, bank, dsns=system
    monkeypatch.setenv('DB_STATEMENT_TIMEOUT_MS','400')
    monkeypatch.setenv('DB_OPERATION_TIMEOUT_SECONDS','1')
    bank.DSN=dsns['A']; bank.SHARD='A'
    admin_dsn=os.environ['BANKING_TEST_DSN']
    # Whole-server change is limited to the deliberately disposable test server.
    with psycopg.connect(admin_dsn,autocommit=True) as admin:
        old=admin.execute('SHOW synchronous_standby_names').fetchone()[0]
        assert not old, 'test server must be standalone/disposable'
        admin.execute("ALTER SYSTEM SET synchronous_standby_names='ANY 1 (missing_test_standby)'")
        admin.execute('SELECT pg_reload_conf()')
        time.sleep(.3)
        try:
            payload={'account':'A0500','amount':1,'request_id':'sync-unknown','epoch':1}
            with bank.app.test_client() as client:
                assert client.post('/internal/deposit',json=payload).status_code==503
                assert client.get('/internal/requests/'+payload['request_id']).status_code==503
                # The marker exists locally; duplicate read-back must STILL block
                # on replication rather than announcing success.
                assert client.post('/internal/deposit',json=payload).status_code==503
        finally:
            admin.execute("ALTER SYSTEM RESET synchronous_standby_names")
            admin.execute('SELECT pg_reload_conf()')
            time.sleep(.3)
    with bank.app.test_client() as client:
        assert client.post('/internal/deposit',json=payload).status_code==200
    with psycopg.connect(dsns['A']) as con:
        assert con.execute('SELECT balance FROM accounts').fetchone()[0]==1001
