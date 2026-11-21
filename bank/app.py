"""Stateless application replica for one authoritative PostgreSQL shard."""
import json
import hashlib
import time
import logging
import os
import re
from contextlib import contextmanager
from contextlib import suppress

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from flask import Flask, jsonify, request
from gateway.db_runtime import connection_options, watch_sync_wait, bounded_db_wait
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
SHARD = os.environ.get("SHARD_ID", "A")
REPLICA = os.environ.get("REPLICA_ID", "A1")
DSN = os.environ.get("DB_DSN", "postgresql://bank:bankpass@localhost:5432/bank")
assert SHARD in ("A", "B", "C")


class BankError(Exception):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def owns(account):
    if not isinstance(account, str) or not re.fullmatch(r"A\d{4}", account):
        return False
    n = int(account[1:])
    return (1 <= n <= 1000) if SHARD == "A" else ((1001 <= n <= 2000) if SHARD == "B" else (1501 <= n <= 2000))


def check_account(account):
    if not owns(account):
        raise BankError(f"{account!r} does not belong to Shard {SHARD}", 400)


def check_amount(amount):
    if type(amount) is not int or not (0 < amount <= 1_000_000_000):
        raise BankError("amount must be a positive integer <= 1000000000")
    return amount


def check_request_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", value):
        raise BankError("request_id must be 1..100 safe characters")
    return value


def check_tx_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{32}", value):
        raise BankError("invalid transaction ID")
    return value


def gid(tx_id):
    return "bank_" + check_tx_id(tx_id)


@contextmanager
def db(*, autocommit=False):
    conn=watch_sync_wait(psycopg.connect(DSN, autocommit=autocommit, connect_timeout=3,
        options=connection_options(), keepalives_idle=5, keepalives_interval=2,
        keepalives_count=2, tcp_user_timeout=10000))
    with bounded_db_wait(conn):
        with conn:
            yield conn


@contextmanager
def participant_session(tx_id):
    # Session-level lock survives PREPARE; all HTTP replicas share this lock.
    # In particular a late PREPARE cannot race the ABORT tombstone.
    with db(autocommit=True) as conn:
        locked = conn.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,1))',
                              (tx_id,)).fetchone()[0]
        if not locked:
            raise BankError('participant busy; retry same transaction ID', 503)
        try:
            yield conn
        finally:
            with suppress(psycopg.Error):
                conn.execute('SELECT pg_advisory_unlock(hashtextextended(%s,1))', (tx_id,))


def durability_barrier(conn, tx_id):
    # Independent of the row locked inside the prepared transaction. This WAL
    # write covers a previous locally durable PREPARE/COMMIT/ABORT as well.
    conn.execute('''INSERT INTO participant_fences(tx_id) VALUES(%s)
                    ON CONFLICT(tx_id) DO UPDATE SET tx_id=excluded.tx_id''', (tx_id,))


def prepared_exists(conn, tx_id):
    row = conn.execute(
        "SELECT 1 FROM pg_prepared_xacts WHERE gid = %s AND database = current_database()",
        (gid(tx_id),),
    ).fetchone()
    return bool(row)


def participant_outcome(conn, tx_id):
    row = conn.execute("SELECT status FROM participant_results WHERE tx_id=%s", (tx_id,)).fetchone()
    return row[0] if row else None


def read_balances(cur, account_ids):
    ids = sorted(set(account_ids))
    cur.execute(
        "SELECT account_id, balance FROM accounts "
        "WHERE account_id = ANY(%s) ORDER BY account_id FOR UPDATE",
        (ids,),
    )
    balances = dict(cur.fetchall())
    if set(balances) != set(ids):
        raise BankError("account not found on this shard", 404)
    return balances


def lock_ownership(cur, account_ids, requested_epoch):
    """Shard-side fencing, not just Gateway routing. FOR SHARE blocks cutover
    until local writes or prepared 2PC participants finish.
    """
    if type(requested_epoch) is not int:
        raise BankError("write requires Shard Map epoch", 409)
    for account in sorted(set(account_ids)):
        n = int(account[1:])
        cur.execute("SELECT epoch,writable FROM ownership WHERE %s BETWEEN first_number AND last_number FOR SHARE", (n,))
        row = cur.fetchone()
        if row is None or not row[1] or row[0] != requested_epoch:
            raise BankError("STALE_ROUTE: epoch or owner changed; retry through Gateway", 409)


@app.errorhandler(BankError)
def bad_request(exc):
    return jsonify(ok=False, error=str(exc), replica=REPLICA), exc.code


@app.errorhandler(psycopg.Error)
def db_error(exc):
    app.logger.error("database error on replica %s: %s", REPLICA, exc)
    return jsonify(ok=False, error="database unavailable, busy, or transaction error", replica=REPLICA), 503


@app.get("/health")
def health():
    with db() as conn:
        conn.execute("SELECT 1")
    return jsonify(ok=True, replica=REPLICA, shard=SHARD)


@app.get("/internal/accounts/<account>")
def balance(account):
    check_account(account)
    with db() as conn:
        row = conn.execute("SELECT balance FROM accounts WHERE account_id=%s", (account,)).fetchone()
    if row is None:
        raise BankError("unknown account", 404)
    app.logger.info("BALANCE %s via %s", account, REPLICA)
    return jsonify(ok=True, account=account, balance=row[0], replica=REPLICA, shard=SHARD)


def local_write(operation, data):
    request_id = check_request_id(data.get("request_id"))
    account = data.get("account")
    source = data.get("source")
    destination = data.get("destination")
    amount = check_amount(data.get("amount"))
    if operation in ("deposit", "withdraw"):
        check_account(account)
        operation_payload = {"account": account, "amount": amount}
        ids = [account]
    else:
        check_account(source)
        check_account(destination)
        if source == destination:
            raise BankError("source and destination must differ")
        operation_payload = {"source": source, "destination": destination, "amount": amount}
        ids = [source, destination]

    with db() as conn:
        with conn.cursor() as cur:
            lock_ownership(cur, ids, data.get("epoch"))
            # Unique request ID serializes retries, even when received by a different replica.
            cur.execute(
                "INSERT INTO local_requests(request_id,op_type,payload) "
                "VALUES(%s,%s,%s) ON CONFLICT DO NOTHING RETURNING request_id",
                (request_id, operation, Jsonb(operation_payload)),
            )
            inserted = cur.fetchone() is not None
            if not inserted:
                cur.execute(
                    "SELECT op_type,payload,result FROM local_requests WHERE request_id=%s",
                    (request_id,),
                )
                existing = cur.fetchone()
                if existing is None or existing[0] != operation or existing[1] != operation_payload:
                    raise BankError("request_id was already used for different payload", 409)
                if existing[2] is None:
                    raise BankError("request_id is not yet resolved; retry later", 503)
                result = existing[2]
                cur.execute('UPDATE local_requests SET result=result WHERE request_id=%s', (request_id,))
            else:
                balances = read_balances(cur, ids)
                if operation == "deposit":
                    new = balances[account] + amount
                    cur.execute("UPDATE accounts SET balance=%s WHERE account_id=%s", (new, account))
                    result = {"account": account, "balance": new, "operation": operation}
                elif operation == "withdraw":
                    new = balances[account] - amount
                    if new < 0:
                        raise BankError("insufficient funds", 409)
                    cur.execute("UPDATE accounts SET balance=%s WHERE account_id=%s", (new, account))
                    result = {"account": account, "balance": new, "operation": operation}
                else:
                    if balances[source] < amount:
                        raise BankError("insufficient funds", 409)
                    new_source = balances[source] - amount
                    new_dest = balances[destination] + amount
                    cur.execute("UPDATE accounts SET balance=%s WHERE account_id=%s", (new_source, source))
                    cur.execute("UPDATE accounts SET balance=%s WHERE account_id=%s", (new_dest, destination))
                    result = {
                        "source": source, "destination": destination,
                        "source_balance": new_source, "destination_balance": new_dest,
                        "operation": "same_shard_transfer",
                    }
                cur.execute(
                    "UPDATE local_requests SET result=%s WHERE request_id=%s",
                    (Jsonb(result), request_id),
                )
    app.logger.info("LOCAL %s request=%s via %s", operation, request_id, REPLICA)
    return jsonify(ok=True, request_id=request_id, replica=REPLICA, shard=SHARD, **result)


@app.post("/internal/deposit")
def deposit():
    return local_write("deposit", request.get_json(silent=True) or {})


@app.post("/internal/withdraw")
def withdraw():
    return local_write("withdraw", request.get_json(silent=True) or {})


@app.post("/internal/transfer/local")
def same_shard_transfer():
    return local_write("transfer", request.get_json(silent=True) or {})


@app.post("/internal/2pc/prepare")
def prepare():
    data = request.get_json(silent=True) or {}
    tx_id = check_tx_id(data.get("tx_id"))
    modifications = data.get("modifications")
    if not isinstance(modifications, list) or len(modifications) != 1:
        raise BankError("exactly one account modification expected per shard")
    change = modifications[0]
    account = change.get("account")
    delta = change.get("delta")
    check_account(account)
    if type(delta) is not int or delta == 0 or abs(delta) > 1_000_000_000:
        raise BankError("invalid delta")

    with participant_session(tx_id) as conn:
        outcome = participant_outcome(conn, tx_id)
        if outcome == "COMMITTED":
            durability_barrier(conn, tx_id)
            return jsonify(ok=True, vote="YES", already="COMMITTED", replica=REPLICA)
        if outcome == "ABORTED":
            durability_barrier(conn, tx_id)
            return jsonify(ok=True, vote="NO", reason="already aborted", replica=REPLICA)
        if prepared_exists(conn, tx_id):
            durability_barrier(conn, tx_id)
            return jsonify(ok=True, vote="YES", already="PREPARED", replica=REPLICA)
        try:
            conn.execute("BEGIN")
            # Configurable wait for contested row locks during 2PC PREPARE.
            # Keep this below the Gateway per-replica HTTP timeout (9s).
            conn.execute(
                "SELECT set_config('lock_timeout', %s, true)",
                (os.getenv("PREPARE_LOCK_TIMEOUT", "5s"),),
            )
            with conn.cursor() as cur:
                lock_ownership(cur, [account], data.get("epoch"))
                balances = read_balances(cur, [account])
                target_balance = balances[account] + delta
                if target_balance < 0:
                    raise BankError("insufficient funds", 409)
                cur.execute("UPDATE accounts SET balance=%s WHERE account_id=%s", (target_balance, account))
                # The committed marker becomes visible atomically with the balance change.
                cur.execute("INSERT INTO participant_results(tx_id,status) VALUES(%s,'COMMITTED')", (tx_id,))
                cur.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid(tx_id))))
        except (BankError, psycopg.Error) as exc:
            with suppress(psycopg.Error):
                conn.execute("ROLLBACK")
            app.logger.info("PREPARE NO tx=%s on %s: %s", tx_id, REPLICA, exc)
            return jsonify(ok=True, vote="NO", reason=str(exc), replica=REPLICA)
    app.logger.info("PREPARE YES tx=%s on %s", tx_id, REPLICA)
    return jsonify(ok=True, vote="YES", replica=REPLICA)


@app.post("/internal/2pc/commit")
def commit_prepared():
    tx_id = check_tx_id((request.get_json(silent=True) or {}).get("tx_id"))
    with participant_session(tx_id) as conn:
        outcome = participant_outcome(conn, tx_id)
        if outcome == "COMMITTED":
            durability_barrier(conn, tx_id)
            return jsonify(ok=True, outcome="COMMITTED", already=True, replica=REPLICA)
        if outcome == "ABORTED":
            raise BankError("cannot commit an aborted transaction", 409)
        if not prepared_exists(conn, tx_id):
            raise BankError("prepared transaction missing; cannot assume COMMIT", 409)
        try:
            conn.execute(sql.SQL("COMMIT PREPARED {}").format(sql.Literal(gid(tx_id))))
        except psycopg.Error:
            # A second replica may have committed concurrently: consult durable marker.
            if participant_outcome(conn, tx_id) != "COMMITTED":
                raise
            durability_barrier(conn, tx_id)
    app.logger.info("COMMIT tx=%s on %s", tx_id, REPLICA)
    return jsonify(ok=True, outcome="COMMITTED", replica=REPLICA)


@app.post("/internal/2pc/abort")
def abort_prepared():
    tx_id = check_tx_id((request.get_json(silent=True) or {}).get("tx_id"))
    with participant_session(tx_id) as conn:
        # Bound waits if an in-flight PREPARE holds a uniqueness lock.
        # Recovery will retry instead of hanging forever on a prepared XID.
        conn.execute("SET lock_timeout = '3s'")
        outcome = participant_outcome(conn, tx_id)
        if outcome == "COMMITTED":
            raise BankError("cannot abort a committed transaction", 409)
        if outcome == "ABORTED":
            durability_barrier(conn, tx_id)
            return jsonify(ok=True, outcome="ABORTED", already=True, replica=REPLICA)
        if prepared_exists(conn, tx_id):
            try:
                conn.execute(sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid(tx_id))))
            except psycopg.Error:
                if prepared_exists(conn, tx_id):
                    raise
        # An abort marker is written after rollback, safe to repeat on reconnect.
        conn.execute(
            "INSERT INTO participant_results(tx_id,status) VALUES(%s,'ABORTED') "
            "ON CONFLICT (tx_id) DO NOTHING", (tx_id,),
        )
        if participant_outcome(conn, tx_id) != "ABORTED":
            raise BankError("abort encountered a conflicting decision", 409)
    app.logger.info("ABORT tx=%s on %s", tx_id, REPLICA)
    return jsonify(ok=True, outcome="ABORTED", replica=REPLICA)


@app.get("/internal/2pc/status/<tx_id>")
def status(tx_id):
    check_tx_id(tx_id)
    with db(autocommit=True) as conn:
        result = participant_outcome(conn, tx_id)
        if not result and prepared_exists(conn, tx_id):
            result = "PREPARED"
        if result:
            durability_barrier(conn, tx_id)
    return jsonify(ok=True, tx_id=tx_id, status=result or "UNKNOWN", replica=REPLICA)


@app.get("/internal/requests/<request_id>")
def read_local_request(request_id):
    check_request_id(request_id)
    with db() as conn:
        record = conn.execute("SELECT op_type,payload,result FROM local_requests WHERE request_id=%s", (request_id,)).fetchone()
        if record and record[2] is not None:
            conn.execute('UPDATE local_requests SET result=result WHERE request_id=%s',(request_id,))
    if not record or record[2] is None:
        return jsonify(ok=False, found=False), 404
    return jsonify(ok=True, found=True, op_type=record[0], payload=record[1], result=record[2])


# Kubernetes observability and workload-driven HPA demonstration.
_BANK_HTTP = Counter('bank_http_requests_total', 'Bank replica requests', ['method','endpoint','status'])
_BANK_LATENCY = Histogram('bank_http_request_seconds','Bank request latency',['endpoint'])
_BANK_DB_UP = Gauge('bank_db_available', 'Writable shard DB is reachable and primary')
_BANK_PREPARED = Gauge('bank_db_prepared_transactions', 'Pending 2PC prepared transactions')

@app.before_request
def _obs_start():
    request._obs_time = time.monotonic()

@app.after_request
def _obs_end(response):
    endpoint = (request.url_rule.rule if request.url_rule else 'unknown')
    _BANK_HTTP.labels(request.method,endpoint,str(response.status_code)).inc()
    _BANK_LATENCY.labels(endpoint).observe(time.monotonic()-getattr(request,'_obs_time',time.monotonic()))
    return response

@app.get('/metrics')
def metrics():
    try:
        with db(autocommit=True) as con:
            standby=con.execute('SELECT pg_is_in_recovery()').fetchone()[0]
            prepared=con.execute('''SELECT count(*) FROM pg_prepared_xacts
                WHERE database=current_database()''').fetchone()[0]
        _BANK_DB_UP.set(0 if standby else 1)
        _BANK_PREPARED.set(prepared)
    except Exception:
        _BANK_DB_UP.set(0)
    return generate_latest(), 200, {'Content-Type':CONTENT_TYPE_LATEST}

@app.get('/internal/cpu-load')
def cpu_load():
    # Disabled by default; use only to demonstrate real CPU-based HPA.
    if os.getenv('ENABLE_CPU_LOAD_ENDPOINT')!='1':
        return jsonify(ok=False, error='load endpoint disabled'),403
    rounds=min(150000,max(1000,int(request.args.get('iterations',50000))))
    payload=b'bank-cpu-load'
    for i in range(rounds):
        payload=hashlib.sha256(payload+i.to_bytes(4,'big')).digest()
    return jsonify(ok=True, iterations=rounds, digest=payload.hex())


@app.get('/live')
def live():
    return jsonify(ok=True, process='alive', replica=REPLICA)
