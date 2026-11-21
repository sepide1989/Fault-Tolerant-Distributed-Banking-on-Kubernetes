"""Bound DB waits without weakening synchronous commit.

A canceled SyncRep wait can leave a locally committed transaction. Never turn
that exception into success; retries must force another synchronous WAL write.
"""
import os
import psycopg
import threading
from contextlib import contextmanager


def watch_sync_wait(conn):
    # PostgreSQL can cancel a synchronous COMMIT wait with WARNING 57014 and
    # still report command success. psycopg does not raise for that warning.
    conn.banking_sync_canceled = False
    def notice(diag):
        if diag.sqlstate == '57014':
            conn.banking_sync_canceled = True
    conn.add_notice_handler(notice)
    return conn


def assert_durable(conn):
    if conn.banking_sync_canceled:
        # Do not clear this flag and reuse the session: the caller must leave
        # the operation UNKNOWN and reconnect for a synchronous retry barrier.
        raise psycopg.OperationalError('synchronous replication wait canceled; outcome unknown')


@contextmanager
def bounded_db_wait(conn):
    # statement_timeout alone does not reliably bound the post-COMMIT SyncRep
    # wait. Cancel from a separate thread, and treat the outcome as unknown.
    expired = threading.Event()
    def cancel():
        expired.set()
        try:
            conn.cancel_safe(timeout=2)
        except psycopg.Error:
            pass  # keepalive/tcp_user_timeout handles a broken transport
    timer=threading.Timer(float(os.getenv('DB_OPERATION_TIMEOUT_SECONDS','10')),cancel)
    timer.daemon=True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()
    if expired.is_set():
        raise psycopg.OperationalError('database deadline exceeded; outcome unknown')
    assert_durable(conn)


def connection_options():
    return ("-c statement_timeout=" + os.getenv("DB_STATEMENT_TIMEOUT_MS", "7000")
            + " -c lock_timeout=" + os.getenv("DB_LOCK_TIMEOUT_MS", "3000")
            + " -c idle_session_timeout=" + os.getenv("DB_IDLE_SESSION_TIMEOUT_MS", "45000")
            + " -c idle_in_transaction_session_timeout=15000")
