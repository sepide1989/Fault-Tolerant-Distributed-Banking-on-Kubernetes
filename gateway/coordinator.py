"""Durable Two-Phase Commit coordinator backed by SQLite WAL.

IMPORTANT: The coordinator runs within the Gateway container (a single Gunicorn
worker) and its volume must survive container crashes. An irreversible COMMIT
never changes to ABORT. Participant operations are idempotent.
"""
import logging
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager

from gateway.routing import ReplicaUnavailable

log = logging.getLogger(__name__)


class DuplicateRequestMismatch(Exception):
    pass


class Coordinator:
    def __init__(self, router, path=None):
        self.router = router
        self.path = path or os.environ.get("COORD_DB", "/state/coordinator.sqlite3")
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        self._locks = {}
        self._locks_lock = threading.Lock()
        self._arm_crash_after_one_ack = False
        self._init_db()
        self._worker = threading.Thread(target=self._recovery_loop, name="2pc-recovery", daemon=True)
        self._worker.start()

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self.connection() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript("""
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    source_account TEXT NOT NULL,
                    destination_account TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    source_shard TEXT NOT NULL,
                    destination_shard TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    decision TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS votes (
                    tx_id TEXT NOT NULL,
                    shard TEXT NOT NULL,
                    vote TEXT NOT NULL,
                    PRIMARY KEY (tx_id, shard)
                );
                CREATE TABLE IF NOT EXISTS acknowledgements (
                    tx_id TEXT NOT NULL,
                    shard TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    PRIMARY KEY (tx_id, shard)
                );
            """)

    def _lock_for(self, tx_id):
        with self._locks_lock:
            return self._locks.setdefault(tx_id, threading.Lock())

    def register(self, request_id, source, destination, amount, source_shard, dest_shard):
        tx_id = uuid.uuid4().hex
        now = time.time()
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute("SELECT * FROM transactions WHERE request_id=?", (request_id,)).fetchone()
            if existing is not None:
                same = (
                    existing["source_account"] == source and existing["destination_account"] == destination
                    and existing["amount"] == amount
                )
                if not same:
                    raise DuplicateRequestMismatch("request_id used for another transfer")
                return existing["tx_id"], False
            con.execute(
                "INSERT INTO transactions(tx_id,request_id,source_account,destination_account,amount,"
                "source_shard,destination_shard,phase,decision,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,'INITIATED',NULL,?,?)",
                (tx_id, request_id, source, destination, amount, source_shard, dest_shard, now, now),
            )
        return tx_id, True

    def _record_phase(self, tx_id, phase, decision=None, error=None):
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT decision FROM transactions WHERE tx_id=?", (tx_id,)).fetchone()
            if row is None:
                raise RuntimeError("unknown transaction")
            if row["decision"] == "COMMIT" and decision == "ABORT":
                raise RuntimeError("FATAL: attempting to abort a durable COMMIT")
            if row["decision"] == "ABORT" and decision == "COMMIT":
                raise RuntimeError("FATAL: attempting to commit a durable ABORT")
            con.execute(
                "UPDATE transactions SET phase=?,decision=COALESCE(?,decision),"
                "error=COALESCE(?,error),updated_at=? WHERE tx_id=?",
                (phase, decision, error, time.time(), tx_id),
            )

    def _vote(self, tx_id, shard, vote):
        with self.connection() as con:
            con.execute(
                "INSERT INTO votes(tx_id,shard,vote) VALUES(?,?,?) "
                "ON CONFLICT(tx_id,shard) DO UPDATE SET vote=excluded.vote",
                (tx_id, shard, vote),
            )

    def _ack(self, tx_id, shard, decision):
        with self.connection() as con:
            con.execute(
                "INSERT OR IGNORE INTO acknowledgements(tx_id,shard,decision) VALUES(?,?,?)",
                (tx_id, shard, decision),
            )

    def raw_transaction(self, tx_id=None, request_id=None):
        with self.connection() as con:
            if tx_id:
                row = con.execute("SELECT * FROM transactions WHERE tx_id=?", (tx_id,)).fetchone()
            else:
                row = con.execute("SELECT * FROM transactions WHERE request_id=?", (request_id,)).fetchone()
            return dict(row) if row else None

    def status(self, tx_id=None, request_id=None):
        record = self.raw_transaction(tx_id=tx_id, request_id=request_id)
        if record is None:
            return {"status": "UNKNOWN", "transaction_id": tx_id, "request_id": request_id}
        with self.connection() as con:
            votes = dict(con.execute("SELECT shard,vote FROM votes WHERE tx_id=?", (record["tx_id"],)).fetchall())
            acked = [row[0] for row in con.execute(
                "SELECT shard FROM acknowledgements WHERE tx_id=? ORDER BY shard", (record["tx_id"],)
            ).fetchall()]
        if record["phase"] != "COMPLETED":
            public_status = "PENDING"
        else:
            public_status = "COMMITTED" if record["decision"] == "COMMIT" else "ABORTED"
        return {
            "transaction_id": record["tx_id"],
            "request_id": record["request_id"],
            "source": record["source_account"], "destination": record["destination_account"],
            "amount": record["amount"], "phase": record["phase"],
            "decision": record["decision"], "status": public_status,
            "votes": votes, "acknowledged_by": acked,
            "error": record["error"],
        }

    def arm_crash(self):
        self._arm_crash_after_one_ack = True
        return {"armed": True, "trigger": "After durable COMMIT and one shard acknowledgment"}

    def execute_new(self, tx_id):
        lock = self._lock_for(tx_id)
        with lock:
            row = self.raw_transaction(tx_id=tx_id)
            if row["phase"] != "INITIATED":
                return self.status(tx_id=tx_id)
            self._record_phase(tx_id, "PREPARING")
            modifications = {
                row["source_shard"]: [{"account": row["source_account"], "delta": -row["amount"]}],
                row["destination_shard"]: [{"account": row["destination_account"], "delta": row["amount"]}],
            }
            # Stable global order avoids circular waits between concurrent
            # A->B and B->A transfers holding prepared PostgreSQL locks.
            for shard in sorted((row["source_shard"], row["destination_shard"])):
                try:
                    code, response = self.router.request(
                        shard, "POST", "/internal/2pc/prepare",
                        {"tx_id": tx_id, "modifications": modifications[shard],
                         "epoch": self.router.locate(modifications[shard][0]["account"])[1]},
                    )
                    if code == 200 and response.get("vote") == "YES":
                        self._vote(tx_id, shard, "YES")
                    else:
                        self._vote(tx_id, shard, "NO")
                        reason = response.get("reason") or response.get("error") or "participant rejected"
                        self._record_phase(tx_id, "ABORT_DECIDED", decision="ABORT", error=reason)
                        return self._finish_decision(tx_id)
                except ReplicaUnavailable as exc:
                    # The vote may have been sent but the acknowledgement lost.
                    # Durable ABORT ensures such uncertain PREPARE is eventually rolled back.
                    self._record_phase(tx_id, "ABORT_DECIDED", decision="ABORT", error=str(exc))
                    return self._finish_decision(tx_id)

            self._record_phase(tx_id, "PREPARED")
            # Fsync decision before sending any COMMIT message.
            self._record_phase(tx_id, "COMMIT_DECIDED", decision="COMMIT")
            return self._finish_decision(tx_id)

    def _finish_decision(self, tx_id):
        row = self.raw_transaction(tx_id=tx_id)
        decision = row["decision"]
        if decision not in ("COMMIT", "ABORT"):
            raise RuntimeError("no durable decision")
        shards = (row["source_shard"], row["destination_shard"])
        with self.connection() as con:
            acked = {x[0] for x in con.execute(
                "SELECT shard FROM acknowledgements WHERE tx_id=? AND decision=?", (tx_id, decision)
            ).fetchall()}
        action = "commit" if decision == "COMMIT" else "abort"
        for shard in shards:
            if shard in acked:
                continue
            try:
                code, response = self.router.request(
                    shard, "POST", f"/internal/2pc/{action}", {"tx_id": tx_id}
                )
                if code == 200 and response.get("ok") and response.get("outcome") == ("COMMITTED" if decision == "COMMIT" else "ABORTED"):
                    self._ack(tx_id, shard, decision)
                    acked.add(shard)
                    if decision == "COMMIT" and self._arm_crash_after_one_ack:
                        self._arm_crash_after_one_ack = False
                        log.error("DEMO: crash after durable COMMIT and one ACK for tx=%s", tx_id)
                        # Ask the Gunicorn master to stop, so the whole Gateway
                        # container exits instead of silently replacing one worker.
                        import signal
                        os.kill(os.getppid(), signal.SIGTERM)
                        os._exit(42)
                else:
                    log.warning("2PC %s pending on shard %s, response=%s", decision, shard, response)
            except ReplicaUnavailable as exc:
                log.warning("2PC %s pending on shard %s: %s", decision, shard, exc)
        if set(shards) == acked:
            self._record_phase(tx_id, "COMPLETED")
        return self.status(tx_id=tx_id)

    def recover_once(self):
        with self.connection() as con:
            pending = [r[0] for r in con.execute(
                "SELECT tx_id FROM transactions WHERE phase != 'COMPLETED' ORDER BY created_at"
            ).fetchall()]
        for tx_id in pending:
            lock = self._lock_for(tx_id)
            if not lock.acquire(blocking=False):
                continue
            try:
                row = self.raw_transaction(tx_id=tx_id)
                if not row or row["phase"] == "COMPLETED":
                    continue
                if row["decision"] is None:
                    # No global commit has been decided: safe to ABORT any prepared branches.
                    self._record_phase(tx_id, "ABORT_DECIDED", decision="ABORT", error="recovered without a commit decision")
                self._finish_decision(tx_id)
            except Exception:
                log.exception("recovery failed for transaction %s (will retry)", tx_id)
            finally:
                lock.release()

    def _recovery_loop(self):
        # A short startup delay allows the web process to initialize.
        time.sleep(3)
        while True:
            try:
                self.recover_once()
            except Exception:
                log.exception("2PC recovery loop error")
            time.sleep(3)
