"""Idempotent schema upgrade for existing persistent banks; never drops existing user data."""
import os
import psycopg

DSNS = {
    'A': os.environ.get('DB_A_DSN', 'postgresql://bank:bankpass@db-a:5432/bank'),
    'B': os.environ.get('DB_B_DSN', 'postgresql://bank:bankpass@db-b:5432/bank'),
    'C': os.environ.get('DB_C_DSN', 'postgresql://bank:bankpass@db-c:5432/bank'),
    'META': os.environ.get('META_DSN', 'postgresql://bank:bankpass@meta-db:5432/bank'),
}

BANK_SCHEMA = '''
CREATE TABLE IF NOT EXISTS ownership (
 zone TEXT PRIMARY KEY, first_number INTEGER NOT NULL, last_number INTEGER NOT NULL,
 epoch INTEGER NOT NULL, writable BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_cursor (key TEXT PRIMARY KEY, seq BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS migration_outbox (
 seq BIGINT PRIMARY KEY, kind TEXT NOT NULL, account_id TEXT, request_id TEXT,
 payload JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_applied (seq BIGINT PRIMARY KEY);
CREATE OR REPLACE FUNCTION capture_migration_change() RETURNS TRIGGER AS $$
DECLARE v_seq BIGINT; v_account TEXT;
BEGIN
 IF TG_TABLE_NAME = 'accounts' THEN
   IF NEW.account_id < 'A1501' OR NEW.account_id > 'A2000' THEN RETURN NEW; END IF;
   UPDATE migration_cursor SET seq=seq+1 WHERE key='B' RETURNING seq INTO v_seq;
   IF v_seq IS NULL THEN RETURN NEW; END IF;
   INSERT INTO migration_outbox(seq,kind,account_id,payload)
      VALUES(v_seq,'account',NEW.account_id,jsonb_build_object('balance',NEW.balance));
 ELSE
   IF NEW.result IS NULL THEN RETURN NEW; END IF;
   v_account := COALESCE(NEW.payload->>'account', NEW.payload->>'source', NEW.payload->>'destination');
   IF NOT (COALESCE((NEW.payload->>'account' BETWEEN 'A1501' AND 'A2000'), FALSE) OR
           COALESCE((NEW.payload->>'source' BETWEEN 'A1501' AND 'A2000'), FALSE) OR
           COALESCE((NEW.payload->>'destination' BETWEEN 'A1501' AND 'A2000'), FALSE)) THEN
      RETURN NEW;
   END IF;
   UPDATE migration_cursor SET seq=seq+1 WHERE key='B' RETURNING seq INTO v_seq;
   IF v_seq IS NULL THEN RETURN NEW; END IF;
   INSERT INTO migration_outbox(seq,kind,request_id,payload)
      VALUES(v_seq,'request',NEW.request_id,
       jsonb_build_object('op_type',NEW.op_type,'payload',NEW.payload,'result',NEW.result));
 END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
'''

META_SCHEMA = '''
CREATE TABLE IF NOT EXISTS shard_ranges (
 first_number INTEGER PRIMARY KEY, last_number INTEGER NOT NULL,
 shard TEXT NOT NULL, epoch INTEGER NOT NULL
);
INSERT INTO shard_ranges(first_number,last_number,shard,epoch)
 VALUES(1,1000,'A',1),(1001,2000,'B',1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS map_epoch (id INTEGER PRIMARY KEY CHECK (id=1), epoch INTEGER NOT NULL);
INSERT INTO map_epoch(id,epoch) VALUES(1,1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS legacy_local_requests (
 request_id TEXT PRIMARY KEY, op_type TEXT NOT NULL, payload JSONB NOT NULL, result JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_job (
 job_id TEXT PRIMARY KEY, phase TEXT NOT NULL, cutover_requested BOOLEAN NOT NULL DEFAULT FALSE,
 snapshot_seq BIGINT NOT NULL DEFAULT 0, applied_seq BIGINT NOT NULL DEFAULT 0,
 epoch INTEGER NOT NULL DEFAULT 2, error TEXT, updated_at TIMESTAMPTZ DEFAULT now()
);
'''


def upgrade():
    for shard in (('C',) if os.getenv('ONLY_C') == '1' else ('A','B')):
        with psycopg.connect(DSNS[shard]) as con:
            con.execute(BANK_SCHEMA)
            if shard == 'A':
                zones = [('normal', 1, 1000, 1, True)]
            elif shard == 'B':
                zones = [('normal', 1001, 1500, 1, True), ('moving', 1501, 2000, 1, True)]
            else:
                zones = [('moving', 1501, 2000, 2, False)]
            for zone in zones:
                con.execute('INSERT INTO ownership(zone,first_number,last_number,epoch,writable) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', zone)
            if shard == 'B':
                con.execute("INSERT INTO migration_cursor(key,seq) VALUES('B',0) ON CONFLICT DO NOTHING")
                con.execute('DROP TRIGGER IF EXISTS migration_accounts_update ON accounts')
                con.execute('CREATE TRIGGER migration_accounts_update AFTER UPDATE OF balance ON accounts FOR EACH ROW EXECUTE FUNCTION capture_migration_change()')
                con.execute('DROP TRIGGER IF EXISTS migration_requests_update ON local_requests')
                con.execute('CREATE TRIGGER migration_requests_update AFTER UPDATE OF result ON local_requests FOR EACH ROW EXECUTE FUNCTION capture_migration_change()')
    if os.getenv('ONLY_C') != '1':
        with psycopg.connect(DSNS['META']) as con:
            con.execute(META_SCHEMA)
    print('BONUS schema upgrade complete; existing balances preserved', flush=True)


if __name__ == '__main__':
    upgrade()
