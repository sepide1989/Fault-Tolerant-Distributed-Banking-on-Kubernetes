"""One-time, idempotent schema/seed initialization for CNPG clusters.
Never resets accounts or writes over existing balances.
"""
import os
import time
import psycopg


SHARD_SCHEMA = '''
CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY,
  balance BIGINT NOT NULL CHECK (balance >= 0)
);
CREATE TABLE IF NOT EXISTS local_requests (
  request_id TEXT PRIMARY KEY,
  op_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  result JSONB
);
CREATE TABLE IF NOT EXISTS participant_results (
  tx_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('COMMITTED','ABORTED'))
);
CREATE TABLE IF NOT EXISTS participant_fences (tx_id TEXT PRIMARY KEY);
'''
META_COORD_SCHEMA = '''
CREATE TABLE IF NOT EXISTS request_registry (
 request_id TEXT PRIMARY KEY, operation TEXT NOT NULL, payload JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS transactions (
 tx_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
 source_account TEXT NOT NULL, destination_account TEXT NOT NULL,
 amount BIGINT NOT NULL, source_shard TEXT NOT NULL,
 destination_shard TEXT NOT NULL, phase TEXT NOT NULL,
 decision TEXT CHECK (decision IN ('COMMIT','ABORT')),
 error TEXT, created_at DOUBLE PRECISION NOT NULL,
 updated_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS votes (
 tx_id TEXT NOT NULL, shard TEXT NOT NULL,
 vote TEXT NOT NULL, PRIMARY KEY (tx_id,shard)
);
CREATE TABLE IF NOT EXISTS acknowledgements (
 tx_id TEXT NOT NULL, shard TEXT NOT NULL, decision TEXT NOT NULL,
 PRIMARY KEY (tx_id,shard)
);
'''

def get_conn(dsn):
    for attempt in range(80):
        try:
            return psycopg.connect(dsn, connect_timeout=3)
        except psycopg.OperationalError:
            print(f'waiting for CNPG primary, attempt={attempt+1}', flush=True)
            time.sleep(3)
    raise RuntimeError('could not contact database after 240s')

def bootstrap():
    for shard, low, high in [('A',1,1000),('B',1001,2000)]:
        with get_conn(os.environ[f'DB_{shard}_DSN']) as con:
            for statement in SHARD_SCHEMA.strip().split(';'):
                if statement.strip():
                    con.execute(statement)
            con.execute('''INSERT INTO accounts(account_id,balance)
             SELECT 'A'||lpad(n::text,4,'0'), CASE WHEN n=1500 THEN 500 ELSE 1000 END
             FROM generate_series(%s::integer,%s::integer) n ON CONFLICT DO NOTHING''', (low,high))
    with get_conn(os.environ['META_DSN']) as con:
        for statement in META_COORD_SCHEMA.strip().split(';'):
            if statement.strip():
                con.execute(statement)
    # Preserve the old Bonus 1 migration tables, epochs and ownership.
    from bonus.bootstrap import upgrade
    upgrade()
    print('Kubernetes schema initialized; existing balances untouched', flush=True)

if __name__ == '__main__':
    bootstrap()
