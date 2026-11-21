"""Add retry durability fences, preserving every account, GID and 2PC decision.

Run in an existing application container after configure(), before new app code.
No seeding, transaction completion, or balance changes are performed.
"""
from gateway.entrypoint_k8s import configure
import os
import psycopg
from gateway.db_runtime import connection_options, watch_sync_wait, assert_durable
from gateway.coordinator_pg import Coordinator


def migrate():
    configure()
    coord=Coordinator(None)
    with coord.connection() as con:
        con.execute('CREATE TABLE IF NOT EXISTS request_registry(request_id TEXT PRIMARY KEY,operation TEXT NOT NULL,payload JSONB NOT NULL)')
    for shard in ('A', 'B'):
        with psycopg.connect(os.environ[f'DB_{shard}_DSN'], options=connection_options()) as con:
            watch_sync_wait(con)
            con.execute('CREATE TABLE IF NOT EXISTS participant_fences(tx_id TEXT PRIMARY KEY)')
            requests=con.execute('SELECT request_id,op_type,payload FROM local_requests').fetchall()
        assert_durable(con)
        for request_id,operation,payload in requests:
            coord.reserve_request(request_id,operation,payload)
        print(shard, 'recovery schema ready')
    with coord.connection() as con:
        requests=con.execute('SELECT request_id,source_account,destination_account,amount FROM transactions').fetchall()
    for request_id,source,destination,amount in requests:
        coord.reserve_request(request_id,'transfer',{'source':source,'destination':destination,'amount':amount})


if __name__ == '__main__':
    migrate()
