"""Construct PostgreSQL DSNs without hard-coding Kubernetes Secrets in manifests."""
import os
import sys
from urllib.parse import quote


def configure():
    user=quote(os.environ['DB_USER'],safe='')
    pwd=quote(os.environ['DB_PASSWORD'],safe='')
    for shard in ('A','B','C','META'):
        host=os.getenv(f'DB_{shard}_HOST', 'bank-meta-rw' if shard=='META' else f'bank-{shard.lower()}-rw')
        os.environ[f'DB_{shard}_DSN']=f'postgresql://{user}:{pwd}@{host}:5432/bank?connect_timeout=4'
    os.environ['META_DSN']=os.environ['DB_META_DSN']
    if os.environ.get('SHARD_ID'):
        os.environ['DB_DSN']=os.environ[f'DB_{os.environ["SHARD_ID"]}_DSN']
    os.environ['COORD_BACKEND']='postgres'

if __name__=='__main__':
    configure()
    role=os.environ.get('ROLE','bank')
    if role=='bootstrap':
        from gateway.k8s_bootstrap import bootstrap
        bootstrap()
    elif role=='bootstrap-c':
        from gateway.k8s_bootstrap import SHARD_SCHEMA,get_conn
        with get_conn(os.environ['DB_C_DSN']) as con:
            for stmt in SHARD_SCHEMA.strip().split(';'):
                if stmt.strip(): con.execute(stmt)
        os.environ['ONLY_C']='1'
        from bonus.bootstrap import upgrade
        upgrade()
    elif role=='migrator':
        os.execvp('gunicorn',['gunicorn','--bind','0.0.0.0:8050',
            '--workers','1','--threads','4','--timeout','120', 'bonus.migrator:app'])
    elif role=='recovery':
        from gateway.recovery import Coordinator, Router
        import time
        coord=Coordinator(Router())
        while True:
            try: coord.recover_once()
            except Exception:
                import logging; logging.exception('recovery pending')
            time.sleep(3)
    else:
        target='gateway.app:app' if role=='gateway' else 'bank.app:app'
        os.execvp('gunicorn',['gunicorn','--bind','0.0.0.0:8000',
            '--workers','1','--threads','8','--timeout','55',
            '--graceful-timeout','45','--access-logfile','-',target])
