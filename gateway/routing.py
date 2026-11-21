"""Dynamic PostgreSQL Shard Map with account ownership epochs and health-aware replicas."""
import os
import re
import threading
import psycopg
import requests
from gateway.db_runtime import connection_options


class RouteError(Exception):
    pass


class ReplicaUnavailable(Exception):
    pass


class Router:
    def __init__(self):
        self.meta_dsn = os.getenv('META_DSN', 'postgresql://bank:bankpass@meta-db:5432/bank')
        self.replicas = {
            sh: os.getenv('REPLICAS_'+sh, f'http://bank-{sh.lower()}1:8000,http://bank-{sh.lower()}2:8000').split(',')
            for sh in ('A','B','C')
        }
        self.index = {s: 0 for s in self.replicas}
        self.mutex = threading.Lock()
        self.http = requests.Session()
        # Cluster-local HTTP must not follow a desktop/system HTTP proxy.
        self.http.trust_env = False

    def locate(self, account):
        if not isinstance(account, str) or not re.fullmatch(r'A\d{4}', account):
            raise RouteError('account must have format A0001..A2000')
        number = int(account[1:])
        with psycopg.connect(self.meta_dsn, connect_timeout=3, options=connection_options()) as con:
            row = con.execute('SELECT shard,epoch FROM shard_ranges WHERE %s BETWEEN first_number AND last_number', (number,)).fetchone()
        if not row:
            raise RouteError('account is not in the Shard Map')
        return row[0], row[1]

    def account_shard(self, account):
        return self.locate(account)[0]

    def _round_robin_order(self, shard):
        servers = self.replicas[shard]
        with self.mutex:
            start = self.index[shard] % len(servers)
            self.index[shard] += 1
        return servers[start:] + servers[:start]

    def request(self, shard, method, path, data=None):
        failures=[]
        for server in self._round_robin_order(shard):
            try:
                health=self.http.get(server+'/health',timeout=1.5)
                if health.status_code != 200:
                    failures.append(server+': unhealthy')
                    continue
                response=self.http.request(method,server+path,json=data,timeout=(2, 11))
                # Retrying on another replica is safe for idempotent local requests and 2PC.
                if response.status_code >= 500:
                    failures.append(f'{server}: HTTP {response.status_code}')
                    continue
                try:
                    return response.status_code,response.json()
                except ValueError:
                    failures.append(server+': invalid JSON')
            except requests.RequestException as exc:
                failures.append(f'{server}: {exc}')
        raise ReplicaUnavailable('No healthy replica for Shard '+shard+' ('+'; '.join(failures)+')')

    def legacy_request(self,request_id):
        with psycopg.connect(self.meta_dsn,connect_timeout=3,options=connection_options()) as con:
            row=con.execute('SELECT op_type,payload,result FROM legacy_local_requests WHERE request_id=%s',(request_id,)).fetchone()
        return None if row is None else {'op_type':row[0],'payload':row[1],'result':row[2]}

    def register_replica(self,shard,url):
        if shard not in self.replicas or not re.fullmatch(r'http://[A-Za-z0-9-]+:8000',url):
            raise RouteError('invalid replica registration')
        with self.mutex:
            if url not in self.replicas[shard]:
                self.replicas[shard].append(url)
        return {'shard':shard,'url':url,'registered':True}

    def topology(self):
        with psycopg.connect(self.meta_dsn, connect_timeout=3,options=connection_options()) as con:
            rows=con.execute('SELECT first_number,last_number,shard,epoch FROM shard_ranges ORDER BY first_number').fetchall()
            version=con.execute('SELECT epoch FROM map_epoch WHERE id=1').fetchone()[0]
        return {'shard_map':{'strategy':'account_numeric_range','version':version,'ranges':[
            {'start':r[0],'end':r[1],'shard':r[2],'epoch':r[3]} for r in rows
        ]},'replicas':self.replicas,'selection': os.getenv('ROUTING_MODE','round_robin_with_health_checks')}
