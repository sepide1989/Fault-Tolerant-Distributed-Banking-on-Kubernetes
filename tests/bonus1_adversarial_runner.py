"""Bonus I adversarial integration tests. Run on HOST with Python 3 + Docker Compose.

Concise audit edition: a few high-value checks, no individual-account dump.

Examples (PowerShell, in assignment2_banking):
    py -3 tests/bonus1_adversarial_runner.py --case all
    py -3 tests/bonus1_adversarial_runner.py --case fencing --keep

Each case creates an independent, disposable Docker Compose project with its own
volumes; NEVER runs `down -v` on the normal banking Compose project.
Requires configurable host ports in docker-compose.yml (included in this zip).
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, ProxyHandler
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
CASES = ('full', 'snapshot', 'replay', 'fencing', 'post-activation')


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Audit:
    def __init__(self, case, keep=False):
        self.case, self.keep = case, keep
        self.project = 'b1audit-' + uuid4().hex[:10]
        self.gateway_port, self.migrator_port = free_port(), free_port()
        while self.migrator_port == self.gateway_port:
            self.migrator_port = free_port()
        self.gateway = f'http://127.0.0.1:{self.gateway_port}'
        self.migrator = f'http://127.0.0.1:{self.migrator_port}'
        self.env = os.environ.copy()
        self.env.update(BANK_GATEWAY_PORT=str(self.gateway_port), MIGRATOR_PORT=str(self.migrator_port),
                        MIGRATION_SNAPSHOT_DELAY='1.2' if case in ('snapshot','replay','full') else '0',
                        MIGRATION_REPLAY_DELAY='12' if case=='replay' else '0',
                        MIGRATION_FENCE_DELAY='12' if case=='fencing' else '0',
                        MIGRATION_CLEANUP_DELAY='12' if case=='post-activation' else '0')
        self.events = []
        self._log_lock = threading.Lock()
        self.log_dir = ROOT / 'reports'
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_prefix = self.log_dir / ('bonus1_' + case + '_' +
                           datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + self.project)
        self.text_log = Path(str(self.log_prefix) + '.log')
        self.service_log = Path(str(self.log_prefix) + '_docker_services.log')
        # Docker Desktop sometimes does not forward dynamically selected host ports
        # to Windows even though `docker ps` reports the port mapping. Using an
        # in-network probe also avoids system HTTP proxies intercepting 127.0.0.1.
        self._docker_network_http = False
        self._direct_opener = build_opener(ProxyHandler({}))

    def note(self, message, **data):
        """Emit concise verification evidence to the console and a text report."""
        event = {'time': datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                 'case': self.case, 'message': message, **data}
        with self._log_lock:
            self.events.append(event)
            detail = ' ' + json.dumps(data, ensure_ascii=False, default=str) if data else ''
            line = f"[{event['time']}] [{self.case.upper()}] {message}{detail}"
            print(line, flush=True)
            with self.text_log.open('a', encoding='utf-8') as f:
                f.write(line + '\n')

    def balance_pair(self, src, dst):
        """Read two example balances; only used when no concurrent load is running."""
        return (self.get_balance(src), self.get_balance(dst))

    def sample_transfer(self, src, dst, amount):
        """Show just one representative before/after transfer per crash scenario."""
        before = self.balance_pair(src, dst)
        rid, result = self.request_transfer(src, dst, amount)
        after = self.balance_pair(src, dst)
        assert after == (before[0] - amount, before[1] + amount), (before, after)
        self.note('SAMPLE_TRANSFER_OK', source=src, destination=dst, amount=amount,
                  before=list(before), after=list(after))
        return rid, result

    def checkpoint(self, label):
        """Aggregate only: row counts and authoritative money, not every account.

        During B-to-C copying, duplicate physical rows are counted just once,
        according to the active shard map. Observational checks are not an atomic
        transaction spanning separate databases.
        """
        topo = self.api('GET', '/api/topology')['shard_map']
        ranges = topo['ranges']
        sums, counts, rows = 0, 0, {}
        for shard in ('A', 'B', 'C'):
            owned = [r for r in ranges if r['shard'] == shard]
            conditions = [f"account_id BETWEEN 'A{int(r['start']):04d}' AND 'A{int(r['end']):04d}'"
                          for r in owned]
            predicate = '(' + ' OR '.join(conditions) + ')' if conditions else 'FALSE'
            query = ("SELECT COUNT(*), COALESCE(SUM(balance),0), "
                     f"COUNT(*) FILTER (WHERE {predicate}), "
                     f"COALESCE(SUM(balance) FILTER (WHERE {predicate}),0) FROM accounts")
            physical_count, _physical_sum, logical_count, logical_sum = map(int, self.sql(shard.lower(), query).split('|'))
            rows[shard] = physical_count
            counts += logical_count
            sums += logical_sum
        result = {'epoch': topo['version'], 'logical_accounts': counts,
                  'logical_total': sums, 'rows': rows}
        self.note('BALANCE_AUDIT ' + label, **result)
        return result

    def status_log(self, label):
        status = self.state()
        self.note('MIGRATION_STATUS ' + label, phase=status.get('phase'),
                  applied_seq=status.get('applied_seq'))
        return status

    def compose(self, *args, timeout=300, check=True):
        cmd = ['docker','compose','-p',self.project,'--profile','bonus1',*args]
        if args and args[0] == 'up':
            # The original runner captures docker build output silently.
            # Emit a heartbeat so Windows users know Docker is still working.
            started = time.monotonic()
            proc = subprocess.Popen(cmd, cwd=ROOT, env=self.env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            while True:
                try:
                    stdout, stderr = proc.communicate(timeout=15)
                    break
                except subprocess.TimeoutExpired:
                    elapsed = round(time.monotonic() - started)
                    if elapsed >= 30 and elapsed % 30 < 15:
                        self.note('DOCKER_STARTUP_IN_PROGRESS', elapsed_seconds=elapsed)
                    if elapsed >= timeout:
                        proc.kill()
                        proc.communicate()
                        raise subprocess.TimeoutExpired(cmd, timeout)
            returncode = proc.returncode
        else:
            proc = subprocess.run(cmd, cwd=ROOT, env=self.env, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  timeout=timeout)
            stdout, stderr, returncode = proc.stdout, proc.stderr, proc.returncode
        if check and returncode:
            raise AssertionError(' '.join(cmd)+'\n'+stdout[-3000:]+'\n'+stderr[-3000:])
        return stdout.strip()

    def http(self, base, method='GET', path='/', body=None, timeout=15):
        """Reach local published port, or fall back to Docker-internal HTTP.

        The fallback runs inside the project's gateway container and reaches
        gateway:8000 or migrator:8050 over the isolated Compose network.
        It does not need any extra Python package on the Windows host.
        """
        if self._docker_network_http:
            return self.http_via_docker(base, method, path, body, timeout)

        headers={'Content-Type':'application/json'}
        req=Request(base+path, data=None if body is None else json.dumps(body).encode(),
                    headers=headers, method=method)
        try:
            with self._direct_opener.open(req,timeout=timeout) as response:
                raw=response.read()
                return response.status, json.loads(raw) if raw else {}
        except HTTPError as e:
            raw=e.read()
            try: data=json.loads(raw)
            except ValueError: data={'raw':raw.decode(errors='replace')}
            return e.code,data
        except (URLError, TimeoutError, ConnectionError) as host_error:
            # Only switch permanently if an actual request succeeds from inside
            # Docker. If both paths fail, keep the original failure visible.
            try:
                result = self.http_via_docker(base, method, path, body, timeout)
            except Exception as docker_error:
                raise URLError('Host path failed: '+str(host_error)
                                +'; Docker network path failed: '+str(docker_error)) from docker_error
            self._docker_network_http=True
            self.note('Windows localhost route inaccessible; using internal Docker network',
                      host_error=str(host_error))
            return result

    def http_via_docker(self, base, method='GET', path='/', body=None, timeout=15):
        service_port = 'gateway:8000' if base == self.gateway else 'migrator:8050'
        script = """import sys,json,urllib.request,urllib.error
p=json.load(sys.stdin)
req=urllib.request.Request('http://'+p['service']+p['path'],
  data=None if p['body'] is None else json.dumps(p['body']).encode(),
  headers={'Content-Type':'application/json'},method=p['method'])
try:
  with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=p['timeout']) as r:
    raw=r.read();code=r.status
except urllib.error.HTTPError as e:
  raw=e.read();code=e.code
except (urllib.error.URLError, OSError, TimeoutError) as e:
  # A deliberately terminated Gateway can close the socket mid-response.
  # Report a transport interruption, not a Python script failure.
  print(json.dumps({'transport_error':type(e).__name__+': '+str(e)}))
  sys.exit(0)
try:data=json.loads(raw) if raw else {}
except ValueError:data={'raw':raw.decode(errors='replace')}
print(json.dumps({'status':code,'data':data}))"""
        cmd=['docker','compose','-p',self.project,'--profile','bonus1',
             'exec','-T','gateway','python','-c',script]
        payload={'service':service_port,'method':method,'path':path,
                 'body':body,'timeout':timeout}
        result=subprocess.run(cmd,input=json.dumps(payload),cwd=ROOT,env=self.env,
                              text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                              timeout=timeout+20)
        if result.returncode:
            detail = result.stderr[-1500:]
            # docker compose exec may itself fail when the container exits
            # at the exact moment of the intentional Gateway crash.
            transient = ('is not running', 'not running', 'remote disconnected',
                         'remotedisconnected', 'connection refused',
                         'connection reset', 'container is restarting')
            if any(phrase in detail.lower() for phrase in transient):
                raise ConnectionError('Docker service temporarily unavailable: ' + detail)
            raise RuntimeError('Internal Docker HTTP probe failed: ' + detail)
        try:
            data=json.loads(result.stdout)
            if 'transport_error' in data:
                raise ConnectionError('Internal Docker HTTP interrupted: '
                                      +data['transport_error'])
            return data['status'],data['data']
        except (ValueError, KeyError) as e:
            raise RuntimeError('Unparseable internal HTTP response: '
                               +result.stdout[-1500:]) from e

    def api(self, method, path, body=None, codes=(200,)):
        status,data=self.http(self.gateway,method,path,body)
        assert status in codes, f'{method} {path}: HTTP {status}: {data}'
        return data

    def control(self, method, path, body=None, codes=(200,)):
        status,data=self.http(self.migrator,method,path,body)
        assert status in codes, f'{method} migration {path}: HTTP {status}: {data}'
        return data

    def state(self):
        status = self.control('GET','/status')
        self._last_migration_state = status
        return status

    def wait(self, condition, label, seconds=150, pause=.22):
        started=time.monotonic()
        limit=started+seconds
        last=None
        while time.monotonic()<limit:
            try:
                last=condition()
                if last:
                    return last
            except (URLError,TimeoutError,ConnectionError):
                pass
            time.sleep(pause)
        self.note('WAIT_TIMEOUT', target=label, last=repr(last))
        raise AssertionError('Timed out waiting for '+label+'; last='+repr(last))

    def phase(self, wanted, seconds=150):
        state = self.wait(lambda: (s if (s:=self.state())['phase']==wanted else None),
                          wanted, seconds)
        self.note('PHASE_REACHED', phase=wanted)
        return state

    def sql(self, shard, query):
        return self.compose('exec','-T','db-'+shard.lower(),'psql','-U','bank','-d','bank',
                            '-v','ON_ERROR_STOP=1','-At','-c',query,timeout=45).strip()

    def total(self, shards=('a','b')):
        return sum(int(self.sql(s,'SELECT COALESCE(SUM(balance),0) FROM accounts')) for s in shards)

    def moving_count(self, shard):
        return int(self.sql(shard,"SELECT COUNT(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'"))

    def compare_moving(self):
        code = '''import psycopg,json
with psycopg.connect('postgresql://bank:bankpass@db-b:5432/bank') as b, psycopg.connect('postgresql://bank:bankpass@db-c:5432/bank') as c:
    q="SELECT account_id,balance FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000' ORDER BY account_id"
    x=b.execute(q).fetchall();y=c.execute(q).fetchall()
    print(json.dumps({'equal':x==y,'b_count':len(x),'c_count':len(y)}))'''
        result=self.compose('exec','-T','gateway','python','-c',code,timeout=40)
        value=json.loads(result)
        self.note('COPY_REPLAY_MATCH', equal=value['equal'], b_rows=value['b_count'], c_rows=value['c_count'])
        assert value['equal'] and value['b_count']==value['c_count']==500, value
        return value

    def direct(self, host, path, body):
        """Call a replica directly from inside Gateway, bypassing public Gateway routing."""
        code='''import sys,json,requests
q=json.load(sys.stdin)
r=requests.post('http://'+q['host']+':8000'+q['path'],json=q['body'],timeout=10)
print(json.dumps({'http':r.status_code,'body':r.json()}))'''
        item={'host':host,'path':path,'body':body}
        cmd=['docker','compose','-p',self.project,'--profile','bonus1','exec','-T','gateway','python','-c',code]
        p=subprocess.run(cmd,input=json.dumps(item),cwd=ROOT,env=self.env,text=True,
                         stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25)
        assert p.returncode==0, p.stderr
        response=json.loads(p.stdout)
        return response

    def request_transfer(self, src, dst, amount=1, request_id=None):
        rid=request_id or 'b1-' + uuid4().hex
        payload={'source':src,'destination':dst,'amount':amount,'request_id':rid}
        for _ in range(60):
            try:
                status,data=self.http(self.gateway,'POST','/api/transfer',payload,timeout=20)
                if status==200:
                    return rid,data
                if status==409 and 'insufficient' in data.get('error','').lower():
                    raise AssertionError('Unexpected insufficient balance: '+repr(data))
                if status not in (202,409,503):
                    raise AssertionError(f'Transfer failed: HTTP {status} {data}')
            except (URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(.35)
        raise AssertionError('Transfer unresolved after retries: '+str(payload))

    def get_balance(self, acct):
        return int(self.api('GET','/api/accounts/'+acct)['balance'])

    def bootstrap(self):
        self.note('Launching independent test project',project=self.project,
                  gateway_port=self.gateway_port,migrator_port=self.migrator_port,
                  report=str(self.text_log))
        self.compose('up','-d','--build',timeout=600)
        self.wait(lambda: self.http(self.gateway,'GET','/health',timeout=4)[0]==200,'Gateway healthy',60,1)
        self.wait(lambda: self.http(self.migrator,'GET','/status',timeout=4)[0]==200,'Migrator healthy',60,1)
        self.note('SERVICES_READY', transport='Docker network' if self._docker_network_http else 'localhost')
        assert self.state()['phase']=='NOT_STARTED',self.state()
        assert self.moving_count('b')==500 and self.moving_count('c')==0
        self.start_total=self.total()
        assert self.start_total==1_999_500, f'Unexpected initial balance total {self.start_total}'
        topo=self.api('GET','/api/topology')['shard_map']
        assert topo['version']==1,topo
        baseline=self.checkpoint('initial_before_migration')
        assert baseline['logical_total']==self.start_total
        assert baseline['logical_accounts']==2000

    def start(self):
        response = self.control('POST','/start')
        self.note('MIGRATION_STARTED')

    def wait_catchup(self):
        return self.phase('CATCHUP',180)

    def exercise_online(self):
        # No bulk-copy freeze: transfers work WHILE snapshot transaction is open.
        self.start()
        self.phase('SNAPSHOT',30)
        # Check this particular transfer begins AND finishes while the bulk copy is ongoing.
        self.request_transfer('A1599','A1799',1)
        assert self.state()['phase']=='SNAPSHOT', 'Bulk copy finished before the live-write probe'
        self.note('LIVE_WRITE_DURING_SNAPSHOT_OK')
        tests=[(f'A{1600+i:04}',f'A{1700+i:04}',1) for i in range(8)]
        tests += [(f'A{1700+i:04}',f'A{1200+i:04}',1) for i in range(4)]
        with ThreadPoolExecutor(max_workers=4) as workers:
            outcomes=list(workers.map(lambda x:self.request_transfer(*x), tests))
        assert len(outcomes)==12
        self.note('CONCURRENT_WRITES_OK', count=len(outcomes))
        self.wait_catchup()
        # Test same-shard and cross-shard 2PC while C catches up (B still owner).
        before=self.total()
        tests=[(f'A{1650+i:04}',f'A{1750+i:04}',2) for i in range(4)]
        tests += [(f'A{1660+i:04}',f'A{500+i:04}',1) for i in range(4)]
        with ThreadPoolExecutor(max_workers=4) as workers:
            outcomes=list(workers.map(lambda x:self.request_transfer(*x),tests))
        assert len(outcomes)==8
        assert self.total()==before==self.start_total,'Money conserved during B->C migration'
        self.note('CATCHUP_TRANSFERS_OK', count=len(outcomes), money_conserved=True)
        self.status_log('during_catchup')
        rid='b1-legacy-'+uuid4().hex
        original=self.request_transfer('A1400','A1600',3,rid)
        self.legacy_request=rid
        self.legacy_balance=(self.get_balance('A1400'),self.get_balance('A1600'))
        duplicate=self.request_transfer('A1400','A1600',3,rid)
        assert duplicate[1].get('source_balance')==original[1].get('source_balance')
        assert self.legacy_balance==(self.get_balance('A1400'),self.get_balance('A1600'))
        cross_id,cross=self.request_transfer('A1675','A0500',1)
        query=self.api('GET','/api/requests/'+cross_id)
        assert query['status']=='COMMITTED' and query['decision']=='COMMIT',query
        assert self.api('GET','/api/transactions/'+query['transaction_id'])['status']=='COMMITTED'
        self.note('2PC_AND_DUPLICATE_REQUEST_OK', commit_status=query['status'], duplicate_charged=False)
        # Fault-injection: crash the Gateway AFTER fsynced global COMMIT decision
        # and after only ONE participant has acknowledged it.
        crash_id='b1-uncertain-'+uuid4().hex
        before_pair=(self.get_balance('A0450'),self.get_balance('A1655'))
        self.api('POST','/api/admin/arm-crash-after-commit',{})
        self.note('GATEWAY_CRASH_TEST_STARTED', amount=7, request_id=crash_id)
        try:
            self.http(self.gateway,'POST','/api/transfer',{
                'source':'A0450','destination':'A1655','amount':7,'request_id':crash_id},timeout=20)
        except (URLError, TimeoutError, ConnectionError) as exc:
            # An interrupted HTTP response is EXPECTED. It is NOT proof of
            # a successful transaction: the durable status is checked below.
            self.note('EXPECTED_GATEWAY_CONNECTION_INTERRUPTION',
                      error_type=type(exc).__name__)
        except RuntimeError as exc:
            # Docker exec may be torn down as the container stops before it
            # can relay its own network exception. Only tolerate a probe
            # transport failure in this deliberately crashing request.
            if not str(exc).startswith(('Internal Docker HTTP probe failed:',
                                        'Unparseable internal HTTP response:')):
                raise
            self.note('EXPECTED_GATEWAY_PROBE_INTERRUPTION',
                      error_type=type(exc).__name__)
        self.wait(lambda: not self.compose('ps','--status','running','-q','gateway',timeout=20),
                  'Gateway crash after durable COMMIT',45,.5)
        self.note('GATEWAY_CRASH_CONFIRMED', request_id=crash_id)
        # IMPORTANT: do not use `docker compose start gateway` here. Compose may
        # also restart the one-shot schema-upgrade dependency, which recreates
        # triggers and can block behind an unresolved prepared 2PC transaction.
        # The gateway must restart FIRST to recover the durable COMMIT and
        # release that participant's locks. All other bank services are already
        # healthy because this test only crashed Gateway.
        self.note('GATEWAY_RESTART_REQUESTED', method='docker start',
                  dependency_services='left_running')
        gateway_containers = subprocess.run(
            ['docker', 'ps', '-a', '-q',
             '--filter', 'label=com.docker.compose.project=' + self.project,
             '--filter', 'label=com.docker.compose.service=gateway'],
            cwd=ROOT, env=self.env, text=True, capture_output=True, timeout=20,
            check=True).stdout.strip().splitlines()
        assert len(gateway_containers) == 1, (
            f'Expected exactly one isolated Gateway container, found: {gateway_containers}')
        started_gateway = subprocess.run(
            ['docker', 'start', gateway_containers[0]],
            cwd=ROOT, env=self.env, text=True, capture_output=True, timeout=30)
        assert started_gateway.returncode == 0, (
            'Direct Gateway restart failed: ' + started_gateway.stderr[-1500:])
        self.wait(lambda: self.http(self.gateway,'GET','/health',timeout=4)[0]==200,
                  'Gateway restarted',60,1)
        self.note('GATEWAY_RESTARTED_HEALTHY')
        status=self.wait(lambda:(x if (x:=self.api('GET','/api/requests/'+crash_id,
                            codes=(200,202)))['status']=='COMMITTED' else None),
                         'durable COMMIT recovered',70,.6)
        after_pair=(self.get_balance('A0450'),self.get_balance('A1655'))
        assert after_pair==(before_pair[0]-7,before_pair[1]+7),(before_pair,after_pair)
        _,again=self.request_transfer('A0450','A1655',7,crash_id)
        assert again.get('status')=='COMMITTED',again
        assert (self.get_balance('A0450'),self.get_balance('A1655'))==after_pair
        assert self.total()==self.start_total
        self.note('2PC_CRASH_RECOVERY_OK', amount=7, before=list(before_pair), after=list(after_pair),
                  committed_once=True)
        block=self.direct('bank-c1','/internal/deposit',{'account':'A1600','amount':1,'request_id':'b1-early-'+uuid4().hex,'epoch':2})
        assert block['http']==409 and 'STALE_ROUTE' in block['body'].get('error',''),block
        self.note('C_PRE_ACTIVATION_WRITE_REJECTED')
        self.wait(lambda: self.state()['applied_seq']>=int(self.sql('b',"SELECT seq FROM migration_cursor WHERE key='B'")),
                  'durable outbox caught up',60,.5)
        self.compare_moving()
        cnt=int(self.sql('b','SELECT COUNT(*) FROM migration_outbox'))
        applied=int(self.sql('c','SELECT COUNT(*) FROM migration_applied'))
        assert cnt>0 and applied>0,(cnt,applied)
        self.note('OUTBOX_REPLAY_OK', outbox_count=cnt, applied_count=applied)
        self.checkpoint('after_online_copy_and_delta_replay')

    def exercise_prepared_fencing(self):
        tx=uuid4().hex
        payload={'tx_id':tx,'modifications':[{'account':'A1688','delta':-1}],'epoch':1}
        response=self.direct('bank-b1','/internal/2pc/prepare',payload)
        assert response['http']==200 and response['body'].get('vote')=='YES',response
        self.note('2PC_PREPARE_LOCK_HELD')
        self.control('POST','/cutover')
        self.phase('FENCING',30)
        time.sleep(4)  # Fence attempts time out; they must not bypass the prepared lock.
        state=self.state()
        assert state['phase']=='FENCING',state
        assert self.sql('b',"SELECT writable::text FROM ownership WHERE zone='moving'")=='true'
        self.note('FENCING_BLOCKED_UNTIL_PREPARE_RESOLVED')
        response=self.direct('bank-b2','/internal/2pc/abort',{'tx_id':tx})
        assert response['http']==200 and response['body'].get('outcome')=='ABORTED',response
        self.note('PREPARE_ABORTED_CUTOVER_CAN_RESUME')

    def verify_final(self):
        self.phase('COMPLETED',180)
        topo=self.api('GET','/api/topology')['shard_map']
        assert topo['version']==2 and any(r['shard']=='C' and r['start']==1501 and r['end']==2000 for r in topo['ranges']),topo
        assert self.api('GET','/api/accounts/A1600')['shard']=='C'
        assert self.api('GET','/api/accounts/A1200')['shard']=='B'
        assert self.moving_count('b')==0 and self.moving_count('c')==500
        assert self.sql('b',"SELECT writable::text||':'||epoch FROM ownership WHERE zone='moving'")=='false:2'
        assert self.sql('c',"SELECT writable::text||':'||epoch FROM ownership WHERE zone='moving'")=='true:2'
        for name in ('bank-b1','bank-b2'):
            for epoch in (1,2):
                rejected=self.direct(name,'/internal/deposit',{
                    'account':'A1600','amount':1,'request_id':'b1-stale-'+uuid4().hex,'epoch':epoch})
                assert rejected['http']==409 and 'STALE_ROUTE' in rejected['body'].get('error',''),rejected
        rejected=self.direct('bank-c1','/internal/deposit',{
            'account':'A1600','amount':1,'request_id':'b1-c-old-'+uuid4().hex,'epoch':1})
        assert rejected['http']==409 and 'STALE_ROUTE' in rejected['body'].get('error',''),rejected
        before_newowner = self.balance_pair('A1601', 'A1602')
        newowner=self.direct('bank-c2','/internal/transfer/local',{
            'source':'A1601','destination':'A1602','amount':1,'request_id':'b1-c-new-'+uuid4().hex,'epoch':2})
        assert newowner['http']==200,newowner
        after_newowner = self.balance_pair('A1601', 'A1602')
        assert after_newowner == (before_newowner[0]-1, before_newowner[1]+1), (before_newowner,after_newowner)
        self.note('C_NEW_OWNER_TRANSFER_OK', amount=1, before=list(before_newowner), after=list(after_newowner))
        assert self.total(('a','b','c'))==self.start_total,'Money lost/created after migration'
        self.note('CUTOVER_VERIFIED', epoch=2, b_moving_accounts=0, c_moving_accounts=500,
                  stale_writes_rejected=True, total=self.start_total)
        final=self.checkpoint('after_completed_cutover')
        assert final['logical_total']==self.start_total, final
        assert final['logical_accounts']==2000, final

    def run_full(self):
        self.exercise_online()
        self.exercise_prepared_fencing()
        self.verify_final()
        # Exactly once even if a same-shard B request is retried after B/C split.
        original=self.legacy_balance
        _,result=self.request_transfer('A1400','A1600',3,self.legacy_request)
        assert result.get('already_committed') is True,result
        assert (self.get_balance('A1400'),self.get_balance('A1600'))==original
        assert self.total(('a','b','c'))==self.start_total
        self.note('CROSS_SHARD_RETRY_EXACTLY_ONCE_OK')
        self.checkpoint('after_full_retry_checks')

    def run_crash(self):
        self.start()
        if self.case=='snapshot':
            self.phase('SNAPSHOT',45)
            # Force a live write during the snapshot to test restart reconciliation.
            self.sample_transfer('A1600','A1700',2)
            self.phase('SNAPSHOT',15)
        elif self.case=='replay':
            self.phase('SNAPSHOT',45)
            self.sample_transfer('A1603','A1703',3)
            self.wait_catchup()
            # DELTA was committed on C; checkpoint is intentionally delayed.
            self.wait(lambda: int(self.sql('c','SELECT COUNT(*) FROM migration_applied'))>0 and
                self.state()['applied_seq']<int(self.sql('b',"SELECT seq FROM migration_cursor WHERE key='B'")),
                'C applied replay while metadata checkpoint is behind',45,.5)
            self.note('REPLAY_CRASH_WINDOW_CONFIRMED')
        elif self.case in ('fencing','post-activation'):
            self.wait_catchup()
            self.sample_transfer('A1603','A1703',3)
            self.control('POST','/cutover')
            self.phase('FENCING' if self.case=='fencing' else 'CLEANUP',90)
            if self.case=='post-activation':
                assert self.api('GET','/api/topology')['shard_map']['version']==2
                assert self.moving_count('b')==500
                self.note('MAP_ACTIVATED_CLEANUP_PENDING', epoch=2, old_b_rows=500)
        self.status_log('immediately_before_SIGKILL')
        self.compose('kill','-s','SIGKILL','migrator',timeout=25)
        self.note('MIGRATOR_KILLED', phase=self.case)
        # Record aggregate proof without querying the intentionally stopped Migrator.
        self.checkpoint('after_SIGKILL_before_restart')
        self.compose('start','migrator',timeout=40)
        self.wait(lambda:self.http(self.migrator,'GET','/status')[0]==200,'Migrator restarted',50)
        self.status_log('after_restart')
        if self.case in ('snapshot','replay'):
            self.wait_catchup()
            self.wait(lambda: self.state()['applied_seq']>=int(self.sql('b',"SELECT seq FROM migration_cursor WHERE key='B'")),
                      'catch-up after SIGKILL',90,.6)
            self.compare_moving()
            self.checkpoint('after_recovery_catchup_before_cutover')
            self.control('POST','/cutover')
        self.verify_final()
        self.note('CRASH_RECOVERY_VERIFIED', scenario=self.case)

    def run(self):
        passed=False; error=None
        self.note('TEST_BEGIN', scenario=self.case)
        try:
            self.bootstrap()
            self.run_full() if self.case=='full' else self.run_crash()
            passed=True
            self.note('TEST_PASS', scenario=self.case)
        except Exception as exc:
            error=f'{type(exc).__name__}: {exc}'
            self.note('TEST_FAIL', error=error)
        finally:
            # Capture deep diagnostics only if the test failed.
            if not passed:
                try:
                    logs = self.compose('logs', '--no-color', '--timestamps', '--tail=150',
                                        check=False, timeout=45)
                    self.service_log.write_text(logs + '\n', encoding='utf-8')
                    self.note('FAILURE_DIAGNOSTICS_SAVED', path=str(self.service_log))
                except Exception as exc:
                    self.note('FAILURE_DIAGNOSTICS_UNAVAILABLE', error=str(exc))
            if not self.keep:
                try:
                    self.compose('down','-v','--remove-orphans',timeout=120,check=True)
                    self.note('ISOLATED_ENVIRONMENT_CLEANED')
                except Exception as exc:
                    self.note('CLEANUP_FAILED', error=str(exc))
            else:
                self.note('ISOLATED_ENVIRONMENT_PRESERVED', project=self.project)
        return {'case':self.case,'pass':passed,'error':error,'project':self.project,
                'text_log':str(self.text_log), 'events':self.events}



def main():
    p=argparse.ArgumentParser(description='Five isolated Bonus I tests with concise verification logs')
    p.add_argument('--case',choices=('all',*CASES),default='all')
    p.add_argument('--keep',action='store_true',help='Preserve isolated test Docker volumes/containers for inspection')
    args=p.parse_args()
    if not (ROOT/'docker-compose.yml').is_file():
        p.error('Run from extracted project tree')
    selected=CASES if args.case=='all' else (args.case,)
    try:
        subprocess.run(['docker','compose','version'],check=True,stdout=subprocess.DEVNULL)
        subprocess.run(['docker','info'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    except (OSError,subprocess.CalledProcessError):
        p.error('Docker Engine + docker compose must be installed and running')
    results=[]
    for case in selected:
        print('\n=== BONUS I ADVERSARIAL CASE:',case,'===',flush=True)
        results.append(Audit(case,keep=args.keep).run())
    outfile=ROOT/'reports'/('bonus1_adversarial_'+datetime.now().strftime('%Y%m%d_%H%M%S')+'.json')
    outfile.parent.mkdir(parents=True,exist_ok=True)
    outfile.write_text(json.dumps({'results':results},indent=2,ensure_ascii=False),encoding='utf-8')
    for result in results:
        print(('PASS' if result['pass'] else 'FAIL'),result['case'],flush=True)
    print('Evidence report:',outfile,flush=True)
    print('Concise .log reports are in:', ROOT/'reports',flush=True)
    return 0 if all(r['pass'] for r in results) else 1


if __name__=='__main__':
    sys.exit(main())
