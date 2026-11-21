"""Bonus II adversarial tests (HOST PowerShell, Python 3 + Docker CLI).

Cases:
  --case controller-crash   Kill controller and B1; verify one replacement after controller restarts.
  --case transfers          Kill A1 during concurrent local and cross-shard transfers.
  --case all                Run both independent checks in sequence.

Tests the existing non-production Docker Compose project. Does NOT reset databases,
remove volumes, or edit banking code. Emits concise .log/.json evidence to reports/bonus2.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import os
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, ProxyHandler
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT = 'distributed-banking-assignment2'
GATEWAY_PORT = 8000
CASES = ('controller-crash', 'transfers')


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


class Suite:
    def __init__(self, case, project):
        self.case = case
        self.project = project
        self.started = datetime.now(timezone.utc)
        self.runid = uuid4().hex[:9]
        self.container = lambda service: f'{project}-{service}-1'
        self.reports = ROOT / 'reports' / 'bonus2'
        self.reports.mkdir(parents=True, exist_ok=True)
        stem = f'bonus2_{case}_{self.started.strftime("%Y%m%d_%H%M%S")}_{self.runid}'
        self.text_path = self.reports / (stem + '.log')
        self.json_path = self.reports / (stem + '.json')
        self.events = []
        self.results = {}
        self.opener = build_opener(ProxyHandler({}))
        self.use_docker_http = False
        self.http_lock = threading.Lock()
        self.gateway_url = None
        self.started_services = []

    def log(self, event, **data):
        item = {'time_utc': timestamp(), 'event': event, **data}
        self.events.append(item)
        detail = ' ' + json.dumps(data, sort_keys=True, ensure_ascii=False, default=str) if data else ''
        line = f"[{item['time_utc']}] [{self.case.upper()}] {event}{detail}"
        print(line, flush=True)
        with self.text_path.open('a', encoding='utf-8') as f:
            f.write(line + '\n')

    def cli(self, *args, timeout=45, check=True, input_data=None):
        p = subprocess.run(['docker', *args], cwd=ROOT, input=input_data,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True, timeout=timeout)
        if check and p.returncode != 0:
            raise RuntimeError(f'docker {" ".join(args[:4])} failed: {p.stderr[-1300:]}')
        return p.stdout.strip()

    def inspect(self, service):
        raw = self.cli('inspect', self.container(service), timeout=20)
        obj = json.loads(raw)
        assert len(obj) == 1, (service, len(obj))
        return obj[0]

    def ready(self, service):
        try:
            d = self.inspect(service)
        except RuntimeError:
            return False
        s = d['State']
        return s['Running'] and s.get('Health', {}).get('Status') == 'healthy'

    def wait(self, predicate, name, timeout=90, interval=.5):
        until = time.monotonic() + timeout
        error = None
        while time.monotonic() < until:
            try:
                result = predicate()
                if result:
                    return result
            except (RuntimeError, OSError, ValueError) as exc:
                error = exc
            time.sleep(interval)
        raise AssertionError(f'Timeout waiting for {name}; last error={error}')

    def metadata(self):
        return {svc: self.inspect(svc)['Id'][:12] for svc in
                ('bank-a1', 'bank-a2', 'bank-b1', 'bank-b2', 'self-healer')}

    def slot_count(self, service):
        ids = self.cli('ps', '-a', '-q',
                       '--filter', f'label=com.docker.compose.project={self.project}',
                       '--filter', f'label=com.docker.compose.service={service}')
        return len([x for x in ids.splitlines() if x.strip()])

    def journal(self):
        code = ('import sqlite3,json,os; p=os.getenv("HEAL_STATE","/healer-state/events.sqlite3"); '
                'c=sqlite3.connect(p); '
                'print(json.dumps(c.execute("SELECT id,slot,event,created_at FROM events ORDER BY id").fetchall()))')
        result = self.cli('exec', self.container('self-healer'), 'python', '-c', code)
        return json.loads(result)

    def init_gateway(self):
        gateway = self.inspect('gateway')
        ports = gateway.get('NetworkSettings', {}).get('Ports', {}).get('8000/tcp') or []
        external_port = next((x.get('HostPort') for x in ports if x.get('HostPort')), None)
        self.gateway_url = f'http://127.0.0.1:{external_port or GATEWAY_PORT}'
        try:
            status, _ = self._http_host('GET', '/health', None, timeout=3)
            if status != 200:
                raise ValueError(status)
            transport = 'Windows localhost'
        except (URLError, TimeoutError, OSError, ValueError):
            self.use_docker_http = True
            status, _ = self._http_docker('GET', '/health', None, timeout=5)
            assert status == 200, status
            transport = 'internal Docker network'
        self.log('SERVICES_READY', http_transport=transport)

    def _http_host(self, method, path, body, timeout=7):
        payload = None if body is None else json.dumps(body).encode('utf-8')
        req = Request(self.gateway_url + path, data=payload, method=method,
                      headers={'Content-Type': 'application/json'})
        try:
            with self.opener.open(req, timeout=timeout) as resp:
                data = resp.read()
                return resp.status, json.loads(data) if data else {}
        except HTTPError as ex:
            raw = ex.read()
            return ex.code, json.loads(raw) if raw else {}

    def _http_docker(self, method, path, body, timeout=8):
        code = '''import json,sys,urllib.request,urllib.error
p=json.load(sys.stdin)
r=urllib.request.Request("http://gateway:8000"+p["path"],data=None if p["body"] is None else json.dumps(p["body"]).encode(),headers={"Content-Type":"application/json"},method=p["method"])
try:
 with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(r,timeout=p["timeout"]) as x:
  raw=x.read();status=x.status
except urllib.error.HTTPError as x:
 raw=x.read();status=x.code
print(json.dumps({"status":status,"body":json.loads(raw) if raw else {}}))'''
        result = self.cli('exec', '-i', self.container('gateway'), 'python', '-c', code,
                          timeout=timeout+20,
                          input_data=json.dumps({'method':method, 'path':path,'body':body,'timeout':timeout}))
        item = json.loads(result)
        return item['status'], item['body']

    def http(self, method, path, body=None, timeout=8):
        with self.http_lock:
            docker_http = self.use_docker_http
        if docker_http:
            return self._http_docker(method, path, body, timeout)
        try:
            return self._http_host(method, path, body, timeout)
        except (URLError, TimeoutError, ConnectionError, OSError):
            with self.http_lock:
                self.use_docker_http = True
            return self._http_docker(method, path, body, timeout)

    def account(self, account):
        status, body = self.http('GET', '/api/accounts/' + account)
        assert status == 200 and body.get('account') == account, (status, body)
        return body

    def grand_total(self):
        total = 0
        for shard in ('a', 'b'):
            result = self.cli('exec', self.container('db-'+shard),
                              'psql', '-U','bank','-d','bank','-At','-v','ON_ERROR_STOP=1',
                              '-c','SELECT COALESCE(SUM(balance),0) FROM accounts', timeout=30)
            total += int(result)
        return total

    def check_replicas(self):
        return {svc: {'id': self.inspect(svc)['Id'][:12], 'healthy':self.ready(svc),
                      'slot_containers': self.slot_count(svc)} for svc in
                ('bank-a1','bank-a2','bank-b1','bank-b2')}

    def basic_setup(self):
        self.cli('info', timeout=20)
        for svc in ('bank-a1','bank-a2','bank-b1','bank-b2'):
            assert self.ready(svc), f'{svc} is not healthy; bring up --profile bonus2 first'
        healer = self.inspect('self-healer')
        assert healer['State']['Running'], 'self-healer is not running'
        for svc in ('self-healer','bank-a1','bank-a2','bank-b1','bank-b2'):
            policy = self.inspect(svc)['HostConfig']['RestartPolicy']['Name']
            assert policy in ('no',''), f'Forbidden auto-restart on {svc}: {policy}'
        self.init_gateway()
        self.log('BASELINE_OK', replicas=self.check_replicas(),
                 docker_auto_restart_disabled=True)

    def controller_crash(self):
        self.basic_setup()
        baseline = self.metadata()
        cursor = self.journal()[-1][0] if self.journal() else 0
        self.log('CONTROLLER_KILL_REQUESTED', controller_id=baseline['self-healer'])
        self.cli('kill','--signal','SIGKILL',self.container('self-healer'),timeout=20)
        self.wait(lambda: not self.inspect('self-healer')['State']['Running'], 'controller stopped', 20)
        assert all(self.ready(s) for s in ('bank-a1','bank-a2','bank-b1','bank-b2'))
        self.log('CONTROLLER_STOPPED', healthy_replicas_untouched=True)
        self.cli('kill','--signal','SIGKILL',self.container('bank-b1'),timeout=20)
        time.sleep(4)
        b1 = self.inspect('bank-b1')
        assert b1['Id'][:12] == baseline['bank-b1'] and not b1['State']['Running'], \
            'B1 replaced while controller was deliberately offline (unexpected)'
        self.log('B1_KILLED_CONTROLLER_OFFLINE', b1_id=baseline['bank-b1'],
                 replacement_while_controller_offline=False)
        self.log('CONTROLLER_RESTART_REQUESTED', method='docker start (test harness only)')
        self.cli('start',self.container('self-healer'),timeout=30)
        self.started_services.append('self-healer')
        self.wait(lambda: self.inspect('self-healer')['State']['Running'], 'controller running', 30)
        recovered = self.wait(lambda: self.ready('bank-b1') and
                              self.inspect('bank-b1')['Id'][:12] != baseline['bank-b1'],
                              'exactly one healthy new B1',90)
        new_id = self.inspect('bank-b1')['Id'][:12]
        assert new_id != baseline['bank-b1']
        assert self.inspect('bank-b2')['Id'][:12] == baseline['bank-b2'] and self.ready('bank-b2')
        assert self.inspect('bank-a1')['Id'][:12] == baseline['bank-a1'] and self.ready('bank-a1')
        assert self.inspect('bank-a2')['Id'][:12] == baseline['bank-a2'] and self.ready('bank-a2')
        actions = [r[2] for r in self.journal() if r[0] > cursor and r[1] == 'bank-b1']
        assert actions.count('failed_removed') == 1 and actions.count('replacement_created') == 1, actions
        self.log('B1_REPLACED_BY_RECOVERED_CONTROLLER', before=baseline['bank-b1'], after=new_id,
                 controller_journal=actions, neighbors_unchanged=True)
        # A second controller restart while all four replicas are healthy must be idempotent.
        self.cli('kill','--signal','SIGKILL',self.container('self-healer'),timeout=20)
        self.cli('start',self.container('self-healer'),timeout=30)
        self.wait(lambda: self.inspect('self-healer')['State']['Running'], 'second controller restart',30)
        time.sleep(5)
        assert self.inspect('bank-b1')['Id'][:12] == new_id
        final = self.check_replicas()
        assert all(x['healthy'] and x['slot_containers']==1 for x in final.values()), final
        actions_after = [r[2] for r in self.journal() if r[0] > cursor and r[1] == 'bank-b1']
        assert actions_after.count('replacement_created') == 1, actions_after
        self.log('NO_DUPLICATES_AFTER_SECOND_CONTROLLER_RESTART', replicas=final,
                 b1_replacement_events=actions_after.count('replacement_created'))
        hit_counts=Counter()
        for _ in range(12):
            body = self.account('A1005')
            hit_counts[body.get('replica','UNKNOWN')] += 1
            if hit_counts['B1'] > 0 and hit_counts['B2'] > 0:
                break
        assert hit_counts['B1'] > 0 and hit_counts['B2'] > 0, dict(hit_counts)
        self.log('GATEWAY_ROUTING_VERIFIED', responses=dict(hit_counts))

    def transfer_one(self, op, kickoff):
        # The tasks are intentionally spread across the outage window.
        index, source, destination, amount, request_id, scheduled = op
        until = time.monotonic()+75
        delay = kickoff + scheduled - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        begin = time.monotonic()
        errors = Counter()
        while time.monotonic() < until:
            try:
                status, data = self.http('POST','/api/transfer',
                                         {'source':source,'destination':destination,
                                          'amount':amount,'request_id':request_id},timeout=12)
                if status == 200:
                    if data.get('status') not in (None, 'COMMITTED') or data.get('ok') is False:
                        raise AssertionError(f'Unexpected success payload: {data}')
                    return {'index':index,'outcome':'COMMITTED','replica':data.get('replica'),
                            'started':begin,'finished':time.monotonic(),
                            'errors_before_success':dict(errors)}
                if status == 409 and data.get('status')=='ABORTED':
                    return {'index':index,'outcome':'ABORTED','replica':None,
                            'started':begin,'finished':time.monotonic(),
                            'errors_before_success':dict(errors)}
                if status == 202:
                    # Do not issue a NEW request ID for a pending 2PC outcome.
                    check_status, check_data = self.http('GET','/api/requests/'+request_id,timeout=10)
                    if check_status == 200 and check_data.get('status') in ('COMMITTED','ABORTED'):
                        return {'index':index,'outcome':check_data['status'], 'replica':None,
                                'started':begin,'finished':time.monotonic(),
                                'errors_before_success':dict(errors)}
                    errors['PENDING'] += 1
                elif status in (500,502,503,504):
                    errors[f'HTTP_{status}'] += 1
                else:
                    raise AssertionError(f'Unexpected transfer outcome {status}: {data}')
            except (URLError, TimeoutError, ConnectionError, OSError, RuntimeError) as exc:
                errors[type(exc).__name__] += 1
            time.sleep(.4)
        raise AssertionError(f'Request still unresolved after retries: {request_id}; {dict(errors)}')

    def transfer_failure(self):
        self.basic_setup()
        before_total = self.grand_total()
        orig = self.metadata()
        requests=[]
        for i in range(16):
            # Same-shard transfer; no prepared transaction needed.
            requests.append((i, f'A{120+i:04}', f'A{220+i:04}', 2,
                             f'b2same-{self.runid}-{i}',i*.28))
        for i in range(16):
            # Cross-shard transfer; 2PC required.
            requests.append((16+i, f'A{320+i:04}', f'A{1320+i:04}', 1,
                             f'b2cross-{self.runid}-{i}',i*.28 + .18))
        accounts = sorted({x[1] for x in requests} | {x[2] for x in requests})
        before = {a:self.account(a)['balance'] for a in accounts}
        self.log('TRANSACTION_BASELINE', transfer_requests=len(requests), same_shard=16,
                 cross_shard_2pc=16, logical_money_before=before_total,
                 sampled_accounts=len(accounts))
        kickoff = time.monotonic()+.3
        completed=[]
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = [pool.submit(self.transfer_one, x, kickoff) for x in requests]
            time.sleep(1.0)
            self.log('A1_KILL_DURING_CONCURRENT_TRANSFERS', previous_id=orig['bank-a1'])
            self.cli('kill','--signal','SIGKILL',self.container('bank-a1'),timeout=20)
            killed = time.monotonic()
            new = self.wait(lambda: self.ready('bank-a1') and
                            self.inspect('bank-a1')['Id'][:12] != orig['bank-a1'],
                            'new A1 healthy',90)
            replacement_ready = time.monotonic()
            for future in as_completed(futures, timeout=110):
                completed.append(future.result())
        new_id = self.inspect('bank-a1')['Id'][:12]
        assert new_id != orig['bank-a1']
        assert self.inspect('bank-a2')['Id'][:12] == orig['bank-a2'] and self.ready('bank-a2')
        assert self.inspect('bank-b1')['Id'][:12] == orig['bank-b1'] and self.ready('bank-b1')
        assert self.inspect('bank-b2')['Id'][:12] == orig['bank-b2'] and self.ready('bank-b2')
        outcomes = {x['index']:x for x in completed}
        assert len(outcomes) == len(requests)
        expected = dict(before)
        committed = aborted = 0
        breakdown = {'same_shard': Counter(), 'cross_shard_2pc': Counter()}
        for i, source, destination, amount, rid, _ in requests:
            outcome = outcomes[i]['outcome']
            category = 'same_shard' if i < 16 else 'cross_shard_2pc'
            breakdown[category][outcome] += 1
            if category == 'cross_shard_2pc':
                status_code, tx_status = self.http('GET', '/api/requests/' + rid)
                assert status_code == 200 and tx_status['status'] == outcome, (rid, tx_status)
            if outcome == 'COMMITTED':
                committed+=1
                expected[source] -= amount
                expected[destination] += amount
            elif outcome == 'ABORTED':
                aborted+=1
            else:
                raise AssertionError(outcome)
        after = {a:self.account(a)['balance'] for a in accounts}
        diffs = {a:{'expected':expected[a], 'actual':after[a]} for a in accounts
                 if after[a] != expected[a]}
        assert not diffs, f'Incorrect per-account final balances: {diffs}'
        after_total = self.grand_total()
        assert after_total == before_total, f'Money not conserved: {before_total} to {after_total}'
        assert committed >= 24, f'Only {committed}/32 requests committed under one replica failure'
        outage_completions = [x for x in completed if killed <= x['finished'] <= replacement_ready]
        assert outage_completions, 'No transfer completion observed during A1 outage'
        a2_while_down = [x for x in outage_completions if x['index'] < 16 and x['replica'] == 'A2']
        assert a2_while_down, 'No local transfer was confirmed by healthy A2 during A1 outage'
        self.log('TRANSFERS_DURING_FAILURE_VERIFIED', attempted=len(requests),
                 committed=committed, aborted=aborted, balance_mismatches=0,
                 money_before=before_total, money_after=after_total,
                 committed_or_aborted=len(completed), outcomes_by_type={k:dict(v) for k,v in breakdown.items()},
                 completed_while_a1_down=len(outage_completions),
                 same_shard_transfers_on_a2_during_outage=len(a2_while_down),
                 retry_errors=dict(sum((Counter(x['errors_before_success']) for x in completed), Counter())))
        self.log('REPLACEMENT_VERIFIED', slot='bank-a1', old_id=orig['bank-a1'],
                 new_id=new_id, healing_seconds=round(replacement_ready-killed,2),
                 neighbor_a2_unchanged=True)
        # Replay the same client request ID. A committed transaction must not charge twice.
        first = next((x for x in requests if outcomes[x[0]]['outcome']=='COMMITTED'),None)
        assert first
        rid = first[4]
        same_accounts = (first[1],first[2])
        balance_bef_dup = tuple(self.account(a)['balance'] for a in same_accounts)
        duplicate_http_status, response = self.http('POST','/api/transfer',{
            'source':first[1], 'destination':first[2], 'amount':first[3], 'request_id':rid})
        assert duplicate_http_status == 200 and response.get('ok') is True, response
        balance_aft_dup = tuple(self.account(a)['balance'] for a in same_accounts)
        assert balance_bef_dup == balance_aft_dup, (balance_bef_dup,balance_aft_dup)
        self.log('DUPLICATE_REQUEST_NO_DOUBLE_DEBIT', request_id=rid,
                 balances_unchanged=True, logical_money=after_total)
        replicas=self.check_replicas()
        assert all(v['healthy'] and v['slot_containers']==1 for v in replicas.values()), replicas
        self.log('FINAL_REPLICA_HEALTH', replicas=replicas)

    def run(self):
        self.log('TEST_BEGIN',case=self.case,project=self.project)
        passed=False
        error=None
        try:
            if self.case=='controller-crash':
                self.controller_crash()
            elif self.case=='transfers':
                self.transfer_failure()
            passed=True
            self.log('TEST_PASS',case=self.case)
        except Exception as exc:
            error=f'{type(exc).__name__}: {exc}'
            self.log('TEST_FAIL',error=error)
        finally:
            # If a test unexpectedly failed while the controller was down,
            # try to restore the controller, but do not silently edit banking data.
            try:
                if not self.inspect('self-healer')['State']['Running']:
                    self.cli('start',self.container('self-healer'),timeout=30)
                    self.log('CONTROLLER_RESTARTED_FOR_SAFETY')
            except Exception as exc:
                self.log('CONTROLLER_RESTORE_FAILED',error=str(exc))
            result={'case':self.case,'passed':passed,'error':error,
                    'project':self.project, 'started_utc':self.started.isoformat(),
                    'finished_utc':timestamp(), 'events':self.events,
                    'limitations':['Existing live test environment; transactions are real test data changes.',
                                   'Controller crash is injected before replacement, not mid-container-create.',
                                   'Does not prove zero transient HTTP failures.']}
            self.log('EVIDENCE_SAVED',text_log=str(self.text_path),json_report=str(self.json_path))
            result['events'] = self.events
            with self.json_path.open('w',encoding='utf-8') as f:
                json.dump(result,f,indent=2,ensure_ascii=False)
        print('BONUS II '+self.case+' '+('PASS' if passed else 'FAIL'),flush=True)
        return passed


def main():
    p=argparse.ArgumentParser(description='Bonus II adversarial controller and transfer tests (host run)')
    p.add_argument('--case', choices=(*CASES,'all'), default='all')
    p.add_argument('--project', default=DEFAULT_PROJECT,
                   help='Compose project label (default: distributed-banking-assignment2)')
    args=p.parse_args()
    chosen = CASES if args.case=='all' else (args.case,)
    results=[]
    for c in chosen:
        print(f'\n=== BONUS II TEST: {c} ===',flush=True)
        results.append(Suite(c,args.project).run())
    return 0 if all(results) else 1


if __name__=='__main__':
    sys.exit(main())
