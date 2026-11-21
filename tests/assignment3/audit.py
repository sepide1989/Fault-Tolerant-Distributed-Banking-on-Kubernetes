"""Read REAL PostgreSQL state (not merely HTTP status) through kubectl exec.

Usage:
  python tests/assignment3/audit.py --expected-total 1999500 --wait-recovery 180
  python tests/assignment3/audit.py --output reports/after-primary.json

Requires: kubectl access and CNPG pods in namespace banking.
"""
import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from scripts import audit_ledger
from scripts import cluster_state

NS='banking'


def kubectl(*args, timeout=45):
    p=subprocess.run(['kubectl','-n',NS,*map(str,args)], capture_output=True,text=True,timeout=timeout)
    if p.returncode:
        raise RuntimeError('kubectl '+ ' '.join(map(str,args)) +': '+p.stderr[-2000:])
    return p.stdout.strip()


def primary(cluster):
    name=kubectl('get','cluster',cluster,'-o','jsonpath={.status.currentPrimary}')
    if not name:
        raise RuntimeError(f'CNPG {cluster} has no current primary')
    return name


def sql(cluster, query):
    # Runs psql in the CNPG container as its postgres Unix account over a local
    # socket; no password placed in shell arguments or logs.
    pod=primary(cluster)
    return kubectl('exec',pod,'--','psql','-X','-qAt','-v','ON_ERROR_STOP=1',
                   '-U','postgres','-d','bank','-c',query,timeout=45)


def replica_roles(cluster):
    pods=json.loads(kubectl('get','pods','-l',f'cnpg.io/cluster={cluster}','-o','json'))['items']
    roles={}
    for pod in pods:
        name=pod['metadata']['name']
        # Query ALL PostgreSQL Pods, not only Ready ones: an isolated split-brain
        # primary must not disappear from the primary count merely because it
        # was removed from Service endpoints / marked NotReady.
        try:
            recovered=kubectl('exec',name,'--','psql','-X','-qAt',
                '-U','postgres','-d','bank','-c','SELECT pg_is_in_recovery()')
            if recovered not in ('t','f'):
                raise RuntimeError('unexpected pg_is_in_recovery() response: '+recovered)
            roles[name]='STANDBY' if recovered=='t' else 'PRIMARY'
        except Exception as exc:
            roles[name]='UNAVAILABLE: '+str(exc)[:120]
    return roles


def snapshot():
    summary={'created_utc':datetime.now(timezone.utc).isoformat(),'databases':{},'balances':{},'transactions':{}}
    total=0
    for cluster in ['bank-a','bank-b']:
        raw=sql(cluster,"SELECT account_id||'|'||balance FROM accounts ORDER BY account_id")
        accounts={}
        for line in raw.splitlines():
            if line:
                name,balance=line.split('|')
                accounts[name]=int(balance)
        summary['balances'][cluster]=accounts
        subtotal=sum(accounts.values())
        total+=subtotal
        out=sql(cluster,"SELECT pg_is_in_recovery(), current_setting('max_prepared_transactions'), (SELECT COUNT(*) FROM pg_prepared_xacts WHERE database=current_database())")
        summary['databases'][cluster]={'primary_pod':primary(cluster),'accounts':len(accounts), 'money':subtotal,'db_state':out}
        summary['databases'][cluster]['replica_roles']=replica_roles(cluster)
        summary['databases'][cluster]['replication']=sql(cluster,"SELECT COALESCE(string_agg(application_name||':'||sync_state,','),'<none>') FROM pg_stat_replication")
        summary['databases'][cluster]['local_journal']=int(sql(cluster,'SELECT COUNT(*) FROM local_requests'))
        summary['databases'][cluster]['participants']=int(sql(cluster,'SELECT COUNT(*) FROM participant_results'))
    summary['total']=total
    summary['transactions']={
        'states':sql('bank-meta',"SELECT phase||'|'||COALESCE(decision,'-')||'|'||COUNT(*) FROM transactions GROUP BY phase,decision ORDER BY phase"),
        'pending_count':int(sql('bank-meta',"SELECT COUNT(*) FROM transactions WHERE phase<>'COMPLETED'")),
        'committed_count':int(sql('bank-meta',"SELECT COUNT(*) FROM transactions WHERE phase='COMPLETED' AND decision='COMMIT'")),
        'aborted_count':int(sql('bank-meta',"SELECT COUNT(*) FROM transactions WHERE phase='COMPLETED' AND decision='ABORT'")),
        'distinct_request_ids':int(sql('bank-meta',"SELECT COUNT(DISTINCT request_id) FROM transactions")),
        'rows_count':int(sql('bank-meta',"SELECT COUNT(*) FROM transactions")),
        'metadata_replication':sql('bank-meta',"SELECT COALESCE(string_agg(application_name||':'||sync_state,','),'<none>') FROM pg_stat_replication"),
        'meta_primary':primary('bank-meta'),
        'meta_db_state':sql('bank-meta', "SELECT pg_is_in_recovery(), current_setting('max_prepared_transactions')"),
        'meta_replica_roles':replica_roles('bank-meta'),
        'meta_prepared':int(sql('bank-meta',"SELECT COUNT(*) FROM pg_prepared_xacts")),
    }
    return audit_ledger.collect(summary,NS)


def main():
    global NS
    parser=argparse.ArgumentParser()
    parser.add_argument('--namespace',default='banking')
    parser.add_argument('--expected-total',type=int)
    parser.add_argument('--expected-accounts',type=int,default=2000)
    parser.add_argument('--wait-recovery',type=int,default=0,help='seconds to wait for pending 2PC resolution')
    parser.add_argument('--output')
    parser.add_argument('--baseline')
    parser.add_argument('--load-log')
    args=parser.parse_args()
    NS=args.namespace
    started=time.monotonic()
    quiescent=False
    while True:
        try:
            result=snapshot()
            recovered=(result['transactions']['pending_count']==0 and
                       all(c['status'].get('readyInstances')==c['spec']['instances'] and
                           len(c.get('instances',[]))==c['spec']['instances'] and
                           all(p.get('ready') and 'pg' in p for p in c['instances'])
                           for c in result['clusters'].values()))
            if recovered:
                repeated=snapshot()
                quiescent=audit_ledger.fingerprint(result)==audit_ledger.fingerprint(repeated)
                result=repeated
        except Exception as exc:
            if time.monotonic()-started<args.wait_recovery:
                time.sleep(3)
                continue
            result={'created_utc':cluster_state.utc(),'status':'FAIL','pass':False,'error':str(exc)}
            cluster_state.write_report(args.output or 'reports/assignment3/audit.json',result)
            print(json.dumps(result,indent=2))
            raise SystemExit(2)
        if quiescent or time.monotonic()-started>=args.wait_recovery:
            break
        time.sleep(5)
    accounts=sum(row['accounts'] for row in result['databases'].values())
    checks={
        'account_count':accounts==args.expected_accounts,
        'non_negative_balances':all(n>=0 for shard in result['balances'].values() for n in shard.values()),
        'unique_cross_shard_request_ids':result['transactions']['distinct_request_ids']==result['transactions']['rows_count'],
        'no_pending_after_wait':result['transactions']['pending_count']==0,
        'all_primaries_writable':all(d['db_state'].startswith('f|') for d in result['databases'].values()),
        'no_duplicate_writable_primaries':all(sum(r=='PRIMARY' for r in d['replica_roles'].values())==1 for d in result['databases'].values()) and sum(r=='PRIMARY' for r in result['transactions']['meta_replica_roles'].values())==1,
        'metadata_primary_writable':result['transactions']['meta_db_state'].startswith('f|'),
    }
    if args.expected_total is not None:
        checks['expected_total']=result['total']==args.expected_total
    baseline=json.loads(Path(args.baseline).read_text(encoding='utf-8')) if args.baseline else None
    checks.update(audit_ledger.validate(result,baseline,args.load_log))
    checks['quiescent_ledger_observed_twice']=quiescent
    result['checks']=checks
    result['pass']=all(checks.values())
    print(json.dumps({'pass':result['pass'],'total':result['total'],'checks':checks},indent=2,ensure_ascii=False))
    if args.output:
        output=Path(args.output)
        output.parent.mkdir(parents=True,exist_ok=True)
        cluster_state.write_report(output,result)
        print('Saved exact all-account balance snapshot to',output)
    if not result['pass']:
        raise SystemExit(2)

if __name__=='__main__':main()
