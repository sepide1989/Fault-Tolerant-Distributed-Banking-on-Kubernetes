"""Extend the existing audit with ID-level durable ledger and balance replay."""
import json
from pathlib import Path
from scripts import cluster_state as cs


def collect(result,namespace):
    cs.NS=namespace
    result['clusters']={n:cs.database_cluster(n) for n in cs.CLUSTERS}
    result['ledger']={'local':{},'participants':{}}
    ledger=result['ledger']
    for name in ('bank-a','bank-b'):
        pod=result['databases'][name]['primary_pod']
        ledger['local'][name]=cs.rows(pod,'SELECT * FROM local_requests ORDER BY request_id')
        ledger['participants'][name]=cs.rows(pod,'SELECT * FROM participant_results ORDER BY tx_id')
    pod=result['transactions']['meta_primary']
    for name,query in [('transactions','SELECT * FROM transactions ORDER BY tx_id'),
                       ('votes','SELECT * FROM votes ORDER BY tx_id,shard'),
                       ('acks','SELECT * FROM acknowledgements ORDER BY tx_id,shard')]:
        ledger[name]=cs.rows(pod,query)
    return result


def fingerprint(result):
    return json.dumps([result['balances'],result['ledger']],sort_keys=True)


def validate(result,baseline=None,load_log=None):
    ledger=result['ledger'];transactions={t['tx_id']:t for t in ledger['transactions']}
    votes={(r['tx_id'],r['shard']):r['vote'] for r in ledger['votes']}
    acks={(r['tx_id'],r['shard']):r['decision'] for r in ledger['acks']}
    participants={s:{r['tx_id']:r['status'] for r in records} for s,records in ledger['participants'].items()}
    balances={k:v for b in result['balances'].values() for k,v in b.items()}
    issues=[]
    checks={'exact_account_ids':set(balances)=={f'A{i:04}' for i in range(1,2001)},
      'account_ownership':all((int(k[1:])<=1000 if s=='bank-a' else int(k[1:])>1000) for s,b in result['balances'].items() for k in b),
      'all_instances_observed':all(len(c['instances'])==c['spec']['instances'] and all('pg' in p for p in c['instances']) for c in result['clusters'].values()),
      'no_prepared_left':all(not p['pg']['prepared'] for c in result['clusters'].values() for p in c['instances'] if 'pg' in p),
      'replication_restored':all(c['status'].get('readyInstances')==c['spec']['instances'] and all(p.get('pg',{}).get('wal_receiver') and p['pg']['wal_receiver'][0]['status']=='streaming' for p in c['instances'] if p.get('pg',{}).get('standby')) for c in result['clusters'].values())}
    for tx,t in transactions.items():
        if t['phase']!='COMPLETED' or t['decision'] not in ('COMMIT','ABORT'):
            issues.append([tx,'nonterminal']);continue
        for shard in {t['source_shard'],t['destination_shard']}:
            want='COMMITTED' if t['decision']=='COMMIT' else 'ABORTED'
            if participants.get('bank-'+shard.lower(),{}).get(tx)!=want:issues.append([tx,shard,'participant mismatch'])
            if acks.get((tx,shard))!=t['decision']:issues.append([tx,shard,'ack mismatch'])
            if t['decision']=='COMMIT' and votes.get((tx,shard))!='YES':issues.append([tx,shard,'missing YES vote'])
    for shard,records in participants.items():
        for tx in records:
            if tx not in transactions:issues.append([tx,shard,'orphan participant'])
    for r in ledger['votes']+ledger['acks']:
        if r['tx_id'] not in transactions:issues.append([r['tx_id'],'orphan coordinator record'])
    local={(s,r['request_id']):r for s,records in ledger['local'].items() for r in records}
    request_ids=[r['request_id'] for r in local.values()]+[r['request_id'] for r in transactions.values()]
    checks['request_id_used_once_globally']=len(request_ids)==len(set(request_ids))
    checks['local_results_complete']=all(r['result'] is not None for r in local.values())
    if baseline:
        oldtx={t['tx_id']:t for t in baseline['ledger']['transactions']}
        oldlocal={(s,r['request_id']):r for s,records in baseline['ledger']['local'].items() for r in records}
        # updated_at can advance on a synchronous retry barrier; identity and
        # terminal decision must remain identical.
        def stable(t):return {k:v for k,v in t.items() if k!='updated_at'}
        checks['baseline_was_valid']=baseline.get('pass') is True
        checks['prior_transactions_preserved']=all(k in transactions and stable(transactions[k])==stable(v) for k,v in oldtx.items())
        checks['prior_local_journals_preserved']=all(local.get(k)==v for k,v in oldlocal.items())
        expected={k:v for b in baseline['balances'].values() for k,v in b.items()}
        def delta(account,amount):expected[account]=expected.get(account,0)+amount
        for tx,t in transactions.items():
            if tx not in oldtx and t['phase']=='COMPLETED' and t['decision']=='COMMIT':
                delta(t['source_account'],-t['amount']);delta(t['destination_account'],t['amount'])
        for key,r in local.items():
            if key in oldlocal:continue
            p=r['payload'];a=p['amount']
            if r['op_type']=='transfer':delta(p['source'],-a);delta(p['destination'],a)
            elif r['op_type'] in ('deposit','withdraw'):delta(p['account'],a if r['op_type']=='deposit' else -a)
            else:issues.append([str(key),'unknown local operation'])
        checks['each_balance_matches_ledger_replay']=expected==balances
        result['balance_mismatches']={k:{'expected':v,'actual':balances.get(k)} for k,v in expected.items() if balances.get(k)!=v}
    if load_log:
        intents={};successes=set();client_issues=[]
        for line in Path(load_log).read_text(encoding='utf-8').splitlines():
            event=json.loads(line)
            if event['event']=='intent':
                p=event['payload'];rid=p['request_id']
                if rid in intents and intents[rid]!=p:client_issues.append([rid,'changed retry payload'])
                intents[rid]=p
            elif event.get('success') is True:successes.add(event['request_id'])
        tr={t['request_id']:t for t in transactions.values()}
        local_by_id={r['request_id']:r for r in local.values()}
        for rid in successes:
            p=intents.get(rid,{})
            if rid in tr:
                t=tr[rid];good=t['phase']=='COMPLETED' and t['decision']=='COMMIT' and (t['source_account'],t['destination_account'],t['amount'])==(p.get('source'),p.get('destination'),p.get('amount'))
            else:
                good=rid in local_by_id and local_by_id[rid]['payload']=={k:v for k,v in p.items() if k!='request_id'} and local_by_id[rid]['result'] is not None
            if not good:client_issues.append([rid,'acknowledged operation missing or changed'])
        checks['acknowledged_requests_preserved']=not client_issues
        result['client_ledger_issues']=client_issues
        result['acknowledged_request_count']=len(successes)
        result['unacknowledged_outcomes']={rid:(tr[rid]['decision'] if rid in tr else 'LOCAL_COMMITTED' if rid in local_by_id else 'UNREGISTERED_NO_DURABLE_EFFECT') for rid in intents.keys()-successes}
    result['ledger_issues']=issues
    checks['2pc_votes_decisions_acks_match_participants']=not issues
    return checks
