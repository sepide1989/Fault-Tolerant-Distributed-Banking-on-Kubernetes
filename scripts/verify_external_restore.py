"""Restore all three DBs into new names and compare full durable bank ledgers.

No live data/Services are replaced. Stops on any uncertainty. Keep clones/PVCs
for inspection; cleanup is a separate reviewed destructive action.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path
import sys
import time
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.audit_ledger import collect,fingerprint


def create_archive_cutoff(pod,marker):
    if not re.fullmatch('[a-zA-Z0-9_-]{1,63}',marker):raise ValueError('unsafe restore-point name')
    # An insert pointer can be just an empty segment header. pg_switch_wal()
    # does nothing when no record has been written since the previous switch.
    # A real restore-point record provides a valid target and forces archiving
    # without changing any banking table or transaction decision.
    cutoff=cs.sql_pod(pod,"SELECT pg_create_restore_point('"+marker+"')::text")
    wanted=cs.sql_pod(pod,"SELECT pg_walfile_name('"+cutoff+"'::pg_lsn)")
    cs.sql_pod(pod,'SELECT pg_switch_wal()')
    return cutoff,wanted


def main():
    p=argparse.ArgumentParser();p.add_argument('--baseline',required=True)
    p.add_argument('--run-id',required=True);p.add_argument('--apply',action='store_true')
    p.add_argument('--timeout',type=int,default=900)
    p.add_argument('--allow-degraded-source',action='store_true',help='Remediation only: requires every banking-ledger check to pass; HA preflight remains separate')
    p.add_argument('--output',default='reports/assignment3/restore-proof.json');a=p.parse_args()
    if not a.run_id.isalnum() or len(a.run_id)>12:p.error('run-id must be 1..12 alphanumeric characters')
    baseline=json.loads(Path(a.baseline).read_text(encoding='utf-8'))
    critical=('account_count','exact_account_ids','non_negative_balances','unique_cross_shard_request_ids',
              'no_pending_after_wait','no_prepared_left','all_primaries_writable','metadata_primary_writable',
              'request_id_used_once_globally','2pc_votes_decisions_acks_match_participants','quiescent_ledger_observed_twice')
    if baseline.get('pass') is not True and not (a.allow_degraded_source and all(baseline.get('checks',{}).get(k) is True for k in critical)):
        raise SystemExit('STOP: a passing quiescent banking ledger is required')
    out={'created_utc':cs.utc(),'status':'FAIL','clusters':{},'cluster_uids':{},
         'audit_sha256':hashlib.sha256(Path(a.baseline).read_bytes()).hexdigest()}
    out['source_ha_was_degraded']=baseline.get('pass') is not True
    try:
        for name in cs.CLUSTERS:
            source=cs.get('cluster',name);out['cluster_uids'][name]=source['metadata']['uid']
            pod=source['status']['currentPrimary'];spec=source['spec']
            plugins=[x for x in spec.get('plugins',[]) if x['name']=='barman-cloud.cloudnative-pg.io' and x.get('isWALArchiver')]
            if len(plugins)!=1:raise RuntimeError(name+': missing unique Barman WAL archiver')
            store=plugins[0]['parameters']['barmanObjectName']
            target=name+'-dr-'+a.run_id.lower()
            clone={'apiVersion':'postgresql.cnpg.io/v1','kind':'Cluster','metadata':{'name':target,'namespace':cs.NS},
                'spec':{'instances':1,'imageName':spec['imageName'],
                    'bootstrap':{'recovery':{'source':'original'}},
                    'externalClusters':[{'name':'original','plugin':{'name':plugins[0]['name'],
                        'parameters':{'barmanObjectName':store,'serverName':name}}}],
                    'storage':spec['storage'],'affinity':spec.get('affinity',{}),
                    'postgresql':{'parameters':{'max_prepared_transactions':'100'}}}}
            out['clusters'][name]={'restore_name':target,'source_primary':pod}
            if not a.apply:
                print(json.dumps(clone,indent=2));continue
            # Fail if source data changed after baseline; do not bless an old backup.
            checks=[('accounts',baseline['balances'].get(name))] if name!='bank-meta' else []
            if name!='bank-meta':
                live={r['account_id']:r['balance'] for r in cs.rows(pod,'SELECT * FROM accounts ORDER BY account_id')}
                if live!=baseline['balances'][name]:raise RuntimeError('baseline changed: '+name)
            marker='banking_dr_'+a.run_id+'_'+name.replace('-','_')+'_'+uuid.uuid4().hex[:8]
            cutoff,wanted=create_archive_cutoff(pod,marker)
            out['clusters'][name]['restore_point']=marker
            deadline=time.monotonic()+min(a.timeout,180)
            while True:
                archived=cs.sql_pod(pod,"SELECT COALESCE(last_archived_wal,'') FROM pg_stat_archiver")
                if len(archived)==24 and archived[:8]==wanted[:8] and archived>=wanted:break
                if time.monotonic()>deadline:raise RuntimeError(name+': cutoff WAL not archived')
                time.sleep(3)
            clone['spec']['bootstrap']['recovery']['recoveryTarget']={'targetLSN':cutoff,'exclusive':False}
            # create, never apply over a previous potentially valuable restore.
            cs.kb('create','-f','-',stdin=json.dumps(clone))
            cs.kb('wait','--for=condition=Ready','cluster/'+target,'--timeout='+str(a.timeout)+'s',timeout=a.timeout+20)
            restored=cs.get('cluster',target)['status']['currentPrimary']
            # Include registry, participant fences, routing/ownership and legacy
            # migration state as well as the money/2PC tables.
            table_query="SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
            tables=[r['tablename'] for r in cs.rows(pod,table_query)]
            restored_tables=[r['tablename'] for r in cs.rows(restored,table_query)]
            matches={'public_table_set':tables==restored_tables}
            for table in tables:
                if not re.fullmatch('[a-zA-Z_][a-zA-Z_0-9]*',table):raise RuntimeError('unexpected public table identifier')
                q='SELECT * FROM public."'+table+'" AS t ORDER BY to_jsonb(t)::text'
                matches[table]=cs.rows(pod,q)==cs.rows(restored,q)
            out['clusters'][name].update({'cutoff_lsn':cutoff,'archived_wal':archived,'matches':matches,
                                         'restore_pg':cs.pg_state(restored)})
            state=out['clusters'][name]['restore_pg']
            if state['standby'] or state['prepared']:raise RuntimeError(name+': restored DB not terminal/writable')
            if not all(matches.values()):raise RuntimeError(name+': restored ledger differs from live data')
        if a.apply:
            # Exact baseline ledger comparison binds proof to the acknowledged history.
            livebase={'databases':baseline['databases'],'transactions':baseline['transactions'],'balances':{}}
            for n in ('bank-a','bank-b'):
                pod=cs.get('cluster',n)['status']['currentPrimary']
                livebase['databases'][n]['primary_pod']=pod
                livebase['balances'][n]={r['account_id']:r['balance'] for r in cs.rows(pod,'SELECT * FROM accounts ORDER BY account_id')}
            livebase['transactions']['meta_primary']=cs.get('cluster','bank-meta')['status']['currentPrimary']
            if fingerprint(collect(livebase,cs.NS))!=fingerprint(baseline):raise RuntimeError('bank ledger changed during restore drill; stop writers and retry with fresh baseline')
            out['status']='PASS'
        else:out['status']='DRY_RUN'
    except Exception as exc:out['error']=str(exc)
    cs.write_report(a.output,out);print(out['status'])
    return 0 if out['status'] in ('PASS','DRY_RUN') else 2


if __name__=='__main__':raise SystemExit(main())
