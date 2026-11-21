"""Fail-closed live diagnostics. A completed Backup is not a restore proof."""
import argparse
import json
from pathlib import Path
import sys
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def inspect(restore_proof=None):
    out={'created_utc':cs.utc(),'status':'FAIL','checks':{},'clusters':{},'errors':{}}
    resources={'nodes':('nodes',),'pvs':('pv',),'pvcs':('pvc',),
       'pods':('pods','-A'),'jobs':('jobs',),'events':('events',),
       'deployments':('deployments','-A'),'storageclasses':('storageclass',),
       'backups':('backups',),'schedules':('scheduledbackups',),'objectstores':('objectstores.barmancloud.cnpg.io',),
       'leases':('leases',)}
    for key,args in resources.items():
        try: out[key]=cs.sanitize(cs.get(*args))
        except Exception as exc: out['errors'][key]=str(exc)
    for name in cs.CLUSTERS:
        try: out['clusters'][name]=cs.database_cluster(name)
        except Exception as exc: out['errors'][name]=str(exc)
    c=out['checks']; nodes=out.get('nodes',{}).get('items',[])
    workers=[n for n in nodes if 'node-role.kubernetes.io/control-plane' not in n['metadata'].get('labels',{})]
    c['one_control_plane_two_workers']=len(nodes)==3 and len(workers)==2
    c['all_nodes_ready']=len(nodes)==3 and all(any(v['type']=='Ready' and v['status']=='True' for v in n['status']['conditions']) for n in nodes)
    for name in cs.CLUSTERS:
        record=out['clusters'].get(name,{}); instances=record.get('instances',[])
        primary=[p for p in instances if p.get('pg',{}).get('standby') is False]
        prefix=name+'.'
        c[prefix+'three_queryable_ready']=len(instances)==3 and all('pg' in p and p['ready'] for p in instances)
        c[prefix+'one_primary_all_observed']=len(primary)==1 and len(instances)==3 and all('pg' in p for p in instances)
        c[prefix+'local_and_remote_standby']=len(primary)==1 and any(p['node']==primary[0]['node'] and p.get('pg',{}).get('standby') for p in instances) and any(p['node']!=primary[0]['node'] and p.get('pg',{}).get('standby') for p in instances)
        sync=record.get('spec',{}).get('postgresql',{}).get('synchronous',{})
        c[prefix+'strict_two_sync']=sync.get('number')==2 and sync.get('dataDurability')=='required'
        c[prefix+'streaming_and_sync']=len(primary)==1 and len(primary[0]['pg']['replication'])==2 and all(r['state']=='streaming' and r['sync_state'] in ('sync','quorum') for r in primary[0]['pg']['replication']) and all(p['pg']['synchronous_commit'] in ('on','remote_apply') and p['pg']['fsync']=='on' for p in instances if 'pg' in p)
        archiver=primary[0]['pg'].get('archiver',{}) if len(primary)==1 else {}
        try:
            success=datetime.fromisoformat(archiver['last_archived_time'])
            failed=datetime.fromisoformat(archiver['last_failed_time']) if archiver.get('last_failed_time') else None
            c[prefix+'archiver_progress_after_last_error']=failed is None or success>=failed
        except (KeyError,TypeError,ValueError):c[prefix+'archiver_progress_after_last_error']=False
        claims=[v for p in instances for v in p['claims']]
        c[prefix+'independent_claims']=len(claims)==3 and len(set(claims))==3
        backups=out.get('backups',{}).get('items',[])
        c[prefix+'completed_backup']=any(b['spec'].get('cluster',{}).get('name')==name and b.get('status',{}).get('phase')=='completed' for b in backups)
    pv_by_name={x['metadata']['name']:x for x in out.get('pvs',{}).get('items',[])}
    pvc_by_name={x['metadata']['name']:x for x in out.get('pvcs',{}).get('items',[])}
    bindings=[]
    for record in out['clusters'].values():
        for instance in record['instances']:
            for claim in instance['claims']:
                pvc=pvc_by_name.get(claim,{}); pv=pv_by_name.get(pvc.get('spec',{}).get('volumeName'),{})
                terms=pv.get('spec',{}).get('nodeAffinity',{}).get('required',{}).get('nodeSelectorTerms',[])
                bindings.append(pvc.get('status',{}).get('phase')=='Bound' and any(
                    e.get('key')=='kubernetes.io/hostname' and e.get('operator')=='In' and instance['node'] in e.get('values',[]) for t in terms for e in t.get('matchExpressions',[])))
    c['all_local_pv_affinities_match']=len(bindings)==9 and all(bindings)
    for app in ('bank-a','bank-b','gateway','coordinator-recovery'):
        placements={p['spec'].get('nodeName') for p in out.get('pods',{}).get('items',[]) if p['metadata']['namespace']==cs.NS and p['metadata'].get('labels',{}).get('app')==app and any(v['type']=='Ready' and v['status']=='True' for v in p.get('status',{}).get('conditions',[]))}
        c[app+'.application_spread']=len(placements)==2 and placements <= {n['metadata']['name'] for n in workers}
    deployments=out.get('deployments',{}).get('items',[])
    operator=next((d for d in deployments if d['metadata']['name']=='cnpg-controller-manager' and d['metadata']['namespace']=='cnpg-system'),{})
    selector=operator.get('spec',{}).get('selector',{}).get('matchLabels',{})
    operator_nodes={p['spec'].get('nodeName') for p in out.get('pods',{}).get('items',[])
        if p['metadata']['namespace']=='cnpg-system' and selector and all(p['metadata'].get('labels',{}).get(k)==v for k,v in selector.items())
        and any(v['type']=='Ready' and v['status']=='True' for v in p.get('status',{}).get('conditions',[]))}
    c['cnpg_operator_survives_either_worker']=len(operator_nodes)==2 and operator.get('status',{}).get('readyReplicas',0)>=2
    # Destructive scenarios need proof produced by an actual independent restore,
    # bound to this cluster UID and current live state. A user boolean is insufficient.
    proof=json.loads(Path(restore_proof).read_text(encoding='utf-8')) if restore_proof else {}
    try:proof_fresh=0<=(datetime.now(timezone.utc)-datetime.fromisoformat(proof['created_utc'])).total_seconds()<86400
    except (KeyError,ValueError,TypeError):proof_fresh=False
    c['all_three_restore_drills_verified']=proof_fresh and proof.get('status')=='PASS' and all(proof.get('cluster_uids',{}).get(n)==out['clusters'].get(n,{}).get('uid') and all(proof.get('clusters',{}).get(n,{}).get('matches',{}).values()) and bool(proof.get('clusters',{}).get(n,{}).get('matches')) for n in cs.CLUSTERS) and bool(proof.get('audit_sha256'))
    c['collection_complete']=not out['errors']
    out['pass']=all(c.values()); out['status']='PASS' if out['pass'] else 'FAIL'
    return out


def main():
    p=argparse.ArgumentParser(); p.add_argument('--namespace',default='banking')
    p.add_argument('--restore-proof');p.add_argument('--output',default='reports/assignment3/preflight.json')
    a=p.parse_args();cs.NS=a.namespace
    result=inspect(a.restore_proof);cs.write_report(a.output,result)
    print(json.dumps({'status':result['status'],'checks':result['checks'],'errors':result['errors']},indent=2))
    return 0 if result['pass'] else 2


if __name__=='__main__':raise SystemExit(main())
