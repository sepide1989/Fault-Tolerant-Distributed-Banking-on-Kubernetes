"""Assess permanent-worker-loss evidence without altering any cluster data."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import PLAN,vm


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-dir',required=True);a=p.parse_args()
    root=Path(a.run_dir)
    read=lambda n:json.loads((root/n).read_text(encoding='utf-8'))
    result=read('result.json');before=read('before.json');after=read('final-audit.json')
    join=read('replacement-join.json');topology=read('after-topology.json')
    proof=read('post-recovery-restore-proof.json');preflight=read('final-preflight.json')
    storage=read('storage-recovery.json');lab=json.loads(PLAN.read_text(encoding='utf-8'))
    old_claims=[c for x in storage['plan']['instances'] for c in x['claims']]
    live_pvs={p['metadata']['name']:p for p in cs.get('pv')['items']}
    active_claims={c for n in cs.CLUSTERS for p in topology['clusters'][n]['instances'] for c in p['claims']}
    live_claims={p['metadata']['uid'] for p in cs.get('pvc')['items'] if p['metadata']['name'] in active_claims}
    counts={}
    for name in ['load.summary.json','post-recovery-load.summary.json']:
        for key,value in read(name)['counts'].items():counts[key]=counts.get(key,0)+value
    checks={
        'permanent_vm_and_disks_removed':result.get('permanent_loss',{}).get('status')=='PERMANENT_VM_LOSS_INJECTED' and
            not Path(result['permanent_loss']['target']).exists() and result['permanent_loss']['vmx'].lower() not in vm('list').lower(),
        'original_vm_files_preserved':all(Path(x['source_vmx']).exists() for x in lab['nodes'].values()),
        'fresh_replacement_node_identity':join['new_node_uid']!=storage['plan']['node_uid'],
        'all_old_lost_pvs_retained':all(live_pvs[c['pv']]['metadata']['uid']==c['pv_uid'] and
            live_pvs[c['pv']]['spec']['persistentVolumeReclaimPolicy']=='Retain' for c in old_claims),
        'active_instances_use_fresh_claims_for_lost_disks':not ({c['uid'] for c in old_claims}&live_claims),
        'surviving_database_lineage_preserved':all(
            {x['pg']['system_id'] for x in after['clusters'][n]['instances']}==
            {x['pg']['system_id'] for x in before['clusters'][n]['instances']} for n in cs.CLUSTERS),
        'full_client_and_ledger_audit':after.get('pass') is True,
        'redundancy_placement_and_sync_verified':preflight.get('pass') is True,
        'fresh_post_recovery_external_restore':proof.get('status')=='PASS',
        'no_observed_conflicting_primaries':all(sum(x.get('pg',{}).get('standby') is False for x in record.get('instances',[]))<=1
            for observation in result['observations'] for record in observation['clusters'].values()),
        'real_load_before_loss_and_after_recovery':read('load.summary.json')['counts'].get('validated_success',0)>0 and
            read('post-recovery-load.summary.json')['counts'].get('validated_success',0)>0,
    }
    result.update({'status':'PASS' if all(checks.values()) else 'PARTIAL','checks':checks,'final_verified_utc':cs.utc(),
        'final_audit':str(root/'final-audit.json'),'final_restore_proof':str(root/'post-recovery-restore-proof.json'),
        'combined_client_counts':counts,'primary_changes':{n:{'before':before['clusters'][n]['status']['currentPrimary'],
            'after':after['clusters'][n]['status']['currentPrimary']} for n in cs.CLUSTERS},
        'limitations':['Infrastructure recovery required fixes to join ordering, controller/PVC deletion races and replica placement; these were applied and recorded. No transaction decision or account balance was edited.',
            'Old standalone restore-drill claims pinned to the destroyed node remain retained/unavailable; they are not production replicas and were not treated as usable recovery sources.']})
    cs.write_report(root/'result.json',result)
    print(json.dumps({'status':result['status'],'checks':checks,'combined_client_counts':counts},indent=2))
    return 0 if result['status']=='PASS' else 2


if __name__=='__main__':raise SystemExit(main())
