"""Assess actual permanent CP loss, management restore and exact banking evidence."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import PLAN,vm


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-dir',required=True)
    p.add_argument('--preflight',default='final-preflight.json');a=p.parse_args()
    root=Path(a.run_dir);read=lambda n:json.loads((root/n).read_text(encoding='utf-8'))
    result=read('result.json');before=read('before.json');after=read('after.json')
    management=read('management-verification.json');restore=read('management-restore.json')
    proof=read('post-recovery-restore-proof.json');preflight=read(a.preflight)
    lab=json.loads(PLAN.read_text(encoding='utf-8'));loss=result['permanent_loss']
    old_cp=next(n for n in read('preflight.json')['nodes']['items'] if
                'node-role.kubernetes.io/control-plane' in n['metadata'].get('labels',{}))
    checks={
        'permanent_cp_vm_and_disks_removed':loss['status']=='PERMANENT_VM_LOSS_INJECTED' and not Path(loss['target']).exists() and loss['vmx'].lower() not in vm('list').lower(),
        'original_vm_files_preserved':all(Path(n['source_vmx']).exists() for n in lab['nodes'].values()),
        'replacement_has_new_hardware_identity':restore['replacement_hardware_uuid']!=old_cp['status']['nodeInfo']['systemUUID'],
        'actual_etcd_pki_archive_restore':restore.get('api_ready') is True and all(restore['restored_cluster_uids_match'].values()),
        'original_resources_and_new_scheduling_verified':management['status']=='PASS' and all(management['checks'].values()),
        'workers_survived':bool(result['observations']) and all(o['both_worker_vms_running'] for o in result['observations']),
        'full_client_and_ledger_audit':after.get('pass') is True,
        'production_replication_and_backup_healthy':preflight.get('pass') is True,
        'fresh_all_database_restore_includes_post_failure_history':proof.get('status')=='PASS',
        'same_database_lineage':all({x['pg']['system_id'] for x in before['clusters'][n]['instances']}==
                                  {x['pg']['system_id'] for x in after['clusters'][n]['instances']} for n in cs.CLUSTERS),
    }
    result.update({'status':'PASS' if all(checks.values()) else 'PARTIAL','checks':checks,'final_verified_utc':cs.utc(),
        'final_audit':str(root/'after.json'),'management_proof':str(root/'management-verification.json'),
        'final_preflight':str(root/a.preflight),
        'primary_changes':{n:{'before':before['clusters'][n]['status']['currentPrimary'],'after':after['clusters'][n]['status']['currentPrimary']} for n in cs.CLUSTERS},
        'limitations':['Client availability was interrupted. Timeouts, 202 and 503 were not counted as success.',
            'Historical standalone drill claims on the previously lost worker remain retained/unavailable, separately from the nine healthy production instances.']})
    cs.write_report(root/'result.json',result)
    print(json.dumps({'status':result['status'],'checks':checks,'load_counts':result['load']['counts']},indent=2))
    return 0 if result['status']=='PASS' else 2


if __name__=='__main__':raise SystemExit(main())
