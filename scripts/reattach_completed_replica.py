"""Reattach a retained PVC ONLY after successful basebackup verification.

No data file, balance, decision, PV or PVC is deleted. Repairs CNPG metadata
left by destroy --keep-pvc, with UID and completed-job preconditions.
"""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--cluster',required=True);p.add_argument('--instance',required=True)
    p.add_argument('--maintenance-pod',required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    cluster=cs.get('cluster',a.cluster);pvc=cs.get('pvc',a.instance);job=cs.get('job',a.instance+'-join')
    if pvc['metadata'].get('ownerReferences'):raise SystemExit('STOP: PVC already has an owner')
    if pvc['metadata'].get('annotations',{}).get('cnpg.io/pvcStatus')!='detached':raise SystemExit('STOP: PVC is not detached')
    if not any(c['type']=='Complete' and c['status']=='True' for c in job.get('status',{}).get('conditions',[])):raise SystemExit('STOP: join Job incomplete')
    if not any(r['uid']==cluster['metadata']['uid'] for r in job['metadata'].get('ownerReferences',[])):raise SystemExit('STOP: job lineage differs')
    pv=cs.get('pv',pvc['spec']['volumeName']);path=pv['spec'].get('hostPath',pv['spec'].get('local',{})).get('path')
    if not path or not path.startswith('/opt/local-path-provisioner/'):raise SystemExit('STOP: unexpected local path')
    data='/host'+path+'/pgdata'
    verify=cs.kb('exec',a.maintenance_pod,'--','pg_verifybackup','--no-parse-wal','--ignore=override.conf',
                 '--ignore=custom.conf','--ignore=postgresql.auto.conf',data,timeout=90)
    control=cs.kb('exec',a.maintenance_pod,'--','pg_controldata',data)
    primary=cs.pg_state(cluster['status']['currentPrimary'])
    if primary['system_id'] not in control:raise SystemExit('STOP: source and retained PVC system IDs differ')
    output={'created_utc':cs.utc(),'status':'DRY_RUN','cluster':a.cluster,'pvc_uid':pvc['metadata']['uid'],
            'job_uid':job['metadata']['uid'],'verification':verify,'primary':primary}
    if a.apply:
        owner={'apiVersion':'postgresql.cnpg.io/v1','kind':'Cluster','name':a.cluster,
               'uid':cluster['metadata']['uid'],'controller':True,'blockOwnerDeletion':True}
        patch=[{'op':'test','path':'/metadata/uid','value':pvc['metadata']['uid']},
               {'op':'add','path':'/metadata/ownerReferences','value':[owner]},
               {'op':'replace','path':'/metadata/annotations/cnpg.io~1pvcStatus','value':'initializing'}]
        cs.kb('patch','pvc',a.instance,'--type=json','-p',json.dumps(patch))
        output['status']='PARTIAL'
    cs.write_report('reports/assignment3/reattach-'+a.instance+'.json',output)
    print(output['status'],verify)


if __name__=='__main__':main()
