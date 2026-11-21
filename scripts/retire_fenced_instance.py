"""Reversible retirement of a fenced replica; keeps its PVC, PV and cold copy.

Pause reconciliation during the change so scale-down cannot remove the new
healthy replacement. No destroy command or PVC/data deletion is used.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.recover_lost_worker import delete_uid


def main():
    p=argparse.ArgumentParser();p.add_argument('--cluster',required=True);p.add_argument('--instance',required=True)
    p.add_argument('--archive-proof',required=True);p.add_argument('--restore-proof',required=True)
    p.add_argument('--apply',action='store_true');p.add_argument('--confirm-instance');a=p.parse_args()
    cluster=cs.get('cluster',a.cluster);pvc=cs.get('pvc',a.instance);pod=cs.get('pod',a.instance)
    archive=json.loads(Path(a.archive_proof).read_text());proof=json.loads(Path(a.restore_proof).read_text())
    if proof.get('status')!='PASS' or any(not all(proof['clusters'][n]['matches'].values()) for n in cs.CLUSTERS):raise SystemExit('STOP: all-three independent restores required')
    if archive.get('pvc_uid')!=pvc['metadata']['uid'] or hashlib.sha256(Path(archive['archive']).read_bytes()).hexdigest()!=archive['sha256']:raise SystemExit('STOP: preserved cold copy/UID mismatch')
    if a.instance not in json.loads(cluster['metadata']['annotations'].get('cnpg.io/fencedInstances','[]')):raise SystemExit('STOP: instance not fenced')
    if a.instance==cluster['status']['currentPrimary']:raise SystemExit('STOP: primary target')
    if any(x.strip()=='postgres' for x in cs.kb('exec',a.instance,'-c','postgres','--','ps','-eo','comm').splitlines()):raise SystemExit('STOP: PostgreSQL still running')
    live=cs.database_cluster(a.cluster);good=[x for x in live['instances'] if x['pod']!=a.instance and 'pg' in x and x['ready']]
    if len(good)!=3 or sum(not x['pg']['standby'] for x in good)!=1:raise SystemExit('STOP: three healthy replacement/source instances required')
    primary=next(x for x in good if not x['pg']['standby'])
    if len(primary['pg']['replication'])!=2 or any(r['state']!='streaming' or r['sync_state'] not in ('sync','quorum') for r in primary['pg']['replication']):raise SystemExit('STOP: two synchronous streams required')
    out={'created_utc':cs.utc(),'status':'DRY_RUN','retained_pvc':a.instance,'pvc_uid':pvc['metadata']['uid'],
         'remaining_instances':[x['pod'] for x in good],'primary':primary['pod']}
    if a.apply:
        if a.confirm_instance!=a.instance:raise SystemExit('STOP: confirm exact fenced instance')
        cs.kb('annotate','cluster',a.cluster,'cnpg.io/reconciliationLoop=disabled','--overwrite')
        time.sleep(2)
        pv=cs.get('pv',pvc['spec']['volumeName'])
        cs.kb('patch','pv',pv['metadata']['name'],'--type=json','-p',json.dumps([
            {'op':'test','path':'/metadata/uid','value':pv['metadata']['uid']},
            {'op':'replace','path':'/spec/persistentVolumeReclaimPolicy','value':'Retain'}]))
        cs.kb('patch','pvc',a.instance,'--type=json','-p',json.dumps([
            {'op':'test','path':'/metadata/uid','value':pvc['metadata']['uid']},
            {'op':'remove','path':'/metadata/ownerReferences'},
            {'op':'replace','path':'/metadata/annotations/cnpg.io~1pvcStatus','value':'detached'}]))
        delete_uid('pods',a.instance,pod['metadata']['uid'])
        cs.kb('patch','cluster',a.cluster,'--type=merge','-p',json.dumps({'spec':{'instances':3}}))
        cs.kb('annotate','cluster',a.cluster,'cnpg.io/reconciliationLoop-','cnpg.io/fencedInstances-')
        out['status']='PARTIAL'
    cs.write_report('reports/assignment3/retire-'+a.instance+'.json',out);print(json.dumps(out,indent=2))


if __name__=='__main__':main()
