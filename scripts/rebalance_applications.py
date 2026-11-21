"""Repair rollout/downscale skew by replacing ONE stateless pod at a time.

Topology spread constrains scheduling; it does not constrain ReplicaSet deletion.
The preflight blocks failures until the resulting placement is actually verified.
"""
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.recover_lost_worker import delete_uid


def ready(app):
    return [p for p in cs.get('pods','-l','app='+app)['items'] if not p['metadata'].get('deletionTimestamp') and any(c['type']=='Ready' and c['status']=='True' for c in p.get('status',{}).get('conditions',[]))]


def main():
    for app in ('bank-a','bank-b','gateway','coordinator-recovery'):
        pods=ready(app)
        if len({p['spec']['nodeName'] for p in pods})>=2:continue
        if len(pods)<2:raise SystemExit('STOP: fewer than two ready replicas for '+app)
        pod=pods[-1]
        if any('persistentVolumeClaim' in v for v in pod['spec'].get('volumes',[])):raise SystemExit('STOP: refuse any pod carrying a PVC')
        print('Replacing stateless pod',pod['metadata']['name'],flush=True)
        # Graceful deletion; a surviving replica and durable coordinator recover
        # any interrupted request. No database process or storage is targeted.
        cs.kb('delete','pod',pod['metadata']['name'],'--wait=false')
        deadline=time.monotonic()+120
        while len({p['spec']['nodeName'] for p in ready(app)})<2:
            if time.monotonic()>deadline:raise SystemExit('FAIL: application placement did not converge: '+app)
            time.sleep(2)
    print('PASS: all four application deployments span both workers')


if __name__=='__main__':main()
