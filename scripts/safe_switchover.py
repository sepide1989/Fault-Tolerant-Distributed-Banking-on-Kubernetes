"""CNPG planned switchover using the same status update as its promote plugin."""
import argparse
import json
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--cluster',required=True);p.add_argument('--target',required=True)
    p.add_argument('--apply',action='store_true');a=p.parse_args()
    cluster=cs.get('cluster',a.cluster);state=cs.database_cluster(a.cluster)
    if cluster['status']['currentPrimary']==a.target:
        print('Already primary:',a.target);return
    candidate=next(x for x in state['instances'] if x['pod']==a.target)
    if not candidate['ready'] or not candidate['pg']['standby'] or len(state['instances'])!=3 or not all(x['ready'] and 'pg' in x for x in state['instances']):raise SystemExit('STOP: all three instances must be healthy; target must be standby')
    if a.apply:
        cluster=cs.get('cluster',a.cluster)
        patch=[{'op':'test','path':'/metadata/resourceVersion','value':cluster['metadata']['resourceVersion']},
               {'op':'replace','path':'/status/targetPrimary','value':a.target},
               {'op':'replace','path':'/status/targetPrimaryTimestamp','value':cs.utc()}]
        print(cs.kb('patch','cluster',a.cluster,'--subresource=status','--type=json','-p',json.dumps(patch)))
        deadline=time.monotonic()+150
        while True:
            now=cs.get('cluster',a.cluster)
            if now['status'].get('currentPrimary')==a.target and now['status'].get('readyInstances')==3:break
            if time.monotonic()>deadline:raise SystemExit('switchover deadline exceeded')
            time.sleep(2)
        cs.write_report('reports/assignment3/switchover-'+a.target+'.json',{'created_utc':cs.utc(),'status':'PASS',
            'before':state,'after':cs.database_cluster(a.cluster)})
    else:print('DRY RUN',cluster['status']['currentPrimary'],'->',a.target)


if __name__=='__main__':main()
