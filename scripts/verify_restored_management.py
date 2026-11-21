"""Verify original resource identities and schedule a NEW PVC-free proof job."""
import argparse
import json
from pathlib import Path
import re
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-dir',required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    root=Path(a.run_dir);load=lambda name:json.loads((root/name).read_text(encoding='utf-8'))
    before=load('preflight.json');result=load('result.json');restore=load('management-restore.json')
    if restore.get('api_ready') is not True:raise SystemExit('STOP: restored API proof required')
    oldnodes={n['metadata']['name']:n for n in before['nodes']['items']}
    newnodes={n['metadata']['name']:n for n in cs.get('nodes')['items']}
    claims={p['metadata']['name']:p for p in before['pvcs']['items']}
    active={c for v in before['clusters'].values() for pod in v['instances'] for c in pod['claims']}
    checks={'same_banking_namespace':cs.get('namespace',cs.NS)['metadata']['uid']==result['namespace_uid'],
        'same_banking_cluster_uids':all(cs.get('cluster',n)['metadata']['uid']==before['clusters'][n]['uid'] for n in cs.CLUSTERS),
        'same_active_pvc_uids':all(cs.get('pvc',n)['metadata']['uid']==claims[n]['metadata']['uid'] for n in active),
        'same_node_resource_uids':set(oldnodes)==set(newnodes) and all(newnodes[n]['metadata']['uid']==v['metadata']['uid'] for n,v in oldnodes.items()),
        'all_three_nodes_ready':len(newnodes)==3 and all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in newnodes.values())}
    name='dr-management-proof-'+root.name.removeprefix('bonus2b-')
    if not re.fullmatch('[a-z0-9-]{1,63}',name):raise SystemExit('STOP: unsafe job name')
    job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':cs.NS},
        'spec':{'backoffLimit':0,'activeDeadlineSeconds':180,'template':{'spec':{'restartPolicy':'Never',
            'nodeSelector':{'banking-worker':'true'},'containers':[{'name':'proof',
             'image':'ghcr.io/cloudnative-pg/postgresql:17-minimal-trixie','imagePullPolicy':'IfNotPresent',
             'command':['/bin/sh','-c','printf "management-restored\\n"; hostname']}]}}}}
    report={'created_utc':cs.utc(),'status':'DRY_RUN','checks':checks}
    if not a.apply:print(json.dumps(report,indent=2));return
    if not all(checks.values()):raise SystemExit('STOP: original resources/workers not preserved')
    cs.kb('create','-f','-',stdin=json.dumps(job))
    cs.kb('wait','--for=condition=complete','job/'+name,'--timeout=180s',timeout=200)
    obj=cs.get('job',name);pods=cs.get('pods','-l','job-name='+name)['items']
    checks['new_job_scheduled_and_executed']=len(pods)==1 and pods[0]['status']['phase']=='Succeeded' and bool(pods[0]['spec'].get('nodeName'))
    checks['proof_job_has_no_pvc']=all(not any('persistentVolumeClaim' in v for v in p['spec'].get('volumes',[])) for p in pods)
    report.update({'status':'PASS' if all(checks.values()) else 'FAIL','job':cs.sanitize(obj),
        'pod':cs.sanitize(pods[0]),'output':cs.kb('logs','job/'+name),
        'scope':'Original namespace/Cluster/PVC/Node resources retained; a fresh stateless job was admitted, scheduled and executed after management restore. Banking ledger audit is separate.'})
    cs.write_report(root/'management-verification.json',report)
    print(json.dumps({'status':report['status'],'checks':checks,'scheduled_node':pods[0]['spec']['nodeName']},indent=2))


if __name__=='__main__':main()
