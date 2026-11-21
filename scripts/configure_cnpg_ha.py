"""Keep the CNPG controller available after either worker fails; retain strict sync."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--apply',action='store_true');a=p.parse_args()
    deployment=cs.get('deployment','cnpg-controller-manager','-n','cnpg-system')
    container=deployment['spec']['template']['spec']['containers'][0]
    if '--leader-elect' not in container.get('args',[]):raise SystemExit('STOP: operator leader election is not enabled')
    labels=deployment['spec']['selector']['matchLabels']
    patch={'spec':{'replicas':2,'template':{'spec':{'nodeSelector':{'banking-worker':'true'},
        'containers':[{'name':c['name'],'imagePullPolicy':'IfNotPresent'} for c in deployment['spec']['template']['spec']['containers']],
        'topologySpreadConstraints':[{'maxSkew':1,'minDomains':2,'matchLabelKeys':['pod-template-hash'],
            'topologyKey':'kubernetes.io/hostname','whenUnsatisfiable':'DoNotSchedule','labelSelector':{'matchLabels':labels}}]}}}}
    if not a.apply:print(json.dumps(patch,indent=2));return
    # Reuse the existing operator image offline, just as for the application.
    archive=Path('reports/assignment3/private/cnpg-operator-image.tar')
    with archive.open('wb') as stream:
        subprocess.run(['kubectl','-n',cs.NS,'exec','banking-maint-c','--','chroot','/host','ctr','-n','k8s.io',
            'images','export','-',container['image']],stdout=stream,check=True,timeout=180)
    with archive.open('rb') as stream:
        subprocess.run(['kubectl','-n',cs.NS,'exec','-i','banking-maint-b','--','chroot','/host','ctr','-n','k8s.io',
            'images','import','-'],stdin=stream,check=True,timeout=180)
    cs.write_report('reports/assignment3/cnpg-ha-change.json',{'created_utc':cs.utc(),'status':'PARTIAL',
        'operator_image':container['image'],'before':cs.sanitize(deployment),'patch':patch})
    print(cs.kb('patch','deployment','cnpg-controller-manager','-n','cnpg-system','--type=strategic','-p',json.dumps(patch)))
    for name in cs.CLUSTERS:
        print(cs.kb('patch','cluster',name,'--type=merge','-p',json.dumps({'spec':{
            'smartShutdownTimeout':0,'postgresql':{'synchronous':{'method':'any','number':2,'dataDurability':'required','failoverQuorum':True}}}})))


if __name__=='__main__':main()
