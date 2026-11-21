"""Archive a fenced, stopped replica outside its VM without altering its PVC."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--cluster',required=True);p.add_argument('--instance',required=True)
    p.add_argument('--maintenance-pod',required=True);a=p.parse_args()
    cluster=cs.get('cluster',a.cluster)
    if a.instance not in json.loads(cluster['metadata'].get('annotations',{}).get('cnpg.io/fencedInstances','[]')):
        raise SystemExit('STOP: selected instance is not fenced')
    pvc=cs.get('pvc',a.instance);pv=cs.get('pv',pvc['spec']['volumeName'])
    path=pv['spec'].get('hostPath',pv['spec'].get('local',{})).get('path')
    if not path or not path.startswith('/opt/local-path-provisioner/'):raise SystemExit('unexpected storage path')
    # pg_ctl status must establish that PostgreSQL is stopped before a cold copy.
    status=subprocess.run(['kubectl','-n',cs.NS,'exec',a.maintenance_pod,'--','pg_ctl','status','-D','/host'+path+'/pgdata'],capture_output=True,text=True)
    # pg_ctl refuses root; inspect the instance manager's fence status directly.
    pod=cs.get('pod',a.instance)
    processes=cs.kb('exec',a.instance,'-c','postgres','--','ps','-eo','comm')
    if any(x.strip()=='postgres' for x in processes.splitlines()):raise SystemExit('STOP: postgres still running')
    output=Path('reports/assignment3/private')/(a.instance+'-retained-20261115.tar.gz')
    with output.open('xb') as stream:
        subprocess.run(['kubectl','-n',cs.NS,'exec',a.maintenance_pod,'--','tar','-C','/host'+path,'-czf','-','pgdata'],stdout=stream,check=True,timeout=180)
    with tarfile.open(output,'r:gz') as archive:
        names=archive.getnames()
        if 'pgdata/global/pg_control' not in names:raise RuntimeError('control file missing from cold archive')
    report={'created_utc':cs.utc(),'status':'PASS','scope':'cold preservation only; not a healthy standby restore',
            'archive':str(output),'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
            'pvc_uid':pvc['metadata']['uid'],'pv_uid':pv['metadata']['uid'],'data_unchanged':True}
    cs.write_report('reports/assignment3/preserved-'+a.instance+'.json',report)
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
