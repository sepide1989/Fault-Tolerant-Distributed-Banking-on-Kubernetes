"""Request fresh independent Barman backups; fail on timeout or failed phase."""
import argparse
import json
from pathlib import Path
import re
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-id',required=True);p.add_argument('--timeout',type=int,default=300);a=p.parse_args()
    if not re.fullmatch('[a-z0-9-]{1,30}',a.run_id):p.error('safe run-id required')
    out={'created_utc':cs.utc(),'status':'FAIL','backups':{}}
    try:
        for cluster in cs.CLUSTERS:
            name=cluster+'-dr-'+a.run_id
            body={'apiVersion':'postgresql.cnpg.io/v1','kind':'Backup','metadata':{'name':name,'namespace':cs.NS},
                  'spec':{'cluster':{'name':cluster},'method':'plugin','pluginConfiguration':{'name':'barman-cloud.cloudnative-pg.io'}}}
            cs.kb('create','-f','-',stdin=json.dumps(body))
            out['backups'][cluster]={'name':name}
        deadline=time.monotonic()+a.timeout
        while True:
            completed=True
            for cluster,record in out['backups'].items():
                backup=cs.get('backup',record['name']);record['status']=cs.sanitize(backup.get('status',{}))
                phase=record['status'].get('phase');print(cluster,phase,flush=True)
                if phase=='failed':raise RuntimeError(cluster+': backup failed: '+record['status'].get('error',''))
                completed &= phase=='completed'
            if completed:out['status']='PASS';break
            if time.monotonic()>deadline:raise RuntimeError('Backup deadline exceeded')
            cs.write_report('reports/assignment3/backups-'+a.run_id+'.json',out);time.sleep(5)
    except Exception as exc:out['error']=str(exc)
    cs.write_report('reports/assignment3/backups-'+a.run_id+'.json',out)
    return 0 if out['status']=='PASS' else 2


if __name__=='__main__':raise SystemExit(main())
