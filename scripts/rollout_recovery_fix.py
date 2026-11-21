"""Review/apply an additive schema migration and controlled recovery image rollout.

Does not alter CNPG topology, WAL/archive plugins, Secrets, balances or decisions.
External Gateway writes pause during journal migration. Keep them paused on failure.
"""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--image',required=True)
    p.add_argument('--apply',action='store_true');p.add_argument('--run-id',required=True)
    a=p.parse_args()
    if not a.run_id.replace('-','').isalnum() or len(a.run_id)>24:p.error('safe run-id required')
    output=Path('reports/assignment3')/('rollout-'+a.run_id+'.json')
    result={'created_utc':cs.utc(),'status':'FAIL','image':a.image,'before':{}}
    try:
        for name in ('gateway','bank-a','bank-b','coordinator-recovery'):
            obj=cs.get('deployment',name)
            result['before'][name]={'replicas':obj['spec']['replicas'],'uid':obj['metadata']['uid'],
                'images':{c['name']:c['image'] for c in obj['spec']['template']['spec']['containers']}}
        jobname='recovery-schema-'+a.run_id
        job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':jobname,'namespace':cs.NS},
            'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,'template':{'spec':{
                'restartPolicy':'Never','containers':[{'name':'migrate','image':a.image,
                'command':['python','-m','scripts.migrate_recovery_schema'],
                'env':[{'name':'DB_USER','valueFrom':{'secretKeyRef':{'name':'bank-db-credentials','key':'username'}}},
                       {'name':'DB_PASSWORD','valueFrom':{'secretKeyRef':{'name':'bank-db-credentials','key':'password'}}}]}]}}}}
        if not a.apply:
            result['status']='DRY_RUN';result['migration_job']=job
            print(json.dumps(result,indent=2));return 0
        cs.write_report(output,result)
        cs.kb('scale','deployment/gateway','--replicas=0')
        cs.kb('wait','--for=delete','pod','-l','app=gateway','--timeout=180s',timeout=200)
        cs.kb('create','-f','-',stdin=json.dumps(job))
        cs.kb('wait','--for=condition=complete','job/'+jobname,'--timeout=600s',timeout=620)
        for name in ('bank-a','bank-b','coordinator-recovery','gateway'):
            containers=result['before'][name]['images']
            if len(containers)!=1:raise RuntimeError('review multi-container deployment before changing images: '+name)
            cs.kb('set','image','deployment/'+name,next(iter(containers))+'='+a.image)
            cs.kb('rollout','status','deployment/'+name,'--timeout=300s',timeout=320)
        cs.kb('scale','deployment/gateway','--replicas='+str(result['before']['gateway']['replicas']))
        cs.kb('rollout','status','deployment/gateway','--timeout=300s',timeout=320)
        result['status']='PARTIAL';result['note']='image rolled out; full DB audit and restore proof still required'
    except Exception as exc:result['error']=str(exc);print(str(exc),file=sys.stderr)
    finally:cs.write_report(output,result)
    return 0 if result['status']=='PARTIAL' else 2


if __name__=='__main__':raise SystemExit(main())
