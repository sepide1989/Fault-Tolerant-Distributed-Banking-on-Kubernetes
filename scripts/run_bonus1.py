"""Evidence runner for reversible worker interruption, never VM/disk deletion.

Use --prepare first. --run requires an explicit --approval string, an already
validated restore proof, and VMX->node mapping from VMware Tools IPs. It starts
load, powers off the specified worker, restores power, then checks every ledger.
No PVC destruction or manual transaction completion exists in this runner.
"""
import argparse
import concurrent.futures
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import re
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.load_transactions import run as load
from scripts.preflight_assignment3 import inspect


def execute(*args,timeout=90):
    r=subprocess.run(list(args),capture_output=True,text=True,timeout=timeout)
    if r.returncode:raise RuntimeError(r.stderr[-1500:] or r.stdout[-1500:])
    return r.stdout.strip()


def main():
    p=argparse.ArgumentParser();p.add_argument('--lost-node',required=True)
    p.add_argument('--vmx',required=True);p.add_argument('--vmrun',required=True)
    p.add_argument('--gateway',required=True);p.add_argument('--restore-proof',required=True)
    p.add_argument('--run-id',required=True);p.add_argument('--run',action='store_true')
    p.add_argument('--approval');p.add_argument('--seconds',type=int,default=180)
    p.add_argument('--failure-after',type=int,default=30);p.add_argument('--down-seconds',type=int,default=30)
    a=p.parse_args();outdir=Path('reports/assignment3')/a.run_id
    if not a.run_id.replace('-','').isalnum():p.error('safe run-id required')
    outdir.mkdir(parents=True,exist_ok=False)
    result={'created_utc':cs.utc(),'status':'FAIL','failure':'reversible worker hard power-off','lost_node':a.lost_node,'observations':[]}
    powered_off=False
    try:
        preflight=inspect(a.restore_proof);cs.write_report(outdir/'preflight.json',preflight)
        if not preflight['pass']:raise RuntimeError('preflight failed; no VM changed')
        node=next(n for n in preflight['nodes']['items'] if n['metadata']['name']==a.lost_node)
        if 'node-role.kubernetes.io/control-plane' in node['metadata'].get('labels',{}):raise RuntimeError('refuse control-plane target')
        raw=Path(a.vmx).read_text(encoding='utf-8',errors='replace')
        bios=re.search(r'^uuid.bios\s*=\s*"([^"]+)"',raw,re.M)
        if not bios:raise RuntimeError('VMX has no stable BIOS UUID')
        value=bytes.fromhex(bios.group(1).replace('-',' '))
        identities={str(uuid.UUID(bytes=value)).lower(),str(uuid.UUID(bytes_le=value)).lower()}
        if node['status']['nodeInfo']['systemUUID'].lower() not in identities:raise RuntimeError('VMX BIOS UUID does not match selected Kubernetes worker')
        result['vmx_bios_uuid']=bios.group(1)
        result['node_system_uuid']=node['status']['nodeInfo']['systemUUID']
        audit=Path('tests/assignment3/audit.py')
        execute(sys.executable,str(audit),'--wait-recovery','180','--output',str(outdir/'before.json'),timeout=400)
        before=json.loads((outdir/'before.json').read_text(encoding='utf-8'))
        if not a.run:
            result['status']='DRY_RUN'
            result['approval_required']='POWER-OFF '+a.lost_node
            return 0
        if a.approval!='POWER-OFF '+a.lost_node:raise RuntimeError('explicit target-specific approval required')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            workload=pool.submit(load,a.gateway,a.seconds,4,outdir/'load.jsonl',7,a.run_id,1)
            time.sleep(a.failure_after)
            execute(a.vmrun,'stop',str(Path(a.vmx).resolve()),'hard');powered_off=True
            result['failure_utc']=cs.utc()
            inventory=execute(a.vmrun,'list')
            if str(Path(a.vmx).resolve()).lower() in inventory.lower():raise RuntimeError('hypervisor did not confirm power-off')
            result['hypervisor_confirmed_off']=True
            deadline=time.monotonic()+a.down_seconds
            while time.monotonic()<deadline:
                observation={'created_utc':cs.utc(),'clusters':{}}
                for name in cs.CLUSTERS:
                    try:observation['clusters'][name]=cs.database_cluster(name,skip_nodes=(a.lost_node,))
                    except Exception as exc:observation['clusters'][name]={'error':str(exc)}
                result['observations'].append(observation)
                cs.write_report(outdir/'result.json',result)
                time.sleep(3)
            execute(a.vmrun,'start',str(Path(a.vmx).resolve()),'nogui');powered_off=False
            result['power_restored_utc']=cs.utc();result['load']=workload.result()
        execute(sys.executable,str(audit),'--wait-recovery','300','--baseline',str(outdir/'before.json'),
                '--load-log',str(outdir/'load.jsonl'),'--output',str(outdir/'after.json'),timeout=600)
        # Sampling cannot prove absence of a hidden primary. Physical hypervisor
        # fencing, SQL observations and restored all-instance audit are recorded.
        if any(sum(p.get('pg',{}).get('standby') is False for p in record.get('instances',[]))>1 for obs in result['observations'] for record in obs['clusters'].values()):raise RuntimeError('multiple primary observation')
        after=json.loads((outdir/'after.json').read_text(encoding='utf-8'))
        affected=[n for n in cs.CLUSTERS if any(x['pod']==before['clusters'][n]['status']['currentPrimary'] and x['node']==a.lost_node for x in before['clusters'][n]['instances'])]
        result['checks']={'hypervisor_fenced':result.get('hypervisor_confirmed_off') is True,
            'full_ledger_audit':after['pass'],
            'actual_primary_failovers':bool(affected) and all(before['clusters'][n]['status']['currentPrimary']!=after['clusters'][n]['status']['currentPrimary'] for n in affected),
            'no_observed_dual_primary':all(sum(p.get('pg',{}).get('standby') is False for p in record.get('instances',[]))<=1 for obs in result['observations'] for record in obs['clusters'].values())}
        result['status']='PASS' if all(result['checks'].values()) else 'PARTIAL'
        result['scope']='Physical worker power-off/restart under load; physical fencing, failover observations and full request-ledger audit retained.'
    except Exception as exc:result['error']=str(exc)
    finally:
        if powered_off:
            try:execute(a.vmrun,'start',str(Path(a.vmx).resolve()),'nogui')
            except Exception as exc:result['power_restore_error']=str(exc)
        cs.write_report(outdir/'result.json',result)
    return 0 if result['status'] in ('DRY_RUN','PASS') else 2


if __name__=='__main__':raise SystemExit(main())
