"""Inject an approved permanent loss of ONE isolated worker under recorded load.

No automatic PVC removal or data promotion. Run the separately guarded recovery
and join tools while this runner records survivors. Final correctness is assessed
only by an exact baseline/client audit. Default is dry-run.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import PLAN,bios_identity,vm
from scripts.load_transactions import run as load
from scripts.preflight_assignment3 import inspect
from scripts.recover_lost_worker import plan as storage_plan


def main():
    p=argparse.ArgumentParser();p.add_argument('--destruction-plan',required=True)
    p.add_argument('--run-id',required=True);p.add_argument('--apply',action='store_true')
    p.add_argument('--approval-token');p.add_argument('--seconds',type=int,default=360)
    p.add_argument('--failure-after',type=int,default=30);a=p.parse_args()
    if not a.run_id.replace('-','').isalnum() or not 15<=a.failure_after<a.seconds<=900:p.error('safe run-id and bounded workload required')
    lab=json.loads(PLAN.read_text(encoding='utf-8'))
    reviewed=json.loads(Path(a.destruction_plan).read_text(encoding='utf-8'))
    if reviewed.get('scenario')!='II-A':raise SystemExit('STOP: worker-loss plan required')
    if Path(os.environ['KUBECONFIG']).resolve()!=Path(lab['clone_kubeconfig']).resolve():raise SystemExit('STOP: isolated kubeconfig required')
    target=reviewed['destruction'];node=cs.get('node',reviewed['node_name'])
    if Path(target['vmx']).resolve()!=Path(lab['nodes']['c']['clone_vmx']).resolve():raise SystemExit('STOP: this runner supports only the reviewed isolated C worker')
    if node['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(target['vmx']):raise SystemExit('STOP: VM/node mapping changed')
    if a.apply and a.approval_token!=target['required_token']:raise SystemExit('STOP: explicit target-specific approval required')
    out=Path('reports/assignment3')/a.run_id;out.mkdir(exist_ok=False)
    result={'created_utc':cs.utc(),'status':'FAIL','scenario':'II-A',
            'lost_node':reviewed['node_name'],'destruction_plan':a.destruction_plan,'observations':[]}
    try:
        preflight=inspect(reviewed['restore_proof']);cs.write_report(out/'preflight.json',preflight)
        if not preflight['pass']:raise RuntimeError('preflight failed; nothing destroyed')
        topology={'nodes':preflight['nodes'],'clusters':preflight['clusters'],
                  'pvcs':preflight['pvcs'],'pvs':preflight['pvs']}
        cs.write_report(out/'topology.json',topology)
        proof=json.loads(Path(reviewed['restore_proof']).read_text(encoding='utf-8'))
        if storage_plan(topology,reviewed['node_name'],proof)!=reviewed['storage_recovery_plan']['plan']:
            raise RuntimeError('STOP: approved storage identities changed')
        r=subprocess.run([sys.executable,'tests/assignment3/audit.py','--wait-recovery','180',
            '--baseline',reviewed['baseline'],'--output',str(out/'before.json')],capture_output=True,timeout=400)
        if r.returncode:raise RuntimeError('fresh pre-loss audit failed: '+r.stdout.decode(errors='replace')[-1200:])
        if not a.apply:result['status']='DRY_RUN';return
        result['approval_scope']='User explicitly approved this isolated VM/disk directory and the five UID-bound main-cluster PVCs. Control-plane deletion excluded.'
        with ThreadPoolExecutor(1) as executor:
            workload=executor.submit(load,lab['clone_gateway'],a.seconds,4,out/'load.jsonl',17,a.run_id,1)
            time.sleep(a.failure_after)
            events=[json.loads(x) for x in (out/'load.jsonl').read_text(encoding='utf-8').splitlines()]
            if not any(x.get('success') for x in events):raise RuntimeError('STOP: no acknowledged pre-loss load; nothing destroyed')
            command=['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File','scripts/destroy_isolated_vm.ps1',
                '-LabRoot',lab['root'],'-Role','c','-ExpectedRawBiosUuid',target['raw_bios_uuid'],
                '-ConfirmToken',a.approval_token,'-Apply']
            r=subprocess.run(command,capture_output=True,text=True,timeout=120)
            if r.returncode:raise RuntimeError(r.stderr[-1500:])
            result['permanent_loss']=json.loads(r.stdout)
            if Path(target['target']).exists() or target['vmx'].lower() in vm('list').lower():raise RuntimeError('physical VM/disk loss unproven')
            result['status']='PARTIAL';result['failure_utc']=result['permanent_loss']['injected_utc']
            cs.write_report(out/'result.json',result)
            print('PERMANENT_VM_LOSS_INJECTED: approved isolated worker/disks no longer exist',flush=True)
            while not workload.done():
                observation={'created_utc':cs.utc(),'clusters':{}}
                for name in cs.CLUSTERS:
                    try:observation['clusters'][name]=cs.database_cluster(name,skip_nodes=(reviewed['node_name'],))
                    except Exception as exc:observation['clusters'][name]={'error':str(exc)}
                result['observations'].append(observation);cs.write_report(out/'result.json',result)
                time.sleep(5)
            result['load']=workload.result()
            result['note']='Permanent failure and load recorded. Replacement/redundancy, exact client audit and final evidence still required; no PASS claim.'
    except Exception as exc:result['error']=str(exc);print(str(exc),file=sys.stderr)
    finally:cs.write_report(out/'result.json',result)


if __name__=='__main__':main()
