"""Record approved permanent isolated-CP loss under a durable client workload.

Restore with restore_isolated_control_plane.py while this runner continues.
No database, worker disk or PVC is changed here. Default is dry-run.
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
import requests
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import PLAN,active_nodes,bios_identity,vm
from scripts.load_transactions import run as load
from scripts.preflight_assignment3 import inspect


def main():
    p=argparse.ArgumentParser();p.add_argument('--destruction-plan',required=True)
    p.add_argument('--run-id',required=True);p.add_argument('--apply',action='store_true')
    p.add_argument('--approval-token');p.add_argument('--seconds',type=int,default=300)
    p.add_argument('--failure-after',type=int,default=30);a=p.parse_args()
    if not a.run_id.replace('-','').isalnum() or not 15<=a.failure_after<a.seconds<=900:p.error('safe run-id and bounded workload required')
    lab=json.loads(PLAN.read_text(encoding='utf-8'));reviewed=json.loads(Path(a.destruction_plan).read_text(encoding='utf-8'))
    if reviewed.get('scenario')!='II-B':raise SystemExit('STOP: control-plane-loss plan required')
    if Path(os.environ['KUBECONFIG']).resolve()!=Path(lab['clone_kubeconfig']).resolve():raise SystemExit('STOP: isolated kubeconfig required')
    target=reviewed['destruction'];node=cs.get('node',reviewed['node_name'])
    if Path(target['vmx']).resolve()!=Path(lab['nodes']['cp']['clone_vmx']).resolve():raise SystemExit('STOP: wrong isolated CP target')
    if node['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(target['vmx']):raise SystemExit('STOP: CP VM/node mapping changed')
    if a.apply and a.approval_token!=target['required_token']:raise SystemExit('STOP: exact CP-specific approval required')
    out=Path('reports/assignment3')/a.run_id;out.mkdir(exist_ok=False)
    gateway='http://'+active_nodes(lab)['b']['management_ip']+':30080'
    result={'created_utc':cs.utc(),'status':'FAIL','scenario':'II-B','gateway':gateway,
        'destruction_plan':a.destruction_plan,'control_plane_backup':reviewed['control_plane_backup'],
        'namespace_uid':cs.get('namespace',cs.NS)['metadata']['uid'],'observations':[]}
    try:
        preflight=inspect(reviewed['restore_proof']);cs.write_report(out/'preflight.json',preflight)
        if not preflight['pass']:raise RuntimeError('preflight failed; nothing destroyed')
        r=subprocess.run([sys.executable,'tests/assignment3/audit.py','--wait-recovery','180',
            '--baseline',reviewed['baseline'],'--output',str(out/'before.json')],capture_output=True,timeout=400)
        if r.returncode:raise RuntimeError('fresh pre-loss audit failed')
        if not a.apply:result['status']='DRY_RUN';return
        result['approval_scope']='User explicitly approved deletion of this isolated CP VM and disks only. Worker data/PVCs/original VMs excluded.'
        with ThreadPoolExecutor(1) as executor:
            workload=executor.submit(load,gateway,a.seconds,4,out/'load.jsonl',29,a.run_id,1)
            time.sleep(a.failure_after)
            events=[json.loads(x) for x in (out/'load.jsonl').read_text(encoding='utf-8').splitlines()]
            if not any(x.get('success') for x in events):raise RuntimeError('STOP: no acknowledged pre-loss load; nothing destroyed')
            r=subprocess.run(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File','scripts/destroy_isolated_vm.ps1',
                '-LabRoot',lab['root'],'-Role','cp','-ExpectedRawBiosUuid',target['raw_bios_uuid'],
                '-ConfirmToken',a.approval_token,'-Apply'],capture_output=True,text=True,timeout=120)
            if r.returncode:raise RuntimeError(r.stderr[-1500:])
            result['permanent_loss']=json.loads(r.stdout)
            if Path(target['target']).exists() or target['vmx'].lower() in vm('list').lower():raise RuntimeError('permanent CP/disk loss unproven')
            result['failure_utc']=result['permanent_loss']['injected_utc'];result['status']='PARTIAL'
            cs.write_report(out/'result.json',result)
            print('PERMANENT_CONTROL_PLANE_LOSS_INJECTED: isolated CP/disks no longer exist',flush=True)
            session=requests.Session();session.trust_env=False
            while not workload.done():
                inventory=vm('list').lower()
                observation={'created_utc':cs.utc(),'both_worker_vms_running':all(
                    active_nodes(lab)[role]['clone_vmx'].lower() in inventory for role in ['b','c'])}
                try:
                    response=session.get(gateway+'/health',timeout=3)
                    observation['gateway_health_http']=response.status_code
                except requests.RequestException as exc:observation['gateway_health_error']=type(exc).__name__
                result['observations'].append(observation);cs.write_report(out/'result.json',result);time.sleep(5)
            session.close();result['load']=workload.result()
            result['note']='Physical CP loss recorded. Management restore, original resources/PVCs, new scheduling and full request-ledger audit still required.'
    except Exception as exc:result['error']=str(exc);print(str(exc),file=sys.stderr)
    finally:cs.write_report(out/'result.json',result)


if __name__=='__main__':main()
