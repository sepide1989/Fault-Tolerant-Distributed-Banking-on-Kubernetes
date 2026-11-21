"""Review the exact isolated VM/disks before asking for destructive approval."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import PLAN,bios_identity


def main():
    p=argparse.ArgumentParser();p.add_argument('--role',choices=['b','c','cp'],required=True)
    p.add_argument('--preflight',required=True);p.add_argument('--baseline',required=True)
    p.add_argument('--restore-proof',required=True)
    p.add_argument('--worker-recovery-plan');p.add_argument('--control-plane-backup');a=p.parse_args()
    plan=json.loads(PLAN.read_text());record=plan['nodes'][a.role]
    if Path(os.environ['KUBECONFIG']).resolve()!=Path(plan['clone_kubeconfig']).resolve():raise SystemExit('STOP: isolated kubeconfig required')
    preflight=json.loads(Path(a.preflight).read_text());baseline=json.loads(Path(a.baseline).read_text());proof=json.loads(Path(a.restore_proof).read_text())
    if preflight.get('pass') is not True or baseline.get('pass') is not True or proof.get('status')!='PASS':raise SystemExit('STOP: verified healthy lab and actual external restore required')
    if hashlib.sha256(Path(a.baseline).read_bytes()).hexdigest()!=proof.get('audit_sha256'):
        raise SystemExit('STOP: restore proof belongs to a different baseline')
    replacement=plan.get('replacements',{}).get('cp' if a.role=='cp' else 'worker',{})
    if not replacement.get('prepared'):raise SystemExit('STOP: clean replacement OS not prepared')
    storage_plan=None;cp_backup=None
    if a.role=='cp':
        if not a.control_plane_backup:p.error('--control-plane-backup required for control-plane destruction')
        cp_backup=json.loads(Path(a.control_plane_backup).read_text(encoding='utf-8'))
        if not all(cp_backup.get('checks',{}).get(k) is True for k in
            ('etcd_snapshot_hash_valid','off_vm_decrypt_and_restore_probe','required_pki_and_kubelet_files_present')):
            raise SystemExit('STOP: actual off-VM etcd/PKI restore probe required')
    else:
        if not a.worker_recovery_plan:p.error('--worker-recovery-plan required for worker destruction')
        storage_plan=json.loads(Path(a.worker_recovery_plan).read_text(encoding='utf-8'))
        if storage_plan.get('status')!='DRY_RUN' or storage_plan.get('plan',{}).get('lost_node')!=record['node_name']:
            raise SystemExit('STOP: wrong or missing reviewed storage recovery plan')
    node=cs.get('node',record['node_name'])
    if node['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(record['clone_vmx']):raise SystemExit('STOP: selected node/clone UUID mismatch')
    if node['status']['nodeInfo']['systemUUID'].lower()==record['source_system_uuid'].lower():raise SystemExit('STOP: original VM selected')
    vmx=Path(record['clone_vmx']);raw=re.search(r'^uuid.bios\s*=\s*"([^"]+)"',vmx.read_text(),re.M).group(1)
    output=subprocess.run(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File','scripts/destroy_isolated_vm.ps1',
        '-LabRoot',plan['root'],'-Role',a.role,'-ExpectedRawBiosUuid',raw],capture_output=True,text=True,check=True)
    destructive=json.loads(output.stdout)
    report={'created_utc':cs.utc(),'status':'PARTIAL','reason':'explicit user approval required before irreversible VM/disk deletion',
        'scenario':'II-B' if a.role=='cp' else 'II-A','node_name':record['node_name'],
        'node_uid':node['metadata']['uid'],'node_system_uuid':node['status']['nodeInfo']['systemUUID'],
        'destruction':destructive,'baseline':a.baseline,'preflight':a.preflight,'restore_proof':a.restore_proof,
        'replacement':replacement,'storage_recovery_plan':storage_plan,'control_plane_backup':a.control_plane_backup,
        'backup_bucket':plan['storage']['bucket'],'data_sources':{n:cs.database_cluster(n) for n in cs.CLUSTERS}}
    cs.write_report('reports/assignment3/bonus2-'+('b' if a.role=='cp' else 'a')+'-destruction-plan.json',report)
    print(json.dumps({k:v for k,v in report.items() if k!='data_sources'},indent=2))


if __name__=='__main__':main()
