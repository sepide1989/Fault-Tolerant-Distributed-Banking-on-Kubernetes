"""Restore the actual etcd/PKI archive onto the quarantined CP OS replacement.

Apply requires physical deletion evidence for the isolated CP copy. Original VMs,
bank data, worker disks and PVCs are never overwritten by this tool.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import (PLAN,PRIVATE,active_nodes,bios_identity,
    clone_address,connect_lab,isolate_network,save,ssh,ssh_args,vm)


def upload(address,path,content):
    # path is a constant controlled by this tool, never a shell-supplied value.
    r=subprocess.run(ssh_args('cp',address)+["umask 077; cat > '"+path+"'"],
        input=content,capture_output=True,timeout=120)
    if r.returncode:raise RuntimeError('private archive upload failed')


def main():
    p=argparse.ArgumentParser();p.add_argument('--failure-report',required=True)
    p.add_argument('--control-plane-backup',required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    lab=json.loads(PLAN.read_text(encoding='utf-8'));record=lab['replacements']['cp']
    proof=json.loads(Path(a.control_plane_backup).read_text(encoding='utf-8'))
    if not all(proof.get('checks',{}).get(k) is True for k in
        ('etcd_snapshot_hash_valid','off_vm_decrypt_and_restore_probe','required_pki_and_kubelet_files_present')):
        raise SystemExit('STOP: actual off-VM backup/restore probe required')
    archive=Path(proof['archive'])
    if hashlib.sha256(archive.read_bytes()).hexdigest()!=proof['archive_sha256']:raise SystemExit('STOP: archive hash differs')
    password=archive.with_name(archive.name.removesuffix('.tar.gz.enc')+'.passphrase')
    if not a.apply:print('DRY RUN: valid archived management state; prepared empty replacement required for apply');return
    failure=json.loads(Path(a.failure_report).read_text(encoding='utf-8'))
    loss=failure.get('permanent_loss',{})
    if loss.get('status')!='PERMANENT_VM_LOSS_INJECTED' or Path(loss.get('vmx','')).resolve()!=Path(lab['nodes']['cp']['clone_vmx']).resolve():raise SystemExit('STOP: wrong permanent CP-loss evidence')
    if Path(lab['nodes']['cp']['clone_vmx']).exists():raise SystemExit('STOP: original isolated CP still exists')
    if not record.get('prepared'):raise SystemExit('STOP: clean CP replacement not prepared')
    if record['clone_vmx'].lower() not in vm('list').lower():vm('start',record['clone_vmx'],'nogui')
    address=clone_address('cp',record);record['management_ip']=address;save(lab)
    empty=ssh('cp',address,'test ! -e /var/lib/etcd/member; test ! -e /etc/kubernetes/manifests/kube-apiserver.yaml; test ! -e /etc/kubernetes/pki/ca.key; echo clean')
    if empty!='clean':raise SystemExit('STOP: inherited management identity/data present')
    isolate_network('cp',{'original_ip':record['node_ip'],'node_name':record['hostname']},address,start_kubelet=False)
    stage='/var/lib/banking-dr-access/cp-recovery-input'
    ssh('cp',address,'umask 077; mkdir -p '+stage)
    upload(address,stage+'/archive.enc',archive.read_bytes())
    upload(address,stage+'/passphrase',password.read_bytes())
    upload(address,stage+'/restore.sh',Path('scripts/restore_control_plane.sh').read_bytes())
    command=f'''set -eu
umask 077
cd {stage}
openssl enc -d -aes-256-cbc -pbkdf2 -iter 200000 -in archive.enc -out bundle.tar.gz -pass file:passphrase
mkdir -p validator
tar -C validator -xzf bundle.tar.gz etcdutl
chmod 700 validator/etcdutl
export CP_BACKUP_ARCHIVE={stage}/archive.enc
export CP_BACKUP_PASSWORD_FILE={stage}/passphrase
export ETCD_NAME={record['hostname']}
export ETCD_PEER_IP={record['node_ip']}
export ETCDUTL_BIN={stage}/validator/etcdutl
export CP_FENCED_CONFIRMED={record['hostname']}
export ISOLATED_REPLACEMENT=1
bash restore.sh --apply
'''
    output=ssh('cp',address,command,timeout=240)
    record['restored']=True;lab.setdefault('active_replacements',{})['cp']='cp'
    os.environ['KUBECONFIG']=connect_lab(lab);save(lab)
    deadline=time.monotonic()+360
    while True:
        try:
            if cs.kb('get','--raw','/readyz')=='ok':break
        except Exception:pass
        if time.monotonic()>deadline:raise RuntimeError('restored API readiness deadline exceeded')
        time.sleep(3)
    deadline=time.monotonic()+180
    while True:
        node=cs.get('node',record['hostname'])
        if node['status']['nodeInfo']['systemUUID'].lower() in bios_identity(record['clone_vmx']) and any(
            c['type']=='Ready' and c['status']=='True' for c in node['status']['conditions']):break
        if time.monotonic()>deadline:raise RuntimeError('restored CP hardware identity/readiness unproven')
        time.sleep(3)
    preserved={n:cs.get('cluster',n)['metadata']['uid']==proof['original_cluster_uids'][n] for n in cs.CLUSTERS}
    report={'created_utc':cs.utc(),'status':'PARTIAL','backup':a.control_plane_backup,'restore_output':output,
        'restored_cluster_uids_match':preserved,'replacement_hardware_uuid':node['status']['nodeInfo']['systemUUID'],
        'node_uid':node['metadata']['uid'],'api_ready':True,'note':'Management state restored; actual scheduling, workers/PVCs, bank ledgers and all client acknowledgments still require validation.'}
    cs.write_report(Path(a.failure_report).parent/'management-restore.json',report)
    if not all(preserved.values()):raise RuntimeError('original banking resources not preserved')
    print('Original Kubernetes management state restored on a new isolated VM',flush=True)


if __name__=='__main__':main()
