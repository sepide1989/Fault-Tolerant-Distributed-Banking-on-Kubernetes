"""Join the prepared clean worker after the approved isolated VM is truly gone.

No PVC deletion, restore from old worker disks or manual promotion. Bootstrap
token travels only in memory/SSH and a root-only guest file; reports redact it.
"""
import argparse
import base64
import json
from pathlib import Path
import re
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from scripts import cluster_state as cs
from scripts.isolated_dr_workspace import PLAN,PRIVATE,bios_identity,clone_address,isolate_network,save,ssh,ssh_args,vm


def main():
    p=argparse.ArgumentParser();p.add_argument('--failure-report',required=True)
    p.add_argument('--apply',action='store_true');a=p.parse_args()
    plan=json.loads(PLAN.read_text(encoding='utf-8'));record=plan['replacements']['worker']
    failure=json.loads(Path(a.failure_report).read_text(encoding='utf-8'))
    if failure.get('permanent_loss',{}).get('status')!='PERMANENT_VM_LOSS_INJECTED':raise SystemExit('STOP: physical loss evidence required')
    if Path(plan['nodes']['c']['clone_vmx']).exists():raise SystemExit('STOP: lost worker VM still exists')
    if not record.get('prepared'):raise SystemExit('STOP: clean replacement not prepared')
    cp=cs.get('node',plan['nodes']['cp']['node_name'])
    if cp['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(plan['nodes']['cp']['clone_vmx']):raise SystemExit('STOP: wrong control plane')
    if not a.apply:print('DRY RUN: join prepared isolated worker, no storage deletion');return
    if record['clone_vmx'].lower() not in vm('list').lower():vm('start',record['clone_vmx'],'nogui')
    address=clone_address('c',record);record['management_ip']=address;save(plan)
    check=ssh('c',address,'test ! -e /etc/kubernetes/kubelet.conf; test ! -e /var/lib/etcd/member; test -z "$(ls -A /opt/local-path-provisioner)"; echo clean')
    if check!='clean':raise SystemExit('STOP: replacement contains inherited active identity/data')
    network={'original_ip':record['node_ip'],'node_name':record['hostname']}
    isolate_network('c',network,address,start_kubelet=False)
    # Make the final application image available without an Internet registry.
    with (PRIVATE/'recovery-image.tar').open('rb') as stream:
        r=subprocess.run(ssh_args('c',address)+['ctr','-n','k8s.io','images','import','-'],stdin=stream,capture_output=True,timeout=120)
        if r.returncode:raise RuntimeError('replacement image import failed')
    cpip=plan['nodes']['cp']['management_ip']
    token=ssh('cp',cpip,'kubeadm token create --kubeconfig /etc/kubernetes/admin.conf --ttl 1h --description banking-isolated-replacement')
    if not re.fullmatch('[a-z0-9]{6}\\.[a-z0-9]{16}',token):raise RuntimeError('unexpected bootstrap token format')
    digest=ssh('cp',cpip,"openssl x509 -pubkey -in /etc/kubernetes/pki/ca.crt -noout | openssl pkey -pubin -outform DER | sha256sum | cut -d' ' -f1")
    config={'apiVersion':'kubeadm.k8s.io/v1beta4','kind':'JoinConfiguration',
        'discovery':{'bootstrapToken':{'token':token,'apiServerEndpoint':'192.168.43.128:6443','caCertHashes':['sha256:'+digest]}},
        'nodeRegistration':{'name':record['hostname'],'criSocket':'unix:///run/containerd/containerd.sock',
            'kubeletExtraArgs':[{'name':'node-ip','value':record['node_ip']}]}}
    encoded=base64.b64encode(yaml.safe_dump(config).encode()).decode()
    script=base64.b64encode(Path('scripts/join_replacement_worker.sh').read_bytes()).decode()
    command=f'''set -eu
umask 077
echo '{encoded}' | base64 -d >/var/lib/banking-dr-access/worker-join.yaml
echo '{script}' | base64 -d >/var/lib/banking-dr-access/join-worker.sh
export WORKER_JOIN_CONFIG=/var/lib/banking-dr-access/worker-join.yaml
export EXPECTED_KUBEADM_VERSION=v1.36.6
export REPLACEMENT_HOSTNAME={record['hostname']}
bash /var/lib/banking-dr-access/join-worker.sh --dry-run
bash /var/lib/banking-dr-access/join-worker.sh --apply
'''
    try:output=ssh('c',address,command,timeout=420).replace(token,'<redacted>')
    except Exception as exc:raise RuntimeError(str(exc).replace(token,'<redacted>')) from None
    cs.kb('label','node',record['hostname'],'banking-worker=true','--overwrite')
    cs.kb('wait','--for=condition=Ready','node/'+record['hostname'],'--timeout=180s',timeout=200)
    node=cs.get('node',record['hostname'])
    if node['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(record['clone_vmx']):raise RuntimeError('replacement node hardware mismatch')
    record['joined']=True;record['node_uid']=node['metadata']['uid'];save(plan)
    cs.write_report(Path(a.failure_report).parent/'replacement-join.json',{'created_utc':cs.utc(),'status':'PARTIAL',
        'node_name':record['hostname'],'new_node_uid':node['metadata']['uid'],'new_node_system_uuid':node['status']['nodeInfo']['systemUUID'],
        'output':output,'note':'Fresh worker joined; old local PV bindings, full redundancy and ledger audit still require verification.'})
    print('Fresh isolated replacement worker joined:',record['hostname'],flush=True)


if __name__=='__main__':main()
