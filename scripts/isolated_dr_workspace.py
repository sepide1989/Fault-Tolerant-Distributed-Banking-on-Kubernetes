"""Prepare full VMware DR copies on VMnet1; originals are never destroyed.

Stages: clone (originals remain online), boot (originals temporarily stopped),
restore-originals (park lab, restore original gateway). Default is dry-run.
No disk/VM deletion is implemented here. The clone boot guard prevents copied
workers from joining the wrong cluster before network isolation is configured.
"""
import argparse
import base64
import hashlib
import gzip
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from scripts import cluster_state as cs

VMRUN=r'C:\Program Files\VMware\VMware Workstation\vmrun.exe'
VMDIR=Path.home()/'Documents'/'Virtual Machines'
SOURCES={'cp':VMDIR/'Ubuntu 64-bit'/'Ubuntu 64-bit.vmx',
         'b':VMDIR/'ServerB'/'ServerB.vmx','c':VMDIR/'ServerC'/'ServerC.vmx'}
IPS={'cp':'192.168.43.128','b':'192.168.43.130','c':'192.168.43.131'}
PRIVATE=Path('reports/assignment3/private').resolve()
PLAN=Path(os.environ.get('BANKING_DR_PLAN',str(PRIVATE/'isolated-dr-plan.json'))).resolve()
if not PLAN.is_relative_to(PRIVATE):raise ValueError('BANKING_DR_PLAN must stay under ignored reports/assignment3/private/')


def command(*args,timeout=300,stdin=None):
    r=subprocess.run(list(map(str,args)),input=stdin,capture_output=True,timeout=timeout)
    if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace')[-1200:] or r.stdout.decode(errors='replace')[-1200:])
    return r.stdout.decode(errors='replace').strip()


def vm(*args,timeout=300):return command(VMRUN,*args,timeout=timeout)


def bios_identity(path,allow_missing=False):
    text=Path(path).read_text(encoding='utf-8',errors='replace')
    match=re.search(r'^uuid.bios\s*=\s*"([^"]+)"',text,re.M)
    if not match:
        if allow_missing:return set()
        raise RuntimeError('VMware has not assigned a BIOS UUID yet')
    raw=match.group(1)
    value=bytes.fromhex(raw.replace('-',' '))
    return {str(uuid.UUID(bytes=value)).lower(),str(uuid.UUID(bytes_le=value)).lower()}


def ssh_args(role,address):
    return ['ssh','-p','2222','-i',str(PRIVATE/'isolated-dr-key'),'-o','BatchMode=yes',
            '-o','ConnectTimeout=6','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes',
            '-o','UserKnownHostsFile="'+str(PRIVATE/'isolated-dr-known-hosts')+'"',
            '-o','HostKeyAlias=banking-dr-'+role,'root@'+address]


def ssh(role,address,script,timeout=180):
    return command(*ssh_args(role,address),'bash','-s',stdin=script.encode(),timeout=timeout)


def save(plan):PLAN.write_text(json.dumps(plan,indent=2),encoding='utf-8')


def active_nodes(plan):
    result=dict(plan['nodes'])
    for role,key in plan.get('active_replacements',{}).items():
        replacement=plan['replacements'][key]
        replacement['original_ip']=replacement['node_ip']
        replacement['node_name']=replacement['hostname']
        result[role]=replacement
    return result


def configure_vmx(path,role):
    text=path.read_text(encoding='utf-8',errors='strict')
    def setvalue(key,value):
        nonlocal text
        pattern=r'^'+re.escape(key)+r'\s*=.*$'
        line=key+' = "'+str(value)+'"'
        text=re.sub(pattern,lambda _:line,text,flags=re.M) if re.search(pattern,text,re.M) else text+'\n'+line
    setvalue('ethernet0.connectionType','custom');setvalue('ethernet0.vnet','VMnet1')
    setvalue('ethernet0.startConnected','TRUE')
    for name in re.findall(r'^(ethernet[1-9][0-9]*)\.present\s*=\s*"TRUE"',text,re.M):setvalue(name+'.present','FALSE')
    setvalue('memsize',4096);setvalue('numvcpus',2)
    setvalue('displayName','banking-dr-isolated-'+role)
    setvalue('uuid.action','create')
    setvalue('isolation.tools.hgfs.disable','TRUE')
    for name in re.findall(r'^(sharedFolder[0-9]+)\.enabled\s*=',text,re.M):setvalue(name+'.enabled','FALSE')
    path.write_text(text,encoding='utf-8')
    # A full clone must have no disk reference outside its own directory.
    for name in re.findall(r'^\S+\.fileName\s*=\s*"([^"]+\.vmdk)"',text,re.M|re.I):
        resolved=(path.parent/name).resolve()
        if not resolved.is_relative_to(path.parent.resolve()):raise RuntimeError('clone disk refers outside lab directory')


def copy_bucket(run_id,existing=False):
    import boto3
    from botocore.config import Config
    stores=[cs.get('objectstore.barmancloud.cnpg.io',n+'-store') for n in cs.CLUSTERS]
    credentials=stores[0]['spec']['configuration']['s3Credentials']
    def read(ref):return base64.b64decode(cs.get('secret',ref['name'])['data'][ref['key']]).decode()
    client=boto3.client('s3',endpoint_url='http://127.0.0.1:9000',
        aws_access_key_id=read(credentials['accessKeyId']),aws_secret_access_key=read(credentials['secretAccessKey']),
        region_name='us-east-1',config=Config(proxies={},connect_timeout=5,read_timeout=30,retries={'max_attempts':2}))
    bucket='banking-dr-'+run_id
    exists=any(b['Name']==bucket for b in client.list_buckets()['Buckets'])
    if exists and not existing:raise RuntimeError('lab bucket already exists; choose a new run ID')
    source=stores[0]['spec']['configuration']['destinationPath'].removeprefix('s3://').split('/')[0]
    if not exists:client.create_bucket(Bucket=bucket)
    objects=0;size=0
    destination={}
    if exists:
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket):
            destination.update({x['Key']:x['ETag'] for x in page.get('Contents',[])})
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=source):
        for obj in page.get('Contents',[]):
            if obj['Key'] in destination:
                if destination[obj['Key']]!=obj['ETag']:
                    # Multipart-upload ETags differ from single CopyObject ETags.
                    # Compare actual content, including gzip CRC verification.
                    def digest(bucket_name):
                        body=client.get_object(Bucket=bucket_name,Key=obj['Key'])['Body']
                        stream=gzip.GzipFile(fileobj=body) if obj['Key'].endswith('.gz') else body
                        h=hashlib.sha256()
                        while chunk:=stream.read(1024*1024):h.update(chunk)
                        body.close();return h.hexdigest()
                    if digest(source)!=digest(bucket):raise RuntimeError('backup content changed; refuse overwrite: '+obj['Key'])
                objects+=1;size+=obj['Size'];continue
            response=client.copy_object(Bucket=bucket,Key=obj['Key'],CopySource={'Bucket':source,'Key':obj['Key']})
            if response['ResponseMetadata']['HTTPStatusCode']!=200:raise RuntimeError('object copy failed')
            objects+=1;size+=obj['Size']
    return {'bucket':bucket,'source_bucket':source,'copied_objects':objects,'copied_bytes':size}


def clone_stage(a):
    baseline=json.loads(Path(a.baseline).read_text(encoding='utf-8'))
    if baseline.get('pass') is not True:raise RuntimeError('passing source baseline required')
    nodes=cs.get('nodes')['items']
    root=VMDIR/('Banking_DR_Isolated_'+a.run_id)
    plan={'created_utc':cs.utc(),'run_id':a.run_id,'root':str(root),'status':'DRY_RUN',
          'baseline':str(Path(a.baseline).resolve()),'source_kubeconfig':str(Path(os.environ['KUBECONFIG']).resolve()),
          'nodes':{},'gateway_replicas':cs.get('deployment','gateway')['spec']['replicas']}
    for role,path in SOURCES.items():
        node=next(n for n in nodes if any(x['address']==IPS[role] for x in n['status']['addresses']))
        if node['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(path):raise RuntimeError('source VM/node UUID mismatch')
        plan['nodes'][role]={'source_vmx':str(path),'node_name':node['metadata']['name'],
            'source_system_uuid':node['status']['nodeInfo']['systemUUID'],
            'clone_vmx':str(root/role/('banking-dr-'+role+'.vmx')),'original_ip':IPS[role]}
    if not a.apply:print(json.dumps(plan,indent=2));return
    if PLAN.exists():
        previous=json.loads(PLAN.read_text())
        if previous['run_id']!=a.run_id or any(Path(x['clone_vmx']).exists() for x in previous['nodes'].values()):
            raise RuntimeError('existing VM copies require explicit resume review')
        plan=previous
    else:
        if root.exists():raise RuntimeError('workspace exists without an ownership plan')
        root.mkdir();save(plan)
    cs.kb('scale','deployment/gateway','--replicas=0')
    cs.kb('wait','--for=delete','pod','-l','app=gateway','--timeout=180s',timeout=200)
    try:
        for n in cs.CLUSTERS:
            pod=cs.get('cluster',n)['status']['currentPrimary']
            cs.sql_pod(pod,'SELECT pg_switch_wal()')
        # All source writers are paused, then copy completed external objects.
        time.sleep(8)
        plan['storage']=copy_bucket(a.run_id,existing='storage' in plan);save(plan)
        # Workstation vmrun requires a powered-off source even with -snapshot.
        # Park the quiescent originals together; cold full copies avoid further
        # memory snapshot pauses and retain their whole independent disk chains.
        for role in ('cp','b','c'):vm('stop',plan['nodes'][role]['source_vmx'],'hard')
        plan['originals_parked']=True;plan['copy_mode']='powered-off full clone';save(plan)
        for role,record in plan['nodes'].items():
            target=Path(record['clone_vmx']);target.parent.mkdir(exist_ok=True)
            vm('clone',record['source_vmx'],target,'full','-cloneName=banking-dr-isolated-'+role,timeout=900)
            configure_vmx(target,role)
            record['clone_bios_identities']=sorted(bios_identity(target,allow_missing=True))
            if record['source_system_uuid'].lower() in record['clone_bios_identities']:raise RuntimeError('clone retained original hardware identity')
            save(plan);print('Created independent VMnet1 full clone:',role,flush=True)
        plan['status']='COPIES_CREATED';save(plan)
    finally:
        if not plan.get('originals_parked'):
            cs.kb('scale','deployment/gateway','--replicas='+str(plan['gateway_replicas']))


def resume_clone_stage(a):
    plan=json.loads(PLAN.read_text())
    if not plan.get('originals_parked'):raise RuntimeError('originals must be parked before cold clone resume')
    running=vm('list').lower()
    if any(x['source_vmx'].lower() in running or x['clone_vmx'].lower() in running for x in plan['nodes'].values()):
        raise RuntimeError('source/copy is running; refuse cold clone resume')
    if not a.apply:print('DRY RUN: finish remaining independent cold copies');return
    for role,record in plan['nodes'].items():
        if record['source_system_uuid'].lower() not in bios_identity(record['source_vmx']):raise RuntimeError('source hardware identity changed')
        target=Path(record['clone_vmx']);target.parent.mkdir(exist_ok=True)
        if not target.exists():vm('clone',record['source_vmx'],target,'full','-cloneName=banking-dr-isolated-'+role,timeout=900)
        configure_vmx(target,role)
        record['clone_bios_identities']=sorted(bios_identity(target,allow_missing=True))
        if record['source_system_uuid'].lower() in record['clone_bios_identities']:raise RuntimeError('copy retained source identity')
        save(plan);print('Independent cold copy ready:',role,flush=True)
    plan['status']='COPIES_CREATED';save(plan)


def clone_address(role,record,timeout=240):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        text=Path(record['clone_vmx']).read_text(encoding='utf-8')
        match=re.search(r'^ethernet0.generatedAddress\s*=\s*"([^"]+)"',text,re.M)
        if match:
            mac=match.group(1).lower()
            leases=Path(r'C:\ProgramData\VMware\vmnetdhcp.leases').read_text(errors='replace')
            candidates=[ip for ip,body in re.findall(r'lease\s+(192\.168\.3\.\d+)\s*{([^}]+)}',leases) if mac in body.lower()]
            for address in reversed(candidates):
                try:
                    identity=ssh(role,address,'cat /sys/class/dmi/id/product_uuid',timeout=10).lower()
                    if identity in bios_identity(record['clone_vmx']):return address
                except Exception:pass
        time.sleep(3)
    raise RuntimeError('clone SSH/DHCP did not become ready: '+role)


def isolate_network(role,record,address,start_kubelet=True):
    raw=json.loads(ssh(role,address,'ip -j -4 address show'))
    interface=next(x['ifname'] for x in raw if any(y.get('local')==address for y in x['addr_info']))
    if not re.fullmatch('[a-zA-Z0-9_.-]+',interface):raise RuntimeError('unexpected interface')
    original=record['original_ip'];hostname=record['node_name']
    script=f'''set -eu
systemctl stop kubelet
hostnamectl set-hostname {hostname}
install -d /var/lib/banking-dr-access /etc/systemd/system/kubelet.service.d
cat >/usr/local/sbin/banking-dr-network <<'NETWORK'
#!/bin/sh
set -eu
ip -4 address show dev {interface} | grep -q 'inet {original}/24' || ip address add {original}/24 dev {interface}
# Linux needs an initial route before OUTPUT service DNAT. VMnet1 DHCP has
# no router; without this route the copied API works but ClusterIP is unreachable.
ip route replace default via 192.168.3.1 dev {interface} onlink metric 900
iptables -C OUTPUT -d 192.168.43.1 -p tcp --dport 9000 -j REJECT 2>/dev/null || iptables -I OUTPUT -d 192.168.43.1 -p tcp --dport 9000 -j REJECT
NETWORK
chmod 700 /usr/local/sbin/banking-dr-network
cat >/etc/systemd/system/banking-dr-network.service <<'UNIT'
[Unit]
Wants=network-online.target
After=network-online.target sys-subsystem-net-devices-{interface}.device
Requires=sys-subsystem-net-devices-{interface}.device
Before=kubelet.service
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/banking-dr-network
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
UNIT
printf '[Unit]\\nRequires=banking-dr-network.service\\nAfter=banking-dr-network.service\\n' >/etc/systemd/system/kubelet.service.d/30-banking-dr-network.conf
printf 'KUBELET_EXTRA_ARGS="--node-ip={original}"\\n' >/etc/default/kubelet
systemctl daemon-reload
systemctl enable --now banking-dr-network.service
touch /var/lib/banking-dr-access/isolated-kubelet-approved
{'systemctl restart kubelet' if start_kubelet else ':'}
'''
    ssh(role,address,script)


def connect_lab(plan):
    # VMnet1 is reachable directly from the host. Preserve CA and SAN checking
    # against the original certificate; no insecure TLS or host route changes.
    address=active_nodes(plan)['cp']['management_ip']
    config=yaml.safe_load(Path(plan['source_kubeconfig']).read_text())
    context=next(x for x in config['contexts'] if x['name']==config['current-context'])
    cluster=next(x for x in config['clusters'] if x['name']==context['context']['cluster'])
    cluster['cluster']['server']='https://'+address+':6443';cluster['cluster']['tls-server-name']='192.168.43.128'
    path=PRIVATE/'isolated-kubeconfig.yaml';path.write_text(yaml.safe_dump(config),encoding='utf-8')
    plan['clone_kubeconfig']=str(path);plan['clone_gateway']='http://'+address+':30080';save(plan)
    return str(path)


def boot_stage(a):
    plan=json.loads(PLAN.read_text());print('Original VMs will be parked; isolated copies use VMnet1',flush=True)
    if not a.apply:return
    os.environ['KUBECONFIG']=plan['source_kubeconfig']
    if not plan.get('originals_parked'):
        cs.kb('scale','deployment/gateway','--replicas=0')
        cs.kb('wait','--for=delete','pod','-l','app=gateway','--timeout=180s',timeout=200)
        for role in ('cp','b','c'):vm('stop',plan['nodes'][role]['source_vmx'],'hard')
    plan['originals_parked']=True;save(plan)
    for role,record in active_nodes(plan).items():
        if record['clone_vmx'].lower() not in vm('list').lower():vm('start',record['clone_vmx'],'nogui')
        address=clone_address(role,record);record['management_ip']=address;save(plan)
        isolate_network(role,record,address);print('Isolated clone configured:',role,address,flush=True)
    os.environ['KUBECONFIG']=connect_lab(plan)
    deadline=time.monotonic()+300
    while True:
        try:
            nodes=cs.get('nodes')['items']
            if len(nodes)==3 and all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes):break
        except Exception:pass
        if time.monotonic()>deadline:raise RuntimeError('isolated cluster API/nodes not ready')
        time.sleep(4)
    cs.kb('set','env','daemonset/calico-node','-n','kube-system','IP_AUTODETECTION_METHOD=kubernetes-internal-ip')
    cs.kb('rollout','status','daemonset/calico-node','-n','kube-system','--timeout=180s',timeout=200)
    operator=cs.get('deployment','cnpg-controller-manager','-n','cnpg-system')
    cs.kb('patch','deployment','cnpg-controller-manager','-n','cnpg-system','--type=strategic','-p',
          json.dumps({'spec':{'template':{'spec':{'containers':[{'name':c['name'],'imagePullPolicy':'IfNotPresent'}
          for c in operator['spec']['template']['spec']['containers']]}}}}))
    cs.kb('rollout','status','deployment/cnpg-controller-manager','-n','cnpg-system','--timeout=180s',timeout=200)
    for name in cs.CLUSTERS:
        store=cs.get('objectstore.barmancloud.cnpg.io',name+'-store')
        cfg=store['spec']['configuration'];cfg['destinationPath']=cfg['destinationPath'].replace('s3://'+plan['storage']['source_bucket']+'/', 's3://'+plan['storage']['bucket']+'/')
        cfg['endpointURL']='http://192.168.3.1:9000'
        cs.kb('patch','objectstore.barmancloud.cnpg.io',name+'-store','--type=merge','-p',json.dumps({'spec':{'configuration':cfg}}))
    for n in cs.CLUSTERS:cs.kb('wait','--for=condition=Ready','cluster/'+n,'--timeout=300s',timeout=320)
    # Verify physical isolation/identity from the API, not only VM display names.
    for node in cs.get('nodes')['items']:
        record=next(x for x in active_nodes(plan).values() if x['node_name']==node['metadata']['name'])
        if node['status']['nodeInfo']['systemUUID'].lower() not in bios_identity(record['clone_vmx']):raise RuntimeError('API connected to original rather than isolated VM')
    cs.kb('scale','deployment/gateway','--replicas='+str(plan['gateway_replicas']))
    cs.kb('rollout','status','deployment/gateway','--timeout=180s',timeout=200)
    plan['status']='ISOLATED_CLUSTER_READY';save(plan)
    print('Isolated banking cluster ready; original VM files are intact',flush=True)


def originals_stage(a):
    plan=json.loads(PLAN.read_text())
    if not a.apply:print('DRY RUN: park clones and start original VMs');return
    # Direct VMnet1 access has no tunnel to stop. Never kill an old saved PID:
    # Windows can reuse it for an unrelated process after a long experiment.
    to_park=[active_nodes(plan)['cp']]+list(plan['nodes'].values())+list(plan.get('replacements',{}).values())
    for record in to_park:
        if record['clone_vmx'].lower() in vm('list').lower():vm('stop',record['clone_vmx'],'hard')
    for role in ('cp','b','c'):
        record=plan['nodes'][role]
        if record['source_vmx'].lower() not in vm('list').lower():vm('start',record['source_vmx'],'nogui')
    os.environ['KUBECONFIG']=plan['source_kubeconfig'];deadline=time.monotonic()+360
    while True:
        try:
            nodes=cs.get('nodes')['items']
            if len(nodes)==3 and all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes):break
        except Exception:pass
        if time.monotonic()>deadline:raise RuntimeError('original node recovery deadline exceeded')
        time.sleep(4)
    cs.kb('scale','deployment/gateway','--replicas='+str(plan['gateway_replicas']))
    cs.kb('rollout','status','deployment/gateway','--timeout=180s',timeout=200)
    plan['originals_parked']=False;plan['status']='LAB_PARKED_ORIGINALS_RUNNING';save(plan)


def main():
    p=argparse.ArgumentParser();p.add_argument('stage',choices=['clone','resume-clone','boot','restore-originals'])
    p.add_argument('--run-id',default='20261115-lab1');p.add_argument('--baseline');p.add_argument('--apply',action='store_true');a=p.parse_args()
    if not re.fullmatch('[a-z0-9-]{1,30}',a.run_id):p.error('safe run-id required')
    if a.stage=='clone' and not a.baseline:p.error('--baseline required')
    {'clone':clone_stage,'resume-clone':resume_clone_stage,'boot':boot_stage,'restore-originals':originals_stage}[a.stage](a)


if __name__=='__main__':main()
