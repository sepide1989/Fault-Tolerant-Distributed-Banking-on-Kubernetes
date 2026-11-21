"""Real off-VM encrypted etcd/PKI backup and restore probe via maintenance pods.

Uses etcdctl/etcdutl from the RUNNING etcd image, rather than guessing versions.
Does not stop etcd, workers or workloads. Never prints credentials or private keys.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def run(pod,*args,namespace='banking',data=None,binary=False):
    r=subprocess.run(['kubectl','-n',namespace,'exec',*( ['-i'] if data is not None else []),pod,'--',*args],
                     input=data,capture_output=True,timeout=180)
    if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace')[-1500:])
    return r.stdout if binary else r.stdout.decode().strip()


def put(pod,path,data):
    # Paths are generated from validated alphanumeric run IDs.
    run(pod,'bash','-c','umask 077; cat > '+path,data=data)


def main():
    p=argparse.ArgumentParser();p.add_argument('--cp-pod',default='banking-maint-cp')
    p.add_argument('--probe-pod',default='banking-maint-b');p.add_argument('--run-id',required=True);a=p.parse_args()
    if not re.fullmatch('[a-z0-9-]{1,32}',a.run_id):p.error('safe run-id required')
    out={'created_utc':cs.utc(),'status':'FAIL'}
    private=Path('reports/assignment3/private');private.mkdir(parents=True,exist_ok=True)
    try:
        nodes=cs.get('nodes')['items'];cp=next(n for n in nodes if 'node-role.kubernetes.io/control-plane' in n['metadata'].get('labels',{}))
        cpname=cp['metadata']['name'];ip=next(x['address'] for x in cp['status']['addresses'] if x['type']=='InternalIP')
        etcdpod='etcd-'+cpname
        etcd=json.loads(cs.kb('get','pod',etcdpod,'-n','kube-system','-o','json'))
        cid=etcd['status']['containerStatuses'][0]['containerID'].removeprefix('containerd://')
        tasks=run(a.cp_pod,'chroot','/host','ctr','-n','k8s.io','tasks','list')
        pid=next(line.split()[1] for line in tasks.splitlines() if line.startswith(cid+' '))
        stage='/var/tmp/banking-cp-'+a.run_id;hstage='/host'+stage
        run(a.cp_pod,'mkdir','-m','700',hstage)
        snapshot='/var/lib/etcd/banking-dr-'+a.run_id+'.db'
        opts=['--endpoints=https://127.0.0.1:2379','--cacert=/etc/kubernetes/pki/etcd/ca.crt',
              '--cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt','--key=/etc/kubernetes/pki/etcd/healthcheck-client.key']
        out['endpoint_health']=run(etcdpod,'etcdctl',*opts,'endpoint','health',namespace='kube-system')
        run(etcdpod,'etcdctl',*opts,'snapshot','save',snapshot,namespace='kube-system')
        run(a.cp_pod,'cp','/host'+snapshot,hstage+'/snapshot.db')
        run(a.cp_pod,'cp','/proc/'+pid+'/root/usr/local/bin/etcdutl',hstage+'/etcdutl')
        out['etcd_version']=run(a.cp_pod,hstage+'/etcdutl','version')
        out['snapshot_status']=json.loads(run(a.cp_pod,hstage+'/etcdutl','snapshot','status',hstage+'/snapshot.db','-w','json'))
        put(a.cp_pod,hstage+'/snapshot-status.json',json.dumps(out['snapshot_status']).encode())
        required=['etc/kubernetes','var/lib/kubelet/config.yaml','var/lib/kubelet/pki','var/lib/kubelet/kubeadm-flags.env']
        run(a.cp_pod,'tar','-C','/host','-cf',hstage+'/control-plane.tar',*required)
        version=run(a.cp_pod,'chroot','/host','kubeadm','version','-o','short')
        put(a.cp_pod,hstage+'/hostname',(cpname+'\n').encode());put(a.cp_pod,hstage+'/kubeadm-version',(version+'\n').encode())
        put(a.cp_pod,hstage+'/etcd-version',out['etcd_version'].encode())
        put(a.cp_pod,hstage+'/kubelet-version',run(a.cp_pod,'chroot','/host','kubelet','--version').encode())
        put(a.cp_pod,hstage+'/kubeadm-config.yaml',cs.kb('get','configmap','kubeadm-config','-n','kube-system','-o','yaml').encode())
        put(a.cp_pod,hstage+'/banking-state.json',cs.kb('get','clusters,pvc','-o','json').encode())
        run(a.cp_pod,'bash','-c','cd '+hstage+' && sha256sum snapshot.db control-plane.tar > SHA256SUMS')
        names=['snapshot.db','snapshot-status.json','control-plane.tar','hostname','kubeadm-version','etcd-version',
               'kubelet-version','kubeadm-config.yaml','banking-state.json','SHA256SUMS','etcdutl']
        run(a.cp_pod,'tar','-C',hstage,'-czf',hstage+'/bundle.tar.gz',*names)
        passfile=private/('control-plane-'+a.run_id+'.passphrase')
        with passfile.open('xb') as f:f.write(secrets.token_urlsafe(48).encode())
        os.chmod(passfile,0o600)
        put(a.cp_pod,hstage+'/passphrase',passfile.read_bytes())
        run(a.cp_pod,'chroot','/host','openssl','enc','-aes-256-cbc','-salt','-pbkdf2','-iter','200000',
            '-in',stage+'/bundle.tar.gz','-out',stage+'/control-plane.tar.gz.enc','-pass','file:'+stage+'/passphrase')
        archive=private/('control-plane-'+a.run_id+'.tar.gz.enc')
        archive.write_bytes(run(a.cp_pod,'cat',hstage+'/control-plane.tar.gz.enc',binary=True))
        out.update({'archive':str(archive),'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
                    'control_plane_node':cpname,'kubeadm_version':version,'off_vm_location':'Windows Host',
                    'original_cluster_uids':{n:cs.get('cluster',n)['metadata']['uid'] for n in cs.CLUSTERS}})
        probe='/var/tmp/banking-cp-probe-'+a.run_id;hprobe='/host'+probe
        run(a.probe_pod,'mkdir','-m','700',hprobe)
        put(a.probe_pod,hprobe+'/control-plane.tar.gz.enc',archive.read_bytes())
        put(a.probe_pod,hprobe+'/passphrase',passfile.read_bytes())
        put(a.probe_pod,hprobe+'/etcdutl',run(a.cp_pod,'cat',hstage+'/etcdutl',binary=True))
        run(a.probe_pod,'chmod','700',hprobe+'/etcdutl')
        env=['env','CP_BACKUP_ARCHIVE='+probe+'/control-plane.tar.gz.enc',
             'CP_BACKUP_PASSWORD_FILE='+probe+'/passphrase','ETCD_NAME='+cpname,'ETCD_PEER_IP='+ip,
             'ETCDUTL_BIN='+probe+'/etcdutl']
        script=Path('scripts/restore_control_plane.sh').read_bytes()
        out['off_vm_restore_probe']=run(a.probe_pod,'chroot','/host',*env,'bash','-s','--','--dry-run',data=script)
        out['status']='PARTIAL'
        out['checks']={'etcd_snapshot_hash_valid':True,'off_vm_decrypt_and_restore_probe':True,
                       'required_pki_and_kubelet_files_present':True,'full_control_plane_loss_experiment':False}
        # Remove only newly generated scratch paths after verified external copy.
        run(a.cp_pod,'rm','-rf','--',hstage)
        run(a.cp_pod,'rm','-f','--','/host'+snapshot)
        run(a.probe_pod,'rm','-rf','--',hprobe)
    except Exception as exc:out['error']=str(exc)
    cs.write_report('reports/assignment3/control-plane-backup-'+a.run_id+'.json',out)
    print(json.dumps({k:v for k,v in out.items() if k not in ('off_vm_restore_probe','snapshot_status')},indent=2))
    return 0 if out['status']=='PARTIAL' else 2


if __name__=='__main__':raise SystemExit(main())
