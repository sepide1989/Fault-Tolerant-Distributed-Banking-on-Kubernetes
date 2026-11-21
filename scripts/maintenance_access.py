"""Create temporary node-bound maintenance pods using the existing PG image.

Allows kubeadm/etcd backup and offline image import when SSH is unavailable.
No host file changes occur at creation; all host commands are explicit afterward.
"""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--node',required=True);p.add_argument('--name',required=True)
    p.add_argument('--apply',action='store_true');a=p.parse_args()
    cs.get('node',a.node)
    pod={'apiVersion':'v1','kind':'Pod','metadata':{'name':a.name,'namespace':cs.NS,'labels':{'purpose':'banking-maintenance'}},
        'spec':{'nodeName':a.node,'hostPID':True,'hostNetwork':True,'restartPolicy':'Never','activeDeadlineSeconds':7200,
                'tolerations':[{'operator':'Exists'}],
                'containers':[{'name':'maintenance','image':'ghcr.io/cloudnative-pg/postgresql:17-minimal-trixie',
                   'imagePullPolicy':'IfNotPresent','command':['/bin/bash','-c','sleep 7200'],
                   'securityContext':{'privileged':True,'runAsUser':0},
                   'volumeMounts':[{'name':'host','mountPath':'/host','mountPropagation':'HostToContainer'}]}],
                'volumes':[{'name':'host','hostPath':{'path':'/','type':'Directory'}}]}}
    if a.apply:
        cs.kb('apply','-f','-',stdin=json.dumps(pod))
        cs.kb('wait','--for=condition=Ready','pod/'+a.name,'--timeout=180s',timeout=200)
        print(a.name,'ready on',a.node)
    else:print(json.dumps(pod,indent=2))


if __name__=='__main__':main()
