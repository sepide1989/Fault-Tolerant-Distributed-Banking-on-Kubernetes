"""Temporary key-only SSH access needed to boot isolated VMware DR copies.

Private key stays under ignored private/. The isolated copies use VMnet1 and
SSH tunnels; originals keep their network configuration. No passwords changed.
"""
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.backup_control_plane_kubectl import run,put

NODES={'cp':('banking-maint-cp','192.168.43.128'),
       'b':('banking-maint-b','192.168.43.130'),'c':('banking-maint-c','192.168.43.131')}


def main():
    private=Path('reports/assignment3/private');private.mkdir(parents=True,exist_ok=True)
    key=private/'isolated-dr-key'
    if not key.exists():subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(key)],check=True)
    public=key.with_suffix('.pub').read_bytes()
    known=[]
    for role,(pod,address) in NODES.items():
        run(pod,'chroot','/host','env','DEBIAN_FRONTEND=noninteractive','apt-get','--no-install-recommends',
            '-o','Acquire::Retries=1','-o','Acquire::http::Timeout=10','install','-y','openssh-server')
        run(pod,'mkdir','-p','/host/var/lib/banking-dr-access','/host/run/sshd')
        run(pod,'chmod','700','/host/var/lib/banking-dr-access')
        put(pod,'/host/var/lib/banking-dr-access/authorized_keys',public)
        config='''Port 2222
ListenAddress 0.0.0.0
HostKey /etc/ssh/ssh_host_ed25519_key
PidFile /run/banking-dr-sshd.pid
AuthorizedKeysFile /var/lib/banking-dr-access/authorized_keys
PermitRootLogin prohibit-password
AuthenticationMethods publickey
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM yes
AllowUsers root
AllowTcpForwarding yes
X11Forwarding no
'''
        put(pod,'/host/var/lib/banking-dr-access/sshd_config',config.encode())
        unit='''[Unit]
Description=Temporary isolated banking DR access
After=network.target
[Service]
ExecStart=/usr/sbin/sshd -D -f /var/lib/banking-dr-access/sshd_config
RuntimeMaxSec=10800
[Install]
WantedBy=multi-user.target
'''
        put(pod,'/host/etc/systemd/system/banking-dr-access.service',unit.encode())
        run(pod,'chroot','/host','ssh-keygen','-A')
        hostkey=run(pod,'cat','/host/etc/ssh/ssh_host_ed25519_key.pub').split()
        known.append('banking-dr-'+role+' '+' '.join(hostkey[:2]))
        run(pod,'chroot','/host','systemctl','daemon-reload')
        run(pod,'chroot','/host','systemctl','enable','--now','banking-dr-access.service')
        original_uuid=run(pod,'cat','/host/sys/class/dmi/id/product_uuid').lower()
        guard='''#!/bin/sh
set -eu
actual=$(tr 'A-Z' 'a-z' </sys/class/dmi/id/product_uuid)
[ "$actual" = "ORIGINAL_UUID" ] || [ -e /var/lib/banking-dr-access/isolated-kubelet-approved ]
'''.replace('ORIGINAL_UUID',original_uuid)
        put(pod,'/host/usr/local/sbin/banking-dr-clone-guard',guard.encode())
        run(pod,'chmod','700','/host/usr/local/sbin/banking-dr-clone-guard')
        run(pod,'mkdir','-p','/host/etc/systemd/system/kubelet.service.d')
        put(pod,'/host/etc/systemd/system/kubelet.service.d/15-banking-dr-clone-guard.conf',
            b'[Service]\nExecStartPre=/usr/local/sbin/banking-dr-clone-guard\n')
        run(pod,'chroot','/host','systemctl','daemon-reload')
        print(role,'temporary SSH service ready',flush=True)
    hosts=private/'isolated-dr-known-hosts';hosts.write_text('\n'.join(known)+'\n',encoding='utf-8')
    for role,(_,address) in NODES.items():
        subprocess.run(['ssh','-p','2222','-i',str(key),'-o','BatchMode=yes','-o','ConnectTimeout=6',
            '-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile="'+str(hosts.resolve())+'"',
            '-o','HostKeyAlias=banking-dr-'+role,'root@'+address,'hostname','-s'],check=True,timeout=15)


if __name__=='__main__':main()
