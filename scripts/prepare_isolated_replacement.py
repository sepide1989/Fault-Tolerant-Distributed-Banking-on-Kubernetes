"""Prepare a replacement OS copy with all inherited cluster/data state quarantined.

Reversible preparation only: no VM/disk/PVC deletion, join, etcd restore or data
promotion. Old directories are MOVED inside this new copy and never used as the
recovery source. The default is dry-run. Prepared replacements remain powered off.
"""
import argparse
import json
from pathlib import Path
import re
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.isolated_dr_workspace import (PLAN,PRIVATE,VMDIR,bios_identity,
    clone_address,configure_vmx,save,ssh,vm)
from scripts import cluster_state as cs


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--role',choices=['worker','cp'],required=True)
    parser.add_argument('--apply',action='store_true');a=parser.parse_args()
    plan=json.loads(PLAN.read_text())
    root=Path(plan['root']).resolve()
    if root.parent!=VMDIR.resolve() or not root.name.startswith('Banking_DR_Isolated_'):
        raise SystemExit('STOP: unexpected isolated workspace')
    source=plan['nodes']['c' if a.role=='worker' else 'cp']
    target=root/'replacements'/a.role/('replacement-'+a.role+'.vmx')
    hostname='banking-worker-replacement-1' if a.role=='worker' else source['node_name']
    report={'created_utc':cs.utc(),'status':'DRY_RUN','role':a.role,'vmx':str(target),
            'hostname':hostname,'source_os':source['source_vmx'],
            'recovery_data_source':'surviving database replicas / separately verified etcd archive'}
    if not a.apply:
        print(json.dumps(report,indent=2));return
    record=plan.setdefault('replacements',{}).get(a.role)
    if record and record.get('prepared'):
        print('Already prepared and parked:',target);return
    if source['source_vmx'].lower() in vm('list').lower():
        raise SystemExit('STOP: source OS must be powered off for an independent full clone')
    if target.exists() and not record:raise SystemExit('STOP: unowned replacement path exists')
    if not record:
        record={'clone_vmx':str(target),'source_system_uuid':source['source_system_uuid']}
        plan['replacements'][a.role]=record;save(plan)
    target.parent.mkdir(parents=True,exist_ok=True)
    if not target.exists():vm('clone',source['source_vmx'],target,'full','-cloneName=banking-dr-replacement-'+a.role,timeout=900)
    if str(target).lower() not in vm('list').lower():
        configure_vmx(target,'replacement-'+a.role)
        text=target.read_text();text=re.sub(r'^memsize\s*=.*$','memsize = "2048"',text,flags=re.M)
        target.write_text(text);vm('start',target,'nogui')
    # Same trusted OS host key as the source copy. Hardware identity is separate.
    alias='c' if a.role=='worker' else 'cp'
    address=clone_address(alias,record);record['management_ip']=address;save(plan)
    original=source['source_system_uuid'].lower()
    if original in bios_identity(target):raise SystemExit('STOP: replacement retained original identity')
    script='''set -eu
actual=$(tr 'A-Z' 'a-z' </sys/class/dmi/id/product_uuid)
[ "$actual" != "SOURCE_UUID" ]
if systemctl is-active --quiet kubelet; then echo 'STOP: inherited kubelet is active'; exit 1; fi
systemctl stop kubelet containerd
install -d -m 700 /var/lib/banking-dr-access
q=/var/lib/banking-dr-access/quarantined-source-state
if [ ! -e "$q/PREPARED" ]; then
  mkdir -p "$q"
  for path in /etc/kubernetes /var/lib/etcd /var/lib/kubelet /opt/local-path-provisioner /etc/cni/net.d /var/lib/cni /var/lib/calico; do
    if [ -e "$path" ]; then
      if findmnt -rn -o TARGET | grep -Eq "^$path(/|$)"; then echo "STOP: mounted inherited state $path"; exit 1; fi
      destination="$q$path"
      mkdir -p "$(dirname "$destination")"
      [ ! -e "$destination" ] || { echo 'STOP: quarantine destination already exists'; exit 1; }
      mv -- "$path" "$destination"
    fi
  done
  touch "$q/PREPARED"
fi
mkdir -p /etc/kubernetes /var/lib/kubelet /opt/local-path-provisioner
test ! -e /etc/kubernetes/kubelet.conf
test ! -e /var/lib/etcd/member
test -z "$(ls -A /opt/local-path-provisioner)"
hostnamectl set-hostname REPLACEMENT_HOSTNAME
systemctl start containerd
printf 'Prepared clean replacement; source cluster identity/data quarantined.\\n'
'''.replace('SOURCE_UUID',original).replace('REPLACEMENT_HOSTNAME',hostname)
    report['preparation']=ssh(alias,address,script)
    report['hardware_identities']=sorted(bios_identity(target))
    report['status']='PARTIAL'
    report['note']='OS replacement prepared, not joined/restored. Banking correctness requires the actual permanent-loss experiment.'
    record.update({'prepared':True,'hostname':hostname,'ssh_alias':alias,
                   'node_ip':'192.168.43.132' if a.role=='worker' else source['original_ip'],
                   'hardware_identities':report['hardware_identities']})
    vm('stop',target,'soft')
    target.write_text(re.sub(r'^memsize\s*=.*$','memsize = "4096"',target.read_text(),flags=re.M))
    save(plan)
    cs.write_report('reports/assignment3/replacement-'+a.role+'-prepared.json',report)
    print('Replacement prepared and parked:',a.role,flush=True)


if __name__=='__main__':main()
