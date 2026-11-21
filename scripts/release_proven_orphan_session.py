"""Close a proven dead coordinator TCP session; never edits a 2PC decision.

Required evidence: physical VM failure report, matching old pod IP, no current
pod with that IP, an idle backend and its advisory lock on an undecided tx.
New sessions must already have server-enforced idle lifetime before using this
one-time migration for old sessions created without that lifetime.
"""
import argparse
from datetime import datetime
import ipaddress
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--pid',type=int,required=True)
    p.add_argument('--failure-dir',required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    directory=Path(a.failure_dir);failure=json.loads((directory/'result.json').read_text())
    before=json.loads((directory/'preflight.json').read_text())
    if not failure.get('hypervisor_confirmed_off'):raise SystemExit('STOP: no confirmed physical failure')
    pod=cs.get('cluster','bank-meta')['status']['currentPrimary']
    rows=cs.rows(pod,f"""SELECT a.pid,a.client_addr::text,a.state,a.state_change::text,
        t.tx_id,t.phase,t.decision FROM pg_stat_activity a JOIN pg_locks l USING(pid)
        JOIN transactions t ON l.objid::bigint=(hashtextextended(t.tx_id,0)&4294967295)
        AND l.classid::bigint=((hashtextextended(t.tx_id,0)>>32)&4294967295)
        WHERE a.pid={a.pid} AND l.locktype='advisory'""")
    if len(rows)!=1:raise SystemExit('STOP: exactly one matching coordinator lock required')
    session=rows[0];address=str(ipaddress.ip_interface(session['client_addr']).ip)
    old=[x for x in before['pods']['items'] if x['status'].get('podIP')==address]
    if len(old)!=1 or old[0]['spec'].get('nodeName')!=failure['lost_node']:raise SystemExit('STOP: backend IP not tied to failed VM')
    if session['state']!='idle' or session['decision'] is not None:raise SystemExit('STOP: session active or transaction already decided')
    if datetime.fromisoformat(session['state_change'])>datetime.fromisoformat(failure['failure_utc']):raise SystemExit('STOP: session was active after VM failure')
    if any(x['status'].get('podIP')==address for x in cs.get('pods','-A')['items']):raise SystemExit('STOP: client IP is currently assigned')
    report={'created_utc':cs.utc(),'status':'DRY_RUN','session':session,'failed_pod':old[0]['metadata']['name'],
            'failed_pod_uid':old[0]['metadata']['uid'],'operation':'close idle TCP backend only; Coordinator retains all decision authority'}
    if a.apply:
        # Additional SQL state guard immediately before termination.
        report['closed']=cs.sql_pod(pod,f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={a.pid} AND state='idle'")=='t'
        report['status']='PARTIAL'
    cs.write_report('reports/assignment3/orphan-session-migration.json',report);print(json.dumps(report,indent=2))


if __name__=='__main__':main()
