"""Finalize a completed physical failure using a later verified stable audit.

Preserves the first verifier result/error; never manufactures a successful run.
"""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-dir',required=True);p.add_argument('--audit',required=True);a=p.parse_args()
    directory=Path(a.run_dir);out=json.loads((directory/'result.json').read_text())
    before=json.loads((directory/'before.json').read_text());after=json.loads(Path(a.audit).read_text())
    affected=[n for n in cs.CLUSTERS if any(x['pod']==before['clusters'][n]['status']['currentPrimary'] and x['node']==out['lost_node'] for x in before['clusters'][n]['instances'])]
    checks={'hypervisor_physically_fenced':out.get('hypervisor_confirmed_off') is True,
        'full_stable_ledger_audit':after.get('pass') is True,
        'actual_failovers':bool(affected) and all(before['clusters'][n]['status']['currentPrimary']!=after['clusters'][n]['status']['currentPrimary'] for n in affected),
        'no_observed_dual_primary':all(sum(x.get('pg',{}).get('standby') is False for x in record.get('instances',[]))<=1 for obs in out['observations'] for record in obs['clusters'].values())}
    out['initial_verifier_error']=out.pop('error',None);out['final_audit']=a.audit;out['checks']=checks
    out['status']='PASS' if all(checks.values()) else 'PARTIAL'
    out['final_verified_utc']=cs.utc()
    out['primary_changes']={n:{'before':before['clusters'][n]['status']['currentPrimary'],
                              'after':after['clusters'][n]['status']['currentPrimary']} for n in cs.CLUSTERS}
    failed=datetime.fromisoformat(out['failure_utc']).timestamp()
    successes=[e['time_unix'] for line in (directory/'load.jsonl').read_text().splitlines()
               if (e:=json.loads(line)).get('success') and e['time_unix']>failed]
    out['first_validated_success_after_failure_seconds']=min(successes)-failed if successes else None
    cs.write_report(directory/'result.json',out);print(json.dumps({'status':out['status'],'checks':checks,'primary_changes':out['primary_changes']},indent=2))


if __name__=='__main__':main()
