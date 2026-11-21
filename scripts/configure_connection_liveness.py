"""Bound server-side detection of dead clients retaining 2PC advisory locks."""
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def main():
    metadata=cs.get('cluster','bank-meta');pod=metadata['status']['currentPrimary']
    evidence=cs.rows(pod,"SELECT a.pid,client_addr::text,state,wait_event,backend_start,state_change,left(query,100) AS query FROM pg_stat_activity a WHERE datname=current_database()")
    cs.write_report('reports/assignment3/orphan-lock-before-liveness.json',{'created_utc':cs.utc(),'status':'PARTIAL','sessions':evidence})
    params={'tcp_keepalives_idle':'5','tcp_keepalives_interval':'2','tcp_keepalives_count':'3','tcp_user_timeout':'10000',
            'idle_session_timeout':'45s','client_connection_check_interval':'1s'}
    for cluster in cs.CLUSTERS:
        print(cs.kb('patch','cluster',cluster,'--type=merge','-p',json.dumps({'spec':{'postgresql':{'parameters':params}}})))


if __name__=='__main__':main()
