"""Read-only bounded Kubernetes/PostgreSQL evidence. Never reads Secret values."""
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

CLUSTERS=('bank-a','bank-b','bank-meta')
NS='banking'


def utc():
    return datetime.now(timezone.utc).isoformat()


def kb(*args, timeout=25, stdin=None):
    result=subprocess.run(['kubectl','--request-timeout=15s','-n',NS,*map(str,args)],
        input=stdin,text=True,encoding='utf-8',capture_output=True,timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'kubectl command failed')
    return result.stdout.strip()


def get(*args):
    return json.loads(kb('get',*args,'-o','json'))


def sql_pod(pod,query):
    return kb('exec',pod,'-c','postgres','--','env',
        'PGOPTIONS=-c statement_timeout=5000 -c lock_timeout=2000',
        'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','bank','-c',query)


def rows(pod,query):
    return json.loads(sql_pod(pod,"SELECT COALESCE(json_agg(q),'[]'::json) FROM ("+query+") q"))


def sanitize(value):
    # Last-applied manifests and container env can contain literal credentials.
    if isinstance(value,dict):
        return {k:sanitize(v) for k,v in value.items()
                if k not in ('data','stringData','managedFields','annotations','env','envFrom')}
    if isinstance(value,list):
        return [sanitize(x) for x in value]
    return value


def write_report(path,report):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    if os.environ.get('KUBECONFIG'):
        report.setdefault('kubeconfig_file',Path(os.environ['KUBECONFIG']).name)
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    checks=report.get('checks',{})
    text=['# Banking evidence', '', 'UTC: '+report.get('created_utc',utc()),
          '', 'Result: '+report.get('status', 'PASS' if report.get('pass') else 'FAIL'), '']
    for name,value in checks.items():
        text.append(f'- {name}: {value}')
    if report.get('error'): text.extend(['',str(report['error'])])
    path.with_suffix('.md').write_text('\n'.join(text).rstrip()+'\n',encoding='utf-8',newline='\n')


def pg_state(pod):
    return rows(pod,"""SELECT pg_is_in_recovery() AS standby,
      current_setting('synchronous_commit') AS synchronous_commit,
      current_setting('synchronous_standby_names') AS sync_names,
      current_setting('max_prepared_transactions')::int AS max_prepared,
      current_setting('fsync') AS fsync,
      (pg_control_system()).system_identifier::text AS system_id,
      (pg_control_checkpoint()).timeline_id AS timeline,
      CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_flush_lsn() END::text AS lsn,
      (SELECT COALESCE(json_agg(r),'[]') FROM
        (SELECT application_name,state,sync_state,sent_lsn::text,write_lsn::text,flush_lsn::text,replay_lsn::text FROM pg_stat_replication) r) AS replication,
      (SELECT COALESCE(json_agg(w),'[]') FROM
        (SELECT status,sender_host,slot_name,written_lsn::text,flushed_lsn::text,received_tli FROM pg_stat_wal_receiver) w) AS wal_receiver,
      (SELECT COALESCE(json_agg(s),'[]') FROM
        (SELECT slot_name,slot_type,active,restart_lsn::text,wal_status,safe_wal_size FROM pg_replication_slots) s) AS slots,
      (SELECT COALESCE(json_agg(p),'[]') FROM
        (SELECT gid,prepared::text,owner FROM pg_prepared_xacts WHERE database=current_database()) p) AS prepared,
      (SELECT count(*) FROM pg_stat_activity WHERE wait_event='SyncRep') AS sync_waiters,
      (SELECT json_build_object('archived_count',archived_count,'failed_count',failed_count,
       'last_archived_wal',last_archived_wal,'last_archived_time',last_archived_time,
       'last_failed_time',last_failed_time) FROM pg_stat_archiver) AS archiver""")[0]


def database_cluster(name,skip_nodes=()):
    obj=get('cluster',name)
    pods=get('pods','-l','cnpg.io/cluster='+name)['items']
    result={'spec':sanitize(obj['spec']),'status':sanitize(obj.get('status',{})),
            'uid':obj['metadata']['uid'],'instances':[]}
    for pod in pods:
        if not any(c['name']=='postgres' for c in pod['spec']['containers']): continue
        instance={'pod':pod['metadata']['name'],'uid':pod['metadata']['uid'],
                  'node':pod['spec'].get('nodeName'),
                  'ready':any(c['type']=='Ready' and c['status']=='True' for c in pod['status'].get('conditions',[])),
                  'claims':[v['persistentVolumeClaim']['claimName'] for v in pod['spec']['volumes'] if 'persistentVolumeClaim' in v]}
        try:
            if instance['node'] in skip_nodes:
                instance['physically_fenced']=True
                raise RuntimeError('VM confirmed powered off in hypervisor; SQL query deliberately skipped')
            instance['pg']=pg_state(instance['pod'])
        except Exception as exc: instance['error']=str(exc)
        result['instances'].append(instance)
    return result
