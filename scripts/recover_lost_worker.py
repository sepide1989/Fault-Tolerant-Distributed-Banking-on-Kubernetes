"""UID-bound, Retain-first recovery for permanently lost LOCAL storage.

Default is dry-run. --apply needs reviewed plan hash, actual fencing and restore
proof. No VM deletion, balance changes, 2PC decisions or manual promotions.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts import cluster_state as cs


def delete_uid(resource,name,uid):
    # DeleteOptions UID precondition prevents deleting a newly recreated object
    # whose name happens to be the same as the stale object in the reviewed plan.
    path='/api/v1/'+('nodes/' if resource=='nodes' else 'namespaces/'+cs.NS+'/'+resource+'/')+name
    body={'apiVersion':'v1','kind':'DeleteOptions','preconditions':{'uid':uid},
          'gracePeriodSeconds':0,'propagationPolicy':'Background'}
    try:cs.kb('delete','--raw',path,'-f','-',stdin=json.dumps(body))
    except RuntimeError as exc:
        if '(NotFound)' not in str(exc):raise


def require_same_surviving_hardware(topology,live_nodes,lost_node):
    # Full VM copies preserve Kubernetes UIDs and PostgreSQL system IDs. Those
    # alone cannot distinguish the original cluster from its isolated DR copy.
    for expected in topology['nodes']['items']:
        name=expected['metadata']['name']
        if name==lost_node:continue
        live=live_nodes.get(name,{})
        wanted=expected.get('status',{}).get('nodeInfo',{}).get('systemUUID','').lower()
        actual=live.get('status',{}).get('nodeInfo',{}).get('systemUUID','').lower()
        if not wanted or wanted!=actual:raise RuntimeError('STOP: surviving node hardware differs from the reviewed topology: '+name)


def plan(topology,lost_node,proof):
    if proof.get('status')!='PASS':raise RuntimeError('verified all-three restore proof required')
    nodes={n['metadata']['name']:n for n in topology['nodes']['items']}
    if lost_node not in nodes or 'node-role.kubernetes.io/control-plane' in nodes[lost_node]['metadata'].get('labels',{}):raise RuntimeError('unknown target or control-plane target')
    pvcs={p['metadata']['name']:p for p in topology['pvcs']['items']}
    pvs={p['metadata']['name']:p for p in topology['pvs']['items']}
    result={'lost_node':lost_node,'node_uid':nodes[lost_node]['metadata']['uid'],'instances':[]}
    for name in cs.CLUSTERS:
        before=topology['clusters'][name]
        if proof['cluster_uids'].get(name)!=before['uid']:raise RuntimeError('backup/topology cluster UID mismatch')
        sync=before['spec']['postgresql']['synchronous']
        if sync.get('number')!=2 or sync.get('dataDurability')!='required':raise RuntimeError('cannot establish remote durability with this sync policy')
        if len(before['instances'])!=3 or not all(x['ready'] and 'pg' in x for x in before['instances']):raise RuntimeError('pre-loss topology was not fully healthy')
        primaries=[x for x in before['instances'] if not x['pg']['standby']]
        if len(primaries)!=1 or len(primaries[0]['pg']['replication'])!=2 or not all(r['state']=='streaming' and r['sync_state'] in ('sync','quorum') for r in primaries[0]['pg']['replication']):raise RuntimeError('pre-loss synchronous replication unproven')
        survivors=[p for p in before['instances'] if p['node']!=lost_node and 'pg' in p]
        if not survivors:raise RuntimeError('no pre-loss remote copy for '+name)
        for instance in before['instances']:
            if instance['node']!=lost_node:continue
            item={'cluster':name,'cluster_uid':before['uid'],'pod':instance['pod'],
                  'pod_uid':instance['uid'],'claims':[],'remote_system_ids':sorted({p['pg']['system_id'] for p in survivors})}
            for claim in instance['claims']:
                pvc=pvcs[claim];pv=pvs[pvc['spec']['volumeName']]
                terms=pv['spec']['nodeAffinity']['required']['nodeSelectorTerms']
                if not any(e['key']=='kubernetes.io/hostname' and e['operator']=='In' and e['values']==[lost_node] for t in terms for e in t.get('matchExpressions',[])):raise RuntimeError('PV not exclusively attached to lost node')
                item['claims'].append({'name':claim,'uid':pvc['metadata']['uid'],'pv':pv['metadata']['name'],'pv_uid':pv['metadata']['uid']})
            result['instances'].append(item)
    if not result['instances']:raise RuntimeError('target has no captured DB instances')
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--topology',required=True);p.add_argument('--lost-node',required=True)
    p.add_argument('--restore-proof',required=True);p.add_argument('--apply',action='store_true')
    p.add_argument('--fenced-confirmed',action='store_true');p.add_argument('--approval-token')
    p.add_argument('--output',default='reports/assignment3/worker-recovery-plan.json');a=p.parse_args()
    result={'created_utc':cs.utc(),'status':'FAIL'}
    try:
        topology=json.loads(Path(a.topology).read_text(encoding='utf-8'))
        proof=json.loads(Path(a.restore_proof).read_text(encoding='utf-8'))
        reviewed=plan(topology,a.lost_node,proof)
        digest=hashlib.sha256(json.dumps(reviewed,sort_keys=True).encode()).hexdigest()
        result.update({'plan':reviewed,'plan_sha256':digest,'required_approval_token':'DELETE-LOST-STORAGE:'+digest})
        if not a.apply:
            result['status']='DRY_RUN';print(json.dumps(result,indent=2));return 0
        if not a.fenced_confirmed or a.approval_token!=result['required_approval_token']:raise RuntimeError('STOP: physical fencing and explicit approval for THIS exact plan are required')
        live_nodes={n['metadata']['name']:n for n in cs.get('nodes')['items']}
        require_same_surviving_hardware(topology,live_nodes,a.lost_node)
        old=live_nodes.get(a.lost_node)
        if old and (old['metadata']['uid']!=reviewed['node_uid'] or any(c['type']=='Ready' and c['status']=='True' for c in old['status']['conditions'])):raise RuntimeError('STOP: selected node is Ready or has a replacement UID')
        workers=[n for n in live_nodes.values() if n['metadata']['name']!=a.lost_node and
                 'node-role.kubernetes.io/control-plane' not in n['metadata'].get('labels',{}) and
                 n['metadata'].get('labels',{}).get('banking-worker')=='true' and
                 any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions'])]
        if len(workers)<2:raise RuntimeError('STOP: join and ready the replacement worker before releasing old claim bindings; otherwise all rebuilt copies may bind to one worker')
        for name in cs.CLUSTERS:
            live=cs.database_cluster(name,skip_nodes=(a.lost_node,))
            if live['uid']!=topology['clusters'][name]['uid']:raise RuntimeError('cluster UID changed')
            if live['spec']['postgresql']['synchronous']!=topology['clusters'][name]['spec']['postgresql']['synchronous']:raise RuntimeError('sync configuration changed since baseline')
            survivors=[x for x in live['instances'] if x['node']!=a.lost_node and 'pg' in x]
            oldids={x['pg']['system_id'] for x in topology['clusters'][name]['instances'] if 'pg' in x}
            if not survivors or any(x['pg']['system_id'] not in oldids for x in survivors):raise RuntimeError('surviving data lineage not established')
            if sum(x['pg']['standby'] is False for x in survivors)>1:raise RuntimeError('conflicting primaries')
        # Retain each old PV atomically before unbinding it. Never run local-path
        # cleanup of potentially valuable old directories as a side effect.
        for item in reviewed['instances']:
            for claim in item['claims']:
                patch=[{'op':'test','path':'/metadata/uid','value':claim['pv_uid']},
                       {'op':'replace','path':'/spec/persistentVolumeReclaimPolicy','value':'Retain'}]
                cs.kb('patch','pv',claim['pv'],'--type=json','-p',json.dumps(patch))
        # Prevent CNPG from recreating a pod referencing a claim that is about
        # to be deleted. Instance managers/PG keep running with strict sync.
        paused={}
        for name in cs.CLUSTERS:
            obj=cs.get('cluster',name)
            paused[name]=obj['metadata'].get('annotations',{}).get('cnpg.io/reconciliationLoop')
            cs.kb('annotate','cluster',name,'cnpg.io/reconciliationLoop=disabled','--overwrite')
        result['reconciliation_paused']=list(paused)
        cs.write_report(a.output,result)
        try:
            if old:delete_uid('nodes',a.lost_node,reviewed['node_uid'])
            for item in reviewed['instances']:
                delete_uid('pods',item['pod'],item['pod_uid'])
            # Wait for actual old-pod removal, including PVC protection users.
            for item in reviewed['instances']:
                cs.kb('wait','--for=delete','pod/'+item['pod'],'--timeout=60s',timeout=80)
            for item in reviewed['instances']:
                for claim in item['claims']:delete_uid('persistentvolumeclaims',claim['name'],claim['uid'])
            for item in reviewed['instances']:
                for claim in item['claims']:
                    cs.kb('wait','--for=delete','pvc/'+claim['name'],'--timeout=60s',timeout=80)
        finally:
            # No more deletions follow this block, even on a partial failure.
            for name,value in paused.items():
                cs.kb('annotate','cluster',name,'cnpg.io/reconciliationLoop-' if value is None else
                      'cnpg.io/reconciliationLoop='+value,'--overwrite')
            result['reconciliation_paused']=[]
        result['status']='PARTIAL'
        result['note']='Old PVs retained. Join replacement worker, label banking-worker=true, let CNPG clone from surviving data; run preflight, restore drill and exact baseline/client audit. No correctness PASS yet.'
    except Exception as exc:result['error']=str(exc);print(str(exc),file=sys.stderr)
    finally:cs.write_report(a.output,result)
    return 2


if __name__=='__main__':raise SystemExit(main())
