"""Confirm 3 instances and preferred local/remote standby placement for Bonus I.
Run BEFORE killing a worker: python scripts/verify_bonus_layout.py
"""
import json
import subprocess

def kubectl(*args):
 return subprocess.check_output(['kubectl','-n','banking',*args],text=True).strip()

valid=True
for c in ['bank-a','bank-b','bank-meta']:
 obj=json.loads(kubectl('get','pods','-l',f'cnpg.io/cluster={c}','-o','json'))
 primary=kubectl('get','cluster',c,'-o','jsonpath={.status.currentPrimary}')
 pods=[(p['metadata']['name'],p['spec'].get('nodeName')) for p in obj['items']]
 nodes=dict(pods)
 if len(pods)<3 or len(set(nodes.values()))<2 or primary not in nodes:
  print('[NOT READY]',c,'primary=',primary,'pods=',pods)
  valid=False
  continue
 local=[x for x,n in pods if n==nodes[primary] and x!=primary]
 remote=[x for x,n in pods if n!=nodes[primary]]
 print(c,'PRIMARY=',primary,'node=',nodes[primary],
       'LOCAL_STANDBY=',local,'REMOTE_STANDBY=',remote)
 if not (local and remote):
  valid=False
  candidates=[x for x,n in pods if n!=nodes[primary]]
  print('[NEEDS SWITCHOEVER] Primary must share worker with a standby. Check CNPG status and then use:')
  print('kubectl cnpg promote -n banking',c,'<standby-on-worker-with-two-db-pods>')
if not valid: raise SystemExit('Layout requires adjustment before the Bonus I worker-failure experiment')
print('[PASS] every DB has 1 local + 1 remote standby with distinct per-instance PVCs')
