"""Custom Docker Engine reconciler. No Docker restart policy, K8s or Swarm.

Deterministic names provide a singleton for each slot even after controller
crashes between create, connect, start, register and journal commits.
"""
import logging
import os
import sqlite3
import time
from datetime import datetime,timezone

import docker
import requests

logging.basicConfig(level=logging.INFO)
log=logging.getLogger('self-healer')
PROJECT=os.getenv('COMPOSE_PROJECT','distributed-banking-assignment2')
NETWORK=f'{PROJECT}_default'
GATEWAY=os.getenv('GATEWAY_URL','http://gateway:8000')
TOKEN=os.getenv('HEALER_TOKEN','dev-local-controller-token')
STATE=os.getenv('HEAL_STATE','/healer-state/events.sqlite3')
INTERVAL=float(os.getenv('HEAL_INTERVAL','3'))
SLOTS=[(s,r) for s in ('A','B') for r in (1,2)]
if os.getenv('HEAL_C') == '1':
    SLOTS.extend(('C',r) for r in (1,2))
client=docker.from_env()
os.makedirs(os.path.dirname(STATE),exist_ok=True)


def journal(slot, action):
    with sqlite3.connect(STATE) as db:
        db.execute('CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, slot TEXT, event TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)')
        db.execute('INSERT INTO events(slot,event) VALUES(?,?)',(slot,action))


def slot_name(shard,number):
    return f'bank-{shard.lower()}{number}'


def env(shard,number):
    return {'SHARD_ID':shard,'REPLICA_ID':shard+str(number),
            'DB_DSN':f'postgresql://bank:bankpass@db-{shard.lower()}:5432/bank'}


def age_seconds(attrs):
    stamp=attrs.get('State',{}).get('StartedAt') or attrs.get('Created') or ''
    try:
        return (datetime.now(timezone.utc)-datetime.fromisoformat(stamp.replace('Z','+00:00'))).total_seconds()
    except ValueError:
        return 999


def healthy(container):
    container.reload()
    state=container.attrs['State']
    if not state.get('Running'):
        return False
    health=state.get('Health',{}).get('Status')
    return health in (None,'healthy','starting')


def register(shard,number):
    alias=slot_name(shard,number)
    try:
        resp=requests.post(GATEWAY+'/api/admin/register-replica',
           json={'shard':shard,'url':f'http://{alias}:8000'},
           headers={'X-Healer-Token':TOKEN},timeout=3)
        if resp.ok:
            log.info('Registered %s at Gateway',alias)
    except requests.RequestException:
        pass # Gateway may itself be down. Re-register in next cycle.


def create_replacement(shard,number,old_labels=None):
    slot=slot_name(shard,number)
    container_name=f'{PROJECT}-{slot}-1'
    image=f'{PROJECT}-{slot}:latest'
    # Preserve Compose ownership labels so `docker compose ps` still shows the
    # replacement (plus deterministic slot identity for controller recovery).
    labels = dict(old_labels or {})
    labels.update({
        'com.docker.compose.project':PROJECT,
        'com.docker.compose.service':slot,
        'com.docker.compose.container-number':'1',
        'com.docker.compose.oneoff':'False',
        'self-healer.slot':slot,
        'self-healer.project':PROJECT,
    })
    log.warning('Creating replacement for %s (image %s)',slot,image)
    container=client.containers.create(
        image=image, name=container_name, environment=env(shard,number),
        command=['gunicorn','--bind','0.0.0.0:8000','--workers','1',
                 '--threads','8','--timeout','45','bank.app:app'],
        restart_policy={'Name':'no'}, detach=True,
        labels=labels,
        healthcheck={'Test':['CMD','python','-c',
                   "import urllib.request; urllib.request.urlopen('http://localhost:8000/health',timeout=2)"],
                   'Interval':5000000000,'Timeout':3000000000,'Retries':5},
    )
    # A crash between create/connect/start is harmless: next reconcile sees
    # the deterministic name and completes the missing steps.
    try:
        client.networks.get(NETWORK).connect(container,aliases=[slot])
    except docker.errors.APIError as e:
        log.warning('Network connection problem %s: %s',slot,e)
        raise
    container.start()
    journal(slot,'replacement_created')


def reconcile_slot(shard,number):
    slot=slot_name(shard,number)
    container_name=f'{PROJECT}-{slot}-1'
    try:
        container=client.containers.get(container_name)
    except docker.errors.NotFound:
        create_replacement(shard,number)
        return
    container.reload()
    state=container.attrs['State']
    if state.get('Status')=='created':
        # Recover from controller crash mid-creation. Ensure network alias.
        net=container.attrs.get('NetworkSettings',{}).get('Networks',{})
        if NETWORK not in net:
            client.networks.get(NETWORK).connect(container,aliases=[slot])
        container.start()
        journal(slot,'resumed_created_slot')
        return
    if state.get('Running'):
        health=state.get('Health',{}).get('Status')
        if health=='unhealthy' and age_seconds(container.attrs)>25:
            log.warning('%s unhealthy; replacing',slot)
        else:
            register(shard,number)
            return
    # Failed or exited replica -> remove and recreate SAME slot, never spawn
    # an extra replica. Healthy neighbors are completely untouched.
    old_labels=container.attrs.get('Config',{}).get('Labels',{})
    container.remove(force=True)
    journal(slot,'failed_removed')
    create_replacement(shard,number,old_labels)


def main():
    journal('controller','started')
    log.info('Self healer started; slots=%s; network=%s',SLOTS,NETWORK)
    while True:
        for shard,number in SLOTS:
            try:
                reconcile_slot(shard,number)
            except (docker.errors.DockerException, requests.RequestException) as exc:
                log.exception('Slot %s%d reconciliation failed: %s',shard,number,exc)
        time.sleep(INTERVAL)


if __name__=='__main__':
    main()
