"""Bonus II: concise, evidence-producing replica self-healing test.

Run INSIDE the self-healer container:
    python tests/bonus2_smoke.py

Writes readable .log and structured .json to the persistent /healer-state/reports
Docker volume. The traffic is READ-ONLY; this test does not prove write/2PC
correctness or recovery after a crash of the controller itself.
"""

from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import threading
import time

import docker
import requests

PROJECT = os.getenv('COMPOSE_PROJECT', 'distributed-banking-assignment2')
SLOT = 'bank-a1'
NAME = f'{PROJECT}-{SLOT}-1'
NEIGHBOR = f'{PROJECT}-bank-a2-1'
GATEWAY = os.getenv('GATEWAY_URL', 'http://gateway:8000')
STATE = os.getenv('HEAL_STATE', '/healer-state/events.sqlite3')
REPORT_DIR = Path(os.getenv('BONUS2_REPORT_DIR', '/healer-state/reports'))
ACCOUNT = 'A0005'
REQUEST_COUNT = 75

REPORT_DIR.mkdir(parents=True, exist_ok=True)
STARTED = datetime.now(timezone.utc)
REPORT_BASE = 'bonus2_smoke_' + STARTED.strftime('%Y%m%d_%H%M%S_%f')
TEXT_FILE = REPORT_DIR / (REPORT_BASE + '.log')
JSON_FILE = REPORT_DIR / (REPORT_BASE + '.json')

EVENTS = []
SUMMARY = {}


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def log(step, **details):
    event = {'time_utc': stamp(), 'step': step, **details}
    EVENTS.append(event)
    detail = ' ' + json.dumps(details, ensure_ascii=False, sort_keys=True, default=str) if details else ''
    line = f"[{event['time_utc']}] [BONUS2] {step}{detail}"
    print(line, flush=True)
    with TEXT_FILE.open('a', encoding='utf-8') as file:
        file.write(line + '\n')


def journal_id():
    with sqlite3.connect(STATE, timeout=5) as con:
        return int(con.execute('SELECT COALESCE(MAX(id), 0) FROM events').fetchone()[0])


def slot_events_after(last_id):
    with sqlite3.connect(STATE, timeout=5) as con:
        return [(str(action), str(created)) for action, created in con.execute(
            'SELECT event,created_at FROM events WHERE id>? AND slot=? ORDER BY id',
            (last_id, SLOT))]


def api_balance():
    response = requests.get(f'{GATEWAY}/api/accounts/{ACCOUNT}', timeout=5)
    response.raise_for_status()
    body = response.json()
    assert body.get('replica') in ('A1', 'A2'), body
    return body


def health_state(container):
    container.reload()
    state = container.attrs.get('State', {})
    return state.get('Status'), state.get('Health', {}).get('Status')


def load_traffic(observations):
    for _ in range(REQUEST_COUNT):
        try:
            response = requests.get(f'{GATEWAY}/api/accounts/{ACCOUNT}', timeout=3)
            if response.ok:
                body = response.json()
                replica = body.get('replica', 'UNKNOWN')
                observations.append({'t': time.monotonic(), 'ok': True, 'replica': replica})
            else:
                observations.append({'t': time.monotonic(), 'ok': False, 'error': f'HTTP_{response.status_code}'})
        except requests.RequestException as exc:
            observations.append({'t': time.monotonic(), 'ok': False, 'error': type(exc).__name__})
        time.sleep(0.12)


def run_test():
    client = docker.from_env()
    old = client.containers.get(NAME)
    neighbor = client.containers.get(NEIGHBOR)
    old_id = old.id
    neighbor_id = neighbor.id
    old_status, old_health = health_state(old)
    neighbor_status, neighbor_health = health_state(neighbor)
    assert old_status == 'running' and old_health == 'healthy', (old_status, old_health)
    assert neighbor_status == 'running' and neighbor_health == 'healthy', (neighbor_status, neighbor_health)

    balance_before = api_balance()['balance']
    baseline_event_id = journal_id()
    log('BASELINE_OK', failed_slot=SLOT, old_container_id=old_id[:12],
        healthy_neighbor='bank-a2', neighbor_container_id=neighbor_id[:12],
        healthy_replicas=2, sample_account=ACCOUNT, sample_balance=balance_before,
        controller_journal_cursor=baseline_event_id)

    observations = []
    thread = threading.Thread(target=load_traffic, args=(observations,), name='gateway-read-load')
    thread.start()
    log('CONCURRENT_READ_LOAD_STARTED', requests=REQUEST_COUNT, account=ACCOUNT)

    time.sleep(0.6)
    log('FAULT_INJECTION_REQUESTED', target=SLOT, signal='SIGKILL', old_container_id=old_id[:12])
    old.kill()
    kill_time = time.monotonic()
    log('REPLICA_KILLED', target=SLOT, old_container_id=old_id[:12])

    replacement = None
    replacement_ready_time = None
    deadline = time.monotonic() + 65
    while time.monotonic() < deadline:
        try:
            container = client.containers.get(NAME)
            status, health = health_state(container)
            if container.id != old_id and status == 'running' and health == 'healthy':
                replacement = container
                replacement_ready_time = time.monotonic()
                break
        except docker.errors.NotFound:
            pass
        time.sleep(0.6)

    # Join even if recovery fails so the traffic summary is available for diagnosis.
    thread.join()
    successes = [o for o in observations if o['ok']]
    failures = [o for o in observations if not o['ok']]
    by_replica = dict(sorted(Counter(o['replica'] for o in successes).items()))
    by_error = dict(sorted(Counter(o['error'] for o in failures).items()))
    during_outage = [o for o in successes if kill_time <= o['t'] and
                     (replacement_ready_time is None or o['t'] < replacement_ready_time)]
    summary = {
        'attempted': len(observations),
        'successful': len(successes),
        'failed': len(failures),
        'handled_by_replica': by_replica,
        'errors_by_type': by_error,
        'successful_while_a1_unavailable': len(during_outage),
        'served_by_a2_while_a1_unavailable': sum(
            o['replica'] == 'A2' for o in during_outage),
    }
    SUMMARY.update(summary)
    log('TRAFFIC_RESULT', **summary)

    assert replacement is not None, 'Self-healer did not create a healthy new A1 within 65s'
    log('REPLACEMENT_HEALTHY', slot=SLOT, new_container_id=replacement.id[:12],
        old_container_id=old_id[:12], different_id=replacement.id != old_id,
        ready_after_kill_seconds=round(replacement_ready_time-kill_time, 2))

    # Check the *controller's persistent journal*, not only the container's new ID.
    journal_events = []
    for _ in range(20):
        journal_events = slot_events_after(baseline_event_id)
        actions = [name for name, _ in journal_events]
        if 'failed_removed' in actions and 'replacement_created' in actions:
            break
        time.sleep(0.3)
    assert 'failed_removed' in actions and 'replacement_created' in actions, journal_events
    log('CONTROLLER_JOURNAL_VERIFIED', actions=[
        {'event': action, 'journal_time_utc': created}
        for action, created in journal_events
        if action in ('failed_removed', 'replacement_created', 'resumed_created_slot')])

    after_status, after_health = health_state(client.containers.get(NEIGHBOR))
    assert client.containers.get(NEIGHBOR).id == neighbor_id, 'Healthy neighbor A2 was replaced'
    assert after_status == 'running' and after_health == 'healthy', (after_status, after_health)
    log('HEALTHY_NEIGHBOR_UNCHANGED', replica='A2', container_id=neighbor_id[:12],
        status=after_status, health=after_health)

    assert len(observations) == REQUEST_COUNT, summary
    assert len(successes) > 0, 'All banking requests failed during the test'
    assert any(o['replica'] == 'A2' for o in during_outage), (
        'No successful A2 response observed between kill and recovery', summary)
    log('HEALTHY_REPLICA_HANDLED_FAILURE', replica='A2',
        successful_during_outage=len(during_outage))

    # The router initially knows both replica DNS names. Prove that the
    # *replacement* is now reachable through actual Gateway requests.
    post_recovery_hits = Counter()
    for _ in range(20):
        body = api_balance()
        post_recovery_hits[body['replica']] += 1
        if post_recovery_hits['A1'] and post_recovery_hits['A2']:
            break
        time.sleep(0.1)
    assert post_recovery_hits['A1'] > 0 and post_recovery_hits['A2'] > 0, dict(post_recovery_hits)
    log('GATEWAY_ROUTING_VERIFIED', replicas_observed=dict(sorted(post_recovery_hits.items())))

    balance_after = api_balance()['balance']
    log('READ_ONLY_BALANCE_OBSERVED', account=ACCOUNT, before=balance_before,
        after=balance_after, unchanged=balance_before == balance_after,
        note='No writes generated by this test; other clients can change the balance')
    log('TEST_PASS', scenario='replica_replacement_under_read_load')


def main():
    log('TEST_BEGIN', scenario='replica_replacement_under_read_load', project=PROJECT)
    passed = False
    error = None
    try:
        run_test()
        passed = True
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        log('TEST_FAIL', error=error)
    finally:
        report = {'test': 'bonus2_smoke', 'passed': passed, 'error': error,
                  'started_utc': STARTED.isoformat(), 'finished_utc': stamp(),
                  'project': PROJECT, 'traffic': SUMMARY, 'events': EVENTS,
                  'limitations': ['Read-only requests: does not prove deposit/transfer correctness',
                                  'Does not test a crash of the self-healing controller']}
        log('EVIDENCE_SAVED', text_log=str(TEXT_FILE), json_report=str(JSON_FILE))
        report['events'] = EVENTS
        JSON_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    print('BONUS II SMOKE ' + ('PASS' if passed else 'FAIL'), flush=True)
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
