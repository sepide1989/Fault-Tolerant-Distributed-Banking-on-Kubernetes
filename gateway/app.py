"""Public API. Clients know only the Gateway; never a database or replica."""
import logging
import os
import re
import uuid
import hmac
import time
import psycopg
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from flask import Flask, jsonify, request

from gateway.routing import Router, RouteError, ReplicaUnavailable
if os.getenv("COORD_BACKEND", "sqlite") == "postgres":
    from gateway.coordinator_pg import Coordinator, DuplicateRequestMismatch
else:
    from gateway.coordinator import Coordinator, DuplicateRequestMismatch

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
router = Router()
coordinator = Coordinator(router)


def invalid(message, code=400):
    return jsonify(ok=False, error=message), code


def parse_id(data):
    request_id = data.get("request_id") or uuid.uuid4().hex
    if not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", request_id):
        raise ValueError("invalid request_id; use letters/digits/_/./:/-")
    return request_id


def parse_amount(data):
    amount = data.get("amount")
    if type(amount) is not int or not (0 < amount <= 1_000_000_000):
        raise ValueError("amount must be a positive integer <= 1000000000")
    return amount


@app.errorhandler(RouteError)
@app.errorhandler(ValueError)
def bad_input(exc):
    return invalid(str(exc), 400)


@app.errorhandler(ReplicaUnavailable)
def service_down(exc):
    return invalid(str(exc), 503)


@app.errorhandler(psycopg.Error)
def metadata_down(exc):
    app.logger.warning('metadata unavailable: %s', type(exc).__name__)
    return jsonify(ok=False, status='UNKNOWN',
                   error='metadata unavailable; retry the SAME request_id'), 503


@app.errorhandler(DuplicateRequestMismatch)
def conflicting_request(exc):
    return invalid(str(exc),409)


def reserve(request_id,operation,payload):
    if hasattr(coordinator,'reserve_request'):
        coordinator.reserve_request(request_id,operation,payload)


@app.get("/health")
def health():
    return jsonify(ok=True, service="gateway")


@app.get("/api/topology")
def topology():
    return jsonify(router.topology())


@app.get("/api/accounts/<account>")
def get_account(account):
    shard = router.account_shard(account)
    code, body = router.request(shard, "GET", "/internal/accounts/" + account)
    return jsonify(body), code


@app.post("/api/deposit")
def deposit():
    data = request.get_json(silent=True) or {}
    account = data.get("account")
    shard = router.account_shard(account)
    payload = {"account": account, "amount": parse_amount(data), "request_id": parse_id(data)}
    reserve(payload['request_id'],'deposit',{'account':account,'amount':payload['amount']})
    payload["epoch"] = router.locate(account)[1]
    code, body = router.request(shard, "POST", "/internal/deposit", payload)
    return jsonify(body), code


@app.post("/api/withdraw")
def withdraw():
    data = request.get_json(silent=True) or {}
    account = data.get("account")
    shard = router.account_shard(account)
    payload = {"account": account, "amount": parse_amount(data), "request_id": parse_id(data)}
    reserve(payload['request_id'],'withdraw',{'account':account,'amount':payload['amount']})
    payload["epoch"] = router.locate(account)[1]
    code, body = router.request(shard, "POST", "/internal/withdraw", payload)
    return jsonify(body), code


@app.post("/api/transfer")
def transfer():
    data = request.get_json(silent=True) or {}
    source = data.get("source")
    destination = data.get("destination")
    if source == destination:
        return invalid("source and destination must differ", 400)
    from_shard = router.account_shard(source)
    to_shard = router.account_shard(destination)
    amount = parse_amount(data)
    request_id = parse_id(data)
    reserve(request_id,'transfer',{'source':source,'destination':destination,'amount':amount})
    # A same-shard transfer performed before migration may become cross-shard
    # afterwards. Consult the durable old B request journal before doing it again.
    if from_shard != to_shard and any(1501 <= int(a[1:]) <= 2000 for a in (source,destination)):
        legacy = router.legacy_request(request_id)
        if legacy:
            expected = {'source':source,'destination':destination,'amount':amount}
            if legacy.get('op_type') != 'transfer' or legacy.get('payload') != expected:
                return invalid('request_id used for another transfer',409)
            return jsonify(ok=True, request_id=request_id, already_committed=True, **legacy['result']), 200
    if from_shard == to_shard:
        # A prior cross-shard request must never become a second local transfer
        # after a map change.
        earlier = coordinator.status(request_id=request_id)
        if earlier['status'] != 'UNKNOWN':
            if (earlier['source'],earlier['destination'],earlier['amount']) != (source,destination,amount):
                return invalid('request_id used for another transfer',409)
            code = 200 if earlier['status']=='COMMITTED' else (409 if earlier['status']=='ABORTED' else 202)
            return jsonify(ok=earlier['status']=='COMMITTED', **earlier), code
        code, body = router.request(from_shard, "POST", "/internal/transfer/local", {
            "source": source, "destination": destination, "amount": amount,
            "request_id": request_id, "epoch": router.locate(source)[1]
        })
        return jsonify(body), code
    try:
        tx_id, created = coordinator.register(
            request_id, source, destination, amount, from_shard, to_shard
        )
    except DuplicateRequestMismatch as exc:
        return invalid(str(exc), 409)
    state = coordinator.execute_new(tx_id) if created else coordinator.status(tx_id=tx_id)
    http_code = 200 if state["status"] == "COMMITTED" else (409 if state["status"] == "ABORTED" else 202)
    return jsonify(ok=state["status"] == "COMMITTED", **state), http_code


@app.get("/api/transactions/<tx_id>")
def transaction_status(tx_id):
    if not re.fullmatch(r"[a-f0-9]{32}", tx_id):
        return invalid("invalid transaction ID", 400)
    state = coordinator.status(tx_id=tx_id)
    return jsonify(state), 404 if state["status"] == "UNKNOWN" else 200


@app.get("/api/requests/<request_id>")
def request_status(request_id):
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", request_id):
        return invalid("invalid request ID", 400)
    state = coordinator.status(request_id=request_id)
    if state['status']=='UNKNOWN' and hasattr(coordinator,'registered_request'):
        registration=coordinator.registered_request(request_id)
        if registration:
            payload=registration['payload']
            account=payload.get('account') or payload.get('source')
            shard=router.account_shard(account)
            code,local=router.request(shard,'GET','/internal/requests/'+request_id)
            if code==200 and local.get('found') and local.get('payload')==payload:
                return jsonify(ok=True,request_id=request_id,status='COMMITTED',
                               phase='COMPLETED',decision='COMMIT',**local['result']),200
    return jsonify(state), 404 if state["status"] == "UNKNOWN" else 200


@app.post("/api/admin/arm-crash-after-commit")
def arm_crash():
    # For controlled demos only. Never expose admin fault-injection to the public Internet.
    if os.environ.get("ENABLE_DEMO_ENDPOINTS") != "1":
        return invalid("demo endpoint disabled", 403)
    return jsonify(coordinator.arm_crash())


@app.post("/api/admin/register-replica")
def register_replica():
    if os.getenv('ENABLE_DYNAMIC_REGISTRATION') != '1':
        return invalid('dynamic replica registration disabled in Kubernetes',403)
    token = os.environ.get('HEALER_TOKEN', '')
    if not hmac.compare_digest(request.headers.get('X-Healer-Token',''), token):
        return invalid('unauthorized',403)
    data=request.get_json(silent=True) or {}
    try:
        return jsonify(router.register_replica(data.get('shard'),data.get('url')))
    except RouteError as exc:
        return invalid(str(exc),400)


_GATEWAY_HTTP = Counter('gateway_http_requests_total', 'Gateway requests', ['method','endpoint','status'])
_GATEWAY_LATENCY = Histogram('gateway_http_request_seconds','Gateway request latency',['endpoint'])
_META_UP = Gauge('bank_metadata_available', 'Metadata PostgreSQL connection availability')
_2PC_STATES = Gauge('bank_2pc_state_count','Durable transaction states',['phase','decision'])

@app.before_request
def _gateway_obs_start():
    request._obs_time=time.monotonic()

@app.after_request
def _gateway_obs_end(response):
    endpoint=request.url_rule.rule if request.url_rule else 'unknown'
    _GATEWAY_HTTP.labels(request.method,endpoint,str(response.status_code)).inc()
    _GATEWAY_LATENCY.labels(endpoint).observe(time.monotonic()-getattr(request,'_obs_time',time.monotonic()))
    return response

@app.get('/metrics')
def gateway_metrics():
    try:
        with coordinator.connection() as con:
            rows=con.execute('''SELECT phase,COALESCE(decision,'UNDECIDED'),count(*)
                                FROM transactions GROUP BY phase,decision''').fetchall()
        _META_UP.set(1)
        _2PC_STATES.clear()
        for phase,decision,count in rows:
            _2PC_STATES.labels(phase,decision).set(count)
    except Exception:
        _META_UP.set(0)
    return generate_latest(),200,{'Content-Type':CONTENT_TYPE_LATEST}

@app.get('/ready')
def gateway_ready():
    try:
        router.topology()  # metadata DB reachable, but not necessarily banking DBs
        return jsonify(ok=True,service='gateway',metadata_available=True)
    except Exception:
        return jsonify(ok=False,service='gateway',metadata_available=False),503
