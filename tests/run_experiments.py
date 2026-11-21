"""Automated subset of the 7 required experiments.

Experiments 1, 4, and 6 have manual Docker steps documented in README_FA.md.
This program covers routing/replica selection, concurrency, local/2PC transfers,
and conservation over concurrent 2PC requests. Safe to rerun: it uses unique IDs
and computes expected balances from observed initial balances.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import time
import uuid

import requests


def unique():
    return uuid.uuid4().hex


class API:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def get(self, path):
        response = requests.get(self.base + path, timeout=20)
        response.raise_for_status()
        return response.json()

    def post(self, path, payload):
        return requests.post(self.base + path, json={**payload, "request_id": unique()}, timeout=40)

    def balance(self, account):
        return self.get("/api/accounts/" + account)["balance"]

    def await_transaction(self, response):
        content = response.json()
        if "transaction_id" not in content:
            raise AssertionError(f"Missing transaction_id: {response.status_code} {content}")
        for _ in range(25):
            result = self.get("/api/transactions/" + content["transaction_id"])
            if result["status"] in ("COMMITTED", "ABORTED"):
                return result
            time.sleep(1)
        return result


def run(api):
    print("TEST 2: Replica round robin for the same shard")
    replicas = Counter(api.get("/api/accounts/A0005")["replica"] for _ in range(12))
    print("Handled by:", dict(replicas))
    assert len(replicas) >= 2, "More than one replica should serve A0005"

    print("TEST 3: Concurrent requests to replicas of one shard")
    initial = api.balance("A0010")
    def local_request(i):
        name = "/api/deposit" if i % 2 == 0 else "/api/withdraw"
        r = api.post(name, {"account": "A0010", "amount": 2})
        return r.status_code, r.json()
    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(local_request, range(40)))
    assert all(code == 200 for code, _ in responses), responses
    final = api.balance("A0010")
    assert final == initial, f"Lost update? expected {initial}, observed {final}"
    print("40 concurrent operations; no lost updates:", final)

    print("TEST 5: Atomic same-shard transfer")
    before_20, before_21 = api.balance("A0020"), api.balance("A0021")
    r = api.post("/api/transfer", {"source": "A0020", "destination": "A0021", "amount": 11})
    assert r.status_code == 200, r.text
    assert api.balance("A0020") == before_20 - 11
    assert api.balance("A0021") == before_21 + 11
    print("Same-shard transfer OK")

    print("TEST 5: Two-Phase Commit cross-shard transfer")
    before_a, before_b = api.balance("A0500"), api.balance("A1500")
    r = api.post("/api/transfer", {"source": "A0500", "destination": "A1500", "amount": 30})
    tx = api.await_transaction(r)
    assert tx["status"] == "COMMITTED", tx
    assert api.balance("A0500") == before_a - 30
    assert api.balance("A1500") == before_b + 30
    assert set(tx["acknowledged_by"]) == {"A", "B"}
    print("Cross-shard 2PC COMMITTED by A and B")

    print("TEST 7: Concurrent overlapping cross-shard transfers")
    accounts = ["A0400", "A0401", "A1400", "A1401"]
    original = {a: api.balance(a) for a in accounts}
    movements = [
        ("A0400", "A1400", 1), ("A1400", "A0400", 1),
        ("A0401", "A1401", 1), ("A1401", "A0401", 1),
    ] * 4
    def send(t):
        src, dest, amount = t
        return api.await_transaction(api.post("/api/transfer", {
            "source": src, "destination": dest, "amount": amount
        }))
    with ThreadPoolExecutor(max_workers=6) as pool:
        outcomes = list(pool.map(send, movements))
    assert all(x["status"] in ("COMMITTED", "ABORTED") for x in outcomes), outcomes
    final_balances = {a: api.balance(a) for a in accounts}
    assert sum(original.values()) == sum(final_balances.values()), (
        "Money must be conserved: " + repr((original, final_balances))
    )
    print("COMMITTED:", sum(x["status"] == "COMMITTED" for x in outcomes),
          "ABORTED:", sum(x["status"] == "ABORTED" for x in outcomes))
    print("Total before = total after =", sum(original.values()))
    print("ALL AUTOMATED CHECKS PASSED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()
    run(API(args.url))
