"""No Docker required: isolated coordinator/recovery unit tests."""
import os
import tempfile
import unittest

from gateway.coordinator import Coordinator, DuplicateRequestMismatch
from gateway.routing import Router, RouteError, ReplicaUnavailable


class FakeRouter:
    def __init__(self):
        self.balances = {"A0500": 1000, "A1500": 500}
        self.prepared = {}
        self.finished = {}
        self.fail_b_commit_once = False

    def locate(self,account):
        return (("A",1) if int(account[1:]) <= 1000 else ("B",1))

    def request(self, shard, method, path, data=None):
        tx_id = data["tx_id"]
        key = (shard, tx_id)
        if path.endswith("/prepare"):
            if key in self.prepared:
                return 200, {"ok": True, "vote": "YES"}
            if key in self.finished:
                return 200, {"ok": True, "vote": "YES" if self.finished[key] == "COMMITTED" else "NO"}
            entry = data["modifications"][0]
            if self.balances[entry["account"]] + entry["delta"] < 0:
                return 200, {"ok": True, "vote": "NO", "reason": "insufficient funds"}
            self.prepared[key] = entry
            return 200, {"ok": True, "vote": "YES"}
        if path.endswith("/commit"):
            if shard == "B" and self.fail_b_commit_once:
                self.fail_b_commit_once = False
                raise ReplicaUnavailable("B disconnected")
            if key in self.prepared:
                entry = self.prepared.pop(key)
                self.balances[entry["account"]] += entry["delta"]
                self.finished[key] = "COMMITTED"
            return 200, {"ok": True, "outcome": "COMMITTED"}
        if path.endswith("/abort"):
            self.prepared.pop(key, None)
            self.finished[key] = "ABORTED"
            return 200, {"ok": True, "outcome": "ABORTED"}
        raise AssertionError(path)


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.router = FakeRouter()
        self.coord = Coordinator(self.router, os.path.join(self.tmp.name, "coordinator.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def register(self, rid, amount=300):
        return self.coord.register(rid, "A0500", "A1500", amount, "A", "B")

    def test_commit_once_and_idempotent_request_id(self):
        txid, created = self.register("req_1")
        self.assertTrue(created)
        result = self.coord.execute_new(txid)
        self.assertEqual(result["status"], "COMMITTED")
        self.assertEqual((self.router.balances["A0500"], self.router.balances["A1500"]), (700, 800))
        duplicate_id, duplicate_created = self.register("req_1")
        self.assertEqual(duplicate_id, txid)
        self.assertFalse(duplicate_created)
        self.assertEqual(sum(self.router.balances.values()), 1500)
        with self.assertRaises(DuplicateRequestMismatch):
            self.register("req_1", 301)

    def test_insufficient_funds_aborts_without_partial_update(self):
        txid, _ = self.register("req_2", 2000)
        result = self.coord.execute_new(txid)
        self.assertEqual(result["status"], "ABORTED")
        self.assertEqual(self.router.balances, {"A0500": 1000, "A1500": 500})

    def test_durable_commit_recovery_after_one_ack(self):
        self.router.fail_b_commit_once = True
        txid, _ = self.register("req_3")
        result = self.coord.execute_new(txid)
        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(result["decision"], "COMMIT")
        self.assertEqual(result["acknowledged_by"], ["A"])
        self.assertEqual(self.router.balances, {"A0500": 700, "A1500": 500})
        # Process restart: instantiate a second coordinator against the same journal.
        second = Coordinator(self.router, self.coord.path)
        second.recover_once()
        result = second.status(tx_id=txid)
        self.assertEqual(result["status"], "COMMITTED")
        self.assertEqual(sum(self.router.balances.values()), 1500)

    def test_recover_undecided_transaction_to_abort(self):
        txid, _ = self.register("req_4")
        self.coord.recover_once()
        self.assertEqual(self.coord.status(tx_id=txid)["status"], "ABORTED")

    def test_shard_map_ranges(self):
        router = Router()
        self.assertEqual(router.account_shard("A0001"), "A")
        self.assertEqual(router.account_shard("A1000"), "A")
        self.assertEqual(router.account_shard("A1001"), "B")
        self.assertIn(router.account_shard("A2000"), ("B", "C"))
        with self.assertRaises(RouteError):
            router.account_shard("A2001")


if __name__ == "__main__":
    unittest.main()
