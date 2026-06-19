import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")

from server import distributor_ledger


class Cursor:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, length):
        return self.rows[:length]


class Collection:
    def __init__(self, rows):
        self.rows = rows

    async def find_one(self, query, *args, **kwargs):
        return next(
            (dict(row) for row in self.rows if all(str(row.get(k)) == str(v) for k, v in query.items())),
            None,
        )

    def find(self, query=None, *args, **kwargs):
        return Cursor(self.rows)


class DistributorLedgerNoPurchaseOrderProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def ledger(self, transactions, purchase_orders, distributor=None):
        fake_db = SimpleNamespace(
            distributors=Collection([distributor or {"id": "d1", "name": "Supplier", "opening_balance": 0}]),
            distributor_transactions=Collection(transactions),
            purchase_orders=Collection(purchase_orders),
        )
        with patch("server.db", fake_db):
            return await distributor_ledger("d1", user={})

    async def test_creating_purchase_order_does_not_create_ledger_transaction(self):
        result = await self.ledger([], [{"id": "po-1", "distributor_id": "d1", "grand_total": 100, "po_date": "2026-06-01"}])

        self.assertEqual(result["transactions"], [])
        self.assertEqual(result["total_purchases"], 0)
        self.assertEqual(result["balance"], 0)

    async def test_editing_purchase_order_does_not_create_or_update_ledger_transaction(self):
        before = await self.ledger([], [{"id": "po-1", "distributor_id": "d1", "grand_total": 100, "po_date": "2026-06-01"}])
        after = await self.ledger([], [{"id": "po-1", "distributor_id": "d1", "grand_total": 250, "po_date": "2026-06-02"}])

        self.assertEqual(before["transactions"], [])
        self.assertEqual(after["transactions"], [])
        self.assertEqual(after["balance"], before["balance"])

    async def test_deleting_purchase_order_does_not_affect_ledger(self):
        transactions = [{"id": "pay-1", "distributor_id": "d1", "type": "payment", "amount": 25, "created_at": "2026-06-03"}]
        before = await self.ledger(transactions, [{"id": "po-1", "distributor_id": "d1", "grand_total": 100, "po_date": "2026-06-01"}])
        after = await self.ledger(transactions, [])

        self.assertEqual(before["transactions"], after["transactions"])
        self.assertEqual(before["balance"], after["balance"])

    async def test_manual_purchase_still_works(self):
        result = await self.ledger([
            {"id": "manual-purchase", "distributor_id": "d1", "type": "purchase", "amount": 75, "created_at": "2026-06-01"},
        ], [])

        self.assertEqual([row["id"] for row in result["transactions"]], ["manual-purchase"])
        self.assertEqual(result["total_purchases"], 75)
        self.assertEqual(result["balance"], 75)

    async def test_purchase_return_adjust_ledger_on_still_works(self):
        result = await self.ledger([
            {"id": "manual-purchase", "distributor_id": "d1", "type": "purchase", "amount": 100, "created_at": "2026-06-01"},
            {"id": "return-1", "distributor_id": "d1", "type": "purchase_return", "amount": 30, "return_id": "r1", "created_at": "2026-06-02"},
        ], [])

        self.assertEqual([row["id"] for row in result["transactions"]], ["manual-purchase", "return-1"])
        self.assertEqual(result["total_adjustments"], 30)
        self.assertEqual(result["balance"], 70)

    async def test_opening_balance_still_works(self):
        result = await self.ledger([], [], {"id": "d1", "name": "Supplier", "opening_balance": 45, "opening_balance_date": "2026-04-01"})

        self.assertEqual(len(result["transactions"]), 1)
        self.assertTrue(result["transactions"][0]["is_opening_balance"])
        self.assertEqual(result["balance"], 45)
