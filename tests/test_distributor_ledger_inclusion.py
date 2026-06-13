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


class DistributorLedgerInclusionTests(unittest.IsolatedAsyncioTestCase):
    async def ledger(self, distributors, transactions, purchase_orders, **filters):
        fake_db = SimpleNamespace(
            distributors=Collection(distributors),
            distributor_transactions=Collection(transactions),
            purchase_orders=Collection(purchase_orders),
        )
        with patch("server.db", fake_db):
            return await distributor_ledger(filters.pop("did", "d1"), user={}, **filters)

    async def test_multiple_purchases_and_payments_are_included_and_balanced_in_date_order(self):
        result = await self.ledger(
            [{"id": "d1", "name": "Supplier", "opening_balance": 0}],
            [{"id": "pay-1", "distributor_id": "d1", "type": "payment", "amount": 50,
              "created_at": "2026-03-03T00:00:00+00:00"}],
            [
                {"id": "po-1", "distributor_id": "d1", "po_no": "PO-1", "grand_total": 100,
                 "po_date": "2026-03-01"},
                {"id": "po-2", "distributor_id": "d1", "po_no": "PO-2", "grand_total": 70,
                 "po_date": "2026-03-02"},
            ],
        )
        rows = [row for row in result["transactions"] if not row.get("is_opening_balance")]
        self.assertEqual([row["type"] for row in rows], ["purchase", "purchase", "payment"])
        self.assertEqual([row["running_balance"] for row in rows], [100, 170, 120])
        self.assertEqual(result["balance"], 120)

    async def test_legacy_ids_and_name_only_fallback_match_without_duplicate_purchase(self):
        result = await self.ledger(
            [{"_id": "object-id", "id": "current-id", "distributor_id": "legacy-id", "name": "Supplier", "opening_balance": 0}],
            [
                {"id": "existing-po", "distributor_id": "legacy-id", "type": "purchase", "amount": 25,
                 "purchase_order_id": "po-1", "created_at": "2026-03-01"},
                {"id": "pay", "distributor_name": "Supplier", "type": "payment", "amount": 5,
                 "created_at": "2026-03-02"},
            ],
            [{"id": "po-1", "distributor_id": "current-id", "grand_total": 25, "po_date": "2026-03-01"}],
            did="object-id",
        )
        rows = [row for row in result["transactions"] if not row.get("is_opening_balance")]
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(row["type"] == "purchase" for row in rows), 1)
        self.assertEqual(sum(row["type"] == "payment" for row in rows), 1)

    async def test_filters_apply_only_when_explicitly_provided(self):
        args = (
            [{"id": "d1", "name": "Supplier", "opening_balance": 0}],
            [{"id": "pay", "distributor_id": "d1", "type": "payment", "amount": 5,
              "created_at": "2026-03-02"}],
            [{"id": "po-1", "distributor_id": "d1", "grand_total": 25, "po_date": "2026-03-01"}],
        )
        unfiltered = await self.ledger(*args)
        filtered = await self.ledger(*args, transaction_type="payment")
        self.assertEqual({row["type"] for row in unfiltered["transactions"]}, {"purchase", "payment"})
        self.assertEqual([row["type"] for row in filtered["transactions"]], ["payment"])
        self.assertEqual(filtered["transactions"][0]["running_balance"], 20)
