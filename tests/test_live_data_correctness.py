import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import (
    _available_stock, _return_status, _safe_legacy_sold_stock,
    list_customers, list_distributors, purchase_return_report,
)


class Cursor:
    def __init__(self, rows): self.rows = [dict(row) for row in rows]
    def sort(self, *args, **kwargs): return self
    async def to_list(self, length): return self.rows if length is None else self.rows[:length]


class Collection:
    def __init__(self, rows): self.rows = rows
    def find(self, query=None, projection=None):
        rows = self.rows
        if query:
            for key, value in query.items():
                if isinstance(value, dict) and "$in" in value:
                    rows = [row for row in rows if row.get(key) in value["$in"]]
        return Cursor(rows)


class LiveDataCorrectnessTests(unittest.IsolatedAsyncioTestCase):
    def test_normalized_stock_prevents_false_sold_out_and_rejects_stale_sold_units(self):
        stale = {"purchased_units": 10, "sold_units": 10, "available_stock": 10}
        self.assertEqual(_safe_legacy_sold_stock(stale), 0)
        rebuilt = {**stale, "sold_units": _safe_legacy_sold_stock(stale)}
        self.assertEqual(_available_stock(rebuilt), 10)
        self.assertEqual(_return_status(rebuilt), "Not Returned")

    async def test_distributor_summary_counts_opening_and_only_payment_transactions_as_paid(self):
        db = SimpleNamespace(
            distributors=Collection([{"id": "d1", "name": "D", "opening_balance": 40}]),
            distributor_transactions=Collection([
                {"distributor_id": "d1", "type": "purchase", "amount": 100},
                {"distributor_id": "d1", "type": "payment", "amount": 30},
                {"distributor_id": "d1", "type": "purchase_return", "amount": 10},
            ]),
        )
        with patch("server.db", db): result = await list_distributors(user={})
        self.assertEqual(result[0]["total_purchases"], 140)
        self.assertEqual(result[0]["total_paid"], 30)
        self.assertEqual(result[0]["outstanding_balance"], 100)

    async def test_customer_summary_combines_invoices_and_unlinked_credit_ledger_without_double_counting(self):
        db = SimpleNamespace(
            customers=Collection([{"id": "c1", "name": "C", "opening_balance": 5}]),
            invoices=Collection([{"id": "i1", "customer_id": "c1", "total": 100, "paid_amount": 20}]),
            customer_transactions=Collection([
                {"customer_id": "c1", "type": "sale", "amount": 80, "invoice_id": "i1"},
                {"customer_id": "c1", "type": "sale", "amount": 25, "reference": "legacy-credit"},
                {"customer_id": "c1", "type": "payment", "amount": 10},
            ]),
        )
        with patch("server.db", db): result = await list_customers(user={})
        self.assertEqual(result[0]["total_sales"], 125)
        self.assertEqual(result[0]["total_paid"], 30)
        self.assertEqual(result[0]["receivable_balance"], 100)

    async def test_purchase_return_report_exposes_non_empty_analytics_rows(self):
        db = SimpleNamespace(purchase_returns=Collection([{"id": "r1", "return_quantity": 2, "purchase_rate": 5}]))
        with patch("server.db", db): result = await purchase_return_report(user={})
        self.assertEqual(result["purchase_returns"][0]["id"], "r1")
        self.assertEqual(result["returns_by_medicine"][0]["value"], 10)
