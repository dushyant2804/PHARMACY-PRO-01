import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import dashboard_summary, expiry_report


class _Cursor:
    def __init__(self, records):
        self.records = records

    async def to_list(self, length):
        return self.records[:length]


class _Collection:
    def __init__(self, records):
        self.records = records

    def find(self, query=None, projection=None):
        records = self.records
        if query and "distributor_id" in query:
            records = [row for row in records if row.get("distributor_id") == query["distributor_id"]]
        return _Cursor(records)


class DashboardSummaryTest(unittest.IsolatedAsyncioTestCase):
    async def test_separates_pending_money_and_limits_dashboard_expired_stock(self):
        today = datetime.now(timezone.utc).date()
        fake_db = SimpleNamespace(
            invoices=_Collection([]),
            expenses=_Collection([]),
            medicines=_Collection([
                {
                    "id": "low-default", "name": "Default", "purchased_units": 3, "sold_units": 1,
                    "low_stock_threshold": 5, "purchase_price": 1,
                },
                {
                    "id": "low-reordered", "name": "Reordered", "purchased_units": 4, "sold_units": 0,
                    "low_stock_threshold": 5, "low_stock_status": "reordered", "purchase_price": 1,
                },
                {
                    "id": "recent-expired", "name": "Recent", "purchased_units": 2, "sold_units": 0,
                    "expiry_date": (today - timedelta(days=90)).isoformat(), "purchase_price": 1,
                },
                {
                    "id": "old-expired", "name": "Old", "purchased_units": 2, "sold_units": 0,
                    "expiry_date": (today - timedelta(days=91)).isoformat(), "purchase_price": 1,
                },
            ]),
            customer_transactions=_Collection([
                {"customer_id": "customer-1", "type": "sale", "amount": 100.129},
                {"customer_id": "customer-1", "type": "payment", "amount": 25.005},
                {"customer_id": "customer-credit", "type": "payment", "amount": 50},
            ]),
            distributors=_Collection([{"id": "dist-1", "opening_balance": 80.129}]),
            distributor_transactions=_Collection([
                {"distributor_id": "dist-1", "type": "payment", "amount": 20},
            ]),
            purchase_orders=_Collection([]),
            regular_patients=_Collection([]),
        )

        with patch("server.db", fake_db):
            result = await dashboard_summary(user={"id": "user"})
            report = await expiry_report(user={"id": "user"})

        self.assertEqual(result["customer_receivables"], 75.12)
        self.assertEqual(result["distributor_payables"], 60.13)
        self.assertEqual(result["pending_payment"], 135.25)
        self.assertEqual(
            {item["id"]: item["status"] for item in result["low_stock_items"]},
            {"low-default": "low_stock", "low-reordered": "reordered"},
        )
        self.assertEqual([item["id"] for item in result["expired_items"]], ["recent-expired"])
        self.assertEqual(
            {item["id"] for item in report["expired"]},
            {"recent-expired", "old-expired"},
        )
