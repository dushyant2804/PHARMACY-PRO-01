import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import LowStockStatusUpdate, dashboard_summary, list_medicines, update_low_stock_status


class _Cursor:
    def __init__(self, records):
        self.records = records

    async def to_list(self, length):
        return list(self.records if length is None else self.records[:length])


class _UpdateResult:
    matched_count = 1


class _Collection:
    def __init__(self, records):
        self.records = records

    def find(self, query=None, projection=None):
        return _Cursor(self.records)

    async def update_one(self, query, update):
        identity = query.get("id") or query["$or"][0]["id"]
        target = next(row for row in self.records if row.get("id") == identity)
        target.update(update["$set"])
        return _UpdateResult()


class LowStockWorkflowTest(unittest.IsolatedAsyncioTestCase):
    async def test_update_status_persists_without_changing_stock(self):
        medicine = {"id": "med-1", "purchased_units": 10, "sold_units": 3, "low_stock_status": "low_stock"}
        fake_db = SimpleNamespace(medicines=_Collection([medicine]))

        with patch("server.db", fake_db):
            result = await update_low_stock_status("med-1", LowStockStatusUpdate(status="reordered"), user={})

        self.assertEqual(result["low_stock_status"], "reordered")
        self.assertEqual(medicine["low_stock_status"], "reordered")
        self.assertEqual(medicine["purchased_units"], 10)
        self.assertEqual(medicine["sold_units"], 3)

    async def test_inventory_returns_default_and_persisted_status(self):
        medicines = _Collection([
            {"id": "med-1", "name": "One", "batch_no": "B1", "purchased_units": 2, "low_stock_threshold": 5},
            {"id": "med-2", "name": "Two", "batch_no": "B2", "purchased_units": 2, "low_stock_threshold": 5, "low_stock_status": "abandoned"},
        ])
        fake_db = SimpleNamespace(medicines=medicines, purchase_orders=_Collection([]), distributors=_Collection([]))

        with patch("server.db", fake_db):
            result = await list_medicines(user={})

        by_id = {row["id"]: row for row in result}
        self.assertEqual(by_id["med-1"]["low_stock_status"], "low_stock")
        self.assertEqual(by_id["med-1"]["batches"][0]["low_stock_status"], "low_stock")
        self.assertEqual(by_id["med-2"]["low_stock_status"], "abandoned")

    async def test_dashboard_returns_low_stock_status(self):
        empty = _Collection([])
        fake_db = SimpleNamespace(
            medicines=_Collection([
                {"id": "med-1", "name": "One", "purchased_units": 1, "low_stock_threshold": 5, "low_stock_status": "reordered", "purchase_price": 1},
                {"medicine_key": "legacy-med-2", "name": "Two", "purchased_units": 1, "low_stock_threshold": 5, "purchase_price": 1},
            ]),
            invoices=empty, expenses=empty, customer_transactions=empty, distributors=empty,
            distributor_transactions=empty, purchase_orders=empty, regular_patients=empty,
        )
        with patch("server.db", fake_db):
            result = await dashboard_summary(user={})

        by_name = {item["name"]: item for item in result["low_stock_items"]}
        self.assertEqual(by_name["One"]["status"], "reordered")
        self.assertEqual(by_name["One"]["low_stock_status"], "reordered")
        self.assertEqual(by_name["One"]["medicine_id"], "med-1")
        self.assertEqual(by_name["One"]["id"], "med-1")
        self.assertEqual(by_name["One"]["_id"], "med-1")
        self.assertEqual(by_name["Two"]["medicine_id"], "legacy-med-2")
        self.assertEqual(by_name["Two"]["id"], "legacy-med-2")
        self.assertEqual(by_name["Two"]["_id"], "legacy-med-2")

    def test_invalid_status_rejected(self):
        with self.assertRaises(ValidationError):
            LowStockStatusUpdate(status="ordered")


if __name__ == "__main__":
    unittest.main()
