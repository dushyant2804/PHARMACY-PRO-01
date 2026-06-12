import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from fastapi import HTTPException
from server import StockAdjustmentCreate, create_stock_adjustment, stock_adjustment_summary


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    def sort(self, *args, **kwargs):
        return self

    def skip(self, amount):
        self.rows = self.rows[amount:]
        return self

    def limit(self, amount):
        self.rows = self.rows[:amount]
        return self

    async def to_list(self, length):
        rows = self.rows if length is None else self.rows[:length]
        return [dict(row) for row in rows]


class Collection:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    @staticmethod
    def _matches(row, query):
        for key, value in (query or {}).items():
            if key == "$expr":
                continue
            if key == "$or":
                if not any(Collection._matches(row, condition) for condition in value):
                    return False
                continue
            if isinstance(value, dict):
                actual = row.get(key)
                if "$gte" in value and actual < value["$gte"]:
                    return False
                if "$lte" in value and actual > value["$lte"]:
                    return False
                continue
            if row.get(key) != value:
                return False
        return True

    async def find_one(self, query, *args, **kwargs):
        return next((row for row in self.rows if self._matches(row, query)), None)

    def find(self, query=None, *args, **kwargs):
        return Cursor([row for row in self.rows if self._matches(row, query or {})])

    async def count_documents(self, query, *args, **kwargs):
        return len([row for row in self.rows if self._matches(row, query)])

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(matched_count=0, modified_count=0)
        row.update(update.get("$set", {}))
        return SimpleNamespace(matched_count=1, modified_count=1)

    async def insert_one(self, document, *args, **kwargs):
        self.rows.append(dict(document))
        return SimpleNamespace(inserted_id=document["id"])


async def fallback_only(operation, fallback):
    return await fallback()


class StockAdjustmentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.medicine = {
            "id": "med-1",
            "name": "Qutan 50",
            "batch_no": "B1",
            "expiry_date": "2027-12-31",
            "purchased_units": 10,
            "sold_units": 2,
            "purchase_return_units": 1,
            "low_stock_threshold": 5,
        }
        self.fake_db = SimpleNamespace(
            medicines=Collection([self.medicine]),
            stock_adjustments=Collection(),
        )
        self.user = {"id": "user-1", "name": "Pharmacist", "role": "pharmacist"}

    def payload(self, quantity, adjustment_type="Manual Correction"):
        return StockAdjustmentCreate(
            adjustment_date="2026-06-12",
            medicine_id="med-1",
            medicine_name="Qutan 50",
            batch_no="B1",
            adjustment_type=adjustment_type,
            quantity=quantity,
            notes="Count correction",
            reference_number="ADJ-1",
        )

    async def create(self, quantity, adjustment_type="Manual Correction"):
        with patch("server.db", self.fake_db), patch(
            "server._run_with_transaction", side_effect=fallback_only
        ):
            return await create_stock_adjustment(self.payload(quantity, adjustment_type), self.user)

    async def test_positive_adjustment_adds_stock_and_records_audit_entry(self):
        result = await self.create(3)

        self.assertEqual(self.medicine["stock_adjustment_units"], 3)
        self.assertEqual(self.medicine["available_stock"], 10)
        self.assertFalse(self.medicine["is_low_stock"])
        self.assertEqual(result["medicine_name"], "Qutan 50")
        self.assertEqual(result["created_by"], "Pharmacist")
        self.assertEqual(len(self.fake_db.stock_adjustments.rows), 1)

    async def test_negative_adjustment_reduces_selected_batch_and_recalculates_status(self):
        await self.create(-7, "Damaged")

        self.assertEqual(self.medicine["stock_adjustment_units"], -7)
        self.assertEqual(self.medicine["available_stock"], 0)
        self.assertTrue(self.medicine["is_low_stock"])
        self.assertEqual(self.medicine["status"], "Returned")
        self.assertEqual(self.medicine["expiry_status"], "safe")

    async def test_over_reduction_is_rejected_without_audit_record(self):
        with self.assertRaises(HTTPException) as caught:
            await self.create(-8, "Theft/Loss")

        self.assertEqual(caught.exception.status_code, 400)
        self.assertNotIn("stock_adjustment_units", self.medicine)
        self.assertEqual(self.fake_db.stock_adjustments.rows, [])

    async def test_summary_reports_directional_and_type_totals(self):
        self.fake_db.stock_adjustments.rows.extend([
            {"adjustment_type": "Manual Correction", "quantity": 4},
            {"adjustment_type": "Damaged", "quantity": -2},
            {"adjustment_type": "Damaged", "quantity": -1},
        ])
        with patch("server.db", self.fake_db):
            result = await stock_adjustment_summary(user=self.user)

        self.assertEqual(result["total_adjustments"], 3)
        self.assertEqual(result["positive_quantity"], 4)
        self.assertEqual(result["negative_quantity"], -3)
        self.assertEqual(result["net_quantity"], 1)
        self.assertEqual(result["by_type"]["Damaged"], {"count": 2, "net_quantity": -3})


if __name__ == "__main__":
    unittest.main()
