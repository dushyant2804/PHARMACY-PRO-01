import os
import re
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import HTTPException

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import reduce_fifo_stock, search_medicines_for_billing


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    def sort(self, field, direction):
        self.rows.sort(key=lambda row: str(row.get(field) or ""), reverse=direction < 0)
        return self

    async def to_list(self, length):
        return [dict(row) for row in self.rows[:length]]


class MedicineCollection:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def find(self, query=None, *args, **kwargs):
        query = query or {}
        self.queries.append(query)
        rows = self.rows
        if "$or" in query:
            rows = [row for row in rows if any(self._matches(row, part) for part in query["$or"])]
        elif "name" in query:
            rows = [row for row in rows if self._matches(row, {"name": query["name"]})]
        return Cursor(rows)

    @staticmethod
    def _matches(row, condition):
        field, expected = next(iter(condition.items()))
        actual = str(row.get(field) or "")
        if isinstance(expected, dict) and "$in" in expected:
            return row.get(field) in expected["$in"]
        if isinstance(expected, dict) and "$regex" in expected:
            flags = re.I if expected.get("$options") == "i" else 0
            return re.search(expected["$regex"], actual, flags) is not None
        return row.get(field) == expected

    async def find_one(self, query, *args, **kwargs):
        return next((row for row in self.rows if row.get("id") == query.get("id")), None)

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(modified_count=0)
        row.update(update["$set"])
        return SimpleNamespace(modified_count=1)


def make_batches():
    return [
        {
            "id": "later", "name": "Amoxyclav 625", "batch_no": "AMX-B2",
            "manufacturer": "HealWell", "barcode": "89010002", "expiry_date": "12/27",
            "purchased_units": 8, "sold_units": 3, "low_stock_threshold": 4,
            "mrp": 120, "gst_rate": 12,
        },
        {
            "id": "earlier", "name": "Amoxyclav 625", "batch_no": "AMX-B1",
            "manufacturer": "HealWell", "barcode": "89010001", "expiry_date": "01/27",
            "purchased_units": 5, "sold_units": 1, "low_stock_threshold": 5,
            "mrp": 110, "gst_rate": 5,
        },
        {
            "id": "other", "name": "Cetirizine", "batch_no": "CTZ-1",
            "manufacturer": "Other Labs", "barcode": "777", "expiry_date": "2028-01-01",
            "purchased_units": 10, "sold_units": 0, "low_stock_threshold": 2,
            "mrp": 20, "gst_rate": 5,
        },
    ]


class QuickCounterBillingTests(unittest.IsolatedAsyncioTestCase):
    async def test_fast_search_matches_supported_autocomplete_fields_and_returns_compact_live_stock(self):
        for term in ("Amoxy", "AMX-B", "Heal", "890100"):
            with self.subTest(term=term):
                medicines = MedicineCollection(make_batches())
                with patch("server.db", SimpleNamespace(medicines=medicines)):
                    response = await search_medicines_for_billing(q=term, limit=10, user={"id": "cashier"})

                self.assertEqual(response, [{
                    "medicine_id": "earlier",
                    "name": "Amoxyclav 625",
                    "available_qty": 9.0,
                    "nearest_expiry": "01/27",
                    "low_stock_threshold": 5,
                    "batch_count": 2,
                    "mrp": 110,
                    "gst": 5,
                }])
                self.assertEqual(set(response[0]), {
                    "medicine_id", "name", "available_qty", "nearest_expiry",
                    "low_stock_threshold", "batch_count", "mrp", "gst",
                })

    async def test_fifo_billing_deducts_nearest_expiry_first_and_rejects_overselling(self):
        batches = make_batches()
        medicines = MedicineCollection(batches)
        with patch("server.db", SimpleNamespace(medicines=medicines)):
            await reduce_fifo_stock("Amoxyclav 625", 6)

            self.assertEqual(batches[1]["sold_units"], 5.0)
            self.assertEqual(batches[1]["available_stock"], 0.0)
            self.assertEqual(batches[0]["sold_units"], 5.0)
            self.assertEqual(batches[0]["available_stock"], 3.0)

            before = [(row["id"], row.get("sold_units")) for row in batches]
            with self.assertRaises(HTTPException) as raised:
                await reduce_fifo_stock("Amoxyclav 625", 4)

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(
            raised.exception.detail,
            "Requested quantity (4) for Amoxyclav 625 exceeds available stock (3)",
        )
        self.assertEqual([(row["id"], row.get("sold_units")) for row in batches], before)
