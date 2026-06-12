import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import (
    _available_stock,
    _set_rounded_stock_delta,
    list_medicines,
    round_qty,
    update_sold_units,
)


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, length):
        return [dict(row) for row in self.rows[:length]]


class Collection:
    def __init__(self, rows):
        self.rows = rows

    def find(self, query=None, *args, **kwargs):
        name = (query or {}).get("name")
        return Cursor([row for row in self.rows if not name or row.get("name") == name])

    async def find_one(self, query, *args, **kwargs):
        if "$or" in query:
            for condition in query["$or"]:
                found = await self.find_one(condition)
                if found:
                    return found
            return None
        return next((row for row in self.rows if all(row.get(k) == v for k, v in query.items() if not k.startswith("$"))), None)

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(modified_count=0)
        row.update(update["$set"])
        return SimpleNamespace(modified_count=1)


class QuantityPrecisionTests(unittest.IsolatedAsyncioTestCase):
    def test_available_stock_rounds_floating_artifact_and_clamps_near_zero(self):
        self.assertEqual(_available_stock({"purchased_units": 7.0, "sold_units": 0.5000000003}), 6.5)
        self.assertEqual(round_qty(-0.00000001), 0.0)

    async def test_stock_deduction_stores_one_decimal(self):
        medicines = Collection([{"id": "med-1", "purchased_units": 10.0, "sold_units": 6.3}])
        with patch("server.db", SimpleNamespace(medicines=medicines)):
            result = await _set_rounded_stock_delta("med-1", "sold_units", 0.2, require_available=True)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(medicines.rows[0]["sold_units"], 6.5)
        self.assertEqual(medicines.rows[0]["available_stock"], 3.5)

    async def test_manual_sold_fifo_allocation_stores_one_decimal(self):
        medicines = Collection([
            {"id": "a", "name": "Decimal Med", "expiry_date": "2027-01-01", "purchased_units": 0.3, "sold_units": 0},
            {"id": "b", "name": "Decimal Med", "expiry_date": "2028-01-01", "purchased_units": 1.0, "sold_units": 0},
        ])

        async def fallback_only(operation, fallback):
            return await fallback()

        with patch("server.db", SimpleNamespace(medicines=medicines)), patch("server._run_with_transaction", side_effect=fallback_only):
            response = await update_sold_units("a", {"sold_units": 0.8}, {"role": "admin"})

        self.assertEqual([row["sold_units"] for row in medicines.rows], [0.3, 0.5])
        self.assertEqual(response["sold_units"], 0.8)

    async def test_inventory_api_response_contains_only_normalized_quantities(self):
        medicines = Collection([{
            "id": "med-1", "name": "Precision", "batch_no": "B1", "expiry_date": "2028-01-01",
            "purchased_units": 7.0000000001, "sold_units": 0.5000000003, "purchase_return_units": 0.0,
        }])
        fake_db = SimpleNamespace(medicines=medicines, purchase_orders=Collection([]), distributors=Collection([]))
        with patch("server.db", fake_db):
            response = await list_medicines(user={"id": "user"})

        self.assertEqual(response[0]["total_stock"], 6.5)
        self.assertEqual(response[0]["batches"][0]["available_stock"], 6.5)
        self.assertNotIn("499999", json.dumps(response))
        self.assertNotIn("0000000", json.dumps(response))


if __name__ == "__main__":
    unittest.main()
