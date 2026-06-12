import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from fastapi import HTTPException
from server import _manual_sold_allocations, update_sold_units


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    async def to_list(self, length):
        rows = self.rows if length is None else self.rows[:length]
        return [dict(row) for row in rows]


class Collection:
    def __init__(self, rows):
        self.rows = rows

    @staticmethod
    def _matches(row, query):
        if "$or" in query:
            return any(Collection._matches(row, condition) for condition in query["$or"])
        return all(row.get(key) == value for key, value in query.items())

    def find(self, query=None, *args, **kwargs):
        query = query or {}
        return Cursor([row for row in self.rows if self._matches(row, query)])

    async def find_one(self, query, *args, **kwargs):
        return next((row for row in self.rows if self._matches(row, query)), None)

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(matched_count=0, modified_count=0)
        row.update(update.get("$set", {}))
        return SimpleNamespace(matched_count=1, modified_count=1)


async def fallback_only(operation, fallback):
    return await fallback()


class ManualSoldFifoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.batches = [
            {
                "id": "batch-b", "name": "Aerocort Inhaler", "batch_no": "B",
                "expiry_date": "2028-01-31", "purchased_units": 5, "sold_units": 0,
            },
            {
                "id": "batch-a", "name": "Aerocort Inhaler", "batch_no": "A",
                "expiry_date": "2027-01-31", "purchased_units": 2, "sold_units": 0,
            },
        ]
        self.fake_db = SimpleNamespace(medicines=Collection(self.batches))

    async def update(self, sold_units):
        with patch("server.db", self.fake_db), patch(
            "server._run_with_transaction", side_effect=fallback_only
        ):
            return await update_sold_units("batch-a", {"sold_units": sold_units}, {"role": "admin"})

    async def test_manual_sold_quantity_distributes_across_batches_fifo(self):
        response = await self.update(7)

        batch_a = next(batch for batch in self.batches if batch["id"] == "batch-a")
        batch_b = next(batch for batch in self.batches if batch["id"] == "batch-b")
        self.assertEqual(batch_a["sold_units"], 2)
        self.assertEqual(batch_b["sold_units"], 5)
        self.assertEqual(batch_a["available_stock"], 0)
        self.assertEqual(batch_b["available_stock"], 0)
        self.assertEqual(batch_a["status"], "Sold Out")
        self.assertEqual(batch_b["status"], "Sold Out")
        self.assertEqual(response["sold_units"], 7)
        self.assertEqual(response["available_stock"], 0)
        self.assertEqual([batch["sold_units"] for batch in response["batches"]], [2, 5])

    async def test_reduction_reverses_from_newest_allocated_batch_first(self):
        await self.update(7)
        response = await self.update(3)

        batch_a = next(batch for batch in self.batches if batch["id"] == "batch-a")
        batch_b = next(batch for batch in self.batches if batch["id"] == "batch-b")
        self.assertEqual(batch_a["sold_units"], 2)
        self.assertEqual(batch_b["sold_units"], 1)
        self.assertEqual(batch_a["available_stock"], 0)
        self.assertEqual(batch_b["available_stock"], 4)
        self.assertEqual(response["available_stock"], 4)

    async def test_returns_reduce_capacity_and_refresh_partial_return_status(self):
        self.batches[1]["purchase_return_units"] = 1
        response = await self.update(2)

        batch_a = next(batch for batch in self.batches if batch["id"] == "batch-a")
        batch_b = next(batch for batch in self.batches if batch["id"] == "batch-b")
        self.assertEqual(batch_a["sold_units"], 1)
        self.assertEqual(batch_a["return_status"], "Returned")
        self.assertEqual(batch_b["sold_units"], 1)
        self.assertEqual(response["return_status"], "Partially Returned")

    async def test_rejects_negative_and_oversold_quantities_without_mutation(self):
        for invalid in (-1, 8, float("inf")):
            with self.subTest(invalid=invalid), self.assertRaises(HTTPException):
                await self.update(invalid)
            self.assertEqual([batch["sold_units"] for batch in self.batches], [0, 0])

    def test_fifo_allocation_handles_legacy_purchased_plus_free_fields(self):
        batches = [{
            "id": "legacy", "expiry_date": "2027-01-01",
            "purchased_quantity": 2, "free_quantity": 1,
        }]
        allocations = _manual_sold_allocations(batches, 3)
        self.assertEqual(allocations[0][1], 3)

    def test_corrupt_negative_return_never_increases_batch_capacity(self):
        batch = {"id": "batch", "purchased_units": 2, "purchase_return_units": -3}
        allocations = _manual_sold_allocations([batch], 2)
        self.assertEqual(allocations[0][1], 2)
        with self.assertRaises(HTTPException):
            _manual_sold_allocations([batch], 3)


if __name__ == "__main__":
    unittest.main()
