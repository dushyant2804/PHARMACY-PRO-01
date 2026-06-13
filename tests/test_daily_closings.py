import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from fastapi import HTTPException
from server import (
    DailyClosingCreate,
    DailyClosingUpdate,
    create_daily_closing,
    get_daily_closing,
    list_daily_closings,
    update_daily_closing,
)


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def sort(self, field, direction):
        self.rows.sort(key=lambda row: row.get(field, ""), reverse=direction < 0)
        return self

    async def to_list(self, length):
        rows = self.rows if length is None else self.rows[:length]
        return [dict(row) for row in rows]


class Collection:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    async def find_one(self, query, *args, **kwargs):
        return next(
            (row for row in self.rows if all(row.get(key) == value for key, value in query.items())),
            None,
        )

    def find(self, query=None, *args, **kwargs):
        query = query or {}
        return Cursor(
            row for row in self.rows if all(row.get(key) == value for key, value in query.items())
        )

    async def insert_one(self, document, *args, **kwargs):
        self.rows.append(dict(document))
        return SimpleNamespace(inserted_id=document["id"])

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(matched_count=0, modified_count=0)
        row.update(update.get("$set", {}))
        return SimpleNamespace(matched_count=1, modified_count=1)


class DailyClosingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.daily_closings = Collection()
        self.daily_sales = Collection([
            {"id": "sale-1", "sale_date": "2026-06-12", "total_amount": 250.0}
        ])
        self.fake_db = SimpleNamespace(
            daily_closings=self.daily_closings,
            daily_sales=self.daily_sales,
        )
        self.cashier = {"id": "cashier-1", "name": "Casey", "role": "cashier"}
        self.pharmacist = {"id": "pharmacist-1", "name": "Pat", "role": "pharmacist"}
        self.admin = {"id": "admin-1", "name": "Alex", "role": "admin"}

    @staticmethod
    def payload(**changes):
        values = {
            "closing_date": "2026-06-12",
            "cash_sales": 500.25,
            "upi_sales": 300,
            "card_sales": 125,
            "credit_sales": 75,
            "expenses": 50,
            "expected_total": 450.25,
            "counted_cash": 445.10,
            "notes": "End of shift",
        }
        values.update(changes)
        return DailyClosingCreate(**values)

    async def create(self, user=None, **changes):
        with patch("server.db", self.fake_db):
            return await create_daily_closing(self.payload(**changes), user or self.cashier)

    async def update(self, closing_id, user=None, **changes):
        with patch("server.db", self.fake_db):
            return await update_daily_closing(
                closing_id, DailyClosingUpdate(**changes), user or self.cashier
            )

    async def test_create_calculates_mismatch_and_preserves_daily_sales(self):
        result = await self.create()

        self.assertEqual(result["mismatch_amount"], 195.10)
        self.assertEqual(result["expected_cash"], 250.0)
        self.assertEqual(result["closing_status"], "excess")
        self.assertEqual(result["created_by"], "Casey")
        self.assertFalse(result["locked"])
        self.assertEqual(len(self.daily_closings.rows), 1)
        self.assertEqual(self.daily_sales.rows[0]["total_amount"], 250.0)

    async def test_update_recalculates_mismatch(self):
        closing = await self.create()

        result = await self.update(closing["id"], counted_cash=451.30, notes="Recounted")

        self.assertEqual(result["mismatch_amount"], 201.30)
        self.assertEqual(result["notes"], "Recounted")

    async def test_cashier_can_lock_then_cannot_edit_locked_closing(self):
        closing = await self.create()
        locked = await self.update(closing["id"], locked=True)
        self.assertTrue(locked["locked"])

        with self.assertRaises(HTTPException) as caught:
            await self.update(closing["id"], notes="Unauthorized edit")

        self.assertEqual(caught.exception.status_code, 403)

    async def test_pharmacist_cannot_edit_locked_closing(self):
        closing = await self.create(locked=True)

        with self.assertRaises(HTTPException) as caught:
            await self.update(closing["id"], user=self.pharmacist, counted_cash=450.25)

        self.assertEqual(caught.exception.status_code, 403)

    async def test_admin_can_edit_and_unlock_locked_closing(self):
        closing = await self.create(locked=True)

        result = await self.update(
            closing["id"], user=self.admin, counted_cash=450.25, locked=False
        )

        self.assertEqual(result["mismatch_amount"], 200.25)
        self.assertFalse(result["locked"])

    async def test_duplicate_date_is_rejected(self):
        await self.create()

        with self.assertRaises(HTTPException) as caught:
            await self.create()

        self.assertEqual(caught.exception.status_code, 409)

    async def test_list_and_get_return_closings(self):
        older = await self.create(closing_date="2026-06-11")
        newer = await self.create(closing_date="2026-06-12")

        with patch("server.db", self.fake_db):
            listed = await list_daily_closings(self.cashier)
            fetched = await get_daily_closing("2026-06-11", self.cashier)

        self.assertEqual([item["id"] for item in listed], [newer["id"], older["id"]])
        self.assertEqual(fetched["id"], older["id"])


if __name__ == "__main__":
    unittest.main()
