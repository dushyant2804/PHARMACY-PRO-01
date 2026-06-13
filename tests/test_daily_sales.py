import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import (
    DailySaleCreate, DailySaleUpdate, create_daily_sale, daily_sales_summary,
    delete_daily_sale, list_daily_sales, update_daily_sale,
)


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def sort(self, *args):
        return self

    async def to_list(self, length):
        return [dict(row) for row in self.rows[:length]]


class Collection:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def find(self, query=None, *args, **kwargs):
        query = query or {}
        return Cursor(row for row in self.rows if all(row.get(k) == v for k, v in query.items()))

    async def find_one(self, query, *args, **kwargs):
        return next((row for row in self.rows if all(row.get(k) == v for k, v in query.items())), None)

    async def insert_one(self, document):
        self.rows.append(dict(document))
        return SimpleNamespace(inserted_id=document["id"])

    async def delete_one(self, query):
        row = await self.find_one(query)
        if row:
            self.rows.remove(row)
        return SimpleNamespace(deleted_count=int(row is not None))

    async def update_one(self, query, update):
        row = await self.find_one(query)
        row.update(update["$set"])
        return SimpleNamespace(modified_count=1)


class DailySalesTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.medicines = Collection([{"id": "med-1", "name": "Medicine", "available_stock": 20}])
        self.sales = Collection()
        self.historical = Collection()
        self.expenses = Collection()
        self.db = SimpleNamespace(
            medicines=self.medicines,
            daily_sales=self.sales,
            historical_sales=self.historical,
            expenses=self.expenses,
        )
        self.user = {"name": "Alex", "role": "admin"}

    async def test_create_update_and_delete_do_not_mutate_inventory(self):
        before = dict(self.medicines.rows[0])
        with patch("server.db", self.db):
            sale = await create_daily_sale(
                DailySaleCreate(medicine_id="med-1", quantity=3, total_amount=45), self.user
            )
            updated = await update_daily_sale(
                sale["id"], DailySaleUpdate(cash_sales=50), self.user
            )
            await delete_daily_sale(sale["id"], self.user)

        self.assertEqual(self.medicines.rows[0], before)
        self.assertEqual(sale["cash_sales"], 45)
        self.assertEqual(updated["gross_sales"], 50)

    async def test_summary_calculates_splits_and_expenses(self):
        with patch("server.db", self.db):
            await create_daily_sale(
                DailySaleCreate(
                    sale_date="2026-06-12", cash_sales=100, upi_sales=75,
                    outstanding_sales=25, card_sales=10,
                ),
                self.user,
            )
            self.expenses.rows.append({"date": "2026-06-12", "amount": 30})
            summary = await daily_sales_summary("2026-06-12", self.user)

        self.assertEqual(summary["gross_sales"], 210)
        self.assertEqual(summary["total_paid"], 185)
        self.assertEqual(summary["total_outstanding"], 25)
        self.assertEqual(summary["total_expenses"], 30)
        self.assertEqual(summary["estimated_net_profit"], 180)

    async def test_historical_and_legacy_entries_remain_readable(self):
        self.sales.rows.append({
            "id": "legacy-live", "sale_date": "2026-06-11", "total_amount": 40,
            "payment_status": "pending", "created_at": "2026-06-11T00:00:00+00:00",
        })
        self.historical.rows.append({
            "id": "historical", "date": "2026-06-11", "cash_amount": 50,
            "upi_amount": 20, "pending_amount": 5, "total_amount": 75,
        })
        with patch("server.db", self.db):
            listed = await list_daily_sales("2026-06-11", self.user)
            summary = await daily_sales_summary("2026-06-11", self.user)

        self.assertEqual(listed[0]["outstanding_sales"], 40)
        self.assertEqual(listed[0]["gross_sales"], 40)
        self.assertEqual(summary["gross_sales"], 115)
        self.assertEqual(summary["total_outstanding"], 45)


if __name__ == "__main__":
    unittest.main()
