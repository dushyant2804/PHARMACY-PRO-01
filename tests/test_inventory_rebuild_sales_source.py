import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import rebuild_inventory


class Cursor:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def __aiter__(self):
        self._iter = iter(self.rows)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def to_list(self, length):
        return self.rows if length is None else self.rows[:length]


class Collection:
    def __init__(self, rows=None):
        self.rows = rows or []

    def find(self, query=None, *args, **kwargs):
        query = query or {}
        rows = self.rows
        for key, expected in query.items():
            rows = [row for row in rows if row.get(key) == expected]
        return Cursor(rows)

    async def update_one(self, query, update, upsert=False, *args, **kwargs):
        row = next((row for row in self.rows if all(row.get(k) == v for k, v in query.items())), None)
        if row is None:
            if upsert:
                row = dict(query)
                self.rows.append(row)
            else:
                return SimpleNamespace(modified_count=0)
        row.update(update.get("$set", {}))
        return SimpleNamespace(modified_count=1)


class InventoryRebuildSalesSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rebuild_drops_stale_manual_sold_units_without_invoice_deduction(self):
        medicines = Collection([{
            "id": "crocin-drops-b1",
            "medicine_key": "crocin drops::B1",
            "name": "Crocin Drops",
            "batch_no": "B1",
            "purchased_units": 3,
            "sold_units": 3,
            "available_stock": 0,
        }])
        purchase_orders = Collection([{"items": [{
            "medicine_key": "crocin drops::B1",
            "name": "Crocin Drops",
            "batch_no": "B1",
            "quantity": 3,
        }]}])
        fake_db = SimpleNamespace(
            medicines=medicines,
            purchase_orders=purchase_orders,
            invoices=Collection([]),
            purchase_returns=Collection([]),
        )

        with patch("server.db", fake_db):
            await rebuild_inventory()

        self.assertEqual(medicines.rows[0]["sold_units"], 0)
        self.assertEqual(medicines.rows[0]["available_stock"], 3)

    async def test_rebuild_preserves_invoice_backed_stock_deductions(self):
        medicines = Collection([{
            "id": "crocin-drops-b1",
            "medicine_key": "crocin drops::B1",
            "name": "Crocin Drops",
            "batch_no": "B1",
            "purchased_units": 3,
            "sold_units": 0,
        }])
        purchase_orders = Collection([{"items": [{
            "medicine_key": "crocin drops::B1",
            "name": "Crocin Drops",
            "batch_no": "B1",
            "quantity": 3,
        }]}])
        invoices = Collection([{
            "id": "inv-1",
            "invoice_no": "INV-1",
            "stock_deductions": [{
                "medicine_id": "crocin-drops-b1",
                "medicine_key": "crocin drops::B1",
                "batch_no": "B1",
                "medicine_name": "Crocin Drops",
                "deduct": 3,
            }],
        }])
        fake_db = SimpleNamespace(
            medicines=medicines,
            purchase_orders=purchase_orders,
            invoices=invoices,
            purchase_returns=Collection([]),
        )

        with patch("server.db", fake_db):
            await rebuild_inventory()

        self.assertEqual(medicines.rows[0]["sold_units"], 3)
        self.assertEqual(medicines.rows[0]["available_stock"], 0)
