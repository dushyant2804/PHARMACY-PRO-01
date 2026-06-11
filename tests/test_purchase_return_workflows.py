import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from fastapi import HTTPException
from server import (
    POCreate,
    POItem,
    POReturnCreditRow,
    PurchaseReturnUpdate,
    _resolve_po_purchase_returns,
    delete_purchase_return,
    update_purchase_return,
)


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    async def to_list(self, length):
        return [dict(row) for row in self.rows[:length]]


class Collection:
    def __init__(self, rows):
        self.rows = rows

    def find(self, query=None, *args, **kwargs):
        return Cursor(self.rows)

    async def find_one(self, query, *args, **kwargs):
        if "$or" in query:
            for condition in query["$or"]:
                found = await self.find_one(condition)
                if found:
                    return found
            return None
        return next((row for row in self.rows if all(row.get(k) == v for k, v in query.items())), None)

    async def insert_one(self, row, *args, **kwargs):
        self.rows.append(dict(row))
        return SimpleNamespace(inserted_id=row.get("id"))

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one({k: v for k, v in query.items() if not k.startswith("$")})
        if not row:
            return SimpleNamespace(modified_count=0)
        target = next(item for item in self.rows if item.get("id") == row.get("id"))
        target.update(update.get("$set", {}))
        for key, amount in update.get("$inc", {}).items():
            target[key] = target.get(key, 0) + amount
        return SimpleNamespace(modified_count=1)

    async def delete_one(self, query, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(deleted_count=0)
        self.rows.remove(next(item for item in self.rows if item.get("id") == row.get("id")))
        return SimpleNamespace(deleted_count=1)


class PurchaseOrderReturnMergeTests(unittest.IsolatedAsyncioTestCase):
    def payload(self, rows):
        return POCreate(
            distributor_id="dist-1",
            distributor_name="Distributor",
            invoice_ref="INV-1",
            po_date="2026-06-10",
            items=[POItem(name="New stock", batch_no="N1", quantity=1, purchase_price=10, mrp=12)],
            purchase_returns=rows,
        )

    async def test_inline_po_return_matches_existing_physical_return(self):
        existing = {
            "id": "return-1", "distributor_id": "dist-1", "medicine_name": "Qutan 50",
            "medicine_key": "qutan 50::B1", "batch_number": "B1", "expiry_date": "12/27",
            "return_quantity": 2, "purchase_rate": 10, "gst_rate": 5, "reason": "Expired",
        }
        fake_db = SimpleNamespace(purchase_returns=Collection([existing]), medicines=Collection([]))
        row = POReturnCreditRow(medicine_name="Qutan 50", medicine_key="qutan 50::B1", batch_number="B1", expiry_date="12/27", return_quantity=2, purchase_rate=10, gst_rate=5, reason="Expired")
        with patch("server.db", fake_db), patch("server._set_purchase_return_stock_delta", new=AsyncMock()) as stock_delta:
            returns, credit = await _resolve_po_purchase_returns(self.payload([row]))
        self.assertEqual([item["id"] for item in returns], ["return-1"])
        self.assertEqual(credit, 20)
        self.assertEqual(len(fake_db.purchase_returns.rows), 1)
        stock_delta.assert_not_awaited()

    async def test_inline_po_return_without_match_creates_once_without_ledger(self):
        medicine = {"id": "med-1", "name": "Saltum DS", "batch_no": "B2", "medicine_key": "saltum ds::B2", "gst_rate": 5}
        fake_db = SimpleNamespace(purchase_returns=Collection([]), medicines=Collection([medicine]))
        row = POReturnCreditRow(medicine_name="Saltum DS", batch_number="B2", expiry_date="11/27", return_quantity=2, purchase_rate=15)
        with patch("server.db", fake_db), patch("server._set_purchase_return_stock_delta", new=AsyncMock()) as stock_delta:
            returns, credit = await _resolve_po_purchase_returns(self.payload([row]))
        self.assertEqual(len(fake_db.purchase_returns.rows), 1)
        self.assertFalse(returns[0]["ledger_adjusted"])
        self.assertTrue(returns[0]["auto_created_from_po_credit"])
        self.assertEqual(credit, 30)
        stock_delta.assert_awaited_once_with("med-1", 2.0)


class PurchaseReturnEditDeleteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.return_row = {
            "id": "return-1", "medicine_id": "med-1", "return_date": "2026-06-01",
            "return_quantity": 2, "purchase_rate": 10, "return_amount": 20,
            "reason": "Expired", "notes": "", "adjust_distributor_ledger": False,
            "ledger_adjusted": False, "ledger_transaction_id": None,
        }
        self.fake_db = SimpleNamespace(
            purchase_returns=Collection([self.return_row]),
            medicines=Collection([{"id": "med-1", "purchased_units": 5, "sold_units": 1, "purchase_return_units": 2}]),
            distributor_transactions=Collection([]),
        )

    async def test_edit_quantity_applies_only_the_stock_delta(self):
        async def fallback_only(operation, fallback):
            return await fallback()
        with patch("server.db", self.fake_db), patch("server._run_with_transaction", side_effect=fallback_only):
            updated = await update_purchase_return("return-1", PurchaseReturnUpdate(return_quantity=1), {"id": "user"})
        self.assertEqual(updated["return_quantity"], 1)
        self.assertEqual(self.fake_db.medicines.rows[0]["purchase_return_units"], 1)

    async def test_delete_restores_stock_and_linked_return_is_blocked(self):
        async def fallback_only(operation, fallback):
            return await fallback()
        with patch("server.db", self.fake_db), patch("server._run_with_transaction", side_effect=fallback_only):
            await delete_purchase_return("return-1", {"id": "user"})
        self.assertEqual(self.fake_db.medicines.rows[0]["purchase_return_units"], 0)
        self.assertEqual(self.fake_db.purchase_returns.rows, [])

        linked = {**self.return_row, "po_adjustment_id": "po-1"}
        self.fake_db.purchase_returns.rows.append(linked)
        with patch("server.db", self.fake_db):
            with self.assertRaises(HTTPException) as caught:
                await delete_purchase_return("return-1", {"id": "user"})
        self.assertEqual(caught.exception.status_code, 409)
