import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import clear_stale_sold_units, list_stale_sold_units, StaleSoldUnitsClearRequest


class Cursor:
    def __init__(self, rows):
        self.rows = rows

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

    def _matches(self, row, query):
        return all(row.get(k) == v for k, v in (query or {}).items())

    def find(self, query=None, *args, **kwargs):
        return Cursor([row for row in self.rows if self._matches(row, query or {})])

    async def find_one(self, query=None, *args, **kwargs):
        return next((row for row in self.rows if self._matches(row, query or {})), None)

    async def update_one(self, query, update, *args, **kwargs):
        row = await self.find_one(query)
        if not row:
            return SimpleNamespace(modified_count=0)
        row.update(update.get("$set", {}))
        return SimpleNamespace(modified_count=1)

    async def insert_one(self, doc, *args, **kwargs):
        self.rows.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get("id"))


class StaleSoldUnitsRepairTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stale = {
            "id": "med-stale",
            "medicine_key": "crocin::B1",
            "name": "Crocin",
            "batch_no": "B1",
            "expiry_date": "2027-01-31",
            "purchased_units": 10,
            "sold_units": 4,
            "purchase_return_units": 1,
            "stock_adjustment_units": -2,
            "available_stock": 3,
            "quantity_units": 3,
        }
        self.backed = {
            "id": "med-backed",
            "medicine_key": "dolo::B2",
            "name": "Dolo",
            "batch_no": "B2",
            "purchased_units": 8,
            "sold_units": 3,
            "purchase_return_units": 0,
            "stock_adjustment_units": 0,
            "available_stock": 5,
        }
        self.fake_db = SimpleNamespace(
            medicines=Collection([self.stale, self.backed]),
            invoices=Collection([{
                "id": "inv-1",
                "stock_deductions": [{
                    "medicine_id": "med-backed",
                    "medicine_key": "dolo::B2",
                    "batch_no": "B2",
                    "deduct": 3,
                }],
            }]),
            audit_logs=Collection(),
        )
        self.admin = {"id": "admin-1", "name": "Owner", "role": "admin"}

    async def test_stale_sold_units_appears_and_invoice_backed_is_excluded(self):
        with patch("server.db", self.fake_db):
            result = await list_stale_sold_units(self.admin)

        self.assertEqual(result["count"], 1)
        row = result["items"][0]
        self.assertEqual(row["medicine_id"], "med-stale")
        self.assertEqual(row["invoice_backed_sold_units"], 0)
        self.assertEqual(row["stale_sold_units"], 4)
        self.assertEqual(row["calculated_stock_if_sold_units_removed"], 7)

    async def test_clear_operation_restores_stock_and_writes_audit_without_touching_returns_or_adjustments(self):
        with patch("server.db", self.fake_db):
            result = await clear_stale_sold_units(
                StaleSoldUnitsClearRequest(medicine_id="med-stale", batch_no="B1", confirm=True),
                self.admin,
            )

        self.assertTrue(result["success"])
        self.assertEqual(self.stale["sold_units"], 0)
        self.assertEqual(self.stale["available_stock"], 7)
        self.assertEqual(self.stale["quantity_units"], 7)
        self.assertEqual(self.stale["purchase_return_units"], 1)
        self.assertEqual(self.stale["stock_adjustment_units"], -2)
        self.assertEqual(len(self.fake_db.audit_logs.rows), 1)
        audit = self.fake_db.audit_logs.rows[0]
        self.assertEqual(audit["action"], "clear_stale_sold_units")
        self.assertEqual(audit["old_sold_units"], 4)
        self.assertEqual(audit["new_stock"], 7)

    async def test_invoice_backed_sold_units_cannot_be_cleared(self):
        with patch("server.db", self.fake_db):
            with self.assertRaises(HTTPException) as caught:
                await clear_stale_sold_units(
                    StaleSoldUnitsClearRequest(medicine_id="med-backed", batch_no="B2", confirm=True),
                    self.admin,
                )

        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(self.backed["sold_units"], 3)
        self.assertEqual(self.fake_db.audit_logs.rows, [])


    async def test_legacy_null_id_row_with_broad_invoice_item_fallback_is_stale_and_clearable(self):
        crocin = {
            "medicine_key": "crocin drops::NC25011",
            "name": "CROCIN DROPS",
            "batch_no": "NC25011",
            "purchased_units": 3,
            "sold_units": 3,
            "purchase_return_units": 0,
            "stock_adjustment_units": 0,
            "available_stock": 0,
            "medicine_id": None,
        }
        fake_db = SimpleNamespace(
            medicines=Collection([crocin]),
            invoices=Collection([{
                "id": "legacy-inv-1",
                "items": [{
                    "name": "CROCIN DROPS",
                    "medicine_key": "crocin drops::NC25011",
                    "quantity": 3,
                }],
            }]),
            audit_logs=Collection(),
        )

        with patch("server.db", fake_db):
            result = await list_stale_sold_units(self.admin)
            clear_result = await clear_stale_sold_units(
                StaleSoldUnitsClearRequest(
                    medicine_id="crocin drops::NC25011",
                    batch_no="NC25011",
                    confirm=True,
                ),
                self.admin,
            )

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["items"][0]["medicine_id"], "crocin drops::NC25011")
        self.assertEqual(result["items"][0]["stale_sold_units"], 3)
        self.assertTrue(clear_result["success"])
        self.assertEqual(crocin["sold_units"], 0)
        self.assertEqual(crocin["available_stock"], 3)

    async def test_legacy_null_id_row_with_exact_invoice_item_batch_is_excluded(self):
        crocin = {
            "medicine_key": "crocin drops::NC25011",
            "name": "CROCIN DROPS",
            "batch_no": "NC25011",
            "purchased_units": 3,
            "sold_units": 3,
            "medicine_id": None,
        }
        fake_db = SimpleNamespace(
            medicines=Collection([crocin]),
            invoices=Collection([{
                "id": "legacy-inv-2",
                "items": [{
                    "medicine_key": "crocin drops::NC25011",
                    "batch_no": "NC25011",
                    "quantity": 3,
                }],
            }]),
            audit_logs=Collection(),
        )

        with patch("server.db", fake_db):
            result = await list_stale_sold_units(self.admin)

        self.assertEqual(result["count"], 0)

    async def test_exact_stock_deduction_still_excludes_legacy_null_id_row(self):
        crocin = {
            "medicine_key": "crocin drops::NC25011",
            "name": "CROCIN DROPS",
            "batch_no": "NC25011",
            "purchased_units": 3,
            "sold_units": 3,
            "medicine_id": None,
        }
        fake_db = SimpleNamespace(
            medicines=Collection([crocin]),
            invoices=Collection([{
                "id": "inv-deduction",
                "stock_deductions": [{
                    "medicine_key": "crocin drops::NC25011",
                    "batch_no": "NC25011",
                    "deduct": 3,
                }],
            }]),
            audit_logs=Collection(),
        )

        with patch("server.db", fake_db):
            result = await list_stale_sold_units(self.admin)

        self.assertEqual(result["count"], 0)

    async def test_confirm_true_is_required(self):
        with patch("server.db", self.fake_db):
            with self.assertRaises(HTTPException) as caught:
                await clear_stale_sold_units(
                    StaleSoldUnitsClearRequest(medicine_id="med-stale", batch_no="B1", confirm=False),
                    self.admin,
                )

        self.assertEqual(caught.exception.status_code, 400)
