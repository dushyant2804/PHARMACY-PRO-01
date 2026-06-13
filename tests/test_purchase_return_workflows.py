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
    _available_stock,
    _resolve_po_purchase_returns,
    _po_return_settlement_fields,
    _purchase_return_settlement_status,
    _return_status,
    delete_purchase_return,
    rebuild_inventory,
    recalculate_purchase_return_stock,
    run_stock_repair,
    update_purchase_return,
)


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __aiter__(self):
        async def iterate():
            for row in self.rows:
                yield dict(row)
        return iterate()

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

    async def update_many(self, query, update, *args, **kwargs):
        for row in self.rows:
            row.update(update.get("$set", {}))
        return SimpleNamespace(modified_count=len(self.rows))

    async def delete_many(self, query, *args, **kwargs):
        deleted = len(self.rows)
        self.rows.clear()
        return SimpleNamespace(deleted_count=deleted)

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

    async def test_existing_unadjusted_return_gets_po_settlement_metadata_without_side_effects(self):
        existing = {
            "id": "return-1", "distributor_id": "dist-1", "medicine_name": "Qutan 50",
            "batch_number": "B1", "return_quantity": 2, "purchase_rate": 10,
            "ledger_adjusted": False, "created_at": "2026-06-01T00:00:00+00:00",
            "notes": "preserve me",
        }
        fake_db = SimpleNamespace(purchase_returns=Collection([existing]), medicines=Collection([]))
        row = POReturnCreditRow(id="return-1", medicine_name="Qutan 50", batch_number="B1", return_quantity=2, purchase_rate=10)
        with patch("server.db", fake_db), patch("server._set_purchase_return_stock_delta", new=AsyncMock()) as stock_delta:
            returns, credit = await _resolve_po_purchase_returns(self.payload([row]))
            fake_db.purchase_returns.rows[0].update(_po_return_settlement_fields("po-1", "2026-06-13T00:00:00+00:00"))

        self.assertEqual(len(fake_db.purchase_returns.rows), 1)
        self.assertEqual(returns[0]["id"], "return-1")
        self.assertEqual(credit, 20.0)
        self.assertEqual(fake_db.purchase_returns.rows[0]["created_at"], "2026-06-01T00:00:00+00:00")
        self.assertEqual(fake_db.purchase_returns.rows[0]["notes"], "preserve me")
        self.assertEqual(_purchase_return_settlement_status(fake_db.purchase_returns.rows[0]), "settled_by_po")
        stock_delta.assert_not_awaited()

    async def test_inline_po_return_matches_without_expiry_and_preserves_history(self):
        existing = {
            "id": "return-1", "distributor_id": "dist-1", "medicine_name": "Qutan 50",
            "batch_number": "B1", "return_quantity": 2, "purchase_rate": 10,
            "reason": "Expired", "ledger_adjusted": False,
        }
        fake_db = SimpleNamespace(purchase_returns=Collection([existing]), medicines=Collection([]))
        row = POReturnCreditRow(
            medicine_name="Qutan 50", batch_number="B1", expiry_date="12/27",
            return_quantity=2, purchase_rate=10, reason="Expired",
        )
        with patch("server.db", fake_db), patch("server._set_purchase_return_stock_delta", new=AsyncMock()) as stock_delta:
            returns, credit = await _resolve_po_purchase_returns(self.payload([row]))
        self.assertEqual(returns[0]["id"], "return-1")
        self.assertEqual(credit, 20.0)
        self.assertEqual(fake_db.purchase_returns.rows, [existing])
        stock_delta.assert_not_awaited()

    async def test_ledger_adjusted_return_is_not_consumed_or_duplicated(self):
        existing = {
            "id": "return-1", "distributor_id": "dist-1", "medicine_name": "Qutan 50",
            "batch_number": "B1", "expiry_date": "12/27", "return_quantity": 2,
            "purchase_rate": 10, "ledger_adjusted": True,
        }
        fake_db = SimpleNamespace(purchase_returns=Collection([existing]), medicines=Collection([]))
        row = POReturnCreditRow(id="return-1", medicine_name="Qutan 50", batch_number="B1", return_quantity=2, purchase_rate=10)
        with patch("server.db", fake_db), patch("server._set_purchase_return_stock_delta", new=AsyncMock()) as stock_delta:
            with self.assertRaises(HTTPException) as caught:
                await _resolve_po_purchase_returns(self.payload([row]))
        self.assertEqual(caught.exception.status_code, 409)
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

    async def test_edit_unsettled_return_updates_safe_fields_and_rate(self):
        async def fallback_only(operation, fallback):
            return await fallback()
        payload = PurchaseReturnUpdate(
            return_date="2026-06-02", reason="Damaged", notes="Updated",
            purchase_rate=12, adjust_distributor_ledger=False,
        )
        with patch("server.db", self.fake_db), patch("server._run_with_transaction", side_effect=fallback_only):
            updated = await update_purchase_return("return-1", payload, {"id": "user"})
        self.assertEqual(updated["return_date"], "2026-06-02")
        self.assertEqual(updated["reason"], "Damaged")
        self.assertEqual(updated["notes"], "Updated")
        self.assertEqual(updated["purchase_rate"], 12.0)
        self.assertEqual(updated["return_amount"], 24.0)

    async def test_settled_or_ledger_adjusted_return_cannot_be_edited(self):
        for settlement in (
            {"settlement_status": "settled_by_po", "settled_by_po": "po-1"},
            {"settlement_status": "ledger_adjusted", "ledger_adjusted": True},
        ):
            self.fake_db.purchase_returns.rows[0].update(settlement)
            with patch("server.db", self.fake_db):
                with self.assertRaises(HTTPException) as caught:
                    await update_purchase_return("return-1", PurchaseReturnUpdate(notes="unsafe"), {"id": "user"})
            self.assertEqual(caught.exception.status_code, 409)
            self.assertIn("cannot be edited", caught.exception.detail)
            self.fake_db.purchase_returns.rows[0] = dict(self.return_row)

    async def test_void_restores_stock_preserves_history_and_linked_return_is_blocked(self):
        async def fallback_only(operation, fallback):
            return await fallback()
        with patch("server.db", self.fake_db), patch("server._run_with_transaction", side_effect=fallback_only):
            response = await delete_purchase_return("return-1", {"id": "user"})
        self.assertEqual(response["message"], "Purchase return voided")
        self.assertEqual(self.fake_db.medicines.rows[0]["purchase_return_units"], 0)
        self.assertEqual(len(self.fake_db.purchase_returns.rows), 1)
        self.assertEqual(self.fake_db.purchase_returns.rows[0]["settlement_status"], "voided")
        self.assertEqual(self.fake_db.purchase_returns.rows[0]["return_quantity"], 2)
        self.assertEqual(self.fake_db.purchase_returns.rows[0]["reason"], "Expired")
        self.assertIn("voided_at", self.fake_db.purchase_returns.rows[0])

        linked = {
            **{key: value for key, value in self.return_row.items() if not key.startswith("voided_")},
            "settlement_status": "settled_by_po", "settled_by_po": "po-1",
        }
        self.fake_db.purchase_returns.rows[:] = [linked]
        with patch("server.db", self.fake_db):
            with self.assertRaises(HTTPException) as caught:
                await delete_purchase_return("return-1", {"id": "user"})
        self.assertEqual(caught.exception.status_code, 409)
        self.assertIn("cannot be voided", caught.exception.detail)

    async def test_void_ledger_adjusted_return_keeps_voided_ledger_history(self):
        self.fake_db.purchase_returns.rows[0].update({
            "ledger_adjusted": True, "adjust_distributor_ledger": True,
            "ledger_transaction_id": "txn-1", "settlement_status": "ledger_adjusted",
        })
        self.fake_db.distributor_transactions.rows.append({
            "id": "txn-1", "return_id": "return-1", "amount": 20,
        })

        async def fallback_only(operation, fallback):
            return await fallback()
        with patch("server.db", self.fake_db), patch("server._run_with_transaction", side_effect=fallback_only):
            await delete_purchase_return("return-1", {"name": "Admin"})

        self.assertEqual(len(self.fake_db.distributor_transactions.rows), 1)
        self.assertEqual(self.fake_db.distributor_transactions.rows[0]["voided_by"], "Admin")
        self.assertIn("voided_at", self.fake_db.distributor_transactions.rows[0])
        self.assertFalse(self.fake_db.purchase_returns.rows[0]["ledger_adjusted"])


class PurchaseReturnStockBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_returns_recalculate_qutan_and_saltum_stock(self):
        medicines = [
            {
                "id": "qutan-med", "medicine_key": "qutan::Q1", "name": "Qutan",
                "batch_no": "Q1", "expiry_date": "12/27", "distributor_id": "dist-1",
                "purchased_units": 5, "sold_units": 3, "purchase_return_units": 0,
            },
            {
                "id": "saltum-med", "medicine_key": "saltum::S1", "name": "Saltum",
                "batch_no": "S1", "expiry_date": "2027-11-30", "distributor_id": "dist-2",
                "purchased_units": 4, "sold_units": 2,
            },
        ]
        legacy_returns = [
            {
                "id": "return-qutan", "medicine_name": "Qutan", "batch_number": "Q1",
                "expiry_date": "2027-12-31", "distributor_id": "dist-1", "return_quantity": 2,
            },
            {
                "id": "return-saltum", "medicine_id": "saltum-med", "medicine_name": "Saltum",
                "batch_number": "S1", "return_quantity": 2,
            },
        ]
        fake_db = SimpleNamespace(
            purchase_returns=Collection(legacy_returns),
            medicines=Collection(medicines),
        )

        with patch("server.db", fake_db):
            result = await recalculate_purchase_return_stock()

        self.assertEqual(result["returns_scanned"], 2)
        self.assertEqual(result["unmatched_returns"], [])
        self.assertEqual(result["matched_returns"], 2)
        self.assertEqual(len(fake_db.purchase_returns.rows), 2)
        for medicine in fake_db.medicines.rows:
            self.assertEqual(medicine["purchase_return_units"], 2)
            self.assertEqual(medicine["available_stock"], 0)
            self.assertEqual(medicine["status"], "Returned")
            self.assertEqual(_available_stock(medicine), 0)
            self.assertEqual(_return_status(medicine), "Returned")
        self.assertEqual(fake_db.medicines.rows[0]["purchased_units"], 5)
        self.assertEqual(fake_db.medicines.rows[0]["sold_units"], 3)
        self.assertEqual(fake_db.medicines.rows[1]["purchased_units"], 4)
        self.assertEqual(fake_db.medicines.rows[1]["sold_units"], 2)

    async def test_manual_stock_repair_endpoint_refreshes_legacy_return_status(self):
        fake_db = SimpleNamespace(
            purchase_returns=Collection([{
                "id": "legacy-return", "medicine_name": "Qutan",
                "batch_number": "Q1", "return_quantity": 2,
            }]),
            medicines=Collection([{
                "id": "qutan-med", "name": "Qutan", "batch_no": "Q1",
                "purchased_units": 5, "sold_units": 3,
            }]),
        )

        with patch("server.db", fake_db):
            response = await run_stock_repair({"role": "admin", "tenant_id": "shop-1"})

        medicine = fake_db.medicines.rows[0]
        self.assertEqual(response, {
            "success": True,
            "updated_medicines": 1,
            "matched_returns": 1,
            "unmatched_returns": [],
        })
        self.assertEqual(medicine["available_stock"], 0)
        self.assertEqual(medicine["status"], "Returned")

    async def test_inventory_rebuild_always_recalculates_purchase_returns(self):
        purchase_orders = Collection([{
            "distributor_id": "dist-1",
            "items": [{"medicine_key": "qutan::Q1", "name": "Qutan", "batch_no": "Q1", "quantity": 5}],
        }])
        fake_db = SimpleNamespace(
            purchase_orders=purchase_orders,
            medicines=Collection([{
                "id": "qutan-med", "medicine_key": "qutan::Q1", "name": "Qutan",
                "batch_no": "Q1", "purchased_units": 5, "sold_units": 3,
            }]),
        )

        with patch("server.db", fake_db), patch(
            "server.recalculate_purchase_return_stock", new=AsyncMock()
        ) as recalculate:
            await rebuild_inventory()

        recalculate.assert_awaited_once_with()
