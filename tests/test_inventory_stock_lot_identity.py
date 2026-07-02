import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import POCreate, PurchaseReturnCreate, _apply_po_inventory_delta, _find_purchase_return_medicine, _rebuild_inventory_for_po_medicines, list_medicines, update_po


class Cursor:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    async def to_list(self, length):
        return self.rows if length is None else self.rows[:length]


class Collection:
    def __init__(self, rows=None):
        self.rows = rows or []

    def find(self, query=None, *args, **kwargs):
        query = query or {}
        rows = self.rows
        if "$or" in query:
            rows = [row for row in rows if any(self._matches(row, condition) for condition in query["$or"])]
        else:
            rows = [row for row in rows if self._matches(row, query)]
        return Cursor(rows)

    async def find_one(self, query=None, *args, **kwargs):
        rows = await self.find(query, *args, **kwargs).to_list(1)
        return rows[0] if rows else None

    async def update_one(self, query, update, upsert=False, *args, **kwargs):
        row = next((row for row in self.rows if self._matches(row, query)), None)
        if row is None:
            if not upsert:
                return SimpleNamespace(modified_count=0, matched_count=0)
            row = {key: value for key, value in query.items() if not key.startswith("$")}
            self.rows.append(row)
        row.update(update.get("$set", {}))
        return SimpleNamespace(modified_count=1, matched_count=1)

    async def update_many(self, query, update, *args, **kwargs):
        count = 0
        for row in self.rows:
            if self._matches(row, query):
                row.update(update.get("$set", {}))
                for key in update.get("$unset", {}):
                    row.pop(key, None)
                count += 1
        return SimpleNamespace(modified_count=count, matched_count=count)

    async def delete_one(self, query, *args, **kwargs):
        before = len(self.rows)
        self.rows = [row for row in self.rows if not self._matches(row, query)]
        return SimpleNamespace(deleted_count=before - len(self.rows))

    def _matches(self, row, query):
        for key, expected in (query or {}).items():
            if key.startswith("$"):
                continue
            if isinstance(expected, dict) and "$in" in expected:
                if row.get(key) not in expected["$in"]:
                    return False
            elif row.get(key) != expected:
                return False
        return True


def medicine(id_, name="Medicine A", batch="ABC123", distributor_id="dist-1", distributor="Distributor 1", qty=1, **extra):
    row = {
        "id": id_,
        "medicine_key": id_,
        "name": name,
        "batch_no": batch,
        "distributor_id": distributor_id,
        "distributor_name": distributor,
        "distributor": distributor,
        "expiry_date": "2028-01-01",
        "pack_size": "10 tabs",
        "purchase_price": 10,
        "mrp": 20,
        "purchased_units": qty,
        "sold_units": 0,
        "purchase_return_units": 0,
    }
    row.update(extra)
    return row


class InventoryStockLotIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def inventory_response(self, rows):
        fake_db = SimpleNamespace(medicines=Collection(rows), purchase_orders=Collection([]), distributors=Collection([]))
        with patch("server.db", fake_db):
            return await list_medicines(user={"id": "tester"})

    async def test_same_medicine_same_distributor_same_batch_merges_main_and_details(self):
        response = await self.inventory_response([
            medicine("lot-1", qty=2),
            medicine("lot-2", qty=3),
        ])
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["total_stock"], 5)
        self.assertEqual(len(response[0]["batches"]), 1)
        self.assertEqual(response[0]["batches"][0]["available_stock"], 5)
        self.assertEqual(response[0]["batches"][0]["distributor_id"], "dist-1")

    async def test_same_medicine_same_distributor_different_batch_keeps_detail_lots(self):
        response = await self.inventory_response([
            medicine("lot-1", batch="ABC123", qty=2),
            medicine("lot-2", batch="DEF456", qty=3),
        ])
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["total_stock"], 5)
        self.assertEqual(len(response[0]["batches"]), 2)
        self.assertEqual({lot["batch_no"] for lot in response[0]["batches"]}, {"ABC123", "DEF456"})

    async def test_same_medicine_different_distributor_same_batch_keeps_detail_lots(self):
        response = await self.inventory_response([
            medicine("lot-1", distributor_id="dist-1", distributor="Distributor 1", qty=2),
            medicine("lot-2", distributor_id="dist-2", distributor="Distributor 2", qty=8),
        ])
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["total_stock"], 10)
        self.assertEqual(len(response[0]["batches"]), 2)
        self.assertEqual({lot["distributor_id"] for lot in response[0]["batches"]}, {"dist-1", "dist-2"})

    async def test_same_medicine_different_distributor_different_batch_keeps_detail_lots(self):
        response = await self.inventory_response([
            medicine("lot-1", distributor_id="dist-1", distributor="Distributor 1", batch="ABC123", qty=2),
            medicine("lot-2", distributor_id="dist-2", distributor="Distributor 2", batch="DEF456", qty=8),
        ])
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["total_stock"], 10)
        self.assertEqual(len(response[0]["batches"]), 2)

    async def test_po_intake_preserves_distributor_context_in_stock_lot_key(self):
        fake_db = SimpleNamespace(medicines=Collection([]))
        payload_1 = POCreate(distributor_id="dist-1", distributor_name="Distributor 1", invoice_ref="A", items=[{"name": "Medicine A", "batch_no": "ABC123", "quantity": 2, "purchase_price": 10, "mrp": 20}])
        payload_2 = POCreate(distributor_id="dist-2", distributor_name="Distributor 2", invoice_ref="B", items=[{"name": "Medicine A", "batch_no": "ABC123", "quantity": 8, "purchase_price": 10, "mrp": 20}])
        po_1 = {"distributor_id": payload_1.distributor_id, "distributor_name": payload_1.distributor_name, "items": [{**payload_1.items[0].model_dump(), "medicine_key": "medicine a::dist-1::ABC123::-::-::10.0::20.0"}]}
        po_2 = {"distributor_id": payload_2.distributor_id, "distributor_name": payload_2.distributor_name, "items": [{**payload_2.items[0].model_dump(), "medicine_key": "medicine a::dist-2::ABC123::-::-::10.0::20.0"}]}
        with patch("server.db", fake_db):
            await _apply_po_inventory_delta(po_1)
            await _apply_po_inventory_delta(po_2)
        self.assertEqual(len(fake_db.medicines.rows), 2)
        self.assertEqual({row["distributor_id"] for row in fake_db.medicines.rows}, {"dist-1", "dist-2"})


    async def test_deleted_medicine_inventory_rebuilt_from_all_old_pos(self):
        po_1 = {"id": "po-1", "distributor_id": "dist-1", "distributor_name": "Distributor 1", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 2, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "medicine_key": "medicine a::dist-1::ABC123::-::-::10.0::20.0"}]}
        po_2 = {"id": "po-2", "distributor_id": "dist-2", "distributor_name": "Distributor 2", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 8, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "medicine_key": "medicine a::dist-2::ABC123::-::-::10.0::20.0"}]}
        fake_db = SimpleNamespace(medicines=Collection([]), purchase_orders=Collection([po_1, po_2]), distributors=Collection([]))
        with patch("server.db", fake_db):
            await _rebuild_inventory_for_po_medicines({"Medicine A"})
            response = await list_medicines(user={"id": "tester"})
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["total_stock"], 10)
        self.assertEqual(len(response[0]["batches"]), 2)
        lots = {lot["distributor_id"]: lot["available_stock"] for lot in response[0]["batches"]}
        self.assertEqual(lots, {"dist-1": 2, "dist-2": 8})

    async def test_rebuild_merges_same_distributor_same_batch_lot(self):
        po_1 = {"id": "po-1", "distributor_id": "dist-1", "distributor_name": "Distributor 1", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 2, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "medicine_key": "medicine a::dist-1::ABC123::-::-::10.0::20.0"}]}
        po_2 = {"id": "po-2", "distributor_id": "dist-1", "distributor_name": "Distributor 1", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 8, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "medicine_key": "medicine a::dist-1::ABC123::-::-::10.0::20.0"}]}
        fake_db = SimpleNamespace(medicines=Collection([]), purchase_orders=Collection([po_1, po_2]), distributors=Collection([]))
        with patch("server.db", fake_db):
            await _rebuild_inventory_for_po_medicines({"Medicine A"})
            response = await list_medicines(user={"id": "tester"})
        self.assertEqual(response[0]["total_stock"], 10)
        self.assertEqual(len(response[0]["batches"]), 1)
        self.assertEqual(response[0]["batches"][0]["available_stock"], 10)

    async def test_update_po_rebuilds_deleted_inventory_from_all_active_pos_for_medicine(self):
        po_1 = {"id": "po-a", "po_no": "PO-A", "distributor_id": "dist-a", "distributor_name": "Distributor A", "invoice_ref": "A", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 5, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "gst_rate": 0, "medicine_key": "medicine a::dist-a::ABC123::-::-::10.0::20.0"}], "purchase_return_ids": []}
        po_2 = {"id": "po-b", "po_no": "PO-B", "distributor_id": "dist-b", "distributor_name": "Distributor B", "invoice_ref": "B", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 20, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "gst_rate": 0, "medicine_key": "medicine a::dist-b::ABC123::-::-::10.0::20.0"}], "purchase_return_ids": []}
        fake_db = SimpleNamespace(medicines=Collection([]), purchase_orders=Collection([po_1, po_2]), purchase_returns=Collection([]), distributors=Collection([]))
        payload_a = POCreate(distributor_id="dist-a", distributor_name="Distributor A", invoice_ref="A", items=[{"name": "Medicine A", "batch_no": "ABC123", "quantity": 5, "purchase_price": 10, "mrp": 20, "gst_rate": 0}])
        payload_b = POCreate(distributor_id="dist-b", distributor_name="Distributor B", invoice_ref="B", items=[{"name": "Medicine A", "batch_no": "ABC123", "quantity": 20, "purchase_price": 10, "mrp": 20, "gst_rate": 0}])

        with patch("server.db", fake_db):
            await update_po("po-a", payload_a, {"role": "admin"})
            await update_po("po-b", payload_b, {"role": "admin"})
            response = await list_medicines(user={"id": "tester"})

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["total_stock"], 25)
        lots = {lot["distributor_id"]: lot["available_stock"] for lot in response[0]["batches"]}
        self.assertEqual(lots, {"dist-a": 5, "dist-b": 20})

    async def test_rebuild_does_not_keep_only_highest_quantity_lot(self):
        po_1 = {"id": "po-1", "distributor_id": "dist-1", "distributor_name": "Distributor 1", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 20, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "medicine_key": "medicine a::dist-1::ABC123::-::-::10.0::20.0"}]}
        po_2 = {"id": "po-2", "distributor_id": "dist-2", "distributor_name": "Distributor 2", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 5, "free_quantity": 0, "purchase_price": 10, "mrp": 20, "medicine_key": "medicine a::dist-2::ABC123::-::-::10.0::20.0"}]}
        fake_db = SimpleNamespace(medicines=Collection([]), purchase_orders=Collection([po_1, po_2]), distributors=Collection([]))
        with patch("server.db", fake_db):
            await _rebuild_inventory_for_po_medicines({"Medicine A"})
            response = await list_medicines(user={"id": "tester"})
        self.assertEqual(response[0]["total_stock"], 25)
        self.assertEqual(len(response[0]["batches"]), 2)
        self.assertNotEqual(response[0]["total_stock"], 20)


    async def test_rebuild_deletes_stale_rows_and_recomputes_distributor_totals(self):
        stale_rows = [
            medicine("stale-dist-a", distributor_id="dist-a", distributor="Distributor A", qty=25, available_stock=25),
            medicine("stale-dist-b", distributor_id="dist-b", distributor="Distributor B", qty=25, available_stock=25),
        ]
        po_1 = {"id": "po-a", "distributor_id": "dist-a", "distributor_name": "Distributor A", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 5, "free_quantity": 0, "purchase_price": 10, "mrp": 20}]}
        po_2 = {"id": "po-b", "distributor_id": "dist-b", "distributor_name": "Distributor B", "items": [{"name": "Medicine A", "batch_no": "ABC123", "quantity": 20, "free_quantity": 0, "purchase_price": 10, "mrp": 20}]}
        fake_db = SimpleNamespace(medicines=Collection(stale_rows), purchase_orders=Collection([po_1, po_2]), distributors=Collection([]))

        with patch("server.db", fake_db):
            await _rebuild_inventory_for_po_medicines({"Medicine A"})
            first_response = await list_medicines(user={"id": "tester"})
            await _rebuild_inventory_for_po_medicines({"Medicine A"})
            second_response = await list_medicines(user={"id": "tester"})

        self.assertEqual(first_response, second_response)
        self.assertEqual(first_response[0]["total_stock"], 25)
        lots = {lot["distributor_id"]: lot["available_stock"] for lot in first_response[0]["batches"]}
        self.assertEqual(lots, {"dist-a": 5, "dist-b": 20})

    async def test_purchase_return_finds_matching_distributor_lot_only(self):
        rows = [
            medicine("lot-1", distributor_id="dist-1", distributor="Distributor 1", qty=2),
            medicine("lot-2", distributor_id="dist-2", distributor="Distributor 2", qty=8),
        ]
        payload = PurchaseReturnCreate(return_date="2026-06-30", distributor="Distributor 2", distributor_id="dist-2", medicine_name="Medicine A", batch_number="ABC123", expiry_date="2028-01-01", return_quantity=1, purchase_rate=10, reason="Expired")
        with patch("server.db", SimpleNamespace(medicines=Collection(rows))):
            match = await _find_purchase_return_medicine(payload)
        self.assertEqual(match["id"], "lot-2")
        self.assertEqual(match["distributor_id"], "dist-2")


if __name__ == "__main__":
    unittest.main()
