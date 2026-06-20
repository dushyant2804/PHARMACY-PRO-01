import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from fastapi import HTTPException
from server import LowStockStatusUpdate, PrivacyPasswordUpdate, dashboard_summary, list_medicines, update_low_stock_status


class _Cursor:
    def __init__(self, records):
        self.records = records

    async def to_list(self, length):
        return list(self.records if length is None else self.records[:length])


class _UpdateResult:
    matched_count = 1


class _Collection:
    def __init__(self, records):
        self.records = records

    def find(self, query=None, projection=None):
        return _Cursor(self.records)

    async def find_one(self, query, projection=None):
        if "$or" in query:
            identity = query["$or"][0]["id"]
            return next((row for row in self.records if row.get("id") == identity or row.get("medicine_key") == identity), None)
        return next((row for row in self.records if all(row.get(k) == v for k, v in query.items())), None)

    async def update_one(self, query, update, upsert=False):
        target = await self.find_one(query)
        if target is None and upsert:
            target = dict(query)
            self.records.append(target)
        if target is None:
            result = _UpdateResult()
            result.matched_count = 0
            return result
        target.update(update["$set"])
        if "$setOnInsert" in update:
            target.update(update["$setOnInsert"])
        return _UpdateResult()



class _RecordingMotorCollection:
    def __init__(self):
        self.calls = []

    async def update_one(self, query, update, *args, **kwargs):
        self.calls.append((query, update, kwargs))
        return _UpdateResult()


class LowStockWorkflowTest(unittest.IsolatedAsyncioTestCase):
    async def test_update_status_persists_without_changing_stock(self):
        medicine = {"id": "med-1", "purchased_units": 10, "sold_units": 3, "low_stock_status": "low_stock"}
        fake_db = SimpleNamespace(medicines=_Collection([medicine]))

        with patch("server.db", fake_db):
            result = await update_low_stock_status("med-1", LowStockStatusUpdate(status="reordered"), user={})

        self.assertEqual(result["low_stock_status"], "reordered")
        self.assertEqual(medicine["low_stock_status"], "reordered")
        self.assertEqual(medicine["purchased_units"], 10)
        self.assertEqual(medicine["sold_units"], 3)

    async def test_inventory_returns_default_and_persisted_status(self):
        medicines = _Collection([
            {"id": "med-1", "name": "One", "batch_no": "B1", "purchased_units": 2, "low_stock_threshold": 5},
            {"id": "med-2", "name": "Two", "batch_no": "B2", "purchased_units": 2, "low_stock_threshold": 5, "low_stock_status": "abandoned"},
        ])
        fake_db = SimpleNamespace(medicines=medicines, purchase_orders=_Collection([]), distributors=_Collection([]))

        with patch("server.db", fake_db):
            result = await list_medicines(user={})

        by_id = {row["id"]: row for row in result}
        self.assertEqual(by_id["med-1"]["low_stock_status"], "low_stock")
        self.assertEqual(by_id["med-1"]["batches"][0]["low_stock_status"], "low_stock")
        self.assertEqual(by_id["med-2"]["low_stock_status"], "abandoned")

    async def test_dashboard_returns_low_stock_status(self):
        empty = _Collection([])
        fake_db = SimpleNamespace(
            medicines=_Collection([
                {"id": "med-1", "name": "One", "purchased_units": 1, "low_stock_threshold": 5, "low_stock_status": "reordered", "purchase_price": 1},
                {"medicine_key": "legacy-med-2", "name": "Two", "purchased_units": 1, "low_stock_threshold": 5, "purchase_price": 1},
            ]),
            invoices=empty, expenses=empty, customer_transactions=empty, distributors=empty,
            distributor_transactions=empty, purchase_orders=empty, regular_patients=empty,
        )
        with patch("server.db", fake_db):
            result = await dashboard_summary(user={})

        by_name = {item["name"]: item for item in result["low_stock_items"]}
        self.assertEqual(by_name["One"]["status"], "reordered")
        self.assertEqual(by_name["One"]["low_stock_status"], "reordered")
        self.assertEqual(by_name["One"]["medicine_id"], "med-1")
        self.assertEqual(by_name["One"]["id"], "med-1")
        self.assertEqual(by_name["One"]["_id"], "med-1")
        self.assertEqual(by_name["Two"]["medicine_id"], "legacy-med-2")
        self.assertEqual(by_name["Two"]["id"], "legacy-med-2")
        self.assertEqual(by_name["Two"]["_id"], "legacy-med-2")


    async def test_low_stock_threshold_locks_unlocks_and_relocks(self):
        from server import LowStockThresholdUpdate, LowStockThresholdUnlock, set_privacy_password, update_low_stock_threshold, unlock_low_stock_threshold
        medicine = {"id": "med-1", "low_stock_threshold": 10}
        medicines = _Collection([medicine])
        settings = _Collection([])
        fake_db = SimpleNamespace(medicines=medicines, settings=settings)
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", fake_db):
            first = await update_low_stock_threshold("med-1", LowStockThresholdUpdate(threshold=6), user=admin)
            self.assertEqual(first["low_stock_threshold"], 6)
            self.assertTrue(first["threshold_locked"])
            self.assertFalse(first["threshold_unlocked"])

            with self.assertRaisesRegex(Exception, "locked"):
                await update_low_stock_threshold("med-1", LowStockThresholdUpdate(threshold=7), user=admin)

            password_response = await set_privacy_password(PrivacyPasswordUpdate(privacy_password="Private1234"), user=admin)
            self.assertTrue(password_response["privacy_password_configured"])
            self.assertNotIn("privacy_password", password_response)
            self.assertNotEqual(settings.records[0]["privacy_password_hash"], "Private1234")

            unlocked = await unlock_low_stock_threshold("med-1", LowStockThresholdUnlock(privacy_password="Private1234"), user=admin)
            self.assertTrue(unlocked["threshold_unlocked"])

            updated = await update_low_stock_threshold("med-1", LowStockThresholdUpdate(threshold=8), user=admin)
            self.assertEqual(updated["low_stock_threshold"], 8)
            self.assertTrue(updated["threshold_locked"])
            self.assertFalse(updated["threshold_unlocked"])

    def test_privacy_password_save_route_supports_post_patch_and_put(self):
        from server import app, set_privacy_password

        matching_routes = [
            route for route in app.routes
            if getattr(route, "path", None) == "/api/settings/privacy-password"
            and set(getattr(route, "methods", set())) & {"POST", "PATCH", "PUT"}
        ]
        methods = set().union(*(route.methods for route in matching_routes))

        self.assertIn("POST", methods)
        self.assertIn("PATCH", methods)
        self.assertIn("PUT", methods)
        self.assertTrue(all(route.endpoint is set_privacy_password for route in matching_routes))

    async def test_privacy_password_save_hashes_password_and_returns_clear_success(self):
        from server import set_privacy_password, verify_password

        settings = _Collection([])
        fake_db = SimpleNamespace(settings=settings)
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", fake_db):
            response = await set_privacy_password(PrivacyPasswordUpdate(privacy_password="Private1234"), user=admin)

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["privacy_password_configured"], True)
        self.assertIn("updated_at", response)
        self.assertNotIn("privacy_password", response)
        self.assertNotIn("privacy_password_hash", response)
        saved_hash = settings.records[0]["privacy_password_hash"]
        self.assertNotEqual(saved_hash, "Private1234")
        self.assertTrue(verify_password("Private1234", saved_hash))


    async def test_privacy_password_save_accepts_requested_test_password(self):
        from server import set_privacy_password, verify_password

        settings = _Collection([])
        fake_db = SimpleNamespace(settings=settings)
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", fake_db):
            response = await set_privacy_password(PrivacyPasswordUpdate(privacy_password="test password"), user=admin)

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["privacy_password_configured"], True)
        self.assertNotIn("privacy_password", response)
        self.assertNotIn("privacy_password_hash", response)
        saved_hash = settings.records[0]["privacy_password_hash"]
        self.assertNotEqual(saved_hash, "test password")
        self.assertTrue(verify_password("test password", saved_hash))


    async def test_privacy_password_tenant_aware_upsert_uses_neutral_selector(self):
        from server import (
            TenantAwareCollection,
            _current_demo,
            _current_tenant,
            _request_active,
            set_privacy_password,
            verify_password,
        )

        raw_settings = _RecordingMotorCollection()
        tenant_settings = TenantAwareCollection(raw_settings, "settings")
        fake_db = SimpleNamespace(settings=tenant_settings)
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1", "shop_id": "shop-1"}
        active = _request_active.set(True)
        tenant = _current_tenant.set("shop-1")
        demo = _current_demo.set(False)
        try:
            with patch("server.db", fake_db):
                response = await set_privacy_password(PrivacyPasswordUpdate(privacy_password="Private1234"), user=admin)
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)

        self.assertEqual(response["ok"], True)
        self.assertNotIn("privacy_password", response)
        self.assertNotIn("privacy_password_hash", response)
        query, update, kwargs = raw_settings.calls[0]
        self.assertEqual(query, {"$and": [{"tenant_id": "shop-1"}, {"key": "privacy_password"}]})
        self.assertNotIn("tenant_id", query["$and"][1])
        self.assertEqual(kwargs, {"upsert": True})
        self.assertNotIn("tenant_id", update["$set"])
        self.assertNotIn("shop_id", update["$set"])
        self.assertEqual(update["$setOnInsert"].get("tenant_id"), "shop-1")
        self.assertEqual(update["$setOnInsert"].get("shop_id"), "shop-1")
        self.assertEqual(update["$setOnInsert"]["key"], "privacy_password")
        self.assertNotEqual(update["$set"]["privacy_password_hash"], "Private1234")
        self.assertTrue(verify_password("Private1234", update["$set"]["privacy_password_hash"]))

    async def test_non_admin_cannot_unlock_or_edit_unlocked_threshold(self):
        from server import LowStockThresholdUpdate, LowStockThresholdUnlock, PrivacyPasswordUpdate, set_privacy_password, update_low_stock_threshold, unlock_low_stock_threshold
        medicine = {"id": "med-1", "low_stock_threshold": 5, "threshold_locked": True, "threshold_unlocked": True}
        fake_db = SimpleNamespace(medicines=_Collection([medicine]), settings=_Collection([]))
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}
        pharmacist = {"id": "pharm-1", "role": "pharmacist", "tenant_id": "shop-1"}

        with patch("server.db", fake_db):
            await set_privacy_password(PrivacyPasswordUpdate(privacy_password="Private1234"), user=admin)
            with self.assertRaises(HTTPException) as unlock_error:
                await unlock_low_stock_threshold("med-1", LowStockThresholdUnlock(privacy_password="Private1234"), user=pharmacist)
            self.assertEqual(unlock_error.exception.status_code, 403)

            with self.assertRaises(HTTPException) as edit_error:
                await update_low_stock_threshold("med-1", LowStockThresholdUpdate(threshold=9), user=pharmacist)
            self.assertEqual(edit_error.exception.status_code, 403)

    def test_invalid_status_rejected(self):
        with self.assertRaises(ValidationError):
            LowStockStatusUpdate(status="ordered")


if __name__ == "__main__":
    unittest.main()
