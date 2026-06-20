import os
import unittest

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from fastapi import HTTPException

from server import (
    APP_RELEASE_NOTES,
    APP_VERSION,
    POCreate,
    POItem,
    SignupRequest,
    UserLogin,
    _apply_po_return_credit,
    _calculate_purchase_order_totals,
)


class VersionAndSignupContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_version_contract_is_frontend_compatible(self):
        from fastapi import Response
        from server import version

        http_response = Response()
        response = await version(http_response)
        self.assertEqual(
            set(response),
            {"current_version", "latest_version", "current_build", "latest_build",
             "full_version", "update_available", "release_date", "release_notes"},
        )
        self.assertEqual(response["latest_version"], APP_VERSION)
        self.assertEqual(response["current_version"], APP_VERSION)
        self.assertTrue(response["latest_build"])
        self.assertTrue(response["current_build"])
        self.assertEqual(response["release_notes"], APP_RELEASE_NOTES)
        self.assertEqual(set(response["release_notes"]), {"new", "improved", "fixed"})
        self.assertTrue(any(response["release_notes"].values()))
        self.assertFalse(response["update_available"])
        self.assertTrue(response["release_date"].endswith("Z"))
        self.assertRegex(response["latest_version"], r"^\d+\.\d+\.\d+$")
        self.assertEqual(response["full_version"], f"{response['latest_version']}+{response['latest_build']}")
        self.assertEqual(http_response.headers["cache-control"], "no-store")

    def test_version_config_supports_expected_update_types_and_returns_a_copy(self):
        from version_config import SUPPORTED_UPDATE_TYPES, VERSION_METADATA, get_version_metadata

        self.assertEqual(SUPPORTED_UPDATE_TYPES, ("patch", "minor", "major"))
        metadata = get_version_metadata()
        metadata["release_notes"]["fixed"].append("changed by caller")
        self.assertNotEqual(metadata["release_notes"], VERSION_METADATA["release_notes"])

    def test_version_update_availability_ignores_build_only_changes_with_same_notes(self):
        from version_config import get_version_metadata

        old_release = get_version_metadata(current_version="3.1.0", current_build="20260611-stock-repair")
        self.assertTrue(old_release["update_available"])

        same_semantic_unknown_build = get_version_metadata(
            current_version=APP_VERSION,
            current_build="20260620-different",
        )
        self.assertFalse(same_semantic_unknown_build["update_available"])

    def test_old_and_new_business_settings_are_normalized_without_data_loss(self):
        from server import normalize_settings

        old = normalize_settings({"key": "main", "business_name": "Legacy Pharmacy", "signature_b64": "sig"})
        self.assertEqual(old["business_name"], "Legacy Pharmacy")
        self.assertEqual(old["signature_b64"], "sig")
        self.assertEqual(old["dl_number_1"], "")
        self.assertIsNone(old["pharmacy_logo"])

        new = normalize_settings({
            "key": "main", "dl_number_1": "DL-ONE", "dl_number_2": "DL-TWO",
            "pharmacy_logo": {"path": "/branding/logo.png", "content_type": "image/png"},
        })
        self.assertEqual(new["dl_number_1"], "DL-ONE")
        self.assertEqual(new["dl_number_2"], "DL-TWO")
        self.assertEqual(new["pharmacy_logo"]["path"], "/branding/logo.png")

    def test_signup_accepts_email_or_mobile_and_requires_matching_method(self):
        email_signup = SignupRequest(
            email="owner@example.com", password="StrongPass123", pharmacy_name="Care Pharmacy",
            owner_name="Owner", contact="5550100", address="1 Main St", state="CA", pincode="90210",
        )
        self.assertEqual(email_signup.method, "email")
        mobile_signup = SignupRequest(
            mobile="+15550100", password="StrongPass123", pharmacy_name="Care Pharmacy",
            owner_name="Owner", contact="+15550100", address="1 Main St", state="CA", pincode="90210",
        )
        self.assertEqual(mobile_signup.method, "mobile")
        with self.assertRaises(ValueError):
            SignupRequest(
                method="mobile", email="owner@example.com", password="StrongPass123", pharmacy_name="Care Pharmacy",
                owner_name="Owner", contact="5550100", address="1 Main St", state="CA", pincode="90210",
            )

    def test_login_contract_preserves_email_and_supports_mobile(self):
        self.assertEqual(str(UserLogin(email="owner@example.com", password="x").email), "owner@example.com")
        self.assertEqual(UserLogin(mobile="+15550100", password="x").mobile, "+15550100")


class PurchaseOrderReturnAdjustmentTest(unittest.TestCase):
    def test_purchase_return_credit_is_calculated_from_selected_returns(self):
        payload = POCreate(
            distributor_id="dist-1", distributor_name="Distributor", invoice_ref="INV-1",
            items=[POItem(name="Medicine", batch_no="B1", quantity=10, purchase_price=10, mrp=12, gst_rate=0)],
            purchase_return_ids=["return-1", "return-2"],
        )
        totals = _calculate_purchase_order_totals(payload)
        adjustment = _apply_po_return_credit(totals, [
            {"id": "return-1", "medicine_name": "Medicine", "batch_number": "B0", "return_amount": 15},
            {"id": "return-2", "medicine_name": "Medicine", "batch_number": "B9", "return_amount": 10},
        ], 25)
        self.assertEqual(adjustment["purchase_return_adjustment"], 25)
        self.assertEqual(adjustment["final_payable_total"], 75)
        self.assertEqual(adjustment["purchase_return_ids"], ["return-1", "return-2"])

    def test_purchase_return_credit_cannot_make_payable_negative(self):
        adjustment = _apply_po_return_credit({"grand_total": 10}, [{"id": "r", "return_amount": 20}], 20)
        self.assertEqual(adjustment["final_payable_total"], 0)

    def test_purchase_return_credit_is_subtracted_from_po_subtotal(self):
        adjustment = _apply_po_return_credit({"sub_total": 100, "grand_total": 118}, [{"id": "r", "return_amount": 20}], 20)
        self.assertEqual(adjustment["final_payable_total"], 80)


class _ListCursor:
    def __init__(self, records):
        self.records = records

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, length):
        return self.records[:length]


class _Collection:
    def __init__(self, records):
        self.records = records

    def find(self, *args, **kwargs):
        return _ListCursor(self.records)

    async def find_one(self, query, *args, **kwargs):
        return next((record for record in self.records if all(record.get(key) == value for key, value in query.items())), None)


class AnalyticsAndReturnContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_no_sales_analytics_is_empty_and_purchase_sales_is_zero(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import _analytics_snapshot

        fake_db = SimpleNamespace(
            invoices=_Collection([]),
            purchase_orders=_Collection([{"created_at": "2026-06-01T00:00:00+00:00", "grand_total": 125}]),
            medicines=_Collection([]), customer_transactions=_Collection([]), distributor_transactions=_Collection([]),
        )
        with patch("server.db", fake_db):
            analytics = await _analytics_snapshot()

        self.assertEqual(analytics["monthly_sales_trend"], [])
        self.assertEqual(analytics["top_selling_medicines"], [])
        self.assertEqual(analytics["payment_mode_distribution"], [])
        self.assertEqual(analytics["purchase_vs_sales"], [{"month": "2026-06", "purchases": 125.0, "sales": 0.0}])

    async def test_expiry_risk_uses_dashboard_rules_and_excludes_sold_out_stock(self):
        from datetime import datetime, timedelta, timezone
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import _analytics_snapshot

        today = datetime.now(timezone.utc).date()
        medicines = [
            {"expiry_date": (today - timedelta(days=1)).isoformat(), "purchased_units": 5, "sold_units": 0, "purchase_price": 2},
            {"expiry_date": (today + timedelta(days=20)).isoformat(), "purchased_units": 6, "sold_units": 1, "purchase_price": 3},
            {"expiry_date": (today + timedelta(days=60)).isoformat(), "purchased_units": 4, "sold_units": 0, "purchase_price": 4},
            {"expiry_date": (today + timedelta(days=200)).isoformat(), "purchased_units": 3, "sold_units": 0, "purchase_price": 5},
            {"expiry_date": (today + timedelta(days=200)).isoformat(), "purchased_units": 3, "sold_units": 3, "purchase_price": 5},
        ]
        fake_db = SimpleNamespace(
            invoices=_Collection([]), purchase_orders=_Collection([]), medicines=_Collection(medicines),
            customer_transactions=_Collection([]), distributor_transactions=_Collection([]),
        )
        with patch("server.db", fake_db):
            analytics = await _analytics_snapshot()

        buckets = {item["bucket"]: item for item in analytics["expiry_risk"]}
        self.assertEqual({key: value["count"] for key, value in buckets.items()}, {"expired": 1, "within_30_days": 1, "within_90_days": 1, "safe": 1})
        self.assertEqual(buckets["safe"]["units"], 3.0)

    async def test_eligible_purchase_return_has_batch_metadata_and_calculated_credit(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import eligible_po_purchase_returns

        return_row = {"id": "return-1", "medicine_id": "med-1", "medicine_name": "Medicine", "batch_number": "B1", "expiry_date": "2027-12-31", "return_quantity": 3, "purchase_rate": 12.5, "return_amount": 0, "distributor_id": "dist-1", "distributor": "Distributor"}
        medicine = {"id": "med-1", "manufacturer": "Maker", "category": "OTC", "mrp": 20, "gst_rate": 5}
        fake_db = SimpleNamespace(purchase_returns=_Collection([return_row]), medicines=_Collection([medicine]))
        with patch("server.db", fake_db):
            result = await eligible_po_purchase_returns("dist-1", {"id": "user"})

        self.assertEqual(result[0]["calculated_return_credit_amount"], 37.5)
        self.assertEqual(result[0]["return_amount"], 37.5)
        self.assertEqual(result[0]["available_return_quantity"], 3.0)
        self.assertEqual(result[0]["manufacturer"], "Maker")
        self.assertEqual(result[0]["distributor_id"], "dist-1")


class TableActionAliasContractTest(unittest.TestCase):
    def test_invoice_normalizer_adds_invoice_id_without_removing_existing_fields(self):
        from server import _normalize_invoice

        invoice = {
            "id": "inv-1",
            "invoice_no": "INV-001",
            "customer_id": "cust-1",
            "customer_name": "Customer",
            "payment_mode": "cash",
            "total": 25,
            "items": [{"medicine_id": "med-1", "line_total": 25}],
        }

        normalized = _normalize_invoice(invoice, include_internal=True)

        self.assertEqual(normalized["id"], "inv-1")
        self.assertEqual(normalized["invoice_id"], "inv-1")
        self.assertEqual(normalized["customer_id"], "cust-1")
        self.assertEqual(normalized["items"][0]["medicine_id"], "med-1")

    def test_ledger_transaction_aliases_preserve_existing_identifiers(self):
        from server import _json_safe_ledger_transaction

        transaction = {
            "id": "txn-1",
            "distributor_id": "dist-1",
            "purchase_order_id": "po-1",
            "reference": "BILL-1",
            "amount": 12.345,
        }

        normalized = _json_safe_ledger_transaction(transaction, include_items=True)

        self.assertEqual(normalized["id"], "txn-1")
        self.assertEqual(normalized["transaction_id"], "txn-1")
        self.assertEqual(normalized["distributor_id"], "dist-1")
        self.assertEqual(normalized["purchase_order_id"], "po-1")
        self.assertEqual(normalized["reference"], "BILL-1")

    def test_purchase_return_alias_keeps_business_fields_available(self):
        from server import _normalized_purchase_return_money

        return_row = {
            "id": "return-1",
            "medicine_id": "med-1",
            "distributor_id": "dist-1",
            "batch_no": "B1",
            "return_quantity": 2,
            "purchase_rate": 10,
            "return_date": "2026-06-20",
        }

        normalized = _normalized_purchase_return_money(return_row)

        self.assertEqual(normalized["id"], "return-1")
        self.assertEqual(normalized["medicine_id"], "med-1")
        self.assertEqual(normalized["distributor_id"], "dist-1")
        self.assertEqual(normalized["batch_no"], "B1")
        self.assertEqual(normalized["return_amount"], 20.0)
        self.assertIn("status", normalized)


if __name__ == "__main__":
    unittest.main()


class _SettingsUpdateResult:
    matched_count = 1


class _SettingsCollection:
    def __init__(self, record=None):
        self.record = record
        self.updates = []

    async def find_one(self, query, projection=None):
        if self.record and self.record.get("key") == query.get("key"):
            return dict(self.record)
        return None

    async def update_one(self, query, update, upsert=False):
        self.updates.append((query, update, upsert))
        if self.record is None:
            self.record = dict(query)
        self.record.update(update["$set"])
        return _SettingsUpdateResult()


class _RecordingSettingsMotorCollection:
    def __init__(self, record=None):
        self.record = record or {"key": "main", "tenant_id": "shop-1", "shop_id": "shop-1"}
        self.calls = []

    async def update_one(self, query, update, *args, **kwargs):
        self.calls.append((query, update, kwargs))
        self.record.update(query if "key" in query else {"key": "main"})
        self.record.update(update.get("$setOnInsert", {}))
        self.record.update(update.get("$set", {}))
        return _SettingsUpdateResult()

    async def find_one(self, query, projection=None):
        return dict(self.record)


class SettingsSaveContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_settings_save_treats_missing_theme_font_logo_as_optional(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import update_settings

        settings = _SettingsCollection({"key": "main", "business_name": "Legacy"})
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", SimpleNamespace(settings=settings)):
            response = await update_settings({"business_phone": "5550100", "selected_theme": None, "selected_font": None}, user=admin)

        saved = settings.updates[0][1]["$set"]
        self.assertNotIn("selected_theme", saved)
        self.assertNotIn("selected_font", saved)
        self.assertEqual(response["selected_theme"], "default")
        self.assertEqual(response["selected_font"], "system")
        self.assertIsNone(response["pharmacy_logo"])
        self.assertEqual(response["business_phone"], "5550100")

    async def test_settings_save_persists_theme_and_font(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import update_settings

        settings = _SettingsCollection({"key": "main"})
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", SimpleNamespace(settings=settings)):
            response = await update_settings({"selected_theme": "dark", "selected_font": "inter"}, user=admin)

        self.assertEqual(settings.record["selected_theme"], "dark")
        self.assertEqual(settings.record["selected_font"], "inter")
        self.assertEqual(response["selected_theme"], "dark")
        self.assertEqual(response["selected_font"], "inter")

    async def test_settings_save_strips_tenant_fields_from_set_but_keeps_tenant_upsert_ownership(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import TenantAwareCollection, _current_demo, _current_tenant, _request_active, update_settings

        raw_settings = _RecordingSettingsMotorCollection()
        tenant_settings = TenantAwareCollection(raw_settings, "settings")
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1", "shop_id": "shop-1"}
        payload = {
            "tenant_id": "shop-1",
            "shop_id": "shop-1",
            "selected_theme": "dark",
            "selected_font": "inter",
            "theme_settings": {"accent": "blue"},
            "pharmacy_logo": {"url": "/uploads/branding/shop-1/logo.png"},
            "business_name": "Care Pharmacy",
            "business_address": "1 Main St",
            "business_phone": "5550100",
            "business_gstin": "GSTIN123",
            "dl_number_1": "DL-ONE",
            "dl_number_2": "DL-TWO",
        }

        active = _request_active.set(True)
        tenant = _current_tenant.set("shop-1")
        demo = _current_demo.set(False)
        try:
            with patch("server.db", SimpleNamespace(settings=tenant_settings)):
                response = await update_settings(payload, user=admin)
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)

        query, update, kwargs = raw_settings.calls[0]
        self.assertEqual(query, {"$and": [{"tenant_id": "shop-1"}, {"key": "main"}]})
        self.assertEqual(kwargs, {"upsert": True})
        self.assertNotIn("tenant_id", update["$set"])
        self.assertNotIn("shop_id", update["$set"])
        self.assertEqual(update["$setOnInsert"]["tenant_id"], "shop-1")
        self.assertEqual(update["$setOnInsert"]["shop_id"], "shop-1")
        self.assertEqual(response["selected_theme"], "dark")
        self.assertEqual(response["selected_font"], "inter")
        self.assertEqual(response["theme_settings"], {"accent": "blue"})
        self.assertEqual(response["pharmacy_logo"]["url"], "/uploads/branding/shop-1/logo.png")
        self.assertEqual(response["business_name"], "Care Pharmacy")
        self.assertEqual(response["business_address"], "1 Main St")
        self.assertEqual(response["business_phone"], "5550100")
        self.assertEqual(response["business_gstin"], "GSTIN123")
        self.assertEqual(response["dl_number_1"], "DL-ONE")
        self.assertEqual(response["dl_number_2"], "DL-TWO")

    async def test_settings_save_returns_validation_error_for_invalid_mongo_field(self):
        from server import update_settings

        with self.assertRaises(HTTPException) as caught:
            await update_settings({"$set": {"business_name": "Bad"}}, user={"role": "admin"})

        self.assertEqual(caught.exception.status_code, 422)

    def test_settings_bson_diagnostics_identify_first_unencodable_field(self):
        from server import _find_first_bson_encoding_failure

        payload = {"$set": {"selected_theme": "default", "new_widget_config": object()}}

        failure = _find_first_bson_encoding_failure(payload)

        self.assertIsNotNone(failure)
        self.assertEqual(failure["field"], "payload.$set.new_widget_config")
        self.assertEqual(failure["value_type"], "object")

    async def test_settings_save_logs_payload_context_before_database_failure(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from pymongo.errors import PyMongoError
        from server import update_settings

        class FailingSettingsCollection:
            async def update_one(self, query, update, upsert=False):
                raise PyMongoError("simulated settings write failure")

        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1", "shop_id": "shop-1"}
        payload = {"selected_theme": "dark", "selected_font": "inter", "theme_settings": {"accent": "blue"}}

        with patch("server.db", SimpleNamespace(settings=FailingSettingsCollection())):
            with self.assertLogs("pharmacy", level="ERROR") as logs:
                with self.assertRaises(HTTPException) as caught:
                    await update_settings(payload, user=admin)

        self.assertEqual(caught.exception.status_code, 503)
        output = "\n".join(logs.output)
        self.assertIn("Database error while saving settings: simulated settings write failure", output)
        self.assertIn("Traceback", output)
