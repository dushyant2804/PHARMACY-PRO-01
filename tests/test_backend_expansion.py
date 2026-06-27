import os
import asyncio
import unittest
from typing import Optional

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
             "full_version", "update_available", "release_date", "release_timestamp", "release_notes"},
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
        self.assertEqual(response["release_timestamp"], response["release_date"])
        self.assertRegex(response["latest_version"], r"^\d+\.\d+\.\d+$")
        self.assertEqual(response["full_version"], f"{response['latest_version']}+{response['latest_build']}")
        self.assertEqual(http_response.headers["cache-control"], "no-store, no-cache, must-revalidate, max-age=0")

    def test_frontend_build_dir_env_overrides_take_precedence(self):
        from pathlib import Path
        from server import _resolve_frontend_build_dir

        root = Path(self.id()).resolve()
        self.assertEqual(
            _resolve_frontend_build_dir({"FRONTEND_BUILD_DIR": "/tmp/custom-build", "FRONTEND_DIST_DIR": "/tmp/custom-dist"}, root),
            Path("/tmp/custom-build").resolve(),
        )
        self.assertEqual(
            _resolve_frontend_build_dir({"FRONTEND_DIST_DIR": "/tmp/custom-dist"}, root),
            Path("/tmp/custom-dist").resolve(),
        )

    def test_frontend_build_dir_prefers_sibling_frontend_before_legacy(self):
        import tempfile
        from pathlib import Path
        from server import _resolve_frontend_build_dir

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "backend"
            sibling_dist = Path(tmp) / "frontend" / "dist"
            sibling_build = Path(tmp) / "frontend" / "build"
            legacy_dist = root / "frontend" / "dist"
            legacy_build = root / "frontend" / "build"
            for path in (sibling_build, legacy_dist, legacy_build):
                path.mkdir(parents=True)

            self.assertEqual(_resolve_frontend_build_dir({}, root), sibling_build.resolve())

            sibling_dist.mkdir(parents=True)
            self.assertEqual(_resolve_frontend_build_dir({}, root), sibling_dist.resolve())

    def test_frontend_build_dir_uses_legacy_dist_before_legacy_build(self):
        import tempfile
        from pathlib import Path
        from server import _resolve_frontend_build_dir

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "backend"
            legacy_dist = root / "frontend" / "dist"
            legacy_build = root / "frontend" / "build"
            legacy_dist.mkdir(parents=True)
            legacy_build.mkdir(parents=True)

            self.assertEqual(_resolve_frontend_build_dir({}, root), legacy_dist.resolve())

    def test_version_config_supports_expected_update_types_and_returns_a_copy(self):
        from version_config import SUPPORTED_UPDATE_TYPES, VERSION_METADATA, get_version_metadata

        self.assertEqual(SUPPORTED_UPDATE_TYPES, ("patch", "minor", "major"))
        metadata = get_version_metadata()
        metadata["release_notes"]["fixed"].append("changed by caller")
        self.assertNotEqual(metadata["release_notes"], VERSION_METADATA["release_notes"])

    def test_version_update_availability_detects_build_only_deployments(self):
        from version_config import get_version_metadata

        old_release = get_version_metadata(current_version="3.1.0", current_build="20260611-stock-repair")
        self.assertTrue(old_release["update_available"])

        same_semantic_unknown_build = get_version_metadata(
            current_version=APP_VERSION,
            current_build="20260620-different",
        )
        self.assertTrue(same_semantic_unknown_build["update_available"])


    async def test_update_check_logs_comparison_and_cache_control(self):
        from fastapi import Response
        from server import check_updates

        http_response = Response()
        with self.assertLogs("pharmacy", level="INFO") as logs:
            response = await check_updates(
                http_response,
                current_version="3.1.0",
                current_build="20260611-stock-repair",
            )

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["current_version"], "3.1.0")
        self.assertEqual(response["latest_version"], APP_VERSION)
        self.assertTrue(response["update_available"])
        self.assertEqual(http_response.headers["cache-control"], "no-store, no-cache, must-revalidate, max-age=0")
        joined = "\n".join(logs.output)
        self.assertIn("Update check:", joined)
        self.assertIn("current_version=3.1.0", joined)
        self.assertIn(f"latest_version={APP_VERSION}", joined)
        self.assertIn("release_timestamp=", joined)
        self.assertIn("update_available=True", joined)
        self.assertIn("cache_control=no-store, no-cache, must-revalidate, max-age=0", joined)


    def test_deployed_build_id_override_changes_returned_metadata(self):
        from version_config import get_version_metadata

        metadata = get_version_metadata(deployed_build_id="20260621-deploytest")

        self.assertEqual(metadata["latest_build"], "20260621-deploytest")
        self.assertEqual(metadata["full_version"], f"{metadata['latest_version']}+20260621-deploytest")
        self.assertTrue(metadata["release_timestamp"].endswith("Z"))

    async def test_version_json_endpoint_returns_deployed_metadata_without_cache(self):
        from fastapi import Response
        from server import version

        http_response = Response()
        response = await version(http_response)

        self.assertEqual(response["latest_version"], APP_VERSION)
        self.assertTrue(response["latest_build"])
        self.assertTrue(response["release_timestamp"].endswith("Z"))
        self.assertEqual(http_response.headers["cache-control"], "no-store, no-cache, must-revalidate, max-age=0")
        self.assertEqual(http_response.headers["pragma"], "no-cache")
        self.assertEqual(http_response.headers["expires"], "0")

    async def test_backup_health_uses_database_health_for_cloud_mode(self):
        import server

        class HealthyRawDb:
            async def command(self, command):
                self.command_seen = command
                return {"ok": 1}

            def __getattr__(self, name):
                raise AssertionError(f"unexpected collection access: {name}")

        original_raw_db = server.raw_db
        original_local_mode = server.LOCAL_MODE
        original_runtime_mode = server.RUNTIME_MODE
        try:
            server.raw_db = HealthyRawDb()
            server.LOCAL_MODE = False
            server.RUNTIME_MODE = "CLOUD_MODE"
            self.assertTrue(await server._database_connected())
        finally:
            server.raw_db = original_raw_db
            server.LOCAL_MODE = original_local_mode
            server.RUNTIME_MODE = original_runtime_mode

    def test_local_health_routes_match_frontend_expectation_without_auth(self):
        import server

        original_database_connected = server._database_connected

        async def healthy_database():
            return True

        try:
            server._database_connected = healthy_database
            payload = asyncio.run(server.api_health())
            self.assertEqual(payload["status"], "ok")
            self.assertTrue(payload["system_stable"])
            self.assertTrue(payload["local_backend_running"])

            route_paths = {route.path for route in server.app.routes}
            self.assertIn("/health", route_paths)
            self.assertIn("/api/health", route_paths)
            self.assertIn("/api/backup/health", route_paths)
            self.assertIn("/api/backup/status", route_paths)
            self.assertNotIn("/api/backup/google-drive/device-login", route_paths)
            self.assertNotIn("/api/backup/google-drive/device-token", route_paths)
        finally:
            server._database_connected = original_database_connected

    def test_health_system_stable_reflects_startup_maintenance_state(self):
        import server

        original_state = dict(server._STARTUP_STABILITY)
        try:
            server._STARTUP_STABILITY.update({
                "maintenance_running": True,
                "tenant_initialization_complete": True,
                "purchase_return_recalculation_complete": True,
                "indexing_complete": True,
            })
            self.assertFalse(server._system_stable())

            server._STARTUP_STABILITY.update({
                "maintenance_running": False,
                "tenant_initialization_complete": True,
                "purchase_return_recalculation_complete": True,
                "indexing_complete": True,
            })
            self.assertTrue(server._system_stable())

            server._STARTUP_STABILITY["purchase_return_recalculation_complete"] = False
            self.assertFalse(server._system_stable())
        finally:
            server._STARTUP_STABILITY.clear()
            server._STARTUP_STABILITY.update(original_state)


    def test_local_import_cors_preflight_allows_deployed_frontend_and_localhost_origins(self):
        import server

        async def asgi_options(path, origin):
            messages = []
            scope = {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "OPTIONS",
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "headers": [
                    (b"host", b"localhost:8000"),
                    (b"origin", origin.encode()),
                    (b"access-control-request-method", b"POST"),
                    (b"access-control-request-headers", b"authorization,content-type"),
                ],
                "client": ("127.0.0.1", 12345),
                "server": ("localhost", 8000),
            }

            received = False

            async def receive():
                nonlocal received
                if received:
                    return {"type": "http.disconnect"}
                received = True
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(message):
                messages.append(message)

            await server.app(scope, receive, send)
            start = next(message for message in messages if message["type"] == "http.response.start")
            headers = {key.decode().lower(): value.decode() for key, value in start["headers"]}
            return start["status"], headers

        origins = [
            "https://pharmacy-pro-01-frontend.onrender.com",
            "http://localhost",
            "http://localhost:3000",
            "http://127.0.0.1",
        ]
        for path in ("/api/local/import/dry-run", "/api/local/import/confirm"):
            for origin in origins:
                status, headers = asyncio.run(asgi_options(path, origin))
                self.assertEqual(status, 200)
                self.assertEqual(headers.get("access-control-allow-origin"), origin)
                self.assertIn("POST", headers.get("access-control-allow-methods", ""))
                self.assertIn("authorization", headers.get("access-control-allow-headers", "").lower())


    async def test_local_import_confirm_accepts_json_body_and_logs_final_overwrite_value(self):
        import server

        class FakeRequest:
            headers = {"origin": "https://pharmacy-pro-01-frontend.onrender.com", "Authorization": "Bearer token"}
            cookies = {}

            async def body(self):
                return b'{"overwrite_local": true}'

        seen = {}
        original_import = server._cloud_to_local_import

        async def fake_import(dry_run, confirm, overwrite_local):
            seen["dry_run"] = dry_run
            seen["confirm"] = confirm
            seen["overwrite_local"] = overwrite_local
            return {"ok": True, "overwrite_local": overwrite_local}

        try:
            server._cloud_to_local_import = fake_import
            with self.assertLogs("pharmacy", level="INFO") as logs:
                response = await server.local_mode_import_confirm(
                    request=FakeRequest(),
                    payload=server.LocalImportConfirmRequest(overwrite_local=True),
                )
        finally:
            server._cloud_to_local_import = original_import

        self.assertEqual(response["overwrite_local"], True)
        self.assertEqual(seen, {"dry_run": False, "confirm": True, "overwrite_local": True})
        joined_logs = "\n".join(logs.output)
        self.assertIn('raw_body={"overwrite_local": true}', joined_logs)
        self.assertIn("parsed_body={'overwrite_local': True}", joined_logs)
        self.assertIn("final_overwrite_local=True", joined_logs)
        self.assertIn("import logic overwrite_local=True", joined_logs)

    def test_local_import_confirm_alias_routes_share_request_model(self):
        import server

        routes = {
            route.path: route
            for route in server.api_router.routes
            if getattr(route, "endpoint", None) is server.local_mode_import_confirm
        }
        self.assertIn("/api/local/import/confirm", routes)
        self.assertIn("/api/local-mode/import/confirm", routes)
        self.assertEqual(routes["/api/local/import/confirm"].body_field.type_, Optional[server.LocalImportConfirmRequest])
        self.assertEqual(routes["/api/local-mode/import/confirm"].body_field.type_, Optional[server.LocalImportConfirmRequest])

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

    def test_settings_response_strips_legacy_welcome_screen_fields(self):
        from server import settings_response

        response = settings_response({
            "key": "main",
            "business_name": "Legacy",
            "welcome_screen": {"enabled": True},
            "welcome_screen_enabled": True,
            "welcome_title": "Hello",
            "welcome_subtitle": "Start",
            "welcome_background": "blue",
            "welcome_logo": "/welcome.png",
            "welcome_message": "Welcome",
            "welcome_settings": {"show": True},
        })

        for field in (
            "welcome_screen",
            "welcome_screen_enabled",
            "welcome_title",
            "welcome_subtitle",
            "welcome_background",
            "welcome_logo",
            "welcome_message",
            "welcome_settings",
        ):
            self.assertNotIn(field, response)
        self.assertEqual(response["business_name"], "Legacy")

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


    async def test_settings_save_does_not_generate_or_persist_welcome_screen_fields(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import update_settings

        settings = _SettingsCollection({"key": "main", "welcome_title": "Stored legacy title"})
        admin = {"id": "admin-1", "email": "admin@example.com", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", SimpleNamespace(settings=settings)):
            response = await update_settings({
                "business_name": "Care Pharmacy",
                "welcome_screen": {"enabled": True},
                "welcome_settings": {"headline": "Hello"},
                "welcome_title": "Hello",
            }, user=admin)

        saved = settings.updates[0][1]["$set"]
        self.assertNotIn("welcome_screen", saved)
        self.assertNotIn("welcome_settings", saved)
        self.assertNotIn("welcome_title", saved)
        self.assertNotIn("welcome_title", response)
        self.assertEqual(response["business_name"], "Care Pharmacy")

    async def test_get_settings_logs_that_welcome_screen_config_is_not_returned(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from server import get_settings

        settings = _SettingsCollection({"key": "main", "welcome_screen": {"enabled": True}})
        user = {"id": "admin-1", "tenant_id": "shop-1"}

        with patch("server.db", SimpleNamespace(settings=settings)), self.assertLogs("pharmacy", level="INFO") as logs:
            response = await get_settings(user=user)

        self.assertNotIn("welcome_screen", response)
        self.assertIn("GET /api/settings Welcome Screen configuration return audit", "\n".join(logs.output))
        self.assertIn("welcome_screen_config_returned", "\n".join(logs.output))

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
