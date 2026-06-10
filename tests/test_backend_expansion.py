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
        self.assertEqual(response["version"], APP_VERSION)
        self.assertTrue(response["build"])
        self.assertIsInstance(response["message"], str)
        self.assertEqual(response["release_notes"], APP_RELEASE_NOTES)
        self.assertEqual(http_response.headers["cache-control"], "no-store, max-age=0")

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


if __name__ == "__main__":
    unittest.main()
