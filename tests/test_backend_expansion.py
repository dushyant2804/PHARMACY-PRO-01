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
        from server import version

        response = await version()
        self.assertEqual(response["version"], APP_VERSION)
        self.assertIsInstance(response["message"], str)
        self.assertEqual(response["release_notes"], APP_RELEASE_NOTES)

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


if __name__ == "__main__":
    unittest.main()
