import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

import server


class _EmptyCursor:
    async def to_list(self, limit=1000):
        return []


class InvoiceEditProtectionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_invoice = {
            "id": "invoice-old",
            "invoice_no": "INV-OLD",
            "created_at": "2026-10-01T10:00:00+00:00",
            "customer_id": "customer-1",
            "items": [{"name": "Med", "quantity": 1}],
            "stock_deductions": [{"medicine_id": "med-batch-1", "deduct": 1}],
        }
        self.payload = server.InvoiceEdit(
            items=[
                server.InvoiceItem(
                    medicine_id="med-batch-1",
                    name="Med",
                    batch_no="B1",
                    expiry_date="12/30",
                    quantity=1,
                    mrp=10,
                )
            ],
            privacy_password="wrong-password",
        )

    async def test_older_invoice_rejects_non_admin_before_any_mutation(self):
        fake_db = SimpleNamespace(
            invoices=SimpleNamespace(find_one=AsyncMock(return_value=self.old_invoice))
        )
        user = {"id": "staff-1", "role": "pharmacist", "tenant_id": "shop-1"}

        with patch("server.db", fake_db):
            with self.assertRaises(HTTPException) as raised:
                await server.update_invoice("invoice-old", self.payload, user=user)

        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("Only an admin", raised.exception.detail)

    async def test_older_invoice_rejects_invalid_privacy_password(self):
        settings = SimpleNamespace(
            find_one=AsyncMock(return_value={
                "privacy_password_hash": server.hash_password("Correct123!")
            })
        )
        fake_db = SimpleNamespace(
            invoices=SimpleNamespace(find_one=AsyncMock(return_value=self.old_invoice)),
            settings=settings,
        )
        user = {"id": "admin-1", "role": "admin", "tenant_id": "shop-1"}

        with patch("server.db", fake_db):
            with self.assertRaises(HTTPException) as raised:
                await server.update_invoice("invoice-old", self.payload, user=user)

        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("valid privacy password", raised.exception.detail)


if __name__ == "__main__":
    unittest.main()
