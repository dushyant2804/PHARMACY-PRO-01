import unittest
from decimal import Decimal

from scripts.diagnose_rk_92082_transactions import (
    amount_matches,
    diagnostic_row,
    row_matches,
)


class RK92082DiagnosticTests(unittest.TestCase):
    def test_matches_positive_and_negative_target_amount(self):
        self.assertTrue(amount_matches(92082))
        self.assertTrue(amount_matches("-92082.00"))
        self.assertFalse(amount_matches(9208.2))

    def test_matches_all_requested_opening_balance_signals(self):
        row = {
            "amount": Decimal("1"),
            "transaction_type": "legacy_opening_balance",
            "reference": "Opening Balance",
            "invoice_ref": "RK-92082",
        }
        reasons = row_matches(row, {"rk-92082"})
        self.assertIn("text contains opening balance/opening_balance", reasons)
        self.assertIn("transaction_type contains opening/opening_balance", reasons)
        self.assertIn("invoice_ref matches requested row reference", reasons)

    def test_diagnostic_preserves_requested_fields_and_raw_json(self):
        row = {
            "_id": "mongo-id",
            "id": "app-id",
            "distributor_id": "rk",
            "amount": 92082,
            "type": "purchase",
            "source_type": "purchase_order",
            "entry_source": "distributor_ledger",
            "created_by": "admin-id",
            "reference": "Opening Balance",
            "invoice_ref": "INV-RK",
            "bill_ref": "BILL-RK",
            "purchase_order_id": "po-id",
            "opening_balance_metadata": {"legacy": True},
            "metadata": {"opening_balance": True},
            "date": "2026-04-01",
            "transaction_date": "2026-04-01",
            "created_at": "2026-04-01T00:00:00Z",
            "notes": "Opening balance duplicate",
        }
        output = diagnostic_row(row, {"name": "R K PHARMA"}, ["amount=±92082"])

        self.assertEqual(output["_id"], "mongo-id")
        self.assertEqual(output["distributor_name"], "R K PHARMA")
        self.assertEqual(output["source_type"], "purchase_order")
        self.assertEqual(output["created_by"], "admin-id")
        self.assertEqual(output["raw_json"], row)
        self.assertEqual(
            output["opening_balance_fields_or_metadata"],
            {
                "opening_balance_metadata": {"legacy": True},
                "metadata": {"opening_balance": True},
            },
        )


if __name__ == "__main__":
    unittest.main()
