import os
import unittest

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import POCreate, POItem, _calculate_purchase_order_totals, _purchase_return_credit, _round_ledger_money


class PurchaseOrderTotalsTest(unittest.TestCase):
    def test_money_uses_decimal_half_up_rounding(self):
        payload = POCreate(
            distributor_id="dist-1",
            distributor_name="Distributor",
            invoice_ref="INV-precision",
            scheme_discount=0.005,
            cash_discount=0.004,
            items=[POItem(name="item", batch_no="b1", quantity=1, purchase_price=10.005, mrp=0, gst_rate=5)],
        )

        totals = _calculate_purchase_order_totals(payload)

        self.assertEqual(totals["sub_total"], 10.01)
        self.assertEqual(totals["scheme_discount"], 0.01)
        self.assertEqual(totals["cash_discount"], 0.0)
        self.assertEqual(_purchase_return_credit({"return_quantity": 1, "purchase_rate": 2.675}), 2.68)
        self.assertEqual(_round_ledger_money(2.675), 2.68)

    def test_discount_is_distributed_by_gst_slab_before_tax(self):
        payload = POCreate(
            distributor_id="dist-1",
            distributor_name="Distributor",
            invoice_ref="INV-1",
            scheme_discount=99.97,
            items=[
                POItem(name="item1", batch_no="b1", quantity=60, free_quantity=2, purchase_price=21.43, mrp=0, gst_rate=5),
                POItem(name="item2", batch_no="b2", quantity=2.5, free_quantity=0.5, purchase_price=94.86, mrp=0, gst_rate=5),
                POItem(name="item3", batch_no="b3", quantity=2.5, free_quantity=0.5, purchase_price=76.08, mrp=0, gst_rate=5),
                POItem(name="item4", batch_no="b4", quantity=1.5, free_quantity=1.5, purchase_price=103.70, mrp=0, gst_rate=18),
                POItem(name="item5", batch_no="b5", quantity=10, free_quantity=0, purchase_price=42.82, mrp=0, gst_rate=5),
                POItem(name="item6", batch_no="b6", quantity=2, free_quantity=1, purchase_price=309.76, mrp=0, gst_rate=5),
                POItem(name="item7", batch_no="b7", quantity=3, free_quantity=0, purchase_price=79.20, mrp=0, gst_rate=5),
                POItem(name="item8", batch_no="b8", quantity=1, free_quantity=0, purchase_price=177.97, mrp=0, gst_rate=18),
            ],
        )

        totals = _calculate_purchase_order_totals(payload)

        self.assertEqual(totals["sub_total"], 3331.99)
        self.assertEqual(totals["discount"], 99.97)
        self.assertEqual(totals["taxable_total"], 3232.02)
        self.assertEqual(totals["total_cgst"], 101.83)
        self.assertEqual(totals["total_sgst"], 101.83)
        self.assertEqual(totals["total"], 3435.68)
        self.assertEqual(totals["round_off"], 0.32)
        self.assertEqual(totals["grand_total"], 3436.0)
        self.assertEqual(
            totals["gst_breakup"],
            [
                {
                    "gst_rate": 5.0,
                    "sub_total": 2998.47,
                    "discount": 89.96,
                    "taxable_total": 2908.51,
                    "cgst": 72.71,
                    "sgst": 72.71,
                    "gst": 145.42,
                },
                {
                    "gst_rate": 18.0,
                    "sub_total": 333.52,
                    "discount": 10.01,
                    "taxable_total": 323.51,
                    "cgst": 29.12,
                    "sgst": 29.12,
                    "gst": 58.24,
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
