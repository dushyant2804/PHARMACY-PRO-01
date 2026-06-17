import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")

from server import distributor_ledger


class Cursor:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, length):
        return self.rows[:length]


class Collection:
    def __init__(self, rows):
        self.rows = rows

    async def find_one(self, query, *args, **kwargs):
        return next(
            (dict(row) for row in self.rows if all(str(row.get(k)) == str(v) for k, v in query.items())),
            None,
        )

    def find(self, query=None, *args, **kwargs):
        return Cursor(self.rows)


class DistributorLedgerInclusionTests(unittest.IsolatedAsyncioTestCase):
    async def ledger(self, distributors, transactions, purchase_orders, **filters):
        fake_db = SimpleNamespace(
            distributors=Collection(distributors),
            distributor_transactions=Collection(transactions),
            purchase_orders=Collection(purchase_orders),
        )
        with patch("server.db", fake_db):
            return await distributor_ledger(filters.pop("did", "d1"), user={}, **filters)

    async def test_multiple_purchases_and_payments_are_included_and_balanced_in_date_order(self):
        result = await self.ledger(
            [{"id": "d1", "name": "Supplier", "opening_balance": 0}],
            [{"id": "pay-1", "distributor_id": "d1", "type": "payment", "amount": 50,
              "created_at": "2026-03-03T00:00:00+00:00"}],
            [
                {"id": "po-1", "distributor_id": "d1", "po_no": "PO-1", "grand_total": 100,
                 "po_date": "2026-03-01"},
                {"id": "po-2", "distributor_id": "d1", "po_no": "PO-2", "grand_total": 70,
                 "po_date": "2026-03-02"},
            ],
        )
        rows = [row for row in result["transactions"] if not row.get("is_opening_balance")]
        self.assertEqual([row["type"] for row in rows], ["purchase", "purchase", "payment"])
        self.assertEqual([row["running_balance"] for row in rows], [100, 170, 120])
        self.assertEqual(result["balance"], 120)

    async def test_legacy_ids_and_name_only_fallback_match_without_duplicate_purchase(self):
        result = await self.ledger(
            [{"_id": "object-id", "id": "current-id", "distributor_id": "legacy-id", "name": "Supplier", "opening_balance": 0}],
            [
                {"id": "existing-po", "distributor_id": "legacy-id", "type": "purchase", "amount": 25,
                 "purchase_order_id": "po-1", "created_at": "2026-03-01"},
                {"id": "pay", "distributor_name": "Supplier", "type": "payment", "amount": 5,
                 "created_at": "2026-03-02"},
            ],
            [{"id": "po-1", "distributor_id": "current-id", "grand_total": 25, "po_date": "2026-03-01"}],
            did="object-id",
        )
        rows = [row for row in result["transactions"] if not row.get("is_opening_balance")]
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(row["type"] == "purchase" for row in rows), 1)
        self.assertEqual(sum(row["type"] == "payment" for row in rows), 1)

    async def test_filters_apply_only_when_explicitly_provided(self):
        args = (
            [{"id": "d1", "name": "Supplier", "opening_balance": 0}],
            [{"id": "pay", "distributor_id": "d1", "type": "payment", "amount": 5,
              "created_at": "2026-03-02"}],
            [{"id": "po-1", "distributor_id": "d1", "grand_total": 25, "po_date": "2026-03-01"}],
        )
        unfiltered = await self.ledger(*args)
        filtered = await self.ledger(*args, transaction_type="payment")
        self.assertEqual({row["type"] for row in unfiltered["transactions"]}, {"purchase", "payment"})
        self.assertEqual([row["type"] for row in filtered["transactions"]], ["payment"])
        self.assertEqual(filtered["transactions"][0]["running_balance"], 20)

    async def test_opening_balance_duplicate_purchase_or_payment_rows_are_not_returned(self):
        result = await self.ledger(
            [{
                "id": "d1",
                "name": "A TO Z MEDICAL AGENCY",
                "opening_balance": 293,
                "opening_balance_date": "2026-04-01",
            }],
            [
                {
                    "id": "legacy-opening-purchase",
                    "distributor_id": "d1",
                    "type": "purchase",
                    "amount": 293,
                    "reference_number": "Opening Balance",
                    "notes": "Opening Balance",
                    "created_at": "2026-04-01",
                },
                {
                    "id": "legacy-opening-payment",
                    "distributor_id": "d1",
                    "type": "payment",
                    "amount": 293,
                    "reference_number": "Opening Balance",
                    "notes": "Opening Balance",
                    "created_at": "2026-04-01",
                },
            ],
            [],
        )

        self.assertEqual(len(result["transactions"]), 1)
        opening_row = result["transactions"][0]
        self.assertTrue(opening_row["is_opening_balance"])
        self.assertEqual(opening_row["type"], "opening_balance")
        self.assertEqual(opening_row["display_type"], "Opening Balance")
        self.assertEqual(opening_row["amount"], 293)
        self.assertEqual(opening_row["running_balance"], 293)
        self.assertEqual(result["balance"], 293)
        self.assertEqual(result["total_purchases"], 293)
        self.assertEqual(result["balance_for_selected_period"], 293)
        self.assertFalse(
            any(row["type"] in {"purchase", "payment"} for row in result["transactions"])
        )

    async def test_arti_style_opening_balance_duplicate_is_excluded_and_selected_balance_is_signed(self):
        result = await self.ledger(
            [{
                "id": "arti",
                "name": "ARTI ENTERPRISES",
                "opening_balance": 1169,
                "opening_balance_date": "2026-04-01",
                "opening_balance_invoice_number": "OB-ARTI-1169",
            }],
            [
                {
                    "id": "duplicate-opening-purchase",
                    "distributor_id": "arti",
                    "type": "purchase",
                    "amount": 1169,
                    "invoice_number": "OB-ARTI-1169",
                    "reference_number": "Opening Balance",
                    "notes": "Opening Balance",
                    "created_at": "2026-04-01",
                },
                {
                    "id": "opening-payment",
                    "distributor_id": "arti",
                    "type": "payment",
                    "amount": 1169,
                    "reference_number": "PMT-OB-ARTI-1169",
                    "created_at": "2026-04-02",
                },
                {
                    "id": "p1",
                    "distributor_id": "arti",
                    "type": "purchase",
                    "amount": 2844,
                    "invoice_number": "INV-2844",
                    "created_at": "2026-04-03",
                },
                {
                    "id": "pay-1",
                    "distributor_id": "arti",
                    "type": "payment",
                    "amount": 4013,
                    "reference_number": "PMT-4013",
                    "created_at": "2026-04-04",
                },
            ],
            [],
            did="arti",
        )

        self.assertEqual(
            [row["id"] for row in result["transactions"]],
            ["opening-balance-arti", "opening-payment", "p1", "pay-1"],
        )
        self.assertEqual(result["total_purchases"], 4013)
        self.assertEqual(result["total_paid"], 5182)
        self.assertEqual(result["balance"], -1169)
        self.assertEqual(result["balance_for_selected_period"], -1169)
        self.assertEqual(result["net_balance_for_selected_period"], -1169)
        self.assertEqual(result["payable_for_selected_period"], 0)
        self.assertEqual(result["receivable_for_selected_period"], 1169)

    async def test_global_opening_balance_purchase_dedupe_for_multiple_distributors(self):
        distributors = [
            {"id": "d1", "name": "Supplier 1", "opening_balance": 100, "opening_balance_date": "2026-04-01"},
            {"id": "d2", "name": "Supplier 2", "opening_balance": 200, "opening_balance_date": "2026-04-02"},
        ]
        transactions = [
            {"id": "ob-1", "distributor_id": "d1", "type": "opening_balance", "amount": 100,
             "invoice_number": "  OB   001 ", "bill_amount": 100, "paid_amount": 0, "due_amount": 100,
             "created_at": "2026-04-01T09:15:00+00:00"},
            {"id": "dup-1", "distributor_id": "d1", "type": "purchase", "amount": 100,
             "invoice_no": "ob 001", "bill_amount": 100, "paid_amount": 0, "due_amount": 100,
             "created_at": "2026-04-01T18:30:00+00:00"},
            {"id": "real-1", "distributor_id": "d1", "type": "purchase", "amount": 100,
             "invoice_number": "INV-REAL", "created_at": "2026-04-01T18:30:00+00:00"},
            {"id": "pay-1", "distributor_id": "d1", "type": "payment", "amount": 100,
             "reference_number": "ob 001", "created_at": "2026-04-01T18:30:00+00:00"},
            {"id": "ob-2", "distributor_id": "d2", "type": "opening_balance", "amount": 200,
             "bill_number": "B-200", "created_at": "2026-04-02"},
            {"id": "dup-2", "distributor_id": "d2", "type": "purchase", "amount": 200,
             "bill_no": " b-200 ", "created_at": "2026-04-02T23:59:00+00:00"},
        ]

        d1 = await self.ledger(distributors, transactions, [], did="d1")
        d2 = await self.ledger(distributors, transactions, [], did="d2")

        self.assertEqual([row["id"] for row in d1["transactions"]], ["ob-1", "pay-1", "real-1"])
        self.assertEqual(sum(row["type"] == "opening_balance" for row in d1["transactions"]), 1)
        self.assertFalse(any(row.get("id") == "dup-1" for row in d1["transactions"]))
        self.assertEqual(d1["transactions"][-1]["running_balance"], 100)
        self.assertEqual(d1["total_purchases"], 200)
        self.assertEqual(d1["total_paid"], 100)
        self.assertEqual(d1["balance"], 100)
        self.assertEqual(d1["balance_for_selected_period"], 100)

        self.assertEqual([row["id"] for row in d2["transactions"]], ["ob-2"])
        self.assertFalse(any(row.get("id") == "dup-2" for row in d2["transactions"]))
        self.assertEqual(d2["total_purchases"], 200)
        self.assertEqual(d2["balance"], 200)

    async def test_opening_balance_duplicate_fallback_matches_without_invoice_reference(self):
        result = await self.ledger(
            [{"id": "d1", "name": "Supplier", "opening_balance": 150}],
            [
                {"id": "ob", "distributor_id": "d1", "type": "opening_balance", "amount": 150,
                 "bill_amount": 150, "paid_amount": 20, "due_amount": 130,
                 "created_at": "2026-04-01T00:00:00+00:00"},
                {"id": "dup", "distributor_id": "d1", "type": "purchase", "amount": 150,
                 "bill_amount": 150, "paid_amount": 20, "due_amount": 130,
                 "created_at": "2026-04-01T12:00:00+00:00"},
            ],
            [],
        )

        self.assertEqual([row["id"] for row in result["transactions"]], ["ob"])
        self.assertEqual(result["total_purchases"], 150)
        self.assertEqual(result["balance"], 150)

from server import _distributor_monthly_summary_data


class DistributorLedgerPurchaseInvoiceDedupeTests(unittest.IsolatedAsyncioTestCase):
    async def ledger(self, distributors, transactions, purchase_orders, **filters):
        fake_db = SimpleNamespace(
            distributors=Collection(distributors),
            distributor_transactions=Collection(transactions),
            purchase_orders=Collection(purchase_orders),
        )
        with patch("server.db", fake_db):
            return await distributor_ledger(filters.pop("did", "d1"), user={}, **filters)

    async def test_duplicate_purchase_invoice_from_transaction_and_po_is_returned_once(self):
        distributors = [{"id": "d1", "name": "Supplier", "opening_balance": 0}]
        transactions = [
            {"id": "txn-purchase", "distributor_id": "d1", "type": "purchase", "amount": 123.456,
             "invoice_no": "INV-100", "reference_number": "INV-100", "created_at": "2026-05-10T09:00:00+00:00"},
            {"id": "payment-1", "distributor_id": "d1", "type": "payment", "amount": 23.46,
             "reference_number": "PAY-INV-100", "receipt_invoice_no": "INV-100", "created_at": "2026-05-11T09:00:00+00:00"},
        ]
        purchase_orders = [
            {"id": "po-100", "distributor_id": "d1", "po_no": "PO-100", "invoice_ref": "INV-100",
             "grand_total": 123.456, "po_date": "2026-05-10", "items": [{"name": "Med"}]},
        ]

        result = await self.ledger(distributors, transactions, purchase_orders)

        purchase_rows = [row for row in result["transactions"] if row.get("type") == "purchase"]
        self.assertEqual(len(purchase_rows), 1)
        self.assertEqual(purchase_rows[0]["purchase_order_id"], "po-100")
        self.assertEqual(purchase_rows[0]["running_balance"], 123.46)
        self.assertEqual(result["total_purchases"], 123.46)
        self.assertEqual(result["total_paid"], 23.46)
        self.assertEqual(result["balance"], 100.0)
        self.assertEqual([row["id"] for row in result["transactions"]], ["purchase-order-po-100", "payment-1"])

    async def test_same_date_and_amount_with_different_invoice_refs_remain_separate(self):
        result = await self.ledger(
            [{"id": "d1", "name": "Supplier", "opening_balance": 0}],
            [
                {"id": "p1", "distributor_id": "d1", "type": "purchase", "amount": 50,
                 "invoice_no": "INV-A", "created_at": "2026-05-10"},
                {"id": "p2", "distributor_id": "d1", "type": "purchase", "amount": 50,
                 "invoice_no": "INV-B", "created_at": "2026-05-10"},
            ],
            [],
        )

        self.assertEqual([row["id"] for row in result["transactions"]], ["p1", "p2"])
        self.assertEqual([row["running_balance"] for row in result["transactions"]], [50, 100])
        self.assertEqual(result["total_purchases"], 100)

    async def test_monthly_summary_keeps_previous_non_ledger_deduped_purchase_set(self):
        fake_db = SimpleNamespace(
            distributors=Collection([{"id": "d1", "name": "Supplier", "opening_balance": 0}]),
            distributor_transactions=Collection([
                {"id": "txn-purchase", "distributor_id": "d1", "type": "purchase", "amount": 80,
                 "invoice_no": "INV-MONTH", "created_at": "2026-05-10"},
                {"id": "payment-1", "distributor_id": "d1", "type": "payment", "amount": 30,
                 "reference_number": "PAY-INV-MONTH", "receipt_invoice_no": "INV-MONTH", "created_at": "2026-05-11"},
            ]),
            purchase_orders=Collection([
                {"id": "po-month", "distributor_id": "d1", "po_no": "PO-MONTH", "invoice_ref": "INV-MONTH",
                 "grand_total": 80, "po_date": "2026-05-10", "items": [{"name": "Med"}]},
            ]),
        )
        with patch("server.db", fake_db):
            result = await _distributor_monthly_summary_data("2026-05", "d1")

        self.assertEqual(result["purchase_total"], 160)
        self.assertEqual(result["payment_total"], 30)
        self.assertEqual(result["net_change"], 130)
        self.assertEqual(result["items"][0]["transaction_count"], 3)
