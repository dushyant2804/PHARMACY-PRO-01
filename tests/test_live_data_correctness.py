import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import (
    _available_stock, _return_status, _safe_legacy_sold_stock,
    list_customers, list_distributors, purchase_return_report,
)


class Cursor:
    def __init__(self, rows): self.rows = [dict(row) for row in rows]
    def sort(self, *args, **kwargs): return self
    async def to_list(self, length): return self.rows if length is None else self.rows[:length]


class Collection:
    def __init__(self, rows): self.rows = rows
    async def find_one(self, query, *args, **kwargs):
        return next(
            (dict(row) for row in self.rows if all(str(row.get(k)) == str(v) for k, v in query.items())),
            None,
        )
    def find(self, query=None, projection=None):
        rows = self.rows
        if query:
            for key, value in query.items():
                if isinstance(value, dict) and "$in" in value:
                    rows = [row for row in rows if row.get(key) in value["$in"]]
        return Cursor(rows)


class LiveDataCorrectnessTests(unittest.IsolatedAsyncioTestCase):
    def test_normalized_stock_prevents_false_sold_out_and_rejects_stale_sold_units(self):
        stale = {"purchased_units": 10, "sold_units": 10, "available_stock": 10}
        self.assertEqual(_safe_legacy_sold_stock(stale), 0)
        rebuilt = {**stale, "sold_units": _safe_legacy_sold_stock(stale)}
        self.assertEqual(_available_stock(rebuilt), 10)
        self.assertEqual(_return_status(rebuilt), "Not Returned")

    async def test_distributor_summary_reconciles_payments_and_adjustments_to_payable(self):
        db = SimpleNamespace(
            distributors=Collection([{"id": "d1", "name": "D", "opening_balance": 40}]),
            distributor_transactions=Collection([
                {"distributor_id": "d1", "type": "purchase", "amount": 100},
                {"distributor_id": "d1", "type": "payment", "amount": 30},
                {"distributor_id": "d1", "type": "purchase_return", "amount": 10},
                {"distributor_id": "d1", "type": "credit_adjustment", "amount": 5},
            ]),
        )
        with patch("server.db", db): result = await list_distributors(user={})
        self.assertEqual(result[0]["total_purchases"], 140)
        self.assertEqual(result[0]["actual_payments"], 30)
        self.assertEqual(result[0]["total_paid"], 45)
        self.assertEqual(result[0]["total_paid_adjusted"], 45)
        self.assertEqual(result[0]["outstanding_balance"], 95)
        self.assertEqual(
            result[0]["total_purchases"] - result[0]["total_paid_adjusted"],
            result[0]["outstanding_balance"],
        )


    async def test_distributor_summary_dedupes_opening_balance_purchase_duplicate(self):
        db = SimpleNamespace(
            distributors=Collection([{
                "id": "d1",
                "name": "D",
                "opening_balance": 1000,
                "opening_balance_date": "2026-04-01",
                "opening_balance_invoice_number": "OB-1000",
            }]),
            distributor_transactions=Collection([
                {
                    "id": "ob",
                    "distributor_id": "d1",
                    "type": "opening_balance",
                    "amount": 1000,
                    "invoice_number": "OB-1000",
                    "transaction_date": "2026-04-01",
                    "reference_number": "Opening Balance",
                },
                {
                    "id": "dup",
                    "distributor_id": "d1",
                    "type": "purchase",
                    "amount": 1000,
                    "invoice_number": "OB-1000",
                    "transaction_date": "2026-04-01",
                    "reference_number": "Opening Balance",
                },
                {"id": "pay", "distributor_id": "d1", "type": "payment", "amount": 500},
            ]),
        )

        with patch("server.db", db):
            result = await list_distributors(user={})

        self.assertEqual(result[0]["total_purchases"], 1000)
        self.assertEqual(result[0]["actual_payments"], 500)
        self.assertEqual(result[0]["total_paid"], 500)
        self.assertEqual(result[0]["total_paid_adjusted"], 500)
        self.assertEqual(result[0]["outstanding_balance"], 500)
        self.assertEqual(
            result[0]["total_purchases"] - result[0]["total_paid_adjusted"],
            result[0]["outstanding_balance"],
        )

    async def test_distributor_summary_keeps_real_purchase_with_different_invoice(self):
        db = SimpleNamespace(
            distributors=Collection([{
                "id": "d1",
                "name": "D",
                "opening_balance": 1000,
                "opening_balance_date": "2026-04-01",
                "opening_balance_invoice_number": "OB-1000",
            }]),
            distributor_transactions=Collection([
                {
                    "id": "ob",
                    "distributor_id": "d1",
                    "type": "opening_balance",
                    "amount": 1000,
                    "invoice_number": "OB-1000",
                    "transaction_date": "2026-04-01",
                },
                {
                    "id": "real",
                    "distributor_id": "d1",
                    "type": "purchase",
                    "amount": 1000,
                    "invoice_number": "REAL-1000",
                    "transaction_date": "2026-04-01",
                },
                {"id": "pay", "distributor_id": "d1", "type": "payment", "amount": 500},
            ]),
        )

        with patch("server.db", db):
            result = await list_distributors(user={})

        self.assertEqual(result[0]["total_purchases"], 2000)
        self.assertEqual(result[0]["actual_payments"], 500)
        self.assertEqual(result[0]["outstanding_balance"], 1500)

    async def test_distributor_receivable_is_separate_from_payable_and_paid_adjusted(self):
        db = SimpleNamespace(
            distributors=Collection([
                {"id": "payable", "name": "Payable"},
                {"id": "receivable", "name": "Receivable"},
            ]),
            distributor_transactions=Collection([
                {"distributor_id": "payable", "type": "purchase", "amount": 100},
                {"distributor_id": "payable", "type": "payment", "amount": 40},
                {"distributor_id": "receivable", "type": "credit_adjustment", "amount": 55},
            ]),
        )

        with patch("server.db", db):
            result = await list_distributors(user={})

        by_id = {item["id"]: item for item in result}
        self.assertEqual(sum(item["total_payable"] for item in result), 60)
        self.assertEqual(sum(item["total_receivable_from_distributors"] for item in result), 55)
        self.assertEqual(sum(item["net_distributor_balance"] for item in result), 5)
        self.assertEqual(sum(item["total_paid_adjusted"] for item in result), 40)
        self.assertEqual(by_id["receivable"]["total_paid_adjusted"], 0)
        self.assertEqual(by_id["receivable"]["total_receivable_from_distributors"], 55)

    async def test_customer_summary_combines_invoices_and_unlinked_credit_ledger_without_double_counting(self):
        db = SimpleNamespace(
            customers=Collection([{"id": "c1", "name": "C", "opening_balance": 5}]),
            invoices=Collection([{"id": "i1", "customer_id": "c1", "total": 100, "paid_amount": 20}]),
            customer_transactions=Collection([
                {"customer_id": "c1", "type": "sale", "amount": 80, "invoice_id": "i1"},
                {"customer_id": "c1", "type": "sale", "amount": 25, "reference": "legacy-credit"},
                {"customer_id": "c1", "type": "payment", "amount": 10},
            ]),
        )
        with patch("server.db", db): result = await list_customers(user={})
        self.assertEqual(result[0]["total_sales"], 125)
        self.assertEqual(result[0]["total_paid"], 30)
        self.assertEqual(result[0]["receivable_balance"], 100)

    async def test_purchase_return_report_exposes_non_empty_analytics_rows(self):
        db = SimpleNamespace(purchase_returns=Collection([{"id": "r1", "return_quantity": 2, "purchase_rate": 5}]))
        with patch("server.db", db): result = await purchase_return_report(user={})
        self.assertEqual(result["purchase_returns"][0]["id"], "r1")
        self.assertEqual(result["returns_by_medicine"][0]["value"], 10)

from server import distributor_ledger


class DistributorListBalanceRestorationRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_distributor_list_balance_uses_transaction_logic_not_ledger_po_rows(self):
        db = SimpleNamespace(
            distributors=Collection([{"id": "midha", "name": "MIDHA DISTRIBUTORS"}]),
            distributor_transactions=Collection([
                {"id": "t-inv", "distributor_id": "midha", "type": "purchase", "amount": 35037.01,
                 "invoice_no": "M-100", "created_at": "2026-05-01"},
                {"id": "pay", "distributor_id": "midha", "type": "payment", "amount": 21265,
                 "created_at": "2026-05-02"},
            ]),
            purchase_orders=Collection([
                {"id": "po-midha", "distributor_id": "midha", "po_no": "PO-MIDHA", "invoice_ref": "M-100",
                 "grand_total": 35037.01, "po_date": "2026-05-01"},
            ]),
        )
        with patch("server.db", db):
            listed = await list_distributors(user={})
            ledger = await distributor_ledger("midha", user={})

        self.assertEqual(listed[0]["current_balance"], 13772.01)
        self.assertEqual(listed[0]["outstanding_balance"], 13772.01)
        self.assertEqual(listed[0]["total_purchases"], 35037.01)
        self.assertEqual(ledger["total_purchases"], 35037.01)
        self.assertEqual(ledger["total_paid"], 21265)
        self.assertEqual(ledger["balance"], 13772.01)
        self.assertEqual(ledger["transactions"][-1]["running_balance"], ledger["balance"])

    async def test_distributor_list_balance_is_not_recalculated_from_ledger_bottom_summary(self):
        db = SimpleNamespace(
            distributors=Collection([{"id": "abhi", "name": "ABHI ENTERPRISES"}]),
            distributor_transactions=Collection([
                {"id": "pay", "distributor_id": "abhi", "type": "payment", "amount": 36649,
                 "created_at": "2026-05-02"},
            ]),
            purchase_orders=Collection([
                {"id": "po-abhi", "distributor_id": "abhi", "po_no": "PO-ABHI", "invoice_ref": "A-100",
                 "grand_total": 50254, "po_date": "2026-05-01"},
            ]),
        )
        with patch("server.db", db):
            listed = await list_distributors(user={})
            ledger = await distributor_ledger("abhi", user={})

        self.assertEqual(listed[0]["current_balance"], -36649)
        self.assertEqual(listed[0]["outstanding_balance"], -36649)
        self.assertEqual(listed[0]["total_purchases"], 0)
        self.assertEqual(ledger["total_purchases"], 0)
        self.assertEqual(ledger["total_paid"], 36649)
        self.assertEqual(ledger["balance"], -36649)
        self.assertEqual(ledger["transactions"][-1]["running_balance"], ledger["balance"])
