import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")
from server import category_profitability, dashboard_summary, dead_stock_report, expiry_report, fast_moving_medicines, medicine_profitability, outstanding_report, purchase_return_report, reorder_report, sales_report, slow_moving_medicines, stock_valuation

class Cursor:
    def __init__(self, rows): self.rows = rows
    async def to_list(self, length): return [dict(row) for row in self.rows[:length]]
class Collection:
    def __init__(self, rows): self.rows = rows
    def find(self, query=None, projection=None):
        rows = self.rows
        if query and "id" in query and isinstance(query["id"], dict): rows = [r for r in rows if r.get("id") in query["id"].get("$in", [])]
        return Cursor(rows)


def assert_no_invalid_numbers(testcase, value):
    if isinstance(value, float):
        testcase.assertFalse(value != value, "NaN returned")
        testcase.assertNotIn(value, (float("inf"), float("-inf")))
    elif isinstance(value, dict):
        for child in value.values():
            assert_no_invalid_numbers(testcase, child)
    elif isinstance(value, list):
        for child in value:
            assert_no_invalid_numbers(testcase, child)


def assert_no_overlay_state(testcase, value, path="payload"):
    overlay_keys = {"loading", "is_loading", "isLoading", "overlay", "show_overlay", "showOverlay"}
    if isinstance(value, dict):
        for key, child in value.items():
            testcase.assertNotIn(key, overlay_keys, f"Backend payload exposed frontend overlay/loading key at {path}.{key}")
            assert_no_overlay_state(testcase, child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            assert_no_overlay_state(testcase, child, f"{path}[{index}]")

def iso(days): return (datetime.now(timezone.utc)-timedelta(days=days)).isoformat()

class ReportsIntelligenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_sales_rounding_batch_cost_and_legacy_fallback(self):
        db = SimpleNamespace(invoices=Collection([{"created_at": "2026-06-01T00:00:00+00:00", "total": 20.125, "gst_total": 1.005, "items": [{"medicine_id":"m1","quantity":2,"line_total":20.125,"purchase_cost":7.115}, {"medicine_id":"m2","quantity":1,"line_total":5,"mrp":5}]}]), medicines=Collection([{"id":"m1","purchase_price":99},{"id":"m2","purchase_price":2}]))
        with patch("server.db", db): result = await sales_report(user={})
        self.assertEqual(result["total_sales"], 20.13); self.assertEqual(result["total_gst"], 1.01)
        self.assertEqual(result["estimated_profit"], 16.01); self.assertEqual(result["monthly_sales_trend"][0]["sales"], 20.13)
        self.assertEqual(result["average_bill_value"], 20.13)
        self.assertEqual(result["monthly_profit_trend"][0]["profit"], 16.01)
        self.assertEqual(result["top_profit_medicines"][0]["medicine"], "Unknown")

    async def test_expiry_value_at_risk(self):
        medicines = Collection([{"id":"expired","purchased_units":3,"sold_units":1,"purchase_price":10.125,"mrp":20,"expiry_date":iso(2)}, {"id":"near","purchased_units":1,"sold_units":0,"purchase_price":5.555,"mrp":10,"expiry_date":iso(-20)}])
        with patch("server.db", SimpleNamespace(medicines=medicines)):
            stock = await stock_valuation(user={}); expiry = await expiry_report(user={})
        self.assertEqual(stock["total_expiry_value_at_risk"], 25.81); self.assertEqual(expiry["total_value_at_risk"], 25.81)
        self.assertEqual(expiry["expiry_risk_count"], 2)
        self.assertGreater(expiry["expiry_value_at_risk"], 0)

    async def test_expiry_report_uses_only_available_stock_and_cost_rate(self):
        medicines = Collection([
            {"id":"expired","purchased_units":5,"sold_units":2,"purchase_return_units":1,"purchase_price":10,"mrp":100,"expiry_date":iso(2)},
            {"id":"30","purchased_units":2,"purchase_price":7,"mrp":70,"expiry_date":iso(-20)},
            {"id":"90","purchased_units":3,"purchase_price":4,"mrp":40,"expiry_date":iso(-60)},
            {"id":"safe","purchased_units":1,"purchase_price":5,"expiry_date":iso(-120)},
            {"id":"sold","purchased_units":2,"sold_units":2,"purchase_price":99,"expiry_date":iso(2)},
            {"id":"returned","purchased_units":2,"purchase_return_units":2,"purchase_price":99,"expiry_date":iso(-10)},
        ])
        with patch("server.db", SimpleNamespace(medicines=medicines)):
            result = await expiry_report(user={})
        self.assertEqual(result["expired_count"], 1)
        self.assertEqual(result["expiring_30_count"], 1)
        self.assertEqual(result["expiring_90_count"], 1)
        self.assertEqual(result["safe_count"], 1)
        self.assertEqual(result["expiry_risk_count"], 3)
        self.assertEqual(result["expired_value_at_risk"], 20)
        self.assertEqual(result["expiring_30_value_at_risk"], 14)
        self.assertEqual(result["expiring_90_value_at_risk"], 12)
        self.assertEqual(result["expiry_value_at_risk"], 46)
        self.assertEqual(result["total_inventory_cost_value"], 51)
        self.assertEqual(result["top_expiry_risk_medicines"][0]["risk_value"], 20)

    async def test_stock_and_expiry_api_expose_exact_non_overlapping_risk_bucket_values(self):
        medicines = Collection([
            {"id":"expired","purchased_units":3,"sold_units":1,"purchase_price":10.125,"expiry_date":iso(2)},
            {"id":"30","purchased_units":2,"purchase_price":7.005,"expiry_date":iso(-30)},
            {"id":"90","purchased_units":3,"purchase_price":4.005,"expiry_date":iso(-31)},
            {"id":"safe","purchased_units":5,"purchase_price":100,"expiry_date":iso(-91)},
            {"id":"sold","purchased_units":2,"sold_units":2,"purchase_price":100,"expiry_date":iso(2)},
            {"id":"returned","purchased_units":2,"purchase_return_units":2,"purchase_price":100,"expiry_date":iso(-10)},
        ])
        with patch("server.db", SimpleNamespace(medicines=medicines)):
            reports = [await stock_valuation(user={}), await expiry_report(user={})]

        bucket_fields = ("expired_value_at_risk", "expiring_30_value_at_risk", "expiring_90_value_at_risk")
        for result in reports:
            self.assertGreater(result["expiry_value_at_risk"], 0)
            for field in bucket_fields:
                self.assertIn(field, result)
                self.assertGreater(result[field], 0)
            self.assertEqual(result["expiry_value_at_risk"], sum(result[field] for field in bucket_fields))
            self.assertEqual(result["expiry_value_at_risk"], 46.28)

    async def test_outstanding_split_and_legacy_distributor_opening_balance(self):
        db = SimpleNamespace(customers=Collection([{"id":"c1","name":"C"}]), distributors=Collection([{"id":"d1","name":"D","opening_balance":10}]), customer_transactions=Collection([{"customer_id":"c1","type":"sale","amount":100,"created_at":iso(100)}, {"customer_id":"c1","type":"payment","amount":25,"created_at":iso(1)}]), distributor_transactions=Collection([]))
        with patch("server.db", db): result = await outstanding_report(user={})
        self.assertEqual(result["customer_receivables"], 75.0); self.assertEqual(result["customer_aging"]["90+"], 75.0)
        self.assertEqual(result["distributor_payables"], 10.0); self.assertEqual(result["distributor_aging"]["0-30"], 10.0)
        self.assertEqual(result["net_exposure"], -65.0)
        self.assertEqual(result["customer_recovery_ranking"][0]["outstanding"], 75.0)

    async def test_purchase_return_summary_uses_hardened_settlement(self):
        db = SimpleNamespace(purchase_returns=Collection([{"return_quantity":2,"purchase_rate":10.125,"ledger_adjusted":True}, {"return_quantity":1,"purchase_rate":5.555}]))
        with patch("server.db", db): result = await purchase_return_report(user={})
        self.assertEqual(result["returned_quantity"], 3); self.assertEqual(result["total_return_value"], 25.81)
        self.assertEqual(result["settled_return_value"], 20.25); self.assertEqual(result["unsettled_return_value"], 5.56)
        self.assertEqual(result["medicine_wise_return_analytics"][0]["return_value"], 20.25)
        self.assertEqual(result["medicine_wise_return_analytics"][0]["status"], "Ledger Adjusted")
        self.assertEqual(result["medicine_wise_return_analytics"][1]["status"], "Credit Pending / Recorded Only")

    async def test_dashboard_and_purchase_return_payloads_do_not_expose_overlay_state(self):
        returns = Collection([
            {
                "id": "return-ledger",
                "medicine_name": "Amoxicillin",
                "distributor_name": "Main Distributor",
                "return_quantity": 2,
                "purchase_rate": 10.125,
                "ledger_adjusted": True,
            },
            {
                "id": "return-pending",
                "medicine_name": "Cetirizine",
                "distributor_name": "Main Distributor",
                "return_quantity": 1,
                "purchase_rate": 5.555,
            },
        ])
        empty = Collection([])
        db = SimpleNamespace(
            invoices=empty, expenses=empty, medicines=empty, customers=empty, distributors=empty,
            customer_transactions=empty, distributor_transactions=empty, purchase_orders=empty,
            regular_patients=empty, purchase_returns=returns,
        )

        with patch("server.db", db):
            dashboard = await dashboard_summary(user={})
            purchase_returns = await purchase_return_report(user={})

        self.assertEqual(dashboard["sales_total"], 0)
        self.assertEqual(dashboard["patient_alerts"], [])
        self.assertEqual(purchase_returns["returned_quantity"], 3)
        self.assertEqual(purchase_returns["total_return_value"], 25.81)
        self.assertEqual(purchase_returns["return_count"], 2)
        self.assertEqual(purchase_returns["summary_buckets"], {
            "Ledger Adjusted Value": 20.25,
            "Pending Credit Value": 5.56,
            "Adjusted in Purchase Value": 0.0,
        })
        for payload in (dashboard, purchase_returns):
            assert_no_invalid_numbers(self, payload)
            assert_no_overlay_state(self, payload)

    async def test_profitability_dead_stock_and_reorder_use_invoice_history(self):
        invoices = Collection([{"created_at":"2026-01-01T00:00:00+00:00","items":[{"medicine_id":"m1","name":"A","quantity":10,"line_total":100,"purchase_cost":60}]}])
        medicines = Collection([{"id":"m1","name":"A","purchase_price":6,"purchased_units":30,"sold_units":10,"category":"OTC"}, {"id":"m2","name":"B","purchase_price":5,"purchased_units":4}])
        with patch("server.db", SimpleNamespace(invoices=invoices, medicines=medicines)):
            profit = await medicine_profitability(user={})
            category = await category_profitability(user={})
            dead = await dead_stock_report(days=90, user={})
            reorder = await reorder_report(user={})
        self.assertEqual(profit["items"][0]["profit"], 40)
        self.assertEqual(category["items"][0]["category"], "OTC")
        self.assertTrue(any(item["medicine"] == "B" for item in dead["items"]))
        self.assertEqual(reorder["items"][0]["medicine"], "A")


    async def test_reports_no_data_return_empty_arrays_and_zero_summaries(self):
        empty = Collection([])
        db = SimpleNamespace(
            invoices=empty, medicines=empty, customers=empty, distributors=empty,
            customer_transactions=empty, distributor_transactions=empty, purchase_returns=empty,
        )
        with patch("server.db", db):
            sales = await sales_report(user={})
            expiry = await expiry_report(user={})
            outstanding = await outstanding_report(user={})
            medicine_profit = await medicine_profitability(user={})
            category_profit = await category_profitability(user={})
            fast = await fast_moving_medicines(user={})
            slow = await slow_moving_medicines(user={})
            dead = await dead_stock_report(user={})
            reorder = await reorder_report(user={})
            returns = await purchase_return_report(user={})

        self.assertEqual(sales["total_sales"], 0)
        self.assertEqual(sales["monthly_sales_trend"], [])
        self.assertEqual(sales["monthly_profit_trend"], [])
        self.assertEqual(sales["payment_mode_distribution"], [])
        self.assertEqual(sales["top_revenue_medicines"], [])
        self.assertEqual(sales["top_profit_medicines"], [])
        self.assertEqual(expiry["expiry_value_at_risk"], 0)
        self.assertEqual(expiry["top_expiry_risk_medicines"], [])
        self.assertEqual(outstanding["customer_receivables"], 0)
        self.assertEqual(outstanding["monthly_outstanding_trend"], [])
        self.assertEqual(outstanding["distributor_outstanding_movement"], [])
        self.assertEqual(medicine_profit["items"], [])
        self.assertEqual(category_profit["items"], [])
        self.assertEqual(fast["items"], [])
        self.assertEqual(slow["items"], [])
        self.assertEqual(dead["items"], [])
        self.assertEqual(reorder["items"], [])
        self.assertEqual(returns["medicine_wise_return_analytics"], [])
        for payload in (sales, expiry, outstanding, medicine_profit, category_profit, fast, slow, dead, reorder, returns):
            assert_no_invalid_numbers(self, payload)

    async def test_chart_ready_arrays_round_money_and_numeric_aging_days(self):
        invoices = Collection([{
            "created_at": "2026-06-01T00:00:00+00:00", "total": "NaN", "gst_total": float("inf"), "payment_mode": "cash",
            "items": [{"medicine_id":"m1","name":"A","quantity":2,"line_total":20.129,"purchase_cost":7.115,"category":"OTC"}],
        }])
        medicines = Collection([{
            "id":"m1", "name":"A", "purchase_price":3.557, "purchased_units":10, "sold_units":2,
            "category":"OTC", "expiry_date": iso(-15),
        }])
        db = SimpleNamespace(
            invoices=invoices, medicines=medicines, customers=Collection([{"id":"c1","name":"C"}]), distributors=Collection([]),
            customer_transactions=Collection([{"customer_id":"c1","type":"sale","amount":10.129,"created_at":"not-a-date"}]),
            distributor_transactions=Collection([]),
        )
        with patch("server.db", db):
            sales = await sales_report(user={})
            expiry = await expiry_report(user={})
            outstanding = await outstanding_report(user={})
            med_profit = await medicine_profitability(user={})
            cat_profit = await category_profitability(user={})
            fast = await fast_moving_medicines(user={})
            slow = await slow_moving_medicines(user={})

        self.assertIsInstance(sales["monthly_sales_trend"], list)
        self.assertEqual(sales["total_sales"], 0)
        self.assertEqual(sales["estimated_profit"], 13.01)
        self.assertEqual(med_profit["items"][0]["revenue"], 20.13)
        self.assertEqual(cat_profit["items"], [{"category":"OTC", "revenue":20.13, "profit":13.01, "margin":64.65}])
        self.assertGreater(expiry["expiry_value_at_risk"], 0)
        self.assertGreater(len(expiry["top_expiry_risk_medicines"]), 0)
        self.assertEqual(expiry["top_expiry_risk_medicines"][0]["risk_value"], expiry["expiring_30_value_at_risk"])
        self.assertIsInstance(outstanding["customers"][0]["aging_days"], int)
        self.assertGreaterEqual(outstanding["customers"][0]["aging_days"], 0)
        self.assertEqual(fast["items"][0]["units_sold"], 2)
        self.assertEqual(slow["items"][0]["current_stock"], 8)
        for payload in (sales, expiry, outstanding, med_profit, cat_profit, fast, slow):
            assert_no_invalid_numbers(self, payload)

class CustomerOutstandingMovementTests(unittest.IsolatedAsyncioTestCase):
    async def test_customer_ledger_monthly_summary_helper_generates_rows_and_ignores_invalid_dates(self):
        from server import _customer_monthly_summary_from_transactions
        rows = _customer_monthly_summary_from_transactions([
            {"customer_id": "c1", "type": "sale", "amount": 100.129, "created_at": "2026-05-01T00:00:00+00:00"},
            {"customer_id": "c1", "type": "payment", "amount": 25.124, "created_at": "2026-05-02T00:00:00+00:00"},
            {"customer_id": "c1", "type": "sale", "amount": 10, "created_at": "not-a-date"},
        ])
        self.assertEqual(rows, [{
            "month": "2026-05",
            "total_credit_sales": 100.13,
            "sales_added": 100.13,
            "total_payments_received": 25.12,
            "net_receivable_movement": 75.01,
            "closing_receivable_balance": 75.01,
            "transaction_count": 2,
        }])

    async def test_outstanding_movement_uses_distributor_ledger_only(self):
        scenarios = [
            (
                SimpleNamespace(customers=Collection([]), distributors=Collection([{"id":"d1","name":"D","opening_balance":10}]), customer_transactions=Collection([]), distributor_transactions=Collection([
                    {"distributor_id":"d1","type":"purchase","amount":20.129,"created_at":"2026-05-01T00:00:00+00:00"},
                    {"distributor_id":"d1","type":"payment","amount":5,"created_at":"bad-date"},
                    {"distributor_id":"d1","type":"payment","amount":3,"created_at":"2026-06-01T00:00:00+00:00"},
                    {"distributor_id":"d1","type":"purchase_return","amount":2,"created_at":"2026-06-02T00:00:00+00:00"},
                ])),
                [
                    {"month":"2026-05", "purchases":20.13, "payments":0.0, "adjustments":0.0, "opening_distributor_payable":0.0, "closing_distributor_payable":30.13, "outstanding_payable":30.13, "net_movement":30.13, "outstanding_increase":30.13, "outstanding_decrease":0.0},
                    {"month":"2026-06", "purchases":0.0, "payments":3.0, "adjustments":2.0, "opening_distributor_payable":30.13, "closing_distributor_payable":25.13, "outstanding_payable":25.13, "net_movement":-5.0, "outstanding_increase":0.0, "outstanding_decrease":5.0},
                ],
            ),
            (
                SimpleNamespace(customers=Collection([{"id":"c1","name":"C"}]), distributors=Collection([]), customer_transactions=Collection([
                    {"customer_id":"c1","type":"sale","amount":30.555,"created_at":"2026-05-01T00:00:00+00:00"},
                    {"customer_id":"c1","type":"payment","amount":10.111,"created_at":"2026-05-03T00:00:00+00:00"},
                ]), distributor_transactions=Collection([])),
                [],
            ),
        ]
        for db, expected in scenarios:
            with patch("server.db", db):
                result = await outstanding_report(user={})
            self.assertEqual(result["distributor_outstanding_movement"], expected)
            self.assertNotIn("outstanding_movement", result)


    async def test_outstanding_movement_returns_single_distributor_opening_balance(self):
        db = SimpleNamespace(
            customers=Collection([{"id":"c1","name":"C"}]),
            distributors=Collection([{"id":"d1","name":"Only Distributor","opening_balance":42.5,"created_at":"2026-04-15T00:00:00+00:00"}]),
            customer_transactions=Collection([
                {"customer_id":"c1","type":"sale","amount":999,"created_at":"2026-04-01T00:00:00+00:00"},
            ]),
            distributor_transactions=Collection([]),
        )
        with patch("server.db", db):
            result = await outstanding_report(user={})

        self.assertEqual(result["distributor_outstanding_movement"], [{
            "month":"2026-04",
            "purchases":0.0,
            "payments":0.0,
            "adjustments":0.0,
            "opening_distributor_payable":0.0,
            "closing_distributor_payable":42.5,
            "outstanding_payable":42.5,
            "net_movement":42.5,
            "outstanding_increase":42.5,
            "outstanding_decrease":0.0,
        }])
        self.assertEqual(result["monthly_outstanding_trend"][0]["customer_receivables"], 0.0)


class CustomerLedgerRouteMonthlySummaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_customer_monthly_summary_appears_when_transactions_exist(self):
        from server import customer_ledger

        class QueryCursor(Cursor):
            def sort(self, *args):
                self.rows = sorted(self.rows, key=lambda row: str(row.get("created_at") or row.get("transaction_date") or row.get("date") or ""))
                return self

        class LedgerCollection(Collection):
            def find(self, query=None, projection=None):
                rows = self.rows
                if query and query.get("customer_id"):
                    rows = [r for r in rows if r.get("customer_id") == query["customer_id"]]
                return QueryCursor(rows)
            async def find_one(self, query=None, projection=None):
                return next((dict(r) for r in self.rows if not query or all(r.get(k) == v for k, v in query.items())), None)
            async def update_one(self, query, update):
                for row in self.rows:
                    if all(row.get(k) == v for k, v in query.items()):
                        row.update(update.get("$set", {}))
                return None

        db = SimpleNamespace(
            customers=LedgerCollection([{"id": "c-real", "name": "Real Customer"}]),
            customer_transactions=LedgerCollection([
                {"id": "sale-1", "customer_id": "c-real", "type": "sale", "amount": 150.25, "transaction_date": "2026-06-05", "created_at": "2026-06-06T10:00:00+00:00"},
                {"id": "pay-1", "customer_id": "c-real", "type": "payment", "amount": 50.10, "date": "2026-06-07", "created_at": "2026-06-07T10:00:00+00:00"},
            ]),
        )
        with patch("server.db", db):
            result = await customer_ledger("c-real", user={})

        self.assertEqual(result["monthly_summary"], [{
            "month": "2026-06",
            "total_credit_sales": 150.25,
            "sales_added": 150.25,
            "total_payments_received": 50.1,
            "net_receivable_movement": 100.15,
            "closing_receivable_balance": 100.15,
            "transaction_count": 2,
        }])
        self.assertEqual(result["monthly_movement_summary"], result["monthly_summary"])

    async def test_customer_monthly_summary_uses_safe_id_date_fallback(self):
        from server import _customer_monthly_summary_from_transactions
        rows = _customer_monthly_summary_from_transactions([
            {"id": "2026-06-09-legacy-sale", "type": "sale", "amount": 10},
            {"id": "uuid-without-date", "type": "sale", "amount": 99},
        ])
        self.assertEqual(rows[0]["month"], "2026-06")
        self.assertEqual(rows[0]["transaction_count"], 1)


class PurchaseReturnBusinessStatusReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_purchase_returns_get_clear_status_labels_and_deleted_excluded(self):
        rows = [
            {"id": "ledger", "medicine_name": "A", "distributor": "D", "return_quantity": 2, "purchase_rate": 10, "return_date": "2026-06-01", "ledger_adjusted": True},
            {"id": "pending", "medicine_name": "B", "distributor": "D", "return_quantity": 1, "purchase_rate": 5, "return_date": "2026-06-02", "ledger_adjusted": False},
            {"id": "po", "medicine_name": "C", "distributor": "D", "return_quantity": 3, "purchase_rate": 7, "return_date": "2026-06-03", "po_adjustment_id": "po-1"},
            {"id": "deleted", "medicine_name": "X", "distributor": "D", "return_quantity": 9, "purchase_rate": 99, "return_date": "2026-06-04", "settlement_status": "deleted"},
        ]
        with patch("server.db", SimpleNamespace(purchase_returns=Collection(rows))):
            result = await purchase_return_report(user={})

        self.assertEqual(result["return_count"], 3)
        statuses = {row["medicine"]: row["status"] for row in result["medicine_wise_return_analytics"]}
        self.assertEqual(statuses, {"A": "Ledger Adjusted", "B": "Credit Pending / Recorded Only", "C": "Adjusted in Purchase"})
        self.assertEqual(result["summary_buckets"], {
            "Ledger Adjusted Value": 20.0,
            "Pending Credit Value": 5.0,
            "Adjusted in Purchase Value": 21.0,
        })
        self.assertEqual({row["id"] for row in result["purchase_returns"]}, {"ledger", "pending", "po"})
        self.assertEqual(next(row for row in result["purchase_returns"] if row["id"] == "po")["status"], "Adjusted in Purchase")
