import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")
from server import category_profitability, dead_stock_report, expiry_report, fast_moving_medicines, medicine_profitability, outstanding_report, purchase_return_report, reorder_report, sales_report, slow_moving_medicines, stock_valuation

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
        self.assertEqual(result["medicine_wise_return_analytics"][0]["value"], 25.81)

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
