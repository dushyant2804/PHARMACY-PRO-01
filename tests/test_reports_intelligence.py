import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")
from server import expiry_report, outstanding_report, purchase_return_report, sales_report, stock_valuation

class Cursor:
    def __init__(self, rows): self.rows = rows
    async def to_list(self, length): return [dict(row) for row in self.rows[:length]]
class Collection:
    def __init__(self, rows): self.rows = rows
    def find(self, query=None, projection=None):
        rows = self.rows
        if query and "id" in query and isinstance(query["id"], dict): rows = [r for r in rows if r.get("id") in query["id"].get("$in", [])]
        return Cursor(rows)

def iso(days): return (datetime.now(timezone.utc)-timedelta(days=days)).isoformat()

class ReportsIntelligenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_sales_rounding_batch_cost_and_legacy_fallback(self):
        db = SimpleNamespace(invoices=Collection([{"created_at": "2026-06-01T00:00:00+00:00", "total": 20.125, "gst_total": 1.005, "items": [{"medicine_id":"m1","quantity":2,"line_total":20.125,"purchase_cost":7.115}, {"medicine_id":"m2","quantity":1,"line_total":5,"mrp":5}]}]), medicines=Collection([{"id":"m1","purchase_price":99},{"id":"m2","purchase_price":2}]))
        with patch("server.db", db): result = await sales_report(user={})
        self.assertEqual(result["total_sales"], 20.13); self.assertEqual(result["total_gst"], 1.01)
        self.assertEqual(result["estimated_profit"], 16.01); self.assertEqual(result["monthly_sales_trend"][0]["sales"], 20.13)

    async def test_expiry_value_at_risk(self):
        medicines = Collection([{"id":"expired","purchased_units":3,"sold_units":1,"purchase_price":10.125,"mrp":20,"expiry_date":iso(2)}, {"id":"near","purchased_units":1,"sold_units":0,"purchase_price":5.555,"mrp":10,"expiry_date":iso(-20)}])
        with patch("server.db", SimpleNamespace(medicines=medicines)):
            stock = await stock_valuation(user={}); expiry = await expiry_report(user={})
        self.assertEqual(stock["total_expiry_value_at_risk"], 25.81); self.assertEqual(expiry["total_value_at_risk"], 25.81)

    async def test_outstanding_split_and_legacy_distributor_opening_balance(self):
        db = SimpleNamespace(customers=Collection([{"id":"c1","name":"C"}]), distributors=Collection([{"id":"d1","name":"D","opening_balance":10}]), customer_transactions=Collection([{"customer_id":"c1","type":"sale","amount":100,"created_at":iso(100)}, {"customer_id":"c1","type":"payment","amount":25,"created_at":iso(1)}]), distributor_transactions=Collection([]))
        with patch("server.db", db): result = await outstanding_report(user={})
        self.assertEqual(result["customer_receivables"], 75.0); self.assertEqual(result["customer_aging"]["90+"], 75.0)
        self.assertEqual(result["distributor_payables"], 10.0); self.assertEqual(result["distributor_aging"]["0-30"], 10.0)

    async def test_purchase_return_summary_uses_hardened_settlement(self):
        db = SimpleNamespace(purchase_returns=Collection([{"return_quantity":2,"purchase_rate":10.125,"ledger_adjusted":True}, {"return_quantity":1,"purchase_rate":5.555}]))
        with patch("server.db", db): result = await purchase_return_report(user={})
        self.assertEqual(result["returned_quantity"], 3); self.assertEqual(result["total_return_value"], 25.81)
        self.assertEqual(result["settled_return_value"], 20.25); self.assertEqual(result["unsettled_return_value"], 5.56)
