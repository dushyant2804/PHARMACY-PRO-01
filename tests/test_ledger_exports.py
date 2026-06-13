import os
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import asyncio
from fastapi import HTTPException

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import _ledger_export_csv, _strip_internal_invoice_fields, export_ledger


async def response_text(response):
    return "".join([chunk.decode() if isinstance(chunk, bytes) else chunk async for chunk in response.body_iterator])


def test_customer_ledger_export_route():
    ledger = {"customer": {"name": "Ada", "phone": "123"}, "balance": 75, "transactions": [{"created_at": "2026-06-01", "type": "sale", "invoice_number": "INV-1", "mode": "credit", "amount": 75, "running_balance": 75, "notes": "Rx"}]}
    with patch("server.customer_ledger", AsyncMock(return_value=ledger)):
        response = asyncio.run(export_ledger("customer", "c1", date(2026, 6, 1), date(2026, 6, 30), {}))
    text = asyncio.run(response_text(response))
    assert "Ada,123" not in text
    assert "Ledger Owner,Ada" in text and "INV-1" in text and "Current Balance,75.0" in text


def test_distributor_ledger_export_route():
    ledger = {"distributor": {"name": "Supply Co", "phone": "456"}, "balance": 20, "transactions": [{"transaction_date": "2026-06-02", "type": "purchase", "bill_number": "B-1", "amount": 20, "running_balance": 20}]}
    with patch("server.distributor_ledger", AsyncMock(return_value=ledger)):
        response = asyncio.run(export_ledger("distributor", "d1", None, None, {}))
    text = asyncio.run(response_text(response))
    assert "Ledger Owner,Supply Co" in text and "B-1" in text and "Phone,456" in text


def test_invalid_ledger_type_rejected():
    with pytest.raises(HTTPException) as error:
        asyncio.run(export_ledger("vendor", "v1", None, None, {}))
    assert error.value.status_code == 400


def test_share_and_export_outputs_never_contain_internal_invoice_fields():
    unsafe = {"purchase_cost": 10, "profit": 2, "margin": 20, "estimated_profit": 2, "margin_percentage": 20, "items": [{"name": "Safe", "purchase_cost": 10}]}
    assert _strip_internal_invoice_fields(unsafe) == {"items": [{"name": "Safe"}]}
    csv_text = _ledger_export_csv("customer", {"customer": {"name": "Safe"}, "balance": 0, "transactions": []}, None, None).lower()
    assert "purchase_cost" not in csv_text and "profit" not in csv_text and "margin" not in csv_text
