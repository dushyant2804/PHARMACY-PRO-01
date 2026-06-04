import os

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")

from server import (  # noqa: E402
    _apply_distributor_transaction,
    _brought_forward_transaction,
    _calculate_distributor_bill_due_status,
    calculate_financial_year,
)


def _with_running_balance(transactions):
    balance = 0.0
    rows = []
    for txn in transactions:
        balance, _bucket = _apply_distributor_transaction(balance, txn)
        rows.append({**txn, "running_balance": round(balance, 2)})
    return rows


def test_calculate_financial_year_for_indian_financial_year_boundaries():
    assert calculate_financial_year("2025-12-10") == "2025-26"
    assert calculate_financial_year("2026-03-31") == "2025-26"
    assert calculate_financial_year("2026-04-01") == "2026-27"


def test_fifo_due_status_after_all_abc_transactions():
    transactions = _with_running_balance([
        {"id": "pur-1", "type": "purchase", "amount": 1000, "created_at": "2026-06-01"},
        {"id": "pay-1", "type": "payment", "amount": 700, "created_at": "2026-06-03"},
        {"id": "pur-2", "type": "purchase", "amount": 1300, "created_at": "2026-06-07"},
        {"id": "pay-2", "type": "payment", "amount": 1500, "created_at": "2026-06-09"},
    ])

    rows = {row["id"]: row for row in _calculate_distributor_bill_due_status(transactions)}

    assert rows["pur-1"]["bill_amount"] == 1000
    assert rows["pur-1"]["paid_amount"] == 1000
    assert rows["pur-1"]["due_amount"] == 0
    assert rows["pur-1"]["due_status"] == "cleared"
    assert rows["pur-2"]["bill_amount"] == 1300
    assert rows["pur-2"]["paid_amount"] == 1200
    assert rows["pur-2"]["due_amount"] == 100
    assert rows["pur-2"]["due_status"] == "oldest_due"
    assert rows["pay-1"]["due_status"] == "payment"
    assert rows["pay-2"]["due_status"] == "payment"


def test_fifo_due_status_before_second_abc_payment():
    transactions = _with_running_balance([
        {"id": "pur-1", "type": "purchase", "amount": 1000, "created_at": "2026-06-01"},
        {"id": "pay-1", "type": "payment", "amount": 700, "created_at": "2026-06-03"},
        {"id": "pur-2", "type": "purchase", "amount": 1300, "created_at": "2026-06-07"},
    ])

    rows = {row["id"]: row for row in _calculate_distributor_bill_due_status(transactions)}

    assert rows["pur-1"]["due_amount"] == 300
    assert rows["pur-1"]["due_status"] == "oldest_due"
    assert rows["pur-2"]["due_amount"] == 1300
    assert rows["pur-2"]["due_status"] == "later_due"


def test_brought_forward_row_shape():
    row = _brought_forward_transaction("dist-1", "2026-27", 250.25)

    assert row["type"] == "brought_forward"
    assert row["created_at"] == "2026-04-01"
    assert row["reference_number"] == "B/F from FY 2025-26"
    assert row["running_balance"] == 250.25
    assert row["due_status"] == "brought_forward"
    assert row["is_deletable"] is False


def test_financial_year_closing_balance_becomes_next_year_brought_forward():
    transactions = [
        {"id": "fy-2025-purchase", "type": "purchase", "amount": 500, "created_at": "2025-12-10"},
        {"id": "fy-2025-payment", "type": "payment", "amount": 200, "created_at": "2026-03-31"},
        {"id": "fy-2026-purchase", "type": "purchase", "amount": 100, "created_at": "2026-04-01"},
    ]

    fy_2025_closing = 0.0
    for txn in transactions[:2]:
        fy_2025_closing, _bucket = _apply_distributor_transaction(fy_2025_closing, txn)

    fy_2026_opening = 0.0
    for txn in transactions:
        if txn["created_at"] < "2026-04-01":
            fy_2026_opening, _bucket = _apply_distributor_transaction(fy_2026_opening, txn)

    assert fy_2025_closing == 300
    assert fy_2026_opening == fy_2025_closing
