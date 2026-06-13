import os

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import InvoiceCreate, _invoice_paid_amount, _normalize_invoice


def test_payment_mode_is_separate_and_supports_mixed():
    invoice = InvoiceCreate(customer_name="Asha", payment_mode="mixed", items=[])
    assert invoice.customer_name == "Asha"
    assert invoice.payment_mode == "mixed"


def test_credit_and_mixed_preserve_due_while_immediate_modes_default_to_paid():
    total = 125.555
    assert _invoice_paid_amount("credit", 0, total) == 0.0
    assert _invoice_paid_amount("mixed", 25.125, total) == 25.13
    for mode in ("cash", "upi", "card"):
        assert _invoice_paid_amount(mode, 0, total) == 125.56


def test_legacy_combined_invoice_is_normalized_without_exposing_profit():
    legacy = {
        "customer_name": "credit",
        "customer": "Legacy Customer",
        "payment": "credit",
        "total": 10.129,
        "paid_amount": 0,
        "due_amount": 10.129,
        "purchase_cost": 4,
        "estimated_profit": 6.129,
        "margin_percentage": 60.51,
        "items": [{
            "name": "Medicine",
            "line_total": 10.129,
            "purchase_cost": 4,
            "estimated_profit": 6.129,
        }],
    }

    public = _normalize_invoice(legacy)
    assert public["customer_name"] == "Legacy Customer"
    assert public["payment_mode"] == "credit"
    assert public["total"] == 10.13
    assert public["due_amount"] == 10.13
    assert "purchase_cost" not in public
    assert "estimated_profit" not in public
    assert "purchase_cost" not in public["items"][0]

    internal = _normalize_invoice(legacy, include_internal=True)
    assert internal["purchase_cost"] == 4.0
    assert internal["estimated_profit"] == 6.13
