import os

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "pharmacy_test")
os.environ.setdefault("JWT_SECRET", "test-secret")

from server import (
    InvoiceCreate,
    _invoice_paid_amount,
    _invoice_user_can_view_internal,
    _normalize_invoice,
    _strip_internal_invoice_fields,
)


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


def test_only_admin_can_receive_internal_invoice_profit_intelligence():
    assert _invoice_user_can_view_internal({"role": "admin"})
    assert not _invoice_user_can_view_internal({"role": "cashier"})
    assert not _invoice_user_can_view_internal({"role": "pharmacist"})
    assert not _invoice_user_can_view_internal(None)


def test_customer_print_pdf_and_share_sanitizer_removes_nested_profit_data():
    payload = {
        "invoice": {
            "purchase_cost": 10,
            "items": [{"estimated_profit": 2, "margin_percentage": 20, "name": "Safe"}],
        },
        "share": [{"purchase_cost": 1, "message": "Invoice"}],
    }
    sanitized = _strip_internal_invoice_fields(payload)
    assert sanitized == {
        "invoice": {"items": [{"name": "Safe"}]},
        "share": [{"message": "Invoice"}],
    }


def test_all_common_invoice_money_aliases_are_two_decimal_safe():
    invoice = {
        field: 10.129
        for field in (
            "subtotal", "discount", "gst", "grand_total", "total", "paid", "due",
            "purchase_cost", "estimated_profit", "margin_percentage",
        )
    }
    normalized = _normalize_invoice(invoice, include_internal=True)
    assert all(value == 10.13 for value in normalized.values() if isinstance(value, float))
