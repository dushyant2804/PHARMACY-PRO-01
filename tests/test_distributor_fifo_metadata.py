import json
import os
import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import (  # noqa: E402
    _apply_distributor_transaction,
    _build_distributor_fifo_metadata,
    _json_safe_ledger_transaction,
    _normalize_opening_balance_transaction,
)


class LegacyId:
    def __str__(self):
        return "legacy-object-id"


def test_fifo_metadata_skips_malformed_rows_and_stays_json_serializable():
    metadata = _build_distributor_fifo_metadata(
        [
            {
                "id": LegacyId(),
                "distributor_id": "dist-1",
                "type": "purchase",
                "amount": Decimal("100.50"),
                "created_at": datetime(2025, 4, 1, tzinfo=timezone.utc),
            },
            {
                "id": "bad-amount",
                "distributor_id": "dist-1",
                "type": "purchase",
                "amount": "not-a-number",
                "created_at": None,
                "transaction_date": "not-a-date",
            },
            {
                "id": "payment-1",
                "distributor_id": "dist-1",
                "type": "payment",
                "amount": "60.25",
                "transaction_date": "2025-04-02",
                "reference": None,
            },
            {
                "id": "empty-payment",
                "distributor_id": "dist-1",
                "type": "payment",
                "amount": None,
            },
        ],
        "dist-1",
    )

    json.dumps(metadata)
    assert metadata["legacy-object-id"]["bill_amount"] == 100.5
    assert metadata["legacy-object-id"]["due_amount"] == 40.25
    assert metadata["legacy-object-id"]["bill_status"] == "oldest_due"
    assert metadata["payment-1"] == {
        "adjusted_against": [
            {
                "invoice_no": "legacy-object-id",
                "transaction_id": "legacy-object-id",
                "amount": 60.25,
            }
        ]
    }
    assert metadata["empty-payment"] == {"adjusted_against": []}
    assert "bad-amount" not in metadata


def test_ledger_transaction_amount_and_opening_balance_guards_handle_bad_values():
    balance, bucket = _apply_distributor_transaction(
        10,
        {"type": "purchase", "amount": "not-a-number"},
    )
    assert balance == 10
    assert bucket == "purchase"

    normalized = _normalize_opening_balance_transaction(
        {"id": LegacyId(), "amount": "bad", "created_at": None},
        {"id": "dist-1", "opening_balance": "50.00"},
    )
    safe = _json_safe_ledger_transaction(normalized)
    json.dumps(safe)
    assert safe["id"] == "legacy-object-id"
    assert safe["amount"] == 0.0


def test_fifo_metadata_clears_bills_in_stable_chronological_order():
    transactions = [
        {
            "id": "INV001",
            "distributor_id": "dist-1",
            "type": "purchase",
            "amount": 1000,
            "transaction_date": "2025-04-01",
            "created_at": "2025-04-01T09:00:00+00:00",
        },
        {
            "id": "PAY001",
            "distributor_id": "dist-1",
            "type": "payment",
            "amount": 700,
            "transaction_date": "2025-04-01",
            "created_at": "2025-04-01T10:00:00+00:00",
        },
        {
            "id": "INV002",
            "distributor_id": "dist-1",
            "type": "purchase",
            "amount": 1300,
            "transaction_date": "2025-04-02",
            "created_at": "2025-04-02T09:00:00+00:00",
        },
        {
            "id": "PAY002",
            "distributor_id": "dist-1",
            "type": "payment",
            "amount": 1500,
            "transaction_date": "2025-04-02",
            "created_at": "2025-04-02T10:00:00+00:00",
        },
    ]

    before_second_payment = _build_distributor_fifo_metadata(transactions[:3], "dist-1")
    assert before_second_payment["INV001"]["paid_amount"] == 700
    assert before_second_payment["INV001"]["due_amount"] == 300
    assert before_second_payment["INV001"]["bill_status"] == "oldest_due"
    assert before_second_payment["INV002"]["paid_amount"] == 0
    assert before_second_payment["INV002"]["due_amount"] == 1300
    assert before_second_payment["INV002"]["bill_status"] == "later_due"

    metadata = _build_distributor_fifo_metadata(transactions, "dist-1")
    assert metadata["INV001"]["paid_amount"] == 1000
    assert metadata["INV001"]["due_amount"] == 0
    assert metadata["INV001"]["bill_status"] == "cleared"
    assert metadata["INV002"]["paid_amount"] == 1200
    assert metadata["INV002"]["due_amount"] == 100
    assert metadata["INV002"]["bill_status"] == "oldest_due"
    assert [
        item["transaction_id"]
        for item in metadata["PAY002"]["adjusted_against"]
    ] == ["INV001", "INV002"]
    assert sum(
        1 for item in metadata.values() if item.get("bill_status") == "oldest_due"
    ) == 1


def test_fifo_metadata_keeps_unapplied_credit_for_following_bill_with_same_timestamp():
    metadata = _build_distributor_fifo_metadata(
        [
            {
                "id": "aaa-payment-before-linked-purchase",
                "distributor_id": "dist-1",
                "type": "payment",
                "amount": 100,
                "created_at": "2025-04-01T09:00:00+00:00",
                "linked_transaction_id": "zzz-linked-purchase",
            },
            {
                "id": "zzz-linked-purchase",
                "distributor_id": "dist-1",
                "type": "purchase",
                "amount": 100,
                "created_at": "2025-04-01T09:00:00+00:00",
            },
            {
                "id": "malformed-credit",
                "distributor_id": "dist-1",
                "type": "payment",
                "amount": "not-a-number",
                "created_at": "not-a-date",
            },
        ],
        "dist-1",
    )

    assert metadata["zzz-linked-purchase"]["paid_amount"] == 100
    assert metadata["zzz-linked-purchase"]["due_amount"] == 0
    assert metadata["zzz-linked-purchase"]["bill_status"] == "cleared"
    assert metadata["aaa-payment-before-linked-purchase"]["adjusted_against"] == [
        {
            "invoice_no": "zzz-linked-purchase",
            "transaction_id": "zzz-linked-purchase",
            "amount": 100,
        }
    ]
    assert metadata["malformed-credit"] == {"adjusted_against": []}
    assert not any(item.get("bill_status") == "oldest_due" for item in metadata.values())
