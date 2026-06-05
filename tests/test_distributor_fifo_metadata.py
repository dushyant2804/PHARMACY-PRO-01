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
