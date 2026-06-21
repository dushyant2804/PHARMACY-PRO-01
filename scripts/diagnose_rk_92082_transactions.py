#!/usr/bin/env python3
"""Read-only production diagnostic for the RK Pharma ₹92,082 ledger duplicate.

The script intentionally does not import ``server`` and performs no writes.
Run it with production ``MONGO_URL`` and ``DB_NAME`` in the environment:

    python scripts/diagnose_rk_92082_transactions.py

If the visible ₹92,082 row has a known invoice/reference value, pass it exactly:

    python scripts/diagnose_rk_92082_transactions.py --row-reference '...'
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

REPO_ROOT = Path(__file__).resolve().parents[1]
AMOUNT_TARGETS = {Decimal("92082"), Decimal("-92082")}
REFERENCE_FIELDS = (
    "reference",
    "reference_number",
    "reference_no",
    "invoice_ref",
    "invoice_number",
    "invoice_no",
    "bill_ref",
    "bill_number",
    "bill_no",
)
TYPE_FIELDS = ("transaction_type", "type")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print raw RK Pharma distributor_transactions related to ₹92,082/opening balance.",
    )
    parser.add_argument("--mongo-url", help="Defaults to MONGO_URL or the repository .env file.")
    parser.add_argument("--db-name", help="Defaults to DB_NAME or the repository .env file.")
    parser.add_argument(
        "--row-reference",
        action="append",
        default=[],
        help="Exact invoice/reference/bill value from the visible ₹92,082 row; repeat as needed.",
    )
    parser.add_argument(
        "--output",
        help="Optional JSON output path. Full matching rows are always printed to stdout.",
    )
    return parser.parse_args()


def configure_environment(args: argparse.Namespace) -> Tuple[str, str]:
    load_dotenv(REPO_ROOT / ".env")
    mongo_url = args.mongo_url or os.environ.get("MONGO_URL")
    db_name = args.db_name or os.environ.get("DB_NAME")
    missing = [name for name, value in (("MONGO_URL", mongo_url), ("DB_NAME", db_name)) if not value]
    if missing:
        raise SystemExit(
            f"Missing {', '.join(missing)}. Supply production values through environment variables "
            "or --mongo-url/--db-name."
        )
    return mongo_url, db_name


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (ObjectId, datetime, date, Decimal)):
        return str(value)
    return value


def normalized(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def amount_matches(value: Any) -> bool:
    try:
        return Decimal(str(value)) in AMOUNT_TARGETS
    except (ValueError, TypeError, ArithmeticError):
        return False


def opening_metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: json_safe(value)
        for key, value in row.items()
        if "opening_balance" in normalized(key) or normalized(key) == "metadata"
    }


def row_matches(row: Dict[str, Any], requested_references: Set[str]) -> List[str]:
    reasons = []
    if amount_matches(row.get("amount")):
        reasons.append("amount=±92082")

    text_fields = {
        key: normalized(value)
        for key, value in row.items()
        if isinstance(value, (str, int, float, Decimal))
    }
    if any("opening balance" in value or "opening_balance" in value for value in text_fields.values()):
        reasons.append("text contains opening balance/opening_balance")

    for field in TYPE_FIELDS:
        value = text_fields.get(field, "")
        if "opening" in value or "opening_balance" in value:
            reasons.append(f"{field} contains opening/opening_balance")

    for field in REFERENCE_FIELDS:
        value = text_fields.get(field, "")
        if value and value in requested_references:
            reasons.append(f"{field} matches requested row reference")

    return list(dict.fromkeys(reasons))


def diagnostic_row(
    row: Dict[str, Any],
    distributor: Dict[str, Any],
    match_reasons: List[str],
) -> Dict[str, Any]:
    return {
        "match_reasons": match_reasons,
        "_id": json_safe(row.get("_id")),
        "id": json_safe(row.get("id")),
        "distributor_id": json_safe(row.get("distributor_id")),
        "distributor_name": distributor.get("name") or distributor.get("distributor_name"),
        "amount": json_safe(row.get("amount")),
        "transaction_type": json_safe(row.get("transaction_type")),
        "type": json_safe(row.get("type")),
        "source_type": json_safe(row.get("source_type")),
        "entry_source": json_safe(row.get("entry_source")),
        "created_by": json_safe(
            row.get("created_by")
            or row.get("created_by_user")
            or row.get("user_id")
            or row.get("created_by_id")
        ),
        "reference": json_safe(row.get("reference")),
        "reference_number": json_safe(row.get("reference_number")),
        "invoice_ref": json_safe(row.get("invoice_ref")),
        "bill_ref": json_safe(row.get("bill_ref")),
        "purchase_order_id": json_safe(row.get("purchase_order_id")),
        "opening_balance_fields_or_metadata": opening_metadata(row),
        "date": json_safe(row.get("date")),
        "transaction_date": json_safe(row.get("transaction_date")),
        "created_at": json_safe(row.get("created_at")),
        "notes": json_safe(row.get("notes")),
        "raw_json": json_safe(row),
    }


def distributor_identity_values(distributor: Dict[str, Any]) -> Set[str]:
    return {
        normalized(distributor.get(field))
        for field in ("_id", "id", "distributor_id")
        if distributor.get(field) not in (None, "")
    }


def belongs_to_distributor(row: Dict[str, Any], identities: Set[str], names: Set[str]) -> bool:
    linked = {
        normalized(row.get(field))
        for field in ("distributor_id", "distributor", "distributorId")
        if row.get(field) not in (None, "")
    }
    if linked:
        return bool(linked & identities)
    row_names = {
        normalized(row.get(field))
        for field in ("distributor_name", "name")
        if row.get(field) not in (None, "")
    }
    return bool(row_names & names)


async def run_diagnostic(
    mongo_url: str,
    db_name: str,
    requested_references: Set[str],
) -> Dict[str, Any]:
    client = AsyncIOMotorClient(mongo_url, serverSelectionTimeoutMS=15000)
    try:
        database = client[db_name]
        await database.command("ping")
        distributors = await database.distributors.find({}).to_list(length=None)
        rk_distributors = [
            row
            for row in distributors
            if normalized(row.get("name") or row.get("distributor_name"))
            in {"rk pharma", "r k pharma"}
        ]
        if not rk_distributors:
            raise SystemExit("No distributor named RK Pharma or R K Pharma was found.")

        all_transactions = await database.distributor_transactions.find({}).to_list(length=None)
        reports = []
        for distributor in rk_distributors:
            identities = distributor_identity_values(distributor)
            names = {
                normalized(distributor.get(field))
                for field in ("name", "distributor_name")
                if distributor.get(field)
            }
            rows = []
            for transaction in all_transactions:
                if not belongs_to_distributor(transaction, identities, names):
                    continue
                reasons = row_matches(transaction, requested_references)
                if reasons:
                    rows.append(diagnostic_row(transaction, distributor, reasons))
            reports.append({
                "distributor": json_safe(distributor),
                "matching_row_count": len(rows),
                "matching_rows": rows,
            })

        purchase_orders = await database.purchase_orders.find({}).to_list(length=None)
        matching_purchase_orders = []
        for distributor in rk_distributors:
            identities = distributor_identity_values(distributor)
            names = {
                normalized(distributor.get(field))
                for field in ("name", "distributor_name")
                if distributor.get(field)
            }
            for purchase_order in purchase_orders:
                if not belongs_to_distributor(purchase_order, identities, names):
                    continue
                reasons = row_matches(purchase_order, requested_references)
                if reasons:
                    matching_purchase_orders.append({
                        "match_reasons": reasons,
                        "distributor_name": distributor.get("name"),
                        "raw_json": json_safe(purchase_order),
                    })

        return {
            "read_only": True,
            "rk_distributors": reports,
            "matching_purchase_orders": matching_purchase_orders,
        }
    finally:
        client.close()


def main() -> int:
    args = parse_args()
    mongo_url, db_name = configure_environment(args)
    requested_references = {normalized(value) for value in args.row_reference if normalized(value)}
    report = asyncio.run(run_diagnostic(mongo_url, db_name, requested_references))
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    print(rendered)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
