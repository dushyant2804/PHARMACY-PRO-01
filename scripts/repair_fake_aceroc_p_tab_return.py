#!/usr/bin/env python3
"""Safely inspect and remove a confirmed fake ACEROC P TAB purchase return.

Default mode is read-only and prints matching active purchase returns. To repair,
re-run with --confirm-id <return id>. Only that exact active ACEROC P TAB return
is soft-deleted, its stock impact is reversed, and its linked distributor ledger
transaction is removed when present.
"""

import argparse
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone
from pprint import pprint

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

MEDICINE_NAME = "ACEROC P TAB"


def active_aceroc_filter(return_id=None):
    query = {
        "medicine_name": {"$regex": r"^\s*ACEROC\s+P\s+TAB\s*$", "$options": "i"},
        "deleted_at": {"$exists": False},
        "voided_at": {"$exists": False},
        "settlement_status": {"$ne": "deleted"},
    }
    if return_id:
        query["id"] = return_id
    return query


def display_row(row):
    return {
        "return_id": row.get("id"),
        "distributor": row.get("distributor") or row.get("distributor_name"),
        "medicine": row.get("medicine_name"),
        "batch": row.get("batch_number") or row.get("batch_no") or row.get("batch"),
        "quantity": row.get("return_quantity") or row.get("quantity"),
        "purchase_rate": row.get("purchase_rate"),
        "adjust_ledger": bool(row.get("adjust_distributor_ledger") or row.get("ledger_adjusted")),
        "linked_ledger_transaction_id": row.get("ledger_transaction_id"),
        "return_date": row.get("return_date"),
        "status": row.get("settlement_status"),
    }


async def main():
    parser = argparse.ArgumentParser(description="Inspect/repair fake ACEROC P TAB purchase return")
    parser.add_argument("--confirm-id", help="Exact return id to soft-delete after reviewing dry-run output")
    parser.add_argument("--admin", default="admin-repair-script", help="Audit name for deleted_by")
    args = parser.parse_args()

    from server import raw_db, _set_rounded_stock_delta

    matches = await raw_db.purchase_returns.find(active_aceroc_filter(), {"_id": 0}).to_list(1000)
    print(f"Active {MEDICINE_NAME} purchase returns found: {len(matches)}")
    for row in matches:
        pprint(display_row(row))

    if not args.confirm_id:
        print("Dry run only. Re-run with --confirm-id <return id> to repair one confirmed fake return.")
        return

    target = await raw_db.purchase_returns.find_one(active_aceroc_filter(args.confirm_id))
    if not target:
        raise SystemExit(f"No active {MEDICINE_NAME} purchase return found for id {args.confirm_id!r}")

    print("Repair target:")
    pprint(display_row(target))

    quantity = float(target.get("return_quantity") or target.get("quantity") or 0)
    medicine_id = target.get("medicine_id")
    if quantity and medicine_id:
        await _set_rounded_stock_delta(medicine_id, "purchase_return_units", -quantity)

    ledger_id = target.get("ledger_transaction_id")
    if ledger_id and (target.get("adjust_distributor_ledger") or target.get("ledger_adjusted")):
        await raw_db.distributor_transactions.delete_one({"id": ledger_id, "return_id": args.confirm_id})

    now = datetime.now(timezone.utc).isoformat()
    result = await raw_db.purchase_returns.update_one(active_aceroc_filter(args.confirm_id), {"$set": {
        "deleted_at": now,
        "deleted_by": args.admin,
        "settlement_status": "deleted",
        "ledger_adjusted": False,
        "adjust_distributor_ledger": False,
        "ledger_transaction_id": None,
        "settled_return_value": 0.0,
    }})
    print({"soft_deleted_count": result.modified_count, "stock_restored_quantity": quantity, "removed_ledger_transaction_id": ledger_id})


if __name__ == "__main__":
    asyncio.run(main())
