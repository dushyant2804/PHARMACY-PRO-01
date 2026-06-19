#!/usr/bin/env python3
"""Forensic report for Crocin Drops sold_units.

Reads MongoDB only by default. Use --repair-stale with --yes to reset only the
identified Crocin Drops batch when sold_units is not backed by invoices or
invoice stock_deductions.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
from collections import defaultdict

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def _qty(value):
    try:
        return round(float(value or 0), 6)
    except (TypeError, ValueError):
        return 0.0


def _clean(row):
    row = dict(row)
    row.pop("_id", None)
    return row


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-url", default=os.environ.get("MONGO_URL"))
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME"))
    parser.add_argument("--name", default="Crocin Drops")
    parser.add_argument("--sold-units", type=float, default=3)
    parser.add_argument("--repair-stale", action="store_true", help="Reset stale sold_units to 0 for matching Crocin Drops batch only.")
    parser.add_argument("--yes", action="store_true", help="Required with --repair-stale to perform the update.")
    args = parser.parse_args()
    if not args.mongo_url or not args.db_name:
        raise SystemExit("MONGO_URL and DB_NAME are required (env, .env, or flags).")

    client = AsyncIOMotorClient(args.mongo_url)
    db = client[args.db_name]

    med_query = {
        "name": {"$regex": f"^{args.name}$", "$options": "i"},
        "$or": [{"sold_units": args.sold_units}, {"sold_quantity": args.sold_units}],
    }
    medicines = [_clean(row) async for row in db.medicines.find(med_query)]
    med_ids = {m.get("id") for m in medicines if m.get("id")}
    med_keys = {m.get("medicine_key") for m in medicines if m.get("medicine_key")}
    batches = {m.get("batch_no") for m in medicines if m.get("batch_no")}

    invoice_matches = []
    invoice_query = {
        "$or": [
            {"items.name": {"$regex": args.name, "$options": "i"}},
            {"stock_deductions.medicine_name": {"$regex": args.name, "$options": "i"}},
            {"items.medicine_id": {"$in": list(med_ids) or ["__none__"]}},
            {"items.medicine_key": {"$in": list(med_keys) or ["__none__"]}},
            {"stock_deductions.medicine_id": {"$in": list(med_ids) or ["__none__"]}},
            {"stock_deductions.medicine_key": {"$in": list(med_keys) or ["__none__"]}},
            {"items.batch_no": {"$in": list(batches) or ["__none__"]}},
            {"stock_deductions.batch_no": {"$in": list(batches) or ["__none__"]}},
        ]
    }
    async for inv in db.invoices.find(invoice_query):
        inv = _clean(inv)
        for item in inv.get("items", []):
            if (
                str(item.get("name", "")).lower() == args.name.lower()
                or item.get("medicine_id") in med_ids
                or item.get("medicine_key") in med_keys
                or item.get("batch_no") in batches
            ):
                invoice_matches.append({
                    "source": "invoice_item",
                    "invoice_id": inv.get("id"),
                    "bill_no": inv.get("invoice_no") or inv.get("invoice_number"),
                    "date": inv.get("created_at"),
                    "customer": inv.get("customer_name") or inv.get("customer"),
                    "quantity": _qty(item.get("units_dispensed", item.get("quantity_units", item.get("quantity")))),
                    "batch_no": item.get("batch_no"),
                    "medicine_id": item.get("medicine_id"),
                    "medicine_key": item.get("medicine_key"),
                    "medicine_name": item.get("name"),
                })
        for deduction in inv.get("stock_deductions", []):
            if (
                str(deduction.get("medicine_name", "")).lower() == args.name.lower()
                or deduction.get("medicine_id") in med_ids
                or deduction.get("medicine_key") in med_keys
                or deduction.get("batch_no") in batches
            ):
                invoice_matches.append({
                    "source": "stock_deduction",
                    "invoice_id": inv.get("id"),
                    "bill_no": inv.get("invoice_no") or inv.get("invoice_number"),
                    "date": inv.get("created_at"),
                    "customer": inv.get("customer_name") or inv.get("customer"),
                    "quantity": _qty(deduction.get("deduct", deduction.get("quantity"))),
                    "batch_no": deduction.get("batch_no"),
                    "medicine_id": deduction.get("medicine_id"),
                    "medicine_key": deduction.get("medicine_key"),
                    "medicine_name": deduction.get("medicine_name"),
                })

    adjustment_matches = [_clean(row) async for row in db.stock_adjustments.find({
        "$or": [
            {"medicine_id": {"$in": list(med_ids) or ["__none__"]}},
            {"medicine_name": {"$regex": args.name, "$options": "i"}},
            {"batch_no": {"$in": list(batches) or ["__none__"]}},
        ]
    })]

    invoice_backed_by_id = defaultdict(float)
    for row in invoice_matches:
        if row["source"] == "stock_deduction" and row.get("medicine_id"):
            invoice_backed_by_id[row["medicine_id"]] += row["quantity"]

    stale = [m for m in medicines if invoice_backed_by_id.get(m.get("id"), 0) != _qty(m.get("sold_units", m.get("sold_quantity")))]
    report = {
        "medicine_rows": medicines,
        "invoice_and_stock_deduction_rows": invoice_matches,
        "stock_adjustment_rows": adjustment_matches,
        "invoice_backed_sold_by_medicine_id": dict(invoice_backed_by_id),
        "classification": "valid_invoice_backed" if medicines and not stale else "stale_legacy_or_manual_sold_units",
        "safe_repair_plan": "Reset sold_units/sold_quantity to 0 only for rows in stale_medicine_ids, then recalculate available_stock/quantity_units from purchased/adjustments/returns.",
        "stale_medicine_ids": [m.get("id") for m in stale],
    }
    print(json.dumps(report, indent=2, default=str))

    if args.repair_stale:
        if not args.yes:
            raise SystemExit("Refusing repair without --yes.")
        for med in stale:
            purchased = _qty(med.get("purchased_units", med.get("quantity", 0)))
            returned = _qty(med.get("purchase_return_units"))
            adjustment = _qty(med.get("stock_adjustment_units"))
            available = max(0, round(purchased + adjustment - returned, 6))
            await db.medicines.update_one(
                {"id": med["id"], "name": med["name"], "batch_no": med.get("batch_no")},
                {"$set": {"sold_units": 0, "available_stock": available, "quantity_units": available}},
            )


if __name__ == "__main__":
    asyncio.run(main())
