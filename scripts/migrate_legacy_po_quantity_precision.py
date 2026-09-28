"""One-time repair for legacy PO quantity rounding.

Historical PharmacyOS builds rounded purchase/free quantities to one decimal.
Per the confirmed historical data rule:
    2.8 + 0.2 -> 2.75 + 0.25
    3.8 + 0.2 -> 3.75 + 0.25
    4.8 + 0.2 -> 4.75 + 0.25
and so on.

Quantities ending in .5 + .5 and whole-number quantities are left untouched.

This script is intentionally manual and LOCAL-ONLY. It never runs at startup.
It backs up every affected PO before changing it, updates PO totals, and then
rebuilds inventory from the corrected purchase orders.
"""

from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from types import SimpleNamespace


ROOT_DIR = Path(__file__).resolve().parents[1]
os.environ["PHARMACYOS_MODE"] = "LOCAL_MODE"
os.environ.setdefault(
    "LOCAL_DB_PATH",
    str(ROOT_DIR / "local_data" / "pharmacyos.sqlite3"),
)

from server import (  # noqa: E402
    POCreate,
    _calculate_purchase_order_totals,
    _invalidate_inventory_dashboard_cache,
    _rebuild_inventory_for_po_medicines,
    db,
)


TWO_PLACES = Decimal("0.01")
TOLERANCE = Decimal("0.001")


def dec(value) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def is_close(a: Decimal, b: Decimal) -> bool:
    return abs(a - b) <= TOLERANCE


def repair_pair(quantity, free_quantity):
    """Return corrected (paid, free), or None when the item is not affected."""
    qty = dec(quantity)
    free = dec(free_quantity)

    # We only repair the exact legacy pattern x.8 + 0.2.
    if qty < Decimal("1"):
        return None
    whole = qty.to_integral_value(rounding=ROUND_FLOOR)
    expected_legacy_qty = whole + Decimal("0.8")
    if not is_close(qty, expected_legacy_qty) or not is_close(free, Decimal("0.2")):
        return None

    corrected_paid = (whole + Decimal("0.75")).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    corrected_free = Decimal("0.25")
    return float(corrected_paid), float(corrected_free)


async def main():
    purchase_orders = await db.purchase_orders.find({}, {"_id": 0}).to_list(None)

    affected = []
    affected_names = set()

    for po in purchase_orders:
        if po.get("deleted_at") or po.get("voided_at") or po.get("status") == "deleted":
            continue

        updated = deepcopy(po)
        changed = False
        changed_items = []

        for index, item in enumerate(updated.get("items", [])):
            repaired = repair_pair(
                item.get("quantity", 0),
                item.get("free_quantity", 0),
            )
            if repaired is None:
                continue

            old_quantity = item.get("quantity")
            old_free = item.get("free_quantity", 0)
            new_quantity, new_free = repaired

            item["quantity"] = new_quantity
            item["free_quantity"] = new_free

            purchase_price = dec(item.get("purchase_price"))
            item["item_total"] = float(
                (purchase_price * Decimal(str(new_quantity))).quantize(
                    TWO_PLACES, rounding=ROUND_HALF_UP
                )
            )

            changed = True
            affected_names.add(str(item.get("name") or "").strip())
            changed_items.append(
                {
                    "index": index,
                    "name": item.get("name"),
                    "batch_no": item.get("batch_no"),
                    "old_quantity": old_quantity,
                    "old_free_quantity": old_free,
                    "new_quantity": new_quantity,
                    "new_free_quantity": new_free,
                }
            )

        if not changed:
            continue

        # Recalculate PO financial totals without rebuilding the item dicts.
        # Existing PO items may contain legacy fields such as medicine_id and
        # medicine_key that must remain untouched by this migration.
        total_items = [
            SimpleNamespace(
                purchase_price=item.get("purchase_price", 0),
                quantity=item.get("quantity", 0),
                gst_rate=item.get("gst_rate", 5),
            )
            for item in updated.get("items", [])
        ]
        totals = _calculate_purchase_order_totals(
            SimpleNamespace(
                items=total_items,
                scheme_discount=updated.get("scheme_discount", 0),
                cash_discount=updated.get("cash_discount", 0),
            )
        )

        return_credit = dec(
            po.get(
                "purchase_return_adjustment",
                po.get("purchase_return_credit", 0),
            )
        ).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)

        payable = max(
            Decimal("0"),
            dec(totals["grand_total"]) - return_credit,
        ).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)

        updated.update(
            {
                "items": updated["items"],
                "sub_total": totals["sub_total"],
                "scheme_discount": totals["scheme_discount"],
                "cash_discount": totals["cash_discount"],
                "discount": totals["discount"],
                "taxable_total": totals["taxable_total"],
                "total_cgst": totals["total_cgst"],
                "total_sgst": totals["total_sgst"],
                "round_off": totals["round_off"],
                "grand_total": totals["grand_total"],
                "total": totals["total"],
                "gst_breakup": totals["gst_breakup"],
                "subtotal_after_purchase_return": float(payable),
                "final_payable_total": float(payable),
            }
        )

        affected.append(
            {
                "po_id": po.get("id"),
                "po_no": po.get("po_no"),
                "distributor_name": po.get("distributor_name"),
                "changes": changed_items,
                "old_grand_total": po.get("grand_total"),
                "new_grand_total": updated.get("grand_total"),
                "backup": po,
            }
        )

    print(f"Found {len(affected)} affected purchase orders.")

    if not affected:
        print("Nothing to migrate. Existing data is already repaired.")
        return

    backup_dir = ROOT_DIR / "backups" / "migrations"
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"legacy_po_quantity_precision_{timestamp}.json"

    backup_payload = {
        "migration": "legacy_po_quantity_precision",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "rule": "x.8 + 0.2 -> x.75 + 0.25",
        "purchase_orders": [row["backup"] for row in affected],
    }
    backup_path.write_text(
        json.dumps(backup_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Backup written: {backup_path}")

    # Write the corrected documents only after the backup has been confirmed.
    for row in affected:
        po = row["backup"]
        # Rebuild the corrected PO again from the recorded change list so the
        # write is based only on the backed-up document and migration rule.
        corrected = deepcopy(po)
        for change in row["changes"]:
            item = corrected["items"][change["index"]]
            item["quantity"] = change["new_quantity"]
            item["free_quantity"] = change["new_free_quantity"]
            item["item_total"] = float(
                (
                    dec(item.get("purchase_price"))
                    * dec(change["new_quantity"])
                ).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            )

        total_items = [
            SimpleNamespace(
                purchase_price=item.get("purchase_price", 0),
                quantity=item.get("quantity", 0),
                gst_rate=item.get("gst_rate", 5),
            )
            for item in corrected.get("items", [])
        ]
        totals = _calculate_purchase_order_totals(
            SimpleNamespace(
                items=total_items,
                scheme_discount=corrected.get("scheme_discount", 0),
                cash_discount=corrected.get("cash_discount", 0),
            )
        )
        return_credit = dec(
            po.get(
                "purchase_return_adjustment",
                po.get("purchase_return_credit", 0),
            )
        ).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
        payable = max(
            Decimal("0"),
            dec(totals["grand_total"]) - return_credit,
        ).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)

        await db.purchase_orders.update_one(
            {"id": row["po_id"]},
            {"$set": {
                "items": payload.model_dump()["items"],
                "sub_total": totals["sub_total"],
                "scheme_discount": totals["scheme_discount"],
                "cash_discount": totals["cash_discount"],
                "discount": totals["discount"],
                "taxable_total": totals["taxable_total"],
                "total_cgst": totals["total_cgst"],
                "total_sgst": totals["total_sgst"],
                "round_off": totals["round_off"],
                "grand_total": totals["grand_total"],
                "total": totals["total"],
                "gst_breakup": totals["gst_breakup"],
                "subtotal_after_purchase_return": float(payable),
                "final_payable_total": float(payable),
            }},
        )

    rebuilt = await _rebuild_inventory_for_po_medicines(affected_names)
    _invalidate_inventory_dashboard_cache(None)

    print(f"Updated {len(affected)} purchase orders.")
    print(f"Rebuilt {rebuilt.get('medicine_batches_updated', 0)} inventory batches.")
    print("Affected POs:")
    for row in affected:
        print(
            f"  {row['po_no'] or row['po_id']}: "
            f"{len(row['changes'])} item(s), "
            f"grand total {row['old_grand_total']} -> {row['new_grand_total']}"
        )
    print("Migration complete.")


if __name__ == "__main__":
    asyncio.run(main())
