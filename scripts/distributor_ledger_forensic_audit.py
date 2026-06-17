#!/usr/bin/env python3
"""Run the distributor ledger forensic audit against a MongoDB database.

This script is intentionally deploy-free: run it from the repository root with
MONGO_URL and DB_NAME set (or pass --mongo-url/--db-name) and it will use the
same backend ledger construction/dedupe code as the API endpoint.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

TARGET_INVOICES = {
    "ABHI ENTERPRISES": ["(Q) 4557", "4557"],
    "ARORA MEDICOSE": ["(C) 3128", "3128", "(C) 3216", "3216"],
    "BALAJI PHARMA": ["(G) 3194", "3194"],
    "KAPIL MEDICOSE": ["A001784"],
    "KISSAN MEDICAL AGENCY": ["B002441"],
    "MIDHA DISTRIBUTORS": [],
    "R K PHARMA": [],
    "VISHAL SURGICAL": ["VS26-27/652", "VS26-27/1834"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the distributor ledger forensic audit without deploying the backend.",
    )
    parser.add_argument("--mongo-url", help="MongoDB connection string. Defaults to MONGO_URL or .env.")
    parser.add_argument("--db-name", help="Database name. Defaults to DB_NAME or .env.")
    parser.add_argument(
        "--json-out",
        default="distributor-ledger-forensic-audit.json",
        help="Path for the full JSON audit output.",
    )
    parser.add_argument(
        "--summary-out",
        default="distributor-ledger-forensic-summary.md",
        help="Path for the human-readable Markdown summary.",
    )
    return parser.parse_args()


def configure_environment(args: argparse.Namespace) -> None:
    load_dotenv(REPO_ROOT / ".env")
    if args.mongo_url:
        os.environ["MONGO_URL"] = args.mongo_url
    if args.db_name:
        os.environ["DB_NAME"] = args.db_name

    missing = [name for name in ("MONGO_URL", "DB_NAME") if not os.environ.get(name)]
    if missing:
        names = ", ".join(missing)
        raise SystemExit(
            f"Missing {names}. Set them in .env, export them, or pass --mongo-url/--db-name."
        )


def row_label(row: dict[str, Any]) -> str:
    return (
        f"source={row.get('source')} transaction_id={row.get('transaction_id')} "
        f"purchase_order_id={row.get('purchase_order_id')} amount={row.get('amount')} "
        f"date={row.get('date')} invoice={row.get('invoice_ref')}"
    )


def matching_focus_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    distributor_name = str(report.get("distributor", {}).get("name") or "")
    needles = [needle.casefold() for needle in TARGET_INVOICES.get(distributor_name, [])]
    if not needles:
        return []
    rows = []
    for row in report.get("before_dedupe", []):
        haystack = " ".join(
            str(value or "")
            for value in [
                row.get("invoice_ref"),
                *(row.get("invoice_identity_variants") or []),
                *(row.get("dedupe_key") or []),
            ]
        ).casefold()
        if any(needle in haystack for needle in needles):
            rows.append(row)
    return rows


def render_summary(audit: dict[str, Any]) -> str:
    lines = ["# Distributor Ledger Forensic Audit", ""]
    lines.append("This report is generated from the live database using backend ledger code, before any frontend caching or rendering.")
    lines.append("")

    for report in audit.get("distributors", []):
        distributor = report.get("distributor", {})
        lines.append(f"## {distributor.get('name')} ({distributor.get('id')})")
        counts = report.get("counts", {})
        lines.append(
            f"- Counts: before={counts.get('before')} after={counts.get('after')} "
            f"removed={counts.get('removed')} surviving_duplicate_groups={counts.get('surviving_duplicate_groups')}"
        )

        focus_rows = matching_focus_rows(report)
        if focus_rows:
            lines.append("- Focus invoice rows before display dedupe:")
            for row in focus_rows:
                lines.append(f"  - {row_label(row)}")

        duplicates = report.get("surviving_duplicate_pairs", [])
        if duplicates:
            lines.append("- Surviving duplicate groups:")
            for group in duplicates:
                lines.append(f"  - Why survived: {group.get('explanation')}")
                rows = group.get("rows", [])
                for index, row in enumerate(rows, start=1):
                    label = chr(ord("A") + index - 1) if index <= 26 else str(index)
                    lines.append(f"    - Row {label}: {row_label(row)}")
        else:
            lines.append("- Surviving duplicate groups: none detected by forensic fingerprint.")

        removed = report.get("removed_rows", [])
        if removed:
            lines.append("- Removed rows:")
            for row in removed:
                analysis = row.get("removal_analysis", {})
                lines.append(f"  - {row_label(row)}")
                lines.append(f"    - Rule: {analysis.get('rule')}")
                lines.append(f"    - Why considered duplicate: {analysis.get('duplicate_reason')}")
                lines.append(f"    - Correctness: {analysis.get('correctness')}")
        else:
            lines.append("- Removed rows: none.")
        lines.append("")
    return "\n".join(lines)


async def run_audit() -> dict[str, Any]:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    import server  # pylint: disable=import-outside-toplevel

    distributors = await server.db.distributors.find({}, {"_id": 0}).to_list(10000)
    wanted = {name.casefold() for name in server.FORENSIC_DISTRIBUTOR_NAMES}
    reports = []
    for dist in distributors:
        name = str(dist.get("name") or dist.get("distributor_name") or "").strip().casefold()
        if name in wanted:
            reports.append(await server._distributor_ledger_forensic_audit_for_dist(dist, dist.get("id")))
    return {"distributors": reports}


def main() -> int:
    args = parse_args()
    configure_environment(args)
    audit = asyncio.run(run_audit())

    json_path = Path(args.json_out)
    summary_path = Path(args.summary_out)
    json_path.write_text(json.dumps(audit, indent=2, default=str), encoding="utf-8")
    summary_path.write_text(render_summary(audit), encoding="utf-8")

    print(f"Wrote full JSON audit to {json_path}")
    print(f"Wrote Markdown summary to {summary_path}")
    print(f"Audited distributors: {len(audit.get('distributors', []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
