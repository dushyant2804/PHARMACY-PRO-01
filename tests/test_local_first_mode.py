import os
import subprocess
import sys
from pathlib import Path


def test_local_mode_sqlite_business_flows_and_backup_safety(tmp_path):
    db_path = tmp_path / "pharmacyos.sqlite3"
    backup_dir = tmp_path / "backups"
    upload_dir = tmp_path / "uploads"
    script = r'''
import asyncio
from pathlib import Path

import server

USER = {"id": "u1", "name": "Local Admin", "email": "admin@example.com", "role": "admin", "tenant_id": "real_shop"}

async def main():
    await server.startup()
    server._request_active.set(False)

    logo_dir = server.UPLOAD_DIR / "branding"
    logo_dir.mkdir(parents=True, exist_ok=True)
    (logo_dir / "logo.txt").write_text("local-logo", encoding="utf-8")

    dist = server.Distributor(id="dist-local", name="Local Distributor")
    cust = server.Customer(id="cust-local", name="Local Customer")
    await server.create_distributor(dist, USER)
    await server.create_customer(cust, USER)

    po = await server.create_po(server.POCreate(
        distributor_id="dist-local",
        distributor_name="Local Distributor",
        invoice_ref="SUP-001",
        po_date="2026-06-21",
        items=[server.POItem(name="LocalMed", batch_no="B1", quantity=20, free_quantity=0, purchase_price=5, mrp=10, gst_rate=12, expiry_date="12/30")],
    ), USER)
    assert po["po_no"].startswith("PO-")

    invoice = await server.create_invoice(server.InvoiceCreate(
        customer_id="cust-local",
        customer_name="Local Customer",
        items=[server.InvoiceItem(medicine_id="", name="LocalMed", batch_no="B1", expiry_date="12/30", quantity=2, unit_type="unit", mrp=10, gst_rate=12)],
        payment_mode="credit",
    ), USER)
    assert invoice["invoice_no"].startswith("INV-")

    adjustment = await server.create_stock_adjustment(server.StockAdjustmentCreate(
        adjustment_date="2026-06-21",
        medicine_id="localmed::B1",
        adjustment_type="correction",
        quantity=1,
        notes="local sqlite adjustment",
    ), USER)
    assert adjustment["resulting_stock"] >= 1

    purchase_return = await server.create_purchase_return(server.PurchaseReturnCreate(
        return_date="2026-06-21",
        distributor="Local Distributor",
        distributor_id="dist-local",
        medicine_name="LocalMed",
        medicine_key="localmed::B1",
        batch_number="B1",
        expiry_date="12/30",
        return_quantity=1,
        purchase_rate=5,
        reason="Damaged",
        adjust_distributor_ledger=True,
    ), USER)
    assert purchase_return["ledger_adjusted"] is True

    customer_txn = await server.add_cust_payment("cust-local", server.PaymentCreate(amount=5, mode="cash", reference_number="RCPT-1"), USER)
    distributor_txn = await server.add_dist_payment("dist-local", server.PaymentCreate(amount=3, mode="upi", reference_number="D-PAY-1"), USER)
    assert customer_txn["type"] == "payment"
    assert distributor_txn["type"] == "payment"

    closing = await server.create_daily_closing(server.DailyClosingCreate(closing_date="2026-06-21", counted_cash=100, opening_cash=50), USER)
    assert closing["closing_date"] == "2026-06-21"

    sales = await server.sales_report(user=USER)
    stock = await server.stock_valuation(user=USER)
    outstanding = await server.outstanding_report(user=USER)
    analytics = await server.report_analytics(user=USER)
    assert isinstance(sales, dict)
    assert isinstance(stock, dict)
    assert isinstance(outstanding, dict)
    assert "monthly_sales_trend" in analytics

    backup = await server._create_local_backup("coverage")
    assert backup["size"] > 0
    assert backup["sha256"]
    assert backup["upload_file_count"] >= 1
    restore_dry_run = await server.backup_restore({"backup_file": backup["backup_file"], "expected_sha256": backup["sha256"]}, dry_run=True, confirm=False, user=USER)
    assert restore_dry_run["dry_run"] is True
    sync_dry_run = await server.backup_sync_retry(dry_run=True, confirm=False, user=USER)
    assert sync_dry_run["dry_run"] is True

    await server.shutdown()

asyncio.run(main())
'''
    env = os.environ.copy()
    env.update({
        "PHARMACYOS_MODE": "LOCAL_MODE",
        "DB_NAME": "unused_local_test",
        "LOCAL_DB_PATH": str(db_path),
        "BACKUP_DIR": str(backup_dir),
        "UPLOAD_DIR": str(upload_dir),
        "JWT_SECRET": "test-secret",
    })
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_local_mode_database_open_failure_is_explicit(tmp_path):
    bad_db_path = tmp_path / "directory-not-file"
    bad_db_path.mkdir()
    env = os.environ.copy()
    env.update({
        "PHARMACYOS_MODE": "LOCAL_MODE",
        "LOCAL_DB_PATH": str(bad_db_path),
        "DB_NAME": "unused_local_test",
        "JWT_SECRET": "test-secret",
    })
    result = subprocess.run(
        [sys.executable, "-c", "import server"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "LOCAL_MODE database could not be opened" in result.stderr



def test_sqlite_adapter_query_and_update_patterns(tmp_path):
    import asyncio

    from local_database import LocalSQLiteDatabase

    async def exercise():
        db = LocalSQLiteDatabase(tmp_path / "adapter.sqlite3")
        await db.items.insert_many([
            {"id": "a", "name": "Alpha", "qty": 2, "tags": ["old"]},
            {"id": "b", "name": "Beta", "qty": 5},
            {"id": "c", "name": "Gamma", "qty": 8},
        ])
        assert await db.items.count_documents({"$and": [{"qty": {"$gte": 2}}, {"name": {"$regex": "a", "$options": "i"}}]}) == 3
        assert await db.items.count_documents({"$nor": [{"id": "a"}]}) == 2
        assert await db.items.count_documents({"$expr": {"$gte": ["$qty", 5]}}) == 2
        page = await db.items.find({}, {"_id": 0}).sort("qty", -1).skip(1).limit(1).to_list(1)
        assert page[0]["id"] == "b"
        await db.items.update_one({"id": "a"}, {"$inc": {"qty": 3}, "$push": {"tags": "new"}})
        updated = await db.items.find_one({"id": "a"})
        assert updated["qty"] == 5
        assert updated["tags"] == ["old", "new"]
        counter = await db.counters.find_one_and_update(
            {"_id": "counter"},
            [{"$set": {"seq": {"$add": [{"$max": [{"$ifNull": ["$seq", 0]}, 10]}, 1]}}}],
            upsert=True,
        )
        assert counter["seq"] == 11

    asyncio.run(exercise())
