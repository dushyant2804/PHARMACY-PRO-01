import os
import sqlite3
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
        "SOURCE_DB_PATH": str(tmp_path / "source.sqlite3"),
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
    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "users" in tables
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 1


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



def test_sqlite_adapter_skips_json_indexes_when_json1_unavailable(tmp_path, monkeypatch, caplog):
    import logging
    import sqlite3

    import local_database
    from local_database import LocalSQLiteDatabase

    real_connect = sqlite3.connect

    class NoJsonConnection:
        def __init__(self, wrapped):
            self._wrapped = wrapped

        def execute(self, sql, *args, **kwargs):
            if "json_extract" in sql.lower():
                raise sqlite3.OperationalError("no such function: json_extract")
            return self._wrapped.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    def connect_without_json1(*args, **kwargs):
        return NoJsonConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(local_database.sqlite3, "connect", connect_without_json1)

    with caplog.at_level(logging.WARNING, logger="pharmacy"):
        db = LocalSQLiteDatabase(tmp_path / "no-json1.sqlite3")

    awaitable = db.items.insert_one({"id": "item-1", "name": "Legacy SQLite"})
    import asyncio
    asyncio.run(awaitable)
    stored = asyncio.run(db.items.find_one({"id": "item-1"}))

    assert stored["name"] == "Legacy SQLite"
    assert "SQLite JSON1 unavailable, skipping JSON indexes" in caplog.text


def test_sqlite_adapter_creates_json_indexes_when_json1_available(tmp_path):
    from local_database import LocalSQLiteDatabase

    db = LocalSQLiteDatabase(tmp_path / "json1.sqlite3")
    _ = db.users
    indexes = db.conn.execute("SELECT name, sql FROM sqlite_master WHERE type='index'").fetchall()

    assert any(name == "idx_documents_collection_updated" for name, _ in indexes)
    assert any(name == "users" for name, _ in db.conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'").fetchall())
    assert any("json_extract" in (sql or "") for _, sql in indexes)


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


def test_local_mode_first_login_cloud_import_preserves_auth_and_requires_overwrite(tmp_path):
    db_path = tmp_path / "local.sqlite3"
    backup_dir = tmp_path / "backups"
    script = r'''
import asyncio
import os
from fastapi import HTTPException
from starlette.responses import Response

import server
from local_database import LocalSQLiteDatabase

PASSWORD = "StrongPass123"

class FakeAdmin:
    async def command(self, name):
        return {"ok": 1}

class FakeClient:
    def __init__(self, *args, **kwargs):
        self.admin = FakeAdmin()
        self.source = LocalSQLiteDatabase(os.environ["SOURCE_DB_PATH"])
    def __getitem__(self, name):
        return self.source
    def close(self):
        pass

async def main():
    fake_client = FakeClient()
    password_hash = server.hash_password(PASSWORD)
    await fake_client.source.users.insert_one({
        "id": "cloud-admin", "email": "owner@example.com", "name": "Cloud Admin",
        "role": "admin", "tenant_id": "real_shop", "shop_id": "real_shop",
        "password_hash": password_hash, "active": True, "is_demo": False,
    })
    await fake_client.source.roles.insert_one({"id": "role-admin", "name": "admin"})
    await fake_client.source.settings.insert_one({"id": "settings", "key": "main", "tenant_id": "real_shop"})
    await fake_client.source.medicines.insert_one({"id": "med-1", "name": "CloudMed", "tenant_id": "real_shop"})
    await fake_client.source.customers.insert_one({"id": "cust-1", "name": "Cloud Customer", "tenant_id": "real_shop"})
    await fake_client.source.distributors.insert_one({"id": "dist-1", "name": "Cloud Distributor", "tenant_id": "real_shop"})
    await fake_client.source.invoices.insert_one({"id": "inv-1", "tenant_id": "real_shop"})
    await fake_client.source.purchase_orders.insert_one({"id": "po-1", "tenant_id": "real_shop"})
    await fake_client.source.customer_transactions.insert_one({"id": "ctxn-1", "tenant_id": "real_shop"})
    await fake_client.source.distributor_transactions.insert_one({"id": "dtxn-1", "tenant_id": "real_shop"})
    await fake_client.source.purchase_returns.insert_one({"id": "ret-1", "tenant_id": "real_shop"})
    await fake_client.source.stock_adjustments.insert_one({"id": "stock-1", "tenant_id": "real_shop"})
    await fake_client.source.daily_sales.insert_one({"id": "sale-1", "tenant_id": "real_shop"})
    await fake_client.source.daily_summary.insert_one({"id": "report-1", "tenant_id": "real_shop"})

    server.AsyncIOMotorClient = lambda *args, **kwargs: fake_client
    dry = await server.local_mode_import_dry_run()
    assert dry["dry_run"] is True
    assert dry["local_database_path"] == str(server.LOCAL_DB_PATH.resolve())
    assert dry["cloud_records_found"]["users"] == 1
    assert dry["local_records_before_import"]["users"] == 0
    assert dry["local_records_after_import"] is None
    assert server.raw_db.collection_table_count("users") == 0

    imported = await server.local_mode_import_confirm(overwrite_local=False)
    assert imported["dry_run"] is False
    assert imported["imported"]["users"] == 1
    assert imported["local_records_after_import"]["users"] == 1
    assert server.raw_db.collection_table_count("users") == imported["imported"]["users"]

    stored = await server.raw_db.users.find_one({"email": "owner@example.com"})
    assert stored["password_hash"] == password_hash
    login = await server.login(server.UserLogin(email="owner@example.com", password=PASSWORD), Response())
    assert login["id"] == "cloud-admin"

    try:
        await server.local_mode_import_confirm(overwrite_local=False)
    except HTTPException as exc:
        assert exc.status_code == 409
    else:
        raise AssertionError("expected overwrite confirmation guard")

    overwritten = await server.local_mode_import_confirm(overwrite_local=True)
    assert overwritten["local_records_before_import"]["users"] == 1
    assert overwritten["local_records_after_import"]["users"] == 1

asyncio.run(main())
'''
    env = os.environ.copy()
    env.update({
        "PHARMACYOS_MODE": "LOCAL_MODE",
        "MONGO_URL": "mongodb://atlas-source.example/pharmacy",
        "DB_NAME": "cloud_source",
        "LOCAL_DB_PATH": str(db_path),
        "SOURCE_DB_PATH": str(tmp_path / "source.sqlite3"),
        "BACKUP_DIR": str(backup_dir),
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

def test_local_mode_auth_reads_imported_users_json_table(tmp_path):
    db_path = tmp_path / "imported.sqlite3"
    backup_dir = tmp_path / "backups"
    script = r'''
import asyncio, json, os, sqlite3
from types import SimpleNamespace
from fastapi import HTTPException
from starlette.responses import Response

import server

PASSWORD = "StrongPass123"

async def main():
    user = {
        "_id": "mongo-admin-id", "email": "dushyantbhadu07@gmail.com", "name": "SHREE SHYAM PHARMACY",
        "role": "admin", "shop_id": "real_shop", "tenant_id": "real_shop", "is_demo": False,
        "active": True, "password_hash": server.hash_password(PASSWORD),
    }
    with sqlite3.connect(os.environ["LOCAL_DB_PATH"]) as conn:
        conn.execute("DROP TABLE IF EXISTS users")
        conn.execute("CREATE TABLE users (_doc_id TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL)")
        conn.execute("INSERT INTO users(_doc_id, data, updated_at) VALUES (?, ?, ?)", (user["_id"], json.dumps(user), "2026-06-24T00:00:00+00:00"))
        conn.commit()

    response = Response()
    login = await server.login(server.UserLogin(email=user["email"], password=PASSWORD), response)
    assert login["id"] == user["_id"]
    assert login["email"] == user["email"]
    assert login["name"] == user["name"]
    assert login["role"] == "admin"
    assert login["shop_id"] == "real_shop"
    assert login["is_demo"] is False
    assert login["token"]

    try:
        await server.login(server.UserLogin(email=user["email"], password="WrongPass123"), Response())
    except HTTPException as exc:
        assert exc.status_code == 401
    else:
        raise AssertionError("invalid password should fail")

    request = SimpleNamespace(cookies={}, headers={"Authorization": "Bearer " + login["token"]}, method="GET", url=SimpleNamespace(path="/api/auth/me"))
    me = await server.get_current_user(request)
    assert me["id"] == user["_id"]
    assert me["email"] == user["email"]
    assert me["name"] == user["name"]
    assert me["role"] == "admin"
    assert me["shop_id"] == "real_shop"
    assert me["is_demo"] is False

    demo = await server.demo_login(Response())
    assert demo["id"] == server.DEMO_USER_ID
    assert demo["is_demo"] is True
    assert demo["shop_id"] == server.DEMO_TENANT_ID

asyncio.run(main())
'''
    env = os.environ.copy()
    env.update({
        "PHARMACYOS_MODE": "LOCAL_MODE",
        "DB_NAME": "unused_local_test",
        "LOCAL_DB_PATH": str(db_path),
        "BACKUP_DIR": str(backup_dir),
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
