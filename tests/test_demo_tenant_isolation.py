import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

import jwt
from fastapi import HTTPException, Response
from starlette.requests import Request
from starlette.responses import JSONResponse

from server import (
    BUSINESS_COLLECTIONS,
    DEMO_TENANT_ID,
    DEMO_USER_ID,
    JWT_ALGORITHM,
    JWT_SECRET,
    SAFE_DEMO_EMAIL,
    TenantAwareCollection,
    UserLogin,
    _canonicalize_user_tenant,
    _current_demo,
    _current_tenant,
    _request_active,
    _seed_demo_data,
    demo_login,
    list_users,
    login,
    tenant_security_context,
)


class RecordingCursor:
    def __init__(self, documents=None):
        self.documents = documents or []

    async def to_list(self, _limit):
        return self.documents


class RecordingCollection:
    def __init__(self, find_one_results=None, find_documents=None):
        self.delete_queries = []
        self.find_queries = []
        self.replacements = []
        self.find_one_results = list(find_one_results or [])
        self.find_documents = find_documents or []

    async def delete_many(self, query):
        self.delete_queries.append(query)
        return SimpleNamespace(deleted_count=0)

    async def replace_one(self, query, replacement, **kwargs):
        self.replacements.append((query, replacement, kwargs))
        return SimpleNamespace(matched_count=1)

    async def find_one(self, query, *args, **kwargs):
        self.find_queries.append(query)
        return self.find_one_results.pop(0) if self.find_one_results else None

    def find(self, query, *args, **kwargs):
        self.find_queries.append(query)
        return RecordingCursor(self.find_documents)


class DemoTenantIsolationTest(unittest.IsolatedAsyncioTestCase):
    def test_demo_identity_is_forced_to_canonical_shop(self):
        user = _canonicalize_user_tenant({
            "id": DEMO_USER_ID,
            "tenant_id": "real_shop",
            "shop_id": "real_shop",
            "is_demo": False,
            "active": False,
        })

        self.assertEqual(user["tenant_id"], DEMO_TENANT_ID)
        self.assertEqual(user["shop_id"], DEMO_TENANT_ID)
        self.assertTrue(user["is_demo"])
        self.assertTrue(user["active"])

    async def test_normal_login_explicitly_excludes_demo_id_tenant_and_flag(self):
        users = RecordingCollection()
        with patch("server.raw_db", SimpleNamespace(users=users)):
            with self.assertRaises(HTTPException) as raised:
                await login(UserLogin(identifier="demo@pharmacyos.local", password="StrongPass123"), Response())

        self.assertEqual(raised.exception.status_code, 401)
        query = users.find_queries[0]["$and"]
        self.assertIn({"id": {"$ne": DEMO_USER_ID}}, query)
        self.assertIn({"tenant_id": {"$ne": DEMO_TENANT_ID}}, query)
        self.assertIn({"is_demo": {"$ne": True}}, query)

    async def test_demo_login_returns_only_canonical_demo_identity_and_token(self):
        demo_user = {
            "id": DEMO_USER_ID, "email": SAFE_DEMO_EMAIL, "name": "Demo Pharmacist", "role": "admin",
            "tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID, "is_demo": True, "active": True,
        }
        users = RecordingCollection(find_one_results=[demo_user])
        with patch("server._seed_demo_data", new=AsyncMock()) as seed, patch("server.raw_db", SimpleNamespace(users=users)):
            result = await demo_login(Response())

        seed.assert_awaited_once()
        self.assertEqual(users.find_queries[0], {"id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "is_demo": True, "active": True})
        self.assertEqual(result["tenant_id"], DEMO_TENANT_ID)
        self.assertTrue(result["is_demo"])
        claims = jwt.decode(result["token"], JWT_SECRET, algorithms=[JWT_ALGORITHM])
        self.assertEqual(claims["tenant_id"], DEMO_TENANT_ID)
        self.assertTrue(claims["is_demo"])

    async def test_reseed_never_deletes_real_user_by_email_and_uses_safe_email_on_admin_conflict(self):
        collections = {name: RecordingCollection() for name in BUSINESS_COLLECTIONS}
        users = RecordingCollection()

        class RecordingDatabase:
            def __init__(self):
                self.users = users

            def __getitem__(self, name):
                return collections[name]

        with patch.dict(os.environ, {"ADMIN_EMAIL": "admin@gmail.com", "DEMO_EMAIL": "admin@gmail.com"}), patch("server.raw_db", RecordingDatabase()):
            await _seed_demo_data("2026-06-10T00:00:00+00:00")

        self.assertEqual(users.replacements[0][1]["email"], SAFE_DEMO_EMAIL)
        self.assertEqual(users.replacements[0][1]["tenant_id"], DEMO_TENANT_ID)
        self.assertEqual(users.replacements[0][1]["shop_id"], DEMO_TENANT_ID)
        self.assertTrue(users.replacements[0][1]["is_demo"])
        self.assertTrue(users.replacements[0][1]["active"])
        for query in users.delete_queries:
            self.assertNotIn("email", query)
            if query.get("tenant_id") == {"$ne": DEMO_TENANT_ID}:
                self.assertEqual(query["$or"], [{"is_demo": True}, {"id": DEMO_USER_ID}])
        for collection in collections.values():
            self.assertEqual(collection.delete_queries, [])
            for _, replacement, _ in collection.replacements:
                self.assertEqual(replacement["tenant_id"], DEMO_TENANT_ID)
                self.assertEqual(replacement["shop_id"], DEMO_TENANT_ID)

    async def test_demo_token_middleware_forces_demo_tenant_context(self):
        demo_user = {
            "id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "shop_id": DEMO_TENANT_ID,
            "is_demo": True, "active": True,
        }
        users = RecordingCollection(find_one_results=[demo_user])
        token = jwt.encode({"sub": DEMO_USER_ID, "tenant_id": "real_shop", "is_demo": True}, JWT_SECRET, algorithm=JWT_ALGORITHM)
        request = Request({
            "type": "http", "method": "GET", "path": "/api/medicines", "headers": [(b"authorization", f"Bearer {token}".encode())],
            "query_string": b"", "server": ("test", 80), "client": ("test", 123), "scheme": "http",
        })

        async def call_next(_request):
            self.assertEqual(_current_tenant.get(), DEMO_TENANT_ID)
            self.assertTrue(_current_demo.get())
            return JSONResponse({"ok": True})

        with patch("server.raw_db", SimpleNamespace(users=users)):
            response = await tenant_security_context(request, call_next)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(users.find_queries[0], {"id": DEMO_USER_ID, "tenant_id": DEMO_TENANT_ID, "is_demo": True})

    async def test_demo_business_reads_are_scoped_and_writes_are_blocked(self):
        active = _request_active.set(True)
        tenant = _current_tenant.set(DEMO_TENANT_ID)
        demo = _current_demo.set(True)
        try:
            for name in ("medicines", "customers", "invoices"):
                collection = Mock()
                scoped = TenantAwareCollection(collection, name)
                scoped.find({})
                self.assertEqual(collection.find.call_args.args[0], {"tenant_id": DEMO_TENANT_ID})
            with self.assertRaises(HTTPException) as post_error:
                await TenantAwareCollection(AsyncMock(), "medicines").insert_one({"id": "new"})
            with self.assertRaises(HTTPException) as put_error:
                await TenantAwareCollection(AsyncMock(), "medicines").update_one({"id": "x"}, {"$set": {"name": "x"}})
            with self.assertRaises(HTTPException) as delete_error:
                await TenantAwareCollection(AsyncMock(), "medicines").delete_one({"id": "x"})
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)

        self.assertEqual({post_error.exception.status_code, put_error.exception.status_code, delete_error.exception.status_code}, {403})

    async def test_user_management_separates_real_and_demo_users(self):
        users = RecordingCollection(find_documents=[])
        with patch("server.raw_db", SimpleNamespace(users=users)):
            await list_users({"id": "real-admin", "role": "admin", "tenant_id": "real_shop", "is_demo": False})
            await list_users({"id": DEMO_USER_ID, "role": "admin", "tenant_id": DEMO_TENANT_ID, "is_demo": True})

        self.assertEqual(users.find_queries[0]["tenant_id"], "real_shop")
        self.assertEqual(users.find_queries[0]["id"], {"$ne": DEMO_USER_ID})
        self.assertEqual(users.find_queries[0]["is_demo"], {"$ne": True})
        self.assertIn("$nor", users.find_queries[0])
        self.assertEqual(len(users.find_queries), 1)


if __name__ == "__main__":
    unittest.main()
