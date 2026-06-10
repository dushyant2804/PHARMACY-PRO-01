import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from fastapi import Response

from server import (
    BUSINESS_COLLECTIONS,
    DEMO_TENANT_ID,
    TenantAwareCollection,
    UserLogin,
    _canonicalize_user_tenant,
    _current_demo,
    _current_tenant,
    _request_active,
    _seed_demo_data,
    hash_password,
    login,
)


class RecordingCollection:
    def __init__(self):
        self.delete_queries = []
        self.replacements = []

    async def delete_many(self, query):
        self.delete_queries.append(query)
        return SimpleNamespace(deleted_count=0)

    async def replace_one(self, query, replacement, **kwargs):
        self.replacements.append((query, replacement, kwargs))
        return SimpleNamespace(matched_count=1)


class DemoTenantIsolationTest(unittest.IsolatedAsyncioTestCase):
    def test_demo_identity_is_forced_to_canonical_shop(self):
        user = _canonicalize_user_tenant({
            "id": "demo-user",
            "tenant_id": "real_shop",
            "shop_id": "real_shop",
            "is_demo": False,
        })

        self.assertEqual(user["tenant_id"], "demo_shop")
        self.assertEqual(user["shop_id"], "demo_shop")
        self.assertTrue(user["is_demo"])

    async def test_normal_login_cannot_return_demo_user_in_real_shop(self):
        demo_user = {
            "id": "demo-user",
            "email": "demo@pharmacy.com",
            "password_hash": hash_password("StrongPass123"),
            "name": "Demo Pharmacist",
            "role": "admin",
            "tenant_id": "real_shop",
            "shop_id": "real_shop",
            "is_demo": False,
            "active": True,
        }
        users = AsyncMock()
        users.find_one.return_value = demo_user

        with patch("server.raw_db", SimpleNamespace(users=users)):
            result = await login(UserLogin(identifier=demo_user["email"], password="StrongPass123"), Response())

        self.assertEqual(result["tenant_id"], DEMO_TENANT_ID)
        self.assertEqual(result["shop_id"], DEMO_TENANT_ID)
        self.assertTrue(result["is_demo"])

    async def test_reseed_removes_misplaced_demo_records_and_stamps_demo_shop(self):
        collections = {name: RecordingCollection() for name in BUSINESS_COLLECTIONS}
        users = RecordingCollection()
        class RecordingDatabase:
            def __init__(self):
                self.users = users

            def __getitem__(self, name):
                return collections[name]

        raw_db = RecordingDatabase()

        with patch("server.raw_db", raw_db):
            await _seed_demo_data("2026-06-10T00:00:00+00:00")

        self.assertEqual(users.replacements[0][1]["tenant_id"], DEMO_TENANT_ID)
        self.assertEqual(users.replacements[0][1]["shop_id"], DEMO_TENANT_ID)
        self.assertTrue(users.replacements[0][1]["is_demo"])
        for collection in collections.values():
            for query in collection.delete_queries:
                self.assertEqual(query["tenant_id"], {"$ne": DEMO_TENANT_ID})
            for _, replacement, _ in collection.replacements:
                self.assertEqual(replacement["tenant_id"], DEMO_TENANT_ID)
                self.assertEqual(replacement["shop_id"], DEMO_TENANT_ID)

    async def test_aggregate_and_distinct_are_scoped_when_switching_tenants(self):
        collection = Mock()
        collection.distinct = AsyncMock(return_value=[])
        scoped = TenantAwareCollection(collection, "medicines")
        active = _request_active.set(True)
        tenant = _current_tenant.set("real_shop")
        demo = _current_demo.set(False)
        try:
            scoped.aggregate([{"$group": {"_id": "$category"}}])
            await scoped.distinct("category", {"active": True})
            _current_tenant.set(DEMO_TENANT_ID)
            scoped.aggregate([])
        finally:
            _current_demo.reset(demo)
            _current_tenant.reset(tenant)
            _request_active.reset(active)

        self.assertEqual(
            collection.aggregate.call_args_list[0].args[0][0],
            {"$match": {"tenant_id": "real_shop"}},
        )
        collection.distinct.assert_awaited_once_with(
            "category",
            {"$and": [{"tenant_id": "real_shop"}, {"active": True}]},
        )
        self.assertEqual(
            collection.aggregate.call_args_list[1].args[0][0],
            {"$match": {"tenant_id": DEMO_TENANT_ID}},
        )


if __name__ == "__main__":
    unittest.main()
