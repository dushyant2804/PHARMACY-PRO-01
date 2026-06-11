import copy
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_pharmacy")

from server import (
    DEMO_TENANT_ID,
    DEMO_USER_ID,
    REAL_TENANT_ID,
    _cleanup_unsafe_real_users,
    _seed_admin_if_enabled,
    list_users,
)


class Cursor:
    def __init__(self, documents):
        self.documents = documents

    async def to_list(self, _limit):
        return copy.deepcopy(self.documents)


class InMemoryUsers:
    def __init__(self, users=()):
        self.users = [copy.deepcopy(user) for user in users]
        self.inserted = []
        self.deleted = []

    @classmethod
    def matches(cls, document, query):
        if "$and" in query and not all(cls.matches(document, item) for item in query["$and"]):
            return False
        if "$or" in query and not any(cls.matches(document, item) for item in query["$or"]):
            return False
        if "$nor" in query and any(cls.matches(document, item) for item in query["$nor"]):
            return False
        for key, expected in query.items():
            if key.startswith("$"):
                continue
            actual = document.get(key)
            if isinstance(expected, dict):
                if "$ne" in expected and actual == expected["$ne"]:
                    return False
                if "$in" in expected and actual not in expected["$in"]:
                    return False
            elif actual != expected:
                return False
        return True

    async def find_one(self, query, *args, **kwargs):
        return next((copy.deepcopy(user) for user in self.users if self.matches(user, query)), None)

    def find(self, query, *args, **kwargs):
        return Cursor([user for user in self.users if self.matches(user, query)])

    async def insert_one(self, document):
        self.inserted.append(copy.deepcopy(document))
        self.users.append(copy.deepcopy(document))
        return SimpleNamespace(inserted_id=document["id"])

    async def count_documents(self, query):
        return sum(self.matches(user, query) for user in self.users)

    async def delete_one(self, query):
        self.deleted.append(copy.deepcopy(query))
        for index, user in enumerate(self.users):
            if self.matches(user, query):
                self.users.pop(index)
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


class AdminSeedingSecurityTest(unittest.IsolatedAsyncioTestCase):
    async def test_startup_admin_seed_is_disabled_unless_explicitly_true(self):
        users = InMemoryUsers()
        with patch.dict(os.environ, {"ADMIN_EMAIL": "owner@example.com", "ADMIN_PASSWORD": "StrongPass123"}, clear=False), \
                patch.dict(os.environ, {"SEED_ADMIN": "false"}), \
                patch("server.raw_db", SimpleNamespace(users=users)):
            await _seed_admin_if_enabled("2026-06-11T00:00:00+00:00")
        self.assertEqual(users.inserted, [])

    async def test_enabled_seed_requires_explicit_credentials(self):
        users = InMemoryUsers()
        with patch.dict(os.environ, {"SEED_ADMIN": "true", "ADMIN_EMAIL": "", "ADMIN_PASSWORD": ""}), \
                patch("server.raw_db", SimpleNamespace(users=users)):
            with self.assertRaisesRegex(RuntimeError, "requires explicit ADMIN_EMAIL and ADMIN_PASSWORD"):
                await _seed_admin_if_enabled("2026-06-11T00:00:00+00:00")
        self.assertEqual(users.inserted, [])

    async def test_enabled_seed_rejects_unsafe_or_weak_credentials(self):
        for email, password in (("admin@pharmacy.com", "StrongPass123"), ("owner@example.com", "admin123"), ("owner@example.com", "weak")):
            users = InMemoryUsers()
            with self.subTest(email=email, password=password), patch.dict(
                os.environ, {"SEED_ADMIN": "true", "ADMIN_EMAIL": email, "ADMIN_PASSWORD": password}
            ), patch("server.raw_db", SimpleNamespace(users=users)):
                with self.assertRaises(RuntimeError):
                    await _seed_admin_if_enabled("2026-06-11T00:00:00+00:00")
                self.assertEqual(users.inserted, [])

    async def test_enabled_seed_uses_only_real_tenant(self):
        users = InMemoryUsers()
        with patch.dict(os.environ, {"SEED_ADMIN": "true", "ADMIN_EMAIL": "owner@example.com", "ADMIN_PASSWORD": "StrongPass123"}), \
                patch("server.raw_db", SimpleNamespace(users=users)):
            await _seed_admin_if_enabled("2026-06-11T00:00:00+00:00")
        self.assertEqual(users.inserted[0]["tenant_id"], REAL_TENANT_ID)
        self.assertEqual(users.inserted[0]["shop_id"], REAL_TENANT_ID)
        self.assertTrue(users.inserted[0]["system_seeded"])

    async def test_cleanup_never_deletes_only_or_normal_real_admin(self):
        only_admin = {"id": "only", "email": "admin@gmail.com", "name": "Administrator", "role": "admin", "tenant_id": REAL_TENANT_ID, "system_seeded": True}
        normal_admin = {"id": "normal", "email": "owner@example.com", "name": "Owner", "role": "admin", "tenant_id": REAL_TENANT_ID}
        for users in (InMemoryUsers([only_admin]), InMemoryUsers([normal_admin])):
            with patch("server.raw_db", SimpleNamespace(users=users)):
                await _cleanup_unsafe_real_users()
            self.assertEqual(len(users.users), 1)
            self.assertEqual(users.deleted, [])

    async def test_cleanup_deletes_only_marked_unsafe_default_when_another_admin_exists(self):
        users = InMemoryUsers([
            {"id": "owner", "email": "owner@example.com", "name": "Owner", "role": "admin", "tenant_id": REAL_TENANT_ID},
            {"id": "unsafe", "email": "admin@pharmacy.com", "name": "Administrator", "role": "admin", "tenant_id": REAL_TENANT_ID, "system_seeded": True},
            {"id": "manual", "email": "admin@gmail.com", "name": "Manual Cashier", "role": "cashier", "tenant_id": REAL_TENANT_ID},
            {"id": "other-tenant", "email": "admin@pharmacy.com", "name": "Administrator", "role": "admin", "tenant_id": "another-shop", "system_seeded": True},
        ])
        with patch("server.raw_db", SimpleNamespace(users=users)):
            await _cleanup_unsafe_real_users()
        self.assertEqual({user["id"] for user in users.users}, {"owner", "manual", "other-tenant"})
        self.assertEqual(users.deleted, [{"id": "unsafe", "tenant_id": REAL_TENANT_ID}])

    async def test_real_user_management_hides_demo_and_marked_unsafe_defaults(self):
        users = InMemoryUsers([
            {"id": "real", "email": "owner@example.com", "role": "admin", "tenant_id": REAL_TENANT_ID, "is_demo": False},
            {"id": DEMO_USER_ID, "email": "demo@example.com", "role": "admin", "tenant_id": REAL_TENANT_ID, "is_demo": False},
            {"id": "leaked-demo", "email": "x@example.com", "role": "admin", "tenant_id": REAL_TENANT_ID, "is_demo": True},
            {"id": "unsafe", "email": "admin@pharmacy.com", "name": "Administrator", "role": "admin", "tenant_id": REAL_TENANT_ID, "system_seeded": True},
            {"id": "manual", "email": "admin@gmail.com", "name": "Manually Created", "role": "cashier", "tenant_id": REAL_TENANT_ID, "is_demo": False},
        ])
        with patch("server.raw_db", SimpleNamespace(users=users)):
            result = await list_users({"id": "real", "role": "admin", "tenant_id": REAL_TENANT_ID, "is_demo": False})
            demo_result = await list_users({"id": DEMO_USER_ID, "role": "admin", "tenant_id": DEMO_TENANT_ID, "is_demo": True})
        self.assertEqual({user["id"] for user in result}, {"real", "manual"})
        self.assertEqual(demo_result, [])


if __name__ == "__main__":
    unittest.main()
